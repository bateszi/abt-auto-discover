package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"golang.org/x/net/html"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
)

type AppConfig struct {
	Db DbConfig `json:"db"`
}

type DbConfig struct {
	User     string `json:"user"`
	Password string `json:"pass"`
	Server   string `json:"server"`
	DbName   string `json:"dbName"`
}

type Post struct {
	Id    int64
	Url   string
	Title string
	Body  string
}

type ExternalUrl struct {
	Link   string
	Url    *url.URL
	PostId int64
}

type ExternalPage struct {
	Url     ExternalUrl
	Html    []byte
	Fetched bool
}

func makeDbConnection() (*sql.DB, error) {
	encodedJson, err := ioutil.ReadFile("config/config.json")
	if err != nil {
		panic(err)
	}

	config := AppConfig{}

	err = json.Unmarshal(encodedJson, &config)
	if err != nil {
		panic(err)
	}

	dbParams := make(map[string]string)
	dbParams["charset"] = "utf8mb4"

	dbConfig := mysql.Config{
		User:   config.Db.User,
		Passwd: config.Db.Password,
		Net:    "tcp",
		Addr:   config.Db.Server,
		DBName: config.Db.DbName,
		Params: dbParams,
	}

	db, err := sql.Open("mysql", dbConfig.FormatDSN())
	if err != nil {
		return db, err
	}

	err = db.Ping()
	if err != nil {
		return db, err
	}

	fmt.Println("opened database connection")

	return db, nil
}

// Get the latest posts added to the posts table that have some content/HTML saved
func getPosts(db *sql.DB) ([]Post, error) {
	var posts []Post

	getPostRows, err := db.Query(
		"SELECT pk_post_id, post_title, link, content " +
			"FROM rss_aggregator.posts " +
			"WHERE pub_date >= now() - INTERVAL 2 hour " +
			"ORDER BY pub_date DESC",
	)

	if err != nil {
		return posts, err
	}

	defer func(getRows *sql.Rows) {
		err := getRows.Close()
		if err != nil {
			panic(err)
		}
	}(getPostRows)

	for getPostRows.Next() {
		var postId int64
		var title string
		var postUrl string
		var body string

		err = getPostRows.Scan(
			&postId,
			&title,
			&postUrl,
			&body,
		)

		if err != nil {
			return posts, nil
		}

		if len(body) > 0 {
			posts = append(posts, Post{
				Id:    postId,
				Url:   postUrl,
				Title: title,
				Body:  body,
			})
		}
	}

	return posts, nil
}

// Parse a post for external links
func getUrlsFromPost(post Post) ([]ExternalUrl, error) {
	var provisionalUrls []string

	r := strings.NewReader(post.Body)
	tokenizer := html.NewTokenizer(r)

	for {
		tokenType := tokenizer.Next()

		if tokenType == html.ErrorToken {
			err := tokenizer.Err()

			if err == io.EOF {
				break
			}
		}

		token := tokenizer.Token()

		if token.Data == "a" {
			for i := range token.Attr {
				if token.Attr[i].Key == "href" {
					provisionalUrls = append(provisionalUrls, token.Attr[i].Val)
				}
			}
		}
	}

	var externalUrls []ExternalUrl

	if len(provisionalUrls) > 0 {
		postUrl, err := url.Parse(post.Url)

		if err != nil {
			fmt.Println("could not parse parent post url", post.Url, err)
			return nil, err
		}

		for key, provisionalUrl := range provisionalUrls {
			parsedUrl, err := url.Parse(provisionalUrl)

			if err != nil {
				fmt.Println("could not parse url", provisionalUrl, err, key)
				continue
			}

			fileExt := strings.ToLower(path.Ext(parsedUrl.Path))

			if fileExt == ".png" || fileExt == ".jpg" || fileExt == ".gif" || fileExt == ".mp4" {
				continue
			}

			if parsedUrl.Scheme != "http" && parsedUrl.Scheme != "https" {
				continue
			}

			if postUrl.Host != parsedUrl.Host {
				externalUrls = append(externalUrls, ExternalUrl{
					Link:   provisionalUrl,
					Url:    parsedUrl,
					PostId: post.Id,
				})
			}
		}
	}

	return externalUrls, nil
}

// The external site may have already been queued, so before we try to fetch it, let's check
func isInBlacklist(db *sql.DB, candidate ExternalUrl) (bool, error) {
	var hostInBlacklist int

	err := db.QueryRow("SELECT COUNT(*) AS ttl "+
		"FROM discovered_sites_blacklist "+
		"WHERE host = ?", candidate.Url.Host).Scan(&hostInBlacklist)

	if err != nil {
		return false, err
	}

	if hostInBlacklist > 0 {
		return true, nil
	}

	return false, nil
}

// Fetch the HTML of the external site/page
func fetchExternalPages(candidates []ExternalUrl) ([]ExternalPage, error) {
	var externalPages []ExternalPage

	var externalPagesWg sync.WaitGroup
	externalPageChannel := make(chan ExternalPage, len(candidates))

	for _, candidate := range candidates {
		externalPagesWg.Add(1)

		go fetchExternalPage(candidate, &externalPagesWg, externalPageChannel)
	}

	externalPagesWg.Wait()
	fmt.Println("finished fetching candidate pages")
	close(externalPageChannel)

	for j := 0; j < len(candidates); j++ {
		externalPageInstance := <-externalPageChannel

		if externalPageInstance.Fetched {
			externalPages = append(externalPages, externalPageInstance)
		}
	}

	return externalPages, nil
}

func fetchExternalPage(candidate ExternalUrl, externalPagesWg *sync.WaitGroup, externalPageChannel chan<- ExternalPage) {
	var externalPage = ExternalPage{
		Url:     candidate,
		Fetched: false,
	}

	defer func(externalPage *ExternalPage, externalPagesWg *sync.WaitGroup, externalPageChannel chan<- ExternalPage) {
		externalPageChannel <- *externalPage
		externalPagesWg.Done()
	}(&externalPage, externalPagesWg, externalPageChannel)

	headReq, err := http.NewRequest("HEAD", candidate.Link, nil)

	if err != nil {
		fmt.Println("could not created head request", candidate.Link, err)
		return
	}

	if !strings.Contains(candidate.Link, "tumblr.com") {
		headReq.Header.Add("User-Agent", "@bateszi auto-discover spider")
	} else {
		headReq.Header.Add("User-Agent", "Baiduspider")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)

	defer func(cancel context.CancelFunc) {
		cancel()
	}(cancel)

	headReq = headReq.WithContext(ctx)

	headHttpClient := &http.Client{}
	headResponse, err := headHttpClient.Do(headReq)

	if err != nil {
		fmt.Println("error making head request", candidate.Link, err)
		return
	}

	defer func(resp *http.Response) {
		_ = resp.Body.Close()
	}(headResponse)

	verifiedContentType := false

	if headResponse.StatusCode == http.StatusOK && headResponse.StatusCode < 300 {
		contentType := headResponse.Header.Get("Content-Type")
		verifiedContentType = strings.Contains(contentType, "text/html")
	}

	if verifiedContentType {
		getReq, err := http.NewRequest("GET", candidate.Link, nil)

		if err != nil {
			fmt.Println("could not created get request", candidate.Link, err)
			return
		}

		if !strings.Contains(candidate.Link, "tumblr.com") {
			getReq.Header.Add("User-Agent", "@bateszi auto-discover spider")
		} else {
			getReq.Header.Add("User-Agent", "Baiduspider")
		}

		getCtx, getCancel := context.WithTimeout(context.Background(), time.Second*10)

		defer func(cancel context.CancelFunc) {
			cancel()
		}(getCancel)

		getReq = getReq.WithContext(getCtx)

		getHttpClient := &http.Client{}
		getResponse, err := getHttpClient.Do(getReq)

		if err != nil {
			fmt.Println("error making get request", candidate.Link, err)
			return
		}

		defer func(resp *http.Response) {
			_ = resp.Body.Close()
		}(getResponse)

		if getResponse.StatusCode == http.StatusOK && getResponse.StatusCode < 300 {
			externalPage.Html, err = ioutil.ReadAll(getResponse.Body)

			if err != nil {
				fmt.Println("could not read response body", candidate.Link, err)
				return
			}

			externalPage.Fetched = true
		}
	}
}

func getRelevancyScore(site ExternalPage) int {
	wordMap := make(map[string]int)
	wordMap["anime"] = 0
	wordMap["manga"] = 0

	r := bytes.NewReader(site.Html)
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		word := strings.ToLower(scanner.Text())

		if count, ok := wordMap[word]; ok {
			wordMap[word] = count + 1
		}
	}

	ttlScore := 0

	for _, wordCount := range wordMap {
		ttlScore = ttlScore + wordCount
	}

	return ttlScore
}

func getRssFeedUrl(site ExternalPage) string {
	var rssFeedUrl string
	hasRssFeed := false

	r := bytes.NewReader(site.Html)
	tokenizer := html.NewTokenizer(r)

	for {
		tokenType := tokenizer.Next()

		if tokenType == html.ErrorToken {
			err := tokenizer.Err()

			if err == io.EOF {
				break
			}
		}

		token := tokenizer.Token()

		if token.Data == "link" && !hasRssFeed {
			linkHref := ""

			for i := range token.Attr {
				if token.Attr[i].Key == "type" && token.Attr[i].Val == "application/rss+xml" {
					hasRssFeed = true
				} else if token.Attr[i].Key == "href" {
					linkHref = token.Attr[i].Val
				}
			}

			if hasRssFeed {
				rssFeedUrl = linkHref
				break
			}
		}
	}

	return rssFeedUrl
}

// Add the site to the queue for review
func addSiteToReviewQueue(db *sql.DB, site ExternalPage, score int, rssFeedUrl string) (bool, error) {
	prospectId := 0
	existingScore := 0
	encountered := 1

	err := db.QueryRow("SELECT pk_prospect_id, score, encountered "+
		"FROM discovered_sites_queue "+
		"WHERE fqdn = ?", site.Url.Url.Host).Scan(&prospectId, &existingScore, &encountered)

	if err != nil {
		if err.Error() != "sql: no rows in result set" {
			return false, err
		}
	}

	if prospectId > 0 {
		existingScore = existingScore + score
		encountered++

		stmt, err := db.Prepare("UPDATE `discovered_sites_queue` " +
			"SET `score` = ?, `encountered` = ?, `feed_url` = ? " +
			"WHERE `fqdn` = ?")

		if err != nil {
			return false, err
		}

		_, err = stmt.Exec(
			existingScore,
			encountered,
			rssFeedUrl,
			site.Url.Url.Host,
		)

		if err != nil {
			return false, err
		}
	} else {
		stmt, err := db.Prepare(
			"INSERT INTO `discovered_sites_queue` (`fqdn`, `score`, `encountered`, `feed_url`) VALUES (?, ?, ?, ?)",
		)

		if err != nil {
			return false, err
		}

		_, err = stmt.Exec(
			site.Url.Url.Host,
			score,
			encountered,
			rssFeedUrl,
		)

		if err != nil {
			return false, err
		}
	}

	fmt.Println("queued", site.Url.Url.Host)
	return true, nil
}

func start() {
	fmt.Println("starting auto discovery service")

	db, err := makeDbConnection()

	if err != nil {
		fmt.Println("could not open db connection", err)
		return
	}

	defer func(db *sql.DB) {
		fmt.Println("closing database connection at", time.Now().Format(time.RFC1123Z))
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}(db)

	posts, err := getPosts(db)

	if err != nil {
		fmt.Println("error getting posts", err)
	}

	var candidates []ExternalUrl

	if len(posts) > 0 {
		for _, post := range posts {
			urls, err := getUrlsFromPost(post)

			if err != nil {
				fmt.Println("error getting urls from posts", err)
			}

			if len(urls) > 0 {
				candidates = append(candidates, urls...)
			}
		}
	}

	if len(candidates) > 0 {
		var scheduledCandidates []ExternalUrl

	checkCandidates:
		for _, candidate := range candidates {
			alreadyDiscovered, err := isInBlacklist(db, candidate)

			if err != nil {
				fmt.Println("error checking if candidate has already been discovered", err)
			}

			if !alreadyDiscovered {
				for _, scheduledCandidate := range scheduledCandidates {
					if scheduledCandidate.Url.Host == candidate.Url.Host {
						continue checkCandidates
					}
				}

				scheduledCandidates = append(scheduledCandidates, candidate)
			}
		}

		if len(scheduledCandidates) > 0 {
			fetchedPages, err := fetchExternalPages(scheduledCandidates)

			if err != nil {
				fmt.Println("there was an error fetching external pages", err)
			}

			for _, fetchedPage := range fetchedPages {
				relevancyScore := getRelevancyScore(fetchedPage)
				rssFeedUrl := getRssFeedUrl(fetchedPage)

				_, err := addSiteToReviewQueue(db, fetchedPage, relevancyScore, rssFeedUrl)

				if err != nil {
					fmt.Println("there was an error adding site to queue", fetchedPage.Url.Link, err)
				}
			}
		}
	}
}

func runService(d time.Duration) {
	ticker := time.NewTicker(d)

	for _ = range ticker.C {
		start()
	}
}

func main() {
	start()

	interval := 2 * time.Hour
	go runService(interval)

	fmt.Println("starting ticker to automatically discover new sites every", interval)

	// Run application indefinitely
	select {}
}
