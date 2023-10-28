package main

import (
	"database/sql"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/lib/pq"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

const DSN = `postgres://postgres:postgrespassword@[::]:7432?sslmode=disable`

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	db, err := sql.Open(`postgres`, DSN)
	check(err)
	fetchThreadPostsViaRows, err := db.Prepare(`SELECT id FROM posts WHERE thread_id = $1 LIMIT $2;`)
	check(err)
	fetchThreadPostsViaArray, err := db.Prepare(`SELECT ARRAY(SELECT id FROM posts WHERE thread_id = $1 LIMIT $2);`)
	check(err)
	fetchMultiThreadPosts, err := db.Prepare(`SELECT thread_id, (array_agg(id))[1:$2] FROM posts
		WHERE thread_id = ANY($1) GROUP BY thread_id;`)
	check(err)

	/*
		// logical unit tests on queries (should be unit tests!)

		THREADS := []int32{
			1704120699,
			760694634,
			1041771526,
			355369712,
			693787124,
			464032376,
			338943421,
			627233705,
			1247484754,
			144833217,
			864959089,
			550614719,
			86996137,
			1220619720,
			1653820545,
			186457978,
			531820194,
			1002078996,
			1001164236,
			934420795,
			1648048755,
			1651650915,
			1210145148,
			781737299,
			946891028,
			1441550032,
			635110282,
			568029274,
			597979419,
			909925540,
			11224305,
			465429524,
			1139094954,
			889600381,
			1843137403,
			1685445639,
			296975846,
			1981157226,
			996970829,
			1347165303,
		}
		LIMIT := 20

		// TESTING using sub-queries and Arrays
		now := time.Now()
		// warning: if limit is signifigantly less than 2000 (max number of threads per post) this may be slower
		check(err)
		rows, err := fetchMultiThreadPosts.Query(pq.Int32Array(THREADS), LIMIT)
		check(err)
		for rows.Next() {
			var (
				thread uint32
				posts  pq.Int32Array
			)
			check(rows.Scan(&thread, &posts))
			if thread == uint32(THREADS[0]) {
				log.Printf(`thread: %d; posts: %v`, thread, len(posts))
			}
		}
		check(rows.Close())
		log.Printf(`sub-queries: %s`, time.Since(now))

		// boring method
		now = time.Now()
		for _, threadID := range THREADS {
			rows, err = fetchThreadPostsViaRows.Query(threadID, LIMIT)
			check(err)
			var count uint32
			for rows.Next() {
				var post uint32
				check(rows.Scan(&post))
				count++
			}
			if threadID == THREADS[0] {
				log.Printf(`thread: %d; posts: %d`, threadID, count)
			}
			check(rows.Close())
		}
		log.Printf(`linear: %s`, time.Since(now))

		// array parser
		now = time.Now()
		for _, threadID := range THREADS {
			var posts pq.Int32Array // this is slightly faster!!!
			row := fetchThreadPostsViaArray.QueryRow(threadID, LIMIT)
			check(row.Scan(&posts))
			check(row.Err())
			if threadID == THREADS[0] {
				log.Printf(`thread: %d; posts: %d`, threadID, len(posts))
			}
		}
		log.Printf(`array: %s`, time.Since(now))

		//*/

	p := &processor{
		threadPostsViaRows:  fetchThreadPostsViaRows,
		threadPostsViaArray: fetchThreadPostsViaArray,
		multiThreadPosts:    fetchMultiThreadPosts,
	}

	l, err := net.Listen(`tcp`, `:8001`) // TODO: test UDP
	check(err)
	log.Printf(`posts on %v`, l.Addr())
	for {
		conn, err := l.Accept()
		check(err)
		log.Printf(`received connection: %v`, conn.RemoteAddr())
		go func() {
			check(p.processBatch(conn))
		}()
	}
}

type processor struct {
	threadPostsViaRows  *sql.Stmt
	threadPostsViaArray *sql.Stmt
	multiThreadPosts    *sql.Stmt
}

/*
Frame Format: (all values are uint32)
Input: [limit][thread_id]
Output: [length][post1_id][post2_id]...[postN_id]
*/

// TODO: batch fetch sets of post IDs

func (p processor) process(conn net.Conn) error {
	defer conn.Close()
	reader := gob.NewDecoder(conn)
	writer := gob.NewEncoder(conn)
	for {

		// parse input
		var args [2]uint32
		if err := reader.Decode(&args); err == io.EOF {
			log.Printf(`closing connection: %v`, conn.RemoteAddr())
			return nil
		} else if err != nil {
			return fmt.Errorf(`decode: %w`, err)
		}
		limit := args[0]
		threadID := args[1]

		// query database
		out, err := p.fetchThreadPostsViaRows(threadID, limit)
		if err != nil {
			return fmt.Errorf(`fetchThread: %w`, err)
		}

		// write result
		if err := writer.Encode(out); err != nil {
			return fmt.Errorf(`encode: %w`, err)
		}
	}
}

// TODO: measure (used in fetchBatchFanOut)
func (p processor) fetchThreadPostsViaRows(threadID, limit uint32) ([]uint32, error) {
	rows, err := p.threadPostsViaRows.Query(threadID, limit)
	if err != nil {
		return nil, fmt.Errorf(`query: %w`, err)
	}
	output := make([]uint32, 0, limit)
	for rows.Next() {
		var id uint32
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf(`scan: %w`, err)
		}
		output = append(output, id)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf(`close: %w`, err)
	}
	return output, nil
}

// TODO: measure (used in fetchBatchFanOut)
func (p processor) fetchThreadPostsViaArray(threadID, limit uint32) ([]uint32, error) {
	var arr pq.Int32Array
	if err := p.threadPostsViaArray.QueryRow(threadID, limit).Scan(&arr); err != nil {
		return nil, fmt.Errorf(`query/scan: %w`, err)
	}
	output := make([]uint32, len(arr))
	for i, out := range arr {
		output[i] = uint32(out)
	}
	return output, nil
}

type PostsRequest struct {
	Limit   uint32
	Threads []uint32
}

type PostsResponse struct {
	Posts map[uint32][]uint32
}

func (p processor) processBatch(conn net.Conn) error {
	defer conn.Close()
	reader := gob.NewDecoder(conn)
	writer := gob.NewEncoder(conn)

	for {

		// parse input
		var args PostsRequest
		if err := reader.Decode(&args); err == io.EOF {
			log.Printf(`closing connection: %v`, conn.RemoteAddr())
			return nil
		} else if err != nil {
			return fmt.Errorf(`decode: %w`, err)
		}

		result := p.fetchBatchMulti(args)

		// write result
		if err := writer.Encode(result); err != nil {
			return fmt.Errorf(`encode: %w`, err)
		}
	}
}

// TODO: measure (used in processBatch)
func (p processor) fetchBatchFanOut(args PostsRequest) PostsResponse {
	// TODO: short circut if only 1 thread fetched

	start := time.Now()

	// query database
	var wg sync.WaitGroup
	var stage sync.Map
	wg.Add(int(len(args.Threads)))
	for _, threadID := range args.Threads {
		go func(threadID uint32) {
			defer wg.Done()
			out, err := p.fetchThreadPostsViaArray(threadID, args.Limit)
			if err != nil {
				panic(err)
			}
			stage.Store(threadID, out)
		}(threadID)
	}
	wg.Wait()

	// serialize
	result := PostsResponse{
		Posts: make(map[uint32][]uint32, int(args.Limit)),
	}
	stage.Range(func(key, value any) bool {
		result.Posts[key.(uint32)] = value.([]uint32)
		return true
	})
	log.Printf(`fetching %d posts of %d threads took %s`, args.Limit, len(args.Threads), time.Since(start))
	return result
}

// TODO: measure (used in processBatch)
func (p processor) fetchBatchMulti(args PostsRequest) PostsResponse {
	start := time.Now()
	result := PostsResponse{
		Posts: make(map[uint32][]uint32, len(args.Threads)),
	}

	// convert unsigned => signed (domain => postgres)
	threads := make([]int32, len(args.Threads))
	for i, threadID := range args.Threads {
		threads[i] = int32(threadID)
	}

	rows, err := p.multiThreadPosts.Query(pq.Int32Array(threads), args.Limit)
	if err != nil {
		panic(err)
	}
	var (
		thread      uint32
		postsSigned pq.Int32Array
	)
	for rows.Next() {
		if err := rows.Scan(&thread, &postsSigned); err != nil {
			panic(err)
		}

		// convert signed to unsigned (postgres => domain)
		posts := make([]uint32, len(postsSigned))
		for i, postID := range postsSigned {
			posts[i] = uint32(postID)
		}
		result.Posts[thread] = posts
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}

	log.Printf(`fetching %d posts of %d threads took %s`, args.Limit, len(args.Threads), time.Since(start))
	return result
}
