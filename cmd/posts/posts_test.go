package main

import (
	"fmt"
	"sync"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

// postgres populated with data from db/posts.sql
var request = PostsRequest{
	Limit: 4000, // larger than all results
	Threads: []int32{
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
	},
}

func checkResults(tb testing.TB, res PostsResponse) {
	assert.Len(tb, res.Posts, 40)
	if request.Limit < 1997 {
		assert.Len(tb, res.Posts[1704120699], int(request.Limit))
	} else {
		assert.Len(tb, res.Posts[1704120699], 1997)
	}

}

func connect(tb testing.TB) *processor {
	p, err := newProcessor()
	if err != nil {
		tb.Skip(err)
		tb.SkipNow()
	}
	return p
}

func TestProcessor(t *testing.T) {
	p := connect(t)

	t.Run(`multi`, func(t *testing.T) {
		res := p.fetchBatchMulti(request)
		checkResults(t, res)
	})

	t.Run(`fanOutArray`, func(t *testing.T) {
		res := p.fetchBatchFanOut(request, p.fetchThreadPostsViaArray)
		checkResults(t, res)
	})

	t.Run(`fanOutRows`, func(t *testing.T) {
		res := p.fetchBatchFanOut(request, p.fetchThreadPostsViaRows)
		checkResults(t, res)
	})
}

func BenchmarkMulti(b *testing.B) {
	p := connect(b)
	benchmark(b, p.fetchBatchMulti)
}

func BenchmarkFanOutArray(b *testing.B) {
	p := connect(b)
	benchmark(b, func(pr PostsRequest) PostsResponse {
		return p.fetchBatchFanOut(pr, p.fetchThreadPostsViaArray)
	})
}

func BenchmarkFanOutRows(b *testing.B) {
	p := connect(b)
	benchmark(b, func(pr PostsRequest) PostsResponse {
		return p.fetchBatchFanOut(pr, p.fetchThreadPostsViaRows)
	})
}

func benchmark(b *testing.B, subject func(PostsRequest) PostsResponse) {
	for i := 0; i < b.N; i++ {
		res := subject(request)
		checkResults(b, res)
	}
}

/// KEEPING THE FOLLOWING AROUND FOR TESTING COMPARISON

// TODO: measure (used in processBatch)
func (p processor) fetchBatchFanOut(args PostsRequest, fn func(threadID, limit int32) ([]int32, error)) PostsResponse {
	// TODO: short circut if only 1 thread fetched

	// start := time.Now()

	// query database
	var wg sync.WaitGroup
	var stage sync.Map
	wg.Add(int(len(args.Threads)))
	for _, threadID := range args.Threads {
		go func(threadID int32) {
			defer wg.Done()
			out, err := fn(threadID, args.Limit)
			check(err)
			stage.Store(threadID, out)
		}(threadID)
	}
	wg.Wait()

	// serialize
	result := PostsResponse{
		Posts: make(map[int32][]int32, int(args.Limit)),
	}
	stage.Range(func(key, value any) bool {
		result.Posts[key.(int32)] = value.([]int32)
		return true
	})
	// log.Printf(`fetching %d posts of %d threads took %s`, args.Limit, len(args.Threads), time.Since(start))
	return result
}

// TODO: measure (used in fetchBatchFanOut)
func (p processor) fetchThreadPostsViaRows(threadID, limit int32) ([]int32, error) {
	rows, err := p.threadPostsViaRows.Query(threadID, limit)
	if err != nil {
		return nil, fmt.Errorf(`query: %w`, err)
	}
	output := make([]int32, 0, limit)
	for rows.Next() {
		var id int32
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
func (p processor) fetchThreadPostsViaArray(threadID, limit int32) ([]int32, error) {
	var arr pq.Int32Array
	if err := p.threadPostsViaArray.QueryRow(threadID, limit).Scan(&arr); err != nil {
		return nil, fmt.Errorf(`query/scan: %w`, err)
	}
	return arr, nil
}
