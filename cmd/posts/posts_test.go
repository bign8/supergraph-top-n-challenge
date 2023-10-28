package main

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

// postgres populated with data from db/posts.sql
var request = PostsRequest{
	Limit: 4000, // larger than all results
	Threads: []uint32{
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

func TestProcessor(t *testing.T) {
	db, err := sql.Open(`postgres`, DSN)
	if err != nil {
		t.Skip(err)
		return
	}

	p := newProcessor(db)

	check := func(tb testing.TB, res PostsResponse) {
		assert.Len(tb, res.Posts, 40)
		assert.Len(tb, res.Posts[1704120699], 1997)
	}

	t.Run(`multi`, func(t *testing.T) {
		res := p.fetchBatchMulti(request)
		check(t, res)
	})

	t.Run(`fanOutArray`, func(t *testing.T) {
		res := p.fetchBatchFanOut(request, p.fetchThreadPostsViaArray)
		check(t, res)
	})

	t.Run(`fanOutRows`, func(t *testing.T) {
		res := p.fetchBatchFanOut(request, p.fetchThreadPostsViaRows)
		check(t, res)
	})
}
