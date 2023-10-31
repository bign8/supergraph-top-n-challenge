package main

import (
	"fmt"

	"github.com/graphql-go/graphql"
)

// tested bia graphiql

func (c client) posts(threadID, limit int32) ([]Identified, error) {
	// time.Sleep(time.Millisecond * 20) // TODO: batching
	// if err := c.enc.Encode([2]int32{limit, threadID}); err != nil {
	// 	return nil, fmt.Errorf(`encode: %w`, err)
	// }
	if err := c.enc.Encode(PostsRequest{Limit: limit, Threads: []int32{threadID}}); err != nil {
		return nil, fmt.Errorf(`encode: %w`, err)
	}
	var batch PostsResponse
	if err := c.dec.Decode(&batch); err != nil {
		return nil, fmt.Errorf(`decode: %w`, err)
	}
	res := batch.Posts[threadID]
	output := make([]Identified, len(res))
	for i, id := range res {
		output[i].ID = int32(id)
	}
	return output, nil
}

func resolvePosts(p graphql.ResolveParams) (any, error) {
	limit := p.Args[`limit`].(int)
	thread := p.Source.(Identified)

	return func() (any, error) {
		// TODO: trace thunks!
		conn := pool.Get().(*client)
		defer pool.Put(conn)

		return conn.posts(thread.ID, int32(limit))
	}, nil
}
