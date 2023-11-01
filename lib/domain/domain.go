package domain

type ThreadsRequest struct {
	Limit   int32
	Headers map[string]string
}

// type ThreadsResponse []int32

type PostsRequest struct {
	Limit   int32
	Threads []int32
	Headers map[string]string
}

type PostsResponse struct {
	Posts map[int32][]int32
}
