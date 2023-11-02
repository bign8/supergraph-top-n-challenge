import http from "k6/http";
import { check, fail, sleep } from "k6";

export const options = {
  vus: 100,
  duration: '1s', //'60s',
};

const query = `query MagicSauce($threadLimit: Int!, $postLimit: Int!) {
  threads(limit: $threadLimit) {
    id
    posts(limit: $postLimit) {
      id
    }
  }
}`;

const THREADS = 4;
const POSTS = 20;

export default function() {
  let res = http.post(__ENV.GRAPHQL_ENDPOINT,
    JSON.stringify({
      operationName: `MagicSauce`,
      query: query,
      variables: {
        threadLimit: THREADS,
        postLimit: POSTS,
      }
    }),
    {
      headers: {
        "Content-Type": "application/json"
      }
    }
  );

  check(res.json().data, {
    'graphql errors': data => !data.hasOwnProperty('errors') || data.errors.length === 0,
    'threads': data => data.threads.length === THREADS,
    'thread posts': data => data.threads.every(thread => thread.posts.length === POSTS)
  })
}
