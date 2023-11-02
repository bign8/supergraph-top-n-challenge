import { post } from "k6/http";
import { check } from "k6";

export const options = {
  scenarios: {
    '100rps': {
      executor: 'constant-arrival-rate',
      duration: '30s',     // How long the test lasts
      rate: 100,           // How many iterations per timeUnit
      timeUnit: '1s',      // Start `rate` iterations per second
      preAllocatedVUs: 50, // Pre-allocate VUs
    },
    // 'burst': {
    //   executor: 'constant-vus',
    //   duration: '1s', // this test is really heavy!
    //   vus: 100
    // }
  },
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
  let res = post(__ENV.GRAPHQL_ENDPOINT,
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
