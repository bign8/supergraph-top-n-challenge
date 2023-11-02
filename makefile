serve:
	docker compose up --build

deps:
	docker compose -f docker-compose-db.yaml up

test:
	docker compose -f k6-test/docker-compose-k6.yaml run k6 run /scripts/graphql.js

pprof:
	go tool pprof -http=: http://localhost:8000/debug/pprof/profile?seconds=20
