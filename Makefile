.PHONY: build up down test build_and

build_and:
	docker-compose -f docker-compose.yaml build

up:
	docker-compose -f docker-compose.yaml up -d --build

down:
	docker-compose -f docker-compose.yaml down

test:
	go test ./... -v

build:
	go build ./cmd/worker && go build ./cmd/processor && go build ./cmd/api-service