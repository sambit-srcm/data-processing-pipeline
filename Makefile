.PHONY: tidy fmt lint test test-integration build ci

tidy:
	go mod tidy

fmt:
	gofmt -w .
	goimports -w .

lint:
	golangci-lint run ./...

test:
	go test -race -count=1 -coverprofile=coverage.out ./...

test-integration:
	go test -race -count=1 ./tests/integration/...

build:
	mkdir -p bin
	go build -o bin/dpp ./cmd/api

ci: tidy lint test build
