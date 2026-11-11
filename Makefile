.PHONY: tidy fmt lint test build ci

tidy:
	go mod tidy

fmt:
	gofmt -w .
	goimports -w .

lint:
	golangci-lint run ./...

test:
	go test -race -count=1 -coverprofile=coverage.out ./...

build:
	mkdir -p bin
	go build -o bin/dpp ./cmd/api

ci: tidy lint test build
