#!/usr/bin/make -f

test: fmt
	GORACE="atexit_sleep_ms=50" go test -count=1 -timeout=1s -short -race -covermode=atomic ./...

fmt:
	go fmt ./... && go mod tidy

test.load: test
	GORACE="atexit_sleep_ms=50" go test -v -count=1 -short=false -race -covermode=atomic -run=TestLoad

.PHONY: test fmt test.load
