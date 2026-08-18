.PHONY: build test race run

build:
	mkdir -p bin
	go build -o bin/queuemaxxing ./cmd/queuemaxxing
	go build -o bin/qmctl ./cmd/qmctl

test:
	go test ./...

race:
	go test -race ./...

run:
	go run ./cmd/queuemaxxing
