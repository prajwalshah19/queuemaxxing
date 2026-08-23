.PHONY: build build-compat test race fidelity run

build:
	mkdir -p bin
	go build -o bin/queuemaxxing ./cmd/queuemaxxing

build-compat: build
	go build -o bin/qmctl ./cmd/qmctl

test:
	go test ./...

race:
	go test -race ./...

fidelity: build
	QUEUEMAXXING_BIN="$(CURDIR)/bin/queuemaxxing" go test -tags=fidelity -count=1 ./...

run:
	go run ./cmd/queuemaxxing serve
