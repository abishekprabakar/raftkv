GO ?= go

.PHONY: all build test vet race fmt bench-storage bench-storage-quick crash-test crash-test-quick results clean

all: vet test

build:
	$(GO) build ./...

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

race:
	$(GO) test -race -count=1 ./...

fmt:
	gofmt -l -w ./cmd ./internal

# Storage engine benchmarks -> results/storage.json
bench-storage:
	$(GO) run ./cmd/storagebench -out results/storage.json

bench-storage-quick:
	$(GO) run ./cmd/storagebench -quick -out results/storage-quick.json

# SIGKILL crash-recovery harness -> results/crash.json
crash-test:
	$(GO) run ./cmd/crashtest -trials 500 -out results/crash.json

crash-test-quick:
	$(GO) run ./cmd/crashtest -trials 20 -v

# Render results/*.json into results/RESULTS.md and the README results block
results:
	$(GO) run ./cmd/results

clean:
	rm -rf bin
