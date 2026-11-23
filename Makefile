GO ?= go

# terraform in deploy/terraform, with the cluster's credentials hidden in its output.
TF ?= scripts/k8s/terraform.sh

.PHONY: all build test vet race fmt proto cluster-up cluster-down kube-up kube-down kube-smoke kube-audit chaos bench-storage bench-storage-quick crash-test crash-test-quick sim results clean

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

# Regenerate the gRPC code from internal/pb/raftkv.proto. Needs protoc on PATH; the
# two Go plugins are pinned in go.mod and built by `go tool`.
proto:
	protoc --plugin=protoc-gen-go=$$($(GO) tool -n protoc-gen-go) \
		--plugin=protoc-gen-go-grpc=$$($(GO) tool -n protoc-gen-go-grpc) \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		internal/pb/raftkv.proto

# A 3-node cluster on Docker Compose. Use the CLI from inside the Compose network:
#   docker compose run --rm cli put k v
cluster-up:
	docker compose up -d --build --wait

cluster-down:
	docker compose down -v

# The same 3 nodes on Kubernetes. Terraform creates a kind cluster inside Docker,
# builds the image, loads it, and deploys raftkv; kube-down removes all of it.
# Then: export KUBECONFIG=deploy/terraform/kubeconfig
kube-up:
	$(TF) init -input=false -lockfile=readonly
	$(TF) apply -auto-approve -input=false

kube-down:
	$(TF) destroy -auto-approve -input=false

# The live checks, 5 leader failovers and one rolling update, on the cluster
# kube-up made. This is what CI runs. Reports go to results/tmp/k8s.
kube-smoke:
	$(GO) run ./cmd/kubebench check -out results/tmp/k8s/checks.json -log results/tmp/k8s/checks.log
	$(GO) run ./cmd/kubebench failover -trials 5 -out results/tmp/k8s/failover.json -artifacts results/tmp/k8s/failover
	$(GO) run ./cmd/kubebench rollout -rounds 1 -out results/tmp/k8s/rollout.json -artifacts results/tmp/k8s/rollout

# Everything in docs/acceptance/k8s.md -> results/k8s-*.json (about 35 minutes).
# It destroys the cluster first and leaves none behind.
kube-audit:
	scripts/audit/k8s/run.sh

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

# Seeded Raft simulation schedules -> results/sim.json
sim:
	$(GO) run ./cmd/raftsim -first 1 -count 10000 -out results/sim.json

# Seeded partition schedules on Docker, checked with Porcupine -> results/chaos.json
chaos:
	$(GO) run ./cmd/chaos -first 1 -count 100 -out results/chaos.json

# Render results/*.json into results/RESULTS.md and the README results block
results:
	$(GO) run ./cmd/results

clean:
	rm -rf bin
