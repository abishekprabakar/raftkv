# One image holds three binaries: raftkvd, the node; raftkv, the CLI, which is also
# the health check; and kvworker, the client the fault-injection harness (cmd/chaos)
# runs inside each node's container.

# Build with the same Go version as go.mod. Static binaries (no cgo), so the
# runtime image needs no C library.
FROM golang:1.27.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
# VERSION is what `raftkvd --version` prints. The rolling-update test builds this
# source twice under two versions, so it can tell which image a pod is running.
ARG VERSION=dev
# The cache mount keeps Go's build cache between builds, so a build that only
# changes VERSION relinks one binary instead of compiling everything again.
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-X main.version=${VERSION}" -o /out/raftkvd ./cmd/raftkvd \
 && CGO_ENABLED=0 go build -trimpath -o /out/raftkv ./cmd/raftkv \
 && CGO_ENABLED=0 go build -trimpath -o /out/kvworker ./cmd/kvworker \
 && mkdir /out/data

# Distroless: no shell and no package manager, just the binaries, running as a
# non-root user.
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/raftkvd /out/raftkv /out/kvworker /usr/local/bin/
# A fresh named volume copies the ownership of the directory it is mounted on, so
# /data must already belong to the nonroot user (65532) for the node to write it.
COPY --from=build --chown=65532:65532 /out/data /data
# 7000 is gRPC, for peers and clients. 8080 is the health endpoints, when
# --health turns them on.
EXPOSE 7000 8080
ENTRYPOINT ["/usr/local/bin/raftkvd"]
