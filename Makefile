VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -X 'github.com/decentrio/stitch/internal/cmd.version=$(VERSION)' \
           -X 'github.com/decentrio/stitch/internal/cmd.commit=$(COMMIT)' \
           -X 'github.com/decentrio/stitch/internal/cmd.date=$(DATE)' \
           -w -s

.PHONY: build install test test-race lint vet tidy clean run

build:
	@go build -mod=readonly -ldflags "$(LDFLAGS)" -o build/stitch ./cmd/stitch

install:
	@go install -mod=readonly -ldflags "$(LDFLAGS)" ./cmd/stitch

test:
	@go test -mod=readonly --timeout=5m ./...

test-race:
	@go test -mod=readonly -race --timeout=10m ./...

vet:
	@go vet ./...

lint:
	@golangci-lint run --timeout=5m

tidy:
	@go mod tidy

clean:
	@rm -rf build/

run: build
	@./build/stitch start --config config-example.yaml
