GO ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS := -s -w -X github.com/willvar/dofs/cli.Version=$(VERSION) -X github.com/willvar/dofs/cli.Commit=$(COMMIT)

.PHONY: build build-postgres test test-postgres test-fuse test-docker test-e2e vet

build:
	mkdir -p bin
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/dofsd ./cmd/dofsd

build-postgres:
	mkdir -p bin
	cd cmd/dofsd-postgres && GOWORK=off $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o ../../bin/dofsd-postgres .

test:
	$(GO) test ./...

test-postgres:
	cd integration/postgres && GOWORK=off $(GO) test ./...

test-fuse:
	DOFS_FUSE_INTEGRATION=1 $(GO) test . -run TestRealFUSEMarvoAtomicIOContract -v -count=1

test-docker:
	DOFS_DOCKER_INTEGRATION=1 $(GO) test . -run TestFUSEBindMountIntoDocker -v -count=1

test-e2e:
	DOFS_S3_FUSE_INTEGRATION=1 $(GO) test ./service -run TestSQLiteS3FUSERestartContract -v -count=1

vet:
	$(GO) vet ./...
