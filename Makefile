.PHONY: all deps clean docker test fmt lint install

TAGS =

INSTALL_DIR        = $(GOPATH)/bin
DEST_DIR           = ./target
PATHINSTBIN        = $(DEST_DIR)/bin
PATHINSTDOCKER     = $(DEST_DIR)/docker

VERSION   := $(shell git describe --tags || echo "v0.0.0")
VER_CUT   := $(shell echo $(VERSION) | cut -c2-)
VER_MAJOR := $(shell echo $(VER_CUT) | cut -f1 -d.)
VER_MINOR := $(shell echo $(VER_CUT) | cut -f2 -d.)
VER_PATCH := $(shell echo $(VER_CUT) | cut -f3 -d.)
VER_RC    := $(shell echo $(VER_PATCH) | cut -f2 -d-)
DATE      := $(shell date +"%Y-%m-%dT%H:%M:%SZ")

LD_FLAGS   = -w -s
GO_FLAGS   =
DOCS_FLAGS =

APPS = volteras-oracle
all: $(APPS)

volteras-oracle:
	@go build -o $(PATHINSTBIN)/volteras-oracle ./cmd/volteras-oracle

install: $(APPS)
	@mkdir -p bin
	@cp $(PATHINSTBIN)/volteras-oracle ./bin/

deps:
	@go mod tidy
	@go mod vendor

docker: deps
	@docker build -f ./resources/docker/Dockerfile . -t dimozone/volteras-oracle:$(VER_CUT)
	@docker tag dimozone/volteras-oracle:$(VER_CUT) dimozone/volteras-oracle:latest

fmt:
	@go list -f {{.Dir}} ./... | xargs -I{} gofmt -w -s {}
	@go mod tidy

lint:
	@golangci-lint run ./...

test: $(APPS)
	@go test $(GO_FLAGS) -timeout 3m -race ./...

migrate:
	@go run ./cmd/volteras-oracle migrate

sqlboiler:
	@sqlboiler psql --no-tests --wipe

addmigration:
	@goose -dir internal/db/migrations create rename_me sql

clean:
	rm -rf $(PATHINSTBIN)
	rm -rf $(DEST_DIR)/dist
	rm -rf $(PATHINSTDOCKER)
	rm -rf ./vendor
