# General variables
IGNORE_LINTING ?= true
OS := $(shell uname | tr '[:upper:]' '[:lower:]')
ARCHITECTURE := $(shell uname -m)
ARCHITECTURE_SUBSTITUTED := $(shell uname -m | sed "s/x86_64/amd64/g")

#Define PROPERTY_FILE 
export CURRENT_DIR := $(shell pwd)

# ===============================================
# Define variables for Git and Versioning
# ===============================================
BRANCH_NAME := $(shell git rev-parse --abbrev-ref HEAD)
CREATED := $(shell date +%Y-%m-%dT%T%z)
GIT_REPO := $(shell git config --get remote.origin.url)
GIT_TOKEN ?= $(shell cat git-token.txt)
REPO_NAME := $(shell basename ${GIT_REPO} .git)
REVISION_ID := $(shell git rev-parse HEAD)
SHORT_SHA := $(shell git rev-parse --short HEAD)
TAG_NAME ?= $(shell git describe --exact-match --tags 2> /dev/null)
VERSION ?= $(if ${TAG_NAME},${TAG_NAME},latest)
VERSION_PATH := github.com/ingka-group-digital/${REPO_NAME}/internal/version



# ===============================================================
# Define Variables for Makefile tools versions and dependencies
# ===============================================================
GOIMPORTS := $(shell which goimports)

GOLANGCI_LINT_VERSION := 1.55.2
GOLANGCI_LINT := bin/golangci-lint_v$(GOLANGCI_LINT_VERSION)/golangci-lint
GOLINTCI_LINT_URL := https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh

GOTESTSUM_VERSION := 1.11.0
GOTESTSUM := bin/gotestsum_v$(GOTESTSUM_VERSION)/gotestsum
GOTESTSUM_URL := https://github.com/gotestyourself/gotestsum/releases/download/v$(GOTESTSUM_VERSION)/gotestsum_$(GOTESTSUM_VERSION)_$(OS)_$(ARCHITECTURE_SUBSTITUTED).tar.gz

all: help

## clean: Clean up all build artifacts
.PHONY: clean
clean:
	@echo "🚀 Cleaning up old artifacts MAIN"

## test: runs go tests
.PHONY: test
test: ${GOTESTSUM}
	@echo "🚀 running tests"
	@bash -c 'set -o pipefail; CGO_ENABLED=1 ${GOTESTSUM} --format testname --no-color=false -- -race ./pkg/... | grep -v "EMPTY"; exit $$?'
	@#go test -cover -count=1 ./internal/...

## test-coverage: creates a test coverage report in HTML format
.PHONY: test-coverage
test-coverage:
	@echo "🚀 creating coverage report in HTML format"
	@go test -coverprofile=coverage.out ./pkg/...
	@go tool cover -html=coverage.out

# Format target
.PHONY: go-format
go-format: ${GOIMPORTS}
	@echo  "🚀 running go fmt"
	@go fmt ./...
	@echo "🚀 Formatting code with goimports"
	@${GOIMPORTS} -w .

## build: Build the application artifacts. Linting can be skipped by setting env variable IGNORE_LINTING.
.PHONY: build
build:
ifeq ("$(IGNORE_LINTING)","false")
	@make lint
endif
	@go mod download
	@go mod tidy
	@echo "🚀 Building artifacts"
	@go build ./pkg/...


## install-hooks: Install Git hooks
.PHONY: install-hooks
install-hooks:
	@echo "🚀 Installing Git hooks"
	@cp hooks/pre-push .git/hooks/pre-push

## uninstall-hooks: Uninstall Git hooks
.PHONY: uninstall-hooks
uninstall-hooks:
	@echo "🚀 Uninstalling Git hooks"
	@rm -f .git/hooks/pre-push


## makefile-check: downloads all binaries if not already present
.PHONY: makefile-check
makefile-check: ${GOTESTSUM} ${GOIMPORTS}

help: Makefile
	@echo
	@echo "📗 Choose a command run in "${REPO_NAME}":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo



# ################################ #
# targets to download the binaries #
# ################################ #

# Ensure gotestsum is installed
${GOTESTSUM}:
	@echo "📦 installing gotestsum v${GOTESTSUM_VERSION}"
	@mkdir -p $(dir ${GOTESTSUM})
	@curl -sSL ${GOTESTSUM_URL} > bin/gotestsum.tar.gz
	@tar -xzf bin/gotestsum.tar.gz -C $(patsubst %/,%,$(dir ${GOTESTSUM}))
	@rm -f bin/gotestsum.tar.gz

# Ensure goimports is installed
${GOIMPORTS}:
	@echo "Installing goimports..."
	@go install golang.org/x/tools/cmd/goimports@latest