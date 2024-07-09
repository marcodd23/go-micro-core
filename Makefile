# General variables
export IGNORE_LINTING ?= true

#Define PROPERTY_FILE 
export CURRENT_DIR := $(shell pwd)

# Dependency versions
export GOMOCK_VERSION ?= v1.5.0
export SWAGGER_VERSION ?= v0.27.0
export GOWRAP_VERSION ?= v1.2.1
export GOLANGCI_LINT_VERSION := v1.54.2
export GOLANGCI_LINT := bin/golangci-lint_$(GOLANGCI_LINT_VERSION)/golangci-lint
export GOLINTCI_URL := https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh


# Versioning
export BRANCH_NAME := $(shell git rev-parse --abbrev-ref HEAD)
export CREATED := $(shell date +%Y-%m-%dT%T%z)
export GIT_REPO := $(shell git config --get remote.origin.url)
export GIT_TOKEN ?= $(shell cat git-token.txt)
export REPO_NAME := $(shell basename ${GIT_REPO} .git)
export REVISION_ID := $(shell git rev-parse HEAD)
export SHORT_SHA := $(shell git rev-parse --short HEAD)
export TAG_NAME ?= $(shell git describe --exact-match --tags 2> /dev/null)
export VERSION ?= $(if ${TAG_NAME},${TAG_NAME},latest)
export VERSION_PATH := github.com/ingka-group-digital/${REPO_NAME}/internal/version

all: help

## clean: Clean up all build artifacts
.PHONY: clean
clean:
	@echo "🚀 Cleaning up old artifacts MAIN"

## test: Runs all tests
.PHONY: test
test:
	@go mod download
	@go mod tidy
	@echo "🚀 Running tests"
	@go test -cover -count=1 ./pkg/...

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


## lint: Lint the source code
.PHONY: lint
lint: ${GOLANGCI_LINT}
	@echo "🚀 Linting code"
	@gofmt -w -s .
	@$(GOLANGCI_LINT) run


## lint-info: Returns information about the current linter being used
lint-info:
	@echo ${GOLANGCI_LINT}


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


${GOLANGCI_LINT}:
	@echo "📦 Installing golangci-lint ${GOLANGCI_LINT_VERSION}"
	@rm -rf ./bin/golangci-lint_*
	@mkdir -p $(dir ${GOLANGCI_LINT})
	@curl -sSfL ${GOLINTCI_URL} | sh -s -- -b ./$(patsubst %/,%,$(dir ${GOLANGCI_LINT})) ${GOLANGCI_LINT_VERSION} > /dev/null 2>&1


help: Makefile
	@echo
	@echo "📗 Choose a command run in "${REPO_NAME}":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo