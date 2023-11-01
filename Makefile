
export GOLANGCI_LINT_VERSION := v1.54.2
export GOLANGCI_LINT := bin/golangci-lint_$(GOLANGCI_LINT_VERSION)/golangci-lint
export GOLANGCI_LINT_URL := https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh

all: help

help: Makefile
	@echo
	@echo "📗 Choose a command run in "${REPO_NAME}":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo

## clean: Clean up all build artifacts
.PHONY: clean
clean:
	@echo "🚀 Cleaning up old artifacts"
	@rm -rf ./bin


## lint: Lint the source code
.PHONY: lint
lint: ${GOLANGCI_LINT}
	@echo "🚀 Linting code"
	@gofmt -w -s .
	@$(GOLANGCI_LINT) run

## test: Runs all tests
.PHONY: test
test:
	@echo "🚀 Running tests"
	@echo "🚀 Running tests for pkg"
	@go test -race -cover -count=1 ./pkg/...

## build: Build the application artifacts. Linting can be skipped by setting env variable IGNORE_LINTING.
.PHONY: build
build: test
	@echo "🚀 Building the library"
	@go build ./pkg/...


## lint-info: Returns information about the current linter being used
lint-info:
	@echo ${GOLANGCI_LINT}

${GOLANGCI_LINT}:
	@echo "📦 Installing golangci-lint ${GOLANGCI_LINT_VERSION}"
	@rm -rf ./bin/golangci-lint_*
	@mkdir -p $(dir ${GOLANGCI_LINT})
	@curl -sSfL ${GOLANGCI_LINT_URL} | sh -s -- -b ./$(patsubst %/,%,$(dir ${GOLANGCI_LINT})) ${GOLANGCI_LINT_VERSION} > /dev/null 2>&1