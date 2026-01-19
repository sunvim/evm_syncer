.PHONY: build test lint docker run clean help

# Variables
BINARY_NAME=evm_syncer
DOCKER_IMAGE=evm_syncer:latest
MAIN_PATH=./cmd/syncer

help: ## Display this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

build: ## Build the binary
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p bin
	go build -o bin/$(BINARY_NAME) $(MAIN_PATH)
	@echo "Build complete: bin/$(BINARY_NAME)"

test: ## Run tests
	@echo "Running tests..."
	go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...
	@echo "Tests complete"

test-coverage: test ## Run tests with coverage report
	@echo "Generating coverage report..."
	go tool cover -html=coverage.txt -o coverage.html
	@echo "Coverage report: coverage.html"

lint: ## Run linter (requires golangci-lint)
	@echo "Running linter..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not found, install it from https://golangci-lint.run/usage/install/" && exit 1)
	golangci-lint run --timeout=5m ./...
	@echo "Linting complete"

docker: ## Build Docker image
	@echo "Building Docker image: $(DOCKER_IMAGE)"
	docker build -t $(DOCKER_IMAGE) -f deployments/docker/Dockerfile .
	@echo "Docker image built: $(DOCKER_IMAGE)"

run: build ## Run the application locally
	@echo "Running $(BINARY_NAME)..."
	./bin/$(BINARY_NAME)

clean: ## Clean build artifacts
	@echo "Cleaning..."
	rm -rf bin/
	rm -rf tmp/
	rm -f coverage.txt coverage.html
	@echo "Clean complete"

deps: ## Download dependencies
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy
	@echo "Dependencies updated"

.DEFAULT_GOAL := help
