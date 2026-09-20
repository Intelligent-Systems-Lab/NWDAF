.PHONY: build clean run test lint lint-fix

BINARY_NAME=nwdaf
BUILD_DIR=bin

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/

run: build
	./$(BUILD_DIR)/$(BINARY_NAME) --config "$(CONFIG)"

clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)

deps:
	go mod tidy
	go mod download

test:
	go test -v ./...

lint:
	golangci-lint run ./...

lint-fix:
	golangci-lint run --fix ./...

.DEFAULT_GOAL := build
