.PHONY: build clean run

BINARY_NAME=nwdaf
BUILD_DIR=bin

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/

run: build
	./$(BUILD_DIR)/$(BINARY_NAME) --config config/nwdafcfg.yaml

clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)

deps:
	go mod tidy
	go mod download

test:
	go test -v ./...

.DEFAULT_GOAL := build
