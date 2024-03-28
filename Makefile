.PHONY: build test test-integration generate install-paramgen install-tools golangci-lint-install

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/lovromazgon/conduit-connector-kafka-broker.version=${VERSION}'" -o conduit-connector-kafka-broker cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

generate:
	go generate ./...

install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -I % go list -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy

lint:
	golangci-lint run -v
