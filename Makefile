BENCH_RUN ?= .

build:
	go build -mod=readonly ./...

# Run unit tests
test:
	env "GORACE=halt_on_error=1" go test -benchtime 1ns -race -bench . -timeout 5s -v ./...

# Run unit tests
test_coverage:
	go test -v -covermode=count -coverprofile=coverage.out ./...

upload_coverage: test_coverage
	goveralls -coverprofile coverage.out

# Format the code
fix:
	find . -iname '*.go' -not -path '*/vendor/*' -print0 | xargs -0 gofmt -s -w
	find . -iname '*.go' -not -path '*/vendor/*' -print0 | xargs -0 goimports -w

# Run benchmark examples
bench:
	go test -v -benchmem -run=^$$ -bench=$(BENCH_RUN) ./...

# Lint the code
lint:
	golangci-lint run

run:
	go run ./example/main.go

# ci installs dep by direct version.  Users install with 'go get'
setup_ci:
	GO111MODULE=on go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.17.1
	GO111MODULE=on go get github.com/mattn/goveralls@4d9899298d217719a8aea971675da567f0e3f96d
