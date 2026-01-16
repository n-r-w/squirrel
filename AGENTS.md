# Project Rules

## Tech Stack

- go 1.23.8
- github.com/georgysavva/scany/v2 for scanning database rows into structs
- github.com/stretchr/testify for testing
- golangci-lint-v2 for linting (`golangci-lint run --config .golangci.yml ./... `)
- github.com/n-r-w/testdock/v2 for integration tests (use `//go:build itest` tag)
- MUST run `go test --race -timeout 120s -tags itest ./...` before finalizing