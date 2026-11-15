// Package apiclient is a typed HTTP client of the service, generated from the
// committed contract (api/openapi.yaml) the way any consumer would generate
// one (decision 0024). The acceptance tests speak to the running binary
// through it. Never edit client.gen.go; run `go generate ./...`.
package apiclient

//go:generate go tool -modfile=../../tools.mod oapi-codegen -config oapi-codegen.yaml ../../api/openapi.yaml
