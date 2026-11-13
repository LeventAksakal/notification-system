// Package api serves the HTTP contract in api/openapi.yaml. The server
// interface and types are generated (api.gen.go); handlers.go implements them.
package api

//go:generate go tool -modfile=../../tools.mod oapi-codegen -config oapi-codegen.yaml ../../api/openapi.yaml
