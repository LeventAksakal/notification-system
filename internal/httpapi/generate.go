// Package httpapi serves the HTTP contract. The contract is this package's
// code (decision 0024): the types in contract.go and the operations in api.go.
// Huma validates requests against them and derives api/openapi.yaml from
// them; clients are generated from that file.
package httpapi

//go:generate go run ./specgen -o ../../api/openapi.yaml
