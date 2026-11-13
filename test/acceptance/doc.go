// Package acceptance holds the black-box acceptance tests: one or more tests
// per row of docs/REQUIREMENTS.md, run against the real binary, MySQL, Valkey
// and the provider double. Every test file carries the build tag
// "integration"; this file has none, so untagged tooling (go vet ./...) still
// finds a package here.
package acceptance
