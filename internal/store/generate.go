// Package store owns SQL access. Queries are written as SQL in queries/ and
// compiled to typed Go in sqlc/ by `task gen`; nothing here is an ORM.
package store

//go:generate go tool -modfile=../../tools.mod sqlc generate -f ../../sqlc.yaml
