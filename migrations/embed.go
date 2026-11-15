// Package migrations embeds the goose files sqldef generates from schema/ (task migrate:diff), so
// the binary migrates itself (`app migrate`) with no extra tool at runtime.
package migrations

import "embed"

// FS holds every migration file.
//
//go:embed *.sql
var FS embed.FS
