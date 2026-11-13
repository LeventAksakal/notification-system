// Package migrations embeds the goose files Atlas generates from schema/, so
// the binary migrates itself (`app migrate`) with no extra tool at runtime.
package migrations

import "embed"

// FS holds every migration file.
//
//go:embed *.sql
var FS embed.FS
