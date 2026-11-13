-- name: GetMeta :one
SELECT k, v, updated_at FROM app_meta WHERE k = ?;

-- name: UpsertMeta :exec
-- sqlc's MySQL parser rejects the 8.0.19 row alias (AS new), so the value is bound twice.
INSERT INTO app_meta (k, v) VALUES (sqlc.arg(k), sqlc.arg(v)) ON DUPLICATE KEY UPDATE v = sqlc.arg(v);
