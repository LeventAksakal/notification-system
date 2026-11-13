-- name: Ping :one
-- sqlc needs at least one query; the readiness probe can use it.
SELECT 1;
