-- A create request stored once per Idempotency-Key (decision 0012). The primary key, not a
-- read-then-write check, decides the race between two identical concurrent requests.
CREATE TABLE idempotency_keys (
  idem_key VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  -- SHA-256 of the canonical request payload.
  request_hash BINARY(32) NOT NULL,
  resource_kind ENUM('notification', 'batch') NOT NULL,
  resource_id CHAR(36) CHARACTER SET ascii NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (idem_key),
  -- Expiry sweep: WHERE created_at < ?.
  KEY idempotency_created_idx (created_at)
);
