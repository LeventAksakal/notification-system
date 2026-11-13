-- Desired state. Edit this, then `task migrate:diff -- <name>`; never hand-write migrations/.
-- app_meta is an example table: key/value pairs the service exposes at GET /v1/meta/{key}.
CREATE TABLE app_meta (
  k VARCHAR(64) NOT NULL,
  v VARCHAR(255) NOT NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (k),
  -- Rules on every layer: the same limit as the API spec's maxLength (64).
  CONSTRAINT k_not_blank CHECK (CHAR_LENGTH(TRIM(k)) > 0)
);
