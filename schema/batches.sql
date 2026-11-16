-- One row per accepted batch request (decision 0011).
CREATE TABLE batches (
  id CHAR(36) CHARACTER SET ascii NOT NULL,
  total INT UNSIGNED NOT NULL,
  correlation_id VARCHAR(128) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  -- Retention: batches older than the retention whose notifications are all gone (decision 0025).
  KEY batches_created_idx (created_at),
  CONSTRAINT batch_size CHECK (total BETWEEN 1 AND 1000)
);
