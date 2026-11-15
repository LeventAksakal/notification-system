-- Desired state. Edit this, then `task migrate:diff -- <name>`; never hand-write migrations/.
-- Limits repeat internal/notification/limits.go; a test asserts they agree (decision 0017).

-- One row per accepted batch request (decision 0011).
CREATE TABLE batches (
  id CHAR(36) CHARACTER SET ascii NOT NULL,
  total INT UNSIGNED NOT NULL,
  correlation_id VARCHAR(128) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  CONSTRAINT batch_size CHECK (total BETWEEN 1 AND 1000)
);

-- The record and the queue (decision 0003). Ids are UUIDv7, so id order is creation order.
CREATE TABLE notifications (
  id CHAR(36) CHARACTER SET ascii NOT NULL,
  batch_id CHAR(36) CHARACTER SET ascii NULL,
  -- Position in the batch request; NULL outside a batch.
  batch_index INT UNSIGNED NULL,
  recipient VARCHAR(255) NOT NULL,
  channel ENUM('sms', 'email', 'push') NOT NULL,
  priority ENUM('high', 'normal', 'low') NOT NULL DEFAULT 'normal',
  -- Final content: templates are rendered at intake (decision 0015).
  content MEDIUMTEXT NOT NULL,
  status ENUM('pending', 'processing', 'sent', 'failed', 'cancelled') NOT NULL DEFAULT 'pending',
  failure_kind ENUM('permanent', 'exhausted') NULL,
  -- Provider calls made in the current attempt budget; reset by a replay.
  attempts INT UNSIGNED NOT NULL DEFAULT 0,
  -- Earliest claim time: the schedule or the next retry (decisions 0007, 0016).
  due_at DATETIME(3) NOT NULL,
  scheduled_at DATETIME(3) NULL,
  -- A claim is a lease; every write by the claimer carries claim_version (decision 0018).
  lease_until DATETIME(3) NULL,
  claim_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  provider_message_id VARCHAR(255) NULL,
  last_error VARCHAR(1024) NULL,
  correlation_id VARCHAR(128) NOT NULL,
  traceparent VARCHAR(64) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  -- Set on reaching sent, failed or cancelled; cleared by a replay.
  finished_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  CONSTRAINT notifications_batch_fk FOREIGN KEY (batch_id) REFERENCES batches (id),
  CONSTRAINT recipient_not_blank CHECK (CHAR_LENGTH(recipient) BETWEEN 1 AND 255),
  CONSTRAINT recipient_email_length CHECK (channel <> 'email' OR CHAR_LENGTH(recipient) <= 254),
  CONSTRAINT recipient_sms_e164 CHECK (channel <> 'sms' OR REGEXP_LIKE(recipient, '^\\+[0-9]{7,15}$')),
  CONSTRAINT content_length CHECK (
    (channel = 'sms' AND CHAR_LENGTH(content) BETWEEN 1 AND 1600)
    OR (channel = 'email' AND CHAR_LENGTH(content) BETWEEN 1 AND 100000)
    OR (channel = 'push' AND CHAR_LENGTH(content) BETWEEN 1 AND 1024)
  ),
  CONSTRAINT failure_kind_only_when_failed CHECK ((status = 'failed') = (failure_kind IS NOT NULL)),
  -- The claim: WHERE status='pending' AND channel=? AND priority=? AND due_at<=NOW(3) ORDER BY due_at.
  KEY claim_idx (status, channel, priority, due_at),
  -- The reaper: WHERE status='processing' AND lease_until < NOW(3).
  KEY lease_idx (status, lease_until),
  -- Batch items in request order.
  KEY batch_idx (batch_id, batch_index),
  -- Lists newest first, filtered by channel or by status.
  KEY list_channel_idx (channel, id),
  KEY list_status_idx (status, id),
  -- Metrics: what finished inside a window.
  KEY finished_idx (finished_at, status, channel)
);

-- One row per provider call (decision 0005); also feeds the metrics endpoint (0014).
CREATE TABLE delivery_attempts (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  notification_id CHAR(36) CHARACTER SET ascii NOT NULL,
  -- 1-based within the attempt budget; restarts after a replay.
  attempt_no INT UNSIGNED NOT NULL,
  channel ENUM('sms', 'email', 'push') NOT NULL,
  started_at DATETIME(3) NOT NULL,
  duration_ms INT UNSIGNED NOT NULL,
  outcome ENUM('accepted', 'transient', 'permanent') NOT NULL,
  -- NULL when no response arrived (timeout, connection error).
  http_status SMALLINT UNSIGNED NULL,
  error VARCHAR(1024) NULL,
  provider_message_id VARCHAR(255) NULL,
  PRIMARY KEY (id),
  CONSTRAINT attempts_notification_fk FOREIGN KEY (notification_id) REFERENCES notifications (id),
  -- A notification's history, oldest first.
  KEY attempts_notification_idx (notification_id, id),
  -- Metrics: attempts started inside a window.
  KEY attempts_window_idx (started_at, channel, outcome)
);

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

-- Message templates with {{variable}} placeholders (decision 0015).
CREATE TABLE templates (
  name VARCHAR(64) CHARACTER SET ascii NOT NULL,
  body MEDIUMTEXT NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (name),
  CONSTRAINT template_name_shape CHECK (REGEXP_LIKE(name, '^[a-z0-9][a-z0-9_-]{0,63}$')),
  CONSTRAINT template_body_length CHECK (CHAR_LENGTH(body) BETWEEN 1 AND 100000)
);
