-- Limits repeat internal/notification/limits.go; a test asserts they agree (decision 0017).
-- CHECKs that compare a column with a string are written as MySQL stores them
-- (SHOW CREATE TABLE); sqldef reports a false change for any other spelling.

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
  -- The state this row last moved from, written only by the triggers below:
  -- 'created' on insert, the old status on every status change. NULL only on
  -- rows that existed before the triggers (decision 0005, "As built").
  status_from ENUM('created', 'pending', 'processing', 'sent', 'failed', 'cancelled') NULL,
  -- Goes up by exactly one with every status change, in the statement that
  -- makes it; creation is 1. Live events carry it, so subscribers order them
  -- exactly and skip what a snapshot already shows (decision 0013).
  status_seq BIGINT UNSIGNED NOT NULL DEFAULT 1,
  failure_kind ENUM('permanent', 'exhausted') NULL,
  -- Provider calls made in the current attempt budget; reset by a replay.
  attempts INT UNSIGNED NOT NULL DEFAULT 0,
  -- Failed calls counted against the attempt budget (APP_MAX_ATTEMPTS): only
  -- those made while the channel's provider was believed healthy; a failure
  -- that belongs to a declared outage is recorded but not counted (decision
  -- 0026). Reset by a replay.
  budget_spent INT UNSIGNED NOT NULL DEFAULT 0,
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
  -- The template and version that rendered content (decision 0015); both NULL
  -- when content was sent as is. No foreign key: the template may be replaced
  -- or deleted later, and the notification keeps what produced it.
  template_name VARCHAR(64) CHARACTER SET ascii NULL,
  template_version INT UNSIGNED NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  -- Set on reaching sent, failed or cancelled; cleared by a replay.
  finished_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  CONSTRAINT notifications_batch_fk FOREIGN KEY (batch_id) REFERENCES batches (id),
  CONSTRAINT recipient_not_blank CHECK (CHAR_LENGTH(recipient) BETWEEN 1 AND 255),
  CONSTRAINT recipient_email_length CHECK (((`channel` <> _utf8mb4'email') or (char_length(`recipient`) <= 254))),
  CONSTRAINT recipient_sms_e164 CHECK (((`channel` <> _utf8mb4'sms') or regexp_like(`recipient`,_utf8mb4'^\\+[0-9]{7,15}$'))),
  CONSTRAINT content_length CHECK ((((`channel` = _utf8mb4'sms') and (char_length(`content`) between 1 and 1600)) or ((`channel` = _utf8mb4'email') and (char_length(`content`) between 1 and 100000)) or ((`channel` = _utf8mb4'push') and (char_length(`content`) between 1 and 1024)))),
  CONSTRAINT failure_kind_only_when_failed CHECK ((status = 'failed') = (failure_kind IS NOT NULL)),
  CONSTRAINT budget_within_attempts CHECK ((`budget_spent` <= `attempts`)),
  CONSTRAINT status_seq_from_one CHECK ((`status_seq` >= 1)),
  -- The state machine (internal/notification/status.go, transitions): every
  -- (status_from, status) pair the triggers can produce must be an allowed
  -- edge; 'created' -> pending is the only way in. store_rules_integration_test.go
  -- asserts this list equals the Go map.
  CONSTRAINT status_transition CHECK (((`status_from` is null) or ((`status_from`,`status`) in ((_utf8mb4'created',_utf8mb4'pending'),(_utf8mb4'pending',_utf8mb4'processing'),(_utf8mb4'pending',_utf8mb4'cancelled'),(_utf8mb4'processing',_utf8mb4'sent'),(_utf8mb4'processing',_utf8mb4'pending'),(_utf8mb4'processing',_utf8mb4'failed'),(_utf8mb4'failed',_utf8mb4'pending'))))),
  CONSTRAINT finished_exactly_when_terminal CHECK (((`status` in (_utf8mb4'sent',_utf8mb4'failed',_utf8mb4'cancelled')) = (`finished_at` is not null))),
  CONSTRAINT lease_only_while_processing CHECK (((`status` = _utf8mb4'processing') = (`lease_until` is not null))),
  CONSTRAINT provider_id_only_when_sent CHECK (((`status` = _utf8mb4'sent') or (`provider_message_id` is null))),
  CONSTRAINT attempted_before_sent_or_failed CHECK (((`status` not in (_utf8mb4'sent',_utf8mb4'failed')) or (`attempts` >= 1))),
  CONSTRAINT batch_position_with_batch CHECK (((`batch_id` is null) = (`batch_index` is null))),
  -- The schedule horizon (notification.ScheduleHorizon, decision 0016) on the
  -- database clock: a CHECK cannot call NOW(), but updated_at is stamped with
  -- the statement's NOW(3) by every write that changes the row (the default
  -- on insert, ON UPDATE), so a row that passed once keeps passing while the
  -- server clock does not step back (decision 0016, "As built").
  CONSTRAINT scheduled_within_horizon CHECK (((`scheduled_at` is null) or (`scheduled_at` <= (`updated_at` + interval 30 day)))),
  CONSTRAINT template_origin_complete CHECK (((`template_name` is null) = (`template_version` is null))),
  CONSTRAINT template_origin_version_positive CHECK ((`template_version` >= 1)),
  -- The claim: WHERE status='pending' AND channel=? AND priority=? AND due_at<=NOW(3) ORDER BY due_at.
  KEY claim_idx (status, channel, priority, due_at),
  -- The reaper: WHERE status='processing' AND lease_until < NOW(3).
  KEY lease_idx (status, lease_until),
  -- Batch items in request order, one row per position.
  UNIQUE KEY batch_position_idx (batch_id, batch_index),
  -- Lists newest first, filtered by channel or by status.
  KEY list_channel_idx (channel, id),
  KEY list_status_idx (status, id),
  -- Metrics: what finished inside a window. Retention: WHERE finished_at < ? (decision 0025).
  KEY finished_idx (finished_at, status, channel)
);

-- Only the triggers write status_from, so status_transition sees every status
-- change, whoever writes it (sqldef cannot parse SIGNAL, decision 0023, so the
-- refusal is the CHECK, not the trigger). They also own status_seq: 1 on
-- insert, OLD + 1 on every status change, unchanged otherwise, so no query can
-- skip or double a step whatever it writes to the column (decision 0013).
CREATE TRIGGER notifications_status_from_insert BEFORE INSERT ON notifications FOR EACH ROW BEGIN
  SET NEW.status_from = 'created';
  SET NEW.status_seq = 1;
END;

CREATE TRIGGER notifications_status_from_update BEFORE UPDATE ON notifications FOR EACH ROW BEGIN
  IF NOT (NEW.status <=> OLD.status) THEN
    SET NEW.status_from = OLD.status;
    SET NEW.status_seq = OLD.status_seq + 1;
  ELSE
    SET NEW.status_from = OLD.status_from;
    SET NEW.status_seq = OLD.status_seq;
  END IF;
END;

-- One row per provider call (decision 0005); also feeds the metrics endpoint (0014).
-- Written in flight and committed before the request leaves, then updated
-- with the result, so a crash cannot lose a call from the history.
CREATE TABLE delivery_attempts (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  notification_id CHAR(36) CHARACTER SET ascii NOT NULL,
  -- 1-based within the attempt budget; restarts after a replay.
  attempt_no INT UNSIGNED NOT NULL,
  channel ENUM('sms', 'email', 'push') NOT NULL,
  started_at DATETIME(3) NOT NULL,
  -- 0 until a result is recorded.
  duration_ms INT UNSIGNED NOT NULL DEFAULT 0,
  outcome ENUM('accepted', 'transient', 'permanent', 'in_flight', 'unknown') NOT NULL,
  -- Where the outcome last moved from, written only by the triggers below
  -- ('created' on insert); NULL on rows older than the triggers.
  outcome_from ENUM('created', 'accepted', 'transient', 'permanent', 'in_flight', 'unknown') NULL,
  -- The claim that made the call (notifications.claim_version); NULL on rows
  -- older than attempts written before the call.
  claim_version BIGINT UNSIGNED NULL,
  -- NULL when no response arrived (timeout, connection error).
  http_status SMALLINT UNSIGNED NULL,
  error VARCHAR(1024) NULL,
  provider_message_id VARCHAR(255) NULL,
  PRIMARY KEY (id),
  CONSTRAINT attempt_no_from_one CHECK ((`attempt_no` >= 1)),
  -- The attempt's state machine (internal/notification/status.go, attemptTransitions).
  CONSTRAINT attempt_outcome_transition CHECK (((`outcome_from` is null) or ((`outcome_from`,`outcome`) in ((_utf8mb4'created',_utf8mb4'in_flight'),(_utf8mb4'in_flight',_utf8mb4'accepted'),(_utf8mb4'in_flight',_utf8mb4'transient'),(_utf8mb4'in_flight',_utf8mb4'permanent'),(_utf8mb4'in_flight',_utf8mb4'unknown'),(_utf8mb4'unknown',_utf8mb4'accepted'),(_utf8mb4'unknown',_utf8mb4'transient'),(_utf8mb4'unknown',_utf8mb4'permanent'))))),
  CONSTRAINT attempt_claim_known CHECK (((`outcome_from` is null) or (`claim_version` is not null))),
  CONSTRAINT attempt_no_result_yet CHECK (((`outcome` not in (_utf8mb4'in_flight',_utf8mb4'unknown')) or ((`http_status` is null) and (`duration_ms` = 0)))),
  CONSTRAINT attempt_provider_id_only_when_accepted CHECK (((`outcome` = _utf8mb4'accepted') or (`provider_message_id` is null))),
  CONSTRAINT attempt_accepted_is_2xx CHECK (((`outcome` <> _utf8mb4'accepted') or ((`http_status` is not null) and (`http_status` between 200 and 299)))),
  CONSTRAINT attempt_http_status_range CHECK ((`http_status` between 100 and 999)),
  CONSTRAINT attempt_error_exactly_when_failed_or_unknown CHECK (((`outcome` in (_utf8mb4'accepted',_utf8mb4'in_flight')) = (`error` is null))),
  CONSTRAINT attempts_notification_fk FOREIGN KEY (notification_id) REFERENCES notifications (id),
  -- A notification's history, oldest first.
  KEY attempts_notification_idx (notification_id, id),
  -- One provider call per claim; the result write finds its row here.
  UNIQUE KEY attempts_claim_idx (notification_id, claim_version),
  -- Metrics: attempts started inside a window.
  KEY attempts_window_idx (started_at, channel, outcome)
);

CREATE TRIGGER delivery_attempts_outcome_from_insert BEFORE INSERT ON delivery_attempts FOR EACH ROW BEGIN
  SET NEW.outcome_from = 'created';
END;

CREATE TRIGGER delivery_attempts_outcome_from_update BEFORE UPDATE ON delivery_attempts FOR EACH ROW BEGIN
  IF NOT (NEW.outcome <=> OLD.outcome) THEN
    SET NEW.outcome_from = OLD.outcome;
  ELSE
    SET NEW.outcome_from = OLD.outcome_from;
  END IF;
END;

