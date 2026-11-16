-- Message templates with {{variable}} placeholders (decision 0015).
-- Limits repeat internal/notification/limits.go (decision 0017); notifications.sql explains the spelling.
CREATE TABLE templates (
  -- A DNS label (RFC 1123, as Kubernetes names use); 'c' matches case-sensitively despite the _ci collation.
  name VARCHAR(63) CHARACTER SET ascii NOT NULL,
  body MEDIUMTEXT NOT NULL,
  -- Optimistic concurrency (decision 0015): 1 when created, +1 on every update,
  -- written only by the triggers below. INT UNSIGNED holds 4,294,967,295
  -- versions, more updates than one template will ever see.
  version INT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (name),
  CONSTRAINT template_name_shape CHECK (((char_length(`name`) between 1 and 63) and regexp_like(`name`,_utf8mb4'^[a-z0-9]([-a-z0-9]*[a-z0-9])?$',_utf8mb4'c'))),
  CONSTRAINT template_body_length CHECK ((length(`body`) between 1 and 32768)),
  CONSTRAINT template_body_not_blank CHECK (regexp_like(`body`,_utf8mb4'[^\\t\\n\\x{0B}\\f\\r \\x{85}\\x{A0}\\x{1680}\\x{2000}-\\x{200A}\\x{2028}\\x{2029}\\x{202F}\\x{205F}\\x{3000}]')),
  CONSTRAINT template_version_positive CHECK ((`version` >= 1))
);

-- Only the triggers write version, so no writer can skip, reuse or reset one
-- (decision 0015, "As built"); the API's conditional UPDATE ... WHERE version = ?
-- relies on it. The CHECK above still holds if the triggers are ever dropped.
CREATE TRIGGER templates_version_insert BEFORE INSERT ON templates FOR EACH ROW BEGIN
  SET NEW.version = 1;
END;

CREATE TRIGGER templates_version_update BEFORE UPDATE ON templates FOR EACH ROW BEGIN
  SET NEW.version = OLD.version + 1;
END;
