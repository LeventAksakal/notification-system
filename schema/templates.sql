-- Message templates with {{variable}} placeholders (decision 0015).
CREATE TABLE templates (
  name VARCHAR(64) CHARACTER SET ascii NOT NULL,
  body MEDIUMTEXT NOT NULL,
  -- Optimistic concurrency (decision 0015): 1 when created, +1 on every update,
  -- written only by the triggers below. INT UNSIGNED holds 4,294,967,295
  -- versions, more updates than one template will ever see.
  version INT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (name),
  CONSTRAINT template_name_shape CHECK (REGEXP_LIKE(name, '^[a-z0-9][a-z0-9_-]{0,63}$')),
  CONSTRAINT template_body_length CHECK (CHAR_LENGTH(body) BETWEEN 1 AND 100000),
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
