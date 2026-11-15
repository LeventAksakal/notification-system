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
