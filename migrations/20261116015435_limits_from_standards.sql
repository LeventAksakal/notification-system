-- +goose Up
ALTER TABLE `notifications` CHANGE COLUMN `template_name` `template_name` varchar(63) CHARACTER SET ascii NULL;
ALTER TABLE `notifications` ADD CONSTRAINT `recipient_not_whitespace` CHECK (regexp_like(recipient, '[^\\t\\n\\x{0B}\\f\\r \\x{85}\\x{A0}\\x{1680}\\x{2000}-\\x{200A}\\x{2028}\\x{2029}\\x{202F}\\x{205F}\\x{3000}]'));
ALTER TABLE `notifications` DROP CONSTRAINT `recipient_email_length`;
ALTER TABLE `notifications` ADD CONSTRAINT `recipient_email_length` CHECK (((channel != 'email') or (length(recipient) <= 254)));
ALTER TABLE `notifications` DROP CONSTRAINT `recipient_sms_e164`;
ALTER TABLE `notifications` ADD CONSTRAINT `recipient_sms_e164` CHECK (((channel != 'sms') or regexp_like(recipient, '^\\+[1-9][0-9]{1,14}$')));
ALTER TABLE `notifications` DROP CONSTRAINT `content_length`;
ALTER TABLE `notifications` ADD CONSTRAINT `content_length` CHECK ((((channel = 'sms') and (char_length(content) between 1 and 1600)) or ((channel = 'email') and (length(content) between 1 and 32768)) or ((channel = 'push') and (length(content) between 1 and 4067))));
ALTER TABLE `notifications` ADD CONSTRAINT `content_not_blank` CHECK (regexp_like(content, '[^\\t\\n\\x{0B}\\f\\r \\x{85}\\x{A0}\\x{1680}\\x{2000}-\\x{200A}\\x{2028}\\x{2029}\\x{202F}\\x{205F}\\x{3000}]'));
ALTER TABLE `templates` CHANGE COLUMN `name` `name` varchar(63) CHARACTER SET ascii NOT NULL;
ALTER TABLE `templates` DROP CONSTRAINT `template_name_shape`;
ALTER TABLE `templates` ADD CONSTRAINT `template_name_shape` CHECK (((char_length(name) between 1 and 63) and regexp_like(name, '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$', 'c')));
ALTER TABLE `templates` DROP CONSTRAINT `template_body_length`;
ALTER TABLE `templates` ADD CONSTRAINT `template_body_length` CHECK ((length(body) between 1 and 32768));
ALTER TABLE `templates` ADD CONSTRAINT `template_body_not_blank` CHECK (regexp_like(body, '[^\\t\\n\\x{0B}\\f\\r \\x{85}\\x{A0}\\x{1680}\\x{2000}-\\x{200A}\\x{2028}\\x{2029}\\x{202F}\\x{205F}\\x{3000}]'));

-- +goose Down
ALTER TABLE `notifications` CHANGE COLUMN `template_name` `template_name` varchar(64) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT null;
ALTER TABLE `notifications` DROP CONSTRAINT `content_length`;
ALTER TABLE `notifications` ADD CONSTRAINT `content_length` CHECK ((((channel = 'sms') and (char_length(content) between 1 and 1600)) or ((channel = 'email') and (char_length(content) between 1 and 100000)) or ((channel = 'push') and (char_length(content) between 1 and 1024))));
ALTER TABLE `notifications` DROP CONSTRAINT `recipient_email_length`;
ALTER TABLE `notifications` ADD CONSTRAINT `recipient_email_length` CHECK (((channel != 'email') or (char_length(recipient) <= 254)));
ALTER TABLE `notifications` DROP CONSTRAINT `recipient_sms_e164`;
ALTER TABLE `notifications` ADD CONSTRAINT `recipient_sms_e164` CHECK (((channel != 'sms') or regexp_like(recipient, '^\\+[0-9]{7,15}$')));
ALTER TABLE `templates` CHANGE COLUMN `name` `name` varchar(64) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL;
ALTER TABLE `templates` DROP CONSTRAINT `template_body_length`;
ALTER TABLE `templates` ADD CONSTRAINT `template_body_length` CHECK ((char_length(body) between 1 and 100000));
ALTER TABLE `templates` DROP CONSTRAINT `template_name_shape`;
ALTER TABLE `templates` ADD CONSTRAINT `template_name_shape` CHECK (regexp_like(name, '^[a-z0-9][a-z0-9_-]{0,63}$'));
ALTER TABLE `notifications` DROP CONSTRAINT `content_not_blank`;
ALTER TABLE `notifications` DROP CONSTRAINT `recipient_not_whitespace`;
ALTER TABLE `templates` DROP CONSTRAINT `template_body_not_blank`;
