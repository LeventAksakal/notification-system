-- +goose Up
ALTER TABLE `notifications` ADD COLUMN `template_name` varchar(64) CHARACTER SET ascii NULL AFTER `traceparent`;
ALTER TABLE `notifications` ADD COLUMN `template_version` int UNSIGNED NULL AFTER `template_name`;
ALTER TABLE `notifications` ADD CONSTRAINT `template_origin_complete` CHECK (((template_name is null) = (template_version is null)));
ALTER TABLE `notifications` ADD CONSTRAINT `template_origin_version_positive` CHECK ((template_version >= 1));
ALTER TABLE `templates` ADD COLUMN `version` int UNSIGNED NOT NULL DEFAULT 1 AFTER `body`;
ALTER TABLE `templates` ADD CONSTRAINT `template_version_positive` CHECK ((version >= 1));
-- +goose StatementBegin
CREATE TRIGGER `templates_version_insert` before insert ON `templates` FOR EACH ROW begin
set NEW.version = 1;
end;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER `templates_version_update` before update ON `templates` FOR EACH ROW begin
set NEW.version = OLD.version + 1;
end;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER IF EXISTS `templates_version_insert`;
DROP TRIGGER IF EXISTS `templates_version_update`;
ALTER TABLE `notifications` DROP CONSTRAINT `template_origin_complete`;
ALTER TABLE `notifications` DROP CONSTRAINT `template_origin_version_positive`;
ALTER TABLE `notifications` DROP COLUMN `template_version`;
ALTER TABLE `notifications` DROP COLUMN `template_name`;
ALTER TABLE `templates` DROP CONSTRAINT `template_version_positive`;
ALTER TABLE `templates` DROP COLUMN `version`;
