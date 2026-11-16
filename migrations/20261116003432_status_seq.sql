-- +goose Up
ALTER TABLE `notifications` ADD COLUMN `status_seq` bigint UNSIGNED NOT NULL DEFAULT 1 AFTER `status_from`;
ALTER TABLE `notifications` ADD CONSTRAINT `status_seq_from_one` CHECK ((status_seq >= 1));
DROP TRIGGER `notifications_status_from_insert`;
-- +goose StatementBegin
CREATE TRIGGER `notifications_status_from_insert` before insert ON `notifications` FOR EACH ROW begin
set NEW.status_from = 'created';
set NEW.status_seq = 1;
end;
-- +goose StatementEnd
DROP TRIGGER `notifications_status_from_update`;
-- +goose StatementBegin
CREATE TRIGGER `notifications_status_from_update` before update ON `notifications` FOR EACH ROW begin
if not (NEW.status <=> OLD.status) then
set NEW.status_from = OLD.status;
set NEW.status_seq = OLD.status_seq + 1;
else
set NEW.status_from = OLD.status_from;
set NEW.status_seq = OLD.status_seq;
end if;
end;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER `notifications_status_from_insert`;
-- +goose StatementBegin
CREATE TRIGGER `notifications_status_from_insert` before insert ON `notifications` FOR EACH ROW begin
set NEW.status_from = 'created';
end;
-- +goose StatementEnd
DROP TRIGGER `notifications_status_from_update`;
-- +goose StatementBegin
CREATE TRIGGER `notifications_status_from_update` before update ON `notifications` FOR EACH ROW begin
if not (NEW.status <=> OLD.status) then
set NEW.status_from = OLD.status;
else
set NEW.status_from = OLD.status_from;
end if;
end;
-- +goose StatementEnd
ALTER TABLE `notifications` DROP CONSTRAINT `status_seq_from_one`;
ALTER TABLE `notifications` DROP COLUMN `status_seq`;
