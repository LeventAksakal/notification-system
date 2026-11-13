-- +goose Up
-- modify "notifications" table
ALTER TABLE `notifications` ADD INDEX `list_status_idx` (`status`, `id`);

-- +goose Down
-- reverse: modify "notifications" table
ALTER TABLE `notifications` DROP INDEX `list_status_idx`;
