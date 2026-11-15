-- +goose Up
ALTER TABLE `notifications` ADD COLUMN `budget_spent` int UNSIGNED NOT NULL DEFAULT 0 AFTER `attempts`;
ALTER TABLE `notifications` ADD CONSTRAINT `budget_within_attempts` CHECK ((budget_spent <= attempts));

-- +goose Down
ALTER TABLE `notifications` DROP CONSTRAINT `budget_within_attempts`;
ALTER TABLE `notifications` DROP COLUMN `budget_spent`;
