-- +goose Up
ALTER TABLE `notifications` ADD CONSTRAINT `scheduled_within_horizon` CHECK (((scheduled_at is null) or (scheduled_at <= (updated_at + interval 30 day))));

-- +goose Down
ALTER TABLE `notifications` DROP CONSTRAINT `scheduled_within_horizon`;
