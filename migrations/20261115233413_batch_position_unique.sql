-- +goose Up
ALTER TABLE `notifications` DROP INDEX `batch_idx`;

-- +goose Down
ALTER TABLE `notifications` ADD KEY `batch_idx` (`batch_id`, `batch_index`);
