-- +goose Up
ALTER TABLE `batches` ADD KEY `batches_created_idx` (`created_at`);
ALTER TABLE `notifications` ADD COLUMN `status_from` enum('created', 'pending', 'processing', 'sent', 'failed', 'cancelled') NULL AFTER `status`;
ALTER TABLE `notifications` ADD UNIQUE KEY `batch_position_idx` (`batch_id`, `batch_index`);
ALTER TABLE `notifications` ADD CONSTRAINT `status_transition` CHECK (((status_from is null) or ((status_from, status) in (('created', 'pending'), ('pending', 'processing'), ('pending', 'cancelled'), ('processing', 'sent'), ('processing', 'pending'), ('processing', 'failed'), ('failed', 'pending')))));
ALTER TABLE `notifications` ADD CONSTRAINT `finished_exactly_when_terminal` CHECK (((status in ('sent', 'failed', 'cancelled')) = (finished_at is not null)));
ALTER TABLE `notifications` ADD CONSTRAINT `lease_only_while_processing` CHECK (((status = 'processing') = (lease_until is not null)));
ALTER TABLE `notifications` ADD CONSTRAINT `provider_id_only_when_sent` CHECK (((status = 'sent') or (provider_message_id is null)));
ALTER TABLE `notifications` ADD CONSTRAINT `attempted_before_sent_or_failed` CHECK (((status not in ('sent', 'failed')) or (attempts >= 1)));
ALTER TABLE `notifications` ADD CONSTRAINT `batch_position_with_batch` CHECK (((batch_id is null) = (batch_index is null)));
ALTER TABLE `delivery_attempts` CHANGE COLUMN `duration_ms` `duration_ms` int UNSIGNED NOT NULL DEFAULT 0;
ALTER TABLE `delivery_attempts` CHANGE COLUMN `outcome` `outcome` enum('accepted', 'transient', 'permanent', 'in_flight', 'unknown') NOT NULL;
ALTER TABLE `delivery_attempts` ADD COLUMN `outcome_from` enum('created', 'accepted', 'transient', 'permanent', 'in_flight', 'unknown') NULL AFTER `outcome`;
ALTER TABLE `delivery_attempts` ADD COLUMN `claim_version` bigint UNSIGNED NULL AFTER `outcome_from`;
ALTER TABLE `delivery_attempts` ADD UNIQUE KEY `attempts_claim_idx` (`notification_id`, `claim_version`);
ALTER TABLE `delivery_attempts` ADD CONSTRAINT `attempt_no_from_one` CHECK ((attempt_no >= 1));
ALTER TABLE `delivery_attempts` ADD CONSTRAINT `attempt_outcome_transition` CHECK (((outcome_from is null) or ((outcome_from, outcome) in (('created', 'in_flight'), ('in_flight', 'accepted'), ('in_flight', 'transient'), ('in_flight', 'permanent'), ('in_flight', 'unknown'), ('unknown', 'accepted'), ('unknown', 'transient'), ('unknown', 'permanent')))));
ALTER TABLE `delivery_attempts` ADD CONSTRAINT `attempt_claim_known` CHECK (((outcome_from is null) or (claim_version is not null)));
ALTER TABLE `delivery_attempts` ADD CONSTRAINT `attempt_no_result_yet` CHECK (((outcome not in ('in_flight', 'unknown')) or ((http_status is null) and (duration_ms = 0))));
ALTER TABLE `delivery_attempts` ADD CONSTRAINT `attempt_provider_id_only_when_accepted` CHECK (((outcome = 'accepted') or (provider_message_id is null)));
ALTER TABLE `delivery_attempts` ADD CONSTRAINT `attempt_accepted_is_2xx` CHECK (((outcome != 'accepted') or ((http_status is not null) and (http_status between 200 and 299))));
ALTER TABLE `delivery_attempts` ADD CONSTRAINT `attempt_http_status_range` CHECK ((http_status between 100 and 999));
ALTER TABLE `delivery_attempts` ADD CONSTRAINT `attempt_error_exactly_when_failed_or_unknown` CHECK (((outcome in ('accepted', 'in_flight')) = (error is null)));
-- +goose StatementBegin
CREATE TRIGGER `notifications_status_from_insert` before insert ON `notifications` FOR EACH ROW begin
set NEW.status_from = 'created';
end;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER `notifications_status_from_update` before update ON `notifications` FOR EACH ROW begin
if not (NEW.status <=> OLD.status) then
set NEW.status_from = OLD.status;
else
set NEW.status_from = OLD.status_from;
end if;
end;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER `delivery_attempts_outcome_from_insert` before insert ON `delivery_attempts` FOR EACH ROW begin
set NEW.outcome_from = 'created';
end;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER `delivery_attempts_outcome_from_update` before update ON `delivery_attempts` FOR EACH ROW begin
if not (NEW.outcome <=> OLD.outcome) then
set NEW.outcome_from = OLD.outcome;
else
set NEW.outcome_from = OLD.outcome_from;
end if;
end;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER IF EXISTS `delivery_attempts_outcome_from_insert`;
DROP TRIGGER IF EXISTS `delivery_attempts_outcome_from_update`;
DROP TRIGGER IF EXISTS `notifications_status_from_insert`;
DROP TRIGGER IF EXISTS `notifications_status_from_update`;
ALTER TABLE `delivery_attempts` CHANGE COLUMN `duration_ms` `duration_ms` int UNSIGNED NOT NULL;
ALTER TABLE `delivery_attempts` CHANGE COLUMN `outcome` `outcome` enum('accepted', 'transient', 'permanent') NOT NULL;
ALTER TABLE `batches` DROP INDEX `batches_created_idx`;
ALTER TABLE `notifications` DROP INDEX `batch_position_idx`;
ALTER TABLE `notifications` DROP CONSTRAINT `attempted_before_sent_or_failed`;
ALTER TABLE `notifications` DROP CONSTRAINT `batch_position_with_batch`;
ALTER TABLE `notifications` DROP CONSTRAINT `finished_exactly_when_terminal`;
ALTER TABLE `notifications` DROP CONSTRAINT `lease_only_while_processing`;
ALTER TABLE `notifications` DROP CONSTRAINT `provider_id_only_when_sent`;
ALTER TABLE `notifications` DROP CONSTRAINT `status_transition`;
ALTER TABLE `notifications` DROP COLUMN `status_from`;
ALTER TABLE `delivery_attempts` DROP INDEX `attempts_claim_idx`;
ALTER TABLE `delivery_attempts` DROP CONSTRAINT `attempt_accepted_is_2xx`;
ALTER TABLE `delivery_attempts` DROP CONSTRAINT `attempt_claim_known`;
ALTER TABLE `delivery_attempts` DROP CONSTRAINT `attempt_error_exactly_when_failed_or_unknown`;
ALTER TABLE `delivery_attempts` DROP CONSTRAINT `attempt_http_status_range`;
ALTER TABLE `delivery_attempts` DROP CONSTRAINT `attempt_no_from_one`;
ALTER TABLE `delivery_attempts` DROP CONSTRAINT `attempt_no_result_yet`;
ALTER TABLE `delivery_attempts` DROP CONSTRAINT `attempt_outcome_transition`;
ALTER TABLE `delivery_attempts` DROP CONSTRAINT `attempt_provider_id_only_when_accepted`;
ALTER TABLE `delivery_attempts` DROP COLUMN `claim_version`;
ALTER TABLE `delivery_attempts` DROP COLUMN `outcome_from`;
