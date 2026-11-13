-- +goose Up
-- create "batches" table
CREATE TABLE `batches` (`id` char(36) CHARSET ascii NOT NULL COLLATE ascii_general_ci, `total` int unsigned NOT NULL, `correlation_id` varchar(128) NOT NULL, `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), PRIMARY KEY (`id`), CONSTRAINT `batch_size` CHECK (`total` between 1 and 1000)) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
-- create "idempotency_keys" table
CREATE TABLE `idempotency_keys` (`idem_key` varchar(255) CHARSET ascii NOT NULL COLLATE ascii_bin, `request_hash` binary(32) NOT NULL, `resource_kind` enum('notification','batch') NOT NULL, `resource_id` char(36) CHARSET ascii NOT NULL COLLATE ascii_general_ci, `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), PRIMARY KEY (`idem_key`), INDEX `idempotency_created_idx` (`created_at`)) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
-- create "templates" table
CREATE TABLE `templates` (`name` varchar(64) CHARSET ascii NOT NULL COLLATE ascii_general_ci, `body` mediumtext NOT NULL, `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3), PRIMARY KEY (`name`), CONSTRAINT `template_body_length` CHECK (char_length(`body`) between 1 and 100000), CONSTRAINT `template_name_shape` CHECK (regexp_like(`name`,_utf8mb4'^[a-z0-9][a-z0-9_-]{0,63}$'))) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
-- create "notifications" table
CREATE TABLE `notifications` (`id` char(36) CHARSET ascii NOT NULL COLLATE ascii_general_ci, `batch_id` char(36) CHARSET ascii NULL COLLATE ascii_general_ci, `batch_index` int unsigned NULL, `recipient` varchar(255) NOT NULL, `channel` enum('sms','email','push') NOT NULL, `priority` enum('high','normal','low') NOT NULL DEFAULT "normal", `content` mediumtext NOT NULL, `status` enum('pending','processing','sent','failed','cancelled') NOT NULL DEFAULT "pending", `failure_kind` enum('permanent','exhausted') NULL, `attempts` int unsigned NOT NULL DEFAULT 0, `due_at` datetime(3) NOT NULL, `scheduled_at` datetime(3) NULL, `lease_until` datetime(3) NULL, `claim_version` bigint unsigned NOT NULL DEFAULT 0, `provider_message_id` varchar(255) NULL, `last_error` varchar(1024) NULL, `correlation_id` varchar(128) NOT NULL, `traceparent` varchar(64) NULL, `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3), `finished_at` datetime(3) NULL, PRIMARY KEY (`id`), INDEX `batch_idx` (`batch_id`, `batch_index`), INDEX `claim_idx` (`status`, `channel`, `priority`, `due_at`), INDEX `finished_idx` (`finished_at`, `status`, `channel`), INDEX `lease_idx` (`status`, `lease_until`), INDEX `list_channel_idx` (`channel`, `id`), CONSTRAINT `notifications_batch_fk` FOREIGN KEY (`batch_id`) REFERENCES `batches` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, CONSTRAINT `content_length` CHECK (((`channel` = _utf8mb4'sms') and (char_length(`content`) between 1 and 1600)) or ((`channel` = _utf8mb4'email') and (char_length(`content`) between 1 and 100000)) or ((`channel` = _utf8mb4'push') and (char_length(`content`) between 1 and 1024))), CONSTRAINT `failure_kind_only_when_failed` CHECK ((`status` = _utf8mb4'failed') = (`failure_kind` is not null)), CONSTRAINT `recipient_email_length` CHECK ((`channel` <> _utf8mb4'email') or (char_length(`recipient`) <= 254)), CONSTRAINT `recipient_not_blank` CHECK (char_length(`recipient`) between 1 and 255), CONSTRAINT `recipient_sms_e164` CHECK ((`channel` <> _utf8mb4'sms') or regexp_like(`recipient`,_utf8mb4'^\\+[0-9]{7,15}$'))) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
-- create "delivery_attempts" table
CREATE TABLE `delivery_attempts` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, `notification_id` char(36) CHARSET ascii NOT NULL COLLATE ascii_general_ci, `attempt_no` int unsigned NOT NULL, `channel` enum('sms','email','push') NOT NULL, `started_at` datetime(3) NOT NULL, `duration_ms` int unsigned NOT NULL, `outcome` enum('accepted','transient','permanent') NOT NULL, `http_status` smallint unsigned NULL, `error` varchar(1024) NULL, `provider_message_id` varchar(255) NULL, PRIMARY KEY (`id`), INDEX `attempts_notification_idx` (`notification_id`, `id`), INDEX `attempts_window_idx` (`started_at`, `channel`, `outcome`), CONSTRAINT `attempts_notification_fk` FOREIGN KEY (`notification_id`) REFERENCES `notifications` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
-- drop "app_meta" table
DROP TABLE `app_meta`;

-- +goose Down
-- reverse: drop "app_meta" table
CREATE TABLE `app_meta` (`k` varchar(64) NOT NULL, `v` varchar(255) NOT NULL, `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3), PRIMARY KEY (`k`), CONSTRAINT `k_not_blank` CHECK (char_length(trim(`k`)) > 0)) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
-- reverse: create "delivery_attempts" table
DROP TABLE `delivery_attempts`;
-- reverse: create "notifications" table
DROP TABLE `notifications`;
-- reverse: create "templates" table
DROP TABLE `templates`;
-- reverse: create "idempotency_keys" table
DROP TABLE `idempotency_keys`;
-- reverse: create "batches" table
DROP TABLE `batches`;
