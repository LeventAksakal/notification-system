-- +goose Up
CREATE TABLE attempts_per_minute (
  minute DATETIME NOT NULL,
  channel ENUM('sms', 'email', 'push') NOT NULL,
  outcome ENUM('accepted', 'transient', 'permanent', 'unknown') NOT NULL,
  shard TINYINT UNSIGNED NOT NULL,
  n INT NOT NULL,
  PRIMARY KEY (minute, channel, outcome, shard),
  CONSTRAINT attempts_per_minute_whole_minute CHECK ((second(`minute`) = 0)),
  CONSTRAINT attempts_per_minute_shard CHECK ((`shard` < 4)),
  CONSTRAINT attempts_per_minute_count CHECK ((`n` >= 0))
);
CREATE TABLE finishes_per_minute (
  minute DATETIME NOT NULL,
  channel ENUM('sms', 'email', 'push') NOT NULL,
  status ENUM('sent', 'failed') NOT NULL,
  latency_le_ms BIGINT UNSIGNED NOT NULL,
  shard TINYINT UNSIGNED NOT NULL,
  n INT NOT NULL,
  PRIMARY KEY (minute, channel, status, latency_le_ms, shard),
  CONSTRAINT finishes_per_minute_whole_minute CHECK ((second(`minute`) = 0)),
  CONSTRAINT finishes_per_minute_shard CHECK ((`shard` < 4)),
  CONSTRAINT finishes_per_minute_count CHECK ((`n` >= 0)),
  CONSTRAINT finishes_per_minute_latency_only_when_sent CHECK (((`status` = _utf8mb4'sent') or (`latency_le_ms` = 0)))
);
ALTER TABLE `notifications` DROP INDEX `claim_idx`;
ALTER TABLE `notifications` ADD KEY `claim_idx` (`status`, `channel`, `priority`, `due_at`, `attempts`);
ALTER TABLE `notifications` DROP INDEX `finished_idx`;
ALTER TABLE `notifications` ADD KEY `finished_idx` (`finished_at`, `status`, `channel`, `created_at`, `scheduled_at`);
-- +goose StatementBegin
CREATE TRIGGER `notifications_finish_rollup_insert` after insert ON `notifications` FOR EACH ROW begin
declare
ms bigint;
if NEW.status in ('sent', 'failed') and NEW.finished_at is not null then
set ms = cast(CEIL(TIMESTAMPDIFF(MICROSECOND, GREATEST(COALESCE(NEW.scheduled_at, NEW.created_at), NEW.created_at), NEW.finished_at) / 1000) as signed);
insert into finishes_per_minute(minute, channel, status, latency_le_ms, shard, n) values (NEW.finished_at - interval (SECOND(NEW.finished_at) * 1000000 + MICROSECOND(NEW.finished_at)) MICROSECOND, NEW.channel, NEW.status, case when NEW.status != 'sent' or ms <= 0 then 0 else (((ms - 1) >> GREATEST(CHAR_LENGTH(BIN(ms - 1)) - 4, 0)) + 1) << GREATEST(CHAR_LENGTH(BIN(ms - 1)) - 4, 0) end, CRC32(NEW.id) % 4, 1) on duplicate key update n = n + 1;
end if;
end;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER `notifications_finish_rollup_update` after update ON `notifications` FOR EACH ROW begin
declare
ms bigint;
if not (NEW.status <=> OLD.status and NEW.finished_at <=> OLD.finished_at and NEW.channel <=> OLD.channel and NEW.created_at <=> OLD.created_at and NEW.scheduled_at <=> OLD.scheduled_at) then
if OLD.status in ('sent', 'failed') and OLD.finished_at is not null then
set ms = cast(CEIL(TIMESTAMPDIFF(MICROSECOND, GREATEST(COALESCE(OLD.scheduled_at, OLD.created_at), OLD.created_at), OLD.finished_at) / 1000) as signed);
update finishes_per_minute set n = n - 1 where minute = OLD.finished_at - interval (SECOND(OLD.finished_at) * 1000000 + MICROSECOND(OLD.finished_at)) MICROSECOND and channel = OLD.channel and status = OLD.status and n > 0 and latency_le_ms = case when OLD.status != 'sent' or ms <= 0 then 0 else (((ms - 1) >> GREATEST(CHAR_LENGTH(BIN(ms - 1)) - 4, 0)) + 1) << GREATEST(CHAR_LENGTH(BIN(ms - 1)) - 4, 0) end order by shard asc limit 1;
if ROW_COUNT() = 0 then
update finishes_per_minute set n = n - 1 where minute = OLD.finished_at - interval (SECOND(OLD.finished_at) * 1000000 + MICROSECOND(OLD.finished_at)) MICROSECOND and channel = OLD.channel and status = OLD.status and n > 0 and latency_le_ms = case when OLD.status != 'sent' or ms <= 0 then 0 else (((ms - 1) >> GREATEST(CHAR_LENGTH(BIN(ms - 1)) - 4, 0)) + 1) << GREATEST(CHAR_LENGTH(BIN(ms - 1)) - 4, 0) end order by shard asc limit 1;
end if;
end if;
if NEW.status in ('sent', 'failed') and NEW.finished_at is not null then
set ms = cast(CEIL(TIMESTAMPDIFF(MICROSECOND, GREATEST(COALESCE(NEW.scheduled_at, NEW.created_at), NEW.created_at), NEW.finished_at) / 1000) as signed);
insert into finishes_per_minute(minute, channel, status, latency_le_ms, shard, n) values (NEW.finished_at - interval (SECOND(NEW.finished_at) * 1000000 + MICROSECOND(NEW.finished_at)) MICROSECOND, NEW.channel, NEW.status, case when NEW.status != 'sent' or ms <= 0 then 0 else (((ms - 1) >> GREATEST(CHAR_LENGTH(BIN(ms - 1)) - 4, 0)) + 1) << GREATEST(CHAR_LENGTH(BIN(ms - 1)) - 4, 0) end, CRC32(NEW.id) % 4, 1) on duplicate key update n = n + 1;
end if;
end if;
end;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER `delivery_attempts_rollup_insert` after insert ON `delivery_attempts` FOR EACH ROW begin
if NEW.outcome in ('accepted', 'transient', 'permanent', 'unknown') then
insert into attempts_per_minute(minute, channel, outcome, shard, n) values (NEW.started_at - interval (SECOND(NEW.started_at) * 1000000 + MICROSECOND(NEW.started_at)) MICROSECOND, NEW.channel, NEW.outcome, NEW.id % 4, 1) on duplicate key update n = n + 1;
end if;
end;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE TRIGGER `delivery_attempts_rollup_update` after update ON `delivery_attempts` FOR EACH ROW begin
if not (NEW.outcome <=> OLD.outcome and NEW.started_at <=> OLD.started_at and NEW.channel <=> OLD.channel) then
if OLD.outcome in ('accepted', 'transient', 'permanent', 'unknown') then
update attempts_per_minute set n = n - 1 where minute = OLD.started_at - interval (SECOND(OLD.started_at) * 1000000 + MICROSECOND(OLD.started_at)) MICROSECOND and channel = OLD.channel and outcome = OLD.outcome and n > 0 order by shard asc limit 1;
if ROW_COUNT() = 0 then
update attempts_per_minute set n = n - 1 where minute = OLD.started_at - interval (SECOND(OLD.started_at) * 1000000 + MICROSECOND(OLD.started_at)) MICROSECOND and channel = OLD.channel and outcome = OLD.outcome and n > 0 order by shard asc limit 1;
end if;
end if;
if NEW.outcome in ('accepted', 'transient', 'permanent', 'unknown') then
insert into attempts_per_minute(minute, channel, outcome, shard, n) values (NEW.started_at - interval (SECOND(NEW.started_at) * 1000000 + MICROSECOND(NEW.started_at)) MICROSECOND, NEW.channel, NEW.outcome, NEW.id % 4, 1) on duplicate key update n = n + 1;
end if;
end if;
end;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER IF EXISTS `delivery_attempts_rollup_insert`;
DROP TRIGGER IF EXISTS `delivery_attempts_rollup_update`;
DROP TRIGGER IF EXISTS `notifications_finish_rollup_insert`;
DROP TRIGGER IF EXISTS `notifications_finish_rollup_update`;
ALTER TABLE `notifications` DROP INDEX `claim_idx`;
ALTER TABLE `notifications` ADD KEY `claim_idx` (`status`, `channel`, `priority`, `due_at`);
ALTER TABLE `notifications` DROP INDEX `finished_idx`;
ALTER TABLE `notifications` ADD KEY `finished_idx` (`finished_at`, `status`, `channel`);
DROP TABLE `attempts_per_minute`;
DROP TABLE `finishes_per_minute`;
