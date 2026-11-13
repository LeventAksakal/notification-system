-- +goose Up
-- create "app_meta" table
CREATE TABLE `app_meta` (`k` varchar(64) NOT NULL, `v` varchar(255) NOT NULL, `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3), PRIMARY KEY (`k`), CONSTRAINT `k_not_blank` CHECK (char_length(trim(`k`)) > 0)) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- +goose Down
-- reverse: create "app_meta" table
DROP TABLE `app_meta`;
