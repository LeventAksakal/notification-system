---
status: closed
created: 2026-11-15
tags: [mysql, migrations, privileges]
---
# Who runs migrations once the schema has triggers (0014)

## Question
`app migrate` runs as the application user `app` and now creates triggers (decision 0005, "As built"). MySQL 8.4 with binary logging on refuses `CREATE TRIGGER` from a user without SUPER (error 1419) unless `log_bin_trust_function_creators` is on, and that variable is deprecated. How should migrations get the right to create triggers in production?

## Context
Measured 15 Nov 2026: MySQL 8.4.13, `log_bin=1`, `binlog_format=ROW`; setting the variable warns "'@@log_bin_trust_function_creators' is deprecated and will be removed in a future release". With ROW format the replica applies row images, not a re-run of the trigger, so the risk the variable guards against (non-deterministic statement replay) does not apply here.

## Options
1. Keep `--log-bin-trust-function-creators=ON` on the server (what compose and the test containers do now). One flag; deprecated, so it breaks on the MySQL release that removes it; on a managed database it is a parameter-group setting.
2. Run migrations as a separate privileged user (a `APP_MIGRATE_DATABASE_DSN` secret used only by `app migrate`); the runtime user keeps DML rights only. Least privilege and future-proof; one more secret and a compose change.
3. Turn binary logging off (`--skip-log-bin`). No flag to maintain; loses point-in-time recovery and replication.

## Recommendation
Option 2 before the first production deployment; option 1 meanwhile (taken as the assumption on 15 Nov).

## Answer
Owner, 16 Nov 2026: covered in the walkthrough. Option 1 stands for this submission (compose and the test containers set `log_bin_trust_function_creators`); option 2, a separate privileged migration user, is the step before a production deployment. Closed by the coordinator.
