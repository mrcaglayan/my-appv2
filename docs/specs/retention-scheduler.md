# Retention Scheduler (PR-I05)

This scheduler enqueues due `DATA_RETENTION_RUN` jobs automatically for active retention policies.

## Entry points

- One-shot tick:
  - `npm run job:retention:schedule-due`
- Continuous polling loop:
  - `npm run jobs:retention:scheduler`

## Environment

- `RETENTION_SCHED_TENANT_ID` (optional): restrict scheduler to a tenant.
- `RETENTION_SCHED_LEGAL_ENTITY_ID` (optional): restrict scheduler to one legal entity.
- `RETENTION_SCHED_USER_ID` (optional): actor user id written into queued payload.
- `RETENTION_SCHED_LIMIT` (optional, default `200`): max active policies scanned per tick.
- `RETENTION_SCHED_DRY_RUN` (optional): only for one-shot tick; no jobs enqueued.
- `RETENTION_SCHED_POLL_MS` (optional, default `60000`): polling interval for loop mode.

## Policy schedule config (in `data_retention_policies.config_json`)

- `schedule_enabled` (bool, default `true`)
- `schedule_interval_minutes` (int, default `1440`, min `1`, max `43200`)

Due calculation:

- baseline = `last_run_at` if present, otherwise `created_at`
- due_at = baseline + `schedule_interval_minutes`
- if `now >= due_at`, scheduler attempts enqueue
- if policy already has a running retention run, enqueue is skipped

Idempotency:

- queued job idempotency key: `RETENTION_SCHED|POLICY:{policyId}|BUCKET:{intervalBucket}`
- retention run idempotency key payload: `RETENTION_RUN_SCHED|POLICY:{policyId}|BUCKET:{intervalBucket}`

This prevents duplicate job enqueue in the same interval bucket while preserving run history across buckets.
