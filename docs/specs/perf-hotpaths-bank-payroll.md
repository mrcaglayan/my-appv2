# PR-H03 Performance Hot Paths (Bank + Payroll)

This note documents the H03 hardening targets implemented in the repo.

## Cursor-Paginated Operational Lists

- `GET /api/v1/bank/statements/imports`
  - sort: `imported_at DESC, id DESC`
  - cursor payload: `{ importedAt, id }`
- `GET /api/v1/bank/statements/lines`
  - sort: `txn_date DESC, id DESC`
  - cursor payload: `{ txnDate, id }`
- `GET /api/v1/bank/reconciliation/queue`
  - sort: `txn_date DESC, id DESC`
  - cursor payload: `{ txnDate, id }`
- `GET /api/v1/bank/reconciliation/exceptions`
  - sort: `status-rank ASC, updated_at DESC, id DESC`
  - cursor payload: `{ statusRank, updatedAt, id }`
- `GET /api/v1/payroll/liabilities`
  - sort: `id DESC`
  - cursor payload: `{ id }`
- `GET /api/v1/payroll/provider-imports`
  - sort: `requested_at DESC, id DESC`
  - cursor payload: `{ requestedAt, id }`

## Response Shape (Additive)

List responses continue to return:

- `rows`
- `total`
- `limit`
- `offset`

H03 adds:

- `pageMode` (`OFFSET` or `CURSOR`)
- `nextCursor` (opaque token or `null`)

## Query/Index Hardening Targets

Main H03 indexes are added in:

- `backend/src/migrations/m062_performance_indexes_and_pagination_hardening.js`

Focus areas:

- bank statement import/line list filters + sort order
- reconciliation queue match aggregation + exception queue ordering
- payroll liabilities and settlement follow-up queries
- payroll provider import job/audit history reads

## EXPLAIN Checks (manual smoke)

Use `EXPLAIN` on representative queries after seeding larger data:

1. bank statement imports list (`legalEntityId`, `bankAccountId`, `status`)
2. reconciliation queue (`legalEntityId`, `bankAccountId`, `reconStatus`)
3. reconciliation exceptions (`status`, `bankAccountId`)
4. payroll liabilities (`runId`, `status`)
5. payroll provider imports (`legalEntityId`, `providerCode`, `status`)

Validate:

- expected composite indexes are used
- no full scans on the main filtered paths
- cursor page 2 returns stable ordering (no duplicates / skips)

