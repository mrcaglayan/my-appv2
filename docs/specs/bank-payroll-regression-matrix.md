# Bank + Payroll Regression Matrix (PR-H08)

## Purpose

This matrix defines the staged release gate for bank/payroll and maps each stage to executable scripts.
The gate is run by `npm run test:e2e:bank-payroll`.

## Stage Map

| Stage ID | Scope | Scripts |
| --- | --- | --- |
| `bank-flow` | Payment batch lifecycle, export/ack, reconciliation, exception queue, approvals | `test:payments:prb04`, `test:bank:prb06`, `test:bank:prb03`, `test:bank:prb07`, `test:bank:prb08a`, `test:bank:prb08b`, `test:bank:prb09` |
| `payroll-flow` | Provider import -> accrual -> liabilities -> settlement -> corrections -> close | `test:payroll:prp01` .. `test:payroll:prp09` |
| `cross-flow` | Jobs/retry, approval engine, ops KPIs, unified exceptions, retention/snapshots, isolation hardening | `test:hardening:prh02`, `test:hardening:prh04`, `test:hardening:prh05`, `test:hardening:prh06`, `test:hardening:prh07`, `test:hardening:prh09` |

## Failure Contract

- Runner stops on first failing stage.
- Failure output includes stage id and failing npm script.
- This makes failure attribution explicit (import, settlement, close, etc.).

## Local Run

```bash
npm run test:e2e:bank-payroll
```

Optional scoped runs:

```bash
BANK_PAYROLL_E2E_ONLY_STAGES=bank-flow npm run test:e2e:bank-payroll
BANK_PAYROLL_E2E_SKIP_STAGES=cross-flow npm run test:e2e:bank-payroll
BANK_PAYROLL_E2E_DRY_RUN=1 npm run test:e2e:bank-payroll
```

## CI

Workflow: `.github/workflows/bank-payroll-release-gate.yml`

- Brings up MySQL 8
- Runs backend migrations
- Seeds core permissions/roles/users
- Executes `test:e2e:bank-payroll`

## Current Status

- H08 release-gate framework is wired and runnable.
- `test:bank:prb03` now runs real service-level assertions (queue/suggestions/match-unmatch/ignore/audit transitions).
- `test:payments:prb04` now runs real service-level assertions (create/idempotency/approve/export/post/audit).
- `test:bank:prb06` now runs real service-level assertions (wrapper export, ack import statuses, idempotency, over-ack guard).
- `test:bank:prb07` now runs real service-level assertions (rule CRUD, preview/apply automation, exception lifecycle, idempotent replay).
- `test:bank:prb08a` now runs real service-level assertions (posting template CRUD, AUTO_POST_TEMPLATE apply path, journal/trace/match idempotency).
- `test:bank:prb08b` now runs real service-level assertions (return processing, manual rejection handling, FX difference adjustment/reconciliation, idempotent apply replay).
- `test:bank:prb09` and `test:payroll:prp08` now run real service-level assertions (SoD/maker-checker, idempotency, lock enforcement).
- `test:hardening:prh02` now runs real service-level assertions (job enqueue idempotency, worker run success/failure lifecycle, attempt history integrity, requeue/cancel admin actions, PAYROLL_IMPORT_APPLY handler integration).
- `test:hardening:prh04` now runs real service-level assertions (unified PAYROLL policy matching by threshold, request idempotency, maker-checker + multi-approver flow, auto-execute on final approval, reject path, scope denial checks).
- `test:hardening:prh05` now runs real service-level assertions (ops dashboard KPI aggregations for bank reconciliation, payment batch health, payroll import/close health, jobs/attempt SLAs, plus legal-entity scope-denial checks).
- `test:hardening:prh06` now runs real service-level assertions (multi-source BANK/PAYROLL workbench refresh normalization, queue summary integrity, claim/resolve/reopen transitions with audit trail, and source-table immutability checks).
- `test:hardening:prh07` now runs real service-level assertions (retention policy create/manual execution/idempotent replay, `DATA_RETENTION_RUN` job-handler execution path, payroll close snapshot export idempotency + stable hashes, and non-destructive core payroll row checks).
- `test:hardening:prh09` now runs real service-level assertions (strict tenant + legal-entity isolation on bank/payroll/workbench services, entity-bound id validation, and cross-tenant access denial checks).
- `test:payroll:prp01` now runs real service-level assertions (import/list/detail/line reads, duplicate checksum conflict, CSV validation failures, scope-permission denial).
- `test:payroll:prp02` now runs real service-level assertions (accrual preview missing mappings, effective-dated component mappings, review/finalize lifecycle, idempotent finalize, journal/audit integrity).
- `test:payroll:prp03` now runs real service-level assertions (liability build lifecycle, NET/STATUTORY/ALL payment prep previews, beneficiary setup gate, payment batch preparation/linking, snapshot capture readiness).
- `test:payroll:prp04` now runs real service-level assertions (B03 reconciliation evidence-driven MARK_PAID sync, idempotent re-apply, cancelled-batch RELEASE_TO_OPEN flow, permission denial checks).
- `test:payroll:prp05` now runs real service-level assertions (RETRO correction shell creation/idempotency, import into target correction shell, correction finalize, regular-run reversal with liability cancellation, correction list integrity, idempotent reverse replay).
- `test:payroll:prp06` now runs real service-level assertions (partial settlement classification/apply, manual override request/reject/approve maker-checker controls, idempotent replays, closed-period manual-settlement lock enforcement, permission denial checks).
- `test:payroll:prp07` now runs real service-level assertions (beneficiary master setup/list, primary switching with single-primary enforcement, immutable snapshot export behavior, new snapshot capture after master switch, missing-beneficiary blocking, permission denial checks).
- `test:payroll:prp09` now runs real service-level assertions (provider adapter registry, connection/ref setup, preview mapping/idempotency, apply maker-checker/idempotency, closed-period import-apply lock gating, scope-permission denial checks).
- All stage scripts in `bank-flow`, `payroll-flow`, and `cross-flow` now run real service-level assertions, and `test:e2e:bank-payroll` passes end-to-end.
