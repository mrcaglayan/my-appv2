# Cari v1 Operations Runbook

## Purpose

This runbook defines how to operate Cari v1 AR/AP workflows in production-like environments and how to troubleshoot common failures without violating ADR-frozen rules.

## Status Terminology (Important)

- Settlement apply results in `POSTED` status in runtime/API rows.
- Reversal remains `REVERSED` and is additive (original row is preserved and linked).
- Open-item report status filters use `OPEN | PARTIALLY_SETTLED | SETTLED | ALL`.

## Counterparty Role Model (Important)

- Counterparty role source of truth is dual booleans: `is_customer`, `is_vendor`.
- At least one role flag must be true.
- Any `counterpartyType` field in responses is derived from booleans (`CUSTOMER`, `VENDOR`, `BOTH`) for compatibility.

## Unapplied Cash Handling

- Unapplied cash is created when settlement incoming amount exceeds allocated amount.
- Unapplied balances are consumed by later settlement applies when `useUnappliedCash=true`.
- Consumption is as-of sensitive in reports; only effects with effective date `<= asOfDate` are included.
- Unapplied rows are never silently netted away in reports unless a report rule explicitly says so.
- Key checks:
  - Verify `cari_unapplied_cash.residual_amount_txn` and `residual_amount_base`.
  - Review settlement apply audit payload (`unappliedConsumed`, `createdUnappliedCashId`).
  - Confirm reversal effects are additive and traceable via linked settlement rows.

## FX Override Policy

FX resolution baseline (exact + prior-date fallback):

- FX override remains controlled by explicit permission (`cari.fx.override`).
- Settlement FX resolution order is deterministic:
  - request `fxRate` (if supplied)
  - exact-date SPOT rate
  - optional nearest-prior SPOT fallback (`PRIOR_DATE`) when enabled
- Fallback controls:
  - default mode can be set by `CARI_SETTLEMENT_FX_FALLBACK_MODE` (`EXACT_ONLY` or `PRIOR_DATE`)
  - max search depth can be set by `CARI_SETTLEMENT_FX_FALLBACK_MAX_DAYS`
  - request-level overrides are supported via `fxFallbackMode` and `fxFallbackMaxDays`
- If neither exact-date nor allowed prior-date rate is available and `fxRate` is not provided, apply must fail with explicit error.
- Operational actions:
  - Validate the expected currency pair rate chain (exact date + prior window) before batch apply windows.
  - Confirm audit/journal evidence captures fallback/override context when used.

## Reversal Effects on Statements and Aging

- Reversal is additive history, not destructive mutation.
- Document reversal impacts statement status and as-of inclusion based on reversal effective date.
- Settlement reversal re-opens impacted residuals as-of the reversal date and deactivates reversed allocations as-of.
- Aging and open-items outputs must change when `asOfDate` crosses reversal dates.
- Operator verification steps:
  - Compare `asOfDate` before/after reversal date.
  - Confirm statement shows reversal links (`reversalOf*`, `reversedBy*`).
  - Confirm residual totals reconcile between document/open-item views.

## Bank-Link Meaning in Cari v1

- Bank-link fields are integration hooks before full bank module rollout.
- `bank_statement_line_id` and `bank_transaction_ref` indicate external bank linkage context.
- `bank_attach_idempotency_key` and `bank_apply_idempotency_key` protect against duplicate bank-triggered requests.
- Bank-linked flows follow the same accounting and idempotency rules as manual apply.

## Source-Aware Settlement Posting Context

- Settlement posting derives context from source linkage and intent:
  - `CASH_LINKED`
  - `MANUAL`
  - `ON_ACCOUNT_APPLY`
- The context influences posting derivation while preserving generic mapping fallback for compatibility.
- Practical checks:
  - verify linked-cash settlements carry cash references (`cash_transaction_id`, link metadata)
  - verify manual flows remain cash-agnostic
  - verify on-account consumption leaves a traceable unapplied-cash history

## Operational Troubleshooting

### Audit visibility endpoint

- Use `GET /api/v1/cari/audit` for support/finance investigation of Cari actions.
- Scope filters: `legalEntityId`, `action`, `resourceType`, `resourceId`, `actorUserId`, `requestId`.
- Time filters: `createdFrom`, `createdTo`.
- Paging/payload controls: `limit`, `offset`, `includePayload`.
- Endpoint is tenant-safe and legal-entity scope-safe via `cari.audit.read`.

### Permission and scope failures (401/403)

- Confirm user has required permission (`cari.*`) and legal-entity scope.
- For scoped users, verify access with and without `legalEntityId` filter.

### Posting/apply blocked by policy or data

- Check fiscal period status and posting preconditions.
- Verify document/open-item statuses are valid for attempted transition.
- Ensure required mappings exist for posting and realized FX entries.

### Idempotency and duplicate-click incidents

- Reuse the same idempotency key to safely replay and inspect prior result.
- Different idempotency keys represent distinct operations and can legitimately fail/succeed independently.

### Reversal failures

- Confirm original row is in reversible state and not already reversed.
- Validate dependent balances were not progressed beyond reversible boundary.

### Reporting mismatches

- Re-run with same `asOfDate`, then with date before/after key events (settle/reverse).
- Check report filters (`role`, `status`, `direction`, `legalEntityId`, `counterpartyId`).
- Use `EXPLAIN` checks to confirm expected indexes are still used on key report query shapes.

### Counterparty payment-term dropdown is empty

- `POST /api/v1/onboarding/company-bootstrap` now auto-seeds default payment terms for each onboarded legal entity.
- `POST /api/v1/org/legal-entities` now also seeds payment terms when `autoProvisionDefaults=true`.
- `POST /api/v1/org/legal-entities` can accept custom `paymentTerms` definitions to seed legal-entity-specific terms at creation time.
- If `POST /api/v1/org/legal-entities` is called with `autoProvisionDefaults=false` and without `paymentTerms`, no terms are seeded for that entity.
- Cari counterparty forms read payment terms from `GET /api/v1/cari/payment-terms` filtered by legal entity.
- If no terms exist for the tenant/legal-entity pair, bootstrap defaults with:
  - `POST /api/v1/onboarding/payment-terms/bootstrap`
- This endpoint is idempotent; reruns only insert missing terms.
- To seed one legal entity or custom terms, pass `legalEntityId`/`legalEntityIds` and `terms` in the request body.

### Audit verification

- Verify audit rows for critical actions:
  - `cari.document.post`
  - `cari.document.reverse`
  - `cari.settlement.apply`
  - `cari.settlement.reverse`

## Manual Smoke Checklist

1. Create a counterparty in an allowed legal entity.
2. Create a draft Cari document.
3. Post the document and verify posting snapshots.
4. Apply a partial settlement.
5. Reverse the settlement.
6. Reverse the posted document.
7. Run AR/AP aging, open-items, and statement reports with explicit `asOfDate`.
8. Verify audit logs for post/apply/reverse actions.
9. Validate idempotency replay behavior for one apply request.
10. Confirm bank-link fields display where available.
11. Run one `paymentChannel=CASH` apply and verify linked cash references are present.
12. Validate settlement reverse guard when linked cash transaction is still `POSTED`.
13. Validate FX fallback behavior:
  - `EXACT_ONLY` missing rate -> explicit failure
  - `PRIOR_DATE` with available prior rate -> success

## UI Route Coverage (PR-11..14)

- `/app/cari-belgeler`: document lifecycle operations (draft, post, reverse) with document-level permissions.
- `/app/cari-settlements`: settlement and bank-link workbench (route open is any-of; actions are permission-gated per panel).
- `/app/cari-audit`: support/finance investigation view over `GET /api/v1/cari/audit`.

## Operator Flow Summary

- Document lifecycle:
  - Create/update/cancel only in `DRAFT`.
  - Post only in `DRAFT`.
  - Reverse only from posted lifecycle states per backend guards.
- Settlement lifecycle:
  - Apply requires idempotency key and allocation rule compliance (`autoAllocate` vs `allocations`).
  - `paymentChannel=CASH` allows linking existing cash txn (`cashTransactionId`) or creating one (`linkedCashTransaction`).
  - Reverse uses `POST /api/v1/cari/settlements/{settlementBatchId}/reverse`.
  - Reverse is blocked when linked cash txn is still posted; reverse cash first.
- Replay and idempotency:
  - `idempotentReplay=true` must be treated as safe replay of an already-applied request.
  - `followUpRisks` is an operational warning input, not a silent ignore field.
- Bank-link meaning:
  - Bank attach/apply actions are explicit, separate from settlement apply.
  - Bank flows keep their own idempotency keys and target validation rules.
- FX override:
  - Only permitted for users with `cari.fx.override`.
  - Override/fallback behavior must be reviewable (`EXACT_ONLY` vs `PRIOR_DATE`, optional max-day bound).

For day-to-day support and finance execution details, use:
- `docs/runbooks/cari-v1-support-finance-ui-guide.md`

## Recommended Commands

- Backend release gate:
  - `cd backend && npm run test:release-gate`
  - Core-only (skip contracts/revenue module extension): `cd backend && RELEASE_GATE_SKIP_CONTRACTS_REVENUE=1 npm run test:release-gate`
- Cari focused quality gate:
  - `cd backend && npm run test:cari-quality-gate`
- OpenAPI/docs validation:
  - `cd backend && npm run test:cari-pr10`
