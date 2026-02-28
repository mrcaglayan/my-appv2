# Cari v1 Support/Finance UI Guide

## Scope

This guide is for support and finance users operating the Cari UI modules:

- `/app/cari-belgeler`
- `/app/cari-settlements`
- `/app/cari-audit`

## Document Lifecycle

- Draft stage (`DRAFT`):
  - Create, edit, cancel actions are available only in draft.
- Posting:
  - Post action is allowed only for draft documents.
  - After post, journal linkage fields become the main accounting reference.
- Reversal:
  - Reverse is allowed only under backend reversal guards for posted lifecycle states.
  - Reversal keeps additive history; original rows remain traceable.

## Settlement Idempotency Behavior

- Apply action always requires `idempotencyKey`.
- Retry with the same key returns a deterministic result.
- Do not generate a new key for accidental double-click retries of the same intent.

## Payment Channel (`MANUAL` / `CASH`) and Linked Cash

- `paymentChannel=MANUAL`:
  - settlement runs without creating a cash transaction.
- `paymentChannel=CASH`:
  - either link an existing cash transaction (`cashTransactionId`)
  - or create one in-flow (`linkedCashTransaction` with register/account context)
- Direction coupling:
  - `AR` -> linked cash type `RECEIPT`
  - `AP` -> linked cash type `PAYOUT`
- Validation guardrails:
  - `linkedCashTransaction` is only valid with `paymentChannel=CASH`
  - `cashTransactionId` and `linkedCashTransaction` cannot be sent together
  - if creating linked cash, register/account requirements must be satisfied

## Replay Behavior (`idempotentReplay`)

- If response contains `idempotentReplay=true`, treat it as informational success.
- Operator message meaning:
  - request was already applied previously
  - current response mirrors existing result
- Do not re-open incident unless output is inconsistent with expected source data.

## Reverse Behavior (Document + Settlement)

- Document reverse:
  - reverses accounting effect with explicit linkage to reversal row/journal context.
- Settlement reverse:
  - called via `POST /api/v1/cari/settlements/{settlementBatchId}/reverse`.
  - re-opens affected balances according to effective-date/as-of rules.
  - if a linked cash transaction is still `POSTED`, reverse is blocked until that cash transaction is reversed.
- Always validate statement/open-items as-of dates before and after reverse date.

## Bank Attach/Apply Meaning

- Bank attach and bank apply are explicit workflows.
- They are not auto-triggered by settlement apply.
- Target rules:
  - `targetType=SETTLEMENT`: requires `settlementBatchId`, no `unappliedCashId`.
  - `targetType=UNAPPLIED_CASH`: requires `unappliedCashId`, no `settlementBatchId`.
- Both flows must send idempotency keys.

## FX Override Use-Case and Permissions

- FX override is a controlled exception path, not the default flow.
- Permission requirement: `cari.fx.override`.
- Override submissions must include explicit justification fields where required by UI/backend contract.
- Without permission, users must use standard rate behavior and should see clear inline guidance.
- Fallback modes:
  - `EXACT_ONLY`: only same-day SPOT rate is accepted
  - `PRIOR_DATE`: nearest prior SPOT can be used (optionally bounded by `fxFallbackMaxDays`)
- If no valid rate is found and no override `fxRate` is provided, apply fails with explicit error.

## Quick Triage Checklist

1. Confirm route-level access permission exists.
2. Confirm action-level permission for the specific button/panel exists.
3. Re-run with same idempotency key for replay-safe inspection.
4. Inspect `requestId` in audit records (`/app/cari-audit`).
5. Recheck report outputs with explicit `asOfDate` around reverse/apply dates.
6. For CASH channel incidents, verify linked cash transaction status and register/session context.
7. Check `followUpRisks` messages; treat them as operational follow-up items, not hard failures.
8. For FX incidents, verify effective fallback mode (`EXACT_ONLY` vs `PRIOR_DATE`) and prior-rate availability.
