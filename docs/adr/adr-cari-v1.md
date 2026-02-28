# ADR: Cari (AR/AP) v1 Scope Lock

- Status: Accepted (updated to match implemented v1 behavior)
- Date: 2026-02-23
- Decision owner: Core ERP team
- Applies to: Cari v1 implementation (PR-01 through PR-10)

## Context

This ADR is the frozen source of truth for Cari v1 lifecycle, numbering, posting, settlement, and reporting semantics.  
It is aligned with the current implementation so backend/frontend teams can ship without re-deciding core accounting rules.

## Decisions (Frozen)

### 1) Document numbering model

- Cari uses two numbering tracks:
  - Draft track for editable drafts.
  - Posted track for accounting-final documents.
- Draft sequence (`sequence_namespace='DRAFT'`) is assigned at draft create/update when needed.
  - Draft uniqueness dimensions: `tenant + legal_entity + direction + sequence_namespace + fiscal_year + sequence_no`.
  - Draft number format: `DRAFT-{direction}-{fiscal_year}-{seq6}`.
- Posted sequence is assigned on `POST` (and on reversal-document creation).
  - Posted uniqueness dimensions: `tenant + legal_entity + direction + sequence_namespace(document_type) + fiscal_year + sequence_no`.
  - Posted number format: `{direction}-{document_type}-{fiscal_year}-{seq6}`.
- Posted numbers are immutable.

### 2) Settlement numbering model

- Settlement namespace is fixed to `SETTLEMENT`.
- Numbering dimensions: `tenant + legal_entity + sequence_namespace + fiscal_year + sequence_no`.
- Number is assigned on settlement apply and on settlement-reversal row creation.
- Number format: `SETTLEMENT-{fiscal_year}-{seq6}`.
- Settlement numbers are immutable after write.

### 3) Status enums and lifecycle semantics

- `cari_documents.status` enum:
  - `DRAFT | POSTED | PARTIALLY_SETTLED | SETTLED | CANCELLED | REVERSED`
- `cari_open_items.status` enum:
  - `OPEN | PARTIALLY_SETTLED | SETTLED | WRITTEN_OFF | CANCELLED`
- `cari_settlement_batches.status` enum:
  - `DRAFT | POSTED | REVERSED | CANCELLED`
- Runtime boundaries:
  - Document create/update/cancel are draft-only.
  - Document post is draft-only.
  - Document reverse is posted-only.
  - Settlement reverse is posted-only.
- Terminology note:
  - Settlement apply transitions settlement batches to `POSTED`.

### 4) Reversal and cancel boundaries

- `CANCEL` is draft-only.
- `REVERSE` is posted-only.
- No destructive mutation of posted accounting history.
- Reversal is additive and traceable:
  - New reversal document/settlement rows are created.
  - Original rows are marked `REVERSED`.

### 5) Mandatory accounting linkage fields

- Documents:
  - `cari_documents.posted_journal_entry_id`
  - `cari_documents.reversal_of_document_id`
- Settlements:
  - `cari_settlement_batches.posted_journal_entry_id`
  - `cari_settlement_batches.reversal_of_settlement_batch_id`
- Single-reversal constraints are enforced by unique linkage keys.

### 6) Counterparty and tenant scope model

- Counterparty and payment term masters are legal-entity scoped.
- Same counterparty code may exist in multiple legal entities for a tenant, but not duplicated in the same `(tenant_id, legal_entity_id)`.
- Counterparty role storage is dual-boolean:
  - `counterparties.is_customer` and `counterparties.is_vendor`
  - At least one role flag must be true.
  - `counterpartyType` in API/report payloads is a derived compatibility field, not a stored source-of-truth enum.
- Cari operations enforce tenant and legal-entity ownership/scope checks before mutations.

### 7) Snapshot policy (historical immutability)

- Snapshot-safe fields are persisted on document rows and used by reports/statements:
  - `counterparty_code_snapshot`
  - `counterparty_name_snapshot`
  - `payment_term_snapshot`
  - `due_date_snapshot`
  - `currency_code_snapshot`
  - `fx_rate_snapshot`
- Snapshots are set from current master data during draft creation/update and frozen for historical reporting once posted.
- Historical output must not drift when master data changes later.

### 8) FX policy and realized FX account source

- Posting and settlement FX resolution are exact-date, pair-specific SPOT lookups by tenant.
- If no exact-date SPOT exists for a non-parity currency pair, an explicit request FX rate is required.
- Locked FX rates require explicit override intent and reason; override is guarded by `cari.fx.override`.
- Realized FX gain/loss account resolution uses `journal_purpose_accounts` mappings (no hardcoded account IDs in service logic).

### 9) Idempotency and concurrency policy

- Settlement apply requires `idempotencyKey`.
- Bank attach/apply flows use dedicated idempotency keys and replay semantics.
- Duplicate idempotency replays must not create duplicate accounting effects.
- Apply/reverse execute transactionally with row-level locking (`FOR UPDATE`) on critical rows (sequences, documents/open items, settlement/unapplied rows).
- Allocation/order strategy is deterministic to reduce race and deadlock risk.
- Over-allocation and cross-key mismatch scenarios are rejected.

### 10) Bank-ready scope (v1)

- v1 includes bank-link compatibility hooks:
  - `bank_statement_line_id`
  - `bank_transaction_ref`
  - bank attach/apply idempotency keys
- v1 does not include full bank reconciliation lifecycle.
- Bank-triggered settlement apply follows the same accounting and safety rules as manual apply.

### 11) As-of report semantics

- Reports accept `asOfDate` and include only accounting effects effective on/before that date.
- Aging bucket basis is open-item due date (`open_item.due_date`, fallback to snapshot/document date as implemented).
- Statement/open-item residuals are computed from original amounts minus as-of-valid allocations.
- Reversed documents/settlements are included/excluded based on reversal effective date relative to `asOfDate`.
- Snapshot fields drive historical labels in AR/AP aging, open-items, and statements.
- Unapplied balances are surfaced explicitly, not silently netted away.

### 12) Audit visibility semantics

- Cari write actions are persisted in `audit_logs`.
- `GET /api/v1/cari/audit` is the supported read endpoint for Cari audit visibility.
- The endpoint is tenant-safe and legal-entity scope-safe and supports time/action/resource filters for support and finance operations.

## Out of Scope (v1)

- Dunning/collections workflows
- Credit insurance workflows
- Full bank reconciliation module
- E-invoice and tax-engine extras beyond core posting/settlement needs

## Implementation guardrails

- PR-01..PR-10 must conform to this ADR.
- Any deviation requires explicit ADR amendment before code change.
- No PR may silently alter these lifecycle, numbering, scope, idempotency, or reporting rules.
