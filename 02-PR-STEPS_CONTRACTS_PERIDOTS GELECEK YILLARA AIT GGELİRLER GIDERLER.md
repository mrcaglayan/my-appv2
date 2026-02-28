## 9) PR-16: Contracts Foundation (Backend First)

### Goal

Introduce contract domain without changing existing Cari behavior.

### Files to create

- `backend/src/migrations/m020_contracts_foundation.js`
- `backend/src/routes/contracts.js`
- `backend/src/routes/contracts.validators.js`
- `backend/src/services/contracts.service.js`
- `backend/scripts/test-contracts-pr16-schema-and-api.js`

### Files to update

- `backend/src/migrations/index.js`
- `backend/src/index.js`
- `backend/src/seedCore.js`
- `backend/scripts/generate-openapi.js`
- `backend/package.json`

### New backend endpoints

- `GET /api/v1/contracts`
- `GET /api/v1/contracts/{contractId}`
- `POST /api/v1/contracts`
- `PUT /api/v1/contracts/{contractId}`
- `POST /api/v1/contracts/{contractId}/activate`
- `POST /api/v1/contracts/{contractId}/suspend`
- `POST /api/v1/contracts/{contractId}/close`
- `POST /api/v1/contracts/{contractId}/cancel`
- `POST /api/v1/contracts/{contractId}/link-document`
- `GET /api/v1/contracts/{contractId}/documents`

### PR-16 Contract Lines Payload Model Freeze

- `POST /api/v1/contracts` accepts nested `lines[]` payload for initial line set.
- `PUT /api/v1/contracts/{contractId}` accepts nested `lines[]` payload and replaces line set atomically.
- PR-16 update semantics are full-replace for lines (patch semantics are out-of-scope for this PR).
- contract header date validation freeze:
  - `startDate` is required on contract write payloads (`POST` and `PUT`).
  - `endDate` is optional.
  - if `endDate` is provided, `startDate <= endDate` is mandatory.
- `lineNo` is non-authoritative in PR-16 write payload.
- backend assigns persisted `lineNo` sequentially by payload array order (`1..N`) on both `POST` and `PUT`.
- `line.status` in write payload is optional; if omitted, default is `ACTIVE`.
- if `line.status` is provided, allowed values are `ACTIVE` and `INACTIVE` only.
- `lineAmountTxn` and `lineAmountBase` must be positive and non-zero in PR-16.
- negative/credit-style contract lines are out-of-scope for PR-16 (future explicit amendment/adjustment model).
- `recognitionMethod` is one of `STRAIGHT_LINE`, `MILESTONE`, `MANUAL` (default `STRAIGHT_LINE`).
- if `recognitionMethod=STRAIGHT_LINE`: `recognitionStartDate` and `recognitionEndDate` are required.
- if `recognitionMethod=MILESTONE` or `MANUAL`: recognition dates are optional; if one date is provided, both must be provided.
- when both recognition dates exist, `recognitionStartDate <= recognitionEndDate` is mandatory.
- Contract save + line validations + line persistence must run in one DB transaction.
- Line account guards must run in the same transaction as contract write.

### PR-16 Contract Read Payload Shape Freeze

- `GET /api/v1/contracts/{contractId}` returns contract header plus `lines[]`.
- `GET /api/v1/contracts/{contractId}` returns all persisted lines (`ACTIVE` + `INACTIVE`) and each line includes `status`.
- `GET /api/v1/contracts/{contractId}` returns persisted `lineNo` for each line (ordered by `lineNo` ascending).
- `GET /api/v1/contracts` returns summary rows only (no nested `lines[]`; optional `lineCount` is allowed).
- if `lineCount` is returned on `GET /api/v1/contracts`, it represents total persisted lines (`ACTIVE` + `INACTIVE`) for the contract.
- default `GET /api/v1/contracts` ordering is `updatedAt DESC, id DESC` (deterministic list order).
- PR-16 read shape is fixed to avoid frontend/API guesswork.

### PR-16 Contract-Documents Read Payload Shape Freeze

- `GET /api/v1/contracts/{contractId}/documents` returns a list of contract-document link rows enriched with minimal document summary.
- each row includes link fields:
  - `linkType`, `linkedAmountTxn`, `linkedAmountBase`, `createdAt`, `createdByUserId`
- each row includes minimal linked-document summary fields:
  - `cariDocumentId`, `documentNo`, `direction`, `status`, `documentDate`, `amountTxn`, `amountBase`
- default `GET /api/v1/contracts/{contractId}/documents` ordering is `createdAt DESC, id DESC`.
- endpoint stays summary-scoped in PR-16; full cari document detail/line payloads are out-of-scope.

### PR-16 Contract Mutation Response Shape Freeze

- contract mutation endpoints return updated contract header summary:
  - `POST /api/v1/contracts`
  - `PUT /api/v1/contracts/{contractId}`
  - `POST /api/v1/contracts/{contractId}/activate`
  - `POST /api/v1/contracts/{contractId}/suspend`
  - `POST /api/v1/contracts/{contractId}/close`
  - `POST /api/v1/contracts/{contractId}/cancel`
- contract mutation responses are summary-scoped in PR-16 (no nested `lines[]`).
- `POST /api/v1/contracts/{contractId}/link-document` returns created link row + minimal linked-document summary.
- link-document mutation response shape is aligned with one row of `GET /api/v1/contracts/{contractId}/documents`.

### Migration skeleton

```js
const migration020ContractsFoundation = {
  key: "m020_contracts_foundation",
  description: "Contracts domain foundation and document links",
  async up(connection) {
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS contracts (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        tenant_id BIGINT UNSIGNED NOT NULL,
        legal_entity_id BIGINT UNSIGNED NOT NULL,
        counterparty_id BIGINT UNSIGNED NOT NULL,
        contract_no VARCHAR(80) NOT NULL,
        contract_type ENUM('CUSTOMER','VENDOR') NOT NULL,
        status ENUM('DRAFT','ACTIVE','SUSPENDED','CLOSED','CANCELLED') NOT NULL DEFAULT 'DRAFT',
        currency_code CHAR(3) NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NULL,
        total_amount_txn DECIMAL(20,6) NOT NULL DEFAULT 0,
        total_amount_base DECIMAL(20,6) NOT NULL DEFAULT 0,
        notes VARCHAR(500) NULL,
        /* Use exact SQL type of users.id in repo; do not assume INT in implementation.
           Enforce tenant-safe creator FK via (tenant_id, created_by_user_id) -> users(tenant_id, id). */
        created_by_user_id INT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_contract_no (tenant_id, legal_entity_id, contract_no),
        UNIQUE KEY uk_contracts_tenant_id_id (tenant_id, id),
        UNIQUE KEY uk_contracts_tenant_entity_id (tenant_id, legal_entity_id, id),
        KEY ix_contract_tenant_id (tenant_id),
        KEY ix_contract_scope (tenant_id, legal_entity_id, counterparty_id, status),
        KEY ix_contract_creator_tenant_user (tenant_id, created_by_user_id),
        CONSTRAINT fk_contracts_tenant
          FOREIGN KEY (tenant_id) REFERENCES tenants(id),
        CONSTRAINT fk_contracts_entity_tenant
          FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
        CONSTRAINT fk_contracts_counterparty_tenant
          FOREIGN KEY (tenant_id, legal_entity_id, counterparty_id)
          REFERENCES counterparties(tenant_id, legal_entity_id, id),
        CONSTRAINT fk_contracts_creator_user
          FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    await connection.execute(`
      CREATE TABLE IF NOT EXISTS contract_lines (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        tenant_id BIGINT UNSIGNED NOT NULL,
        contract_id BIGINT UNSIGNED NOT NULL,
        line_no INT NOT NULL,
        description VARCHAR(255) NOT NULL,
        line_amount_txn DECIMAL(20,6) NOT NULL,
        line_amount_base DECIMAL(20,6) NOT NULL,
        recognition_method ENUM('STRAIGHT_LINE','MILESTONE','MANUAL') NOT NULL DEFAULT 'STRAIGHT_LINE',
        recognition_start_date DATE NULL,
        recognition_end_date DATE NULL,
        deferred_account_id BIGINT UNSIGNED NULL,
        revenue_account_id BIGINT UNSIGNED NULL,
        status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_contract_lines_tenant_id_id (tenant_id, id),
        UNIQUE KEY uk_contract_line_no (tenant_id, contract_id, line_no),
        KEY ix_contract_line_scope (tenant_id, contract_id, status),
        CONSTRAINT fk_contract_lines_tenant
          FOREIGN KEY (tenant_id) REFERENCES tenants(id),
        CONSTRAINT fk_contract_lines_contract_tenant
          FOREIGN KEY (tenant_id, contract_id) REFERENCES contracts(tenant_id, id),
        CONSTRAINT fk_contract_lines_deferred_account
          FOREIGN KEY (deferred_account_id) REFERENCES accounts(id),
        CONSTRAINT fk_contract_lines_revenue_account
          FOREIGN KEY (revenue_account_id) REFERENCES accounts(id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    await connection.execute(`
      CREATE TABLE IF NOT EXISTS contract_document_links (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        tenant_id BIGINT UNSIGNED NOT NULL,
        legal_entity_id BIGINT UNSIGNED NOT NULL,
        contract_id BIGINT UNSIGNED NOT NULL,
        cari_document_id BIGINT UNSIGNED NOT NULL,
        link_type ENUM('BILLING','ADVANCE','ADJUSTMENT') NOT NULL,
        linked_amount_txn DECIMAL(20,6) NOT NULL DEFAULT 0,
        linked_amount_base DECIMAL(20,6) NOT NULL DEFAULT 0,
        /* Use exact SQL type of users.id in repo; do not assume INT in implementation.
           Enforce tenant-safe creator FK via (tenant_id, created_by_user_id) -> users(tenant_id, id). */
        created_by_user_id INT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_contract_doc_links_tenant_id_id (tenant_id, id),
        UNIQUE KEY uk_contract_doc_link (tenant_id, contract_id, cari_document_id, link_type),
        KEY ix_contract_doc_link_scope (tenant_id, legal_entity_id, contract_id, cari_document_id),
        KEY ix_contract_doc_link_creator_tenant_user (tenant_id, created_by_user_id),
        CONSTRAINT fk_contract_doc_links_tenant
          FOREIGN KEY (tenant_id) REFERENCES tenants(id),
        CONSTRAINT fk_contract_doc_links_entity_tenant
          FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
        CONSTRAINT fk_contract_doc_links_contract_tenant
          FOREIGN KEY (tenant_id, legal_entity_id, contract_id)
          REFERENCES contracts(tenant_id, legal_entity_id, id),
        CONSTRAINT fk_contract_doc_links_cari_doc_tenant
          FOREIGN KEY (tenant_id, legal_entity_id, cari_document_id)
          REFERENCES cari_documents(tenant_id, legal_entity_id, id),
        CONSTRAINT fk_contract_doc_links_creator_user
          FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
  },
};

export default migration020ContractsFoundation;
```

### New permissions (seed + RBAC)

- `contract.read`
- `contract.upsert`
- `contract.activate`
- `contract.suspend`
- `contract.close`
- `contract.cancel`
- `contract.link_document`

### PR-16 Endpoint Permission Mapping (explicit)

- `GET /api/v1/contracts` -> `contract.read`
- `GET /api/v1/contracts/{contractId}` -> `contract.read`
- `POST /api/v1/contracts` -> `contract.upsert`
- `PUT /api/v1/contracts/{contractId}` -> `contract.upsert`
- `POST /api/v1/contracts/{contractId}/activate` -> `contract.activate`
- `POST /api/v1/contracts/{contractId}/suspend` -> `contract.suspend`
- `POST /api/v1/contracts/{contractId}/close` -> `contract.close`
- `POST /api/v1/contracts/{contractId}/cancel` -> `contract.cancel`
- `POST /api/v1/contracts/{contractId}/link-document` -> `contract.link_document`
- `GET /api/v1/contracts/{contractId}/documents` -> `contract.read`

### Domain correctness lock (PR-16)

- DB integrity discipline (repo-style):
  - keep composite unique keys for tenant/entity-safe FK targets on new tables.
  - enforce composite FKs so cross-tenant/cross-entity mistakes fail at DB level, not only in service logic.
- Link-document direction compatibility must be enforced in service layer:
  - `contract_type=CUSTOMER` can link only to `cari_documents.direction=AR`
  - `contract_type=VENDOR` can link only to `cari_documents.direction=AP`
  - reject mismatches with explicit validation error.
- Link-document currency compatibility freeze:
  - `contracts.currency_code` must match `cari_documents.currency_code` for link operations.
  - reject cross-currency contract-document links in PR-16.
  - multi-currency linking (with link-level currency/fx snapshot semantics) is out-of-scope for PR-16.
- Counterparty role compatibility must be enforced in service layer:
  - `contract_type=CUSTOMER` requires `counterparty.is_customer = true`
  - `contract_type=VENDOR` requires `counterparty.is_vendor = true`
  - reject role-incompatible contract/counterparty combinations with explicit validation error.
- `contract_lines` legal-entity denormalization decision for PR-16:
  - keep normalized via parent `contracts.legal_entity_id` (no extra `legal_entity_id` column in `contract_lines` for now)
  - if reporting/index pressure appears later, add denormalization in a separate optimization PR.
- Contract-line account mapping safety (mandatory):
  - `assertAccountBelongsToTenant(...)` is mandatory as tenant/COA ownership pre-check, but not sufficient on its own.
  - if `deferred_account_id` or `revenue_account_id` is provided, account must belong to same tenant.
  - account must be legal-entity compatible with contract scope (`account -> coa -> legal_entity_id`).
  - account must use legal-entity chart scope for contract posting context (`coa.scope='LEGAL_ENTITY'`).
  - account must be active and postable (`is_active=true`, `allow_posting=true`).
  - account type compatibility:
    - `CUSTOMER`: `deferred_account_id` -> `LIABILITY`, `revenue_account_id` -> `REVENUE`
    - `VENDOR`: `deferred_account_id` -> `ASSET`, `revenue_account_id` -> `EXPENSE`
- `contract_document_links` legal-entity decision (freeze now):
  - store `legal_entity_id` on link rows to enable strong composite FK enforcement to both
    `contracts` and `cari_documents`.
- Link eligibility freeze:
  - link only accounting-final posted-family statuses (`POSTED`, `PARTIALLY_SETTLED`, `SETTLED`).
  - reject non-final/non-linkable statuses (`DRAFT`, `CANCELLED`, `REVERSED`).
- Contract lifecycle freeze for linking:
  - allow link when contract status is `DRAFT` or `ACTIVE`.
  - reject link when `SUSPENDED`, `CLOSED`, or `CANCELLED`.
- Contract lifecycle transition and endpoint freeze:
  - `DRAFT -> ACTIVE` via `POST /contracts/{id}/activate`
  - `ACTIVE -> SUSPENDED` via `POST /contracts/{id}/suspend`
  - `SUSPENDED -> ACTIVE` via `POST /contracts/{id}/activate`
  - `ACTIVE/SUSPENDED -> CLOSED` via `POST /contracts/{id}/close`
  - `DRAFT -> CANCELLED` via `POST /contracts/{id}/cancel`
  - `CLOSED` and `CANCELLED` are terminal.
- Contract editability-by-status freeze:
  - `PUT /contracts/{id}` is allowed only when current contract status is `DRAFT`.
  - reject update attempts for `ACTIVE`, `SUSPENDED`, `CLOSED`, `CANCELLED`.
  - active-contract amendment/versioning is out-of-scope for PR-16.
- Header totals source-of-truth freeze:
  - `contracts.total_amount_txn` and `contracts.total_amount_base` are system-derived from active `contract_lines`.
  - manual header total overrides are not allowed in PR-16.
- Contract header date validation freeze:
  - `startDate` is required for contract write flows (`POST` and `PUT`).
  - `endDate` is optional.
  - when `endDate` exists, enforce `startDate <= endDate`.
- Contract-line status I/O freeze:
  - `POST /contracts` and `PUT /contracts/{id}` accept optional `line.status` (`ACTIVE` | `INACTIVE`).
  - omitted `line.status` is treated as `ACTIVE`.
  - `GET /contracts/{id}` returns all lines with explicit `status` (not only active lines).
  - header totals are computed from `status='ACTIVE'` lines only.
- Contract-line numbering freeze:
  - `lineNo` is backend-assigned in PR-16 and not client-authoritative.
  - persisted `lineNo` is generated sequentially from write payload order (`1..N`) on create and full-replace update.
  - `GET /contracts/{id}` returns lines with persisted `lineNo` values.
- Contract-line amount sign/zero freeze:
  - `lineAmountTxn` and `lineAmountBase` must be strictly `> 0` in PR-16 write flows.
  - zero or negative line amounts must be rejected by validator/service.
  - discount/adjustment modeling through negative contract lines is out-of-scope for PR-16.
- Contract-line recognition date validation freeze:
  - `STRAIGHT_LINE` requires both `recognitionStartDate` and `recognitionEndDate`.
  - `MILESTONE`/`MANUAL` allow null dates, but partial single-date payload is not allowed.
  - if both dates are present, enforce `recognitionStartDate <= recognitionEndDate`.
  - advanced milestone/manual schedule semantics remain out-of-scope for PR-16.
- Link amount policy freeze:
  - partial linking is allowed.
  - cumulative linked amount per document must not exceed document amount
    (`linked_amount_txn` <= `cari_documents.amount_txn` and
    `linked_amount_base` <= `cari_documents.amount_base`).
  - PR-16 tuple rule (Option A):
    - one link action per (`tenant_id`, `contract_id`, `cari_document_id`, `link_type`) tuple.
    - linked amount must be final for that tuple in PR-16 (no same-tuple top-up action).
  - cap validation must be transaction-safe:
    - lock source document row (`cari_documents`) with `FOR UPDATE` (primary serialization guard).
    - lock existing link rows for the same document using row-query `FOR UPDATE` (not aggregate-only locking).
    - compute/read current linked totals under lock.
    - validate cap, insert link, and commit in one DB transaction.
- Auditability freeze:
  - no silent link-row edits in PR-16.
  - corrections should be explicit (future unlink/adjustment action), with audit logging.
- Link immutability/uniqueness consistency freeze:
  - keep one immutable row per (`tenant_id`, `contract_id`, `cari_document_id`, `link_type`) in PR-16.
  - keep `uk_contract_doc_link` as-is in PR-16.
  - append-style correction rows for same tuple are out-of-scope for PR-16 and belong to future explicit unlink/adjustment PR.
- Link-document request payload schema freeze:
  - request fields: `cariDocumentId`, `linkType`, `linkedAmountTxn`, `linkedAmountBase`.
  - `notes` is out-of-scope for PR-16 link-document request payload.
  - validation:
    - `cariDocumentId` must be positive integer.
    - `linkType` must be one of `BILLING`, `ADVANCE`, `ADJUSTMENT`.
    - `linkedAmountTxn` and `linkedAmountBase` must be positive and non-zero.
    - decimal precision/scale must conform to PR-16 amount columns (max 6 fractional digits).
- Contract-documents read payload shape freeze:
  - `GET /contracts/{id}/documents` returns deterministic link rows with minimal linked-document summary.
  - link row fields: `linkType`, `linkedAmountTxn`, `linkedAmountBase`, `createdAt`, `createdByUserId`.
  - linked-document summary fields: `cariDocumentId`, `documentNo`, `direction`, `status`, `documentDate`, `amountTxn`, `amountBase`.
  - default ordering is `createdAt DESC, id DESC`.
  - no full cari document line/detail payload on this endpoint in PR-16.
- Mutation response shape freeze:
  - `POST /contracts`, `PUT /contracts/{id}`, and lifecycle mutations
    (`activate`/`suspend`/`close`/`cancel`) return updated contract header summary.
  - contract mutation responses are summary-scoped (no nested `lines[]`).
  - `POST /contracts/{id}/link-document` returns created link row + minimal linked-document summary
    with the same row shape used by `GET /contracts/{id}/documents`.
- Creator-user audit FK freeze (mandatory for PR-16):
  - precondition: `users` must expose tenant-composite unique key `(tenant_id, id)` (repo baseline: `uk_users_tenant_id_id`).
  - add tenant-safe composite FK:
    `contracts(tenant_id, created_by_user_id) -> users(tenant_id, id)`.
  - add tenant-safe composite FK:
    `contract_document_links(tenant_id, created_by_user_id) -> users(tenant_id, id)`.
- Contract lines API contract freeze:
  - lines are accepted only as nested `lines[]` in `POST /contracts` and `PUT /contracts/{id}` for PR-16.
  - `PUT /contracts/{id}` replaces line set atomically (no partial line patch semantics in PR-16).
  - contract + lines write path is transaction-bound; line account validations execute in same transaction.
- Scope boundary:
  - PR-16 is contracts foundation + lifecycle + link-document only.
  - periodization/deferred/accrual logic is out-of-scope and belongs to PR-17B/17C/17D.

### PR-16 Helper Reuse Guard (mandatory)

- Reuse existing backend helpers instead of introducing PR-specific parallel utilities.
- Account guard reuse:
  - use `assertAccountBelongsToTenant(...)` as the first-step guard for contract-line account checks (tenant-safe account + COA context source-of-truth).
  - helper return is not sufficient alone; still run explicit checks for `account_type`, `is_active`, `allow_posting`, and legal-entity chart compatibility.
- Transaction wrapper reuse:
  - use `withTransaction(...)` for contract create/update (header+lines), lifecycle-sensitive writes, and link-document cap validation flows.
- Amount parsing reuse:
  - use shared `parseAmount(...)` helper for PR-16 amount parsing and 6-decimal discipline.
  - `parseRequiredAmount(...)` is not a shared global helper yet (currently local in Cari validator). Avoid creating PR-specific duplicates.
  - preferred Option A: extract `parseRequiredAmount(...)` into a shared validator helper module (for example alongside `parseAmount(...)`) and reuse it.
  - if extraction is deferred, call `parseAmount(..., { required: true, allowZero: false })` directly (or via a thin local wrapper) without duplicating parsing logic.
- Route RBAC reuse:
  - follow the existing route-level RBAC middleware pattern from current route files (no ad-hoc permission checks in service layer).

### Checklist

- [ ] Add migration and wire `m020` in `backend/src/migrations/index.js`.
- [ ] Add contracts route/validator/service split.
- [ ] Add index mount in `backend/src/index.js`:
  - [ ] `app.use("/api/v1/contracts", requireAuth, contractsRoutes);`
- [ ] Add permissions in `seedCore` and role mapping.
  - [ ] `contract.read`
  - [ ] `contract.upsert`
  - [ ] `contract.activate`
  - [ ] `contract.suspend`
  - [ ] `contract.close`
  - [ ] `contract.cancel`
  - [ ] `contract.link_document`
- [ ] Add OpenAPI route docs in generator.
- [ ] Reuse existing backend helpers in PR-16 (no parallel helper drift):
  - [ ] `assertAccountBelongsToTenant(...)`
  - [ ] `withTransaction(...)`
  - [ ] shared `parseAmount(...)`
  - [ ] `parseRequiredAmount(...)` is shared-extracted (preferred) or PR code uses `parseAmount(..., { required: true, allowZero: false })` without duplicating amount-parse logic
  - [ ] existing route RBAC middleware pattern
- [ ] Enforce PR-16 endpoint-permission mapping in routes/OpenAPI:
  - [ ] `GET /contracts` -> `contract.read`
  - [ ] `GET /contracts/{id}` -> `contract.read`
  - [ ] `POST /contracts` -> `contract.upsert`
  - [ ] `PUT /contracts/{id}` -> `contract.upsert`
  - [ ] `POST /contracts/{id}/activate` -> `contract.activate`
  - [ ] `POST /contracts/{id}/suspend` -> `contract.suspend`
  - [ ] `POST /contracts/{id}/close` -> `contract.close`
  - [ ] `POST /contracts/{id}/cancel` -> `contract.cancel`
  - [ ] `POST /contracts/{id}/link-document` -> `contract.link_document`
  - [ ] `GET /contracts/{id}/documents` -> `contract.read`
- [ ] Add lifecycle routes for status completeness:
  - [ ] `POST /contracts/{id}/suspend`
  - [ ] `POST /contracts/{id}/cancel`
- [ ] Freeze contract lines API contract in route validators + OpenAPI:
  - [ ] `POST /contracts` accepts nested `lines[]`
  - [ ] `PUT /contracts/{id}` accepts nested `lines[]` with full-replace semantics
  - [ ] freeze contract header date validation:
    - [ ] `startDate` required on `POST` and `PUT`
    - [ ] `endDate` optional
    - [ ] if `endDate` exists => enforce `startDate <= endDate`
  - [ ] `lineNo` is backend-assigned from payload order (`1..N`) and not client-authoritative
  - [ ] `line.status` optional in write payload (`ACTIVE`/`INACTIVE`); omitted defaults to `ACTIVE`
  - [ ] `lineAmountTxn` and `lineAmountBase` are required positive non-zero values (`> 0`)
  - [ ] reject zero/negative line amounts in PR-16
  - [ ] freeze recognition-method/date validation:
    - [ ] `STRAIGHT_LINE` => `recognitionStartDate` + `recognitionEndDate` required
    - [ ] `MILESTONE`/`MANUAL` => dates optional, but no partial single-date payload
    - [ ] when both dates present => enforce `recognitionStartDate <= recognitionEndDate`
  - [ ] no separate `/contracts/{id}/lines*` endpoints in PR-16
- [ ] Freeze contract read payload shape in route/OpenAPI:
  - [ ] `GET /contracts/{id}` returns header + all `lines[]` (`ACTIVE` + `INACTIVE`) with `status` and `lineNo`
  - [ ] `GET /contracts/{id}` line array ordering is deterministic (`lineNo ASC`)
  - [ ] `GET /contracts` returns summary rows only (no nested `lines[]`; optional `lineCount` allowed)
  - [ ] `lineCount` (if included) means total persisted lines (`ACTIVE` + `INACTIVE`)
  - [ ] default contract list sort is `updatedAt DESC, id DESC`
- [ ] Freeze contract-documents read payload shape in route/OpenAPI:
  - [ ] `GET /contracts/{id}/documents` returns deterministic link rows with:
    - [ ] link fields: `linkType`, `linkedAmountTxn`, `linkedAmountBase`, `createdAt`, `createdByUserId`
    - [ ] minimal document summary: `cariDocumentId`, `documentNo`, `direction`, `status`, `documentDate`, `amountTxn`, `amountBase`
  - [ ] default documents list sort is `createdAt DESC, id DESC`
  - [ ] keep endpoint summary-only (no full document detail/line payload)
- [ ] Freeze mutation response shapes in route/OpenAPI:
  - [ ] `POST /contracts`, `PUT /contracts/{id}`, and lifecycle endpoints
    (`activate`/`suspend`/`close`/`cancel`) return updated contract header summary
  - [ ] contract mutation responses remain summary-scoped (no nested `lines[]`)
  - [ ] `POST /contracts/{id}/link-document` returns created link row + minimal document summary
    with the same row shape as `GET /contracts/{id}/documents`
- [ ] Add composite unique keys/FKs on new tables in migration (tenant/entity-safe).
- [ ] Match `created_by_user_id` to `users.id` exact SQL type (including signedness/width), not just logical `INT`.
- [ ] Keep `created_by_user_id` SQL type exactly equal to `users.id` in current repo (`INT`, signed).
- [ ] Enforce creator-user audit FKs (mandatory for PR-16):
  - [ ] confirm `users` has tenant-composite unique key `(tenant_id, id)` (repo baseline `uk_users_tenant_id_id`)
  - [ ] `contracts(tenant_id, created_by_user_id) -> users(tenant_id, id)` FK
  - [ ] `contract_document_links(tenant_id, created_by_user_id) -> users(tenant_id, id)` FK
- [ ] Add DB existence FKs for line account references:
  - [ ] `contract_lines.deferred_account_id -> accounts(id)`
  - [ ] `contract_lines.revenue_account_id -> accounts(id)`
- [ ] Enforce contract-line account mapping safety:
  - [ ] validate `deferred_account_id` / `revenue_account_id` with tenant-safe account guard
  - [ ] enforce legal-entity compatibility via `account -> coa`
  - [ ] enforce `is_active=true` and `allow_posting=true`
  - [ ] enforce account-type matrix:
    - [ ] CUSTOMER -> deferred `LIABILITY`, revenue `REVENUE`
    - [ ] VENDOR -> deferred `ASSET`, revenue `EXPENSE`
- [ ] Enforce strict scope checks in link service:
  - [ ] linked `cari_document` must match contract `tenant_id`
  - [ ] linked `cari_document` must match contract `legal_entity_id`
- [ ] Enforce link currency compatibility in service:
  - [ ] `contract.currency_code` must equal linked `cari_document.currency_code`
  - [ ] reject cross-currency link attempts in PR-16
- [ ] Enforce `contract_type` vs document direction compatibility:
  - [ ] CUSTOMER -> AR only
  - [ ] VENDOR -> AP only
- [ ] Enforce contract vs counterparty role compatibility:
  - [ ] CUSTOMER contract requires `counterparty.is_customer=true`
  - [ ] VENDOR contract requires `counterparty.is_vendor=true`
- [ ] Enforce link eligibility rules:
  - [ ] only posted-family statuses can be linked (`POSTED`/`PARTIALLY_SETTLED`/`SETTLED`)
  - [ ] reject `DRAFT`/`CANCELLED`/`REVERSED` documents
- [ ] Enforce contract status rules for linking:
  - [ ] allow only `DRAFT`/`ACTIVE`
  - [ ] reject `SUSPENDED`/`CLOSED`/`CANCELLED`
- [ ] Enforce lifecycle transition matrix and terminal states through dedicated endpoints.
- [ ] Enforce contract editability by status:
  - [ ] allow `PUT /contracts/{id}` only when status is `DRAFT`
  - [ ] reject `PUT` when status is `ACTIVE`/`SUSPENDED`/`CLOSED`/`CANCELLED`
- [ ] Enforce header totals as system-derived from active lines:
  - [ ] recalculate `total_amount_txn/base` from active `contract_lines` during create/update
  - [ ] reject or ignore manual header total drift in payload
- [ ] Freeze and validate link-document request payload schema:
  - [ ] required fields: `cariDocumentId`, `linkType`, `linkedAmountTxn`, `linkedAmountBase`
  - [ ] enforce positive non-zero amounts and max 6 fractional digits
  - [ ] keep `notes` out-of-scope for PR-16 link payload
- [ ] Enforce atomic contract+line write behavior:
  - [ ] line validations and account guards execute in same transaction as contract save
  - [ ] `PUT /contracts/{id}` line replacement is atomic (all-or-nothing)
- [ ] Enforce partial-link cap:
  - [ ] cumulative linked amounts cannot exceed source document amounts
    (`linked_amount_txn` <= `cari_documents.amount_txn`,
    `linked_amount_base` <= `cari_documents.amount_base`)
  - [ ] enforce cap check inside one DB transaction with locking:
    - [ ] lock source `cari_documents` row under `FOR UPDATE`
    - [ ] lock existing link rows for the same document with row-query `FOR UPDATE` (not aggregate-only lock path)
    - [ ] compute/read current linked totals + cap reference rows under lock
    - [ ] validate + insert link within same transaction
    - [ ] commit atomically (rollback on validation failure)
    - [ ] source document row lock is the primary serialization guard; link-row locks protect existing-row updates/reads
- [ ] Keep `contract_lines` normalized (no `legal_entity_id` denormalization in PR-16 migration).
- [ ] Keep `contract_document_links` with `legal_entity_id` for DB-level entity safety.
- [ ] Keep link rows immutable in PR-16 (no silent update flow).
- [ ] Keep link immutability + uniqueness model internally consistent in PR-16:
  - [ ] keep one immutable row per (`tenant_id`,`contract_id`,`cari_document_id`,`link_type`)
  - [ ] keep `uk_contract_doc_link` unique key unchanged
  - [ ] enforce Option A tuple behavior: no second same-tuple insert and no same-tuple top-up update in PR-16
  - [ ] defer append-style adjustments to future explicit unlink/adjustment PR
- [ ] Keep PR-16 free of periodization/deferred/accrual posting logic.
- [ ] Add PR-16 integration test + package script.

### Acceptance

- Contracts CRUD and lifecycle stable.
- Contract-document links are tenant-safe and scope-safe.
- Contract-document linking enforces type/direction compatibility (`CUSTOMER/AR`, `VENDOR/AP`).
- Contract-document linking enforces currency compatibility (`contracts.currency_code == cari_documents.currency_code` in PR-16).
- Contract type enforces counterparty role compatibility (`CUSTOMER -> is_customer`, `VENDOR -> is_vendor`).
- Contract linking enforces posted-only + contract-status eligibility rules.
- Contract-line account references are tenant/entity safe, postable/active, and type-compatible.
- Contract status enum and lifecycle endpoints are fully aligned (`activate/suspend/close/cancel`).
- Contract editability is status-safe (`PUT` allowed only in `DRAFT`; non-draft updates rejected).
- Header totals are system-derived from active lines (no manual header total drift).
- Contract header date semantics are explicit and deterministic:
  - `startDate` is required on `POST`/`PUT`
  - `endDate` is optional
  - provided header dates must satisfy `startDate <= endDate`
- Line status semantics are explicit and deterministic:
  - write payload accepts optional `line.status` (`ACTIVE`/`INACTIVE`, default `ACTIVE`)
  - detail read returns all lines with status
  - totals include only active lines
- Line numbering semantics are explicit and deterministic:
  - backend assigns `lineNo` from write payload order (`1..N`)
  - detail read returns persisted `lineNo` and deterministic line order
- Contract-line amount semantics are explicit and deterministic:
  - `lineAmountTxn` and `lineAmountBase` are positive non-zero in PR-16
  - zero/negative line amounts are rejected (negative adjustment lines out-of-scope)
- Contract-line recognition date semantics are explicit and deterministic:
  - `STRAIGHT_LINE` requires start/end dates
  - `MILESTONE`/`MANUAL` allow null dates but reject partial single-date payloads
  - provided date ranges must satisfy `start <= end`
- Contract lines API contract is explicit and deterministic (nested `lines[]` on create/update, atomic full-replace on `PUT`).
- Contract read payload shape is deterministic (`GET /contracts/{id}` includes `lines[]`; `GET /contracts` is summary-only).
- Contract list semantics are deterministic:
  - `lineCount` on list rows means total persisted lines (`ACTIVE` + `INACTIVE`)
  - default list order is `updatedAt DESC, id DESC`
- Contract-documents read payload shape is deterministic:
  - `GET /contracts/{id}/documents` returns link rows + minimal document summary fields only
  - default documents list order is `createdAt DESC, id DESC`
  - response shape is stable for PR-18 UI consumption (no endpoint-level guessing)
- Mutation response shapes are deterministic for PR-16 UI flows:
  - contract mutation endpoints (`POST`/`PUT`/lifecycle) return updated header summary without `lines[]`
  - link-document mutation returns created row aligned to `GET /contracts/{id}/documents` row shape
- Contract and line validation/persistence are atomic in one transaction.
- Link-document request payload schema is explicit and validated (required fields, positive non-zero amounts, max 6 decimals).
- Creator-user audit references are enforced by tenant-safe composite DB FKs
  (`contracts(tenant_id, created_by_user_id)` and
  `contract_document_links(tenant_id, created_by_user_id)` -> `users(tenant_id, id)`).
- DB-level FKs/composite keys prevent cross-tenant/cross-entity link corruption.
- Partial linking works with capped totals per document under PR-16 Option A (single final link action per tuple, no same-tuple top-up).
- Capped linking remains correct under concurrency (no race-based cap overshoot).
- Link immutability policy is unambiguous and consistent with DB uniqueness constraints.
- Contracts foundation remains isolated from periodization engine concerns.
- Existing backend helper discipline is preserved (reuse of account guard / transaction wrapper / amount parsing / RBAC middleware pattern; no parallel utility drift).
- No regressions in Cari endpoints/tests.

### Global Guardrails Check (Mandatory)

- [ ] Section 1) Global Guardrails maddeleri bu PR icin tek tek dogrulandi.
- [ ] ADR-frozen kurallar korunuyor (docs/adr/adr-cari-v1.md).
- [ ] Tenant/legal-entity scope ve RBAC kontrolleri korunuyor.
- [ ] Route -> validator -> service ayrimi korunuyor.
- [ ] Endpoint kontrati degistiyse OpenAPI generator guncellendi ve cikti uretildi.
- [ ] Bu PR testi + mevcut regresyon testleri yesil.

### Canonical Route/Permission/Data Model Mapping (PR-16)

- Section `2.3` namespace coverage:
  - `/api/v1/contracts/*`
- Section `2.4` permission alignment:
  - `contract.read`, `contract.upsert`, `contract.activate`, `contract.suspend`,
    `contract.close`, `contract.cancel`, `contract.link_document`
  - Endpoint mapping:
    - `/contracts` (GET) -> `contract.read`
    - `/contracts/{contractId}` (GET) -> `contract.read`
    - `/contracts` (POST) -> `contract.upsert`
    - `/contracts/{contractId}` (PUT) -> `contract.upsert`
    - `/contracts/{contractId}/activate` -> `contract.activate`
    - `/contracts/{contractId}/suspend` -> `contract.suspend`
    - `/contracts/{contractId}/close` -> `contract.close`
    - `/contracts/{contractId}/cancel` -> `contract.cancel`
    - `/contracts/{contractId}/link-document` -> `contract.link_document`
    - `/contracts/{contractId}/documents` (GET) -> `contract.read`
- Section `3` data model alignment:
  - existing Cari tables stay unchanged semantically.
  - `contract_document_links` includes `legal_entity_id` and references both `contracts` and `cari_documents`
    with composite tenant/entity-safe FKs.
  - `contract_lines` stays normalized through `contracts` in PR-16 (no premature denormalization).

---

## 10) PR-17 Split: Deferred + Accrual Periodization Engine (18x/28x/38x/48x)

### Why split PR-17 into 17A/17B/17C/17D

- Reviewability: each accounting family/lifecycle is independently reviewable.
- Rollback safety: a bad phase can be reverted without rolling back full engine scope.
- Cleaner test gates: each PR has its own deterministic pass/fail boundary.
- Lower regression risk on existing accounting behavior.

### Shared accounting semantics (apply to PR-17A..17D)

- Namespace: `/api/v1/revenue-recognition/*`
- Classification fields in schedule/run/subledger rows:
  - `maturity_bucket` (`SHORT_TERM` | `LONG_TERM`)
  - `maturity_date`
  - `reclass_required`
  - `account_family` (`DEFREV` | `ACCRUED_REVENUE` | `ACCRUED_EXPENSE` | `PREPAID_EXPENSE`)
- Naming freeze:
  - use neutral `maturity_bucket` across all PR-17 schemas/contracts (assets + liabilities)
  - `liability_bucket` is retired before schema freeze to avoid semantic drift
- Initial bucket split by maturity:
  - <= 12 months -> short-term (`180/181/380/381`)
  - > 12 months -> long-term (`280/281/480/481`)
- Mandatory long->short reclass:
  - `280 -> 180`, `281 -> 181`, `480 -> 380`, `481 -> 381`
- GL must reconcile to subledger by tenant/legal-entity/period/currency.
- Consolidation reports must show short/long balances separately by family (no default netting).

### Shared accounting mapping (journal_purpose_accounts)

- `PREPAID_EXP_SHORT_ASSET` (180)
- `PREPAID_EXP_LONG_ASSET` (280)
- `ACCR_REV_SHORT_ASSET` (181)
- `ACCR_REV_LONG_ASSET` (281)
- `DEFREV_SHORT_LIABILITY` (380)
- `DEFREV_LONG_LIABILITY` (480)
- `ACCR_EXP_SHORT_LIABILITY` (381)
- `ACCR_EXP_LONG_LIABILITY` (481)
- `DEFREV_REVENUE`
- `DEFREV_RECLASS`
- `PREPAID_EXPENSE`
- `PREPAID_RECLASS`
- `ACCR_REV_REVENUE`
- `ACCR_REV_RECLASS`
- `ACCR_EXP_EXPENSE`
- `ACCR_EXP_RECLASS`

### Shared posting setup guard (mandatory across PR-17B/17C)

- Posting must hard-fail if required `journal_purpose_accounts` mappings are missing.
- Behavior must stay consistent with existing Cari posting services:
  - resolve required purpose codes by tenant + legal entity
  - return explicit setup-required validation when mapping is absent
- Do not silently fallback to arbitrary accounts.

### Shared permissions (seed + RBAC)

- `revenue.schedule.read`
- `revenue.schedule.generate`
- `revenue.run.read`
- `revenue.run.create`
- `revenue.run.post`
- `revenue.run.reverse`
- `revenue.report.read`

### PR-17 Core Endpoint Permission Mapping (explicit)

- `GET /api/v1/revenue-recognition/schedules` -> `revenue.schedule.read`
- `GET /api/v1/revenue-recognition/runs` -> `revenue.run.read`
- `GET /api/v1/revenue-recognition/reports/*` -> `revenue.report.read`
- `POST /api/v1/revenue-recognition/schedules/generate` -> `revenue.schedule.generate`
- `POST /api/v1/revenue-recognition/runs` -> `revenue.run.create`
- `POST /api/v1/revenue-recognition/runs/{runId}/post` -> `revenue.run.post`
- `POST /api/v1/revenue-recognition/runs/{runId}/reverse` -> `revenue.run.reverse`
- This core mapping is shared for PR-17B/17D and consumed by PR-18 UI action guards.

### 10.1 PR-17A: Foundation (No posting)

Goal:

- Build schema, permissions, route/validator/service skeletons, and OpenAPI base.
- Do not implement posting/reversal/accrual settlement logic in this PR.

Files to create:

- `backend/src/migrations/m021_revenue_recognition_schedules.js`
- `backend/src/routes/revenue-recognition.js`
- `backend/src/routes/revenue-recognition.validators.js`
- `backend/src/services/revenue-recognition.service.js`
- `backend/scripts/test-revenue-pr17a-foundation.js`

Files to update:

- `backend/src/migrations/index.js`
- `backend/src/index.js`
- `backend/src/seedCore.js`
- `backend/scripts/generate-openapi.js`
- `backend/package.json`

Scope checklist:

- [ ] Create base tables:
  - [ ] `revenue_recognition_schedules`
  - [ ] `revenue_recognition_schedule_lines`
  - [ ] `revenue_recognition_runs`
  - [ ] `revenue_recognition_run_lines`
  - [ ] `revenue_recognition_subledger_entries`
- [ ] Enforce tenant/legal-entity DB discipline in foundation schema (PR-16 style):
  - [ ] add composite unique keys required for tenant-safe FK targets on new PR-17 tables
  - [ ] add composite FKs (`tenant_id`, `legal_entity_id`, `...`) where references are entity-scoped
  - [ ] ensure cross-tenant/cross-entity references fail at DB level, not only service logic
- [ ] Add deterministic source identity fields + unique keys for rerun idempotency.
- [ ] Add run status model (`DRAFT`, `READY`, `POSTED`, `REVERSED` at minimum).
- [ ] Add reversal linkage columns (`reversal_of_*`) in run/run-line schema.
- [ ] Add FX snapshot columns (`currency_code`, `fx_rate`, txn/base amounts).
- [ ] Add posted journal reference columns for auditable posting linkage.
- [ ] Mount namespace route:
  - [ ] `app.use("/api/v1/revenue-recognition", requireAuth, revenueRecognitionRoutes);`
- [ ] Add permissions in seed + role mappings.
- [ ] Add OpenAPI tags/routes for foundation endpoints.
- [ ] Freeze and document core endpoint-permission mapping in routes + OpenAPI (schedules/runs/reports read+mutate endpoints).
- [ ] Define canonical PR-17 purpose-code set and OpenAPI docs for operator setup expectations.
- [ ] Add test script `test:revenue-pr17a`.
- [ ] Assert no posting/reversal side effects are active in PR-17A.

Acceptance:

- Schema and permission foundation is ready.
- Namespace and validator/service skeletons are in place.
- No accounting posting behavior has been introduced yet.

### 10.2 PR-17B: DEFREV + PREPAID (380/480, 180/280)

Goal:

- Implement posting/reversal/reclass for deferred revenue and prepaid expense families.

Primary endpoints activated in this PR:

- `POST /api/v1/revenue-recognition/schedules/generate`
- `GET /api/v1/revenue-recognition/schedules`
- `POST /api/v1/revenue-recognition/runs`
- `GET /api/v1/revenue-recognition/runs`
- `POST /api/v1/revenue-recognition/runs/{runId}/post`
- `POST /api/v1/revenue-recognition/runs/{runId}/reverse`

Scope checklist:

- [ ] Implement DEFREV family flow (`380/480`) with recognition posting.
- [ ] Implement PREPAID family flow (`180/280`) with amortization posting.
- [ ] Implement long->short reclass for `280->180` and `480->380`.
- [ ] Add duplicate-line guard for reruns (same source should not create duplicate open lines).
- [ ] Enforce posting setup guard:
  - [ ] load required purpose-account mappings by tenant/legal-entity
  - [ ] fail with explicit setup-required error if any required mapping is missing
- [ ] Enforce period-open validation before post/reverse actions:
  - [ ] `runs/{runId}/post` must fail when target period is not `OPEN`
  - [ ] `runs/{runId}/reverse` must fail when target period is not `OPEN`
- [ ] Enforce run status-transition guards:
  - [ ] prevent double-post (already `POSTED` cannot be posted again)
  - [ ] reverse allowed only from `POSTED` state
  - [ ] prevent reverse of non-posted/already-reversed runs
- [ ] Keep explicit original-run linkage on reversals.
- [ ] Add test script `test:revenue-pr17b`.

Acceptance:

- DEFREV/PREPAID posting and reversal are balanced and auditable.
- Reclass behavior is deterministic.
- Reruns are idempotent at schedule/run-line level for this scope.
- Missing required purpose-account setup fails fast with explicit setup-required errors.
- Post/reverse actions enforce period-open checks and strict run status-transition guards.

### 10.3 PR-17C: Accruals (181/281, 381/481) + Settle/Reverse

Goal:

- Implement accrued revenue/expense generation and due-based settle/reverse lifecycle.

Primary endpoints activated in this PR:

- `POST /api/v1/revenue-recognition/accruals/generate`
- `POST /api/v1/revenue-recognition/accruals/{accrualId}/settle`
- `POST /api/v1/revenue-recognition/accruals/{accrualId}/reverse`

### PR-17C Endpoint Permission Mapping (explicit)

- `POST /api/v1/revenue-recognition/accruals/generate` -> `revenue.run.create`
- `POST /api/v1/revenue-recognition/accruals/{accrualId}/settle` -> `revenue.run.post`
- `POST /api/v1/revenue-recognition/accruals/{accrualId}/reverse` -> `revenue.run.reverse`
- PR-17C reuses `revenue.run.*` permissions for accrual lifecycle actions (no new `revenue.accrual.*` permissions in this phase).

Scope checklist:

- [ ] Implement ACCRUED_REVENUE (`181/281`) lifecycle.
- [ ] Implement ACCRUED_EXPENSE (`381/481`) lifecycle.
- [ ] Enforce due-based closure and reversal boundaries.
- [ ] Implement long->short reclass for `281->181` and `481->381`.
- [ ] Enforce posting setup guard for accrual posting/settlement/reversal paths (same rules as PR-17B).
- [ ] Enforce period-open validation for accrual posting actions:
  - [ ] `accruals/{accrualId}/settle` must fail when target period is not `OPEN`
  - [ ] `accruals/{accrualId}/reverse` must fail when target period is not `OPEN`
- [ ] Enforce accrual status-transition guards:
  - [ ] prevent double-settle on already settled/closed accruals
  - [ ] reverse allowed only from settled/posted accrual state
  - [ ] prevent reverse of non-settled/already-reversed accruals
- [ ] Enforce PR-17C endpoint-permission mapping exactly as documented (`generate -> revenue.run.create`, `settle -> revenue.run.post`, `reverse -> revenue.run.reverse`).
- [ ] Add test script `test:revenue-pr17c`.

Acceptance:

- Accrual generation/settlement/reversal behavior is deterministic and scoped correctly.
- Subledger and GL remain reconciled for accrual families.
- Missing required purpose-account setup fails fast with explicit setup-required errors.
- Settle/reverse actions enforce period-open checks and strict accrual status-transition guards.

### 10.4 PR-17D: Reports + Reconciliation + UI-facing Endpoint Polish

Goal:

- Finalize reporting surface and reconciliation guarantees for frontend consumption.

Primary endpoints activated/refined in this PR:

- `GET /api/v1/revenue-recognition/reports/future-year-rollforward`
- `GET /api/v1/revenue-recognition/reports/deferred-revenue-split`
- `GET /api/v1/revenue-recognition/reports/accrual-split`
- `GET /api/v1/revenue-recognition/reports/prepaid-expense-split`

Scope checklist:

- [ ] Add/finish rollforward and split reports with legal-entity/time filters.
- [ ] Add prepaid split report endpoint for PR-18 prepaid carry UI:
  - [ ] `GET /api/v1/revenue-recognition/reports/prepaid-expense-split`
  - [ ] expose short/long prepaid balances (`180/280`) with legal-entity/time filters
- [ ] Add reconciliation assertions between rollforward totals and posted GL movements.
- [ ] Add subledger-to-GL reconciliation checks per period/legal-entity/currency.
- [ ] Add query-shape/index checks for report queries (`EXPLAIN`).
- [ ] Add test script `test:revenue-pr17d`.

Acceptance:

- Reporting layer is stable, auditable, and frontend-ready.
- Consolidation consumers can use split balances without manual corrections.
- PR-17D report surface includes prepaid split data required by PR-18 (`/reports/prepaid-expense-split` for `180/280`).

### Global Guardrails Check (Mandatory for each PR-17x)

- [ ] Section 1) Global Guardrails maddeleri bu PR icin tek tek dogrulandi.
- [ ] ADR-frozen kurallar korunuyor (docs/adr/adr-cari-v1.md).
- [ ] Tenant/legal-entity scope ve RBAC kontrolleri korunuyor.
- [ ] Route -> validator -> service ayrimi korunuyor.
- [ ] Endpoint kontrati degistiyse OpenAPI generator guncellendi ve cikti uretildi.
- [ ] Bu PR testi + mevcut regresyon testleri yesil.

### Canonical Route/Permission/Data Model Mapping (PR-17A..17D)

- Section `2.3` namespace coverage:
  - `/api/v1/revenue-recognition/*`
- Permission alignment:
  - `revenue.schedule.read`, `revenue.schedule.generate`, `revenue.run.read`,
    `revenue.run.create`, `revenue.run.post`, `revenue.run.reverse`, `revenue.report.read`
  - Core endpoint mapping:
    - `/schedules` (GET) -> `revenue.schedule.read`
    - `/runs` (GET) -> `revenue.run.read`
    - `/reports/*` (GET) -> `revenue.report.read`
    - `/schedules/generate` (POST) -> `revenue.schedule.generate`
    - `/runs` (POST) -> `revenue.run.create`
    - `/runs/{runId}/post` (POST) -> `revenue.run.post`
    - `/runs/{runId}/reverse` (POST) -> `revenue.run.reverse`
  - PR-17C accrual endpoint mapping:
    - `/accruals/generate` -> `revenue.run.create`
    - `/accruals/{accrualId}/settle` -> `revenue.run.post`
    - `/accruals/{accrualId}/reverse` -> `revenue.run.reverse`
  - PR-17D report endpoint surface (all require `revenue.report.read`):
    - `/reports/future-year-rollforward`
    - `/reports/deferred-revenue-split`
    - `/reports/accrual-split`
    - `/reports/prepaid-expense-split`
- Section `3` data model alignment:
  - existing Cari tables remain semantically stable.
  - new revenue-recognition tables stay tenant/legal-entity safe.
  - periodization semantics remain explicit across DEFREV/ACCRUAL/PREPAID families.

---

## 11) PR-18: UI for Contracts + Periodization Split

### Goal

Convert both placeholder main-menu modules into real UI modules, with periodization split UX:

- Gelecek Aylar Gelirleri / Gelecek Yillar Gelirleri
- Gelir Tahakkuklari (kisa/uzun)
- Gider Tahakkuklari (kisa/uzun)
- Gelecek Aylara/Yillara Ait Giderler (prepaid carry)

### Product naming note (future-safe)

- Current route `/app/gelecek-yillar-gelirleri` can stay for backward compatibility.
- Since UI scope includes deferred + accrual + prepaid families, consider a broader product/module name later
  (for example `Donemsellik ve Tahakkuklar` / `Periodization & Accruals`).
- If renamed later, keep route aliases/redirects to avoid breaking bookmarks/integrations.

### Existing placeholder routes to convert

- `/app/contracts`
- `/app/gelecek-yillar-gelirleri`

### Files to create

- `frontend/src/api/contracts.js`
- `frontend/src/api/revenueRecognition.js`
- `frontend/src/pages/contracts/ContractsPage.jsx`
- `frontend/src/pages/contracts/contractsUtils.js`
- `frontend/src/pages/revenue/FutureYearRevenuePage.jsx`
- `frontend/src/pages/revenue/revenueRecognitionUtils.js`
- `frontend/src/pages/revenue/revenueFetchGating.js`
- `backend/scripts/test-contracts-revenue-pr18-frontend-smoke.js`
- `backend/scripts/test-contracts-revenue-pr18-fetch-gating.js`

### Files to update

- `frontend/src/App.jsx`
- `frontend/src/layouts/sidebarConfig.js`
- `frontend/src/i18n/messages.js`
- `backend/package.json`

### Route wiring

- `/app/contracts` -> `<ContractsPage />` (permission: `contract.read`)
- `/app/gelecek-yillar-gelirleri` -> `<FutureYearRevenuePage />` (permission any-of: `revenue.schedule.read`, `revenue.run.read`, `revenue.report.read`)

### PR-18 per-action UI permission mapping (mandatory)

- Contracts page route open:
  - `contract.read`
- Contracts page actions:
  - create/edit contract -> `contract.upsert`
  - activate contract -> `contract.activate`
  - suspend contract -> `contract.suspend`
  - close contract -> `contract.close`
  - cancel contract -> `contract.cancel`
  - link document submit action -> `contract.link_document`
  - counterparty picker read (when using Cari counterparties APIs) -> `cari.card.read`
  - line account picker read (when using GL accounts APIs) -> `gl.account.read`
  - link document picker read (when using Cari documents APIs) -> `cari.doc.read`
- Revenue page route open:
  - any of: `revenue.schedule.read`, `revenue.run.read`, `revenue.report.read`
- Revenue page actions:
  - schedule generate -> `revenue.schedule.generate`
  - run create -> `revenue.run.create`
  - run post -> `revenue.run.post`
  - run reverse -> `revenue.run.reverse`

### PR-18 Contracts Link-Picker Read-Dependency Rule (mandatory)

- If `ContractsPage` uses `/api/v1/cari/documents*` for link-document picker/search, picker fetch/render must be gated by `cari.doc.read`.
- Missing `cari.doc.read` must not cause background fetch noise; picker UI should be hidden/disabled with controlled fallback messaging.
- Optional alternative (future): provide a contract-scoped `linkable-documents` backend endpoint under contract permissions to remove direct `cari.doc.read` dependency.

### PR-18 Contracts Counterparty-Picker Read-Dependency Rule (mandatory)

- If `ContractsPage` uses `/api/v1/cari/counterparties*` for counterparty picker/search, picker fetch/render must be gated by `cari.card.read`.
- Missing `cari.card.read` must not cause background fetch noise; counterparty picker UI should be hidden/disabled with controlled fallback messaging.

### PR-18 Contracts Line Account-Picker Read-Dependency Rule (mandatory)

- If `ContractsPage` uses GL accounts APIs (for example `/api/v1/gl/accounts*`) for `deferredAccountId` / `revenueAccountId` selection, picker fetch/render must be gated by `gl.account.read`.
- Missing `gl.account.read` must not cause background fetch noise; account picker UI should be hidden/disabled with controlled fallback messaging.

### PR-18 Revenue Read-Dependency UI Rule (mandatory)

- Revenue page route can open with any-of `revenue.schedule.read`, `revenue.run.read`, `revenue.report.read`, but sub-sections must be permission-gated before fetch/render.
- Schedule list/widgets:
  - require `revenue.schedule.read`
  - if missing, do not call schedules read endpoints and hide/disable schedule-read UI blocks.
- Runs list/widgets:
  - require `revenue.run.read`
  - if missing, do not call runs read endpoints and hide/disable run-read UI blocks.
- Reports panel/widgets:
  - require `revenue.report.read`
  - if missing, do not call reports endpoints and hide/disable report UI blocks.
- UI must avoid unauthorized background reads; missing read permission should produce controlled UI fallback, not avoidable `403` noise.

### PR-18 Fetch-Gating Testability Rule (mandatory)

- Extract revenue read-fetch gating into small pure helper(s) (for example `revenueFetchGating.js`).
- `FutureYearRevenuePage` must consume helper outputs before any schedules/runs/reports fetch is executed.
- Extract contracts picker read-fetch gating into small pure helper(s) (for example `contractsPermissionGating.js` or pure helpers in `contractsUtils.js`).
- `ContractsPage` must consume helper outputs before any counterparty/account/document picker fetch is executed.
- Validate helper behavior with a dedicated test script covering permission combinations:
  - no read permissions
  - only `revenue.schedule.read`
  - only `revenue.run.read`
  - only `revenue.report.read`
  - mixed read permissions (`schedule.read`, `run.read`, `report.read`)
- Validate contracts picker gating helper behavior with dedicated assertions covering:
  - missing `cari.card.read` blocks counterparties picker fetch
  - missing `gl.account.read` blocks line account picker fetch
  - missing `cari.doc.read` blocks link-document picker fetch
  - matching read permission enables each picker fetch path
- Keep `test-contracts-revenue-pr18-frontend-smoke.js` focused on static assertions
  (`App.jsx` route wiring, `sidebarConfig.js`, `messages.js`), not runtime fetch suppression.

Guard source note (repo-aligned):

- Permission labels above are enforced via `sidebarConfig.js` `requiredPermissions`.
- In current `App.jsx`, `withPermissionGuard` reads permissions from sidebar map by `appPath`.
- Do not rely on a route object `permissions` field for PR-18.

### PR-18 Permission Semantics Freeze (mandatory)

- `requiredPermissions: [...]` arrays in route/sidebar guards are any-of (`OR`) by default.
- Route-level any-of is correct for `/app/gelecek-yillar-gelirleri` open policy.
- Do not combine unrelated action/picker dependencies into one plain permission array and assume `AND`.
- For action/picker gating, use separate booleans per dependency (recommended), or explicit `allOf` where true `AND` semantics are required.
- Example implementation shape:
  - edit panel visibility by `contract.upsert`
  - counterparty picker fetch/render by `cari.card.read`
  - line account picker fetch/render by `gl.account.read`
  - link-document picker fetch/render by `cari.doc.read`

### Placeholder-to-implemented conversion lock (mandatory)

- `frontend/src/App.jsx`
  - Replace placeholder elements with real components for:
    - `/app/contracts`
    - `/app/gelecek-yillar-gelirleri`
  - Ensure both are included in implemented route list/branch used by app shell
    (no fallback `ModulePlaceholderPage` for these two routes).
  - Repo-specific rule:
    - in current `App.jsx`, placeholder routing is derived from `implementedPaths` vs sidebar links.
    - therefore `sidebarConfig.js` `implemented: true` alone is not sufficient.
    - both routes must exist in `implementedRoutes` (so they are part of `implementedPaths`).
- `frontend/src/layouts/sidebarConfig.js`
  - `Contracts` menu item must include:
    - `requiredPermissions: ["contract.read"]`
    - `implemented: true`
  - `Gelecek Yillar Gelirleri` menu item must include:
    - `requiredPermissions: ["revenue.schedule.read", "revenue.run.read", "revenue.report.read"]` (any-of)
    - `implemented: true`
- `frontend/src/i18n/messages.js`
  - Add/verify `sidebar.byPath` keys for:
    - `/app/contracts`
    - `/app/gelecek-yillar-gelirleri`

### Checklist

- [ ] Implement Contracts list/create/edit/activate/suspend/close/cancel/link-document flow.
- [ ] Implement Contracts lifecycle action controls with transition guards:
  - [ ] activate
  - [ ] suspend
  - [ ] close
  - [ ] cancel
- [ ] Implement Periodization Split UI:
  - [ ] schedule generation trigger
  - [ ] run create/post/reverse
  - [ ] rollforward report filters + table/cards
  - [ ] split liability cards/tables: Gelecek Aylar Gelirleri vs Gelecek Yillar Gelirleri
  - [ ] tahakkuk cards/tables: Gelir Tahakkuklari (181/281) and Gider Tahakkuklari (381/481)
  - [ ] prepaid carry cards/tables: 180/280
  - [ ] reclass visibility: moved from long-term to short-term (period basis)
  - [ ] subledger/GL reconciliation summary panel
- [ ] Replace placeholders in `App.jsx` with real page components for both routes (implemented route list included).
- [ ] Verify both routes are in `implementedRoutes`/`implementedPaths` and not rendered via `placeholderRoutes`.
- [ ] Set sidebar permission/implementation flags:
  - [ ] `/app/contracts` -> `requiredPermissions: ["contract.read"]`, `implemented: true`
  - [ ] `/app/gelecek-yillar-gelirleri` -> `requiredPermissions: ["revenue.schedule.read", "revenue.run.read", "revenue.report.read"]` (any-of), `implemented: true`
- [ ] Keep guard model repo-aligned (no route object `permissions` dependency for these routes).
- [ ] Add/verify `messages.js` `sidebar.byPath` labels for both routes.
- [ ] Keep frontend API helper pattern consistent with PR-11:
  - [ ] use shared API error/query helpers (extend to generic `apiCommon` if adopted)
- [ ] Freeze permission semantics explicitly:
  - [ ] do not model `AND` dependencies with a plain `requiredPermissions` array
  - [ ] use separate dependency booleans or explicit `allOf` for true `AND` checks
- [ ] Add per-action permission checks (not only route-level).
  - [ ] Contracts action buttons/panels follow:
    - [ ] `contract.upsert`, `contract.activate`, `contract.suspend`, `contract.close`, `contract.cancel`, `contract.link_document`
    - [ ] counterparty picker/read calls to `/api/v1/cari/counterparties*` are gated by `cari.card.read`
    - [ ] line account picker/read calls to GL accounts APIs are gated by `gl.account.read`
    - [ ] link-document picker/read calls to `/api/v1/cari/documents*` are gated by `cari.doc.read`
  - [ ] Revenue action buttons/panels follow:
    - [ ] `revenue.schedule.generate`, `revenue.run.create`, `revenue.run.post`, `revenue.run.reverse`
- [ ] Enforce contracts counterparty-picker read-dependency gating:
  - [ ] if picker uses Cari counterparties APIs, require `cari.card.read` before fetch/render
  - [ ] do not perform unauthorized background calls to `/api/v1/cari/counterparties*`
  - [ ] provide controlled UI fallback when counterparty-picker read permission is missing
- [ ] Enforce contracts line account-picker read-dependency gating:
  - [ ] if picker uses GL accounts APIs, require `gl.account.read` before fetch/render
  - [ ] do not perform unauthorized background calls to GL account lookup endpoints
  - [ ] provide controlled UI fallback when account-picker read permission is missing
- [ ] Enforce contracts link-picker read-dependency gating:
  - [ ] if picker uses Cari documents APIs, require `cari.doc.read` before fetch/render
  - [ ] do not perform unauthorized background calls to `/api/v1/cari/documents*`
  - [ ] provide controlled UI fallback when picker read permission is missing
- [ ] Enforce revenue read-dependency UI gating before data fetch/render:
  - [ ] schedules sections fetch/render only with `revenue.schedule.read`
  - [ ] runs sections fetch/render only with `revenue.run.read`
  - [ ] reports sections fetch/render only with `revenue.report.read`
  - [ ] no unauthorized background fetches for hidden/disabled sections (avoid avoidable `403` responses)
- [ ] Extract revenue fetch gating into pure helper(s) and use helper decisions in page loaders before request dispatch.
- [ ] Extract contracts picker gating into pure helper(s) (`contractsPermissionGating.js` or `contractsUtils.js`) and use helper decisions before picker request dispatch.
- [ ] Add helper-focused test coverage for gating behavior across permission combinations (separate from static smoke checks).
- [ ] Update sidebar labels/translations.
- [ ] Add PR-18 frontend smoke script:
  - [ ] assert `App.jsx` uses real components (not placeholders) for both routes
  - [ ] assert both routes are present in implemented route branch (not only sidebar metadata)
  - [ ] assert `sidebarConfig.js` contains `requiredPermissions` and `implemented: true` for both routes
  - [ ] assert `messages.js` contains `sidebar.byPath` entries for both routes
  - [ ] keep this script static-surface only (no runtime fetch-suppression proof)
- [ ] Add PR-18 fetch-gating behavior test script:
  - [ ] assert helper outputs prevent schedules/runs/reports fetch attempts when matching read permission is missing
  - [ ] assert helper outputs allow fetch when matching read permission exists
  - [ ] assert contracts picker-gating helper outputs prevent counterparties/accounts/documents picker fetch attempts when matching read permission is missing
  - [ ] assert contracts picker-gating helper outputs allow picker fetch when matching read permission exists

### Acceptance

- Main menu modules are fully functional (not placeholders).
- Unauthorized users cannot see/use menu actions outside granted permissions.
- Contract and periodization flows reconcile with backend reports.
- Deferred revenue UI clearly separates short-term vs long-term balances and reclass movements.
- Tahakkuk and prepaid flows are visible with open/closed status and due-based closures.
- Revenue page section rendering/fetching is permission-aware (`schedule.read`, `run.read`, `report.read`) and avoids avoidable `403` fetch noise.
- Revenue route-level gate is flexible:
  - users with only `revenue.schedule.read` or only `revenue.run.read` can open the route.
  - users with only `revenue.report.read` can also open the route.
  - section fetch/render still stays permission-scoped (`schedule.read` / `run.read` / `report.read`).
- Permission semantics are implementation-safe:
  - route/sidebar `requiredPermissions` arrays are treated as any-of (`OR`)
  - action/picker dependencies that require `AND` semantics are enforced via separate checks or explicit `allOf`
- Counterparty picker UX is RBAC-consistent:
  - when picker uses Cari counterparties endpoints, `cari.card.read` is enforced before fetch/render
  - missing picker-read permission does not produce avoidable background `403` noise
- Contracts line account-picker UX is RBAC-consistent:
  - when picker uses GL account endpoints, `gl.account.read` is enforced before fetch/render
  - missing picker-read permission does not produce avoidable background `403` noise
- Link-document picker UX is RBAC-consistent:
  - when picker uses Cari document endpoints, `cari.doc.read` is enforced before fetch/render
  - missing picker-read permission does not produce avoidable background `403` noise
- Contracts picker fetch suppression is testable and tested via helper-level gating checks
  (counterparty/account/document picker fetch decisions are not hidden only in component conditionals).
- Revenue fetch suppression is testable and tested via dedicated helper-level gating tests
  (static smoke script remains route/sidebar/messages focused).
- No regression in Cari quality gate.

### Global Guardrails Check (Mandatory)

- [ ] Section 1) Global Guardrails maddeleri bu PR icin tek tek dogrulandi.
- [ ] ADR-frozen kurallar korunuyor (docs/adr/adr-cari-v1.md).
- [ ] Tenant/legal-entity scope ve RBAC kontrolleri korunuyor.
- [ ] Route -> validator -> service ayrimi korunuyor.
- [ ] Endpoint kontrati degistiyse OpenAPI generator guncellendi ve cikti uretildi.
- [ ] Bu PR testi + mevcut regresyon testleri yesil.

### Canonical Route/Permission/Data Model Mapping (PR-18)

- Frontend route coverage for new modules:
  - `/app/contracts`
  - `/app/gelecek-yillar-gelirleri`
- Section `2.3` backend namespace consumption:
  - `/api/v1/contracts/*`
  - `/api/v1/revenue-recognition/*`
  - prepaid split dependency for UI:
    - `/api/v1/revenue-recognition/reports/prepaid-expense-split`
- Permission alignment:
  - route-level:
    - `/app/contracts` -> `contract.read`
    - `/app/gelecek-yillar-gelirleri` -> any-of `revenue.schedule.read`, `revenue.run.read`, `revenue.report.read`
  - semantics:
    - route/sidebar `requiredPermissions` arrays are any-of (`OR`) in this repo
    - action/picker `AND` dependencies must be expressed explicitly (separate checks or `allOf`), not inferred from a plain array
  - action-level:
    - contracts: `contract.upsert`, `contract.activate`, `contract.suspend`, `contract.close`,
      `contract.cancel`, `contract.link_document`
    - revenue: `revenue.schedule.generate`, `revenue.run.create`, `revenue.run.post`, `revenue.run.reverse`
  - contracts picker read-dependency:
    - if Contracts counterparty picker consumes `/api/v1/cari/counterparties*`, require `cari.card.read`
    - if Contracts line account picker consumes GL accounts APIs, require `gl.account.read`
    - if Contracts link-document picker consumes `/api/v1/cari/documents*`, require `cari.doc.read`
  - section read-level (Revenue page):
    - schedules widgets/lists -> `revenue.schedule.read`
    - runs widgets/lists -> `revenue.run.read`
    - reports widgets/panels -> `revenue.report.read`
- Data model alignment:
  - UI payloads must preserve tenant/legal-entity boundaries and not change Cari v1 semantics.
  - periodization UI must honor `SHORT_TERM` / `LONG_TERM` bucket semantics from backend across all families.

---

## 12) PR-19: Counterparty AR/AP Account Mapping + Posting Resolution

### Goal

Introduce optional per-counterparty AR/AP control-account mapping for Cari flows without breaking current behavior.

### Why this PR

- Today Cari posting resolves control+offset accounts from `journal_purpose_accounts`.
- Some deployments need counterparty-level control account overrides (customer segment/vendor segment).
- PR-19 adds these overrides safely while keeping current fallback behavior.

### Scope

- Add nullable account mapping fields on `counterparties`:
  - `ar_account_id`
  - `ap_account_id`
- Extend existing counterparty create/update/read contracts.
- Update Cari document/settlement posting resolution:
  - control account: counterparty override if present, otherwise existing purpose mapping
  - offset account: continue from existing purpose mapping (no behavioral change)
- Keep backward compatibility for existing tenants and counterparties.

### Files to create

- `backend/src/migrations/m022_counterparty_account_mapping.js`
- `backend/scripts/test-cari-pr19-counterparty-account-mapping-and-posting-resolution.js`
- `backend/scripts/test-cari-pr19-frontend-counterparty-account-fields-smoke.js` (optional but recommended)

### Files to update

- `backend/src/migrations/index.js`
- `backend/src/routes/cari.counterparty.validators.js`
- `backend/src/services/cari.counterparty.service.js`
- `backend/src/services/cari.document.service.js`
- `backend/src/services/cari.settlement.service.js`
- `backend/scripts/generate-openapi.js`
- `backend/package.json`
- `frontend/src/api/cariCounterparty.js`
- `frontend/src/pages/cari/counterpartyFormUtils.js`
- `frontend/src/pages/cari/CariCounterpartyPage.jsx`
- `frontend/src/i18n/messages.js`

### API contract updates (existing endpoints)

- `POST /api/v1/cari/counterparties`
- `PUT /api/v1/cari/counterparties/{counterpartyId}`
- `GET /api/v1/cari/counterparties`
- `GET /api/v1/cari/counterparties/{counterpartyId}`

New/extended fields:

- `arAccountId` (optional)
- `apAccountId` (optional)

PUT null-clearing semantics (freeze):

- field omitted in `PUT /api/v1/cari/counterparties/{counterpartyId}` => keep existing mapping unchanged
- explicit `arAccountId: null` => clear AR mapping
- explicit `apAccountId: null` => clear AP mapping

Response enrichment (recommended):

- `arAccountCode`, `arAccountName`
- `apAccountCode`, `apAccountName`
- null behavior:
  - if `arAccountId` is null or unresolved, return `arAccountCode=null`, `arAccountName=null`
  - if `apAccountId` is null or unresolved, return `apAccountCode=null`, `apAccountName=null`
- query-shape freeze:
  - list/search/sort by enrichment fields (`arAccountCode`/`arAccountName`/`apAccountCode`/`apAccountName`) is out-of-scope for PR-19
  - avoid introducing SQL/index churn for enrichment-field sorting/filtering in this PR

### PR-19 Frontend AR/AP Account-Picker Read-Dependency Rule (mandatory)

- If counterparty create/update UI uses GL accounts APIs (for example `/api/v1/gl/accounts*`) for `arAccountId` / `apAccountId` picker/search, picker fetch/render must be gated by `gl.account.read`.
- Missing `gl.account.read` must not cause background fetch noise; AR/AP account selectors should be hidden/disabled with controlled fallback messaging.
- This is a frontend read-dependency rule; it does not introduce a new PR-19 permission code.

### Migration skeleton (`m022_counterparty_account_mapping.js`)

```js
// Follow m019 migration helper pattern.
// Do not use legacy ignoreDuplicate* placeholders.
const ignorableErrnos = new Set([
  1060, // duplicate column
  1061, // duplicate index/key
  1826, // duplicate constraint name
]);

async function safeExecute(connection, sql, params = []) {
  try {
    await connection.execute(sql, params);
  } catch (err) {
    if (ignorableErrnos.has(err?.errno)) {
      return;
    }
    throw err;
  }
}

async function hasColumn(connection, tableName, columnName) {
  const [rows] = await connection.execute(
    `SELECT 1
     FROM information_schema.columns
     WHERE table_schema = DATABASE()
       AND table_name = ?
       AND column_name = ?
     LIMIT 1`,
    [tableName, columnName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

async function hasIndex(connection, tableName, indexName) {
  const [rows] = await connection.execute(
    `SELECT 1
     FROM information_schema.statistics
     WHERE table_schema = DATABASE()
       AND table_name = ?
       AND index_name = ?
     LIMIT 1`,
    [tableName, indexName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

async function hasForeignKey(connection, tableName, constraintName) {
  const [rows] = await connection.execute(
    `SELECT 1
     FROM information_schema.table_constraints
     WHERE constraint_schema = DATABASE()
       AND table_name = ?
       AND constraint_type = 'FOREIGN KEY'
       AND constraint_name = ?
     LIMIT 1`,
    [tableName, constraintName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

const migration022CounterpartyAccountMapping = {
  key: "m022_counterparty_account_mapping",
  description: "Add per-counterparty AR/AP account mapping",
  async up(connection) {
    if (!(await hasColumn(connection, "counterparties", "ar_account_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD COLUMN ar_account_id BIGINT UNSIGNED NULL AFTER default_payment_term_id`
      );
    }

    if (!(await hasColumn(connection, "counterparties", "ap_account_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD COLUMN ap_account_id BIGINT UNSIGNED NULL AFTER ar_account_id`
      );
    }

    if (!(await hasIndex(connection, "counterparties", "ix_counterparties_ar_account"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD KEY ix_counterparties_ar_account (ar_account_id)`
      );
    }

    if (!(await hasIndex(connection, "counterparties", "ix_counterparties_ap_account"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD KEY ix_counterparties_ap_account (ap_account_id)`
      );
    }

    if (!(await hasForeignKey(connection, "counterparties", "fk_counterparties_ar_account"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD CONSTRAINT fk_counterparties_ar_account
         FOREIGN KEY (ar_account_id) REFERENCES accounts(id)`
      );
    }

    if (!(await hasForeignKey(connection, "counterparties", "fk_counterparties_ap_account"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD CONSTRAINT fk_counterparties_ap_account
         FOREIGN KEY (ap_account_id) REFERENCES accounts(id)`
      );
    }
  },
};

export default migration022CounterpartyAccountMapping;
```

### Domain correctness lock (mandatory)

- Account scope safety:
  - mapped account must belong to same tenant (tenant-safe guard).
  - mapped account must be legal-entity compatible with counterparty scope via `accounts -> charts_of_accounts`.
  - mapped account must use legal-entity scoped chart for Cari control usage
    (`charts_of_accounts.scope='LEGAL_ENTITY'` and
    `charts_of_accounts.legal_entity_id = counterparty.legal_entity_id`).
  - service-layer enforcement is mandatory (`assertAccountBelongsToTenant` + explicit chart/entity/type/postability checks), DB FK alone is not sufficient.
  - `assertAccountBelongsToTenant(...)` alone is not sufficient for PR-19 rules because helper output does not enforce `account_type`, `is_active`, or `allow_posting`.
- Posting suitability:
  - mapped account must be active and postable (`is_active=true`, `allow_posting=true`).
- Posting-time revalidation (mandatory):
  - mapped account suitability/scope/type checks must be re-validated at posting time
    (document post + settlement post), not only at counterparty save/update time.
  - if mapped account becomes inactive/non-postable/wrong-scope/wrong-type later, posting must fail with explicit validation.
- Type compatibility:
  - `arAccountId` must be `ASSET`.
  - `apAccountId` must be `LIABILITY`.
- Role compatibility:
  - if `isCustomer=false`, reject `arAccountId`.
  - if `isVendor=false`, reject `apAccountId`.
- Backward compatibility:
  - mappings are optional.
  - if missing, existing purpose-account path remains default.
- Null-clearing update semantics (mandatory):
  - omitted `arAccountId`/`apAccountId` in PUT means "no change"
  - explicit `null` means "clear mapping"
  - service/validator behavior must keep this distinction explicitly
- Offset resolution unchanged:
  - offsets continue from `journal_purpose_accounts` (`CARI_AR_OFFSET`, `CARI_AP_OFFSET`).
- Settlement behavior when `counterparty_id` is null:
  - skip counterparty override path entirely.
  - use purpose-account mapping path only.

### Posting resolution policy (explicit)

For AR document/settlement:

- if counterparty has `arAccountId`, use it as control account
- otherwise use `CARI_AR_CONTROL`
- offset remains `CARI_AR_OFFSET`

For AP document/settlement:

- if counterparty has `apAccountId`, use it as control account
- otherwise use `CARI_AP_CONTROL`
- offset remains `CARI_AP_OFFSET`

### PR-19 Helper Reuse Guard (mandatory)

- Reuse existing backend helpers instead of introducing PR-specific parallel utilities.
- Account guard reuse:
  - use `assertAccountBelongsToTenant(...)` as the first-step guard for counterparty AR/AP mapping validation and posting-time revalidation.
  - helper return is not sufficient alone; still run explicit checks for `account_type`, `is_active`, `allow_posting`, and legal-entity chart compatibility.
- Transaction wrapper reuse:
  - use `withTransaction(...)` for counterparty mapping writes and posting-time validation+posting flows where atomicity is required.
- Amount parsing reuse:
  - where touched posting flows parse amounts, reuse shared `parseAmount(...)` for 6-decimal discipline.
  - `parseRequiredAmount(...)` is not a shared global helper yet (currently local in Cari validator). Avoid introducing parallel PR-specific copies.
  - preferred Option A: extract `parseRequiredAmount(...)` into a shared validator helper module and reuse it.
  - if extraction is deferred, use `parseAmount(..., { required: true, allowZero: false })` path directly (or a thin wrapper) without duplicating parsing logic.
- Route RBAC reuse:
  - preserve existing route-level RBAC middleware pattern in Cari route files (no ad-hoc permission checks in services).

### Checklist

- [ ] Add migration and wire `m022` in `backend/src/migrations/index.js`.
- [ ] Follow m019-style migration guard pattern for `m022` (`safeExecute` + `hasColumn`/`hasIndex`/`hasForeignKey`), not `ignoreDuplicate*` placeholders.
- [ ] Extend counterparty validator/service contracts for `arAccountId` / `apAccountId`.
- [ ] Validate account scope + legal-entity compatibility via account guard (`accounts -> coa`).
- [ ] Validate account suitability (`is_active`, `allow_posting`).
- [ ] Validate account type compatibility (`ASSET` for AR, `LIABILITY` for AP).
- [ ] Validate role compatibility (`isCustomer` / `isVendor`).
- [ ] Freeze nullable mapping update semantics in validator/service:
  - [ ] omitted `arAccountId`/`apAccountId` => keep existing value
  - [ ] explicit `arAccountId: null` / `apAccountId: null` => clear value
- [ ] Enforce service-layer account guard pattern:
  - [ ] `assertAccountBelongsToTenant(...)` for mapped accounts
  - [ ] additional legal-entity/chart compatibility checks before persist/use
- [ ] Update posting account resolution in:
  - [ ] `cari.document.service.js`
  - [ ] `cari.settlement.service.js`
- [ ] Re-validate mapped control account at posting time (document + settlement):
  - [ ] tenant/legal-entity/chart compatibility still valid
  - [ ] `is_active=true` and `allow_posting=true` still valid
  - [ ] account type compatibility (`ASSET` for AR / `LIABILITY` for AP) still valid
  - [ ] fail posting explicitly if mapping became invalid after counterparty save
- [ ] Define and enforce settlement behavior when `counterparty_id` is null:
  - [ ] no override lookup
  - [ ] purpose-account mapping only
- [ ] Keep fallback behavior to `journal_purpose_accounts` when no mapping exists.
- [ ] Keep offset account resolution unchanged.
- [ ] Update OpenAPI generator + generated output.
- [ ] Reuse existing backend helpers in PR-19 (no parallel helper drift):
  - [ ] `assertAccountBelongsToTenant(...)`
  - [ ] `withTransaction(...)`
  - [ ] shared `parseAmount(...)` (for touched amount parsing paths)
  - [ ] `parseRequiredAmount(...)` is shared-extracted (preferred) or touched paths use `parseAmount(..., { required: true, allowZero: false })` without duplicated parse logic
  - [ ] existing route RBAC middleware pattern
- [ ] Enforce frontend AR/AP account-picker read-dependency gating:
  - [ ] if counterparty form uses GL accounts APIs, require `gl.account.read` before account-picker fetch/render
  - [ ] do not perform unauthorized background calls to `/api/v1/gl/accounts*`
  - [ ] provide controlled UI fallback when account-picker read permission is missing
- [ ] Freeze enrichment response null semantics:
  - [ ] unresolved/null AR mapping -> `arAccountCode=null`, `arAccountName=null`
  - [ ] unresolved/null AP mapping -> `apAccountCode=null`, `apAccountName=null`
- [ ] Keep enrichment-field search/sort/filter out-of-scope for PR-19 (no SQL/index expansion on enrichment fields).
- [ ] Add tests:
  - [ ] valid CUSTOMER + `arAccountId` in same tenant/entity -> success
  - [ ] valid VENDOR + `apAccountId` in same tenant/entity -> success
  - [ ] reject cross-tenant / wrong-entity mapping
  - [ ] reject wrong account type
  - [ ] reject inactive/non-postable mapped account
  - [ ] PUT omitted mapping fields keep existing DB values
  - [ ] PUT explicit `null` clears mapping fields
  - [ ] posting fails when previously mapped account becomes invalid after save (inactive/non-postable/wrong-scope/wrong-type)
  - [ ] read/list responses return null enrichment fields when mapping is null/unresolved
  - [ ] posting uses mapped control account when present
  - [ ] posting falls back correctly when mapping absent
- [ ] Add frontend smoke coverage for PR-19 counterparty AR/AP account fields (optional but recommended):
  - [ ] account picker fetch is suppressed when `gl.account.read` is missing
  - [ ] account picker fetch/render is enabled when `gl.account.read` exists
- [ ] Add script `test:cari-pr19` in `backend/package.json`.

### Acceptance

- Per-counterparty AR/AP mapping works and remains optional.
- Existing behavior remains unchanged when mappings are empty.
- PUT semantics for nullable mappings are deterministic:
  - omitted field keeps value
  - explicit `null` clears value
- Enrichment null semantics are deterministic:
  - null/unresolved AR mapping -> `arAccountCode/name` are null
  - null/unresolved AP mapping -> `apAccountCode/name` are null
- Posting safely resolves mapped control accounts where present.
- Posting-time guard is robust:
  - mapped account validity is re-checked at posting time, so stale/deactivated mappings cannot post silently
- Strict tenant/legal-entity/type/postability checks prevent account leakage.
- Settlement flows with null `counterparty_id` are deterministic and use purpose mapping only.
- Counterparty AR/AP account-picker UX is RBAC-consistent:
  - when form uses GL account endpoints, `gl.account.read` is enforced before fetch/render
  - missing account-picker read permission does not produce avoidable background `403` noise
  - account-picker UI uses controlled fallback (hidden/disabled) when permission is missing
- Existing backend helper discipline is preserved (reuse of account guard / transaction wrapper / amount parsing / RBAC middleware pattern; no parallel utility drift).
- No regressions in existing Cari document/settlement flows.

### Commands

```powershell
cd backend
npm run test:cari-pr19
npm run test:cari-pr05
npm run test:cari-pr06
npm run test:cari-pr07
npm run test:cari-pr08
```

### Global Guardrails Check (Mandatory)

- [ ] Section 1) Global Guardrails maddeleri bu PR icin tek tek dogrulandi.
- [ ] ADR-frozen kurallar korunuyor (docs/adr/adr-cari-v1.md).
- [ ] Tenant/legal-entity scope ve RBAC kontrolleri korunuyor.
- [ ] Route -> validator -> service ayrimi korunuyor.
- [ ] Endpoint kontrati degistiyse OpenAPI generator guncellendi ve cikti uretildi.
- [ ] Bu PR testi + mevcut regresyon testleri yesil.

### Canonical Route/Permission/Data Model Mapping (PR-19)

- Section `2.2` affected endpoint families:
  - `/api/v1/cari/counterparties*`
  - existing document/settlement posting paths (resolution logic only)
  - `/api/v1/gl/accounts*` (frontend AR/AP account-picker read dependency only)
- Section `2.4` permission alignment:
  - existing Cari card/doc/settlement permissions remain in force (no new permission required).
  - frontend dependency:
    - if counterparty AR/AP account picker uses GL accounts APIs, require `gl.account.read` before fetch/render
- Section `3` data model alignment:
  - `counterparties` gains optional `ar_account_id` and `ap_account_id`.
  - no semantic mutation in `cari_documents` / settlement tables.
