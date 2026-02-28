Revised PR plan (audited against current my-app)
Date: 2026-02-25

This version keeps the same product goals, but adjusts the steps to match the current repository reality.
The PR sections below are physically ordered backend first, then frontend.

------------------------------------------------------------------------------
Audit findings that change the original thread
------------------------------------------------------------------------------

1) Shareholder parent mapping API already exists with strict validation
- Existing: `/api/v1/org/shareholder-journal-config` (GET/POST).
- Existing validation already enforces:
  - EQUITY account type
  - non-postable parent (`allow_posting=false`)
  - expected normal side by purpose
- New generic mapping APIs must not bypass this logic.

2) GL setup page is already large and active
- `GlSetupPage.jsx` already includes books/coas/accounts/account-mappings and TR default loader.
- New wizard/manual sections must be additive, not a replacement.

3) Existing readiness endpoint must remain untouched
- `/api/v1/onboarding/readiness` is already used by `TenantReadinessProvider`.
- Do not change response shape/logic.
- Add separate module readiness endpoint.

4) Migration sequence already reached `m029`
- New migration for policy-pack apply metadata should start at `m030`.

5) Existing pages already contain partial setup checks
- `OrganizationManagementPage.jsx` and `AcilisFisiOlustur.jsx` already include shareholder setup checks/warnings.
- New readiness gating should integrate with these checks and avoid conflicting UX.

------------------------------------------------------------------------------
Global rules (for every PR)
------------------------------------------------------------------------------

- Templates remain optional. Manual mapping path must fully work without templates.
- No silent writes. Preview/resolve endpoints are read-only.
- Existing backend posting guards remain authoritative (`400` on invalid/missing mapping).
- All mapping/readiness/apply operations are scoped by `tenantId + legalEntityId`.
- Keep `/api/v1/onboarding/readiness` behavior unchanged.
- Re-applying packs must be idempotent at mapping level (upsert-safe, no duplicates).

------------------------------------------------------------------------------
PR-01 - Locked policy-pack catalog service and tenant read APIs
------------------------------------------------------------------------------

Goal
- Add locked TR/AF/US catalog once, expose for tenant setup flows.

Backend changes
- Create `backend/src/services/policy-packs.service.js`
  - packs:
    - `TR_UNIFORM_V1`
    - `AF_STARTER_V1`
    - `US_GAAP_STARTER_V1`
  - exports:
    - `listPolicyPacks()`
    - `getPolicyPack(packId)`
- Create tenant read router:
  - `backend/src/routes/onboarding.policy-packs.routes.js`
  - `GET /api/v1/onboarding/policy-packs`
  - `GET /api/v1/onboarding/policy-packs/:packId`
  - authz: `requirePermission("org.tree.read")`
- Mount tenant router in `backend/src/index.js` as an additional `/api/v1/onboarding` mount (do not replace existing `onboardingRoutes` mount).

Acceptance
- Tenant context can read locked pack definitions.
- List returns `packId, countryIso2, label, locked=true`.
- Detail returns module payload and purpose targets.

Tests
- `backend/scripts/test-policy-packs-readonly.js`
- include onboarding endpoint checks.

------------------------------------------------------------------------------
PR-02 - CARI purpose mappings API (manual setup foundation)
------------------------------------------------------------------------------

Goal
- Add tenant API for manual `journal_purpose_accounts` mapping for CARI purposes.

Important integration rule
- Keep shareholder parent mapping on existing `/api/v1/org/shareholder-journal-config`.
- Do not create a second path that weakens existing shareholder validation.

Backend changes
- Create:
  - `backend/src/services/gl.purpose-mappings.service.js`
  - `backend/src/routes/gl.purpose-mappings.routes.js`
- Endpoints:
  - `GET /api/v1/gl/journal-purpose-accounts?legalEntityId=...`
  - `POST /api/v1/gl/journal-purpose-accounts`
- Validation:
  - account exists
  - account belongs to tenant
  - account belongs to selected LEGAL_ENTITY chart for `legalEntityId`
  - account is active
  - account is postable (`allow_posting=true`) for CARI posting compatibility
  - optionally enforce leaf account (recommended, but postable is the must-have minimum)
- Purpose-code policy:
  - support CARI purpose codes here (`CARI_AR_*`, `CARI_AP_*`)
  - for `SHAREHOLDER_*`, return clear error directing caller to org shareholder config endpoint
- RBAC:
  - GET: `gl.account.read`
  - POST: `gl.account.upsert`
- Register route from `backend/src/routes/gl.js`.

Acceptance
- List/upsert works for legal-entity CARI purpose mappings.
- Shareholder purpose update attempts are safely redirected to existing org API path.

Tests
- `backend/scripts/test-gl-purpose-mappings-api.js`

------------------------------------------------------------------------------
PR-03 - Policy-pack resolver API (preview only)
------------------------------------------------------------------------------

Goal
- For `packId + legalEntityId`, compute proposed mappings without writing.

Backend changes
- Create:
  - `backend/src/services/policy-packs.resolve.service.js`
  - `backend/src/routes/onboarding.policy-packs.resolve.routes.js`
- Endpoint:
  - `POST /api/v1/onboarding/policy-packs/:packId/resolve`
  - payload: `{ legalEntityId }`
- Resolver rules:
  - match legal-entity accounts by pack rules (`codeExact` first)
  - TR AP offset fallback: use `632` if `770` missing
  - if duplicate candidates exist for same code, return `missing=true` with reason `ambiguous_match`
  - validate suitability during resolve (preview must match apply constraints):
    - CARI purposes: active + postable + same legal entity chart
    - Shareholder parent purposes: EQUITY + non-postable + expected normal side + same legal entity
  - when code exists but fails suitability, return `missing=true` with reason `unsuitable_match`
  - return per purpose:
    - resolved row: accountId/accountCode/confidence
    - missing row: reason + suggestCreate payload
- RBAC: `org.tree.read`

Acceptance
- TR default chart resolves required rows.
- Missing/ambiguous rows are returned as unresolved with clear reasons.

Tests
- `backend/scripts/test-policy-pack-resolve.js`

------------------------------------------------------------------------------
PR-04 - Apply pack API and apply metadata
------------------------------------------------------------------------------

Goal
- Explicitly apply confirmed rows into `journal_purpose_accounts` and store audit metadata.

Backend changes
- Migration:
  - `backend/src/migrations/m030_legal_entity_policy_packs.js`
  - table `legal_entity_policy_packs`:
    - `id` PK (auto increment)
    - `tenant_id`, `legal_entity_id`, `pack_id`, `mode`, `payload_json`
    - `applied_by_user_id`, `applied_at`
    - index `(tenant_id, legal_entity_id, applied_at)`
  - metadata storage mode: **history** (append one row per apply action; no unique `(tenant_id, legal_entity_id)` constraint)
- Service:
  - `backend/src/services/policy-packs.apply.service.js`
  - `applyPolicyPack({ tenantId, userId, legalEntityId, packId, mode, rows })`
- Validation rules:
  - all accounts tenant/entity scoped + active
  - CARI rows must be postable (`allow_posting=true`)
  - AR control != AR offset, AP control != AP offset
  - shareholder parent rows validated through existing shareholder helper constraints (EQUITY, non-postable, expected side)
- Apply logic:
  - `MERGE`: upsert only provided purpose codes
  - `OVERWRITE`: delete existing mappings for the pack-managed purpose codes for that legal entity, then insert/upsert provided rows
  - reuse `upsertJournalPurposeAccountTx` pattern for idempotent mapping writes
- Route:
  - `POST /api/v1/onboarding/policy-packs/:packId/apply`
  - authz: `gl.account.upsert`

Acceptance
- Confirm apply writes mappings only after explicit call.
- Invalid rows return clear `400`.
- Metadata row is appended for each apply action (history mode).

Tests
- `backend/scripts/test-policy-pack-apply.js`

------------------------------------------------------------------------------
PR-05 - Module readiness API (CARI posting + shareholder commitment)
------------------------------------------------------------------------------

Goal
- Add proactive module readiness endpoint, without changing existing onboarding readiness.

Backend changes
- Create:
  - `backend/src/services/moduleReadiness.service.js`
  - `backend/src/routes/onboarding.module-readiness.routes.js`
- Endpoint:
  - `GET /api/v1/onboarding/module-readiness?legalEntityId=...`
- Response shape:
  - `modules.cariPosting.byLegalEntity[]`
  - `modules.shareholderCommitment.byLegalEntity[]`
- CARI readiness checks:
  - required codes: `CARI_AR_CONTROL`, `CARI_AR_OFFSET`, `CARI_AP_CONTROL`, `CARI_AP_OFFSET`
  - mapping exists, account exists in same legal entity chart, active, postable
  - control/offset distinct per direction
- Shareholder readiness checks:
  - required codes: `SHAREHOLDER_CAPITAL_CREDIT_PARENT`, `SHAREHOLDER_COMMITMENT_DEBIT_PARENT`
  - exists, account active, EQUITY, non-postable, distinct
  - include normal-side validation (capital=CREDIT, commitment=DEBIT)
- RBAC: use scoped permission pattern, e.g.
  - `requirePermission("org.tree.read", { resolveScope: (req, tenantId) => legalEntityId ? { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId } : { scopeType: "TENANT", scopeId: tenantId } })`

Acceptance
- Fresh tenant: not ready with detailed missing/invalid reasons.
- After valid manual mapping or pack apply: readiness true.

Tests
- `backend/scripts/test-module-readiness.js`

------------------------------------------------------------------------------
PR-06 - Frontend module readiness provider and API client
------------------------------------------------------------------------------

Goal
- Make module readiness available across pages with cache + refresh.

Frontend changes
- Create:
  - `frontend/src/api/moduleReadiness.js`
  - `frontend/src/readiness/ModuleReadinessProvider.jsx`
  - `frontend/src/readiness/useModuleReadiness.js`
- Wrap tenant app shell in app root with `ModuleReadinessProvider` (next to existing tenant readiness provider).
- Add helper selectors by `(moduleKey, legalEntityId)`.

Acceptance
- Any page can read current module readiness and trigger refresh after mapping changes.

------------------------------------------------------------------------------
PR-07 - GL Setup page: Template wizard + advanced manual mappings
------------------------------------------------------------------------------

Goal
- In existing `GlSetupPage`, make both setup paths explicit and first-class.

Frontend changes

Section A: Recommended template wizard
- legal entity picker
- pack picker (country-aware)
- preview button -> resolver API
- confirm apply button -> apply API
- preview table:
  - purposeCode
  - proposed account or missing state
  - confidence badge
  - missing rows can be manually overridden before apply
- after apply:
  - show applied pack info and timestamp from response
  - refresh module readiness

Section B: Advanced manual purpose mappings
- legal entity picker
- required code table for:
  - CARI: 4 codes via new GL purpose mapping API
  - shareholder: 2 parent codes via existing org shareholder config API
- per-row status chips from module readiness
- save actions with clear error mapping
- shareholder save model:
  - save both shareholder parent IDs together in one submit action (existing org endpoint requires both), not independent per-row writes

Keep existing GlSetup sections
- Do not remove current Books/CoA/Accounts/Account Mapping tools.

Acceptance
- Manual path works without template use.
- Wizard preview does not write until confirm.
- Both paths converge to same readiness endpoint result.

------------------------------------------------------------------------------
PR-08 - UI gating integration on CARI and shareholder entry points
------------------------------------------------------------------------------

Goal
- Block avoidable failures before backend `400`, with clear fix paths.

Frontend changes

1) `CariDocumentsPage.jsx`
- for selected document legal entity:
  - if `cariPosting` not ready, show persistent warning card
  - disable `Post Draft`
  - show reasons (missing/invalid mapping)
  - CTA links:
    - manual fix -> `/app/ayarlar/hesap-plani-ayarlari`
    - template setup -> same GL setup page (wizard section)

2) `CariSettlementsPage.jsx`
- for selected legal entity:
  - if `cariPosting` not ready, show warning and disable apply/bank-apply actions
  - keep reverse actions available for already-posted historical rows

3) `OrganizationManagementPage.jsx`
- add module readiness badge/card for shareholder commitment
- gate batch commitment preview/execute behind readiness + existing checks
- CTA links to manual and template setup

4) `AcilisFisiOlustur.jsx`
- keep existing checklist card
- integrate module-readiness signals in warning details
- do not globally disable journal creation (page is generic, not only commitment flow)

5) i18n
- add messages for setup incomplete/missing purpose mapping and CTA labels.

Acceptance
- CARI posting actions are disabled when setup is incomplete.
- Shareholder batch commitment actions are blocked when module readiness is false.
- Users get direct fix paths instead of generic backend errors.

------------------------------------------------------------------------------
PR-09 - USA default CoA loader and allowPosting fix
------------------------------------------------------------------------------

Goal
- Add US defaults and stop forcing all loader-created accounts to postable.

Frontend changes
- Update `GlSetupPage.jsx`:
  - add `USA_DEFAULT_COA_ACCOUNTS` constant
  - add "Load USA defaults" button
  - change account/default bulk loader payloads to:
    - `allowPosting: account.allowPosting ?? true`
- Include non-postable sample parent accounts in USA defaults (for example 3100/3110 style parent rows).
- apply same `allowPosting` behavior to existing TR/default loaders too, not only USA path.

Acceptance
- USA defaults can be loaded from UI.
- non-postable parent rows are created with `allow_posting=false`.
- TR/default loaders also preserve explicit `allowPosting=false` values when provided.

------------------------------------------------------------------------------
Final end-to-end acceptance
------------------------------------------------------------------------------

Path 1: Template setup
- select legal entity -> preview TR/AF/US pack -> confirm apply -> module readiness true -> CARI post/apply enabled.

Path 2: Manual setup
- open GL setup manual mapping -> map 4 CARI codes + 2 shareholder parent codes -> readiness true -> actions enabled.

Security
- Tenant pages require tenant auth + existing RBAC.

Compatibility
- Existing `/api/v1/onboarding/readiness` and tenant readiness guard remain unchanged.
