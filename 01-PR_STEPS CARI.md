# PR Steps (Detailed, Non-Break, Repo-Aligned)

## 0) Baseline Snapshot (Current Repo)

This plan is aligned to the current implementation patterns in:

- Route wiring: `backend/src/index.js`, `backend/src/routes/*`
- Service split: `backend/src/services/*`
- Validation split: `backend/src/routes/*validators.js`
- Migration style: `backend/src/migrations/m01x_*.js`, `backend/src/migrations/index.js`
- Frontend route model: `frontend/src/App.jsx`, `frontend/src/layouts/sidebarConfig.js`
- Frontend API style: `frontend/src/api/*.js`
- Smoke/integration tests style: `backend/scripts/test-cari-pr0x-*.js`

Current important facts:

- Cari backend is already strong for docs/settlement/reports/audit.
- Frontend has Cari Counterparty + Reports pages, but not Documents/Settlements/Audit pages.
- Sidebar has Contracts and Gelecek Yillar Gelirleri as placeholders (PR-18 upgrades this to full periodization split views).

---

## 1) Global Guardrails (Apply to Every PR)

- Keep ADR-frozen Cari behavior unchanged (`docs/adr/adr-cari-v1.md`).
- No destructive migration for existing Cari tables.
- Keep route -> validator -> service split exactly.
- Keep tenant and legal-entity scope checks on every write/read endpoint.
- Keep RBAC checks at route level and action-level UI checks in frontend.
- Keep OpenAPI generation source of truth in `backend/scripts/generate-openapi.js`.
- Keep existing tests green; add targeted script per PR.
- Use additive changes first, then integrations.

Important frontend guard detail:

- `withPermissionGuard` in `frontend/src/App.jsx` uses `RequirePermission anyOf`.
- Prefer single route-level read permission when it exists.
- If module has no read permission (current settlements case), use explicit any-of action permissions.
- Action buttons in pages must check specific write permissions with `hasPermission`.

---

## 2) Canonical Route and Permission Map

## 2.1 Frontend routes to add (PR-11)

| Module           | Route                   | Sidebar Label                          | Route-Level Permission                                                                     |
| ---------------- | ----------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------ |
| Cari Documents   | `/app/cari-belgeler`    | `Cari Belgeler`                        | `cari.doc.read`                                                                            |
| Cari Settlements | `/app/cari-settlements` | `Cari Mahsuplastirma / Tahsilat-Odeme` | `anyOf(cari.settlement.apply, cari.settlement.reverse, cari.bank.attach, cari.bank.apply)` |
| Cari Audit       | `/app/cari-audit`       | `Cari Denetim Izleri`                  | `cari.audit.read`                                                                          |

## 2.2 Existing backend endpoints to consume (PR-11..14)

- Documents:
  - `GET /api/v1/cari/documents`
  - `GET /api/v1/cari/documents/{documentId}`
  - `POST /api/v1/cari/documents`
  - `PUT /api/v1/cari/documents/{documentId}`
  - `POST /api/v1/cari/documents/{documentId}/cancel`
  - `POST /api/v1/cari/documents/{documentId}/post`
  - `POST /api/v1/cari/documents/{documentId}/reverse`
- Settlements:
  - `POST /api/v1/cari/settlements/apply`
  - `POST /api/v1/cari/settlements/{settlementBatchId}/reverse`
  - `POST /api/v1/cari/bank/attach`
  - `POST /api/v1/cari/bank/apply`
- Reports:
  - `GET /api/v1/cari/reports/open-items`
  - `GET /api/v1/cari/reports/statement`
- Audit:
  - `GET /api/v1/cari/audit`

## 2.3 New backend endpoint namespaces (PR-16..18)

- Contracts:
  - `/api/v1/contracts/*`
- Deferred + Accrual Periodization (Gelecek Aylar/Yillar Gelirler + Gelir/Gider Tahakkuklari):
  - `/api/v1/revenue-recognition/*`

## 2.4 Exact Cari permission matrix for UI guards (must use backend names)

- Route-level guard:
  - Documents page: `cari.doc.read`
  - Settlements page: any of `cari.settlement.apply`, `cari.settlement.reverse`, `cari.bank.attach`, `cari.bank.apply`
  - Audit page: `cari.audit.read`
- Action-level guards:
  - Document list/detail: `cari.doc.read`
  - Draft create: `cari.doc.create`
  - Draft edit/cancel: `cari.doc.update`
  - Post: `cari.doc.post`
  - Reverse posted: `cari.doc.reverse`
  - Post with FX override: `cari.fx.override` (in addition to `cari.doc.post`)
  - Settlement apply: `cari.settlement.apply`
  - Settlement reverse: `cari.settlement.reverse`
  - Bank attach: `cari.bank.attach`
  - Bank apply: `cari.bank.apply`
  - Audit read: `cari.audit.read`

---

## 3) Existing Cari Data Model to Respect (No semantic drift)

Core tables already in use:

- `cari_documents`
  - keys: `tenant_id`, `legal_entity_id`, `counterparty_id`
  - amounts: `amount_txn`, `amount_base`, `open_amount_txn`, `open_amount_base`
  - status: `DRAFT`, `POSTED`, `PARTIALLY_SETTLED`, `SETTLED`, `CANCELLED`, `REVERSED`
  - snapshots: `counterparty_*_snapshot`, `payment_term_snapshot`, `due_date_snapshot`, `currency_code_snapshot`, `fx_rate_snapshot`
- `cari_open_items`
  - status: `OPEN`, `PARTIALLY_SETTLED`, `SETTLED`, `WRITTEN_OFF`, `CANCELLED`
  - residuals and settled amounts maintained during settlement apply/reverse
- `cari_settlement_batches`
  - status: `DRAFT`, `POSTED`, `REVERSED`, `CANCELLED`
  - bank link fields and idempotency fields
- `cari_settlement_allocations`
  - apply allocations against open items
- `cari_unapplied_cash`
  - status: `UNAPPLIED`, `PARTIALLY_APPLIED`, `FULLY_APPLIED`, `REFUNDED`, `REVERSED`
- `audit_logs`
  - source for `GET /api/v1/cari/audit`

---

## 4) PR-11: Cari API Clients + Route Foundation

### Goal

Add frontend foundations only, no backend business logic change.

### Files to create

- `frontend/src/api/cariCommon.js`
- `frontend/src/api/cariDocuments.js`
- `frontend/src/api/cariSettlements.js`
- `frontend/src/api/cariAudit.js`
- `backend/scripts/test-cari-pr11-frontend-routing-and-api-clients.js`

### Files to update

- `frontend/src/App.jsx`
- `frontend/src/layouts/sidebarConfig.js`
- `frontend/src/i18n/messages.js`
- `frontend/src/api/cariCounterparty.js`
- `frontend/src/api/cariPaymentTerms.js`
- `frontend/src/api/cariReports.js`
- `backend/package.json`

### Concrete skeletons

`frontend/src/api/cariCommon.js`

```js
export function toCariQueryString(params = {}) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") continue;
    searchParams.set(key, String(value));
  }
  const query = searchParams.toString();
  return query ? `?${query}` : "";
}

export function parseCariApiError(error) {
  const status = Number(error?.response?.status || error?.status || 0) || null;
  const data = error?.response?.data || error?.data || {};
  const message = String(data?.message || error?.message || "Request failed");
  const requestId = data?.requestId || error?.requestId || null;
  const isIdempotentReplay = Boolean(data?.idempotentReplay);
  const followUpRisks = Array.isArray(data?.followUpRisks)
    ? data.followUpRisks
    : [];

  // Keep legacy compatibility for pages/utilities that still read
  // err.response.data.message/requestId (Axios-style shape).
  const response = {
    status,
    data: {
      ...data,
      message,
      requestId,
      idempotentReplay: isIdempotentReplay,
      followUpRisks,
    },
  };

  return {
    status,
    message,
    requestId,
    isValidation: status === 400,
    isPermission: status === 401 || status === 403,
    isIdempotentReplay,
    followUpRisks,
    response,
    originalError: error,
  };
}

export function extractCariReplayAndRisks(payload) {
  return {
    idempotentReplay: Boolean(payload?.idempotentReplay),
    followUpRisks: Array.isArray(payload?.followUpRisks)
      ? payload.followUpRisks
      : [],
  };
}
```

`frontend/src/api/cariDocuments.js`

```js
import { api } from "./client.js";
import { parseCariApiError, toCariQueryString } from "./cariCommon.js";

async function run(requestFn) {
  try {
    const response = await requestFn();
    return response.data;
  } catch (error) {
    throw parseCariApiError(error);
  }
}

export async function listCariDocuments(params = {}) {
  return run(() =>
    api.get(`/api/v1/cari/documents${toCariQueryString(params)}`),
  );
}

export async function getCariDocument(documentId) {
  return run(() => api.get(`/api/v1/cari/documents/${documentId}`));
}

export async function createCariDocument(payload) {
  return run(() => api.post("/api/v1/cari/documents", payload));
}

export async function updateCariDocument(documentId, payload) {
  return run(() => api.put(`/api/v1/cari/documents/${documentId}`, payload));
}

export async function cancelCariDocument(documentId) {
  return run(() => api.post(`/api/v1/cari/documents/${documentId}/cancel`, {}));
}

export async function postCariDocument(documentId, payload = {}) {
  return run(() =>
    api.post(`/api/v1/cari/documents/${documentId}/post`, payload),
  );
}

export async function reverseCariDocument(documentId, payload = {}) {
  return run(() =>
    api.post(`/api/v1/cari/documents/${documentId}/reverse`, payload),
  );
}
```

`frontend/src/api/cariSettlements.js`

```js
import { api } from "./client.js";
import { parseCariApiError } from "./cariCommon.js";

async function run(requestFn) {
  try {
    const response = await requestFn();
    return response.data;
  } catch (error) {
    throw parseCariApiError(error);
  }
}

export async function applyCariSettlement(payload) {
  return run(() => api.post("/api/v1/cari/settlements/apply", payload));
}

export async function reverseCariSettlement(settlementBatchId, payload = {}) {
  return run(() =>
    api.post(`/api/v1/cari/settlements/${settlementBatchId}/reverse`, payload),
  );
}

export async function attachCariBankReference(payload) {
  return run(() => api.post("/api/v1/cari/bank/attach", payload));
}

export async function applyCariBankSettlement(payload) {
  return run(() => api.post("/api/v1/cari/bank/apply", payload));
}
```

`frontend/src/api/cariAudit.js`

```js
import { api } from "./client.js";
import { parseCariApiError, toCariQueryString } from "./cariCommon.js";

async function run(requestFn) {
  try {
    const response = await requestFn();
    return response.data;
  } catch (error) {
    throw parseCariApiError(error);
  }
}

export async function listCariAudit(params = {}) {
  return run(() => api.get(`/api/v1/cari/audit${toCariQueryString(params)}`));
}
```

`frontend/src/App.jsx` (route entries)

```jsx
{
  appPath: "/app/cari-belgeler",
  childPath: "cari-belgeler",
  element: <ModulePlaceholderPage title="Cari Belgeler" path="/app/cari-belgeler" />,
},
{
  appPath: "/app/cari-settlements",
  childPath: "cari-settlements",
  element: <ModulePlaceholderPage title="Cari Mahsuplastirma / Tahsilat-Odeme" path="/app/cari-settlements" />,
},
{
  appPath: "/app/cari-audit",
  childPath: "cari-audit",
  element: <ModulePlaceholderPage title="Cari Denetim Izleri" path="/app/cari-audit" />,
},
```

Important App guard note (repo-aligned):

- Current `App.jsx` permission guard resolves from `sidebarLinkByPath`:
  - `withPermissionGuard(appPath, element)` -> `sidebar requiredPermissions`
- Do not rely on a route object `permissions` field for PR-11.
- Route access control for these entries is valid only when matching sidebar items define
  exact `requiredPermissions`.

`frontend/src/layouts/sidebarConfig.js` (under `Cari Islemler`)

```js
{
  label: "Cari Belgeler",
  to: "/app/cari-belgeler",
  requiredPermissions: ["cari.doc.read"],
  implemented: true,
},
{
  label: "Cari Mahsuplastirma / Tahsilat-Odeme",
  to: "/app/cari-settlements",
  requiredPermissions: [
    "cari.settlement.apply",
    "cari.settlement.reverse",
    "cari.bank.attach",
    "cari.bank.apply",
  ],
  implemented: true,
},
{
  label: "Cari Denetim Izleri",
  to: "/app/cari-audit",
  requiredPermissions: ["cari.audit.read"],
  implemented: true,
},
```

`backend/scripts/test-cari-pr11-frontend-routing-and-api-clients.js`

```js
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function assert(c, m) {
  if (!c) throw new Error(m);
}
function hasPath(source, pathValue) {
  const escaped = pathValue.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(escaped).test(source);
}
function getImplementedRoutesBlock(source) {
  const start = source.indexOf("const implementedRoutes = [");
  if (start < 0) return "";
  const end = source.indexOf("const implementedPaths =", start);
  if (end < 0) return source.slice(start);
  return source.slice(start, end);
}
function countOccurrences(source, literal) {
  const escaped = literal.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(escaped, "g");
  return (source.match(re) || []).length;
}
function hasCariCommonImport(source) {
  return /from\s+['"]\.\/cariCommon\.js['"]/.test(source);
}

async function main() {
  const root = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "..",
    "..",
  );
  const app = await readFile(
    path.resolve(root, "frontend/src/App.jsx"),
    "utf8",
  );
  const sidebar = await readFile(
    path.resolve(root, "frontend/src/layouts/sidebarConfig.js"),
    "utf8",
  );
  const i18nMessages = await readFile(
    path.resolve(root, "frontend/src/i18n/messages.js"),
    "utf8",
  );
  const apiCommon = await readFile(
    path.resolve(root, "frontend/src/api/cariCommon.js"),
    "utf8",
  );
  const apiDocs = await readFile(
    path.resolve(root, "frontend/src/api/cariDocuments.js"),
    "utf8",
  );
  const apiSettle = await readFile(
    path.resolve(root, "frontend/src/api/cariSettlements.js"),
    "utf8",
  );
  const apiAudit = await readFile(
    path.resolve(root, "frontend/src/api/cariAudit.js"),
    "utf8",
  );
  const apiCounterparty = await readFile(
    path.resolve(root, "frontend/src/api/cariCounterparty.js"),
    "utf8",
  );
  const apiPaymentTerms = await readFile(
    path.resolve(root, "frontend/src/api/cariPaymentTerms.js"),
    "utf8",
  );
  const apiReports = await readFile(
    path.resolve(root, "frontend/src/api/cariReports.js"),
    "utf8",
  );
  const implementedRoutesBlock = getImplementedRoutesBlock(app);

  assert(implementedRoutesBlock, "missing implementedRoutes block in App.jsx");
  assert(
    hasPath(implementedRoutesBlock, "/app/cari-belgeler"),
    "missing /app/cari-belgeler in implementedRoutes",
  );
  assert(
    hasPath(implementedRoutesBlock, "/app/cari-settlements"),
    "missing /app/cari-settlements in implementedRoutes",
  );
  assert(
    hasPath(implementedRoutesBlock, "/app/cari-audit"),
    "missing /app/cari-audit in implementedRoutes",
  );
  assert(
    hasPath(sidebar, "/app/cari-belgeler"),
    "missing sidebar cari-belgeler",
  );
  assert(
    hasPath(sidebar, "/app/cari-settlements"),
    "missing sidebar cari-settlements",
  );
  assert(hasPath(sidebar, "/app/cari-audit"), "missing sidebar cari-audit");
  assert(
    /sidebar\s*:\s*\{[\s\S]*?byPath\s*:\s*\{/.test(i18nMessages),
    "missing i18n sidebar.byPath structure",
  );
  assert(
    countOccurrences(i18nMessages, "/app/cari-belgeler") >= 2,
    "missing i18n key /app/cari-belgeler in tr/en maps",
  );
  assert(
    countOccurrences(i18nMessages, "/app/cari-settlements") >= 2,
    "missing i18n key /app/cari-settlements in tr/en maps",
  );
  assert(
    countOccurrences(i18nMessages, "/app/cari-audit") >= 2,
    "missing i18n key /app/cari-audit in tr/en maps",
  );
  assert(apiCommon.includes("parseCariApiError"), "cariCommon parser missing");
  assert(
    apiCommon.includes("toCariQueryString"),
    "cariCommon query helper missing",
  );
  assert(
    apiCommon.includes("response"),
    "cariCommon should keep axios-compatible response shape",
  );
  assert(apiDocs.includes("/api/v1/cari/documents"), "docs api path missing");
  assert(
    apiSettle.includes("/api/v1/cari/settlements/apply"),
    "settlement api path missing",
  );
  assert(apiAudit.includes("/api/v1/cari/audit"), "audit api path missing");
  assert(
    hasCariCommonImport(apiCounterparty),
    "cariCounterparty should use cariCommon",
  );
  assert(
    hasCariCommonImport(apiPaymentTerms),
    "cariPaymentTerms should use cariCommon",
  );
  assert(hasCariCommonImport(apiReports), "cariReports should use cariCommon");

  console.log("PR-11 smoke passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

### Checklist

- [ ] Create shared `frontend/src/api/cariCommon.js` and wire all Cari API files through it.
- [ ] Create 3 frontend API files with exact endpoint names.
- [ ] Refactor existing Cari API files to use `cariCommon.js`:
  - [ ] `frontend/src/api/cariCounterparty.js`
  - [ ] `frontend/src/api/cariPaymentTerms.js`
  - [ ] `frontend/src/api/cariReports.js`
- [ ] Add 3 implemented routes in `App.jsx`.
- [ ] Add 3 sidebar links under Cari section.
- [ ] Add i18n `sidebar.byPath` keys for the 3 routes (TR + EN message maps).
- [ ] Keep App guard model repo-aligned:
  - [ ] rely on sidebar `requiredPermissions` for route guarding
  - [ ] do not add/use route-level `permissions` field in PR-11 entries
- [ ] Use exact backend permission names in route/sidebar guards (no alias names).
- [ ] Keep error normalization backward-compatible:
  - [ ] normalized error still exposes Axios-like `response.status` and `response.data.*`
  - [ ] legacy callers reading `err.response.data.message` continue to work
- [ ] Add PR-11 smoke script and package script:
  - [ ] `test:cari-pr11`
- [ ] Keep PR-11 smoke assertions formatting-agnostic (path/pattern checks instead of fragile exact string fragments).
- [ ] Ensure PR-11 smoke validates all three sidebar routes and i18n `sidebar.byPath` keys.
- [ ] Ensure PR-11 smoke validates the 3 Cari routes are in `implementedRoutes` branch (not only present somewhere in `App.jsx`).

### Acceptance

- Authorized users with route permission can open pages.
- Unauthorized users blocked via `RequirePermission`.
- Shared helper normalizes API errors and query-string behavior consistently across new and existing Cari API files.
- Existing utilities that parse Axios-style errors (`err.response.data.message`) keep working.
- Existing Cari pages continue to work.

### Command

```powershell
cd backend
npm run test:cari-pr11
```

### Global Guardrails Check (Mandatory)

- [ ] Section 1) Global Guardrails maddeleri bu PR icin tek tek dogrulandi.
- [ ] ADR-frozen kurallar korunuyor (docs/adr/adr-cari-v1.md).
- [ ] Tenant/legal-entity scope ve RBAC kontrolleri korunuyor.
- [ ] Route -> validator -> service ayrimi korunuyor.
- [ ] Endpoint kontrati degistiyse OpenAPI generator guncellendi ve cikti uretildi.
- [ ] Bu PR testi + mevcut regresyon testleri yesil.

### Canonical Route/Permission/Data Model Mapping (PR-11)

- Section `2.1` route coverage:
  - `/app/cari-belgeler`
  - `/app/cari-settlements`
  - `/app/cari-audit`
- Section `2.2` endpoint families to wire in API clients:
  - `/api/v1/cari/documents*`
  - `/api/v1/cari/settlements*` + `/api/v1/cari/bank/*`
  - `/api/v1/cari/audit`
- Section `2.4` permission alignment:
  - route guards use exact backend names (`cari.doc.read`, settlements `anyOf(...)`, `cari.audit.read`)
- Section `3` data model alignment:
  - PR-11 does not mutate DB tables; only frontend wiring and API client contracts.

---

## 5) PR-12: Cari Documents Page (Draft/Post/Reverse UI)

### Goal

Implement complete document lifecycle UI on existing backend.

### Files to create

- `frontend/src/pages/cari/CariDocumentsPage.jsx`
- `frontend/src/pages/cari/cariDocumentsUtils.js`
- `backend/scripts/test-cari-pr12-frontend-documents-smoke.js`
- `backend/scripts/test-cari-pr12-documents-date-filter-contract.js`

### Files to update

- `frontend/src/App.jsx` (replace placeholder element for `/app/cari-belgeler`)
- `frontend/src/i18n/messages.js`
- `backend/src/routes/cari.document.validators.js` (add date-range filter parsing)
- `backend/src/services/cari.document.service.js` (apply date-range SQL conditions)
- `backend/scripts/generate-openapi.js` (document new query params)
- `backend/package.json`

### Concrete route

- `/app/cari-belgeler` -> `<CariDocumentsPage />`
- Route guard source (repo-aligned): `sidebarConfig.js` `requiredPermissions` for `/app/cari-belgeler`
  must stay `["cari.doc.read"]`.
- Do not rely on route object `permissions` field for PR-12 (`App.jsx` guard uses sidebar map).

### UI behavior rules (must match backend)

- Reverse action only when document status is `POSTED`.
- Cancel/Edit only for `DRAFT`.
- Post only for `DRAFT`.
- List/filter must support:
  - `legalEntityId`
  - `counterpartyId`
  - `direction` (`AR`/`AP`)
  - `documentType`
  - `status`
  - `dateFrom` + `dateTo` (primary document date range params)
  - backward-compatible aliases accepted by backend validator:
    - `documentDateFrom` -> normalized to internal `dateFrom`
    - `documentDateTo` -> normalized to internal `dateTo`
- Detail drawer/modal must show:
  - `documentNo`
  - `status`
  - `postedJournalEntryId`
  - snapshot fields (`counterparty*Snapshot`, `dueDateSnapshot`, `fxRateSnapshot`, `currencyCodeSnapshot`)
  - reversal linkage:
    - `reversalOfDocumentId` from document `GET/list` response
    - reverse action nested response from `POST /reverse`:
      - `response.row.id` (reversal document id)
      - `response.row.documentNo` (reversal document no, if shown in UI)
      - `response.journal.reversalJournalEntryId`
- FX override in post action:
  - UI must expose explicit `useFxOverride` checkbox + `fxOverrideReason` field.
  - `useFxOverride=true` requires `fxOverrideReason`.
  - only users with `cari.fx.override` can submit with override.
  - when unauthorized, show explicit inline message (not generic failure).

Important repo note:

- Current `parseDocumentReadFilters` does not parse date range yet.
- PR-12 must include backend support for both date param variants:
  - primary: `dateFrom` / `dateTo`
  - legacy aliases: `documentDateFrom` / `documentDateTo`
- Validator/service must normalize both variants to one internal filter shape
  (`dateFrom`, `dateTo`) so existing callers/scripts do not break.
- Option A lock for reversal linkage (scope-safe):
  - backend mapper field is `reversalOfDocumentId` (not `reversedDocumentId`)
  - do not require flat `reversalDocumentId` / `reversalJournalEntryId` in `GET /documents` for PR-12
  - read reverse linkage from nested reverse response:
    - `response.row.id`
    - `response.journal.reversalJournalEntryId`
  - if flattening is needed, do it in a separate backend enhancement PR.
- Create draft payload alignment:
  - do not force line-items in UI if backend create contract is header-level for current scope
  - keep payload minimal and backend-validator-compatible
- Due date rule alignment:
  - `dueDate` is conditionally required based on `documentType` validator rules
  - UI validation/help text must follow backend rule, not a single global required flag
- Guard alignment:
  - replacing placeholder with `<CariDocumentsPage />` must preserve guard behavior via
    sidebar permission mapping (`cari.doc.read`).
  - no new route-level `permissions` field dependency should be introduced.

### Skeleton

`frontend/src/pages/cari/cariDocumentsUtils.js`

```js
export const DOCUMENT_STATUSES = [
  "DRAFT",
  "POSTED",
  "PARTIALLY_SETTLED",
  "SETTLED",
  "CANCELLED",
  "REVERSED",
];

export function buildDocumentListQuery(filters) {
  return {
    legalEntityId: filters.legalEntityId || undefined,
    counterpartyId: filters.counterpartyId || undefined,
    direction: filters.direction || undefined,
    documentType: filters.documentType || undefined,
    status: filters.status || undefined,
    dateFrom: filters.dateFrom || filters.documentDateFrom || undefined,
    dateTo: filters.dateTo || filters.documentDateTo || undefined,
    q: filters.q || undefined,
    limit: filters.limit || 100,
    offset: filters.offset || 0,
  };
}
```

`frontend/src/pages/cari/CariDocumentsPage.jsx` (minimal structure)

```jsx
import { useEffect, useMemo, useState } from "react";
import {
  listCariDocuments,
  createCariDocument,
  updateCariDocument,
  cancelCariDocument,
  postCariDocument,
  reverseCariDocument,
} from "../../api/cariDocuments.js";
import { useAuth } from "../../auth/useAuth.js";
import { buildDocumentListQuery } from "./cariDocumentsUtils.js";

export default function CariDocumentsPage() {
  const { hasPermission } = useAuth();
  const canRead = hasPermission("cari.doc.read");
  const canCreate = hasPermission("cari.doc.create");
  const canUpdate = hasPermission("cari.doc.update");
  const canPost = hasPermission("cari.doc.post");
  const canReverse = hasPermission("cari.doc.reverse");
  const canFxOverride = hasPermission("cari.fx.override");

  // filters, list state, form state, selected row state
  // load list on mount/filter changes
  // action handlers: create/update/cancel/post/reverse
  // map backend error message to inline alert
  // render table + form + action panel
  return <div>Cari Documents</div>;
}
```

### Checklist

- [ ] Build list/filter with backend query params exactly.
- [ ] Add backend date-range support for documents list with alias compatibility:
  - [ ] accept `dateFrom` / `dateTo`
  - [ ] accept legacy `documentDateFrom` / `documentDateTo`
  - [ ] normalize aliases to internal `dateFrom` / `dateTo` filter shape
- [ ] Keep route guard wiring repo-aligned:
  - [ ] `/app/cari-belgeler` remains permission-gated through sidebar `requiredPermissions: ["cari.doc.read"]`
  - [ ] do not introduce route object `permissions` dependency
- [ ] Build create draft form with required fields.
- [ ] Build edit/cancel/post/reverse action panel.
- [ ] Implement permission-aware buttons.
- [ ] Render posted journal reference and snapshot fields.
- [ ] Add detail drawer/modal with reversal linkage visibility using `reversalOfDocumentId`.
- [ ] Show reverse result linkage from nested response in result panel/detail state:
  - [ ] `response.row.id` / `response.row.documentNo`
  - [ ] `response.journal.reversalJournalEntryId`
- [ ] Keep create payload header-level unless backend contract expands (no artificial line-items requirement in PR-12).
- [ ] Apply `dueDate` conditional required logic based on selected `documentType` and backend validator rules.
- [ ] Add explicit FX override UX and unauthorized guidance text.
- [ ] Add frontend smoke script:
  - [ ] verify route mounts `CariDocumentsPage`
  - [ ] verify filters include date range
  - [ ] verify action buttons and labels exist
  - [ ] verify API function usage by source scan
  - [ ] verify source uses `reversalOfDocumentId` and does not reference `reversedDocumentId`
  - [ ] verify reverse action result wiring uses nested fields:
    - [ ] `response.row.id` / `response.row.documentNo`
    - [ ] `response.journal.reversalJournalEntryId`
- [ ] Add backend filter contract test for date params and aliases:
  - [ ] `dateFrom` / `dateTo`
  - [ ] `documentDateFrom` / `documentDateTo` -> same normalized behavior
- [ ] Add script alias: `test:cari-pr12`

### Acceptance

- Draft create/update/cancel works.
- Post works and shows `postedJournalEntryId`.
- Reverse works only on posted docs.
- Reversal linkage rendering matches real backend payload names and sources.
- Reverse action UI reads real nested response shape (`row.*`, `journal.reversalJournalEntryId`) without requiring flat fields.
- Create flow matches real backend payload contract (header-level scope for this PR).
- `dueDate` required behavior matches backend `documentType` validation.
- Date-range filter is supported server-side and reflected in OpenAPI.
- Date filter remains backward-compatible for existing callers/scripts that still send
  `documentDateFrom` / `documentDateTo`.
- No regressions in `test:cari-pr05` and `test:cari-pr06`.

### Commands

```powershell
cd backend
npm run test:cari-pr12
npm run test:cari-pr12-documents-date-filter
npm run test:cari-pr05
npm run test:cari-pr06
```

### Global Guardrails Check (Mandatory)

- [ ] Section 1) Global Guardrails maddeleri bu PR icin tek tek dogrulandi.
- [ ] ADR-frozen kurallar korunuyor (docs/adr/adr-cari-v1.md).
- [ ] Tenant/legal-entity scope ve RBAC kontrolleri korunuyor.
- [ ] Route -> validator -> service ayrimi korunuyor.
- [ ] Endpoint kontrati degistiyse OpenAPI generator guncellendi ve cikti uretildi.
- [ ] Bu PR testi + mevcut regresyon testleri yesil.

### Canonical Route/Permission/Data Model Mapping (PR-12)

- Section `2.1` route coverage:
  - `/app/cari-belgeler`
- Section `2.2` endpoint coverage:
  - `GET /api/v1/cari/documents`
  - `GET /api/v1/cari/documents/{documentId}`
  - `POST /api/v1/cari/documents`
  - `PUT /api/v1/cari/documents/{documentId}`
  - `POST /api/v1/cari/documents/{documentId}/cancel`
  - `POST /api/v1/cari/documents/{documentId}/post`
  - `POST /api/v1/cari/documents/{documentId}/reverse`
- Section `2.4` permission alignment:
  - `cari.doc.read`, `cari.doc.create`, `cari.doc.update`, `cari.doc.post`, `cari.doc.reverse`, `cari.fx.override`
- Section `3` data model alignment:
  - `cari_documents` status + snapshot fields are primary UI source.
  - open/residual values must stay consistent with backend-managed fields.

---

## 6) PR-13: Settlement Workbench UI

### Goal

Implement settlement apply/reverse with unapplied and bank-link controls.

### Files to create

- `frontend/src/pages/cari/CariSettlementsPage.jsx`
- `frontend/src/pages/cari/cariSettlementsUtils.js`
- `frontend/src/pages/cari/cariIdempotency.js`
- `backend/scripts/test-cari-pr13-frontend-settlement-smoke.js`

### Files to update

- `frontend/src/App.jsx` (replace placeholder for `/app/cari-settlements`)
- `frontend/src/i18n/messages.js`
- `frontend/src/api/cariReports.js` (if query contract needs alignment for preview filters)
- `backend/package.json`

### Concrete route

- `/app/cari-settlements` -> `<CariSettlementsPage />`

### Backend payload rules to enforce in UI

- `allocations` required when `autoAllocate=false`.
- `allocations` must be empty when `autoAllocate=true`.
- `idempotencyKey` required for apply.
- Reverse uses `POST /settlements/{settlementBatchId}/reverse`.
- Bank attach target rules:
  - `targetType=SETTLEMENT` => `settlementBatchId` required, `unappliedCashId` empty.
  - `targetType=UNAPPLIED_CASH` => inverse.
- Bank attach idempotency:
  - `POST /api/v1/cari/bank/attach` requires `idempotencyKey`.
- Bank apply idempotency:
  - `POST /api/v1/cari/bank/apply` requires idempotency key
    (`bankApplyIdempotencyKey` preferred; `idempotencyKey` fallback accepted by validator).
  - bank apply also requires bank reference context (`bankStatementLineId` or `bankTransactionRef`).

Critical UX requirements

- Client-side idempotency key handling:
  - generate once per submit intent
  - reuse same key on retry
  - do not regenerate on double-click or refresh retry
  - keep pending key in `sessionStorage` until success/final failure
  - persist key by intent scope (not one global key):
    - minimum scope: `legalEntityId + counterpartyId`
    - recommended scope: add `direction`
    - optional hardening: include payload fingerprint (`currencyCode`, `incomingAmountTxn`, `settlementDate`)
- Final-failure classification for pending key cleanup:
  - clear pending key on final client-side failures (`400`, `401`, `403`)
  - keep pending key on retryable/server failures (`408`, `429`, `5xx`, network/timeout)
  - clear pending key on successful apply responses (including `idempotentReplay=true`)
- Bank attach/apply idempotency UX:
  - UI must generate and send idempotency keys for bank attach and bank apply submissions.
  - if full retry persistence is not implemented yet for bank flows, at minimum generate-on-submit and
    reuse key within the same in-flight attempt (double-click safe).
- Replay feedback:
  - if backend returns `idempotentReplay=true`, show info banner:
    - "Bu istek daha once uygulanmis; mevcut sonuc gosteriliyor."
- Show backend `followUpRisks` in warning panel.
- Deterministic allocation preview:
  - when `autoAllocate=true`, show expected allocation order (oldest due first)
  - preview columns: open item id/doc no, due date, open amount, expected applied, expected residual
- Keep workflows explicit:
  - settlement apply/reverse UI separate from bank attach/apply UI
  - no hidden automatic bank attach during settlement apply

### Auto-allocation preview data source (mandatory)

- Source API client: `getCariOpenItemsReport(...)` from `frontend/src/api/cariReports.js`
- Source endpoint: `GET /api/v1/cari/reports/open-items`
- Required preview filters:
  - `legalEntityId`
  - `counterpartyId`
  - `asOfDate`
- Optional preview filter:
  - `direction` (`AR`/`AP`) when direction is selected in UI
- Wiring rule:
  - Fetch open items when preview filters change and feed returned rows into
    `buildAutoAllocatePreview(openItems, incomingAmountTxn)`.
  - In preview math, use `residualAmountTxnAsOf` as the open balance source
    from report rows.
  - Do not use static/empty arrays for preview calculation except initial empty state.

### Direction safety for auto-allocation (mandatory)

- Backend settlement apply determines final direction from fetched open items.
- When open items contain mixed `AR/AP`, backend rejects with one-direction constraint.
- UX guard for PR-13:
  - require explicit direction selection before `autoAllocate=true` submit.
  - load preview rows with selected direction filter.
  - if no direction selected, disable auto-allocate submit and show guidance.
  - if mixed-direction risk is detected in fetched rows, force manual allocation flow or
    require backend enhancement in a separate PR.

### Skeleton

`frontend/src/pages/cari/cariSettlementsUtils.js`

```js
export function buildAutoAllocatePreview(
  openItems = [],
  incomingAmountTxn = 0,
) {
  const sorted = [...openItems].sort((a, b) => {
    const aDue = String(a?.dueDate || "");
    const bDue = String(b?.dueDate || "");
    if (aDue !== bDue) return aDue.localeCompare(bDue);
    return Number(a?.openItemId || 0) - Number(b?.openItemId || 0);
  });

  let remaining = Number(incomingAmountTxn || 0);
  return sorted.map((item) => {
    const openTxn = Number(item?.residualAmountTxnAsOf || 0);
    const applyTxn = Math.max(0, Math.min(openTxn, remaining));
    remaining = Math.max(0, remaining - applyTxn);
    return {
      openItemId: item?.openItemId || null,
      documentNo: item?.documentNo || null,
      dueDate: item?.dueDate || null,
      openAmountTxn: openTxn,
      expectedApplyTxn: applyTxn,
      expectedResidualTxn: Math.max(0, openTxn - applyTxn),
    };
  });
}

export function buildSettlementApplyPayload(form) {
  return {
    legalEntityId: Number(form.legalEntityId),
    counterpartyId: Number(form.counterpartyId),
    direction: form.direction || undefined,
    settlementDate: form.settlementDate,
    currencyCode: form.currencyCode,
    incomingAmountTxn: Number(form.incomingAmountTxn || 0),
    idempotencyKey: String(form.idempotencyKey || "").trim(),
    autoAllocate: Boolean(form.autoAllocate),
    useUnappliedCash: Boolean(form.useUnappliedCash),
    allocations: Array.isArray(form.allocations) ? form.allocations : [],
    fxRate: form.fxRate || undefined,
    note: form.note || undefined,
  };
}
```

`frontend/src/pages/cari/cariIdempotency.js`

```js
const STORAGE_PREFIX = "cari:settlement:pending";

export function buildSettlementIntentScope(form = {}) {
  const legalEntityId = String(form?.legalEntityId || "na");
  const counterpartyId = String(form?.counterpartyId || "na");
  const direction = String(form?.direction || "na");
  return `${legalEntityId}:${counterpartyId}:${direction}`;
}

export function buildSettlementIntentFingerprint(form = {}) {
  return JSON.stringify({
    currencyCode: String(form?.currencyCode || ""),
    incomingAmountTxn: Number(form?.incomingAmountTxn || 0),
    settlementDate: String(form?.settlementDate || ""),
  });
}

function getScopedStorageKey(intentScope) {
  return `${STORAGE_PREFIX}:${intentScope || "na:na:na"}`;
}

export function loadPendingIdempotencyKey(intentScope, intentFingerprint) {
  const raw = sessionStorage.getItem(getScopedStorageKey(intentScope)) || "";
  if (!raw) return "";
  try {
    const parsed = JSON.parse(raw);
    if (!parsed?.key) return "";
    if (intentFingerprint && parsed?.fingerprint !== intentFingerprint)
      return "";
    return String(parsed.key);
  } catch {
    return "";
  }
}

export function createPendingIdempotencyKey(intentScope, intentFingerprint) {
  const existing = loadPendingIdempotencyKey(intentScope, intentFingerprint);
  if (existing) return existing;
  const key = `CARI-SET-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  sessionStorage.setItem(
    getScopedStorageKey(intentScope),
    JSON.stringify({ key, fingerprint: intentFingerprint || "" }),
  );
  return key;
}

export function clearPendingIdempotencyKey(intentScope) {
  sessionStorage.removeItem(getScopedStorageKey(intentScope));
}

export function shouldClearPendingKeyAfterError(error) {
  const status = Number(error?.status || error?.response?.status || 0);
  return status === 400 || status === 401 || status === 403;
}

export function createEphemeralIdempotencyKey(prefix = "CARI-IDEMP") {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}
```

`frontend/src/pages/cari/CariSettlementsPage.jsx`

```jsx
import { useEffect, useMemo, useState } from "react";
import {
  applyCariSettlement,
  reverseCariSettlement,
  attachCariBankReference,
  applyCariBankSettlement,
} from "../../api/cariSettlements.js";
import { getCariOpenItemsReport } from "../../api/cariReports.js";
import { extractCariReplayAndRisks } from "../../api/cariCommon.js";
import { useAuth } from "../../auth/useAuth.js";
import {
  buildAutoAllocatePreview,
  buildSettlementApplyPayload,
} from "./cariSettlementsUtils.js";
import {
  buildSettlementIntentFingerprint,
  buildSettlementIntentScope,
  clearPendingIdempotencyKey,
  createEphemeralIdempotencyKey,
  createPendingIdempotencyKey,
  loadPendingIdempotencyKey,
  shouldClearPendingKeyAfterError,
} from "./cariIdempotency.js";

export default function CariSettlementsPage() {
  const { hasPermission } = useAuth();
  const canApply = hasPermission("cari.settlement.apply");
  const canReverse = hasPermission("cari.settlement.reverse");
  const canBankAttach = hasPermission("cari.bank.attach");
  const canBankApply = hasPermission("cari.bank.apply");
  const [previewFilters, setPreviewFilters] = useState({
    legalEntityId: "",
    counterpartyId: "",
    asOfDate: "",
    direction: "",
  });
  const [openItems, setOpenItems] = useState([]);
  const [applyForm, setApplyForm] = useState({
    legalEntityId: "",
    counterpartyId: "",
    direction: "",
    settlementDate: "",
    currencyCode: "",
    incomingAmountTxn: 0,
    idempotencyKey: "",
    autoAllocate: true,
    useUnappliedCash: false,
    allocations: [],
    fxRate: "",
    note: "",
  });

  // separate tabs/sections:
  // 1) Settlement Apply/Reverse
  // 2) Bank Attach/Apply
  // apply submit:
  // - reuse stable idempotency key until request completes
  // - parse idempotentReplay + followUpRisks
  // - clear key on success and final client failures; keep key for retryable failures
  useEffect(() => {
    const { legalEntityId, counterpartyId, asOfDate, direction } =
      previewFilters;
    if (!legalEntityId || !counterpartyId || !asOfDate) {
      setOpenItems([]);
      return;
    }

    let isMounted = true;
    async function loadPreviewOpenItems() {
      const payload = await getCariOpenItemsReport({
        legalEntityId,
        counterpartyId,
        asOfDate,
        direction: direction || undefined,
      });
      const rows = Array.isArray(payload?.rows) ? payload.rows : [];
      if (isMounted) setOpenItems(rows);
    }

    loadPreviewOpenItems();
    return () => {
      isMounted = false;
    };
  }, [
    previewFilters.legalEntityId,
    previewFilters.counterpartyId,
    previewFilters.asOfDate,
    previewFilters.direction,
  ]);

  const previewRows = useMemo(
    () =>
      buildAutoAllocatePreview(
        openItems,
        Number(applyForm.incomingAmountTxn || 0),
      ),
    [openItems, applyForm.incomingAmountTxn],
  );
  const applyIntentScope = useMemo(
    () => buildSettlementIntentScope(applyForm),
    [applyForm.legalEntityId, applyForm.counterpartyId, applyForm.direction],
  );
  const applyIntentFingerprint = useMemo(
    () => buildSettlementIntentFingerprint(applyForm),
    [
      applyForm.currencyCode,
      applyForm.incomingAmountTxn,
      applyForm.settlementDate,
    ],
  );

  useEffect(() => {
    const pendingKey = loadPendingIdempotencyKey(
      applyIntentScope,
      applyIntentFingerprint,
    );
    setApplyForm((prev) =>
      prev.idempotencyKey === pendingKey
        ? prev
        : { ...prev, idempotencyKey: pendingKey },
    );
  }, [applyIntentScope, applyIntentFingerprint]);

  async function onApply(form = applyForm) {
    if (form.autoAllocate && !form.direction) {
      throw new Error("Direction is required for auto-allocation.");
    }
    const intentScope = buildSettlementIntentScope(form);
    const intentFingerprint = buildSettlementIntentFingerprint(form);
    const idempotencyKey =
      form.idempotencyKey ||
      createPendingIdempotencyKey(intentScope, intentFingerprint);
    setApplyForm((prev) => ({ ...prev, idempotencyKey }));
    try {
      const payload = buildSettlementApplyPayload({ ...form, idempotencyKey });
      const response = await applyCariSettlement(payload);
      const replayState = extractCariReplayAndRisks(response);
      // replay is also a successful terminal result
      clearPendingIdempotencyKey(intentScope);
      return { response, replayState };
    } catch (error) {
      if (shouldClearPendingKeyAfterError(error)) {
        clearPendingIdempotencyKey(intentScope);
      }
      throw error;
    }
  }

  async function onBankAttach(form) {
    const idempotencyKey =
      form.idempotencyKey || createEphemeralIdempotencyKey("CARI-BANK-ATTACH");
    return attachCariBankReference({ ...form, idempotencyKey });
  }

  async function onBankApply(form) {
    const bankApplyIdempotencyKey =
      form.bankApplyIdempotencyKey ||
      createEphemeralIdempotencyKey("CARI-BANK-APPLY");
    return applyCariBankSettlement({ ...form, bankApplyIdempotencyKey });
  }

  return <div>Cari Settlements</div>;
}
```

### Checklist

- [ ] Build apply section (manual/auto allocate).
- [ ] Build reverse section.
- [ ] Fetch preview source data via `getCariOpenItemsReport(...)` (not static rows).
- [ ] Use `residualAmountTxnAsOf` from open-items rows as preview open balance input.
- [ ] Send required preview filters: `legalEntityId`, `counterpartyId`, `asOfDate`.
- [ ] Send optional `direction` filter when selected.
- [ ] Add bank idempotency key handling:
  - [ ] bank attach sends `idempotencyKey`
  - [ ] bank apply sends `bankApplyIdempotencyKey` (or validator-compatible fallback)
- [ ] Add in-flight submit guards for bank actions:
  - [ ] disable bank attach submit while attach request is in-flight
  - [ ] disable bank apply submit while apply request is in-flight
- [ ] Build deterministic auto-allocation preview (oldest due first).
- [ ] Add mixed-direction UX guard for auto-allocate:
  - [ ] require direction when `autoAllocate=true`
  - [ ] disable or block auto-allocate submit if direction is missing
  - [ ] show guidance when mixed-direction settlement risk exists
- [ ] Build bank attach + bank apply sections as separate workflow panel.
- [ ] Render response blocks for `allocations`, `fx`, `unapplied`, `followUpRisks`.
- [ ] Implement stable idempotency key reuse across retries/double-click and preload from `loadPendingIdempotencyKey()` after refresh.
- [ ] Avoid stale key reuse across different settlement intents:
  - [ ] key storage is intent-scoped (minimum `legalEntityId + counterpartyId`, recommended `+direction`)
  - [ ] pending key lookup also validates an intent fingerprint (`currencyCode`, `incomingAmountTxn`, `settlementDate`) before reuse
- [ ] Implement final-failure classification for pending key cleanup:
  - [ ] clear on `400/401/403`
  - [ ] keep on retryable/server/network failures
  - [ ] clear on successful apply (including `idempotentReplay=true`)
- [ ] Show idempotent replay info as non-error.
- [ ] Show `followUpRisks` as warning box.
- [ ] Add smoke script + `test:cari-pr13`:
  - [ ] verify bank attach/apply submit handlers include idempotency key fields
  - [ ] verify auto-allocate direction guard exists in submit flow
  - [ ] verify preview uses `residualAmountTxnAsOf`
  - [ ] verify settlement idempotency persistence is scoped by intent (not a single global storage key)

### Acceptance

- Settlement workbench route open rule is `any-of` settlement permissions
  (`cari.settlement.apply`, `cari.settlement.reverse`, `cari.bank.attach`, `cari.bank.apply`),
  and action buttons/panels remain strictly per-action permission gated.
- Partial/full settlement flows work through UI.
- Reverse settlement flow works.
- Unapplied consumption/create visibility works.
- Replay and idempotency behavior is explicit and support-friendly.
- Bank attach/apply actions stay explicit and separate from settlement actions.
- Auto-allocation preview is fed by `/api/v1/cari/reports/open-items` rows using
  `legalEntityId`, `counterpartyId`, `asOfDate` (and optional `direction`).
- Bank attach/apply submissions always include required idempotency keys.
- Auto-allocate UX prevents mixed-direction confusion before submit.
- Pending idempotency key cleanup matches final-failure classification rules.
- Settlement idempotency key persistence is intent-scoped and fingerprint-aware, so changed
  settlement context does not accidentally reuse stale keys.
- No regressions in `test:cari-pr07` and `test:cari-pr08`.

### Commands

```powershell
cd backend
npm run test:cari-pr13
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

### Canonical Route/Permission/Data Model Mapping (PR-13)

- Section `2.1` route coverage:
  - `/app/cari-settlements`
- Section `2.2` endpoint coverage:
  - `POST /api/v1/cari/settlements/apply`
  - `POST /api/v1/cari/settlements/{settlementBatchId}/reverse`
  - `POST /api/v1/cari/bank/attach`
  - `POST /api/v1/cari/bank/apply`
  - `GET /api/v1/cari/reports/open-items` (preview source for auto-allocation UI)
- Section `2.4` permission alignment:
  - route open (`/app/cari-settlements`) is any-of:
    `cari.settlement.apply`, `cari.settlement.reverse`, `cari.bank.attach`, `cari.bank.apply`
  - action-level enforcement stays strict per action/panel:
    - settlement apply -> `cari.settlement.apply`
    - settlement reverse -> `cari.settlement.reverse`
    - bank attach -> `cari.bank.attach`
    - bank apply -> `cari.bank.apply`
- Section `3` data model alignment:
  - `cari_settlement_batches`, `cari_settlement_allocations`, `cari_unapplied_cash`
  - `cari_open_items` and `cari_documents` open balances must reconcile after apply/reverse.

---

## 7) PR-14: Cari Audit Page

### Goal

Expose finance/support investigation screen for existing audit endpoint.

Repo reality check (locked for this plan):

- `GET /api/v1/cari/audit` is already implemented in `backend/src/services/cari.audit.service.js`.
- This PR is frontend-first, not a fake page on a stub endpoint.
- Optional backend hardening in this PR is limited to query/index/perf checks, not endpoint creation.

### Files to create

- `frontend/src/pages/cari/CariAuditPage.jsx`
- `backend/scripts/test-cari-pr14-frontend-audit-smoke.js`

### Files to update

- `frontend/src/App.jsx` (replace placeholder for `/app/cari-audit`)
- `frontend/src/i18n/messages.js`
- `backend/package.json`

### Concrete route

- `/app/cari-audit` -> `<CariAuditPage />`

### Skeleton

```jsx
import { useEffect, useState } from "react";
import { listCariAudit } from "../../api/cariAudit.js";
import { useAuth } from "../../auth/useAuth.js";

function toDayBoundsForAuditFilters({ createdFrom, createdTo }) {
  // If UI uses <input type="date"> values (YYYY-MM-DD), convert to full-day datetime bounds.
  const from = createdFrom ? new Date(`${createdFrom}T00:00:00.000`) : null;
  const to = createdTo ? new Date(`${createdTo}T23:59:59.999`) : null;

  return {
    createdFrom: from ? from.toISOString() : "",
    createdTo: to ? to.toISOString() : "",
  };
}

export default function CariAuditPage() {
  const { hasPermission } = useAuth();
  const canReadAudit = hasPermission("cari.audit.read");
  const [filters, setFilters] = useState({
    legalEntityId: "",
    action: "",
    resourceType: "",
    resourceId: "",
    actorUserId: "",
    requestId: "",
    createdFrom: "",
    createdTo: "",
    includePayload: false,
    limit: 100,
    offset: 0,
  });
  // load list, pagination
  // before listCariAudit call, normalize date-only inputs:
  // createdFrom -> 00:00:00.000, createdTo -> 23:59:59.999
  // requestId should be copyable from each row
  // payload detail panels should be collapsed by default and expanded on demand
  return <div>Cari Audit</div>;
}
```

### Checklist

- [ ] Implement filter form for all backend query params.
- [ ] Implement paginated table.
- [ ] Support `includePayload` toggle.
- [ ] Show `byAction` summary counts.
- [ ] Show `requestId` prominently.
- [ ] Add copy button for `requestId` in row/detail views.
- [ ] Keep payload panels collapsed by default; expand only when user requests.
- [ ] Keep `legalEntityId`, `action`, `resourceType`, `resourceId`, `actorUserId`, `requestId`, `createdFrom`, `createdTo` filters visible by default.
- [ ] If date-only inputs are used for `createdFrom`/`createdTo`, convert to datetime bounds before API call:
  - [ ] `createdFrom` -> start-of-day (`00:00:00.000`)
  - [ ] `createdTo` -> end-of-day (`23:59:59.999`)
- [ ] Add timezone edge test for date-bound conversion:
  - [ ] verify `toDayBoundsForAuditFilters` keeps end-of-day inclusion behavior across non-UTC offsets
        (example coverage: `UTC+04:30` environment case).
- [ ] Add smoke script + `test:cari-pr14`.

### Acceptance

- Tenant/legal-entity safe visibility preserved by backend.
- Support and finance can filter quickly by action/resource/date.
- Page is backed by live `audit_logs` data (not placeholder output).
- `requestId` and payload visibility patterns are support-friendly (copyable IDs, lazy payload read).
- Date-range UX does not accidentally exclude end-of-day rows when using date inputs.
- Date-bound conversion remains correct in timezone edge scenarios (including non-UTC offsets like `UTC+04:30`).
- Existing PR-10 docs/openapi checks remain green.

### Commands

```powershell
cd backend
npm run test:cari-pr14
npm run test:cari-pr10
```

### Global Guardrails Check (Mandatory)

- [ ] Section 1) Global Guardrails maddeleri bu PR icin tek tek dogrulandi.
- [ ] ADR-frozen kurallar korunuyor (docs/adr/adr-cari-v1.md).
- [ ] Tenant/legal-entity scope ve RBAC kontrolleri korunuyor.
- [ ] Route -> validator -> service ayrimi korunuyor.
- [ ] Endpoint kontrati degistiyse OpenAPI generator guncellendi ve cikti uretildi.
- [ ] Bu PR testi + mevcut regresyon testleri yesil.

### Canonical Route/Permission/Data Model Mapping (PR-14)

- Section `2.1` route coverage:
  - `/app/cari-audit`
- Section `2.2` endpoint coverage:
  - `GET /api/v1/cari/audit`
- Section `2.4` permission alignment:
  - `cari.audit.read`
- Section `3` data model alignment:
  - read-only view over `audit_logs`.

---

## 8) PR-15: Quality Gate Lock + Docs Sync

### Goal

Finalize Cari operational quality gate after PR-11..14.

### Files to update

- `backend/package.json`
- `backend/scripts/test-cari-pr10-quality-gate-and-docs.js`
- `docs/runbooks/cari-v1-operations.md`
- `docs/kullanim-kilavuzlari/cari-islemler-kullanim-kilavuzu.md`
- `backend/openapi.yaml` (generated output)

### Files to create

- `docs/runbooks/cari-v1-support-finance-ui-guide.md`

### Quality gate script lock (explicit)

`backend/scripts/test-cari-pr10-quality-gate-and-docs.js` must explicitly:

- include the new PR-12 contract check in gate flow:
  - `test:cari-pr12-documents-date-filter`
- run OpenAPI generation and immediately validate staleness:
  - run `npm run openapi:generate`
  - then fail if `backend/openapi.yaml` still has drift vs generated output
    (for example via `git diff --exit-code -- backend/openapi.yaml`).

### Checklist

- [ ] Add scripts:
  - [ ] `test:cari-pr11`
  - [ ] `test:cari-pr12`
  - [ ] `test:cari-pr12-documents-date-filter`
  - [ ] `test:cari-pr13`
  - [ ] `test:cari-pr14`
- [ ] Extend `test:cari-quality-gate` chain with PR-11..14 scripts plus `test:cari-pr12-documents-date-filter`.
- [ ] In `backend/scripts/test-cari-pr10-quality-gate-and-docs.js`, explicitly run/assert `test:cari-pr12-documents-date-filter`.
- [ ] Update runbook with new UI routes and operator flows.
- [ ] Add short support/finance UI operations guide:
  - [ ] document lifecycle
  - [ ] settlement idempotency behavior
  - [ ] replay behavior (`idempotentReplay`)
  - [ ] reverse behavior (document + settlement)
  - [ ] bank attach/apply meaning
  - [ ] FX override use-case and permission expectations
- [ ] Regenerate OpenAPI:
  - [ ] `cd backend && npm run openapi:generate`
- [ ] Validate OpenAPI generation in CI script (fail if stale).
- [ ] In quality gate script, stale-openapi check is executed immediately after `openapi:generate`.
- [ ] Keep `test:cari-pr10` openapi/doc assertions valid.

### Acceptance

- All Cari flows now covered:
  - Counterparty + Payment terms
  - Documents
  - Settlements
  - Reports
  - Audit
- Support/finance can operate using runbook + UI guide without dev intervention.

### Commands

```powershell
cd backend
npm run test:cari-quality-gate
npm run test:release-gate
```

### Global Guardrails Check (Mandatory)

- [ ] Section 1) Global Guardrails maddeleri bu PR icin tek tek dogrulandi.
- [ ] ADR-frozen kurallar korunuyor (docs/adr/adr-cari-v1.md).
- [ ] Tenant/legal-entity scope ve RBAC kontrolleri korunuyor.
- [ ] Route -> validator -> service ayrimi korunuyor.
- [ ] Endpoint kontrati degistiyse OpenAPI generator guncellendi ve cikti uretildi.
- [ ] Bu PR testi + mevcut regresyon testleri yesil.

### Canonical Route/Permission/Data Model Mapping (PR-15)

- Section `2.1` route coverage (quality gate scope):
  - `/app/cari-belgeler`, `/app/cari-settlements`, `/app/cari-audit`
- Section `2.2` endpoint coverage (quality gate scope):
  - all Cari document, settlement/bank, report, and audit endpoints
- Section `2.4` permission alignment:
  - quality scripts validate route/action permission assumptions remain correct
- Section `3` data model alignment:
  - no semantic drift in `cari_documents`, `cari_open_items`, `cari_settlement_batches`,
    `cari_settlement_allocations`, `cari_unapplied_cash`, `audit_logs`.

---

## 00) Test Script and NPM Script Matrix

Add the following scripts in `backend/package.json`:

- `test:cari-pr11`
- `test:cari-pr12`
- `test:cari-pr12-documents-date-filter`
- `test:cari-pr13`
- `test:cari-pr14`
- `test:contracts-pr16`
- `test:revenue-pr17a`
- `test:revenue-pr17b`
- `test:revenue-pr17c`
- `test:revenue-pr17d`
- `test:revenue-pr17-all` (aggregate chain)
- `test:contracts-revenue-pr18`

Update chained scripts:

- Extend `test:cari-quality-gate` with PR-11..14 plus date-filter contract script.
- `backend/scripts/test-cari-pr10-quality-gate-and-docs.js` must explicitly:
  - run/assert `test:cari-pr12-documents-date-filter`
  - run `openapi:generate` and then fail on stale `backend/openapi.yaml`
    using path-safe diff form: `git diff --exit-code -- backend/openapi.yaml`
- Add a new chain script:
  - `test:contracts-revenue-gate` -> PR-16 + PR-17A + PR-17B + PR-17C + PR-17D + PR-18
- Optionally extend `test:release-gate` once modules are production-ready.

---

## 00) Recommended Execution Order

1. PR-11
2. PR-12
3. PR-13
4. PR-14
5. PR-15
6. PR-16
7. PR-17A (foundation, no posting)
8. PR-17B (`380/480` + `180/280`)
9. PR-17C (`181/281` + `381/481`)
10. PR-17D (reports + reconciliation + endpoint polish)
11. PR-18 (UI deepening on completed phase data)

Do not start PR-16 before PR-15 is green.
Do not start PR-17B before PR-17A is green.
Do not start PR-17C before PR-17B is green.
Do not start PR-17D or PR-18 before PR-17C is green.

---

## 00) Final Release Checklist

- [ ] `npm run test:cari-quality-gate` passes.
- [ ] `npm run openapi:generate` completed and `backend/openapi.yaml` updated.
- [ ] Stale-openapi check after `openapi:generate` passes (no silent drift),
      using `git diff --exit-code -- backend/openapi.yaml`.
- [ ] `docs/runbooks/cari-v1-operations.md` updated with final operational flow.
- [ ] `docs/runbooks/cari-v1-support-finance-ui-guide.md` is present and current.
- [ ] `docs/kullanim-kilavuzlari/cari-islemler-kullanim-kilavuzu.md` reflects final UI.
- [ ] Periodization split (18x/28x/38x/48x) reconciles across subledger, GL, and consolidation.
- [ ] Contracts + periodization test gate passes (`PR-16 + PR-17A..D + PR-18`).
- [ ] No unresolved high-severity permission/tenant-scope issues.
