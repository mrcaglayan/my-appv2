# 05-IMPROVEMENTS

## Scope
Execution tracker for the post-`04-BANKS_AND_PAYROLLS_ESM.md` improvement wave.
Format intentionally matches your checklist style so we can mark items as we ship.

## Baseline Notes (Repo Reality)
- Latest migration is `m068_*`; next new migration must start from `m069_*`.
- Current auth/profile route style is `/me` and `/auth/*`.
- `Exceptions Workbench`, `Jobs`, `Retention`, and much of idempotency are already implemented and should be treated as extension/hardening work, not net-new foundations.

## Execution Tracker (Update As You Implement)
Tag legend: `(hot: yes)` means likely touches conflict-prone files (`AppLayout.jsx`, `sidebarConfig.js`, `messages.js`, `App.jsx`).

### UX + Product Flow (ordered)
- [x] PR-UX01 Working Context Provider (LE/OU/Period) - header + provider foundations (implemented, hot: yes)
- [x] PR-UX02 Apply Working Context defaults on existing pages/forms (implemented, hot: no)
- [x] PR-UX03 Persist filters/table prefs in local storage hooks (implemented: filter persistence + reusable hooks; advanced table prefs remain in UX24, hot: low)
- [x] PR-UX04 Server-side user context preferences (`/me/preferences`, migration `m067_*`) (implemented: `user_preferences` + `GET/PUT /me/preferences` + `WorkingContextProvider` server hydrate/sync)
- [x] PR-UX05 Permissions visible in sidebar (disabled + reason, not hidden) (implemented: permission-locked items are shown with reason + copy access request action, hot: yes)

- [x] PR-UX06 Upgrade `Dashboard.jsx` into actionable finance console (implemented: KPI cards + queue links + readiness + scoped refresh, hot: no)
- [x] PR-UX07 Exception queue tabs + queue counts (implemented: tabs `All/Needs Review/Approval/Stuck/Mine/Resolved` + counts from `summary.by_status` with supplemental `stuck/mine` counters, hot: no)
- [x] PR-UX08 Add `sla_due_at` + urgency sort (implemented: migration `m068_*` + backend SLA enrichment + urgency ordering + FE sort selector/SLA badges, hot: no)
- [x] PR-UX09 Exception bulk actions (preferred: backend bulk endpoints; fallback: FE batching with concurrency control) (implemented: `POST /api/v1/exceptions/workbench/bulk-action` + multi-select bulk toolbar in workbench UI, hot: no)

- [x] PR-CORE05 Extend existing backend error envelope + FE centralized toasts/handling (`message` + `requestId` already exists) (implemented: API error toasts + shared app toast channel; core cash/journal/exceptions success messages now toastified)
- [x] PR-CORE01 Standardize pagination contracts across modules (implemented: shared `backend/src/utils/pagination.js` + applied to Cari Documents, Cash Transactions/Transit, Exceptions Workbench)

- [x] PR-UX10 Shared `Combobox` component (new `frontend/src/components`) (implemented: reusable accessible combobox with keyboard nav + loading/empty states + custom option rendering, hot: no)
- [x] PR-UX11 Counterparty typeahead in Cari Documents/Settlements (implemented: shared Combobox wired to Cari Documents filter/create/edit and Cari Settlements apply/bank-apply counterparty selectors, hot: no)
- [x] PR-UX12 GL account lookup with searchable API (`q`) + breadcrumb display (implemented: `GET /api/v1/gl/accounts` now supports `q` + breadcrumb fields and Cari Counterparty/Cari Settlements account selectors use server-side q lookup with breadcrumb descriptions, hot: no)
- [ ] PR-UX13-A Inline counterparty create from lookups (API exists; not started)
- [ ] PR-UX13-B Inline payment term create (backend write endpoint required first) (blocked)

- [ ] PR-UX14 Shared lifecycle rules + `StatusTimeline` component (not started)
- [ ] PR-UX15 Apply lifecycle UI to Cari Documents (not started)
- [ ] PR-UX16 Apply lifecycle UI to Cash Transactions/Sessions (not started)
- [ ] PR-UX17 Apply lifecycle UI to Payroll flows (not started)

- [ ] PR-UX18 Deep-link support (`documentId/journalId/exceptionId`) (not started)
- [ ] PR-UX19 Related panel (GL/open items/exceptions/audit) + source-link strategy (blocked by backend linking design)

- [ ] PR-UX20 Evidence storage foundation (DB + adapter + routes) (not started)
- [ ] PR-UX21 Evidence uploader UI + attach to Cari Docs (not started)
- [ ] PR-UX22 Evidence-required policy checks for risky actions (not started)

- [ ] PR-UX23 Shared CSV export helper + list page export actions (not started)
- [ ] PR-UX24 Column chooser + sticky headers + per-page table prefs (not started)
- [ ] PR-UX25 Saved Views (server-side, per-user) (not started)

- [ ] PR-UX26 Smarter defaults in Cari forms (not started)
- [ ] PR-UX27 Cari clone + recurring templates (not started)
- [ ] PR-UX28 Cash transaction templates/presets (not started)

- [ ] PR-UX29 Internal comments v1 (not started)
- [ ] PR-UX30 Mentions + in-app notifications (not started)
- [ ] PR-UX31 Ops status note / blocked reason (not started)

- [ ] PR-UX32 Invite flow (copy-link, no SMTP dependency) (not started)
- [ ] PR-UX33 Password reset token flow (not started)
- [ ] PR-UX34 Tenant feature flags (`tenant_features` + `/me/features`) (not started)
- [ ] PR-UX35 Usage + audit export endpoints/UI (not started)

- [ ] PR-CORE02 Idempotency standardization for remaining risky endpoints (partial foundation exists)
- [ ] PR-CORE03 Optimistic locking (`row_version`) on editable entities (not started)
- [ ] PR-CORE04 Job progress/retry UX on top of H02 jobs engine (partial foundation exists)

## Follow-up RS Tracker (Improvement Scope Only)

### Scope note
Intentional not-yet-implemented placeholders (Stock, Fixed Assets, generic Reports, and period-end placeholder submodules) are excluded from this tracker by request.

### Wiring follow-ups to prevent misses in implemented modules
- [ ] RS-WIRE-01 For each improvement PR, enforce same-PR wiring across:
  `App.jsx route`, `sidebarConfig.js` entry, `messages.js` labels, and related API client wiring
- [ ] RS-WIRE-02 Add a lightweight CI check for new implemented routes so a page cannot ship without sidebar + i18n wiring
- [ ] RS-WIRE-03 Add release-gate smoke coverage for each newly implemented improvement page before marking `[x]`

## Dependency Follow-ups (Non-placeholder blockers)
- [ ] RS-DEP-01 Payment term write API for UX13-B (`POST /api/v1/cari/payment-terms` + permission + frontend client)
- [ ] RS-DEP-02 Source-linking contract for UX19 Related Panel:
  choose minimal contract (`journal_source_links` table OR `source_ref_type` + `source_ref_id` on journals), write links during posting, then ship UI
- [x] RS-DEP-03 Global frontend error/toast strategy required by CORE05 (`frontend/src/api/client.js` interceptor + UI surface) (implemented)

## Working Rules While Executing
- Keep additive migrations only, no destructive changes.
- Batch hot-file edits (`AppLayout.jsx`, `App.jsx`, `sidebarConfig.js`, `messages.js`) to reduce merge conflicts.
- Keep route-level permission guards even when adding better UX visibility.
- Add smoke/test scripts per PR as done in Bank/Payroll wave.

## Acceptance + Smoke Placeholders
- [ ] PR-UX02 acceptance: context defaults are applied only to empty fields; user-entered values are never overwritten
  smoke: `backend/scripts/test-ux-prux02-context-defaults.js` (or FE e2e equivalent)
- [x] PR-UX03 acceptance: filters survive refresh/navigation; reset clears state + storage
  smoke: `backend/scripts/test-ux-prux03-persisted-filters.js` (or FE e2e equivalent)
- [x] PR-UX04 acceptance: working context is restored from server preferences across devices while preserving localStorage fallback
  smoke: `backend/scripts/test-ux-prux04-me-preferences.js` (to add)
- [x] PR-UX05 acceptance: sidebar keeps permission-gated items visible as disabled with lock reason and copy-access-request action
  smoke: `backend/scripts/test-ux-prux05-sidebar-permissions-visible.js` (to add)
- [x] PR-UX06 acceptance: dashboard presents actionable queues (`To Post`, `To Settle`, `Exceptions`, `Period Close Blockers`) and links with scoped refresh
  smoke: `backend/scripts/test-ux-prux06-finance-console.js` (to add)
- [x] PR-UX07 acceptance: exception queue tabs drive filters and show queue counts without backend schema changes
  smoke: `backend/scripts/test-ux-prux07-exception-queues.js` (to add)
- [x] PR-UX08 acceptance: exceptions expose `sla_due_at` and support urgency-first ordering in workbench UI
  smoke: `backend/scripts/test-ux-prux08-exception-sla-urgency.js` (to add)
- [x] PR-UX09 acceptance: exception workbench supports multi-select bulk claim/resolve/ignore/reopen with partial-success reporting
  smoke: `backend/scripts/test-ux-prux09-exception-bulk-actions.js`
- [x] PR-UX10 acceptance: shared combobox supports reusable typeahead UX with keyboard navigation, a11y roles, and loading/empty rendering hooks
  smoke: `backend/scripts/test-ux-prux10-shared-combobox.js`
- [x] PR-UX11 acceptance: Cari Documents and Cari Settlements expose counterparty lookup typeahead controls that drive `counterpartyId` form/filter fields
  smoke: `backend/scripts/test-ux-prux11-counterparty-typeahead.js`
- [x] PR-UX12 acceptance: GL account lookups support backend `q` search and show breadcrumb paths in Cari account selector UIs
  smoke: `backend/scripts/test-ux-prux12-gl-account-lookup-and-breadcrumb.js`
- [x] PR-CORE05 acceptance: standardized user-facing error handling + copyable requestId/details
  smoke: `backend/scripts/test-ux-prcore05-error-envelope.js`
- [x] PR-CORE01 acceptance: key list endpoints return consistent `rows + total + limit + offset` with `pagination` metadata
  smoke: `backend/scripts/test-ux-prcore01-pagination-contracts.js` (to add)

## Immediate Next Step
- Continue with `PR-UX13-A`.
- After each merged PR, update this tracker line from `[ ]` to `[x]` with a short `(implemented)` note.
