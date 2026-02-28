Yes — **right call**. Skip bank-related integrations for now until the Bank module (statement import/reconciliation tables + APIs + UI) is actually implemented.

Below is a **PR-by-PR integration roadmap** focused only on:

* **Cari**
* **Contracts**
* **Deferred Revenue / RevRec (Gelecek Yıllara Ait Gelirler)**
* **Cash (Tediye / Tahsilat)**

---

# Integration Roadmap (Bank excluded)

## Global rules for every PR (repeat these in each PR)

Use these as non-negotiable acceptance rules:

1. **No double-posting risk**

   * Same business event should not create duplicate cash/cari/revrec records.

2. **Idempotent actions**

   * Re-running the same action (same contract billing batch, same settlement apply, same schedule generation) should be safe.

3. **Linkability first**

   * Every cross-module record should store source references (IDs) for traceability.

4. **GL posting remains explicit**

   * Integration can create drafts/links, but posting should stay controlled (unless explicitly designed otherwise).

5. **Backward compatible**

   * Existing manual flows continue to work while new synced flows are added.

6. **Auditability**

   * User, timestamp, source module/action should be recorded where possible.

---

## PR-17: Cari ↔ Cash Integration Foundation (Linking + Metadata Hardening)

### Goal

Create the **data and service foundation** to safely connect **cash tediye/tahsilat** with **cari settlements**.

### Scope

* Add strong linkage fields between:

  * cash transactions
  * cari settlements / unapplied cash
* Introduce clear source semantics:

  * `source_module`
  * `source_entity_type`
  * `source_entity_id`
  * `integration_link_status` (optional but useful)

### Touch points (likely)

* `backend/src/migrations/*` (new migration)
* `backend/src/services/cash.service.js`
* `backend/src/services/cari*.service.js` (settlement/unapplied cash logic)
* `backend/src/routes/cash*.js`
* `backend/src/routes/cari*.js`

### Key decisions in this PR

* Define canonical link directions:

  * `cash_transaction_id` on cari settlement rows (recommended)
  * optional reverse ref on cash side for quick lookup
* Standardize cash txn types for integration:

  * `RECEIPT` = tahsilat candidate
  * `PAYOUT` = tediye candidate

### Acceptance

* You can create a cash transaction with a valid cari counterparty reference and source metadata.
* Cari settlement/unapplied cash rows can store a linked cash transaction ID.
* Existing manual cash + manual cari flows still work.

### Smoke test

* Create cash receipt → no settlement yet, but link fields accept/save.
* Create cari settlement → can save linked cash txn ID.
* Duplicate link attempt is blocked or clearly handled.

---

## PR-18: Integrated Tahsilat/Tediye Flow (Cash-driven → Cari settlement)

### Goal

From a **Cash transaction** (receipt/payout), allow optional **auto-create/apply Cari settlement** in the same action.

### Scope

* New integrated backend flow:

  * Cash receipt + apply to open AR docs
  * Cash payout + apply to open AP docs
* If no docs selected:

  * create/update **unapplied cash** in Cari
* Prevent over-application and duplicate application

### API design (recommended)

Add integrated endpoints or flags on existing cash create:

* Option A (cleaner): new endpoints

  * `POST /api/v1/cash/transactions/:id/apply-cari`
* Option B (fast): extend existing create transaction payload

  * `applyCari: true`
  * `applications: [{ cariDocumentId, amount }]`

### Touch points (likely)

* `backend/src/services/cash.service.js`
* `backend/src/services/cari.settlement.service.js`
* `backend/src/routes/cash*.js`
* validators for payloads

### Acceptance

* **Tahsilat** (RECEIPT) can apply to one or more open customer docs.
* **Tediye** (PAYOUT) can apply to one or more open vendor docs.
* Unapplied remainder goes to cari unapplied cash (if allowed).
* Cash and cari sides are linked and traceable.
* Re-sending same apply request does not double-apply.

### Smoke test

* Receipt 10,000 → apply 7,000 to invoice A, 3,000 to invoice B.
* Receipt 10,000 → apply 6,000 only, remaining 4,000 stored as unapplied.
* Payout to vendor bill works the same.
* Duplicate apply call is rejected/idempotent.

---

## PR-19: Cari-driven Payment Flow (Cari settlement → optional Cash transaction)

### Goal

Support the reverse direction: from **Cari settlement UI/API**, optionally create a linked **Cash transaction**.

### Why separate from PR-18?

PR-18 handles cash-first flow. Many users work cari-first. This PR prevents “two disconnected workflows.”

### Scope

* Extend cari settlement apply endpoint(s) with payment method:

  * `paymentChannel: CASH | MANUAL`
* If `CASH`, create linked cash transaction automatically
* If `MANUAL`, no cash transaction is created (current behavior preserved)

### Touch points

* `backend/src/services/cari.settlement.service.js`
* `backend/src/services/cash.service.js`
* `backend/src/routes/cari*.js`
* frontend cari settlement forms/pages

### Acceptance

* Cari settlement can create linked cash txn when payment channel is cash.
* Manual settlements still work unchanged.
* Duplicate submission doesn’t create duplicate cash txns.

### Smoke test

* Apply cari tahsilat from invoice screen with `CASH` → cash receipt created and linked.
* Apply with `MANUAL` → no cash txn.
* Reverse settlement also handles linked cash transaction rules safely (either block if posted or reverse workflow defined).

---
Use these as non-negotiable acceptance rules:

1. **No double-posting risk**

   * Same business event should not create duplicate cash/cari/revrec records.

2. **Idempotent actions**

   * Re-running the same action (same contract billing batch, same settlement apply, same schedule generation) should be safe.

3. **Linkability first**

   * Every cross-module record should store source references (IDs) for traceability.

4. **GL posting remains explicit**

   * Integration can create drafts/links, but posting should stay controlled (unless explicitly designed otherwise).

5. **Backward compatible**

   * Existing manual flows continue to work while new synced flows are added.

6. **Auditability**

   * User, timestamp, source module/action should be recorded where possible.

---

## PR-20: Frontend Integration UX for Tahsilat / Tediye (Pickers + Guided Flow)

### Goal

Make the integration usable and safe (no manual ID typing).

### Scope

* Cash transaction form improvements:

  * Cari counterparty picker
  * Open document picker (for apply)
  * “Apply now” section (optional)
* Cari settlement page improvements:

  * “Create linked cash transaction” toggle
  * Payment channel selector (Cash / Manual)
* Display linked records in both modules:

  * Cash txn detail → linked cari settlement(s)
  * Cari settlement/doc → linked cash txn

### Touch points (frontend)

* `frontend/src/pages/cash/*`
* `frontend/src/pages/cari/*`
* `frontend/src/api/cash*.js`
* `frontend/src/api/cariSettlements.js`
* shared picker/search components (if you have them)

### Acceptance

* User can complete tahsilat/tediye integration without entering raw IDs.
* Linked record badges/links are visible.
* Validation errors are clear (over-apply, wrong counterparty, closed session, etc.)

### Smoke test

* From cash receipt screen, search customer + select open invoices + apply.
* From cari settlement screen, create linked cash receipt.
* Open both detail pages and confirm cross-links are visible.

---

## PR-21: Contracts → Cari Documents Auto-Billing (Invoice / Advance generation)

### Goal

Move from manual link-only behavior to **contract-driven billing document generation**.

### Scope

* Generate cari documents from contracts / contract lines:

  * invoice
  * advance billing / advance receipt document (based on your document model)
  * adjustment (optional in v1)
* Auto-create contract-document link immediately after generation
* Use contract metadata for defaults:

  * counterparty
  * currency
  * dates
  * references

### API design (recommended)

* `POST /api/v1/contracts/:id/generate-billing`

  * payload can support:

    * selected contract lines
    * billing date
    * doc type
    * amount strategy (full / partial / milestone)

### Touch points

* `backend/src/services/contracts.service.js`
* `backend/src/services/cari*.service.js` (document creation)
* `backend/src/routes/contracts.js`
* validators
* frontend contract detail page

### Acceptance

* Contract billing action creates valid cari document(s)
* Contract-document links are created automatically
* Repeating same generation with same reference doesn’t duplicate (idempotency key or business key)
* Manual document linking still works

### Smoke test

* Contract with 2 lines → generate invoice → cari docs created + linked
* Contract advance billing → creates proper cari doc type + link
* Duplicate click → no duplicate doc batch

---
Use these as non-negotiable acceptance rules:

1. **No double-posting risk**

   * Same business event should not create duplicate cash/cari/revrec records.

2. **Idempotent actions**

   * Re-running the same action (same contract billing batch, same settlement apply, same schedule generation) should be safe.

3. **Linkability first**

   * Every cross-module record should store source references (IDs) for traceability.

4. **GL posting remains explicit**

   * Integration can create drafts/links, but posting should stay controlled (unless explicitly designed otherwise).

5. **Backward compatible**

   * Existing manual flows continue to work while new synced flows are added.

6. **Auditability**

   * User, timestamp, source module/action should be recorded where possible.

---

## PR-22: Contracts → RevRec Auto Schedule Generation (Deferred Revenue sync)

### Goal

Automatically generate **RevRec schedules** from contract lines / linked billing docs using your existing revrec schema.

### Scope

* Use contract line fields already in schema:

  * `recognition_method`
  * `deferred_account_id`
  * `revenue_account_id`
* Auto-create schedule lines and populate source refs:

  * `source_contract_id`
  * `source_contract_line_id`
  * `source_cari_document_id` (when available)
* Support draft generation first (recommended), not auto-posting

### API design (recommended)

* `POST /api/v1/contracts/:id/generate-revrec`

  * or line-level endpoint
* Optional modes:

  * by contract line
  * by linked cari document
  * regenerate missing only

### Touch points

* `backend/src/services/contracts.service.js`
* `backend/src/services/revenueRecognition*.js`
* `backend/src/routes/contracts.js` and/or revrec routes
* RevRec frontend page / Contract detail page (button + status)

### Acceptance

* RevRec schedules generate from contract data without manual re-entry.
* Source FK fields are populated correctly.
* Re-run does not create duplicate schedule lines for same source/bucket.

### Smoke test

* Contract line with straight-line recognition → monthly schedule generated.
* Contract line linked to billing doc → schedule lines include source cari doc ID.
* Re-run “generate missing only” produces no duplicates.

---
Use these as non-negotiable acceptance rules:

1. **No double-posting risk**

   * Same business event should not create duplicate cash/cari/revrec records.

2. **Idempotent actions**

   * Re-running the same action (same contract billing batch, same settlement apply, same schedule generation) should be safe.

3. **Linkability first**

   * Every cross-module record should store source references (IDs) for traceability.

4. **GL posting remains explicit**

   * Integration can create drafts/links, but posting should stay controlled (unless explicitly designed otherwise).

5. **Backward compatible**

   * Existing manual flows continue to work while new synced flows are added.

6. **Auditability**

   * User, timestamp, source module/action should be recorded where possible.

---

## PR-23: RevRec Posting Account Derivation from Contract Line (Line-level accounting wins)

### Goal

Use **contract-line accounts** during revrec posting, instead of relying only on global purpose mappings.

### Scope

* RevRec posting logic account resolution order:

  1. `contract_line.deferred_account_id` / `revenue_account_id`
  2. fallback to existing global `journal_purpose_accounts`
* Validate missing accounts cleanly (draft/posting error message)

### Why this matters

You already store line-level accounting intent in contracts. This PR makes that data actually drive journals.

### Touch points

* `backend/src/services/revenueRecognition*.js` (posting logic)
* journal posting helper/service
* tests for account resolution

### Acceptance

* RevRec posting uses line-level accounts when available.
* Falls back to global mapping only if line-level values are empty.
* No regression in current revrec posting flows.

### Smoke test

* Contract line has custom deferred/revenue accounts → journal uses them.
* Another line without custom accounts → fallback mapping used.
* Missing both → clear validation error.

---
Use these as non-negotiable acceptance rules:

1. **No double-posting risk**

   * Same business event should not create duplicate cash/cari/revrec records.

2. **Idempotent actions**

   * Re-running the same action (same contract billing batch, same settlement apply, same schedule generation) should be safe.

3. **Linkability first**

   * Every cross-module record should store source references (IDs) for traceability.

4. **GL posting remains explicit**

   * Integration can create drafts/links, but posting should stay controlled (unless explicitly designed otherwise).

5. **Backward compatible**

   * Existing manual flows continue to work while new synced flows are added.

6. **Auditability**

   * User, timestamp, source module/action should be recorded where possible.

---

## PR-24: Contract Financial Rollups (Billed / Collected / Deferred / Recognized)

### Goal

Make Contracts page the operational + accounting summary screen.

### Scope

Add contract-level KPIs, computed from linked modules:

* **Billed amount** (linked cari docs)
* **Collected amount** (via cari settlements / cash-linked settlements)
* **Deferred balance** (revrec unrecognized)
* **Recognized to date** (posted revrec)
* **Open receivable/payable** (if relevant to your contract type)

### Implementation notes

* Start with **computed read model** (service query/aggregation)
* Persist snapshot later only if performance becomes an issue

### Touch points

* `backend/src/services/contracts.service.js` (summary aggregations)
* `backend/src/routes/contracts.js` (summary endpoint fields)
* `frontend/src/pages/contracts/*` (KPI cards, status bars)

### Acceptance

* Contract detail API returns rollups consistently.
* Rollups match underlying cari/revrec/cash links.
* Null/partial states handled (contract with no billing yet, billing but no revrec, etc.)

### Smoke test

* Contract billed but no collection → billed > 0, collected = 0
* Contract billed + partial collection + partial recognition → all KPIs reconcile
* Contract with no linked records → zero-safe response

---
Use these as non-negotiable acceptance rules:

1. **No double-posting risk**

   * Same business event should not create duplicate cash/cari/revrec records.

2. **Idempotent actions**

   * Re-running the same action (same contract billing batch, same settlement apply, same schedule generation) should be safe.

3. **Linkability first**

   * Every cross-module record should store source references (IDs) for traceability.

4. **GL posting remains explicit**

   * Integration can create drafts/links, but posting should stay controlled (unless explicitly designed otherwise).

5. **Backward compatible**

   * Existing manual flows continue to work while new synced flows are added.

6. **Auditability**

   * User, timestamp, source module/action should be recorded where possible.

---

## PR-25: Settlement Posting Refinement (Source-aware posting + FX fallback)

### Goal

Upgrade cari settlement posting quality (still non-bank), especially for cash/manual scenarios and FX robustness.

### Scope

* Improve settlement posting derivation by source/context:

  * cash-linked
  * manual
  * on-account/unapplied apply
* FX handling refinement:

  * fallback to nearest prior rate (or configured fallback)
  * clear error if no valid rate
* Keep existing generic logic as fallback for compatibility

### Touch points

* `backend/src/services/cari.settlement.service.js`
* FX/rates utility/service
* GL posting helpers
* tests for FX and posting cases

### Acceptance

* Settlement posting behavior is deterministic by source type.
* FX date gaps no longer fail unnecessarily (if fallback enabled).
* Existing non-FX behavior unchanged.

### Smoke test

* Cash-linked settlement in FX currency posts correctly.
* Exact-date rate missing, prior-date rate available → uses fallback (if configured).
* Manual local-currency settlement remains unchanged.

---
Use these as non-negotiable acceptance rules:

1. **No double-posting risk**

   * Same business event should not create duplicate cash/cari/revrec records.

2. **Idempotent actions**

   * Re-running the same action (same contract billing batch, same settlement apply, same schedule generation) should be safe.

3. **Linkability first**

   * Every cross-module record should store source references (IDs) for traceability.

4. **GL posting remains explicit**

   * Integration can create drafts/links, but posting should stay controlled (unless explicitly designed otherwise).

5. **Backward compatible**

   * Existing manual flows continue to work while new synced flows are added.

6. **Auditability**

   * User, timestamp, source module/action should be recorded where possible.

---

## PR-26: Cash Cross-OU Transfer / Transit Workflow (v2, optional after core sync)

### Goal

Implement proper **CASH_IN_TRANSIT** flow for transfers between registers/OUs.

### Scope

* Pair transfer-out and transfer-in transactions
* Transit state machine:

  * initiated
  * in_transit
  * received
  * canceled/reversed
* Prevent orphaned or one-sided transfers

### Why later

This is important, but not needed for the first integration wave of tahsilat/tediye + contracts/revrec sync.

### Touch points

* `backend/src/services/cash.service.js`
* cash routes + validators
* cash UI transfer screens/details

### Acceptance

* Transfer-out creates transit record
* Transfer-in closes the same transit record
* Reversal rules are controlled and auditable

### Smoke test

* Register A → Register B transfer completes with two linked txns + transit state
* Duplicate receive is blocked
* Reversal preserves audit trail

---
Use these as non-negotiable acceptance rules:

1. **No double-posting risk**

   * Same business event should not create duplicate cash/cari/revrec records.

2. **Idempotent actions**

   * Re-running the same action (same contract billing batch, same settlement apply, same schedule generation) should be safe.

3. **Linkability first**

   * Every cross-module record should store source references (IDs) for traceability.

4. **GL posting remains explicit**

   * Integration can create drafts/links, but posting should stay controlled (unless explicitly designed otherwise).

5. **Backward compatible**

   * Existing manual flows continue to work while new synced flows are added.

6. **Auditability**

   * User, timestamp, source module/action should be recorded where possible.

---

# Suggested execution order (practical)

If you want the cleanest value fast:

1. **PR-17** foundation links (done)
2. **PR-18** cash-first tahsilat/tediye apply (done)
3. **PR-20** frontend UX for integrated flow (done)
4. **PR-19** cari-first reverse integration (done)
5. **PR-21** contracts → cari auto billing (done)
6. **PR-22** contracts → revrec schedule auto-generation (done)
7. **PR-23** revrec account derivation from contract lines (done)
8. **PR-24** contract rollup KPIs (done)
9. **PR-25** settlement posting/FX refinement (done)
10. **PR-26** cross-OU transit 

---

# One extra recommendation (important)

Before PR-18, add a tiny **integration identity strategy** (can be in PR-17):

* `integration_event_uid` (UUID/string)
* unique constraint per source action

This will save you from duplicate clicks / retries causing duplicate settlement or billing actions.

---


at the end, after analyzing through the all application edit the related manuals in the same style for the changes we have done in 00-CASH CARI INTEGRATION.md file that others explained in detailed with examples etc. also if you see missing ones add them as wellfor the relevant flows in the relevant manuals. 