# Next PR Block / Backlog (After Current Steps)

Assumption for this page:
- PR-16, PR-17A/17B/17C/17D, PR-18, and PR-19 are implemented and green.

Goal:
- Capture items that were explicitly deferred/out-of-scope and can now be handled as follow-up PR blocks.

## Proposed Next PR Blocks

| Next PR | Scope | Why It Was Deferred | Source |
| --- | --- | --- | --- |
| PR-20 | Contract document unlink/adjustment flow (explicit corrective actions, audit-safe) | PR-16 kept links immutable and deferred corrections to a future explicit PR | `PR_STEPS.md:1619`; contracts-steps doc `:321`; contracts-steps doc `:508` |
| PR-21 | Contract amendment/versioning + partial line update semantics | PR-16 locked to full-replace lines and draft-only edits | contracts-steps doc `:40`; contracts-steps doc `:276` |
| PR-22 (Implemented) | Negative/credit contract line model | Negative/adjustment lines were intentionally out-of-scope in PR-16 | contracts-steps doc `:50`; contracts-steps doc `:296` |
| PR-23 (Implemented) | Multi-currency contract-document linking (link-level FX snapshot semantics) | Cross-currency linking was explicitly out-of-scope in PR-16 | contracts-steps doc `:240` |
| PR-24 (Implemented) | Advanced `MILESTONE`/`MANUAL` recognition schedule semantics | Only baseline date validation was in scope for PR-16 | contracts-steps doc `:301` |
| PR-25 (Implemented) | Contract-scoped linkable-documents endpoint to remove direct `cari.doc.read` dependency in Contracts UI | Marked as optional future alternative in PR-18 notes | contracts-steps doc `:961` |
| PR-26 (Implemented) | Counterparty enrichment search/sort/filter by AR/AP account fields | Explicitly out-of-scope in PR-19 | contracts-steps doc `:1262`; contracts-steps doc `:1499` |
| PR-27 (Implemented) | Reporting/index optimization pass (contract-line denormalization only if needed) | Deferred behind real index/report pressure | `PR_STEPS.md:1600`; contracts-steps doc `:247` |
| PR-28 (Implemented) | Product/module rename hardening + route alias/redirect compatibility | Rename was intentionally postponed | `PR_STEPS.md:1901`; `PR_STEPS.md:1903`; contracts-steps doc `:902`; contracts-steps doc `:904` |
| PR-29 (Implemented) | Release gate expansion to include contracts/revenue modules in final chain | Kept optional until modules are production-ready; now wired into release gate with explicit opt-out flag | `01-PR_STEPS CARI.md:1549`; `backend/package.json`; `backend/scripts/test-release-gate.js` |

## Contracts-Steps Doc Reference

- `PR-STEPS_CONTRACTS_PERIDOTS GELECEK YILLARA AIT GGELIRLER GIDERLER.md`

## Immediate Order Recommendation

1. PR-20 (unlink/adjustment)
2. PR-21 (amendment/versioning + partial update)
3. PR-22 (negative lines)
4. PR-23 (implemented)
5. PR-24 (advanced milestone/manual semantics)
6. PR-25 (implemented)
7. PR-26 (enrichment search/sort/filter)
8. PR-27 (denormalization/index optimization only if pressure exists)
9. PR-28 (implemented)
10. PR-29 (implemented)

## Validation Note

Before opening each PR above, confirm the related item is not already implemented in code and tests.
