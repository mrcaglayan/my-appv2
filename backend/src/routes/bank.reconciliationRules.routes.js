import express from "express";
import { requirePermission, buildScopeFilter, assertScopeAccess } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { resolveBankAccountScope } from "../services/bank.accounts.service.js";
import {
  parseReconciliationAutoApplyInput,
  parseReconciliationAutoPreviewInput,
  parseReconciliationRuleCreateInput,
  parseReconciliationRuleListFilters,
  parseReconciliationRuleUpdateInput,
  parseReconciliationRuleIdParam,
} from "./bank.reconciliationRules.validators.js";
import {
  listReconciliationRuleRows,
  getReconciliationRuleById,
  createReconciliationRule,
  updateReconciliationRule,
} from "../services/bank.reconciliationRules.service.js";
import {
  previewBankReconciliationAutoRun,
  applyBankReconciliationAutoRun,
} from "../services/bank.reconciliationEngine.service.js";

const router = express.Router();

async function resolveRulesScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId ?? req.body?.legalEntityId ?? req.body?.legal_entity_id);
  if (legalEntityId) return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  const bankAccountId = parsePositiveInt(req.query?.bankAccountId ?? req.body?.bankAccountId ?? req.body?.bank_account_id);
  if (bankAccountId) return resolveBankAccountScope(bankAccountId, tenantId);
  return null;
}

router.get(
  "/rules",
  requirePermission("bank.reconcile.rules.read", { resolveScope: resolveRulesScope }),
  asyncHandler(async (req, res) => {
    const filters = parseReconciliationRuleListFilters(req);
    const result = await listReconciliationRuleRows({
      req,
      tenantId: filters.tenantId,
      filters,
      assertScopeAccess,
    });
    return res.json({ tenantId: filters.tenantId, ...result });
  })
);

router.get(
  "/rules/:ruleId",
  requirePermission("bank.reconcile.rules.read"),
  asyncHandler(async (req, res) => {
    const tenantId = parseReconciliationRuleListFilters(req).tenantId;
    const ruleId = parseReconciliationRuleIdParam(req);
    const row = await getReconciliationRuleById({ req, tenantId, ruleId, assertScopeAccess });
    return res.json({ tenantId, row });
  })
);

router.post(
  "/rules",
  requirePermission("bank.reconcile.rules.write", { resolveScope: resolveRulesScope }),
  asyncHandler(async (req, res) => {
    const input = parseReconciliationRuleCreateInput(req);
    const result = await createReconciliationRule({ req, input, assertScopeAccess });
    return res.status(201).json({
      tenantId: input.tenantId,
      row: result?.row || result || null,
      approval_required: Boolean(result?.approval_required),
      approval_request: result?.approval_request || null,
      idempotent: Boolean(result?.idempotent),
    });
  })
);

router.patch(
  "/rules/:ruleId",
  requirePermission("bank.reconcile.rules.write"),
  asyncHandler(async (req, res) => {
    const input = parseReconciliationRuleUpdateInput(req);
    const result = await updateReconciliationRule({ req, input, assertScopeAccess });
    return res.json({
      tenantId: input.tenantId,
      row: result?.row || result || null,
      approval_required: Boolean(result?.approval_required),
      approval_request: result?.approval_request || null,
      idempotent: Boolean(result?.idempotent),
    });
  })
);

router.post(
  "/auto/preview",
  requirePermission("bank.reconcile.auto.run", { resolveScope: resolveRulesScope }),
  asyncHandler(async (req, res) => {
    const input = parseReconciliationAutoPreviewInput(req);
    const result = await previewBankReconciliationAutoRun({
      req,
      tenantId: input.tenantId,
      filters: input,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.post(
  "/auto/apply",
  requirePermission("bank.reconcile.auto.run", { resolveScope: resolveRulesScope }),
  asyncHandler(async (req, res) => {
    const input = parseReconciliationAutoApplyInput(req);
    const result = await applyBankReconciliationAutoRun({
      req,
      tenantId: input.tenantId,
      filters: input,
      runRequestId: input.runRequestId || null,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

export default router;
