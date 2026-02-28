import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { requireTenantId } from "./cash.validators.common.js";
import { resolveBankAccountScope } from "../services/bank.accounts.service.js";
import { resolveBankStatementLineScope } from "../services/bank.statements.service.js";
import {
  getReconciliationSuggestionsForLine,
  ignoreReconciliationLine,
  listReconciliationAuditRows,
  listReconciliationQueueRows,
  matchReconciliationLine,
  unignoreReconciliationLine,
  unmatchReconciliationLine,
} from "../services/bank.reconciliation.service.js";
import {
  parseReconciliationAuditFilters,
  parseReconciliationIgnoreInput,
  parseReconciliationLineIdParam,
  parseReconciliationMatchInput,
  parseReconciliationQueueFilters,
  parseReconciliationUnignoreInput,
  parseReconciliationUnmatchInput,
} from "./bank.reconciliation.validators.js";

const router = express.Router();

async function resolveQueueListScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
  if (legalEntityId) {
    return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  }
  const bankAccountId = parsePositiveInt(req.query?.bankAccountId);
  if (bankAccountId) {
    return resolveBankAccountScope(bankAccountId, tenantId);
  }
  return null;
}

async function resolveAuditListScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
  if (legalEntityId) {
    return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  }
  const statementLineId = parsePositiveInt(req.query?.statementLineId ?? req.query?.statement_line_id);
  if (statementLineId) {
    return resolveBankStatementLineScope(statementLineId, tenantId);
  }
  return null;
}

router.get(
  "/queue",
  requirePermission("bank.reconcile.read", {
    resolveScope: resolveQueueListScope,
  }),
  asyncHandler(async (req, res) => {
    const filters = parseReconciliationQueueFilters(req);
    const result = await listReconciliationQueueRows({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({
      tenantId: filters.tenantId,
      ...result,
    });
  })
);

router.get(
  "/queue/:lineId/suggestions",
  requirePermission("bank.reconcile.read", {
    resolveScope: async (req, tenantId) => {
      return resolveBankStatementLineScope(req.params?.lineId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const lineId = parseReconciliationLineIdParam(req);
    const userId = parsePositiveInt(req.user?.userId);
    const result = await getReconciliationSuggestionsForLine({
      req,
      tenantId,
      lineId,
      userId,
      assertScopeAccess,
    });
    return res.json({
      tenantId,
      ...result,
    });
  })
);

router.post(
  "/queue/:lineId/match",
  requirePermission("bank.reconcile.write", {
    resolveScope: async (req, tenantId) => {
      return resolveBankStatementLineScope(req.params?.lineId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseReconciliationMatchInput(req);
    const result = await matchReconciliationLine({
      req,
      tenantId: payload.tenantId,
      lineId: payload.lineId,
      matchInput: payload,
      userId: payload.userId,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      ...result,
    });
  })
);

router.post(
  "/queue/:lineId/unmatch",
  requirePermission("bank.reconcile.write", {
    resolveScope: async (req, tenantId) => {
      return resolveBankStatementLineScope(req.params?.lineId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseReconciliationUnmatchInput(req);
    const result = await unmatchReconciliationLine({
      req,
      tenantId: payload.tenantId,
      lineId: payload.lineId,
      unmatchInput: payload,
      userId: payload.userId,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      ...result,
    });
  })
);

router.post(
  "/queue/:lineId/ignore",
  requirePermission("bank.reconcile.write", {
    resolveScope: async (req, tenantId) => {
      return resolveBankStatementLineScope(req.params?.lineId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseReconciliationIgnoreInput(req);
    const result = await ignoreReconciliationLine({
      req,
      tenantId: payload.tenantId,
      lineId: payload.lineId,
      ignoreInput: payload,
      userId: payload.userId,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      ...result,
    });
  })
);

router.post(
  "/queue/:lineId/unignore",
  requirePermission("bank.reconcile.write", {
    resolveScope: async (req, tenantId) => {
      return resolveBankStatementLineScope(req.params?.lineId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseReconciliationUnignoreInput(req);
    const result = await unignoreReconciliationLine({
      req,
      tenantId: payload.tenantId,
      lineId: payload.lineId,
      unignoreInput: payload,
      userId: payload.userId,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      ...result,
    });
  })
);

router.get(
  "/audit",
  requirePermission("bank.reconcile.read", {
    resolveScope: resolveAuditListScope,
  }),
  asyncHandler(async (req, res) => {
    const filters = parseReconciliationAuditFilters(req);
    const result = await listReconciliationAuditRows({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({
      tenantId: filters.tenantId,
      ...result,
    });
  })
);

export default router;
