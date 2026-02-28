import express from "express";
import { requirePermission, buildScopeFilter, assertScopeAccess } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { resolveBankAccountScope } from "../services/bank.accounts.service.js";
import {
  resolveBankReconciliationExceptionScope,
  listReconciliationExceptionRows,
  getReconciliationExceptionById,
  assignReconciliationException,
  resolveReconciliationException,
  ignoreReconciliationException,
  retryReconciliationException,
} from "../services/bank.reconciliationExceptions.service.js";
import {
  parseReconciliationExceptionListFilters,
  parseReconciliationExceptionIdParam,
  parseReconciliationExceptionAssignInput,
  parseReconciliationExceptionResolveInput,
  parseReconciliationExceptionIgnoreInput,
  parseReconciliationExceptionRetryInput,
} from "./bank.reconciliationExceptions.validators.js";

const router = express.Router();

async function resolveExceptionListScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
  if (legalEntityId) return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  const bankAccountId = parsePositiveInt(req.query?.bankAccountId);
  if (bankAccountId) return resolveBankAccountScope(bankAccountId, tenantId);
  return null;
}

router.get(
  "/exceptions",
  requirePermission("bank.reconcile.exceptions.read", { resolveScope: resolveExceptionListScope }),
  asyncHandler(async (req, res) => {
    const filters = parseReconciliationExceptionListFilters(req);
    const result = await listReconciliationExceptionRows({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: filters.tenantId, ...result });
  })
);

router.get(
  "/exceptions/:exceptionId",
  requirePermission("bank.reconcile.exceptions.read", {
    resolveScope: (req, tenantId) => resolveBankReconciliationExceptionScope(req.params?.exceptionId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const tenantId = parseReconciliationExceptionListFilters(req).tenantId;
    const exceptionId = parseReconciliationExceptionIdParam(req);
    const row = await getReconciliationExceptionById({ req, tenantId, exceptionId, assertScopeAccess });
    return res.json({ tenantId, row });
  })
);

router.post(
  "/exceptions/:exceptionId/assign",
  requirePermission("bank.reconcile.exceptions.write", {
    resolveScope: (req, tenantId) => resolveBankReconciliationExceptionScope(req.params?.exceptionId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseReconciliationExceptionAssignInput(req);
    const row = await assignReconciliationException({
      req,
      tenantId: input.tenantId,
      exceptionId: input.exceptionId,
      assignedToUserId: input.assignedToUserId,
      userId: input.userId,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, row });
  })
);

router.post(
  "/exceptions/:exceptionId/resolve",
  requirePermission("bank.reconcile.exceptions.write", {
    resolveScope: (req, tenantId) => resolveBankReconciliationExceptionScope(req.params?.exceptionId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseReconciliationExceptionResolveInput(req);
    const result = await resolveReconciliationException({
      req,
      tenantId: input.tenantId,
      exceptionId: input.exceptionId,
      resolutionCode: input.resolutionCode,
      resolutionNote: input.resolutionNote,
      userId: input.userId,
      assertScopeAccess,
    });
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
  "/exceptions/:exceptionId/ignore",
  requirePermission("bank.reconcile.exceptions.write", {
    resolveScope: (req, tenantId) => resolveBankReconciliationExceptionScope(req.params?.exceptionId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseReconciliationExceptionIgnoreInput(req);
    const result = await ignoreReconciliationException({
      req,
      tenantId: input.tenantId,
      exceptionId: input.exceptionId,
      resolutionNote: input.resolutionNote,
      userId: input.userId,
      assertScopeAccess,
    });
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
  "/exceptions/:exceptionId/retry",
  requirePermission("bank.reconcile.exceptions.write", {
    resolveScope: (req, tenantId) => resolveBankReconciliationExceptionScope(req.params?.exceptionId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseReconciliationExceptionRetryInput(req);
    const result = await retryReconciliationException({
      req,
      tenantId: input.tenantId,
      exceptionId: input.exceptionId,
      note: input.note,
      userId: input.userId,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

export default router;
