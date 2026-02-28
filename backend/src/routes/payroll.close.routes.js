import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import {
  approveAndClosePayrollPeriod,
  getPayrollPeriodCloseDetail,
  listPayrollPeriodCloseRows,
  preparePayrollPeriodClose,
  reopenPayrollPeriodClose,
  requestPayrollPeriodClose,
  resolvePayrollPeriodCloseScope,
} from "../services/payroll.close.service.js";
import {
  parsePayrollPeriodCloseApproveInput,
  parsePayrollPeriodCloseListInput,
  parsePayrollPeriodClosePrepareInput,
  parsePayrollPeriodCloseReadInput,
  parsePayrollPeriodCloseReopenInput,
  parsePayrollPeriodCloseRequestInput,
} from "./payroll.close.validators.js";

const router = express.Router();

function resolveLegalEntityScopeFromInput(input) {
  const legalEntityId = parsePositiveInt(input?.legalEntityId ?? input?.legal_entity_id);
  if (!legalEntityId) return null;
  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: legalEntityId,
  };
}

router.get(
  "/",
  requirePermission("payroll.close.read", {
    resolveScope: async (req) => resolveLegalEntityScopeFromInput(req.query),
  }),
  asyncHandler(async (req, res) => {
    const filters = parsePayrollPeriodCloseListInput(req);
    const result = await listPayrollPeriodCloseRows({
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
  "/:closeId",
  requirePermission("payroll.close.read", {
    resolveScope: async (req, tenantId) => resolvePayrollPeriodCloseScope(req.params?.closeId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollPeriodCloseReadInput(req);
    const result = await getPayrollPeriodCloseDetail({
      req,
      tenantId: payload.tenantId,
      closeId: payload.closeId,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      closeId: payload.closeId,
      ...result,
    });
  })
);

router.post(
  "/prepare",
  requirePermission("payroll.close.prepare", {
    resolveScope: async (req) => resolveLegalEntityScopeFromInput(req.body),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollPeriodClosePrepareInput(req);
    const result = await preparePayrollPeriodClose({
      req,
      tenantId: payload.tenantId,
      userId: payload.userId,
      input: payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      legalEntityId: payload.legalEntityId,
      ...result,
    });
  })
);

router.post(
  "/:closeId/request",
  requirePermission("payroll.close.request", {
    resolveScope: async (req, tenantId) => resolvePayrollPeriodCloseScope(req.params?.closeId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollPeriodCloseRequestInput(req);
    const result = await requestPayrollPeriodClose({
      req,
      tenantId: payload.tenantId,
      userId: payload.userId,
      closeId: payload.closeId,
      note: payload.note,
      requestIdempotencyKey: payload.requestIdempotencyKey,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      closeId: payload.closeId,
      ...result,
    });
  })
);

router.post(
  "/:closeId/approve-close",
  requirePermission("payroll.close.approve", {
    resolveScope: async (req, tenantId) => resolvePayrollPeriodCloseScope(req.params?.closeId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollPeriodCloseApproveInput(req);
    const result = await approveAndClosePayrollPeriod({
      req,
      tenantId: payload.tenantId,
      userId: payload.userId,
      closeId: payload.closeId,
      note: payload.note,
      closeIdempotencyKey: payload.closeIdempotencyKey,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      closeId: payload.closeId,
      ...result,
    });
  })
);

router.post(
  "/:closeId/reopen",
  requirePermission("payroll.close.reopen", {
    resolveScope: async (req, tenantId) => resolvePayrollPeriodCloseScope(req.params?.closeId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollPeriodCloseReopenInput(req);
    const result = await reopenPayrollPeriodClose({
      req,
      tenantId: payload.tenantId,
      userId: payload.userId,
      closeId: payload.closeId,
      reason: payload.reason,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      closeId: payload.closeId,
      ...result,
    });
  })
);

export default router;
