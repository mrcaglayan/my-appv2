import express from "express";
import { assertScopeAccess, requirePermission } from "../middleware/rbac.js";
import { asyncHandler } from "./_utils.js";
import {
  approveApplyPayrollManualSettlementRequest,
  listPayrollManualSettlementRequests,
  rejectPayrollManualSettlementRequest,
  resolvePayrollLiabilityScope,
  resolvePayrollSettlementOverrideRequestScope,
  createPayrollManualSettlementRequest,
} from "../services/payroll.settlementOverrides.service.js";
import {
  parsePayrollManualSettlementRequestApproveInput,
  parsePayrollManualSettlementRequestCreateInput,
  parsePayrollManualSettlementRequestListInput,
  parsePayrollManualSettlementRequestRejectInput,
} from "./payroll.settlementOverrides.validators.js";

const router = express.Router();

router.get(
  "/liabilities/:liabilityId/manual-settlement-requests",
  requirePermission("payroll.settlement.override.read", {
    resolveScope: async (req, tenantId) =>
      resolvePayrollLiabilityScope(req.params?.liabilityId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollManualSettlementRequestListInput(req);
    const result = await listPayrollManualSettlementRequests({
      req,
      tenantId: payload.tenantId,
      liabilityId: payload.liabilityId,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      liabilityId: payload.liabilityId,
      ...result,
    });
  })
);

router.post(
  "/liabilities/:liabilityId/manual-settlement-requests",
  requirePermission("payroll.settlement.override.request", {
    resolveScope: async (req, tenantId) =>
      resolvePayrollLiabilityScope(req.params?.liabilityId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollManualSettlementRequestCreateInput(req);
    const result = await createPayrollManualSettlementRequest({
      req,
      tenantId: payload.tenantId,
      liabilityId: payload.liabilityId,
      userId: payload.userId,
      input: payload,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: payload.tenantId,
      liabilityId: payload.liabilityId,
      ...result,
    });
  })
);

router.post(
  "/manual-settlement-requests/:requestId/approve-apply",
  requirePermission("payroll.settlement.override.approve", {
    resolveScope: async (req, tenantId) =>
      resolvePayrollSettlementOverrideRequestScope(req.params?.requestId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollManualSettlementRequestApproveInput(req);
    const result = await approveApplyPayrollManualSettlementRequest({
      req,
      tenantId: payload.tenantId,
      requestId: payload.requestId,
      userId: payload.userId,
      decisionNote: payload.decisionNote,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      requestId: payload.requestId,
      ...result,
    });
  })
);

router.post(
  "/manual-settlement-requests/:requestId/reject",
  requirePermission("payroll.settlement.override.approve", {
    resolveScope: async (req, tenantId) =>
      resolvePayrollSettlementOverrideRequestScope(req.params?.requestId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollManualSettlementRequestRejectInput(req);
    const result = await rejectPayrollManualSettlementRequest({
      req,
      tenantId: payload.tenantId,
      requestId: payload.requestId,
      userId: payload.userId,
      decisionNote: payload.decisionNote,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      requestId: payload.requestId,
      ...result,
    });
  })
);

export default router;
