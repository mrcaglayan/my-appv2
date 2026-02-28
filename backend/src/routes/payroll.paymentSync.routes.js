import express from "express";
import { assertScopeAccess, requirePermission } from "../middleware/rbac.js";
import { asyncHandler } from "./_utils.js";
import { resolvePayrollRunScope } from "../services/payroll.runs.service.js";
import {
  applyPayrollRunPaymentSync,
  getPayrollRunPaymentSyncPreview,
} from "../services/payroll.paymentSync.service.js";
import {
  parsePayrollPaymentSyncApplyInput,
  parsePayrollPaymentSyncPreviewInput,
} from "./payroll.paymentSync.validators.js";

const router = express.Router();

router.get(
  "/:runId/payment-sync-preview",
  requirePermission("payroll.payment.sync.read", {
    resolveScope: async (req, tenantId) => resolvePayrollRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollPaymentSyncPreviewInput(req);
    const preview = await getPayrollRunPaymentSyncPreview({
      req,
      tenantId: payload.tenantId,
      runId: payload.runId,
      scope: payload.scope,
      allowB04OnlySettlement: payload.allowB04OnlySettlement,
      assertScopeAccess,
    });

    return res.json({
      tenantId: payload.tenantId,
      runId: payload.runId,
      preview,
    });
  })
);

router.post(
  "/:runId/payment-sync-apply",
  requirePermission("payroll.payment.sync.apply", {
    resolveScope: async (req, tenantId) => resolvePayrollRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollPaymentSyncApplyInput(req);
    const result = await applyPayrollRunPaymentSync({
      req,
      tenantId: payload.tenantId,
      runId: payload.runId,
      userId: payload.userId,
      scope: payload.scope,
      allowB04OnlySettlement: payload.allowB04OnlySettlement,
      note: payload.note,
      assertScopeAccess,
    });

    return res.json({
      tenantId: payload.tenantId,
      runId: payload.runId,
      ...result,
    });
  })
);

export default router;

