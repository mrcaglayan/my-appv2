import express from "express";
import { assertScopeAccess, requirePermission } from "../middleware/rbac.js";
import { asyncHandler } from "./_utils.js";
import { resolvePayrollRunScope, getPayrollRunByIdForTenant } from "../services/payroll.runs.service.js";
import {
  finalizePayrollRunAccrual,
  getPayrollRunAccrualPreview,
  markPayrollRunReviewed,
} from "../services/payroll.accruals.service.js";
import {
  parsePayrollAccrualPreviewInput,
  parsePayrollRunFinalizeInput,
  parsePayrollRunReviewInput,
} from "./payroll.accruals.validators.js";

const router = express.Router();

router.get(
  "/:runId/accrual-preview",
  requirePermission("payroll.runs.read", {
    resolveScope: async (req, tenantId) => resolvePayrollRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollAccrualPreviewInput(req);
    const preview = await getPayrollRunAccrualPreview({
      req,
      tenantId: payload.tenantId,
      runId: payload.runId,
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
  "/:runId/review",
  requirePermission("payroll.runs.review", {
    resolveScope: async (req, tenantId) => resolvePayrollRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollRunReviewInput(req);
    const result = await markPayrollRunReviewed({
      req,
      tenantId: payload.tenantId,
      runId: payload.runId,
      userId: payload.userId,
      note: payload.note,
      assertScopeAccess,
    });
    const row = await getPayrollRunByIdForTenant({
      req,
      tenantId: payload.tenantId,
      runId: payload.runId,
      assertScopeAccess,
    });

    return res.json({
      tenantId: payload.tenantId,
      runId: payload.runId,
      idempotentReplay: Boolean(result?.idempotentReplay),
      row,
    });
  })
);

router.post(
  "/:runId/finalize",
  requirePermission("payroll.runs.finalize", {
    resolveScope: async (req, tenantId) => resolvePayrollRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollRunFinalizeInput(req);
    const result = await finalizePayrollRunAccrual({
      req,
      tenantId: payload.tenantId,
      runId: payload.runId,
      userId: payload.userId,
      note: payload.note,
      forceFromImported: payload.forceFromImported,
      assertScopeAccess,
    });
    const row = await getPayrollRunByIdForTenant({
      req,
      tenantId: payload.tenantId,
      runId: payload.runId,
      assertScopeAccess,
    });

    return res.json({
      tenantId: payload.tenantId,
      runId: payload.runId,
      idempotentReplay: Boolean(result?.idempotentReplay),
      accrualJournalEntryId: result?.accrualJournalEntryId || null,
      preview: result?.preview || null,
      row,
    });
  })
);

export default router;

