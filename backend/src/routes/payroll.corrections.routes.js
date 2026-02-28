import express from "express";
import { assertScopeAccess, requirePermission } from "../middleware/rbac.js";
import { asyncHandler } from "./_utils.js";
import { resolvePayrollRunScope } from "../services/payroll.runs.service.js";
import {
  createPayrollCorrectionShell,
  listPayrollRunCorrections,
  resolvePayrollCorrectionShellScope,
  reversePayrollRunWithCorrection,
} from "../services/payroll.corrections.service.js";
import {
  parsePayrollCorrectionShellCreateInput,
  parsePayrollRunCorrectionsReadInput,
  parsePayrollRunReverseInput,
} from "./payroll.corrections.validators.js";

const router = express.Router();

router.get(
  "/runs/:runId/corrections",
  requirePermission("payroll.corrections.read", {
    resolveScope: async (req, tenantId) => resolvePayrollRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollRunCorrectionsReadInput(req);
    const result = await listPayrollRunCorrections({
      req,
      tenantId: payload.tenantId,
      runId: payload.runId,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      runId: payload.runId,
      ...result,
    });
  })
);

router.post(
  "/runs/:runId/reverse",
  requirePermission("payroll.corrections.reverse", {
    resolveScope: async (req, tenantId) => resolvePayrollRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollRunReverseInput(req);
    const result = await reversePayrollRunWithCorrection({
      req,
      tenantId: payload.tenantId,
      runId: payload.runId,
      userId: payload.userId,
      reason: payload.reason,
      note: payload.note,
      idempotencyKey: payload.idempotencyKey,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      runId: payload.runId,
      ...result,
    });
  })
);

router.post(
  "/corrections/shell",
  requirePermission("payroll.corrections.create", {
    resolveScope: async (req, tenantId) => resolvePayrollCorrectionShellScope(req.body, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollCorrectionShellCreateInput(req);
    const result = await createPayrollCorrectionShell({
      req,
      tenantId: payload.tenantId,
      userId: payload.userId,
      input: payload,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: payload.tenantId,
      ...result,
    });
  })
);

export default router;
