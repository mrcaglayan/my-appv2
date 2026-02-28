import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { resolvePayrollRunScope } from "../services/payroll.runs.service.js";
import {
  buildPayrollRunLiabilities,
  createPayrollRunPaymentBatchFromLiabilities,
  getPayrollRunLiabilitiesDetail,
  getPayrollRunLiabilityPaymentBatchPreview,
  listPayrollLiabilityRows,
} from "../services/payroll.liabilities.service.js";
import {
  parsePayrollLiabilityListFilters,
  parsePayrollRunCreatePaymentBatchInput,
  parsePayrollRunLiabilityBuildInput,
  parsePayrollRunLiabilityReadInput,
  parsePayrollRunPaymentBatchPreviewInput,
} from "./payroll.liabilities.validators.js";

const router = express.Router();

async function resolvePayrollLiabilityListScope(req, tenantId) {
  const runId = parsePositiveInt(req.query?.runId ?? req.query?.run_id);
  if (runId) {
    return resolvePayrollRunScope(runId, tenantId);
  }
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId ?? req.query?.legal_entity_id);
  if (legalEntityId) {
    return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  }
  return null;
}

router.get(
  "/liabilities",
  requirePermission("payroll.liabilities.read", {
    resolveScope: resolvePayrollLiabilityListScope,
  }),
  asyncHandler(async (req, res) => {
    const filters = parsePayrollLiabilityListFilters(req);
    const result = await listPayrollLiabilityRows({
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

router.post(
  "/runs/:runId/liabilities/build",
  requirePermission("payroll.liabilities.build", {
    resolveScope: async (req, tenantId) => resolvePayrollRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollRunLiabilityBuildInput(req);
    const result = await buildPayrollRunLiabilities({
      req,
      tenantId: payload.tenantId,
      runId: payload.runId,
      userId: payload.userId,
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

router.get(
  "/runs/:runId/liabilities",
  requirePermission("payroll.liabilities.read", {
    resolveScope: async (req, tenantId) => resolvePayrollRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollRunLiabilityReadInput(req);
    const result = await getPayrollRunLiabilitiesDetail({
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

router.get(
  "/runs/:runId/payment-batch-preview",
  requirePermission("payroll.liabilities.read", {
    resolveScope: async (req, tenantId) => resolvePayrollRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollRunPaymentBatchPreviewInput(req);
    const preview = await getPayrollRunLiabilityPaymentBatchPreview({
      req,
      tenantId: payload.tenantId,
      runId: payload.runId,
      scope: payload.scope,
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
  "/runs/:runId/payment-batches",
  requirePermission("payroll.payment.prepare", {
    resolveScope: async (req, tenantId) => resolvePayrollRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePayrollRunCreatePaymentBatchInput(req);
    const result = await createPayrollRunPaymentBatchFromLiabilities({
      req,
      tenantId: payload.tenantId,
      runId: payload.runId,
      userId: payload.userId,
      input: payload,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: payload.tenantId,
      runId: payload.runId,
      ...result,
    });
  })
);

export default router;

