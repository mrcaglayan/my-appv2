import express from "express";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import {
  parseRevenueAccrualActionInput,
  parseRevenueAccrualGenerateInput,
  parseRevenueReportFilters,
  parseRevenueRunActionInput,
  parseRevenueRunCreateInput,
  parseRevenueRunListFilters,
  parseRevenueScheduleGenerateInput,
  parseRevenueScheduleListFilters,
} from "./revenue-recognition.validators.js";
import {
  generateRevenueAccrual,
  createRevenueRecognitionRun,
  generateRevenueRecognitionSchedule,
  getRevenueAccrualSplitReport,
  getRevenueDeferredRevenueSplitReport,
  getRevenueFutureYearRollforwardReport,
  getRevenuePrepaidExpenseSplitReport,
  listRevenueRecognitionRuns,
  listRevenueRecognitionSchedules,
  postRevenueRecognitionRun,
  reverseRevenueAccrual,
  resolveRevenueRunScope,
  settleRevenueAccrual,
  reverseRevenueRecognitionRun,
} from "../services/revenue-recognition.service.js";

const router = express.Router();

function resolveLegalEntityScopeFromQuery(req) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
  if (!legalEntityId) {
    return null;
  }
  return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
}

function resolveLegalEntityScopeFromBody(req) {
  const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
  if (!legalEntityId) {
    return null;
  }
  return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
}

router.get(
  "/schedules",
  requirePermission("revenue.schedule.read", {
    resolveScope: async (req) => resolveLegalEntityScopeFromQuery(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseRevenueScheduleListFilters(req);
    const result = await listRevenueRecognitionSchedules({
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
  "/schedules/generate",
  requirePermission("revenue.schedule.generate", {
    resolveScope: async (req) => resolveLegalEntityScopeFromBody(req),
  }),
  asyncHandler(async (req, res) => {
    const payload = parseRevenueScheduleGenerateInput(req);
    const row = await generateRevenueRecognitionSchedule({
      req,
      payload,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.get(
  "/runs",
  requirePermission("revenue.run.read", {
    resolveScope: async (req) => resolveLegalEntityScopeFromQuery(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseRevenueRunListFilters(req);
    const result = await listRevenueRecognitionRuns({
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
  "/runs",
  requirePermission("revenue.run.create", {
    resolveScope: async (req) => resolveLegalEntityScopeFromBody(req),
  }),
  asyncHandler(async (req, res) => {
    const payload = parseRevenueRunCreateInput(req);
    const row = await createRevenueRecognitionRun({
      req,
      payload,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.post(
  "/runs/:runId/post",
  requirePermission("revenue.run.post", {
    resolveScope: async (req, tenantId) => resolveRevenueRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parseRevenueRunActionInput(req);
    const result = await postRevenueRecognitionRun({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      ...result,
    });
  })
);

router.post(
  "/runs/:runId/reverse",
  requirePermission("revenue.run.reverse", {
    resolveScope: async (req, tenantId) => resolveRevenueRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parseRevenueRunActionInput(req);
    const result = await reverseRevenueRecognitionRun({
      req,
      payload,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: payload.tenantId,
      ...result,
    });
  })
);

router.post(
  "/accruals/generate",
  requirePermission("revenue.run.create", {
    resolveScope: async (req) => resolveLegalEntityScopeFromBody(req),
  }),
  asyncHandler(async (req, res) => {
    const payload = parseRevenueAccrualGenerateInput(req);
    const row = await generateRevenueAccrual({
      req,
      payload,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.post(
  "/accruals/:accrualId/settle",
  requirePermission("revenue.run.post", {
    resolveScope: async (req, tenantId) =>
      resolveRevenueRunScope(req.params?.accrualId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parseRevenueAccrualActionInput(req);
    const result = await settleRevenueAccrual({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      ...result,
    });
  })
);

router.post(
  "/accruals/:accrualId/reverse",
  requirePermission("revenue.run.reverse", {
    resolveScope: async (req, tenantId) =>
      resolveRevenueRunScope(req.params?.accrualId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const payload = parseRevenueAccrualActionInput(req);
    const result = await reverseRevenueAccrual({
      req,
      payload,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: payload.tenantId,
      ...result,
    });
  })
);

router.get(
  "/reports/future-year-rollforward",
  requirePermission("revenue.report.read", {
    resolveScope: async (req) => resolveLegalEntityScopeFromQuery(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseRevenueReportFilters(req);
    const result = await getRevenueFutureYearRollforwardReport({
      req,
      filters,
      assertScopeAccess,
      buildScopeFilter,
    });
    return res.json({
      tenantId: filters.tenantId,
      ...result,
    });
  })
);

router.get(
  "/reports/deferred-revenue-split",
  requirePermission("revenue.report.read", {
    resolveScope: async (req) => resolveLegalEntityScopeFromQuery(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseRevenueReportFilters(req);
    const result = await getRevenueDeferredRevenueSplitReport({
      req,
      filters,
      assertScopeAccess,
      buildScopeFilter,
    });
    return res.json({
      tenantId: filters.tenantId,
      ...result,
    });
  })
);

router.get(
  "/reports/accrual-split",
  requirePermission("revenue.report.read", {
    resolveScope: async (req) => resolveLegalEntityScopeFromQuery(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseRevenueReportFilters(req);
    const result = await getRevenueAccrualSplitReport({
      req,
      filters,
      assertScopeAccess,
      buildScopeFilter,
    });
    return res.json({
      tenantId: filters.tenantId,
      ...result,
    });
  })
);

router.get(
  "/reports/prepaid-expense-split",
  requirePermission("revenue.report.read", {
    resolveScope: async (req) => resolveLegalEntityScopeFromQuery(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseRevenueReportFilters(req);
    const result = await getRevenuePrepaidExpenseSplitReport({
      req,
      filters,
      assertScopeAccess,
      buildScopeFilter,
    });
    return res.json({
      tenantId: filters.tenantId,
      ...result,
    });
  })
);

export default router;
