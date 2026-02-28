import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, badRequest, parsePositiveInt, resolveTenantId } from "./_utils.js";
import { resolveBankAccountScope } from "../services/bank.accounts.service.js";
import {
  getOpsBankPaymentBatchesHealth,
  getOpsBankReconciliationSummary,
  getOpsJobsHealth,
  getOpsPayrollCloseStatus,
  getOpsPayrollImportHealth,
} from "../services/ops.dashboard.service.js";

const router = express.Router();

function requireTenantIdFromReq(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) throw badRequest("tenantId is required");
  return tenantId;
}

function parseDateString(value, label) {
  if (value === undefined || value === null || value === "") return null;
  const raw = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw badRequest(`${label} must be YYYY-MM-DD`);
  }
  return raw;
}

function parsePositiveIntMaybe(value, label) {
  if (value === undefined || value === null || value === "") return null;
  const parsed = parsePositiveInt(value);
  if (!parsed) throw badRequest(`${label} must be a positive integer`);
  return parsed;
}

function parseOpsCommonFilters(req) {
  const dateFrom = parseDateString(req.query?.dateFrom ?? req.query?.date_from, "dateFrom");
  const dateTo = parseDateString(req.query?.dateTo ?? req.query?.date_to, "dateTo");
  const daysRaw = req.query?.days;
  const days =
    daysRaw === undefined || daysRaw === null || daysRaw === ""
      ? null
      : parsePositiveIntMaybe(daysRaw, "days");

  return {
    tenantId: requireTenantIdFromReq(req),
    legalEntityId: parsePositiveIntMaybe(req.query?.legalEntityId ?? req.query?.legal_entity_id, "legalEntityId"),
    bankAccountId: parsePositiveIntMaybe(req.query?.bankAccountId ?? req.query?.bank_account_id, "bankAccountId"),
    dateFrom,
    dateTo,
    days,
  };
}

function parseOpsJobsFilters(req) {
  const base = parseOpsCommonFilters(req);
  return {
    ...base,
    moduleCode: req.query?.moduleCode ?? req.query?.module_code ?? null,
    jobType: req.query?.jobType ?? req.query?.job_type ?? null,
    queueName: req.query?.queueName ?? req.query?.queue_name ?? null,
  };
}

function parseOpsPayrollImportFilters(req) {
  const base = parseOpsCommonFilters(req);
  return {
    ...base,
    providerCode: req.query?.providerCode ?? req.query?.provider_code ?? null,
  };
}

async function resolveOpsScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(
    req.query?.legalEntityId ?? req.query?.legal_entity_id ?? req.body?.legalEntityId ?? req.body?.legal_entity_id
  );
  if (legalEntityId) return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };

  const bankAccountId = parsePositiveInt(
    req.query?.bankAccountId ?? req.query?.bank_account_id ?? req.body?.bankAccountId ?? req.body?.bank_account_id
  );
  if (bankAccountId) return resolveBankAccountScope(bankAccountId, tenantId);
  return null;
}

router.get(
  "/bank/reconciliation-summary",
  requirePermission("ops.dashboard.read", { resolveScope: resolveOpsScope }),
  asyncHandler(async (req, res) => {
    const filters = parseOpsCommonFilters(req);
    const result = await getOpsBankReconciliationSummary({
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
  "/bank/payment-batches-health",
  requirePermission("ops.dashboard.read", { resolveScope: resolveOpsScope }),
  asyncHandler(async (req, res) => {
    const filters = parseOpsCommonFilters(req);
    const result = await getOpsBankPaymentBatchesHealth({
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
  "/payroll/import-health",
  requirePermission("ops.dashboard.read", { resolveScope: resolveOpsScope }),
  asyncHandler(async (req, res) => {
    const filters = parseOpsPayrollImportFilters(req);
    const result = await getOpsPayrollImportHealth({
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
  "/payroll/close-status",
  requirePermission("ops.dashboard.read", { resolveScope: resolveOpsScope }),
  asyncHandler(async (req, res) => {
    const filters = parseOpsCommonFilters(req);
    const result = await getOpsPayrollCloseStatus({
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
  "/jobs/health",
  requirePermission("ops.dashboard.read"),
  asyncHandler(async (req, res) => {
    const filters = parseOpsJobsFilters(req);
    const result = await getOpsJobsHealth({
      tenantId: filters.tenantId,
      filters,
    });
    return res.json({ tenantId: filters.tenantId, ...result });
  })
);

export default router;
