import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import {
  applyPayrollProviderImport,
  createPayrollProviderConnection,
  enqueuePayrollProviderImportApplyJob,
  getPayrollProviderImportJobDetail,
  listPayrollEmployeeProviderRefs,
  listPayrollProviderConnections,
  listPayrollProviderImportJobs,
  listSupportedPayrollProviders,
  maskPayrollProviderImportRawPayload,
  previewPayrollProviderImport,
  purgePayrollProviderImportRawPayload,
  resolvePayrollProviderConnectionScope,
  resolvePayrollProviderImportJobScope,
  upsertPayrollEmployeeProviderRef,
  updatePayrollProviderConnection,
} from "../services/payroll.providers.service.js";
import {
  parsePayrollEmployeeProviderRefListInput,
  parsePayrollEmployeeProviderRefUpsertInput,
  parsePayrollProviderConnectionCreateInput,
  parsePayrollProviderConnectionListInput,
  parsePayrollProviderConnectionUpdateInput,
  parsePayrollProviderImportApplyInput,
  parsePayrollProviderImportJobListInput,
  parsePayrollProviderImportJobReadInput,
  parsePayrollProviderImportPreviewInput,
  parseSensitiveRetentionActionInput,
} from "./payroll.providers.validators.js";

const router = express.Router();

function resolveLegalEntityScopeFromQuery(req) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId ?? req.query?.legal_entity_id);
  if (!legalEntityId) return null;
  return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
}

function resolveLegalEntityScopeFromBody(req) {
  const legalEntityId = parsePositiveInt(req.body?.legalEntityId ?? req.body?.legal_entity_id);
  if (!legalEntityId) return null;
  return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
}

function parseAsyncFlag(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes";
}

router.get(
  "/providers",
  requirePermission("payroll.provider.read"),
  asyncHandler(async (_req, res) => {
    return res.json({ items: listSupportedPayrollProviders() });
  })
);

router.get(
  "/provider-connections",
  requirePermission("payroll.provider.read", { resolveScope: resolveLegalEntityScopeFromQuery }),
  asyncHandler(async (req, res) => {
    const filters = parsePayrollProviderConnectionListInput(req);
    const result = await listPayrollProviderConnections({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: filters.tenantId, ...result });
  })
);

router.post(
  "/provider-connections",
  requirePermission("payroll.provider.write", { resolveScope: resolveLegalEntityScopeFromBody }),
  asyncHandler(async (req, res) => {
    const input = parsePayrollProviderConnectionCreateInput(req);
    const row = await createPayrollProviderConnection({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      input,
      assertScopeAccess,
    });
    return res.status(201).json({ tenantId: input.tenantId, row });
  })
);

router.patch(
  "/provider-connections/:connectionId",
  requirePermission("payroll.provider.write", {
    resolveScope: async (req, tenantId) =>
      resolvePayrollProviderConnectionScope(req.params?.connectionId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parsePayrollProviderConnectionUpdateInput(req);
    const row = await updatePayrollProviderConnection({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      connectionId: input.connectionId,
      input,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, row });
  })
);

router.get(
  "/employee-provider-refs",
  requirePermission("payroll.provider.mapping.read", { resolveScope: resolveLegalEntityScopeFromQuery }),
  asyncHandler(async (req, res) => {
    const filters = parsePayrollEmployeeProviderRefListInput(req);
    const result = await listPayrollEmployeeProviderRefs({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: filters.tenantId, ...result });
  })
);

router.post(
  "/employee-provider-refs",
  requirePermission("payroll.provider.mapping.write", { resolveScope: resolveLegalEntityScopeFromBody }),
  asyncHandler(async (req, res) => {
    const input = parsePayrollEmployeeProviderRefUpsertInput(req);
    const row = await upsertPayrollEmployeeProviderRef({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      input,
      assertScopeAccess,
    });
    return res.status(201).json({ tenantId: input.tenantId, row });
  })
);

router.get(
  "/provider-imports",
  requirePermission("payroll.provider.import.read", { resolveScope: resolveLegalEntityScopeFromQuery }),
  asyncHandler(async (req, res) => {
    const filters = parsePayrollProviderImportJobListInput(req);
    const result = await listPayrollProviderImportJobs({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: filters.tenantId, ...result });
  })
);

router.post(
  "/provider-imports/preview",
  requirePermission("payroll.provider.import.preview", {
    resolveScope: async (req, tenantId) => {
      const connectionId = parsePositiveInt(
        req.body?.payrollProviderConnectionId ?? req.body?.payroll_provider_connection_id
      );
      if (!connectionId) return null;
      return resolvePayrollProviderConnectionScope(connectionId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const input = parsePayrollProviderImportPreviewInput(req);
    const result = await previewPayrollProviderImport({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      input,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.get(
  "/provider-imports/:importJobId",
  requirePermission("payroll.provider.import.read", {
    resolveScope: async (req, tenantId) =>
      resolvePayrollProviderImportJobScope(req.params?.importJobId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parsePayrollProviderImportJobReadInput(req);
    const result = await getPayrollProviderImportJobDetail({
      req,
      tenantId: input.tenantId,
      importJobId: input.importJobId,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.post(
  "/provider-imports/:importJobId/apply",
  requirePermission("payroll.provider.import.apply", {
    resolveScope: async (req, tenantId) =>
      resolvePayrollProviderImportJobScope(req.params?.importJobId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parsePayrollProviderImportApplyInput(req);
    const asyncMode = parseAsyncFlag(req.query?.async ?? req.body?.async);
    if (asyncMode) {
      const queued = await enqueuePayrollProviderImportApplyJob({
        req,
        tenantId: input.tenantId,
        userId: input.userId,
        importJobId: input.importJobId,
        input,
        assertScopeAccess,
      });
      return res.status(202).json({
        tenantId: input.tenantId,
        queued: true,
        idempotent: Boolean(queued?.idempotent),
        job: queued?.job || null,
      });
    }

    const result = await applyPayrollProviderImport({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      importJobId: input.importJobId,
      input,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.post(
  "/provider-imports/:importJobId/mask-raw-payload",
  requirePermission("payroll.provider.import.retention.manage", {
    resolveScope: async (req, tenantId) =>
      resolvePayrollProviderImportJobScope(req.params?.importJobId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseSensitiveRetentionActionInput(req);
    const result = await maskPayrollProviderImportRawPayload({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      importJobId: input.importJobId,
      reason: input.reason,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.post(
  "/provider-imports/:importJobId/purge-raw-payload",
  requirePermission("payroll.provider.import.retention.manage", {
    resolveScope: async (req, tenantId) =>
      resolvePayrollProviderImportJobScope(req.params?.importJobId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseSensitiveRetentionActionInput(req);
    const result = await purgePayrollProviderImportRawPayload({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      importJobId: input.importJobId,
      reason: input.reason,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

export default router;
