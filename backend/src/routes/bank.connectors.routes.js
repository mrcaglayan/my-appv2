import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { requireTenantId } from "./cash.validators.common.js";
import {
  createBankConnector,
  getBankConnectorByIdForTenant,
  listBankConnectorRows,
  listBankConnectorSyncRuns,
  resolveBankConnectorScope,
  runBankConnectorStatementSync,
  testBankConnectorConnection,
  updateBankConnectorById,
  upsertBankConnectorAccountLink,
} from "../services/bank.connectors.service.js";
import {
  parseBankConnectorAccountLinkInput,
  parseBankConnectorCreateInput,
  parseBankConnectorIdParam,
  parseBankConnectorListFilters,
  parseBankConnectorSyncRunListFilters,
  parseBankConnectorSyncTriggerInput,
  parseBankConnectorUpdateInput,
} from "./bank.connectors.validators.js";

const router = express.Router();

function resolveConnectorListScope(req) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
  if (legalEntityId) {
    return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  }
  return null;
}

router.get(
  "/connectors",
  requirePermission("bank.connectors.read", {
    resolveScope: resolveConnectorListScope,
  }),
  asyncHandler(async (req, res) => {
    const filters = parseBankConnectorListFilters(req);
    const result = await listBankConnectorRows({
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
  "/connectors/:connectorId",
  requirePermission("bank.connectors.read", {
    resolveScope: async (req, tenantId) =>
      resolveBankConnectorScope(req.params?.connectorId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const connectorId = parseBankConnectorIdParam(req);
    const result = await getBankConnectorByIdForTenant({
      req,
      tenantId,
      connectorId,
      assertScopeAccess,
    });
    return res.json({
      tenantId,
      ...result,
    });
  })
);

router.post(
  "/connectors",
  requirePermission("bank.connectors.write", {
    resolveScope: async (req) => {
      const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankConnectorCreateInput(req);
    const result = await createBankConnector({
      req,
      input,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: input.tenantId,
      ...result,
    });
  })
);

router.patch(
  "/connectors/:connectorId",
  requirePermission("bank.connectors.write", {
    resolveScope: async (req, tenantId) =>
      resolveBankConnectorScope(req.params?.connectorId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankConnectorUpdateInput(req);
    const result = await updateBankConnectorById({
      req,
      input,
      assertScopeAccess,
    });
    return res.json({
      tenantId: input.tenantId,
      ...result,
    });
  })
);

router.get(
  "/connectors/:connectorId/account-links",
  requirePermission("bank.connectors.read", {
    resolveScope: async (req, tenantId) =>
      resolveBankConnectorScope(req.params?.connectorId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const connectorId = parseBankConnectorIdParam(req);
    const result = await getBankConnectorByIdForTenant({
      req,
      tenantId,
      connectorId,
      assertScopeAccess,
    });
    return res.json({
      tenantId,
      connector: result.connector,
      rows: result.account_links || [],
    });
  })
);

router.put(
  "/connectors/:connectorId/account-links",
  requirePermission("bank.connectors.write", {
    resolveScope: async (req, tenantId) =>
      resolveBankConnectorScope(req.params?.connectorId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankConnectorAccountLinkInput(req);
    const result = await upsertBankConnectorAccountLink({
      req,
      input,
      assertScopeAccess,
    });
    return res.json({
      tenantId: input.tenantId,
      ...result,
    });
  })
);

router.get(
  "/connectors/:connectorId/sync-runs",
  requirePermission("bank.connectors.read", {
    resolveScope: async (req, tenantId) =>
      resolveBankConnectorScope(req.params?.connectorId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankConnectorSyncRunListFilters(req);
    const result = await listBankConnectorSyncRuns({
      req,
      tenantId: input.tenantId,
      connectorId: input.connectorId,
      limit: input.limit,
      offset: input.offset,
      assertScopeAccess,
    });
    return res.json({
      tenantId: input.tenantId,
      ...result,
    });
  })
);

router.post(
  "/connectors/:connectorId/test",
  requirePermission("bank.connectors.sync", {
    resolveScope: async (req, tenantId) =>
      resolveBankConnectorScope(req.params?.connectorId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const connectorId = parseBankConnectorIdParam(req);
    const result = await testBankConnectorConnection({
      req,
      tenantId,
      connectorId,
      assertScopeAccess,
    });
    return res.json({
      tenantId,
      ...result,
    });
  })
);

router.post(
  "/connectors/:connectorId/sync-statements",
  requirePermission("bank.connectors.sync", {
    resolveScope: async (req, tenantId) =>
      resolveBankConnectorScope(req.params?.connectorId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankConnectorSyncTriggerInput(req);
    const result = await runBankConnectorStatementSync({
      req,
      tenantId: input.tenantId,
      connectorId: input.connectorId,
      userId: input.userId,
      input,
      assertScopeAccess,
    });
    return res.json({
      tenantId: input.tenantId,
      ...result,
    });
  })
);

export default router;
