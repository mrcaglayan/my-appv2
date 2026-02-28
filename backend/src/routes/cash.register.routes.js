import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import {
  parseCashRegisterIdParam,
  parseCashRegisterReadFilters,
  parseCashRegisterStatusUpdateInput,
  parseCashRegisterUpsertInput,
} from "./cash.register.validators.js";
import { requireTenantId } from "./cash.validators.common.js";
import {
  getCashRegisterByIdForTenant,
  listCashRegisterRows,
  resolveCashRegisterScope,
  setCashRegisterStatus,
  upsertCashRegister,
} from "../services/cash.register.service.js";

const router = express.Router();

router.get(
  "/",
  requirePermission("cash.register.read", {
    resolveScope: async (req) => {
      const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const filters = parseCashRegisterReadFilters(req);
    const result = await listCashRegisterRows({
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
  "/:registerId",
  requirePermission("cash.register.read", {
    resolveScope: async (req, tenantId) => {
      return resolveCashRegisterScope(req.params?.registerId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const registerId = parseCashRegisterIdParam(req);
    const row = await getCashRegisterByIdForTenant({
      req,
      tenantId,
      registerId,
      assertScopeAccess,
    });
    return res.json({
      tenantId,
      row,
    });
  })
);

router.post(
  "/",
  requirePermission("cash.register.upsert", {
    resolveScope: async (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }

      const registerId = parsePositiveInt(req.body?.id);
      if (registerId) {
        return resolveCashRegisterScope(registerId, tenantId);
      }
      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseCashRegisterUpsertInput(req);
    const row = await upsertCashRegister({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.post(
  "/:registerId/status",
  requirePermission("cash.register.upsert", {
    resolveScope: async (req, tenantId) => {
      return resolveCashRegisterScope(req.params?.registerId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseCashRegisterStatusUpdateInput(req);
    const row = await setCashRegisterStatus({
      req,
      tenantId: payload.tenantId,
      registerId: payload.registerId,
      targetStatus: payload.status,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

export default router;
