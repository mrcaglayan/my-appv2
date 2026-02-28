import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler } from "./_utils.js";
import {
  parseCashExceptionReadFilters,
  parseCashExceptionRouteScopeInput,
} from "./cash.exception.validators.js";
import { resolveCashRegisterScope } from "../services/cash.register.service.js";
import { listCashExceptionSnapshot } from "../services/cash.exception.service.js";

const router = express.Router();

router.get(
  "/",
  requirePermission("cash.report.read", {
    resolveScope: async (req, tenantId) => {
      const scopeInput = parseCashExceptionRouteScopeInput(req);
      if (scopeInput.legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: scopeInput.legalEntityId };
      }
      if (scopeInput.registerId) {
        return resolveCashRegisterScope(scopeInput.registerId, tenantId);
      }
      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const filters = parseCashExceptionReadFilters(req);
    const snapshot = await listCashExceptionSnapshot({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });

    return res.json({
      tenantId: filters.tenantId,
      filters: {
        registerId: filters.registerId,
        legalEntityId: filters.legalEntityId,
        operatingUnitId: filters.operatingUnitId,
        fromDate: filters.fromDate,
        toDate: filters.toDate,
        minAbsVariance: filters.minAbsVariance,
        type: filters.types,
        limit: filters.limit,
        offset: filters.offset,
        includeRows: filters.includeRows,
      },
      ...snapshot,
    });
  })
);

export default router;
