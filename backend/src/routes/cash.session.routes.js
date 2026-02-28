import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import {
  parseCashSessionCloseInput,
  parseCashSessionIdParam,
  parseCashSessionOpenInput,
  parseCashSessionReadFilters,
} from "./cash.session.validators.js";
import { requireTenantId } from "./cash.validators.common.js";
import { resolveCashRegisterScope } from "../services/cash.register.service.js";
import {
  closeCashSessionById,
  getCashSessionByIdForTenant,
  listCashSessionRows,
  openCashSession,
  resolveCashSessionScope,
} from "../services/cash.session.service.js";

const router = express.Router();

const requireCashVarianceApprovePermission = requirePermission("cash.variance.approve", {
  resolveScope: async (req, tenantId) => {
    return resolveCashSessionScope(req.params?.sessionId, tenantId);
  },
});

async function runPermissionMiddleware(middleware, req, res) {
  await new Promise((resolve, reject) => {
    middleware(req, res, (err) => {
      if (err) {
        reject(err);
        return;
      }
      resolve();
    });
  });
}

router.get(
  "/",
  requirePermission("cash.register.read", {
    resolveScope: async (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }

      const registerId = parsePositiveInt(req.query?.registerId);
      if (registerId) {
        return resolveCashRegisterScope(registerId, tenantId);
      }
      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const filters = parseCashSessionReadFilters(req);
    const result = await listCashSessionRows({
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
  "/:sessionId",
  requirePermission("cash.register.read", {
    resolveScope: async (req, tenantId) => {
      return resolveCashSessionScope(req.params?.sessionId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const sessionId = parseCashSessionIdParam(req);
    const row = await getCashSessionByIdForTenant({
      req,
      tenantId,
      sessionId,
      assertScopeAccess,
    });
    return res.json({
      tenantId,
      row,
    });
  })
);

router.post(
  "/open",
  requirePermission("cash.session.open", {
    resolveScope: async (req, tenantId) => {
      return resolveCashRegisterScope(req.body?.registerId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseCashSessionOpenInput(req);
    const row = await openCashSession({
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
  "/:sessionId/close",
  requirePermission("cash.session.close", {
    resolveScope: async (req, tenantId) => {
      return resolveCashSessionScope(req.params?.sessionId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseCashSessionCloseInput(req);
    if (payload.approveVariance) {
      await runPermissionMiddleware(requireCashVarianceApprovePermission, req, res);
    }
    const row = await closeCashSessionById({
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

export default router;
