import express from "express";
import {
  assertScopeAccess,
  buildScopeFilter,
  requirePermission,
} from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import {
  parsePaymentTermIdParam,
  parsePaymentTermReadFilters,
} from "./cari.payment-term.validators.js";
import { requireTenantId } from "./cash.validators.common.js";
import {
  getPaymentTermByIdForTenant,
  listPaymentTerms,
  resolvePaymentTermScope,
} from "../services/cari.payment-term.service.js";

const router = express.Router();

router.get(
  "/",
  requirePermission("cari.card.read", {
    resolveScope: async (req) => {
      const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const filters = parsePaymentTermReadFilters(req);
    const result = await listPaymentTerms({
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
  "/:paymentTermId",
  requirePermission("cari.card.read", {
    resolveScope: async (req, tenantId) => {
      return resolvePaymentTermScope(req.params?.paymentTermId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const paymentTermId = parsePaymentTermIdParam(req);
    const row = await getPaymentTermByIdForTenant({
      req,
      tenantId,
      paymentTermId,
      assertScopeAccess,
    });
    return res.json({
      tenantId,
      row,
    });
  })
);

export default router;
