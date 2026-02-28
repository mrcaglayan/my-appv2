import express from "express";
import { assertScopeAccess, requirePermission } from "../middleware/rbac.js";
import { assertLegalEntityBelongsToTenant } from "../tenantGuards.js";
import {
  asyncHandler,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";
import { getModuleReadiness } from "../services/module-readiness.service.js";

const router = express.Router();

router.get(
  "/module-readiness",
  requirePermission("org.tree.read", {
    resolveScope: (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return { scopeType: "TENANT", scopeId: tenantId };
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const hasLegalEntityIdFilter =
      req.query?.legalEntityId !== undefined && req.query?.legalEntityId !== "";
    const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
    if (hasLegalEntityIdFilter && !legalEntityId) {
      throw badRequest("legalEntityId must be a positive integer");
    }

    if (legalEntityId) {
      await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
      assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
    }

    const payload = await getModuleReadiness(tenantId, legalEntityId);
    return res.json(payload);
  })
);

export default router;
