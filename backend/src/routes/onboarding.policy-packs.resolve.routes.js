import express from "express";
import { assertScopeAccess, requirePermission } from "../middleware/rbac.js";
import { assertLegalEntityBelongsToTenant } from "../tenantGuards.js";
import {
  asyncHandler,
  assertRequiredFields,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";
import { resolvePolicyPack } from "../services/policy-packs.resolve.service.js";

const router = express.Router();

router.post(
  "/policy-packs/:packId/resolve",
  requirePermission("org.tree.read", {
    resolveScope: (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
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

    assertRequiredFields(req.body, ["legalEntityId"]);
    const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
    if (!legalEntityId) {
      throw badRequest("legalEntityId must be a positive integer");
    }

    await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
    assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");

    const payload = await resolvePolicyPack({
      tenantId,
      legalEntityId,
      packId: req.params?.packId,
    });
    if (!payload) {
      const err = new Error("Policy pack not found");
      err.status = 404;
      throw err;
    }

    return res.json(payload);
  })
);

export default router;

