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
import { applyPolicyPack } from "../services/policy-packs.apply.service.js";

const router = express.Router();

router.post(
  "/policy-packs/:packId/apply",
  requirePermission("gl.account.upsert", {
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

    const userId = parsePositiveInt(req.user?.userId);
    if (!userId) {
      throw badRequest("Authenticated user is required");
    }

    assertRequiredFields(req.body, ["legalEntityId", "rows"]);
    const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
    if (!legalEntityId) {
      throw badRequest("legalEntityId must be a positive integer");
    }

    await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
    assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");

    const payload = await applyPolicyPack({
      tenantId,
      userId,
      legalEntityId,
      packId: req.params?.packId,
      mode: req.body?.mode,
      rows: req.body?.rows,
    });
    if (!payload) {
      const err = new Error("Policy pack not found");
      err.status = 404;
      throw err;
    }

    return res.status(201).json({
      ok: true,
      ...payload,
    });
  })
);

export default router;

