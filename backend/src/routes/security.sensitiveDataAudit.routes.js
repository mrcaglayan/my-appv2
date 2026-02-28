import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { listSensitiveDataAuditRows } from "../services/security.sensitiveDataAudit.service.js";
import { parseSensitiveDataAuditListInput } from "./security.sensitiveDataAudit.validators.js";

const router = express.Router();

function resolveLegalEntityScopeFromQuery(req) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId ?? req.query?.legal_entity_id);
  if (!legalEntityId) return null;
  return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
}

router.get(
  "/sensitive-data-audit",
  requirePermission("security.sensitive_data.audit.read", {
    resolveScope: resolveLegalEntityScopeFromQuery,
  }),
  asyncHandler(async (req, res) => {
    const filters = parseSensitiveDataAuditListInput(req);
    const result = await listSensitiveDataAuditRows({
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

export default router;

