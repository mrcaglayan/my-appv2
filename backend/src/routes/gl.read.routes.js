import { query } from "../db.js";
import {
  assertScopeAccess,
  buildScopeFilter,
  requirePermission,
} from "../middleware/rbac.js";
import {
  asyncHandler,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";

export function registerGlReadCoreRoutes(router) {
  router.get(
    "/books",
    requirePermission("gl.book.read", {
      resolveScope: (req) => {
        const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
        return legalEntityId
          ? { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId }
          : null;
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) throw badRequest("tenantId is required");

      const legalEntityId = parsePositiveInt(req.query.legalEntityId);
      if (legalEntityId) {
        assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
      }

      const conditions = ["tenant_id = ?"];
      const params = [tenantId];
      conditions.push(buildScopeFilter(req, "legal_entity", "legal_entity_id", params));
      if (legalEntityId) {
        conditions.push("legal_entity_id = ?");
        params.push(legalEntityId);
      }

      const result = await query(
        `SELECT id, tenant_id, legal_entity_id, calendar_id, code, name, book_type, base_currency_code, created_at
         FROM books
         WHERE ${conditions.join(" AND ")}
         ORDER BY id`,
        params
      );

      return res.json({ tenantId, rows: result.rows });
    })
  );

  router.get(
    "/coas",
    requirePermission("gl.coa.read", {
      resolveScope: (req) => {
        const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
        return legalEntityId
          ? { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId }
          : null;
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) throw badRequest("tenantId is required");

      const legalEntityId = parsePositiveInt(req.query.legalEntityId);
      if (legalEntityId) {
        assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
      }

      const scope = req.query.scope ? String(req.query.scope).toUpperCase() : null;
      const conditions = ["tenant_id = ?"];
      const params = [tenantId];
      const legalScopeFilter = buildScopeFilter(
        req,
        "legal_entity",
        "legal_entity_id",
        params
      );
      conditions.push(`(legal_entity_id IS NULL OR ${legalScopeFilter})`);
      if (legalEntityId) {
        conditions.push("legal_entity_id = ?");
        params.push(legalEntityId);
      }
      if (scope) {
        conditions.push("scope = ?");
        params.push(scope);
      }

      const result = await query(
        `SELECT id, tenant_id, legal_entity_id, scope, code, name, created_at
         FROM charts_of_accounts
         WHERE ${conditions.join(" AND ")}
         ORDER BY id`,
        params
      );

      return res.json({ tenantId, rows: result.rows });
    })
  );

  router.get(
    "/accounts",
    requirePermission("gl.account.read", {
      resolveScope: (req) => {
        const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
        return legalEntityId
          ? { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId }
          : null;
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) throw badRequest("tenantId is required");

      const coaId = parsePositiveInt(req.query.coaId);
      const legalEntityId = parsePositiveInt(req.query.legalEntityId);
      const includeInactive =
        String(req.query.includeInactive || "").toLowerCase() === "true";

      if (legalEntityId) {
        assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
      }

      const conditions = ["c.tenant_id = ?"];
      const params = [tenantId];
      const legalScopeFilter = buildScopeFilter(
        req,
        "legal_entity",
        "c.legal_entity_id",
        params
      );
      conditions.push(`(c.legal_entity_id IS NULL OR ${legalScopeFilter})`);
      if (coaId) {
        conditions.push("a.coa_id = ?");
        params.push(coaId);
      }
      if (legalEntityId) {
        conditions.push("c.legal_entity_id = ?");
        params.push(legalEntityId);
      }
      if (!includeInactive) {
        conditions.push("a.is_active = TRUE");
      }

      const result = await query(
        `SELECT
           a.id, c.tenant_id, a.coa_id, c.legal_entity_id, c.scope, c.code AS coa_code,
           a.code, a.name, a.account_type, a.normal_side, a.allow_posting,
           a.parent_account_id, a.is_active, a.created_at
         FROM accounts a
         JOIN charts_of_accounts c ON c.id = a.coa_id
         WHERE ${conditions.join(" AND ")}
         ORDER BY c.id, a.code`,
        params
      );

      return res.json({ tenantId, rows: result.rows });
    })
  );
}
