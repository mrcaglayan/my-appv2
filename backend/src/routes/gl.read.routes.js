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

function normalizeSearchText(value, maxLength = 120) {
  const text = String(value || "").trim();
  return text ? text.slice(0, maxLength) : "";
}

function buildAccountBreadcrumbResolver(hierarchyRows = []) {
  const accountMap = new Map();
  for (const row of hierarchyRows || []) {
    const id = parsePositiveInt(row?.id);
    if (!id) {
      continue;
    }
    accountMap.set(id, {
      id,
      parentAccountId: parsePositiveInt(row?.parent_account_id),
      code: String(row?.code || "").trim(),
      name: String(row?.name || "").trim(),
    });
  }

  const cache = new Map();
  return (accountIdRaw) => {
    const accountId = parsePositiveInt(accountIdRaw);
    if (!accountId) {
      return {
        label: null,
        codes: null,
        names: null,
      };
    }
    if (cache.has(accountId)) {
      return cache.get(accountId);
    }

    const visited = new Set();
    const chain = [];
    let cursor = accountId;
    while (cursor && !visited.has(cursor)) {
      visited.add(cursor);
      const account = accountMap.get(cursor);
      if (!account) {
        break;
      }
      chain.push(account);
      cursor = parsePositiveInt(account.parentAccountId);
    }
    chain.reverse();

    const codeSegments = chain.map((row) => String(row.code || "").trim()).filter(Boolean);
    const nameSegments = chain.map((row) => String(row.name || "").trim()).filter(Boolean);
    const labelSegments = chain
      .map((row) => {
        const code = String(row.code || "").trim();
        const name = String(row.name || "").trim();
        if (code && name) {
          return `${code} - ${name}`;
        }
        return code || name || null;
      })
      .filter(Boolean);

    const resolved = {
      label: labelSegments.length > 0 ? labelSegments.join(" > ") : null,
      codes: codeSegments.length > 0 ? codeSegments.join(" > ") : null,
      names: nameSegments.length > 0 ? nameSegments.join(" > ") : null,
    };
    cache.set(accountId, resolved);
    return resolved;
  };
}

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
      const q = normalizeSearchText(req.query.q);
      const limitRaw = Number(req.query.limit);
      const limit =
        Number.isInteger(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 1000) : null;
      const offsetRaw = Number(req.query.offset);
      const offset = Number.isInteger(offsetRaw) && offsetRaw >= 0 ? offsetRaw : 0;

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
      if (q) {
        conditions.push("(a.code LIKE ? OR a.name LIKE ?)");
        params.push(`%${q}%`, `%${q}%`);
      }

      const limitSql = limit ? ` LIMIT ${limit} OFFSET ${offset}` : "";

      const result = await query(
        `SELECT
           a.id, c.tenant_id, a.coa_id, c.legal_entity_id, c.scope, c.code AS coa_code,
           a.code, a.name, a.account_type, a.normal_side, a.allow_posting,
           a.parent_account_id, a.is_active, a.created_at
         FROM accounts a
         JOIN charts_of_accounts c ON c.id = a.coa_id
         WHERE ${conditions.join(" AND ")}
         ORDER BY c.id, a.code${limitSql}`,
        params
      );

      const rows = Array.isArray(result.rows) ? result.rows : [];
      if (rows.length > 0) {
        const hierarchyConditions = ["c.tenant_id = ?"];
        const hierarchyParams = [tenantId];
        const hierarchyScopeFilter = buildScopeFilter(
          req,
          "legal_entity",
          "c.legal_entity_id",
          hierarchyParams
        );
        hierarchyConditions.push(`(c.legal_entity_id IS NULL OR ${hierarchyScopeFilter})`);
        if (coaId) {
          hierarchyConditions.push("a.coa_id = ?");
          hierarchyParams.push(coaId);
        }
        if (legalEntityId) {
          hierarchyConditions.push("c.legal_entity_id = ?");
          hierarchyParams.push(legalEntityId);
        }

        const hierarchyResult = await query(
          `SELECT
             a.id,
             a.parent_account_id,
             a.code,
             a.name
           FROM accounts a
           JOIN charts_of_accounts c ON c.id = a.coa_id
           WHERE ${hierarchyConditions.join(" AND ")}`,
          hierarchyParams
        );

        const resolveBreadcrumb = buildAccountBreadcrumbResolver(hierarchyResult.rows || []);
        for (const row of rows) {
          const breadcrumb = resolveBreadcrumb(row?.id);
          row.account_breadcrumb = breadcrumb.label;
          row.account_breadcrumb_codes = breadcrumb.codes;
          row.account_breadcrumb_names = breadcrumb.names;
        }
      }

      return res.json({ tenantId, rows });
    })
  );
}
