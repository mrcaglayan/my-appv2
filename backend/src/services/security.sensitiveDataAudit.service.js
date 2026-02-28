import { query } from "../db.js";
import { parsePositiveInt } from "../routes/_utils.js";

function parseOptionalJson(value) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function buildScopeCondition({ req, filters, params, buildScopeFilter, assertScopeAccess }) {
  const legalEntityId = parsePositiveInt(filters?.legalEntityId);
  if (legalEntityId) {
    if (assertScopeAccess) {
      assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
    }
    params.push(legalEntityId);
    return "a.legal_entity_id = ?";
  }
  if (typeof buildScopeFilter === "function") {
    // Rows without legal_entity_id are tenant-level and excluded for scoped users.
    return `(${buildScopeFilter(req, "legal_entity", "a.legal_entity_id", params)})`;
  }
  return "1 = 1";
}

export async function listSensitiveDataAuditRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["a.tenant_id = ?"];
  conditions.push(
    buildScopeCondition({
      req,
      filters,
      params,
      buildScopeFilter,
      assertScopeAccess,
    })
  );

  if (filters.moduleCode) {
    conditions.push("a.module_code = ?");
    params.push(filters.moduleCode);
  }
  if (filters.objectType) {
    conditions.push("a.object_type = ?");
    params.push(filters.objectType);
  }
  if (filters.action) {
    conditions.push("a.action = ?");
    params.push(filters.action);
  }
  if (filters.dateFrom) {
    conditions.push("DATE(a.acted_at) >= ?");
    params.push(filters.dateFrom);
  }
  if (filters.dateTo) {
    conditions.push("DATE(a.acted_at) <= ?");
    params.push(filters.dateTo);
  }
  if (filters.q) {
    const like = `%${filters.q}%`;
    conditions.push(
      "(a.note LIKE ? OR a.module_code LIKE ? OR a.object_type LIKE ? OR CAST(a.object_id AS CHAR) LIKE ? OR a.action LIKE ?)"
    );
    params.push(like, like, like, like, like);
  }

  const whereSql = conditions.join(" AND ");
  const countRes = await query(
    `SELECT COUNT(*) AS total
     FROM sensitive_data_audit a
     WHERE ${whereSql}`,
    params
  );
  const total = Number(countRes.rows?.[0]?.total || 0);
  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset = Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;

  const listRes = await query(
    `SELECT
        a.*,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name,
        u.email AS acted_by_user_email,
        u.name AS acted_by_user_name
     FROM sensitive_data_audit a
     LEFT JOIN legal_entities le
       ON le.tenant_id = a.tenant_id
      AND le.id = a.legal_entity_id
     LEFT JOIN users u
       ON u.tenant_id = a.tenant_id
      AND u.id = a.acted_by_user_id
     WHERE ${whereSql}
     ORDER BY a.acted_at DESC, a.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: (listRes.rows || []).map((row) => ({
      ...row,
      payload_json: parseOptionalJson(row.payload_json),
    })),
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export default {
  listSensitiveDataAuditRows,
};

