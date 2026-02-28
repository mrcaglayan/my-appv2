import { query } from "../db.js";
import { parsePositiveInt } from "../routes/_utils.js";

function parseJsonPayload(value) {
  if (!value) {
    return null;
  }
  if (typeof value === "object") {
    return value;
  }
  if (typeof value === "string") {
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }
  return null;
}

function mapAuditRow(row, includePayload) {
  const payload = includePayload ? parseJsonPayload(row.payload_json) : undefined;
  return {
    auditLogId: parsePositiveInt(row.id),
    tenantId: parsePositiveInt(row.tenant_id),
    action: row.action || null,
    resourceType: row.resource_type || null,
    resourceId: row.resource_id || null,
    resourceIdInt: parsePositiveInt(row.resource_id),
    scopeType: row.scope_type || null,
    scopeId: parsePositiveInt(row.scope_id),
    legalEntityCode: row.legal_entity_code || null,
    legalEntityName: row.legal_entity_name || null,
    actorUserId: parsePositiveInt(row.user_id),
    actorEmail: row.actor_email || null,
    actorName: row.actor_name || null,
    requestId: row.request_id || null,
    ipAddress: row.ip_address || null,
    userAgent: row.user_agent || null,
    createdAt: row.created_at || null,
    ...(includePayload ? { payload } : {}),
  };
}

function toActionSummaryRows(rows) {
  return (rows || []).map((row) => ({
    action: row.action || null,
    count: Number(row.row_count || 0),
  }));
}

function buildAuditWhereSql({
  req,
  filters,
  params,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const conditions = [
    "al.tenant_id = ?",
    "al.action LIKE 'cari.%'",
    "al.scope_type = 'LEGAL_ENTITY'",
  ];
  params.push(filters.tenantId);

  if (typeof buildScopeFilter === "function") {
    conditions.push(buildScopeFilter(req, "legal_entity", "al.scope_id", params));
  }

  if (filters.legalEntityId) {
    if (typeof assertScopeAccess === "function") {
      assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    }
    conditions.push("al.scope_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.action) {
    if (filters.action.endsWith("*")) {
      conditions.push("al.action LIKE ?");
      params.push(`${filters.action.slice(0, -1)}%`);
    } else {
      conditions.push("al.action = ?");
      params.push(filters.action);
    }
  }

  if (filters.resourceType) {
    conditions.push("al.resource_type = ?");
    params.push(filters.resourceType);
  }

  if (filters.resourceId) {
    conditions.push("al.resource_id = ?");
    params.push(filters.resourceId);
  }

  if (filters.actorUserId) {
    conditions.push("al.user_id = ?");
    params.push(filters.actorUserId);
  }

  if (filters.requestId) {
    conditions.push("al.request_id = ?");
    params.push(filters.requestId);
  }

  if (filters.createdFrom) {
    conditions.push("al.created_at >= ?");
    params.push(filters.createdFrom);
  }

  if (filters.createdTo) {
    conditions.push("al.created_at <= ?");
    params.push(filters.createdTo);
  }

  return conditions.join(" AND ");
}

export async function getCariAuditTrail({
  req,
  filters,
  buildScopeFilter,
  assertScopeAccess,
  runQuery = query,
}) {
  const params = [];
  const whereSql = buildAuditWhereSql({
    req,
    filters,
    params,
    buildScopeFilter,
    assertScopeAccess,
  });

  const [countResult, rowsResult, byActionResult] = await Promise.all([
    runQuery(
      `SELECT COUNT(*) AS total
       FROM audit_logs al
       WHERE ${whereSql}`,
      params
    ),
    runQuery(
      `SELECT
         al.id,
         al.tenant_id,
         al.user_id,
         al.action,
         al.resource_type,
         al.resource_id,
         al.scope_type,
         al.scope_id,
         al.request_id,
         al.ip_address,
         al.user_agent,
         al.payload_json,
         al.created_at,
         u.email AS actor_email,
         u.name AS actor_name,
         le.code AS legal_entity_code,
         le.name AS legal_entity_name
       FROM audit_logs al
       LEFT JOIN users u
         ON u.id = al.user_id
        AND u.tenant_id = al.tenant_id
       LEFT JOIN legal_entities le
         ON le.id = al.scope_id
        AND le.tenant_id = al.tenant_id
       WHERE ${whereSql}
       ORDER BY al.created_at DESC, al.id DESC
       LIMIT ${filters.limit}
       OFFSET ${filters.offset}`,
      params
    ),
    runQuery(
      `SELECT
         al.action,
         COUNT(*) AS row_count
       FROM audit_logs al
       WHERE ${whereSql}
       GROUP BY al.action
       ORDER BY row_count DESC, al.action ASC`,
      params
    ),
  ]);

  const rows = (rowsResult.rows || []).map((row) =>
    mapAuditRow(row, Boolean(filters.includePayload))
  );

  return {
    tenantId: filters.tenantId,
    legalEntityId: filters.legalEntityId || null,
    action: filters.action || null,
    resourceType: filters.resourceType || null,
    resourceId: filters.resourceId || null,
    actorUserId: filters.actorUserId || null,
    requestId: filters.requestId || null,
    createdFrom: filters.createdFrom || null,
    createdTo: filters.createdTo || null,
    includePayload: Boolean(filters.includePayload),
    total: Number(countResult.rows?.[0]?.total || 0),
    limit: filters.limit,
    offset: filters.offset,
    byAction: toActionSummaryRows(byActionResult.rows),
    rows,
  };
}
