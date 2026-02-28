import express from "express";
import { query } from "../db.js";
import { getScopeContext, requirePermission } from "../middleware/rbac.js";
import {
  asyncHandler,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";

const router = express.Router();

const VALID_SCOPE_TYPES = new Set([
  "TENANT",
  "GROUP",
  "COUNTRY",
  "LEGAL_ENTITY",
  "OPERATING_UNIT",
]);

function normalizeScopeTypeOrNull(value, fieldName = "scopeType") {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const normalized = String(value).toUpperCase();
  if (!VALID_SCOPE_TYPES.has(normalized)) {
    throw badRequest(
      `${fieldName} must be one of TENANT, GROUP, COUNTRY, LEGAL_ENTITY, OPERATING_UNIT`
    );
  }
  return normalized;
}

function parsePagination(queryParams) {
  const page = parsePositiveInt(queryParams.page) || 1;
  const pageSize = Math.min(parsePositiveInt(queryParams.pageSize) || 50, 200);
  const offset = (page - 1) * pageSize;
  return { page, pageSize, offset };
}

function buildScopedVisibilityCondition(scopeContext, params) {
  if (!scopeContext) {
    return "1 = 0";
  }
  if (scopeContext.tenantWide) {
    return "1 = 1";
  }

  const clauses = [];
  const byType = [
    ["GROUP", scopeContext.groups],
    ["COUNTRY", scopeContext.countries],
    ["LEGAL_ENTITY", scopeContext.legalEntities],
    ["OPERATING_UNIT", scopeContext.operatingUnits],
  ];

  for (const [scopeType, set] of byType) {
    const ids = Array.from(set || []);
    if (ids.length === 0) {
      continue;
    }
    params.push(...ids);
    clauses.push(
      `(l.scope_type = '${scopeType}' AND l.scope_id IN (${ids
        .map(() => "?")
        .join(", ")}))`
    );
  }

  if (clauses.length === 0) {
    return "1 = 0";
  }

  return `(${clauses.join(" OR ")})`;
}

router.get(
  "/audit-logs",
  requirePermission("security.audit.read"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const { page, pageSize, offset } = parsePagination(req.query);
    const scopeType =
      normalizeScopeTypeOrNull(req.query.scopeType) ||
      normalizeScopeTypeOrNull(req.query.orgScopeType, "orgScopeType");
    const scopeId = parsePositiveInt(req.query.scopeId || req.query.orgScopeId);
    const actorUserId = parsePositiveInt(req.query.actorUserId);
    const targetUserId = parsePositiveInt(req.query.targetUserId);
    const action = req.query.action ? String(req.query.action).trim() : null;
    const resourceType = req.query.resourceType
      ? String(req.query.resourceType).trim()
      : null;
    const createdFrom = req.query.createdFrom
      ? String(req.query.createdFrom).trim()
      : null;
    const createdTo = req.query.createdTo
      ? String(req.query.createdTo).trim()
      : null;

    if (scopeType && !scopeId) {
      throw badRequest("scopeId is required when scopeType is provided");
    }

    const conditions = ["l.tenant_id = ?"];
    const params = [tenantId];

    const scopeContext = getScopeContext(req);
    conditions.push(buildScopedVisibilityCondition(scopeContext, params));

    if (scopeType) {
      conditions.push("l.scope_type = ?");
      params.push(scopeType);
    }
    if (scopeId) {
      conditions.push("l.scope_id = ?");
      params.push(scopeId);
    }
    if (actorUserId) {
      conditions.push("l.actor_user_id = ?");
      params.push(actorUserId);
    }
    if (targetUserId) {
      conditions.push("l.target_user_id = ?");
      params.push(targetUserId);
    }
    if (action) {
      conditions.push("l.action = ?");
      params.push(action);
    }
    if (resourceType) {
      conditions.push("l.resource_type = ?");
      params.push(resourceType);
    }
    if (createdFrom) {
      conditions.push("l.created_at >= ?");
      params.push(createdFrom);
    }
    if (createdTo) {
      conditions.push("l.created_at <= ?");
      params.push(createdTo);
    }

    const whereClause = conditions.join(" AND ");
    const countResult = await query(
      `SELECT COUNT(*) AS total
       FROM rbac_audit_logs l
       WHERE ${whereClause}`,
      params
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const rowsResult = await query(
      `SELECT
         l.id,
         l.tenant_id,
         l.actor_user_id,
         actor.email AS actor_user_email,
         actor.name AS actor_user_name,
         l.target_user_id,
         target.email AS target_user_email,
         target.name AS target_user_name,
         l.action,
         l.resource_type,
         l.resource_id,
         l.scope_type,
         l.scope_id,
         l.request_id,
         l.ip_address,
         l.user_agent,
         l.payload_json,
         l.created_at
       FROM rbac_audit_logs l
       LEFT JOIN users actor ON actor.id = l.actor_user_id
       LEFT JOIN users target ON target.id = l.target_user_id
       WHERE ${whereClause}
       ORDER BY l.created_at DESC, l.id DESC
       LIMIT ${pageSize}
       OFFSET ${offset}`,
      params
    );

    return res.json({
      tenantId,
      filters: {
        scopeType: scopeType || null,
        scopeId: scopeId || null,
        actorUserId: actorUserId || null,
        targetUserId: targetUserId || null,
        action,
        resourceType,
        createdFrom,
        createdTo,
      },
      pagination: {
        page,
        pageSize,
        total,
        totalPages: total > 0 ? Math.ceil(total / pageSize) : 0,
      },
      rows: rowsResult.rows,
    });
  })
);

export default router;
