import { query } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";

function mapPaymentTermRow(row) {
  return {
    id: parsePositiveInt(row.id),
    tenantId: parsePositiveInt(row.tenant_id),
    legalEntityId: parsePositiveInt(row.legal_entity_id),
    code: row.code,
    name: row.name,
    dueDays: Number(row.due_days || 0),
    graceDays: Number(row.grace_days || 0),
    isEndOfMonth: row.is_end_of_month === true || row.is_end_of_month === 1 || row.is_end_of_month === "1",
    status: row.status,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
  };
}

export async function resolvePaymentTermScope(paymentTermId, tenantId) {
  const parsedPaymentTermId = parsePositiveInt(paymentTermId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedPaymentTermId || !parsedTenantId) {
    return null;
  }

  const result = await query(
    `SELECT legal_entity_id
     FROM payment_terms
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [parsedTenantId, parsedPaymentTermId]
  );
  const row = result.rows?.[0] || null;
  if (!row) {
    return null;
  }
  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: parsePositiveInt(row.legal_entity_id),
  };
}

export async function listPaymentTerms({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["pt.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "pt.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("pt.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.status) {
    conditions.push("pt.status = ?");
    params.push(filters.status);
  }

  if (filters.q) {
    conditions.push("(pt.code LIKE ? OR pt.name LIKE ?)");
    params.push(`%${filters.q}%`, `%${filters.q}%`);
  }

  const whereSql = conditions.join(" AND ");
  const totalResult = await query(
    `SELECT COUNT(*) AS row_count
     FROM payment_terms pt
     WHERE ${whereSql}`,
    params
  );
  const total = Number(totalResult.rows?.[0]?.row_count || 0);

  const safeLimit =
    Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset =
    Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;

  const result = await query(
    `SELECT
        pt.id,
        pt.tenant_id,
        pt.legal_entity_id,
        pt.code,
        pt.name,
        pt.due_days,
        pt.grace_days,
        pt.is_end_of_month,
        pt.status,
        pt.created_at,
        pt.updated_at
     FROM payment_terms pt
     WHERE ${whereSql}
     ORDER BY pt.code ASC, pt.id ASC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: (result.rows || []).map(mapPaymentTermRow),
    total,
    limit: safeLimit,
    offset: safeOffset,
  };
}

export async function getPaymentTermByIdForTenant({
  req,
  tenantId,
  paymentTermId,
  assertScopeAccess,
}) {
  const result = await query(
    `SELECT
        id,
        tenant_id,
        legal_entity_id,
        code,
        name,
        due_days,
        grace_days,
        is_end_of_month,
        status,
        created_at,
        updated_at
     FROM payment_terms
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, paymentTermId]
  );
  const row = result.rows?.[0] || null;
  if (!row) {
    throw badRequest("Payment term not found");
  }

  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "paymentTermId");
  return mapPaymentTermRow(row);
}
