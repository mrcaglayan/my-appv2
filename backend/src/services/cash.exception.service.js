import { query } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { findCashRegisterById } from "./cash.queries.js";

function emptySection() {
  return {
    total: 0,
    rows: [],
  };
}

function normalizeBoolean(value, fallback = false) {
  if (typeof value === "boolean") {
    return value;
  }
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "n"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function isTypeRequested(typeSet, typeCode) {
  if (!typeSet || typeSet.size === 0) {
    return true;
  }
  return typeSet.has(typeCode);
}

function legalEntityScopeExpression() {
  return "COALESCE(je.legal_entity_id, CASE WHEN UPPER(al.scope_type) = 'LEGAL_ENTITY' THEN CAST(al.scope_id AS UNSIGNED) ELSE NULL END)";
}

function normalizeSqlJsonValue(value) {
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value === "object") {
    return value;
  }
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function normalizeLimit(value, fallback = 100) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function normalizeOffset(value, fallback = 0) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    return fallback;
  }
  return parsed;
}

async function runCountAndRows({
  countSql,
  rowSql,
  countParams,
  rowParams,
  includeRows,
  label = "unknown",
}) {
  let total = 0;
  try {
    const countResult = await query(countSql, countParams);
    total = Number(countResult.rows?.[0]?.total || 0);
  } catch (err) {
    err.message = `cash.exceptions.${label}.count failed: ${err.message}`;
    throw err;
  }

  if (!normalizeBoolean(includeRows, true)) {
    return { total, rows: [] };
  }

  try {
    const rowResult = await query(rowSql, rowParams);
    return {
      total,
      rows: rowResult.rows || [],
    };
  } catch (err) {
    err.message = `cash.exceptions.${label}.rows failed: ${err.message}`;
    throw err;
  }
}

function buildSessionFilters({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  registerId,
}) {
  const params = [tenantId];
  const conditions = ["cs.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "cr.legal_entity_id", params));

  if (filters.legalEntityId) {
    conditions.push("cr.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }
  if (filters.operatingUnitId) {
    conditions.push("cr.operating_unit_id = ?");
    params.push(filters.operatingUnitId);
  }
  if (registerId) {
    conditions.push("cs.cash_register_id = ?");
    params.push(registerId);
  }
  if (filters.fromDate) {
    conditions.push("DATE(COALESCE(cs.closed_at, cs.opened_at)) >= ?");
    params.push(filters.fromDate);
  }
  if (filters.toDate) {
    conditions.push("DATE(COALESCE(cs.closed_at, cs.opened_at)) <= ?");
    params.push(filters.toDate);
  }

  return {
    whereSql: conditions.join(" AND "),
    params,
  };
}

function buildTransactionFilters({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  registerId,
}) {
  const params = [tenantId];
  const conditions = ["ct.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "cr.legal_entity_id", params));

  if (filters.legalEntityId) {
    conditions.push("cr.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }
  if (filters.operatingUnitId) {
    conditions.push("cr.operating_unit_id = ?");
    params.push(filters.operatingUnitId);
  }
  if (registerId) {
    conditions.push("ct.cash_register_id = ?");
    params.push(registerId);
  }
  if (filters.fromDate) {
    conditions.push("ct.book_date >= ?");
    params.push(filters.fromDate);
  }
  if (filters.toDate) {
    conditions.push("ct.book_date <= ?");
    params.push(filters.toDate);
  }

  return {
    whereSql: conditions.join(" AND "),
    params,
  };
}

function buildGlAuditFilters({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  registerAccountId,
  actionCodes,
}) {
  const legalScopeSql = legalEntityScopeExpression();
  const params = [tenantId];
  const conditions = ["al.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", legalScopeSql, params));

  if (filters.legalEntityId) {
    conditions.push(`${legalScopeSql} = ?`);
    params.push(filters.legalEntityId);
  }
  if (filters.fromDate) {
    conditions.push("DATE(al.created_at) >= ?");
    params.push(filters.fromDate);
  }
  if (filters.toDate) {
    conditions.push("DATE(al.created_at) <= ?");
    params.push(filters.toDate);
  }
  if (filters.operatingUnitId) {
    conditions.push(
      "EXISTS (SELECT 1 FROM journal_lines jl_ou WHERE jl_ou.journal_entry_id = je.id AND jl_ou.operating_unit_id = ?)"
    );
    params.push(filters.operatingUnitId);
  }
  if (registerAccountId) {
    conditions.push(
      "EXISTS (SELECT 1 FROM journal_lines jl_reg WHERE jl_reg.journal_entry_id = je.id AND jl_reg.account_id = ?)"
    );
    params.push(registerAccountId);
  }
  if (Array.isArray(actionCodes) && actionCodes.length > 0) {
    conditions.push(`al.action IN (${actionCodes.map(() => "?").join(", ")})`);
    params.push(...actionCodes);
  } else {
    conditions.push("1 = 0");
  }

  return {
    whereSql: conditions.join(" AND "),
    params,
  };
}

function normalizeGlAuditRows(rows) {
  return (rows || []).map((row) => ({
    ...row,
    payload_json: normalizeSqlJsonValue(row.payload_json),
  }));
}

export async function listCashExceptionSnapshot({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }

  const requestedTypes = new Set(
    (Array.isArray(filters.types) ? filters.types : []).map((value) =>
      String(value || "").trim().toUpperCase()
    )
  );
  const includeRows = normalizeBoolean(filters.includeRows, true);

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
  }
  if (filters.operatingUnitId) {
    assertScopeAccess(req, "operating_unit", filters.operatingUnitId, "operatingUnitId");
  }

  let register = null;
  let registerAccountId = null;
  if (filters.registerId) {
    register = await findCashRegisterById({
      tenantId,
      registerId: filters.registerId,
    });
    if (!register) {
      throw badRequest("registerId not found for tenant");
    }

    assertScopeAccess(req, "legal_entity", register.legal_entity_id, "registerId");
    if (register.operating_unit_id) {
      assertScopeAccess(req, "operating_unit", register.operating_unit_id, "registerId");
    }

    if (
      filters.legalEntityId &&
      parsePositiveInt(register.legal_entity_id) !== filters.legalEntityId
    ) {
      throw badRequest("registerId does not belong to legalEntityId");
    }
    if (
      filters.operatingUnitId &&
      parsePositiveInt(register.operating_unit_id) !== filters.operatingUnitId
    ) {
      throw badRequest("registerId does not belong to operatingUnitId");
    }

    registerAccountId = parsePositiveInt(register.account_id);
  }

  const sessionFromSql = `
    FROM cash_sessions cs
    JOIN cash_registers cr ON cr.id = cs.cash_register_id
    JOIN legal_entities le ON le.id = cr.legal_entity_id
    LEFT JOIN operating_units ou ON ou.id = cr.operating_unit_id
    LEFT JOIN users closeu ON closeu.id = cs.closed_by_user_id
  `;

  const transactionFromSql = `
    FROM cash_transactions ct
    JOIN cash_registers cr ON cr.id = ct.cash_register_id
    JOIN legal_entities le ON le.id = cr.legal_entity_id
    LEFT JOIN operating_units ou ON ou.id = cr.operating_unit_id
  `;

  const glLegalScopeSql = legalEntityScopeExpression();
  const glFromSql = `
    FROM audit_logs al
    LEFT JOIN journal_entries je
      ON je.id = CAST(al.resource_id AS UNSIGNED)
     AND je.tenant_id = al.tenant_id
    LEFT JOIN legal_entities le
      ON le.id = ${glLegalScopeSql}
  `;

  const sessionFilters = buildSessionFilters({
    req,
    tenantId,
    filters,
    buildScopeFilter,
    registerId: filters.registerId,
  });
  const transactionFilters = buildTransactionFilters({
    req,
    tenantId,
    filters,
    buildScopeFilter,
    registerId: filters.registerId,
  });

  const glActionCodes = [];
  if (isTypeRequested(requestedTypes, "GL_CASH_CONTROL_WARN")) {
    glActionCodes.push("gl.cash_control.warn");
  }
  if (isTypeRequested(requestedTypes, "GL_CASH_CONTROL_OVERRIDE")) {
    glActionCodes.push("gl.cash_control.override");
  }
  const glFilters = buildGlAuditFilters({
    req,
    tenantId,
    filters,
    buildScopeFilter,
    registerAccountId,
    actionCodes: glActionCodes,
  });

  const safeLimit = normalizeLimit(filters.limit, 100);
  const safeOffset = normalizeOffset(filters.offset, 0);

  const highVariancePromise = isTypeRequested(requestedTypes, "HIGH_VARIANCE")
      ? runCountAndRows({
          label: "highVariance",
          countSql: `
          SELECT COUNT(*) AS total
          ${sessionFromSql}
          WHERE ${sessionFilters.whereSql}
            AND cs.closed_at IS NOT NULL
            AND ABS(COALESCE(cs.variance_amount, 0)) > ?
        `,
        rowSql: `
          SELECT
            cs.id,
            cs.status,
            cs.cash_register_id,
            cs.expected_closing_amount,
            cs.counted_closing_amount,
            cs.variance_amount,
            cs.closed_reason,
            cs.close_note,
            cs.closed_at,
            cr.legal_entity_id,
            cr.operating_unit_id,
            cr.code AS cash_register_code,
            cr.name AS cash_register_name,
            le.code AS legal_entity_code,
            le.name AS legal_entity_name,
            ou.code AS operating_unit_code,
            ou.name AS operating_unit_name,
            closeu.email AS closed_by_email
          ${sessionFromSql}
          WHERE ${sessionFilters.whereSql}
            AND cs.closed_at IS NOT NULL
            AND ABS(COALESCE(cs.variance_amount, 0)) > ?
          ORDER BY ABS(COALESCE(cs.variance_amount, 0)) DESC, cs.id DESC
          LIMIT ${safeLimit}
          OFFSET ${safeOffset}
        `,
        countParams: [...sessionFilters.params, filters.minAbsVariance],
        rowParams: [...sessionFilters.params, filters.minAbsVariance],
        includeRows,
      })
    : Promise.resolve(emptySection());

  const forcedClosePromise = isTypeRequested(requestedTypes, "FORCED_CLOSE")
      ? runCountAndRows({
          label: "forcedClose",
          countSql: `
          SELECT COUNT(*) AS total
          ${sessionFromSql}
          WHERE ${sessionFilters.whereSql}
            AND UPPER(cs.closed_reason) = 'FORCED_CLOSE'
        `,
        rowSql: `
          SELECT
            cs.id,
            cs.status,
            cs.cash_register_id,
            cs.closed_reason,
            cs.close_note,
            cs.closed_at,
            cs.variance_amount,
            cr.legal_entity_id,
            cr.operating_unit_id,
            cr.code AS cash_register_code,
            cr.name AS cash_register_name,
            le.code AS legal_entity_code,
            le.name AS legal_entity_name,
            ou.code AS operating_unit_code,
            ou.name AS operating_unit_name,
            closeu.email AS closed_by_email
          ${sessionFromSql}
          WHERE ${sessionFilters.whereSql}
            AND UPPER(cs.closed_reason) = 'FORCED_CLOSE'
          ORDER BY cs.closed_at DESC, cs.id DESC
          LIMIT ${safeLimit}
          OFFSET ${safeOffset}
        `,
        countParams: sessionFilters.params,
        rowParams: sessionFilters.params,
        includeRows,
      })
    : Promise.resolve(emptySection());

  const overrideUsagePromise = isTypeRequested(requestedTypes, "OVERRIDE_USAGE")
      ? runCountAndRows({
          label: "overrideUsage",
          countSql: `
          SELECT COUNT(*) AS total
          ${transactionFromSql}
          WHERE ${transactionFilters.whereSql}
            AND (ct.override_reason IS NOT NULL OR ct.override_cash_control = TRUE)
        `,
        rowSql: `
          SELECT
            ct.id,
            ct.txn_no,
            ct.txn_type,
            ct.status,
            ct.book_date,
            ct.amount,
            ct.currency_code,
            ct.override_cash_control,
            ct.override_reason,
            ct.posted_journal_entry_id,
            ct.created_at,
            ct.cash_register_id,
            cr.legal_entity_id,
            cr.operating_unit_id,
            cr.code AS cash_register_code,
            cr.name AS cash_register_name,
            le.code AS legal_entity_code,
            le.name AS legal_entity_name,
            ou.code AS operating_unit_code,
            ou.name AS operating_unit_name
          ${transactionFromSql}
          WHERE ${transactionFilters.whereSql}
            AND (ct.override_reason IS NOT NULL OR ct.override_cash_control = TRUE)
          ORDER BY ct.id DESC
          LIMIT ${safeLimit}
          OFFSET ${safeOffset}
        `,
        countParams: transactionFilters.params,
        rowParams: transactionFilters.params,
        includeRows,
      })
    : Promise.resolve(emptySection());

  const unpostedPromise = isTypeRequested(requestedTypes, "UNPOSTED")
      ? runCountAndRows({
          label: "unposted",
          countSql: `
          SELECT COUNT(*) AS total
          ${transactionFromSql}
          WHERE ${transactionFilters.whereSql}
            AND ct.status IN ('DRAFT', 'SUBMITTED', 'APPROVED')
        `,
        rowSql: `
          SELECT
            ct.id,
            ct.txn_no,
            ct.txn_type,
            ct.status,
            ct.book_date,
            ct.amount,
            ct.currency_code,
            ct.created_at,
            ct.cash_register_id,
            cr.legal_entity_id,
            cr.operating_unit_id,
            cr.code AS cash_register_code,
            cr.name AS cash_register_name,
            le.code AS legal_entity_code,
            le.name AS legal_entity_name,
            ou.code AS operating_unit_code,
            ou.name AS operating_unit_name
          ${transactionFromSql}
          WHERE ${transactionFilters.whereSql}
            AND ct.status IN ('DRAFT', 'SUBMITTED', 'APPROVED')
          ORDER BY ct.id DESC
          LIMIT ${safeLimit}
          OFFSET ${safeOffset}
        `,
        countParams: transactionFilters.params,
        rowParams: transactionFilters.params,
        includeRows,
      })
    : Promise.resolve(emptySection());

  const glCashControlEventsPromise =
    glActionCodes.length > 0
      ? runCountAndRows({
          label: "glCashControlEvents",
          countSql: `
            SELECT COUNT(*) AS total
            ${glFromSql}
            WHERE ${glFilters.whereSql}
          `,
          rowSql: `
            SELECT
              al.id,
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
              je.journal_no,
              je.legal_entity_id AS journal_legal_entity_id,
              le.code AS legal_entity_code,
              le.name AS legal_entity_name
            ${glFromSql}
            WHERE ${glFilters.whereSql}
            ORDER BY al.id DESC
            LIMIT ${safeLimit}
            OFFSET ${safeOffset}
          `,
          countParams: glFilters.params,
          rowParams: glFilters.params,
          includeRows,
        }).then((result) => ({
          total: result.total,
          rows: normalizeGlAuditRows(result.rows),
        }))
      : Promise.resolve(emptySection());

  const [
    highVariance,
    forcedClose,
    overrideUsage,
    unposted,
    glCashControlEvents,
  ] = await Promise.all([
    highVariancePromise,
    forcedClosePromise,
    overrideUsagePromise,
    unpostedPromise,
    glCashControlEventsPromise,
  ]);

  const notes = [];
  if (filters.registerId && registerAccountId) {
    notes.push(
      "Direct GL cash-control events are filtered by the selected register account for registerId scope."
    );
  }

  return {
    sections: {
      highVariance,
      forcedClose,
      overrideUsage,
      unposted,
      glCashControlEvents,
    },
    summary: {
      highVarianceCount: highVariance.total,
      forcedCloseCount: forcedClose.total,
      overrideUsageCount: overrideUsage.total,
      unpostedCount: unposted.total,
      glCashControlEventCount: glCashControlEvents.total,
    },
    notes,
  };
}
