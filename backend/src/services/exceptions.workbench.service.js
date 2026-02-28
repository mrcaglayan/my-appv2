import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  buildOffsetPaginationResult,
  resolveOffsetPagination,
} from "../utils/pagination.js";

function u(value) {
  return String(value || "").trim().toUpperCase();
}

function toInt(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? Number(Math.trunc(n)) : fallback;
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function parseJson(value, fallback = null) {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

const SLA_HOURS_BY_SEVERITY = Object.freeze({
  CRITICAL: 4,
  HIGH: 24,
  MEDIUM: 72,
  LOW: 120,
});

function parseDateTime(value) {
  if (!value) return null;
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }
  const parsed = new Date(String(value));
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function addHours(date, hours) {
  const next = new Date(date.getTime());
  next.setTime(next.getTime() + Number(hours || 0) * 60 * 60 * 1000);
  return next;
}

function formatSqlDateTime(date) {
  return date.toISOString().slice(0, 19).replace("T", " ");
}

function computeSlaDueAt({ severity, anchorAt }) {
  const normalizedSeverity = u(severity);
  const hours = SLA_HOURS_BY_SEVERITY[normalizedSeverity] ?? SLA_HOURS_BY_SEVERITY.MEDIUM;
  const anchorDate = parseDateTime(anchorAt) || new Date();
  return formatSqlDateTime(addHours(anchorDate, hours));
}

function parseIsoDate(value, label = "date") {
  if (!value) return null;
  const raw = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw badRequest(`${label} must be YYYY-MM-DD`);
  }
  const d = new Date(`${raw}T00:00:00.000Z`);
  if (Number.isNaN(d.getTime())) throw badRequest(`${label} is invalid`);
  return d;
}

function formatIsoDate(date) {
  return date.toISOString().slice(0, 10);
}

function addDays(date, days) {
  const next = new Date(date.getTime());
  next.setUTCDate(next.getUTCDate() + Number(days || 0));
  return next;
}

function normalizeWindow(filters = {}, defaultDays = 180) {
  const rawDays = Number(filters.days);
  const days = Number.isInteger(rawDays) && rawDays > 0 ? Math.min(rawDays, 3660) : defaultDays;

  let dateFrom = parseIsoDate(filters.dateFrom, "dateFrom");
  let dateTo = parseIsoDate(filters.dateTo, "dateTo");
  const today = parseIsoDate(formatIsoDate(new Date()), "today");

  if (!dateFrom && !dateTo) {
    dateTo = today;
    dateFrom = addDays(dateTo, -(days - 1));
  } else if (!dateFrom && dateTo) {
    dateFrom = addDays(dateTo, -(days - 1));
  } else if (dateFrom && !dateTo) {
    dateTo = addDays(dateFrom, days - 1);
  }

  if (dateFrom.getTime() > dateTo.getTime()) {
    throw badRequest("dateFrom cannot be after dateTo");
  }

  return {
    days,
    dateFrom: formatIsoDate(dateFrom),
    dateTo: formatIsoDate(dateTo),
    startTs: `${formatIsoDate(dateFrom)} 00:00:00`,
    endExclusiveTs: `${formatIsoDate(addDays(dateTo, 1))} 00:00:00`,
  };
}

function notFound(message) {
  const err = new Error(message);
  err.status = 404;
  return err;
}

function conflict(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}

function buildScopedLegalEntityWhere({
  req,
  tenantId,
  filters,
  alias,
  params,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const where = [`${alias}.tenant_id = ?`];
  params.push(tenantId);

  const legalEntityId = parsePositiveInt(filters.legalEntityId);
  if (legalEntityId) {
    if (typeof assertScopeAccess === "function") {
      assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
    }
    where.push(`${alias}.legal_entity_id = ?`);
    params.push(legalEntityId);
  } else if (typeof buildScopeFilter === "function") {
    where.push(buildScopeFilter(req, "legal_entity", `${alias}.legal_entity_id`, params));
  }

  return where;
}

function mapBankReconExceptionStatus(status) {
  const value = u(status);
  if (value === "ASSIGNED") return "IN_REVIEW";
  if (value === "RESOLVED") return "RESOLVED";
  if (value === "IGNORED") return "IGNORED";
  return "OPEN";
}

function mapBankReconExceptionSeverity(value) {
  const level = u(value);
  if (["LOW", "MEDIUM", "HIGH", "CRITICAL"].includes(level)) return level;
  return "MEDIUM";
}

function mapBankReturnStatus(eventStatus) {
  const status = u(eventStatus);
  if (status === "IGNORED") return "IGNORED";
  return "OPEN";
}

function mapBankReturnSeverity(eventType, reasonCode) {
  const t = u(eventType);
  const rc = u(reasonCode);
  if (t === "PAYMENT_REJECTED" || rc.includes("REJECT")) return "HIGH";
  if (t === "PAYMENT_RETURNED") return "MEDIUM";
  return "LOW";
}

function mapPayrollImportStatus(jobStatus) {
  const status = u(jobStatus);
  if (status === "APPLIED") return "RESOLVED";
  if (status === "REJECTED") return "OPEN";
  if (status === "FAILED") return "OPEN";
  if (status === "APPLYING") return "IN_REVIEW";
  return "OPEN";
}

function mapPayrollImportSeverity(jobStatus) {
  const status = u(jobStatus);
  if (status === "FAILED") return "HIGH";
  if (status === "REJECTED") return "MEDIUM";
  if (status === "APPLYING") return "LOW";
  return "LOW";
}

function mapPayrollCheckSeverity(severity, checkStatus) {
  const s = u(severity);
  if (s === "ERROR") return "HIGH";
  if (s === "WARN") return u(checkStatus) === "FAIL" ? "MEDIUM" : "LOW";
  return "LOW";
}

function mapPayrollCloseCheckStatus(checkStatus) {
  const status = u(checkStatus);
  return status === "FAIL" ? "OPEN" : "RESOLVED";
}

function hydrateWorkbenchRow(row) {
  if (!row) return null;
  return {
    ...row,
    payload_json: parseJson(row.payload_json, null),
  };
}

function buildListOrdering(sortBy = "priority") {
  const key = u(sortBy);
  if (key === "LAST_SEEN_ASC") {
    return "ew.last_seen_at ASC, ew.id ASC";
  }
  if (key === "UPDATED_DESC") {
    return "ew.updated_at DESC, ew.id DESC";
  }
  if (key === "URGENCY" || key === "URGENCY_ASC") {
    return `CASE
              WHEN ew.status IN ('RESOLVED', 'IGNORED') THEN 1
              ELSE 0
            END ASC,
            CASE
              WHEN ew.sla_due_at IS NULL THEN 1
              ELSE 0
            END ASC,
            ew.sla_due_at ASC,
            CASE ew.severity
              WHEN 'CRITICAL' THEN 0
              WHEN 'HIGH' THEN 1
              WHEN 'MEDIUM' THEN 2
              WHEN 'LOW' THEN 3
              ELSE 4
            END ASC,
            ew.last_seen_at DESC,
            ew.id DESC`;
  }
  return `CASE ew.status
            WHEN 'OPEN' THEN 0
            WHEN 'IN_REVIEW' THEN 1
            WHEN 'RESOLVED' THEN 2
            WHEN 'IGNORED' THEN 3
            ELSE 4
          END ASC,
          CASE ew.severity
            WHEN 'CRITICAL' THEN 0
            WHEN 'HIGH' THEN 1
            WHEN 'MEDIUM' THEN 2
            WHEN 'LOW' THEN 3
            ELSE 4
          END ASC,
          ew.last_seen_at DESC,
          ew.id DESC`;
}

async function upsertExceptionRow({ runQuery, item }) {
  await runQuery(
    `INSERT INTO exception_workbench (
        tenant_id,
        legal_entity_id,
        module_code,
        exception_type,
        source_type,
        source_key,
        source_ref,
        source_ref_id,
        source_status_code,
        severity,
        sla_due_at,
        status,
        owner_user_id,
        title,
        description,
        payload_json,
        last_seen_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON DUPLICATE KEY UPDATE
        legal_entity_id = VALUES(legal_entity_id),
        module_code = VALUES(module_code),
        exception_type = VALUES(exception_type),
        source_type = VALUES(source_type),
        source_ref = VALUES(source_ref),
        source_ref_id = VALUES(source_ref_id),
        source_status_code = VALUES(source_status_code),
        severity = VALUES(severity),
        sla_due_at = COALESCE(VALUES(sla_due_at), exception_workbench.sla_due_at),
        title = VALUES(title),
        description = VALUES(description),
        payload_json = VALUES(payload_json),
        status = CASE
          WHEN VALUES(status) = 'OPEN' THEN
            CASE
              WHEN exception_workbench.status = 'IN_REVIEW' THEN 'IN_REVIEW'
              WHEN exception_workbench.status = 'IGNORED' THEN 'IGNORED'
              ELSE 'OPEN'
            END
          WHEN VALUES(status) = 'IN_REVIEW' THEN
            CASE
              WHEN exception_workbench.status = 'IGNORED' THEN 'IGNORED'
              ELSE 'IN_REVIEW'
            END
          ELSE VALUES(status)
        END,
        owner_user_id = CASE
          WHEN VALUES(status) IN ('OPEN','IN_REVIEW') THEN COALESCE(VALUES(owner_user_id), exception_workbench.owner_user_id)
          ELSE exception_workbench.owner_user_id
        END,
        last_seen_at = CURRENT_TIMESTAMP`,
    [
      item.tenant_id,
      item.legal_entity_id || null,
      item.module_code,
      item.exception_type,
      item.source_type,
      item.source_key,
      item.source_ref || null,
      item.source_ref_id || null,
      item.source_status_code || null,
      item.severity,
      item.sla_due_at || null,
      item.status,
      item.owner_user_id || null,
      item.title,
      item.description || null,
      safeJson(item.payload_json || null),
    ]
  );
}

async function collectBankReconciliationExceptionItems({
  req,
  tenantId,
  filters,
  window,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [];
  const where = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "e",
    params,
    buildScopeFilter,
    assertScopeAccess,
  });
  where.push("e.first_seen_at >= ?");
  params.push(window.startTs);
  where.push("e.first_seen_at < ?");
  params.push(window.endExclusiveTs);

  const res = await query(
    `SELECT
        e.id,
        e.tenant_id,
        e.legal_entity_id,
        e.statement_line_id,
        e.bank_account_id,
        e.status AS source_status,
        e.severity AS source_severity,
        e.reason_code,
        e.reason_message,
        e.occurrence_count,
        e.suggested_action_type,
        e.suggested_payload_json,
        e.assigned_to_user_id,
        e.first_seen_at,
        e.last_seen_at
     FROM bank_reconciliation_exceptions e
     WHERE ${where.join(" AND ")}`,
    params
  );

  return (res.rows || []).map((row) => {
    const severity = mapBankReconExceptionSeverity(row.source_severity);
    return {
      tenant_id: parsePositiveInt(row.tenant_id),
      legal_entity_id: parsePositiveInt(row.legal_entity_id),
      module_code: "BANK",
      exception_type: "BANK_RECON_EXCEPTION",
      source_type: "BANK_RECON_EXCEPTION",
      source_key: `BANK:RECON_EXCEPTION:${row.id}`,
      source_ref: row.reason_code || `LINE:${row.statement_line_id}`,
      source_ref_id: parsePositiveInt(row.id),
      source_status_code: u(row.source_status),
      severity,
      sla_due_at: computeSlaDueAt({
        severity,
        anchorAt: row.first_seen_at || row.last_seen_at,
      }),
      status: mapBankReconExceptionStatus(row.source_status),
      owner_user_id: parsePositiveInt(row.assigned_to_user_id) || null,
      title: `Bank reconciliation exception: ${row.reason_code || "UNSPECIFIED"}`,
      description: row.reason_message || null,
      payload_json: {
        statement_line_id: parsePositiveInt(row.statement_line_id) || null,
        bank_account_id: parsePositiveInt(row.bank_account_id) || null,
        occurrence_count: toInt(row.occurrence_count, 1),
        suggested_action_type: row.suggested_action_type || null,
        suggested_payload_json: parseJson(row.suggested_payload_json, null),
        first_seen_at: row.first_seen_at || null,
        last_seen_at: row.last_seen_at || null,
      },
    };
  });
}

async function collectBankPaymentReturnItems({
  req,
  tenantId,
  filters,
  window,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [];
  const where = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "e",
    params,
    buildScopeFilter,
    assertScopeAccess,
  });
  where.push("e.created_at >= ?");
  params.push(window.startTs);
  where.push("e.created_at < ?");
  params.push(window.endExclusiveTs);

  const res = await query(
    `SELECT
        e.id,
        e.tenant_id,
        e.legal_entity_id,
        e.event_type,
        e.event_status,
        e.reason_code,
        e.reason_message,
        e.amount,
        e.currency_code,
        e.payment_batch_id,
        e.payment_batch_line_id,
        e.bank_statement_line_id,
        e.payment_batch_ack_import_id,
        e.payment_batch_ack_import_line_id,
        e.source_type,
        e.source_ref,
        e.created_at,
        e.updated_at
     FROM bank_payment_return_events e
     WHERE ${where.join(" AND ")}`,
    params
  );

  return (res.rows || []).map((row) => {
    const severity = mapBankReturnSeverity(row.event_type, row.reason_code);
    return {
      tenant_id: parsePositiveInt(row.tenant_id),
      legal_entity_id: parsePositiveInt(row.legal_entity_id),
      module_code: "BANK",
      exception_type: "BANK_PAYMENT_RETURN",
      source_type: "BANK_PAYMENT_RETURN_EVENT",
      source_key: `BANK:PAYMENT_RETURN_EVENT:${row.id}`,
      source_ref: row.source_ref || row.reason_code || null,
      source_ref_id: parsePositiveInt(row.id),
      source_status_code: u(row.event_status),
      severity,
      sla_due_at: computeSlaDueAt({
        severity,
        anchorAt: row.created_at || row.updated_at,
      }),
      status: mapBankReturnStatus(row.event_status),
      owner_user_id: null,
      title: `Bank payment return: ${u(row.event_type || "RETURN_EVENT")}`,
      description: row.reason_message || null,
      payload_json: {
        payment_batch_id: parsePositiveInt(row.payment_batch_id) || null,
        payment_batch_line_id: parsePositiveInt(row.payment_batch_line_id) || null,
        bank_statement_line_id: parsePositiveInt(row.bank_statement_line_id) || null,
        payment_batch_ack_import_id: parsePositiveInt(row.payment_batch_ack_import_id) || null,
        payment_batch_ack_import_line_id: parsePositiveInt(row.payment_batch_ack_import_line_id) || null,
        event_type: u(row.event_type),
        event_status: u(row.event_status),
        amount: Number(row.amount || 0),
        currency_code: row.currency_code || null,
        source_type: row.source_type || null,
        created_at: row.created_at || null,
        updated_at: row.updated_at || null,
      },
    };
  });
}

async function collectPayrollImportExceptionItems({
  req,
  tenantId,
  filters,
  window,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [];
  const where = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "j",
    params,
    buildScopeFilter,
    assertScopeAccess,
  });
  where.push("j.requested_at >= ?");
  params.push(window.startTs);
  where.push("j.requested_at < ?");
  params.push(window.endExclusiveTs);
  where.push("j.status IN ('FAILED','REJECTED','APPLIED')");

  const res = await query(
    `SELECT
        j.id,
        j.tenant_id,
        j.legal_entity_id,
        j.provider_code,
        j.payroll_period,
        j.status AS source_status,
        j.failure_message,
        j.validation_errors_json,
        j.match_errors_json,
        j.requested_at,
        j.applied_at
     FROM payroll_provider_import_jobs j
     WHERE ${where.join(" AND ")}`,
    params
  );

  return (res.rows || []).map((row) => {
    const sourceStatus = u(row.source_status);
    const severity = mapPayrollImportSeverity(sourceStatus);
    const errorCount =
      (Array.isArray(parseJson(row.validation_errors_json, []))
        ? parseJson(row.validation_errors_json, []).length
        : 0) +
      (Array.isArray(parseJson(row.match_errors_json, []))
        ? parseJson(row.match_errors_json, []).length
        : 0);
    return {
      tenant_id: parsePositiveInt(row.tenant_id),
      legal_entity_id: parsePositiveInt(row.legal_entity_id),
      module_code: "PAYROLL",
      exception_type: "PAYROLL_PROVIDER_IMPORT",
      source_type: "PAYROLL_PROVIDER_IMPORT_JOB",
      source_key: `PAYROLL:PROVIDER_IMPORT_JOB:${row.id}`,
      source_ref: row.provider_code || null,
      source_ref_id: parsePositiveInt(row.id),
      source_status_code: sourceStatus,
      severity,
      sla_due_at: computeSlaDueAt({
        severity,
        anchorAt: row.requested_at || row.applied_at,
      }),
      status: mapPayrollImportStatus(sourceStatus),
      owner_user_id: null,
      title: `Payroll provider import ${sourceStatus}: ${row.provider_code || "UNKNOWN"}`,
      description: row.failure_message || (errorCount > 0 ? `${errorCount} validation/match errors` : null),
      payload_json: {
        provider_code: row.provider_code || null,
        payroll_period: row.payroll_period || null,
        source_status: sourceStatus,
        failure_message: row.failure_message || null,
        validation_errors_json: parseJson(row.validation_errors_json, null),
        match_errors_json: parseJson(row.match_errors_json, null),
        requested_at: row.requested_at || null,
        applied_at: row.applied_at || null,
      },
    };
  });
}

async function collectPayrollCloseCheckItems({
  req,
  tenantId,
  filters,
  window,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [];
  const where = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "pc",
    params,
    buildScopeFilter,
    assertScopeAccess,
  });
  where.push("pc.period_end >= ?");
  params.push(window.dateFrom);
  where.push("pc.period_start <= ?");
  params.push(window.dateTo);

  const res = await query(
    `SELECT
        c.id,
        c.tenant_id,
        c.legal_entity_id,
        c.check_code,
        c.check_name,
        c.severity,
        c.status AS check_status,
        c.metric_value,
        c.metric_text,
        c.details_json,
        c.updated_at,
        pc.id AS period_close_id,
        pc.period_start,
        pc.period_end,
        pc.status AS period_status
     FROM payroll_period_close_checks c
     JOIN payroll_period_closes pc
       ON pc.tenant_id = c.tenant_id
      AND pc.legal_entity_id = c.legal_entity_id
      AND pc.id = c.payroll_period_close_id
     WHERE ${where.join(" AND ")}`,
    params
  );

  return (res.rows || []).map((row) => {
    const checkStatus = u(row.check_status);
    const severity = mapPayrollCheckSeverity(row.severity, checkStatus);
    const periodRange = `${row.period_start || "?"}..${row.period_end || "?"}`;
    return {
      tenant_id: parsePositiveInt(row.tenant_id),
      legal_entity_id: parsePositiveInt(row.legal_entity_id),
      module_code: "PAYROLL",
      exception_type: "PAYROLL_CLOSE_CHECK",
      source_type: "PAYROLL_CLOSE_CHECK",
      source_key: `PAYROLL:CLOSE_CHECK:${row.period_close_id}:${row.check_code}`,
      source_ref: periodRange,
      source_ref_id: parsePositiveInt(row.period_close_id),
      source_status_code: checkStatus,
      severity,
      sla_due_at: computeSlaDueAt({
        severity,
        anchorAt: row.updated_at,
      }),
      status: mapPayrollCloseCheckStatus(checkStatus),
      owner_user_id: null,
      title: `Payroll close check: ${row.check_code}`,
      description: row.check_name || row.metric_text || null,
      payload_json: {
        period_close_id: parsePositiveInt(row.period_close_id) || null,
        period_start: row.period_start || null,
        period_end: row.period_end || null,
        period_status: row.period_status || null,
        check_code: row.check_code || null,
        check_name: row.check_name || null,
        severity: row.severity || null,
        check_status: checkStatus,
        metric_value: row.metric_value === null || row.metric_value === undefined ? null : Number(row.metric_value),
        metric_text: row.metric_text || null,
        details_json: parseJson(row.details_json, null),
        updated_at: row.updated_at || null,
      },
    };
  });
}

async function getExceptionWorkbenchRow({ tenantId, exceptionId, runQuery = query, forUpdate = false }) {
  const res = await runQuery(
    `SELECT *
     FROM exception_workbench
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1
     ${forUpdate ? "FOR UPDATE" : ""}`,
    [tenantId, exceptionId]
  );
  return hydrateWorkbenchRow(res.rows?.[0] || null);
}

async function insertExceptionWorkbenchAudit({
  runQuery = query,
  tenantId,
  exceptionId,
  eventType,
  fromStatus = null,
  toStatus = null,
  payload = null,
  actedByUserId = null,
}) {
  await runQuery(
    `INSERT INTO exception_workbench_audit (
        tenant_id,
        exception_workbench_id,
        event_type,
        from_status,
        to_status,
        payload_json,
        acted_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [
      tenantId,
      exceptionId,
      u(eventType),
      fromStatus || null,
      toStatus || null,
      safeJson(payload || null),
      parsePositiveInt(actedByUserId) || null,
    ]
  );
}

export async function resolveExceptionWorkbenchScope(exceptionId, tenantId) {
  const parsedId = parsePositiveInt(exceptionId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedId || !parsedTenantId) return null;
  const row = await getExceptionWorkbenchRow({
    tenantId: parsedTenantId,
    exceptionId: parsedId,
    runQuery: query,
  });
  if (!row) return null;
  const legalEntityId = parsePositiveInt(row.legal_entity_id);
  if (!legalEntityId) return null;
  return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
}

export async function refreshExceptionWorkbench({
  req,
  tenantId,
  filters = {},
  buildScopeFilter,
  assertScopeAccess,
}) {
  const window = normalizeWindow(filters, 180);
  const sources = await Promise.all([
    collectBankReconciliationExceptionItems({
      req,
      tenantId,
      filters,
      window,
      buildScopeFilter,
      assertScopeAccess,
    }),
    collectBankPaymentReturnItems({
      req,
      tenantId,
      filters,
      window,
      buildScopeFilter,
      assertScopeAccess,
    }),
    collectPayrollImportExceptionItems({
      req,
      tenantId,
      filters,
      window,
      buildScopeFilter,
      assertScopeAccess,
    }),
    collectPayrollCloseCheckItems({
      req,
      tenantId,
      filters,
      window,
      buildScopeFilter,
      assertScopeAccess,
    }),
  ]);

  const allItems = sources.flat();
  let processed = 0;
  await withTransaction(async (tx) => {
    for (const item of allItems) {
      // eslint-disable-next-line no-await-in-loop
      await upsertExceptionRow({ runQuery: tx.query, item });
      processed += 1;
    }
  });

  return {
    window,
    processed,
    by_source: {
      bank_reconciliation_exceptions: sources[0].length,
      bank_payment_return_events: sources[1].length,
      payroll_provider_import_jobs: sources[2].length,
      payroll_close_checks: sources[3].length,
    },
  };
}

export async function listExceptionWorkbenchRows({
  req,
  tenantId,
  filters = {},
  buildScopeFilter,
  assertScopeAccess,
}) {
  const shouldRefresh = filters.refresh !== false;
  let refreshSummary = null;
  if (shouldRefresh) {
    refreshSummary = await refreshExceptionWorkbench({
      req,
      tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
  }

  const params = [];
  const where = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "ew",
    params,
    buildScopeFilter,
    assertScopeAccess,
  });

  if (filters.moduleCode) {
    where.push("ew.module_code = ?");
    params.push(u(filters.moduleCode));
  }
  if (filters.status) {
    where.push("ew.status = ?");
    params.push(u(filters.status));
  }
  if (filters.severity) {
    where.push("ew.severity = ?");
    params.push(u(filters.severity));
  }
  if (filters.ownerUserId) {
    where.push("ew.owner_user_id = ?");
    params.push(parsePositiveInt(filters.ownerUserId));
  }
  if (filters.exceptionType) {
    where.push("ew.exception_type = ?");
    params.push(u(filters.exceptionType));
  }
  if (filters.sourceType) {
    where.push("ew.source_type = ?");
    params.push(u(filters.sourceType));
  }
  if (filters.q) {
    const q = `%${String(filters.q).trim()}%`;
    where.push("(ew.title LIKE ? OR ew.description LIKE ? OR ew.source_ref LIKE ? OR ew.source_key LIKE ?)");
    params.push(q, q, q, q);
  }

  const whereSql = where.join(" AND ");
  const countRes = await query(
    `SELECT COUNT(*) AS total
     FROM exception_workbench ew
     WHERE ${whereSql}`,
    params
  );
  const total = toInt(countRes.rows?.[0]?.total, 0);

  const pagination = resolveOffsetPagination(filters, {
    defaultLimit: 100,
    defaultOffset: 0,
    maxLimit: 500,
  });

  const listRes = await query(
    `SELECT ew.*
     FROM exception_workbench ew
     WHERE ${whereSql}
     ORDER BY ${buildListOrdering(filters.sortBy)}
     LIMIT ${pagination.limit} OFFSET ${pagination.offset}`,
    params
  );

  const summaryStatusRes = await query(
    `SELECT ew.status, COUNT(*) AS total
     FROM exception_workbench ew
     WHERE ${whereSql}
     GROUP BY ew.status`,
    params
  );
  const summaryModuleRes = await query(
    `SELECT ew.module_code, COUNT(*) AS total
     FROM exception_workbench ew
     WHERE ${whereSql}
     GROUP BY ew.module_code`,
    params
  );
  const summarySeverityRes = await query(
    `SELECT ew.severity, COUNT(*) AS total
     FROM exception_workbench ew
     WHERE ${whereSql}
     GROUP BY ew.severity`,
    params
  );

  const rows = (listRes.rows || []).map(hydrateWorkbenchRow);
  return buildOffsetPaginationResult({
    rows,
    total,
    limit: pagination.limit,
    offset: pagination.offset,
    extra: {
      summary: {
        by_status: Object.fromEntries((summaryStatusRes.rows || []).map((row) => [row.status, toInt(row.total, 0)])),
        by_module: Object.fromEntries((summaryModuleRes.rows || []).map((row) => [row.module_code, toInt(row.total, 0)])),
        by_severity: Object.fromEntries((summarySeverityRes.rows || []).map((row) => [row.severity, toInt(row.total, 0)])),
      },
      refreshed: shouldRefresh,
      refresh: refreshSummary,
    },
  });
}

export async function getExceptionWorkbenchById({
  req,
  tenantId,
  exceptionId,
  assertScopeAccess,
}) {
  const row = await getExceptionWorkbenchRow({ tenantId, exceptionId });
  if (!row) throw notFound("Exception not found");
  const legalEntityId = parsePositiveInt(row.legal_entity_id);
  if (legalEntityId) {
    assertScopeAccess(req, "legal_entity", legalEntityId, "exceptionId");
  }

  const auditRes = await query(
    `SELECT *
     FROM exception_workbench_audit
     WHERE tenant_id = ?
       AND exception_workbench_id = ?
     ORDER BY id DESC`,
    [tenantId, exceptionId]
  );

  return {
    row,
    audit: (auditRes.rows || []).map((item) => ({
      ...item,
      payload_json: parseJson(item.payload_json, null),
    })),
  };
}

export async function claimExceptionWorkbench({
  req,
  tenantId,
  exceptionId,
  actorUserId,
  ownerUserId = null,
  note = null,
  assertScopeAccess,
}) {
  return withTransaction(async (tx) => {
    const current = await getExceptionWorkbenchRow({
      tenantId,
      exceptionId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!current) throw notFound("Exception not found");
    const legalEntityId = parsePositiveInt(current.legal_entity_id);
    if (legalEntityId) {
      assertScopeAccess(req, "legal_entity", legalEntityId, "exceptionId");
    }
    if (["RESOLVED", "IGNORED"].includes(u(current.status))) {
      throw conflict("Resolved/ignored exceptions cannot be claimed");
    }

    const nextOwner = parsePositiveInt(ownerUserId) || parsePositiveInt(actorUserId) || null;
    const nextStatus = "IN_REVIEW";
    await tx.query(
      `UPDATE exception_workbench
       SET owner_user_id = ?,
           status = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [nextOwner, nextStatus, tenantId, exceptionId]
    );

    await insertExceptionWorkbenchAudit({
      runQuery: tx.query,
      tenantId,
      exceptionId,
      eventType: "CLAIM",
      fromStatus: current.status,
      toStatus: nextStatus,
      payload: { note: note || null, owner_user_id: nextOwner },
      actedByUserId: actorUserId,
    });

    return getExceptionWorkbenchRow({ tenantId, exceptionId, runQuery: tx.query });
  });
}

async function applyStatusTransition({
  req,
  tenantId,
  exceptionId,
  actorUserId,
  targetStatus,
  resolutionAction = null,
  resolutionNote = null,
  assertScopeAccess,
}) {
  return withTransaction(async (tx) => {
    const current = await getExceptionWorkbenchRow({
      tenantId,
      exceptionId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!current) throw notFound("Exception not found");
    const legalEntityId = parsePositiveInt(current.legal_entity_id);
    if (legalEntityId) {
      assertScopeAccess(req, "legal_entity", legalEntityId, "exceptionId");
    }

    const nextStatus = u(targetStatus);
    if (!["OPEN", "IN_REVIEW", "RESOLVED", "IGNORED"].includes(nextStatus)) {
      throw badRequest("Invalid target status");
    }
    if (u(current.status) === nextStatus) {
      return current;
    }

    const nextResolvedBy =
      nextStatus === "RESOLVED" || nextStatus === "IGNORED"
        ? parsePositiveInt(actorUserId) || null
        : null;
    const nextResolvedAt = nextStatus === "RESOLVED" || nextStatus === "IGNORED" ? true : false;

    await tx.query(
      `UPDATE exception_workbench
       SET status = ?,
           resolution_action = ?,
           resolution_note = ?,
           resolved_by_user_id = ?,
           resolved_at = CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [
        nextStatus,
        resolutionAction || null,
        resolutionNote || null,
        nextResolvedBy,
        nextResolvedAt ? 1 : 0,
        tenantId,
        exceptionId,
      ]
    );

    await insertExceptionWorkbenchAudit({
      runQuery: tx.query,
      tenantId,
      exceptionId,
      eventType: nextStatus === "OPEN" ? "REOPEN" : nextStatus,
      fromStatus: current.status,
      toStatus: nextStatus,
      payload: {
        resolution_action: resolutionAction || null,
        resolution_note: resolutionNote || null,
      },
      actedByUserId: actorUserId,
    });

    return getExceptionWorkbenchRow({ tenantId, exceptionId, runQuery: tx.query });
  });
}

export async function resolveExceptionWorkbench({
  req,
  tenantId,
  exceptionId,
  actorUserId,
  resolutionAction = null,
  resolutionNote = null,
  assertScopeAccess,
}) {
  return applyStatusTransition({
    req,
    tenantId,
    exceptionId,
    actorUserId,
    targetStatus: "RESOLVED",
    resolutionAction,
    resolutionNote,
    assertScopeAccess,
  });
}

export async function ignoreExceptionWorkbench({
  req,
  tenantId,
  exceptionId,
  actorUserId,
  resolutionAction = null,
  resolutionNote = null,
  assertScopeAccess,
}) {
  return applyStatusTransition({
    req,
    tenantId,
    exceptionId,
    actorUserId,
    targetStatus: "IGNORED",
    resolutionAction,
    resolutionNote,
    assertScopeAccess,
  });
}

export async function reopenExceptionWorkbench({
  req,
  tenantId,
  exceptionId,
  actorUserId,
  resolutionNote = null,
  assertScopeAccess,
}) {
  return applyStatusTransition({
    req,
    tenantId,
    exceptionId,
    actorUserId,
    targetStatus: "OPEN",
    resolutionAction: "REOPEN",
    resolutionNote,
    assertScopeAccess,
  });
}

function normalizeBulkAction(action) {
  const normalized = u(action);
  if (["CLAIM", "RESOLVE", "IGNORE", "REOPEN"].includes(normalized)) {
    return normalized;
  }
  throw badRequest("action must be one of: claim, resolve, ignore, reopen");
}

function toBulkActionErrorResult(exceptionId, error) {
  return {
    exception_id: parsePositiveInt(exceptionId) || null,
    ok: false,
    status: toInt(error?.status, null),
    code: error?.code ? String(error.code) : null,
    message: String(error?.message || "Action failed"),
  };
}

export async function bulkActionExceptionWorkbench({
  req,
  tenantId,
  action,
  exceptionIds = [],
  actorUserId,
  ownerUserId = null,
  note = null,
  resolutionAction = null,
  resolutionNote = null,
  continueOnError = true,
  assertScopeAccess,
}) {
  const normalizedAction = normalizeBulkAction(action);
  const uniqueIds = Array.from(
    new Set((Array.isArray(exceptionIds) ? exceptionIds : []).map((value) => parsePositiveInt(value)).filter(Boolean))
  );

  if (uniqueIds.length === 0) {
    throw badRequest("exceptionIds must include at least one positive integer id");
  }
  if (uniqueIds.length > 200) {
    throw badRequest("exceptionIds supports at most 200 ids per request");
  }

  const rows = [];
  const results = [];
  let stoppedOnError = false;

  for (const exceptionId of uniqueIds) {
    try {
      let row = null;
      if (normalizedAction === "CLAIM") {
        row = await claimExceptionWorkbench({
          req,
          tenantId,
          exceptionId,
          actorUserId,
          ownerUserId,
          note,
          assertScopeAccess,
        });
      } else if (normalizedAction === "RESOLVE") {
        row = await resolveExceptionWorkbench({
          req,
          tenantId,
          exceptionId,
          actorUserId,
          resolutionAction: resolutionAction || "MANUAL_RESOLVE",
          resolutionNote,
          assertScopeAccess,
        });
      } else if (normalizedAction === "IGNORE") {
        row = await ignoreExceptionWorkbench({
          req,
          tenantId,
          exceptionId,
          actorUserId,
          resolutionAction: resolutionAction || "MANUAL_IGNORE",
          resolutionNote,
          assertScopeAccess,
        });
      } else {
        row = await reopenExceptionWorkbench({
          req,
          tenantId,
          exceptionId,
          actorUserId,
          resolutionNote,
          assertScopeAccess,
        });
      }

      rows.push(row);
      results.push({
        exception_id: exceptionId,
        ok: true,
        row,
      });
    } catch (error) {
      results.push(toBulkActionErrorResult(exceptionId, error));
      if (continueOnError === false) {
        stoppedOnError = true;
        break;
      }
    }
  }

  const failed = results.filter((item) => !item.ok).length;
  return {
    action: normalizedAction,
    requested: uniqueIds.length,
    succeeded: rows.length,
    failed,
    stopped_on_error: stoppedOnError,
    rows,
    results,
  };
}

export default {
  resolveExceptionWorkbenchScope,
  refreshExceptionWorkbench,
  listExceptionWorkbenchRows,
  getExceptionWorkbenchById,
  claimExceptionWorkbench,
  resolveExceptionWorkbench,
  ignoreExceptionWorkbench,
  reopenExceptionWorkbench,
  bulkActionExceptionWorkbench,
};
