import { query } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";

function u(value) {
  return String(value || "").trim().toUpperCase();
}

function toInt(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? Number(Math.trunc(n)) : fallback;
}

function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function parseIsoDate(value, label = "date") {
  if (!value) return null;
  const raw = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw badRequest(`${label} must be YYYY-MM-DD`);
  }
  const d = new Date(`${raw}T00:00:00.000Z`);
  if (Number.isNaN(d.getTime())) {
    throw badRequest(`${label} is invalid`);
  }
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

function normalizeWindow(filters = {}, defaultDays = 30) {
  const rawDays = Number(filters.days);
  const days =
    Number.isInteger(rawDays) && rawDays > 0
      ? Math.min(rawDays, 3660)
      : defaultDays;

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

  if (parsePositiveInt(filters.legalEntityId)) {
    const legalEntityId = parsePositiveInt(filters.legalEntityId);
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

function appendBankAccountFilter(where, params, alias, bankAccountId) {
  const parsed = parsePositiveInt(bankAccountId);
  if (!parsed) return;
  where.push(`${alias}.bank_account_id = ?`);
  params.push(parsed);
}

function appendDateRangeDateColumn(where, params, column, window) {
  where.push(`${column} >= ?`);
  params.push(window.dateFrom);
  where.push(`${column} <= ?`);
  params.push(window.dateTo);
}

function appendDateRangeTimestampColumn(where, params, column, window) {
  where.push(`${column} >= ?`);
  params.push(window.startTs);
  where.push(`${column} < ?`);
  params.push(window.endExclusiveTs);
}

function appendPeriodOverlap(where, params, startColumn, endColumn, window) {
  where.push(`${endColumn} >= ?`);
  params.push(window.dateFrom);
  where.push(`${startColumn} <= ?`);
  params.push(window.dateTo);
}

function rowsToCountMap(rows = [], keyField = "status", valueField = "total") {
  const out = {};
  for (const row of rows || []) {
    const key = String(row?.[keyField] ?? "UNKNOWN").trim() || "UNKNOWN";
    out[key] = toInt(row?.[valueField], 0);
  }
  return out;
}

function rowsToNumberMap(rows = [], keyField = "bucket", valueField = "total") {
  const out = {};
  for (const row of rows || []) {
    const key = String(row?.[keyField] ?? "UNKNOWN").trim() || "UNKNOWN";
    out[key] = toNum(row?.[valueField], 0);
  }
  return out;
}

async function querySingleRow(sql, params) {
  const res = await query(sql, params);
  return res.rows?.[0] || {};
}

export async function getOpsBankReconciliationSummary({
  req,
  tenantId,
  filters = {},
  buildScopeFilter,
  assertScopeAccess,
}) {
  const window = normalizeWindow(filters, 30);

  const lineParams = [];
  const lineWhere = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "l",
    params: lineParams,
    buildScopeFilter,
    assertScopeAccess,
  });
  appendBankAccountFilter(lineWhere, lineParams, "l", filters.bankAccountId);
  appendDateRangeDateColumn(lineWhere, lineParams, "l.txn_date", window);
  const lineWhereSql = lineWhere.join(" AND ");

  const lineStatusRes = await query(
    `SELECT l.recon_status AS status, COUNT(*) AS total
     FROM bank_statement_lines l
     WHERE ${lineWhereSql}
     GROUP BY l.recon_status`,
    lineParams
  );

  const lineSla = await querySingleRow(
    `SELECT
        COUNT(*) AS statement_lines_total,
        SUM(CASE WHEN l.recon_status IN ('UNMATCHED','PARTIAL') THEN 1 ELSE 0 END) AS unmatched_open_total,
        SUM(CASE WHEN l.recon_status IN ('UNMATCHED','PARTIAL')
                   AND DATEDIFF(CURDATE(), l.txn_date) <= 1 THEN 1 ELSE 0 END) AS unmatched_age_0_1d,
        SUM(CASE WHEN l.recon_status IN ('UNMATCHED','PARTIAL')
                   AND DATEDIFF(CURDATE(), l.txn_date) BETWEEN 2 AND 7 THEN 1 ELSE 0 END) AS unmatched_age_2_7d,
        SUM(CASE WHEN l.recon_status IN ('UNMATCHED','PARTIAL')
                   AND DATEDIFF(CURDATE(), l.txn_date) BETWEEN 8 AND 30 THEN 1 ELSE 0 END) AS unmatched_age_8_30d,
        SUM(CASE WHEN l.recon_status IN ('UNMATCHED','PARTIAL')
                   AND DATEDIFF(CURDATE(), l.txn_date) > 30 THEN 1 ELSE 0 END) AS unmatched_age_31_plus_d
     FROM bank_statement_lines l
     WHERE ${lineWhereSql}`,
    lineParams
  );

  const exParams = [];
  const exWhere = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "e",
    params: exParams,
    buildScopeFilter,
    assertScopeAccess,
  });
  appendBankAccountFilter(exWhere, exParams, "e", filters.bankAccountId);
  appendDateRangeTimestampColumn(exWhere, exParams, "e.first_seen_at", window);
  const exWhereSql = exWhere.join(" AND ");

  const exceptionStatusRes = await query(
    `SELECT e.status, COUNT(*) AS total
     FROM bank_reconciliation_exceptions e
     WHERE ${exWhereSql}
     GROUP BY e.status`,
    exParams
  );

  const exceptionSeverityRes = await query(
    `SELECT e.severity, COUNT(*) AS total
     FROM bank_reconciliation_exceptions e
     WHERE ${exWhereSql}
     GROUP BY e.severity`,
    exParams
  );

  const exceptionAging = await querySingleRow(
    `SELECT
        SUM(CASE WHEN e.status IN ('OPEN','ASSIGNED')
                   AND TIMESTAMPDIFF(HOUR, e.first_seen_at, CURRENT_TIMESTAMP) <= 24 THEN 1 ELSE 0 END) AS age_0_24h,
        SUM(CASE WHEN e.status IN ('OPEN','ASSIGNED')
                   AND TIMESTAMPDIFF(HOUR, e.first_seen_at, CURRENT_TIMESTAMP) BETWEEN 25 AND 72 THEN 1 ELSE 0 END) AS age_25_72h,
        SUM(CASE WHEN e.status IN ('OPEN','ASSIGNED')
                   AND TIMESTAMPDIFF(HOUR, e.first_seen_at, CURRENT_TIMESTAMP) BETWEEN 73 AND 168 THEN 1 ELSE 0 END) AS age_73_168h,
        SUM(CASE WHEN e.status IN ('OPEN','ASSIGNED')
                   AND TIMESTAMPDIFF(HOUR, e.first_seen_at, CURRENT_TIMESTAMP) > 168 THEN 1 ELSE 0 END) AS age_gt_168h,
        MAX(CASE WHEN e.status IN ('OPEN','ASSIGNED')
                  THEN TIMESTAMPDIFF(HOUR, e.first_seen_at, CURRENT_TIMESTAMP) ELSE NULL END) AS oldest_open_hours
     FROM bank_reconciliation_exceptions e
     WHERE ${exWhereSql}`,
    exParams
  );

  return {
    window,
    filters: {
      legalEntityId: parsePositiveInt(filters.legalEntityId) || null,
      bankAccountId: parsePositiveInt(filters.bankAccountId) || null,
    },
    statement_lines: {
      by_recon_status: rowsToCountMap(lineStatusRes.rows, "status", "total"),
      total_in_window: toInt(lineSla.statement_lines_total, 0),
    },
    exceptions: {
      by_status: rowsToCountMap(exceptionStatusRes.rows, "status", "total"),
      by_severity: rowsToCountMap(exceptionSeverityRes.rows, "severity", "total"),
      aging_open_assigned: {
        "0_24h": toInt(exceptionAging.age_0_24h, 0),
        "25_72h": toInt(exceptionAging.age_25_72h, 0),
        "73_168h": toInt(exceptionAging.age_73_168h, 0),
        gt_168h: toInt(exceptionAging.age_gt_168h, 0),
      },
      oldest_open_hours: toInt(exceptionAging.oldest_open_hours, 0),
    },
    sla: {
      unmatched_open_total: toInt(lineSla.unmatched_open_total, 0),
      unmatched_aging_buckets: {
        "0_1d": toInt(lineSla.unmatched_age_0_1d, 0),
        "2_7d": toInt(lineSla.unmatched_age_2_7d, 0),
        "8_30d": toInt(lineSla.unmatched_age_8_30d, 0),
        "31_plus_d": toInt(lineSla.unmatched_age_31_plus_d, 0),
      },
    },
  };
}

export async function getOpsBankPaymentBatchesHealth({
  req,
  tenantId,
  filters = {},
  buildScopeFilter,
  assertScopeAccess,
}) {
  const window = normalizeWindow(filters, 30);

  const batchParams = [];
  const batchWhere = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "pb",
    params: batchParams,
    buildScopeFilter,
    assertScopeAccess,
  });
  appendBankAccountFilter(batchWhere, batchParams, "pb", filters.bankAccountId);
  appendDateRangeTimestampColumn(batchWhere, batchParams, "pb.created_at", window);
  const batchWhereSql = batchWhere.join(" AND ");

  const batchStatusRes = await query(
    `SELECT pb.status, COUNT(*) AS total
     FROM payment_batches pb
     WHERE ${batchWhereSql}
     GROUP BY pb.status`,
    batchParams
  );
  const exportStatusRes = await query(
    `SELECT pb.bank_export_status AS status, COUNT(*) AS total
     FROM payment_batches pb
     WHERE ${batchWhereSql}
     GROUP BY pb.bank_export_status`,
    batchParams
  );
  const ackStatusRes = await query(
    `SELECT pb.bank_ack_status AS status, COUNT(*) AS total
     FROM payment_batches pb
     WHERE ${batchWhereSql}
     GROUP BY pb.bank_ack_status`,
    batchParams
  );

  const batchKpi = await querySingleRow(
    `SELECT
        COUNT(*) AS total_batches,
        SUM(CASE WHEN pb.bank_export_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_exports,
        SUM(CASE WHEN pb.bank_ack_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_acks,
        SUM(CASE WHEN pb.bank_export_status IN ('NOT_EXPORTED','FAILED')
                   AND pb.status IN ('APPROVED','EXPORTED','POSTED','FAILED')
                 THEN 1 ELSE 0 END) AS pending_export_batches,
        SUM(CASE WHEN pb.bank_export_status IN ('EXPORTED','SENT')
                   AND pb.bank_ack_status IN ('NOT_ACKED','PARTIAL')
                 THEN 1 ELSE 0 END) AS awaiting_ack_batches,
        SUM(CASE WHEN pb.bank_export_status IN ('EXPORTED','SENT')
                   AND pb.bank_ack_status IN ('NOT_ACKED','PARTIAL')
                   AND pb.exported_at IS NOT NULL
                   AND TIMESTAMPDIFF(HOUR, pb.exported_at, CURRENT_TIMESTAMP) > 24
                 THEN 1 ELSE 0 END) AS awaiting_ack_gt_24h
     FROM payment_batches pb
     WHERE ${batchWhereSql}`,
    batchParams
  );

  const lineParams = [];
  const lineWhere = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "pb",
    params: lineParams,
    buildScopeFilter,
    assertScopeAccess,
  });
  appendBankAccountFilter(lineWhere, lineParams, "pb", filters.bankAccountId);
  appendDateRangeTimestampColumn(lineWhere, lineParams, "pb.created_at", window);
  const lineWhereSql = lineWhere.join(" AND ");

  const lineExecStatusRes = await query(
    `SELECT pbl.bank_execution_status AS status, COUNT(*) AS total
     FROM payment_batch_lines pbl
     JOIN payment_batches pb
       ON pb.tenant_id = pbl.tenant_id
      AND pb.legal_entity_id = pbl.legal_entity_id
      AND pb.id = pbl.batch_id
     WHERE ${lineWhereSql}
     GROUP BY pbl.bank_execution_status`,
    lineParams
  );

  const ackParams = [];
  const ackWhere = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "ai",
    params: ackParams,
    buildScopeFilter,
    assertScopeAccess,
  });
  appendDateRangeTimestampColumn(ackWhere, ackParams, "ai.created_at", window);
  const ackWhereSql = ackWhere.join(" AND ");

  const ackImportStatusRes = await query(
    `SELECT ai.status, COUNT(*) AS total
     FROM payment_batch_ack_imports ai
     WHERE ${ackWhereSql}
     GROUP BY ai.status`,
    ackParams
  );

  return {
    window,
    filters: {
      legalEntityId: parsePositiveInt(filters.legalEntityId) || null,
      bankAccountId: parsePositiveInt(filters.bankAccountId) || null,
    },
    batches: {
      by_status: rowsToCountMap(batchStatusRes.rows, "status", "total"),
      by_bank_export_status: rowsToCountMap(exportStatusRes.rows, "status", "total"),
      by_bank_ack_status: rowsToCountMap(ackStatusRes.rows, "status", "total"),
      total_in_window: toInt(batchKpi.total_batches, 0),
    },
    execution_lines: {
      by_bank_execution_status: rowsToCountMap(lineExecStatusRes.rows, "status", "total"),
    },
    ack_imports: {
      by_status: rowsToCountMap(ackImportStatusRes.rows, "status", "total"),
    },
    sla: {
      failed_exports: toInt(batchKpi.failed_exports, 0),
      failed_acks: toInt(batchKpi.failed_acks, 0),
      pending_export_batches: toInt(batchKpi.pending_export_batches, 0),
      awaiting_ack_batches: toInt(batchKpi.awaiting_ack_batches, 0),
      awaiting_ack_gt_24h: toInt(batchKpi.awaiting_ack_gt_24h, 0),
    },
  };
}

export async function getOpsPayrollImportHealth({
  req,
  tenantId,
  filters = {},
  buildScopeFilter,
  assertScopeAccess,
}) {
  const window = normalizeWindow(filters, 30);

  const jobParams = [];
  const jobWhere = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "j",
    params: jobParams,
    buildScopeFilter,
    assertScopeAccess,
  });
  appendDateRangeTimestampColumn(jobWhere, jobParams, "j.requested_at", window);
  if (String(filters.providerCode || "").trim()) {
    jobWhere.push("j.provider_code = ?");
    jobParams.push(u(filters.providerCode));
  }
  const jobWhereSql = jobWhere.join(" AND ");

  const statusRes = await query(
    `SELECT j.status, COUNT(*) AS total
     FROM payroll_provider_import_jobs j
     WHERE ${jobWhereSql}
     GROUP BY j.status`,
    jobParams
  );

  const providerRes = await query(
    `SELECT j.provider_code, COUNT(*) AS total
     FROM payroll_provider_import_jobs j
     WHERE ${jobWhereSql}
     GROUP BY j.provider_code
     ORDER BY total DESC, j.provider_code ASC
     LIMIT 10`,
    jobParams
  );

  const kpi = await querySingleRow(
    `SELECT
        COUNT(*) AS total_jobs,
        SUM(CASE WHEN j.status = 'PREVIEWED' THEN 1 ELSE 0 END) AS previewed_jobs,
        SUM(CASE WHEN j.status = 'APPLYING' THEN 1 ELSE 0 END) AS applying_jobs,
        SUM(CASE WHEN j.status = 'APPLIED' THEN 1 ELSE 0 END) AS applied_jobs,
        SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_jobs,
        SUM(CASE WHEN j.status = 'REJECTED' THEN 1 ELSE 0 END) AS rejected_jobs,
        AVG(CASE WHEN j.status = 'APPLIED' AND j.applied_at IS NOT NULL
                 THEN TIMESTAMPDIFF(MINUTE, j.requested_at, j.applied_at)
                 ELSE NULL END) AS avg_apply_latency_minutes,
        MAX(CASE WHEN j.status IN ('PREVIEWED','APPLYING','FAILED')
                 THEN TIMESTAMPDIFF(HOUR, j.requested_at, CURRENT_TIMESTAMP)
                 ELSE NULL END) AS oldest_pending_or_failed_hours
     FROM payroll_provider_import_jobs j
     WHERE ${jobWhereSql}`,
    jobParams
  );

  const aging = await querySingleRow(
    `SELECT
        SUM(CASE WHEN j.status IN ('PREVIEWED','APPLYING')
                   AND TIMESTAMPDIFF(HOUR, j.requested_at, CURRENT_TIMESTAMP) <= 1 THEN 1 ELSE 0 END) AS active_age_0_1h,
        SUM(CASE WHEN j.status IN ('PREVIEWED','APPLYING')
                   AND TIMESTAMPDIFF(HOUR, j.requested_at, CURRENT_TIMESTAMP) BETWEEN 2 AND 24 THEN 1 ELSE 0 END) AS active_age_2_24h,
        SUM(CASE WHEN j.status IN ('PREVIEWED','APPLYING')
                   AND TIMESTAMPDIFF(HOUR, j.requested_at, CURRENT_TIMESTAMP) > 24 THEN 1 ELSE 0 END) AS active_age_gt_24h,
        SUM(CASE WHEN j.status = 'FAILED'
                   AND TIMESTAMPDIFF(HOUR, j.requested_at, CURRENT_TIMESTAMP) <= 24 THEN 1 ELSE 0 END) AS failed_age_0_24h,
        SUM(CASE WHEN j.status = 'FAILED'
                   AND TIMESTAMPDIFF(HOUR, j.requested_at, CURRENT_TIMESTAMP) > 24 THEN 1 ELSE 0 END) AS failed_age_gt_24h
     FROM payroll_provider_import_jobs j
     WHERE ${jobWhereSql}`,
    jobParams
  );

  const runParams = [];
  const runWhere = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "r",
    params: runParams,
    buildScopeFilter,
    assertScopeAccess,
  });
  appendDateRangeTimestampColumn(runWhere, runParams, "r.imported_at", window);
  runWhere.push("r.source_type = 'PROVIDER_IMPORT'");
  const runWhereSql = runWhere.join(" AND ");

  const runStatusRes = await query(
    `SELECT r.status, COUNT(*) AS total
     FROM payroll_runs r
     WHERE ${runWhereSql}
     GROUP BY r.status`,
    runParams
  );

  return {
    window,
    filters: {
      legalEntityId: parsePositiveInt(filters.legalEntityId) || null,
      providerCode: String(filters.providerCode || "").trim().toUpperCase() || null,
    },
    provider_import_jobs: {
      by_status: rowsToCountMap(statusRes.rows, "status", "total"),
      top_providers: rowsToNumberMap(providerRes.rows, "provider_code", "total"),
      total_in_window: toInt(kpi.total_jobs, 0),
    },
    payroll_runs_from_provider_imports: {
      by_status: rowsToCountMap(runStatusRes.rows, "status", "total"),
    },
    sla: {
      previewed_jobs: toInt(kpi.previewed_jobs, 0),
      applying_jobs: toInt(kpi.applying_jobs, 0),
      applied_jobs: toInt(kpi.applied_jobs, 0),
      failed_jobs: toInt(kpi.failed_jobs, 0),
      rejected_jobs: toInt(kpi.rejected_jobs, 0),
      avg_apply_latency_minutes: Number(
        toNum(kpi.avg_apply_latency_minutes, 0).toFixed(2)
      ),
      oldest_pending_or_failed_hours: toInt(kpi.oldest_pending_or_failed_hours, 0),
      active_aging_buckets: {
        "0_1h": toInt(aging.active_age_0_1h, 0),
        "2_24h": toInt(aging.active_age_2_24h, 0),
        gt_24h: toInt(aging.active_age_gt_24h, 0),
      },
      failed_aging_buckets: {
        "0_24h": toInt(aging.failed_age_0_24h, 0),
        gt_24h: toInt(aging.failed_age_gt_24h, 0),
      },
    },
  };
}

export async function getOpsPayrollCloseStatus({
  req,
  tenantId,
  filters = {},
  buildScopeFilter,
  assertScopeAccess,
}) {
  const window = normalizeWindow(filters, 180);

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
  appendPeriodOverlap(where, params, "pc.period_start", "pc.period_end", window);
  const whereSql = where.join(" AND ");

  const statusRes = await query(
    `SELECT pc.status, COUNT(*) AS total
     FROM payroll_period_closes pc
     WHERE ${whereSql}
     GROUP BY pc.status`,
    params
  );

  const kpi = await querySingleRow(
    `SELECT
        COUNT(*) AS total_periods,
        SUM(CASE WHEN pc.status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_periods,
        SUM(CASE WHEN pc.status <> 'CLOSED' THEN 1 ELSE 0 END) AS open_or_in_progress_periods,
        SUM(CASE WHEN pc.status IN ('DRAFT','READY','REQUESTED','REOPENED')
                 THEN pc.failed_checks ELSE 0 END) AS failed_checks_open_periods,
        SUM(CASE WHEN pc.status IN ('DRAFT','READY','REQUESTED','REOPENED')
                 THEN pc.warning_checks ELSE 0 END) AS warning_checks_open_periods,
        SUM(CASE WHEN pc.status = 'REQUESTED'
                   AND pc.requested_at IS NOT NULL
                   AND TIMESTAMPDIFF(HOUR, pc.requested_at, CURRENT_TIMESTAMP) > 24
                 THEN 1 ELSE 0 END) AS requested_gt_24h,
        MAX(CASE WHEN pc.status = 'REQUESTED' AND pc.requested_at IS NOT NULL
                 THEN TIMESTAMPDIFF(HOUR, pc.requested_at, CURRENT_TIMESTAMP) ELSE NULL END) AS oldest_requested_hours
     FROM payroll_period_closes pc
     WHERE ${whereSql}`,
    params
  );

  const checkParams = [];
  const checkWhere = buildScopedLegalEntityWhere({
    req,
    tenantId,
    filters,
    alias: "pc",
    params: checkParams,
    buildScopeFilter,
    assertScopeAccess,
  });
  appendPeriodOverlap(checkWhere, checkParams, "pc.period_start", "pc.period_end", window);
  const checkWhereSql = checkWhere.join(" AND ");

  const checklistStatusRes = await query(
    `SELECT c.status, COUNT(*) AS total
     FROM payroll_period_close_checks c
     JOIN payroll_period_closes pc
       ON pc.tenant_id = c.tenant_id
      AND pc.legal_entity_id = c.legal_entity_id
      AND pc.id = c.payroll_period_close_id
     WHERE ${checkWhereSql}
     GROUP BY c.status`,
    checkParams
  );

  return {
    window,
    filters: {
      legalEntityId: parsePositiveInt(filters.legalEntityId) || null,
    },
    periods: {
      by_status: rowsToCountMap(statusRes.rows, "status", "total"),
      total_in_window: toInt(kpi.total_periods, 0),
      closed_periods: toInt(kpi.closed_periods, 0),
      open_or_in_progress_periods: toInt(kpi.open_or_in_progress_periods, 0),
    },
    checks: {
      by_status: rowsToCountMap(checklistStatusRes.rows, "status", "total"),
      failed_checks_open_periods: toInt(kpi.failed_checks_open_periods, 0),
      warning_checks_open_periods: toInt(kpi.warning_checks_open_periods, 0),
    },
    sla: {
      requested_gt_24h: toInt(kpi.requested_gt_24h, 0),
      oldest_requested_hours: toInt(kpi.oldest_requested_hours, 0),
    },
  };
}

export async function getOpsJobsHealth({
  tenantId,
  filters = {},
}) {
  const window = normalizeWindow(filters, 30);

  const params = [tenantId];
  const where = ["j.tenant_id = ?"];
  appendDateRangeTimestampColumn(where, params, "j.created_at", window);
  if (String(filters.moduleCode || "").trim()) {
    where.push("j.module_code = ?");
    params.push(u(filters.moduleCode));
  }
  if (String(filters.jobType || "").trim()) {
    where.push("j.job_type = ?");
    params.push(u(filters.jobType));
  }
  if (String(filters.queueName || "").trim()) {
    where.push("j.queue_name = ?");
    params.push(String(filters.queueName).trim());
  }
  const whereSql = where.join(" AND ");

  const statusRes = await query(
    `SELECT j.status, COUNT(*) AS total
     FROM app_jobs j
     WHERE ${whereSql}
     GROUP BY j.status`,
    params
  );

  const moduleRes = await query(
    `SELECT j.module_code, COUNT(*) AS total
     FROM app_jobs j
     WHERE ${whereSql}
     GROUP BY j.module_code
     ORDER BY total DESC, j.module_code ASC`,
    params
  );

  const queueRes = await query(
    `SELECT j.queue_name, COUNT(*) AS total
     FROM app_jobs j
     WHERE ${whereSql}
     GROUP BY j.queue_name
     ORDER BY total DESC, j.queue_name ASC
     LIMIT 10`,
    params
  );

  const kpi = await querySingleRow(
    `SELECT
        COUNT(*) AS total_jobs,
        SUM(CASE WHEN j.status = 'FAILED_FINAL' THEN 1 ELSE 0 END) AS failed_final_jobs,
        SUM(CASE WHEN j.status = 'FAILED_RETRYABLE' THEN 1 ELSE 0 END) AS failed_retryable_jobs,
        SUM(CASE WHEN j.status = 'QUEUED' AND j.run_after_at <= CURRENT_TIMESTAMP THEN 1 ELSE 0 END) AS queued_due_now,
        SUM(CASE WHEN j.status = 'FAILED_RETRYABLE' AND j.run_after_at <= CURRENT_TIMESTAMP THEN 1 ELSE 0 END) AS retries_due_now,
        SUM(CASE WHEN j.status IN ('QUEUED','FAILED_RETRYABLE') AND j.run_after_at > CURRENT_TIMESTAMP THEN 1 ELSE 0 END) AS delayed_not_due,
        SUM(CASE WHEN j.status = 'RUNNING' THEN 1 ELSE 0 END) AS running_jobs,
        SUM(CASE WHEN j.status = 'RUNNING'
                   AND COALESCE(j.started_at, j.locked_at, j.updated_at) < DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 15 MINUTE)
                 THEN 1 ELSE 0 END) AS running_gt_15m,
        SUM(CASE WHEN j.status = 'RUNNING'
                   AND COALESCE(j.started_at, j.locked_at, j.updated_at) < DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 60 MINUTE)
                 THEN 1 ELSE 0 END) AS running_gt_60m,
        MAX(CASE WHEN j.status IN ('QUEUED','FAILED_RETRYABLE') AND j.run_after_at <= CURRENT_TIMESTAMP
                 THEN TIMESTAMPDIFF(MINUTE, j.run_after_at, CURRENT_TIMESTAMP) ELSE NULL END) AS oldest_due_minutes
     FROM app_jobs j
     WHERE ${whereSql}`,
    params
  );

  const attemptsRes = await querySingleRow(
    `SELECT
        COUNT(*) AS attempts_total,
        SUM(CASE WHEN a.status = 'RUNNING' THEN 1 ELSE 0 END) AS running_attempts,
        SUM(CASE WHEN a.status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_attempts,
        SUM(CASE WHEN a.status LIKE 'FAILED%' THEN 1 ELSE 0 END) AS failed_attempts
     FROM app_job_attempts a
     WHERE a.tenant_id = ?
       AND a.started_at >= ?
       AND a.started_at < ?`,
    [tenantId, window.startTs, window.endExclusiveTs]
  );

  return {
    window,
    filters: {
      moduleCode: String(filters.moduleCode || "").trim().toUpperCase() || null,
      jobType: String(filters.jobType || "").trim().toUpperCase() || null,
      queueName: String(filters.queueName || "").trim() || null,
    },
    jobs: {
      by_status: rowsToCountMap(statusRes.rows, "status", "total"),
      by_module: rowsToNumberMap(moduleRes.rows, "module_code", "total"),
      top_queues: rowsToNumberMap(queueRes.rows, "queue_name", "total"),
      total_in_window: toInt(kpi.total_jobs, 0),
    },
    attempts: {
      total_in_window: toInt(attemptsRes.attempts_total, 0),
      running: toInt(attemptsRes.running_attempts, 0),
      succeeded: toInt(attemptsRes.succeeded_attempts, 0),
      failed: toInt(attemptsRes.failed_attempts, 0),
    },
    sla: {
      failed_final_jobs: toInt(kpi.failed_final_jobs, 0),
      failed_retryable_jobs: toInt(kpi.failed_retryable_jobs, 0),
      queued_due_now: toInt(kpi.queued_due_now, 0),
      retries_due_now: toInt(kpi.retries_due_now, 0),
      delayed_not_due: toInt(kpi.delayed_not_due, 0),
      running_jobs: toInt(kpi.running_jobs, 0),
      running_gt_15m: toInt(kpi.running_gt_15m, 0),
      running_gt_60m: toInt(kpi.running_gt_60m, 0),
      oldest_due_minutes: toInt(kpi.oldest_due_minutes, 0),
    },
  };
}

export default {
  getOpsBankReconciliationSummary,
  getOpsBankPaymentBatchesHealth,
  getOpsPayrollImportHealth,
  getOpsPayrollCloseStatus,
  getOpsJobsHealth,
};
