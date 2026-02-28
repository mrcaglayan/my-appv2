import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { evaluateApprovalNeed, submitApprovalRequest } from "./approvalPolicies.service.js";

const CLOSE_STATUS_VALUES = new Set(["DRAFT", "READY", "REQUESTED", "CLOSED", "REOPENED"]);

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function toAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Number(parsed.toFixed(6));
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function parseOptionalJson(value) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function toDateOnly(value) {
  if (!value) return null;
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    const year = value.getFullYear();
    const month = String(value.getMonth() + 1).padStart(2, "0");
    const day = String(value.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }
  const text = String(value);
  if (/^\d{4}-\d{2}-\d{2}/.test(text)) return text.slice(0, 10);
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  const year = parsed.getFullYear();
  const month = String(parsed.getMonth() + 1).padStart(2, "0");
  const day = String(parsed.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function makeNotFound(message) {
  const err = new Error(message);
  err.status = 404;
  return err;
}

function makeConflict(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}

function noopScopeAccess() {
  return true;
}

async function getPeriodCloseScopeRow({ tenantId, closeId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, tenant_id, legal_entity_id
     FROM payroll_period_closes
     WHERE tenant_id = ? AND id = ?
     LIMIT 1`,
    [tenantId, closeId]
  );
  return result.rows?.[0] || null;
}

async function getPeriodCloseById({ tenantId, closeId, runQuery = query, forUpdate = false }) {
  const result = await runQuery(
    `SELECT *
     FROM payroll_period_closes
     WHERE tenant_id = ? AND id = ?
     LIMIT 1${forUpdate ? " FOR UPDATE" : ""}`,
    [tenantId, closeId]
  );
  return result.rows?.[0] || null;
}

function mapCloseRow(row) {
  if (!row) return null;
  return {
    ...row,
    status: normalizeUpperText(row.status),
    period_start: toDateOnly(row.period_start),
    period_end: toDateOnly(row.period_end),
  };
}

function mapCheckRow(row) {
  if (!row) return null;
  return {
    ...row,
    severity: normalizeUpperText(row.severity),
    status: normalizeUpperText(row.status),
    metric_value: row.metric_value == null ? null : toAmount(row.metric_value),
    details_json: parseOptionalJson(row.details_json),
  };
}

function mapAuditRow(row) {
  if (!row) return null;
  return {
    ...row,
    action: normalizeUpperText(row.action),
    action_status: normalizeUpperText(row.action_status),
    payload_json: parseOptionalJson(row.payload_json),
  };
}

async function writeCloseAudit({
  tenantId,
  legalEntityId,
  closeId,
  action,
  note = null,
  payload = null,
  userId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO payroll_period_close_audit (
        tenant_id, legal_entity_id, payroll_period_close_id,
        action, action_status, note, payload_json, acted_by_user_id
      )
      VALUES (?, ?, ?, ?, 'CONFIRMED', ?, ?, ?)`,
    [tenantId, legalEntityId, closeId, normalizeUpperText(action), note || null, safeJson(payload), userId]
  );
}

async function getOrCreatePeriodCloseForUpdate({
  tenantId,
  legalEntityId,
  periodStart,
  periodEnd,
  runQuery,
}) {
  const existing = await runQuery(
    `SELECT *
     FROM payroll_period_closes
     WHERE tenant_id = ? AND legal_entity_id = ? AND period_start = ? AND period_end = ?
     LIMIT 1
     FOR UPDATE`,
    [tenantId, legalEntityId, periodStart, periodEnd]
  );
  if (existing.rows?.[0]) return existing.rows[0];

  await runQuery(
    `INSERT INTO payroll_period_closes (
        tenant_id, legal_entity_id, period_start, period_end, status
      )
      VALUES (?, ?, ?, ?, 'DRAFT')`,
    [tenantId, legalEntityId, periodStart, periodEnd]
  );

  const created = await runQuery(
    `SELECT *
     FROM payroll_period_closes
     WHERE tenant_id = ? AND legal_entity_id = ? AND period_start = ? AND period_end = ?
     LIMIT 1
     FOR UPDATE`,
    [tenantId, legalEntityId, periodStart, periodEnd]
  );
  return created.rows?.[0] || null;
}

function summarizeChecks(checks = []) {
  let totalChecks = 0;
  let passedChecks = 0;
  let failedChecks = 0;
  let warningChecks = 0;

  for (const check of checks) {
    totalChecks += 1;
    const severity = normalizeUpperText(check.severity);
    const status = normalizeUpperText(check.status);
    if (status === "PASS") {
      passedChecks += 1;
    }
    if (severity === "ERROR" && status === "FAIL") {
      failedChecks += 1;
    }
    if (status === "WARN" || (severity === "WARN" && status === "FAIL")) {
      warningChecks += 1;
    }
  }

  return {
    totalChecks,
    passedChecks,
    failedChecks,
    warningChecks,
  };
}

async function computeChecklist({
  tenantId,
  legalEntityId,
  periodStart,
  periodEnd,
  runQuery = query,
}) {
  const runsStats = await runQuery(
    `SELECT
        COUNT(*) AS run_count,
        COALESCE(SUM(CASE WHEN status IN ('DRAFT','IMPORTED','REVIEWED') THEN 1 ELSE 0 END), 0)
          AS non_finalized_runs,
        COALESCE(SUM(CASE WHEN status = 'FINALIZED' AND accrual_journal_entry_id IS NULL THEN 1 ELSE 0 END), 0)
          AS finalized_missing_accrual_journal
     FROM payroll_runs
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND payroll_period BETWEEN ? AND ?`,
    [tenantId, legalEntityId, periodStart, periodEnd]
  );
  const runStats = runsStats.rows?.[0] || {};

  const correctionDraftStats = await runQuery(
    `SELECT
        COALESCE(SUM(CASE WHEN run_type = 'RETRO' AND status = 'DRAFT' THEN 1 ELSE 0 END), 0)
          AS retro_draft_count,
        COALESCE(SUM(CASE WHEN run_type = 'OFF_CYCLE' AND status = 'DRAFT' THEN 1 ELSE 0 END), 0)
          AS off_cycle_draft_count
     FROM payroll_runs
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND payroll_period BETWEEN ? AND ?`,
    [tenantId, legalEntityId, periodStart, periodEnd]
  );
  const corrStats = correctionDraftStats.rows?.[0] || {};

  const overrideStatsResult = await runQuery(
    `SELECT
        COUNT(*) AS pending_override_count
     FROM payroll_liability_override_requests r
     JOIN payroll_runs pr
       ON pr.tenant_id = r.tenant_id
      AND pr.legal_entity_id = r.legal_entity_id
      AND pr.id = r.run_id
     WHERE r.tenant_id = ?
       AND r.legal_entity_id = ?
       AND r.status = 'REQUESTED'
       AND pr.payroll_period BETWEEN ? AND ?`,
    [tenantId, legalEntityId, periodStart, periodEnd]
  );
  const overrideStats = overrideStatsResult.rows?.[0] || {};

  const beneficiaryStatsResult = await runQuery(
    `SELECT
        COUNT(*) AS employee_liabilities_in_payment_flow,
        COALESCE(SUM(
          CASE
            WHEN latest_pl.id IS NULL THEN 1
            WHEN latest_pl.beneficiary_snapshot_status = 'NOT_REQUIRED' THEN 0
            WHEN latest_pl.beneficiary_bank_snapshot_id IS NULL THEN 1
            ELSE 0
          END
        ), 0) AS missing_beneficiary_snapshot_count
     FROM payroll_run_liabilities l
     JOIN payroll_runs pr
       ON pr.tenant_id = l.tenant_id
      AND pr.legal_entity_id = l.legal_entity_id
      AND pr.id = l.run_id
     LEFT JOIN payroll_liability_payment_links latest_pl
       ON latest_pl.tenant_id = l.tenant_id
      AND latest_pl.legal_entity_id = l.legal_entity_id
      AND latest_pl.run_id = l.run_id
      AND latest_pl.payroll_liability_id = l.id
      AND latest_pl.id = (
        SELECT pl2.id
        FROM payroll_liability_payment_links pl2
        WHERE pl2.tenant_id = l.tenant_id
          AND pl2.legal_entity_id = l.legal_entity_id
          AND pl2.run_id = l.run_id
          AND pl2.payroll_liability_id = l.id
        ORDER BY pl2.id DESC
        LIMIT 1
      )
     WHERE l.tenant_id = ?
       AND l.legal_entity_id = ?
       AND pr.payroll_period BETWEEN ? AND ?
       AND UPPER(COALESCE(l.beneficiary_type, '')) = 'EMPLOYEE'
       AND UPPER(COALESCE(l.status, '')) IN ('IN_BATCH','PARTIALLY_PAID','PAID')`,
    [tenantId, legalEntityId, periodStart, periodEnd]
  );
  const beneficiaryStats = beneficiaryStatsResult.rows?.[0] || {};

  const checks = [
    {
      check_code: "RUNS_NO_NON_FINALIZED",
      check_name: "No non-finalized payroll runs in period",
      severity: "ERROR",
      status: Number(runStats.non_finalized_runs || 0) === 0 ? "PASS" : "FAIL",
      metric_value: Number(runStats.non_finalized_runs || 0),
      metric_text: `${Number(runStats.non_finalized_runs || 0)} runs in DRAFT/IMPORTED/REVIEWED`,
      details_json: null,
      sort_order: 10,
    },
    {
      check_code: "RUNS_ACCRUAL_POSTED",
      check_name: "Finalized payroll runs have accrual journal posted",
      severity: "ERROR",
      status: Number(runStats.finalized_missing_accrual_journal || 0) === 0 ? "PASS" : "FAIL",
      metric_value: Number(runStats.finalized_missing_accrual_journal || 0),
      metric_text: `${Number(runStats.finalized_missing_accrual_journal || 0)} finalized runs missing accrual journal`,
      details_json: null,
      sort_order: 20,
    },
    {
      check_code: "RETRO_DRAFTS_NONE",
      check_name: "No open RETRO correction shells in period",
      severity: "ERROR",
      status: Number(corrStats.retro_draft_count || 0) === 0 ? "PASS" : "FAIL",
      metric_value: Number(corrStats.retro_draft_count || 0),
      metric_text: `${Number(corrStats.retro_draft_count || 0)} RETRO draft shells`,
      details_json: null,
      sort_order: 30,
    },
    {
      check_code: "OFF_CYCLE_DRAFTS_WARN",
      check_name: "Open OFF_CYCLE draft shells (warning)",
      severity: "WARN",
      status: Number(corrStats.off_cycle_draft_count || 0) === 0 ? "PASS" : "WARN",
      metric_value: Number(corrStats.off_cycle_draft_count || 0),
      metric_text: `${Number(corrStats.off_cycle_draft_count || 0)} OFF_CYCLE draft shells`,
      details_json: null,
      sort_order: 35,
    },
    {
      check_code: "MANUAL_OVERRIDE_REQUESTS_NONE",
      check_name: "No pending manual settlement override requests",
      severity: "ERROR",
      status: Number(overrideStats.pending_override_count || 0) === 0 ? "PASS" : "FAIL",
      metric_value: Number(overrideStats.pending_override_count || 0),
      metric_text: `${Number(overrideStats.pending_override_count || 0)} pending override requests`,
      details_json: null,
      sort_order: 40,
    },
    {
      check_code: "BENEFICIARY_SNAPSHOTS_READY",
      check_name: "Employee liabilities in payment flow have beneficiary snapshots",
      severity: "ERROR",
      status: Number(beneficiaryStats.missing_beneficiary_snapshot_count || 0) === 0 ? "PASS" : "FAIL",
      metric_value: Number(beneficiaryStats.missing_beneficiary_snapshot_count || 0),
      metric_text: `${Number(beneficiaryStats.missing_beneficiary_snapshot_count || 0)} missing snapshots`,
      details_json: {
        employee_liabilities_in_payment_flow: Number(
          beneficiaryStats.employee_liabilities_in_payment_flow || 0
        ),
      },
      sort_order: 50,
    },
    {
      check_code: "RUN_COUNT_INFO",
      check_name: "Payroll runs in period",
      severity: "INFO",
      status: "PASS",
      metric_value: Number(runStats.run_count || 0),
      metric_text: `${Number(runStats.run_count || 0)} runs`,
      details_json: null,
      sort_order: 100,
    },
  ];

  return checks;
}

async function upsertChecklistRows({
  tenantId,
  legalEntityId,
  closeId,
  checks,
  runQuery = query,
}) {
  for (const check of checks || []) {
    // eslint-disable-next-line no-await-in-loop
    await runQuery(
      `INSERT INTO payroll_period_close_checks (
          tenant_id, legal_entity_id, payroll_period_close_id,
          check_code, check_name, severity, status,
          metric_value, metric_text, details_json, sort_order
       )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         check_name = VALUES(check_name),
         severity = VALUES(severity),
         status = VALUES(status),
         metric_value = VALUES(metric_value),
         metric_text = VALUES(metric_text),
         details_json = VALUES(details_json),
         sort_order = VALUES(sort_order),
         updated_at = CURRENT_TIMESTAMP`,
      [
        tenantId,
        legalEntityId,
        closeId,
        check.check_code,
        check.check_name,
        normalizeUpperText(check.severity),
        normalizeUpperText(check.status),
        check.metric_value == null ? null : toAmount(check.metric_value),
        check.metric_text || null,
        safeJson(check.details_json ?? null),
        Number(check.sort_order || 100),
      ]
    );
  }
}

async function listPeriodCloseChecks({ tenantId, closeId, runQuery = query }) {
  const result = await runQuery(
    `SELECT *
     FROM payroll_period_close_checks
     WHERE tenant_id = ? AND payroll_period_close_id = ?
     ORDER BY sort_order ASC, id ASC`,
    [tenantId, closeId]
  );
  return (result.rows || []).map(mapCheckRow);
}

async function listPeriodCloseAudit({ tenantId, closeId, runQuery = query }) {
  const result = await runQuery(
    `SELECT *
     FROM payroll_period_close_audit
     WHERE tenant_id = ? AND payroll_period_close_id = ?
     ORDER BY id DESC`,
    [tenantId, closeId]
  );
  return (result.rows || []).map(mapAuditRow);
}

function assertNoErrorCheckFailures(checks = []) {
  const failing = (checks || []).filter(
    (check) =>
      normalizeUpperText(check?.severity) === "ERROR" &&
      normalizeUpperText(check?.status) === "FAIL"
  );
  if (failing.length > 0) {
    throw makeConflict("Payroll period close request blocked: checklist has failing ERROR checks");
  }
}

export async function resolvePayrollPeriodCloseScope(closeId, tenantId) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedCloseId = parsePositiveInt(closeId);
  if (!parsedTenantId || !parsedCloseId) return null;
  const row = await getPeriodCloseScopeRow({ tenantId: parsedTenantId, closeId: parsedCloseId });
  if (!row) return null;
  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: parsePositiveInt(row.legal_entity_id),
  };
}

export async function listPayrollPeriodCloseRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["pc.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "pc.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("pc.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }
  if (filters.status && CLOSE_STATUS_VALUES.has(filters.status)) {
    conditions.push("pc.status = ?");
    params.push(filters.status);
  }
  if (filters.periodStart) {
    conditions.push("pc.period_end >= ?");
    params.push(filters.periodStart);
  }
  if (filters.periodEnd) {
    conditions.push("pc.period_start <= ?");
    params.push(filters.periodEnd);
  }

  const whereSql = conditions.join(" AND ");
  const countResult = await query(
    `SELECT COUNT(*) AS total FROM payroll_period_closes pc WHERE ${whereSql}`,
    params
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset = Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const listResult = await query(
    `SELECT
        pc.*,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM payroll_period_closes pc
     JOIN legal_entities le
       ON le.id = pc.legal_entity_id
      AND le.tenant_id = pc.tenant_id
     WHERE ${whereSql}
     ORDER BY pc.period_start DESC, pc.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: (listResult.rows || []).map((row) => mapCloseRow(row)),
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getPayrollPeriodCloseDetail({
  req,
  tenantId,
  closeId,
  assertScopeAccess,
}) {
  const close = await getPeriodCloseById({ tenantId, closeId });
  if (!close) throw makeNotFound("Payroll period close not found");
  assertScopeAccess(req, "legal_entity", parsePositiveInt(close.legal_entity_id), "closeId");
  const [checks, audit] = await Promise.all([
    listPeriodCloseChecks({ tenantId, closeId }),
    listPeriodCloseAudit({ tenantId, closeId }),
  ]);
  return {
    close: mapCloseRow(close),
    checks,
    audit,
  };
}

export async function preparePayrollPeriodClose({
  req,
  tenantId,
  userId,
  input,
  assertScopeAccess,
}) {
  assertScopeAccess(req, "legal_entity", input.legalEntityId, "legalEntityId");

  let closeId = null;
  await withTransaction(async (tx) => {
    let close = await getOrCreatePeriodCloseForUpdate({
      tenantId,
      legalEntityId: input.legalEntityId,
      periodStart: input.periodStart,
      periodEnd: input.periodEnd,
      runQuery: tx.query,
    });
    if (!close) throw new Error("Failed to load/create payroll period close");

    const currentStatus = normalizeUpperText(close.status);
    if (currentStatus === "CLOSED") {
      throw makeConflict("Payroll period is CLOSED. Reopen before preparing checklist again.");
    }

    const checks = await computeChecklist({
      tenantId,
      legalEntityId: input.legalEntityId,
      periodStart: input.periodStart,
      periodEnd: input.periodEnd,
      runQuery: tx.query,
    });
    await upsertChecklistRows({
      tenantId,
      legalEntityId: input.legalEntityId,
      closeId: parsePositiveInt(close.id),
      checks,
      runQuery: tx.query,
    });

    const summary = summarizeChecks(checks);
    const nextStatus = summary.failedChecks === 0 ? "READY" : "DRAFT";

    await tx.query(
      `UPDATE payroll_period_closes
       SET status = ?,
           checklist_version = checklist_version + 1,
           total_checks = ?,
           passed_checks = ?,
           failed_checks = ?,
           warning_checks = ?,
           lock_run_changes = ?,
           lock_manual_settlements = ?,
           lock_payment_prep = ?,
           prepare_note = ?,
           prepared_by_user_id = ?,
           prepared_at = CURRENT_TIMESTAMP,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
      [
        nextStatus,
        summary.totalChecks,
        summary.passedChecks,
        summary.failedChecks,
        summary.warningChecks,
        input.lockRunChanges ? 1 : 0,
        input.lockManualSettlements ? 1 : 0,
        input.lockPaymentPrep ? 1 : 0,
        input.note || null,
        userId,
        tenantId,
        input.legalEntityId,
        parsePositiveInt(close.id),
      ]
    );

    closeId = parsePositiveInt(close.id);
    await writeCloseAudit({
      tenantId,
      legalEntityId: input.legalEntityId,
      closeId,
      action: "PREPARED",
      note: input.note || null,
      payload: {
        period_start: input.periodStart,
        period_end: input.periodEnd,
        status: nextStatus,
        summary,
        lock_flags: {
          lock_run_changes: Boolean(input.lockRunChanges),
          lock_manual_settlements: Boolean(input.lockManualSettlements),
          lock_payment_prep: Boolean(input.lockPaymentPrep),
        },
      },
      userId,
      runQuery: tx.query,
    });
  });

  return getPayrollPeriodCloseDetail({ req, tenantId, closeId, assertScopeAccess });
}

export async function requestPayrollPeriodClose({
  req,
  tenantId,
  userId,
  closeId,
  note = null,
  requestIdempotencyKey = null,
  assertScopeAccess,
}) {
  await withTransaction(async (tx) => {
    const close = await getPeriodCloseById({ tenantId, closeId, runQuery: tx.query, forUpdate: true });
    if (!close) throw makeNotFound("Payroll period close not found");
    const legalEntityId = parsePositiveInt(close.legal_entity_id);
    assertScopeAccess(req, "legal_entity", legalEntityId, "closeId");

    const currentStatus = normalizeUpperText(close.status);
    if (
      requestIdempotencyKey &&
      close.request_idempotency_key &&
      String(close.request_idempotency_key) === String(requestIdempotencyKey) &&
      ["REQUESTED", "CLOSED"].includes(currentStatus)
    ) {
      return;
    }

    if (!["READY", "REQUESTED"].includes(currentStatus)) {
      throw makeConflict(`Payroll period close must be READY before request-close (current: ${currentStatus})`);
    }

    const checks = await listPeriodCloseChecks({ tenantId, closeId, runQuery: tx.query });
    assertNoErrorCheckFailures(checks);

    await tx.query(
      `UPDATE payroll_period_closes
       SET status = 'REQUESTED',
           request_note = ?,
           request_idempotency_key = COALESCE(?, request_idempotency_key),
           requested_by_user_id = ?,
           requested_at = CURRENT_TIMESTAMP,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
      [note || null, requestIdempotencyKey || null, userId, tenantId, legalEntityId, closeId]
    );

    await writeCloseAudit({
      tenantId,
      legalEntityId,
      closeId,
      action: "REQUESTED",
      note: note || null,
      payload: {
        request_idempotency_key: requestIdempotencyKey || null,
      },
      userId,
      runQuery: tx.query,
    });
  });

  return getPayrollPeriodCloseDetail({ req, tenantId, closeId, assertScopeAccess });
}

export async function approveAndClosePayrollPeriod({
  req,
  tenantId,
  userId,
  closeId,
  note = null,
  closeIdempotencyKey = null,
  assertScopeAccess,
  skipUnifiedApprovalGate = false,
  approvalRequestId = null,
}) {
  if (!skipUnifiedApprovalGate) {
    const previewClose = await getPeriodCloseById({ tenantId, closeId });
    if (!previewClose) throw makeNotFound("Payroll period close not found");
    const legalEntityId = parsePositiveInt(previewClose.legal_entity_id);
    assertScopeAccess(req, "legal_entity", legalEntityId, "closeId");

    const currentStatus = normalizeUpperText(previewClose.status);
    if (currentStatus === "REQUESTED") {
      const gov = await evaluateApprovalNeed({
        moduleCode: "PAYROLL",
        tenantId,
        targetType: "PAYROLL_PERIOD_CLOSE",
        actionType: "APPROVE_CLOSE",
        legalEntityId,
      });
      if (gov?.approval_required || gov?.approvalRequired) {
        const submitRes = await submitApprovalRequest({
          tenantId,
          userId,
          requestInput: {
            moduleCode: "PAYROLL",
            requestKey: `PRP08:APPROVE_CLOSE:${tenantId}:${closeId}`,
            targetType: "PAYROLL_PERIOD_CLOSE",
            targetId: closeId,
            actionType: "APPROVE_CLOSE",
            legalEntityId,
            actionPayload: {
              closeId,
              note: note || null,
              closeIdempotencyKey: closeIdempotencyKey || null,
            },
            targetSnapshot: {
              module_code: "PAYROLL",
              target_type: "PAYROLL_PERIOD_CLOSE",
              target_id: closeId,
              legal_entity_id: legalEntityId,
              period_start: toDateOnly(previewClose.period_start),
              period_end: toDateOnly(previewClose.period_end),
              status: currentStatus,
            },
          },
        });
        return {
          close: mapCloseRow(previewClose),
          approval_required: true,
          approval_request: submitRes?.item || null,
          idempotent: Boolean(submitRes?.idempotent),
        };
      }
    }
  }

  await withTransaction(async (tx) => {
    const close = await getPeriodCloseById({ tenantId, closeId, runQuery: tx.query, forUpdate: true });
    if (!close) throw makeNotFound("Payroll period close not found");
    const legalEntityId = parsePositiveInt(close.legal_entity_id);
    assertScopeAccess(req, "legal_entity", legalEntityId, "closeId");

    const currentStatus = normalizeUpperText(close.status);
    if (
      closeIdempotencyKey &&
      close.close_idempotency_key &&
      String(close.close_idempotency_key) === String(closeIdempotencyKey) &&
      currentStatus === "CLOSED"
    ) {
      return;
    }

    if (currentStatus !== "REQUESTED") {
      throw makeConflict(`Payroll period close must be REQUESTED before approve-close (current: ${currentStatus})`);
    }

    const requesterId = parsePositiveInt(close.requested_by_user_id);
    if (requesterId && requesterId === parsePositiveInt(userId)) {
      const err = new Error("Maker-checker violation: requester cannot approve-close the same payroll period");
      err.status = 403;
      throw err;
    }

    const checks = await listPeriodCloseChecks({ tenantId, closeId, runQuery: tx.query });
    assertNoErrorCheckFailures(checks);
    const summary = summarizeChecks(checks);

    await tx.query(
      `UPDATE payroll_period_closes
       SET status = 'CLOSED',
           close_note = ?,
           close_idempotency_key = COALESCE(?, close_idempotency_key),
           approved_by_user_id = ?,
           approved_at = CURRENT_TIMESTAMP,
           closed_by_user_id = ?,
           closed_at = CURRENT_TIMESTAMP,
           updated_at = CURRENT_TIMESTAMP,
           total_checks = ?,
           passed_checks = ?,
           failed_checks = ?,
           warning_checks = ?
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
      [
        note || null,
        closeIdempotencyKey || null,
        userId,
        userId,
        summary.totalChecks,
        summary.passedChecks,
        summary.failedChecks,
        summary.warningChecks,
        tenantId,
        legalEntityId,
        closeId,
      ]
    );

    await writeCloseAudit({
      tenantId,
      legalEntityId,
      closeId,
      action: "CLOSED",
      note: note || null,
      payload: {
        close_idempotency_key: closeIdempotencyKey || null,
        approval_request_id: parsePositiveInt(approvalRequestId) || null,
        lock_flags: {
          lock_run_changes: Boolean(Number(close.lock_run_changes || 0)),
          lock_manual_settlements: Boolean(Number(close.lock_manual_settlements || 0)),
          lock_payment_prep: Boolean(Number(close.lock_payment_prep || 0)),
        },
      },
      userId,
      runQuery: tx.query,
    });
  });

  return getPayrollPeriodCloseDetail({ req, tenantId, closeId, assertScopeAccess });
}

export async function reopenPayrollPeriodClose({
  req,
  tenantId,
  userId,
  closeId,
  reason,
  assertScopeAccess,
  skipUnifiedApprovalGate = false,
  approvalRequestId = null,
}) {
  if (!skipUnifiedApprovalGate) {
    const previewClose = await getPeriodCloseById({ tenantId, closeId });
    if (!previewClose) throw makeNotFound("Payroll period close not found");
    const legalEntityId = parsePositiveInt(previewClose.legal_entity_id);
    assertScopeAccess(req, "legal_entity", legalEntityId, "closeId");
    const currentStatus = normalizeUpperText(previewClose.status);
    if (currentStatus === "CLOSED") {
      const gov = await evaluateApprovalNeed({
        moduleCode: "PAYROLL",
        tenantId,
        targetType: "PAYROLL_PERIOD_CLOSE",
        actionType: "REOPEN",
        legalEntityId,
      });
      if (gov?.approval_required || gov?.approvalRequired) {
        const submitRes = await submitApprovalRequest({
          tenantId,
          userId,
          requestInput: {
            moduleCode: "PAYROLL",
            requestKey: `PRP08:REOPEN:${tenantId}:${closeId}`,
            targetType: "PAYROLL_PERIOD_CLOSE",
            targetId: closeId,
            actionType: "REOPEN",
            legalEntityId,
            actionPayload: {
              closeId,
              reason,
            },
            targetSnapshot: {
              module_code: "PAYROLL",
              target_type: "PAYROLL_PERIOD_CLOSE",
              target_id: closeId,
              legal_entity_id: legalEntityId,
              period_start: toDateOnly(previewClose.period_start),
              period_end: toDateOnly(previewClose.period_end),
              status: currentStatus,
            },
          },
        });
        return {
          close: mapCloseRow(previewClose),
          approval_required: true,
          approval_request: submitRes?.item || null,
          idempotent: Boolean(submitRes?.idempotent),
        };
      }
    }
  }

  await withTransaction(async (tx) => {
    const close = await getPeriodCloseById({ tenantId, closeId, runQuery: tx.query, forUpdate: true });
    if (!close) throw makeNotFound("Payroll period close not found");
    const legalEntityId = parsePositiveInt(close.legal_entity_id);
    assertScopeAccess(req, "legal_entity", legalEntityId, "closeId");

    const currentStatus = normalizeUpperText(close.status);
    if (currentStatus !== "CLOSED") {
      throw makeConflict(`Only CLOSED payroll periods can be reopened (current: ${currentStatus})`);
    }

    await tx.query(
      `UPDATE payroll_period_closes
       SET status = 'REOPENED',
           reopen_reason = ?,
           reopened_by_user_id = ?,
           reopened_at = CURRENT_TIMESTAMP,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
      [reason, userId, tenantId, legalEntityId, closeId]
    );

    await writeCloseAudit({
      tenantId,
      legalEntityId,
      closeId,
      action: "REOPENED",
      note: reason,
      payload: { reason, approval_request_id: parsePositiveInt(approvalRequestId) || null },
      userId,
      runQuery: tx.query,
    });
  });

  return getPayrollPeriodCloseDetail({ req, tenantId, closeId, assertScopeAccess });
}

export async function assertPayrollPeriodActionAllowed({
  tenantId,
  legalEntityId,
  payrollPeriod,
  actionType,
  runQuery = query,
}) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedLegalEntityId = parsePositiveInt(legalEntityId);
  const date = toDateOnly(payrollPeriod);
  if (!parsedTenantId || !parsedLegalEntityId || !date) {
    return { allowed: true, close: null };
  }

  const result = await runQuery(
    `SELECT *
     FROM payroll_period_closes
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND status = 'CLOSED'
       AND ? BETWEEN period_start AND period_end
     ORDER BY period_start DESC, id DESC
     LIMIT 1`,
    [parsedTenantId, parsedLegalEntityId, date]
  );
  const close = result.rows?.[0] || null;
  if (!close) {
    return { allowed: true, close: null };
  }

  const action = normalizeUpperText(actionType);
  const blocked =
    (action.startsWith("RUN_") && Number(close.lock_run_changes || 0) === 1) ||
    (action.startsWith("MANUAL_SETTLEMENT_") && Number(close.lock_manual_settlements || 0) === 1) ||
    (action.startsWith("PAYMENT_PREP_") && Number(close.lock_payment_prep || 0) === 1);

  if (blocked) {
    const err = makeConflict(`Payroll period is CLOSED and locked for action ${action}`);
    err.code = "PAYROLL_PERIOD_LOCKED";
    err.details = {
      payroll_period_close_id: parsePositiveInt(close.id),
      tenant_id: parsedTenantId,
      legal_entity_id: parsedLegalEntityId,
      period_start: toDateOnly(close.period_start),
      period_end: toDateOnly(close.period_end),
      action_type: action,
      lock_flags: {
        lock_run_changes: Boolean(Number(close.lock_run_changes || 0)),
        lock_manual_settlements: Boolean(Number(close.lock_manual_settlements || 0)),
        lock_payment_prep: Boolean(Number(close.lock_payment_prep || 0)),
      },
    };
    throw err;
  }

  return { allowed: true, close: mapCloseRow(close) };
}

export async function executeApprovedPayrollPeriodClose({
  tenantId,
  approvalRequestId,
  approvedByUserId,
  payload = {},
}) {
  const closeId = parsePositiveInt(payload?.closeId ?? payload?.close_id);
  if (!closeId) {
    throw badRequest("Approved payroll period close payload is missing closeId");
  }
  return approveAndClosePayrollPeriod({
    req: null,
    tenantId,
    userId: parsePositiveInt(approvedByUserId) || null,
    closeId,
    note: String(payload?.note || "").trim() || null,
    closeIdempotencyKey:
      String((payload?.closeIdempotencyKey ?? payload?.close_idempotency_key) || "").trim() || null,
    assertScopeAccess: noopScopeAccess,
    skipUnifiedApprovalGate: true,
    approvalRequestId,
  });
}

export async function executeApprovedPayrollPeriodReopen({
  tenantId,
  approvalRequestId,
  approvedByUserId,
  payload = {},
}) {
  const closeId = parsePositiveInt(payload?.closeId ?? payload?.close_id);
  if (!closeId) {
    throw badRequest("Approved payroll period reopen payload is missing closeId");
  }
  const reason = String(payload?.reason || "").trim();
  if (!reason) {
    throw badRequest("Approved payroll period reopen payload is missing reason");
  }
  return reopenPayrollPeriodClose({
    req: null,
    tenantId,
    userId: parsePositiveInt(approvedByUserId) || null,
    closeId,
    reason,
    assertScopeAccess: noopScopeAccess,
    skipUnifiedApprovalGate: true,
    approvalRequestId,
  });
}

export default {
  resolvePayrollPeriodCloseScope,
  listPayrollPeriodCloseRows,
  getPayrollPeriodCloseDetail,
  preparePayrollPeriodClose,
  requestPayrollPeriodClose,
  approveAndClosePayrollPeriod,
  reopenPayrollPeriodClose,
  executeApprovedPayrollPeriodClose,
  executeApprovedPayrollPeriodReopen,
  assertPayrollPeriodActionAllowed,
};
