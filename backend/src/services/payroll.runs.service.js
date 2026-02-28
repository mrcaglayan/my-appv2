import crypto from "node:crypto";
import { query, withTransaction } from "../db.js";
import { assertCurrencyExists, assertLegalEntityBelongsToTenant } from "../tenantGuards.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { parsePayrollCsv } from "./payroll.parsers.csv.js";

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function toAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Number(parsed.toFixed(6));
}

function toDateOnly(value) {
  const pad2 = (n) => String(n).padStart(2, "0");
  if (!value) {
    return null;
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      return null;
    }
    return `${value.getFullYear()}-${pad2(value.getMonth() + 1)}-${pad2(value.getDate())}`;
  }
  const asString = String(value);
  if (/^\d{4}-\d{2}-\d{2}/.test(asString)) {
    return asString.slice(0, 10);
  }
  const parsed = new Date(asString);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString().slice(0, 10);
}

function sha256(value) {
  return crypto.createHash("sha256").update(String(value ?? ""), "utf8").digest("hex");
}

function parseOptionalJson(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  if (typeof value !== "string") {
    return value;
  }
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function isDuplicateEntryError(err) {
  return Number(err?.errno) === 1062 || normalizeUpperText(err?.code) === "ER_DUP_ENTRY";
}

function duplicateKeyName(err) {
  const message = String(err?.sqlMessage || err?.message || "");
  const keyMatch = message.match(/for key ['`"]([^'"`]+)['`"]/i);
  return keyMatch?.[1] || "";
}

function conflictError(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}

function normalizeHashPart(value) {
  if (value === undefined || value === null) {
    return "";
  }
  return String(value)
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase();
}

function buildPayrollLineHash(runHeader, row) {
  const key = [
    normalizeHashPart(runHeader.entity_code),
    normalizeHashPart(runHeader.provider_code),
    normalizeHashPart(runHeader.payroll_period),
    normalizeHashPart(row.employee_code),
    normalizeHashPart(row.employee_name),
    normalizeHashPart(row.cost_center_code || ""),
    normalizeHashPart(Number(row.gross_pay || 0).toFixed(6)),
    normalizeHashPart(Number(row.net_pay || 0).toFixed(6)),
    normalizeHashPart(Number(row.employee_tax || 0).toFixed(6)),
    normalizeHashPart(Number(row.employee_social_security || 0).toFixed(6)),
    normalizeHashPart(Number(row.employer_tax || 0).toFixed(6)),
    normalizeHashPart(Number(row.employer_social_security || 0).toFixed(6)),
  ].join("|");
  return sha256(key);
}

function zeroPayrollTotals() {
  return {
    total_base_salary: 0,
    total_overtime_pay: 0,
    total_bonus_pay: 0,
    total_allowances: 0,
    total_gross_pay: 0,
    total_employee_tax: 0,
    total_employee_social_security: 0,
    total_other_deductions: 0,
    total_net_pay: 0,
    total_employer_tax: 0,
    total_employer_social_security: 0,
  };
}

function accumulatePayrollTotals(totals, row) {
  totals.total_base_salary += toAmount(row.base_salary);
  totals.total_overtime_pay += toAmount(row.overtime_pay);
  totals.total_bonus_pay += toAmount(row.bonus_pay);
  totals.total_allowances += toAmount(row.allowances_total);
  totals.total_gross_pay += toAmount(row.gross_pay);
  totals.total_employee_tax += toAmount(row.employee_tax);
  totals.total_employee_social_security += toAmount(row.employee_social_security);
  totals.total_other_deductions += toAmount(row.other_deductions);
  totals.total_net_pay += toAmount(row.net_pay);
  totals.total_employer_tax += toAmount(row.employer_tax);
  totals.total_employer_social_security += toAmount(row.employer_social_security);
}

function roundPayrollTotals(totals) {
  const rounded = {};
  for (const [key, value] of Object.entries(totals || {})) {
    rounded[key] = toAmount(value);
  }
  return rounded;
}

async function findPayrollRunScopeById({ tenantId, runId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, legal_entity_id
     FROM payroll_runs
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, runId]
  );
  return result.rows?.[0] || null;
}

async function findPayrollRunHeaderById({ tenantId, runId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        r.id,
        r.tenant_id,
        r.legal_entity_id,
        r.run_no,
        r.provider_code,
        r.entity_code,
        r.payroll_period,
        r.pay_date,
        r.currency_code,
        r.source_batch_ref,
        r.original_filename,
        r.file_checksum,
        r.status,
        r.source_type,
        r.source_provider_code,
        r.source_provider_import_job_id,
        r.run_type,
        r.correction_of_run_id,
        r.correction_reason,
        r.is_reversed,
        r.reversed_by_run_id,
        r.reversed_at,
        r.line_count_total,
        r.line_count_inserted,
        r.line_count_duplicates,
        r.employee_count,
        r.total_base_salary,
        r.total_overtime_pay,
        r.total_bonus_pay,
        r.total_allowances,
        r.total_gross_pay,
        r.total_employee_tax,
        r.total_employee_social_security,
        r.total_other_deductions,
        r.total_net_pay,
        r.total_employer_tax,
        r.total_employer_social_security,
        r.raw_meta_json,
        r.imported_by_user_id,
        r.reviewed_by_user_id,
        r.reviewed_at,
        r.finalized_by_user_id,
        r.finalized_at,
        r.accrual_journal_entry_id,
        r.accrual_posted_by_user_id,
        r.accrual_posted_at,
        r.imported_at,
        r.created_at,
        r.updated_at,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name,
        le.functional_currency_code AS legal_entity_functional_currency_code
     FROM payroll_runs r
     JOIN legal_entities le
       ON le.id = r.legal_entity_id
      AND le.tenant_id = r.tenant_id
     WHERE r.tenant_id = ?
       AND r.id = ?
     LIMIT 1`,
    [tenantId, runId]
  );
  const row = result.rows?.[0] || null;
  if (!row) {
    return null;
  }
  return {
    ...row,
    raw_meta_json: parseOptionalJson(row.raw_meta_json),
  };
}

async function findPayrollRunHeaderForUpdate({ tenantId, runId, runQuery }) {
  const result = await runQuery(
    `SELECT *
     FROM payroll_runs
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1
     FOR UPDATE`,
    [tenantId, runId]
  );
  return result.rows?.[0] || null;
}

async function findPayrollRunLinesByRunId({ tenantId, runId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        l.id,
        l.tenant_id,
        l.legal_entity_id,
        l.run_id,
        l.line_no,
        l.employee_code,
        l.employee_name,
        l.cost_center_code,
        l.base_salary,
        l.overtime_pay,
        l.bonus_pay,
        l.allowances_total,
        l.gross_pay,
        l.employee_tax,
        l.employee_social_security,
        l.other_deductions,
        l.net_pay,
        l.employer_tax,
        l.employer_social_security,
        l.line_hash,
        l.raw_row_json,
        l.created_at
     FROM payroll_run_lines l
     WHERE l.tenant_id = ?
       AND l.run_id = ?
     ORDER BY l.line_no ASC, l.id ASC`,
    [tenantId, runId]
  );
  return (result.rows || []).map((row) => ({
    ...row,
    raw_row_json: parseOptionalJson(row.raw_row_json),
  }));
}

async function findPayrollRunAuditByRunId({ tenantId, runId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        a.id,
        a.tenant_id,
        a.legal_entity_id,
        a.run_id,
        a.action,
        a.payload_json,
        a.acted_by_user_id,
        a.acted_at
     FROM payroll_run_audit a
     WHERE a.tenant_id = ?
       AND a.run_id = ?
     ORDER BY a.id DESC`,
    [tenantId, runId]
  );
  return (result.rows || []).map((row) => ({
    ...row,
    payload_json: parseOptionalJson(row.payload_json),
  }));
}

async function buildPayrollRunDetail({ tenantId, runId, runQuery = query }) {
  const header = await findPayrollRunHeaderById({ tenantId, runId, runQuery });
  if (!header) {
    return null;
  }

  const [lines, audit] = await Promise.all([
    findPayrollRunLinesByRunId({ tenantId, runId, runQuery }),
    findPayrollRunAuditByRunId({ tenantId, runId, runQuery }),
  ]);

  return {
    ...header,
    lines,
    audit,
  };
}

async function writePayrollRunAudit({
  tenantId,
  legalEntityId,
  runId,
  action,
  payload = null,
  userId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO payroll_run_audit (
        tenant_id,
        legal_entity_id,
        run_id,
        action,
        payload_json,
        acted_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, runId, action, safeJson(payload), userId]
  );
}

async function nextPayrollRunNo({ tenantId, legalEntityId, payrollPeriod, runQuery = query }) {
  const yyyymm = String(payrollPeriod || "").slice(0, 7).replace("-", "");
  const result = await runQuery(
    `SELECT COALESCE(COUNT(*), 0) + 1 AS next_seq
     FROM payroll_runs
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND payroll_period = ?`,
    [tenantId, legalEntityId, payrollPeriod]
  );
  const nextSeq = Number(result.rows?.[0]?.next_seq || 1);
  return `PR-${yyyymm}-${String(legalEntityId)}-${String(nextSeq).padStart(4, "0")}`;
}

async function getExistingRunByChecksum({
  tenantId,
  legalEntityId,
  payrollPeriod,
  providerCode,
  fileChecksum,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT id
     FROM payroll_runs
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND payroll_period = ?
       AND provider_code = ?
       AND file_checksum = ?
     LIMIT 1`,
    [tenantId, legalEntityId, payrollPeriod, providerCode, fileChecksum]
  );
  return result.rows?.[0] || null;
}

async function getPayrollRunDetailOrThrow({ tenantId, runId, runQuery = query }) {
  const row = await buildPayrollRunDetail({ tenantId, runId, runQuery });
  if (!row) {
    throw badRequest("Payroll run not found");
  }
  return row;
}

export async function resolvePayrollRunScope(runId, tenantId) {
  const parsedRunId = parsePositiveInt(runId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedRunId || !parsedTenantId) {
    return null;
  }

  const row = await findPayrollRunScopeById({
    tenantId: parsedTenantId,
    runId: parsedRunId,
  });
  if (!row) {
    return null;
  }

  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: parsePositiveInt(row.legal_entity_id),
  };
}

export async function listPayrollRunRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["r.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "r.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("r.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }
  if (filters.entityCode) {
    conditions.push("r.entity_code = ?");
    params.push(filters.entityCode);
  }
  if (filters.providerCode) {
    conditions.push("r.provider_code = ?");
    params.push(filters.providerCode);
  }
  if (filters.payrollPeriod) {
    conditions.push("r.payroll_period = ?");
    params.push(filters.payrollPeriod);
  }
  if (filters.status) {
    conditions.push("r.status = ?");
    params.push(filters.status);
  }
  if (filters.q) {
    const like = `%${filters.q}%`;
    conditions.push(
      "(r.run_no LIKE ? OR r.entity_code LIKE ? OR r.provider_code LIKE ? OR r.original_filename LIKE ?)"
    );
    params.push(like, like, like, like);
  }

  const whereSql = conditions.join(" AND ");

  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM payroll_runs r
     WHERE ${whereSql}`,
    params
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const safeLimit =
    Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset =
    Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;

  const listResult = await query(
    `SELECT
        r.id,
        r.tenant_id,
        r.legal_entity_id,
        r.run_no,
        r.provider_code,
        r.entity_code,
        r.payroll_period,
        r.pay_date,
        r.currency_code,
        r.status,
        r.source_type,
        r.source_provider_code,
        r.source_provider_import_job_id,
        r.run_type,
        r.correction_of_run_id,
        r.is_reversed,
        r.reversed_by_run_id,
        r.line_count_total,
        r.line_count_inserted,
        r.line_count_duplicates,
        r.employee_count,
        r.total_gross_pay,
        r.total_net_pay,
        r.total_employee_tax,
        r.total_employee_social_security,
        r.total_employer_tax,
        r.total_employer_social_security,
        r.accrual_journal_entry_id,
        r.reviewed_at,
        r.finalized_at,
        r.imported_at,
        r.created_at,
        le.name AS legal_entity_name
     FROM payroll_runs r
     JOIN legal_entities le
       ON le.id = r.legal_entity_id
      AND le.tenant_id = r.tenant_id
     WHERE ${whereSql}
     ORDER BY r.payroll_period DESC, r.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: listResult.rows || [],
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getPayrollRunByIdForTenant({
  req,
  tenantId,
  runId,
  assertScopeAccess,
}) {
  const row = await getPayrollRunDetailOrThrow({ tenantId, runId });
  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "runId");
  return row;
}

export async function listPayrollRunLineRows({
  req,
  tenantId,
  runId,
  filters,
  assertScopeAccess,
}) {
  const header = await findPayrollRunHeaderById({ tenantId, runId });
  if (!header) {
    throw badRequest("Payroll run not found");
  }
  assertScopeAccess(req, "legal_entity", header.legal_entity_id, "runId");

  const params = [tenantId, runId];
  const conditions = ["l.tenant_id = ?", "l.run_id = ?"];

  if (filters.costCenterCode) {
    conditions.push("l.cost_center_code = ?");
    params.push(filters.costCenterCode);
  }
  if (filters.q) {
    const like = `%${filters.q}%`;
    conditions.push("(l.employee_code LIKE ? OR l.employee_name LIKE ? OR l.cost_center_code LIKE ?)");
    params.push(like, like, like);
  }

  const whereSql = conditions.join(" AND ");

  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM payroll_run_lines l
     WHERE ${whereSql}`,
    params
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const safeLimit =
    Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 200;
  const safeOffset =
    Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;

  const listResult = await query(
    `SELECT
        l.id,
        l.tenant_id,
        l.legal_entity_id,
        l.run_id,
        l.line_no,
        l.employee_code,
        l.employee_name,
        l.cost_center_code,
        l.base_salary,
        l.overtime_pay,
        l.bonus_pay,
        l.allowances_total,
        l.gross_pay,
        l.employee_tax,
        l.employee_social_security,
        l.other_deductions,
        l.net_pay,
        l.employer_tax,
        l.employer_social_security,
        l.line_hash,
        l.raw_row_json,
        l.created_at
     FROM payroll_run_lines l
     WHERE ${whereSql}
     ORDER BY l.line_no ASC, l.id ASC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: (listResult.rows || []).map((row) => ({
      ...row,
      raw_row_json: parseOptionalJson(row.raw_row_json),
    })),
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function importPayrollRunCsv({
  req,
  payload,
  assertScopeAccess,
}) {
  await assertCurrencyExists(payload.currencyCode, "currencyCode");
  if (payload.legalEntityId) {
    await assertLegalEntityBelongsToTenant(payload.tenantId, payload.legalEntityId, "legalEntityId");
    assertScopeAccess(req, "legal_entity", payload.legalEntityId, "legalEntityId");
  }

  let parsedRows;
  let normalizedCsvChecksum;
  let employeeCodes;
  let roundedTotals;
  try {
    parsedRows = parsePayrollCsv(payload.csvText);
    normalizedCsvChecksum = sha256(String(payload.csvText || "").replace(/\r\n/g, "\n"));
    employeeCodes = new Set();
    const totals = zeroPayrollTotals();

    for (const row of parsedRows) {
      employeeCodes.add(String(row.employee_code || "").trim().toUpperCase());
      accumulatePayrollTotals(totals, row);
    }

    roundedTotals = roundPayrollTotals(totals);
  } catch (err) {
    throw badRequest(err?.message || "Payroll CSV parse/validation failed");
  }

  try {
    const runId = await withTransaction(async (tx) => {
      let resolvedLegalEntityId = parsePositiveInt(payload.legalEntityId);
      let entityCode = null;
      let runNo = null;
      let newRunId = null;
      let importedIntoCorrectionShell = false;

      if (parsePositiveInt(payload.targetRunId)) {
        const targetRunId = parsePositiveInt(payload.targetRunId);
        const targetRun = await findPayrollRunHeaderForUpdate({
          tenantId: payload.tenantId,
          runId: targetRunId,
          runQuery: tx.query,
        });
        if (!targetRun) {
          throw badRequest("targetRunId not found");
        }

        resolvedLegalEntityId = parsePositiveInt(targetRun.legal_entity_id);
        assertScopeAccess(req, "legal_entity", resolvedLegalEntityId, "targetRunId");

        if (payload.legalEntityId && resolvedLegalEntityId !== parsePositiveInt(payload.legalEntityId)) {
          throw badRequest("legalEntityId must match targetRunId legal entity");
        }
        if (normalizeUpperText(targetRun.status) !== "DRAFT") {
          throw badRequest("targetRunId must be a DRAFT payroll correction shell");
        }
        if (!["OFF_CYCLE", "RETRO"].includes(normalizeUpperText(targetRun.run_type || "REGULAR"))) {
          throw badRequest("targetRunId must be an OFF_CYCLE or RETRO correction shell");
        }

        entityCode = normalizeUpperText(targetRun.entity_code);
        if (!entityCode) {
          throw badRequest("targetRunId entity_code is missing");
        }
        if (normalizeUpperText(targetRun.provider_code) !== payload.providerCode) {
          throw badRequest("providerCode must match target correction shell");
        }
        if (toDateOnly(targetRun.payroll_period) !== payload.payrollPeriod) {
          throw badRequest("payrollPeriod must match target correction shell");
        }
        if (toDateOnly(targetRun.pay_date) !== payload.payDate) {
          throw badRequest("payDate must match target correction shell");
        }
        if (normalizeUpperText(targetRun.currency_code) !== payload.currencyCode) {
          throw badRequest("currencyCode must match target correction shell");
        }

        const linePresence = await tx.query(
          `SELECT id
           FROM payroll_run_lines
           WHERE tenant_id = ? AND legal_entity_id = ? AND run_id = ?
           LIMIT 1
           FOR UPDATE`,
          [payload.tenantId, resolvedLegalEntityId, targetRunId]
        );
        if (linePresence.rows?.[0]?.id) {
          throw conflictError("targetRunId already has payroll lines; create a new correction shell");
        }

        const existing = await getExistingRunByChecksum({
          tenantId: payload.tenantId,
          legalEntityId: resolvedLegalEntityId,
          payrollPeriod: payload.payrollPeriod,
          providerCode: payload.providerCode,
          fileChecksum: normalizedCsvChecksum,
          runQuery: tx.query,
        });
        if (existing?.id && parsePositiveInt(existing.id) !== targetRunId) {
          throw conflictError("Payroll CSV already imported for this entity/period/provider (same checksum)");
        }

        await tx.query(
          `UPDATE payroll_runs
           SET provider_code = ?,
               entity_code = ?,
               payroll_period = ?,
               pay_date = ?,
               currency_code = ?,
               source_batch_ref = COALESCE(?, source_batch_ref),
               original_filename = ?,
               file_checksum = ?,
               status = 'IMPORTED',
               line_count_total = ?,
               line_count_inserted = 0,
               line_count_duplicates = 0,
               employee_count = ?,
               total_base_salary = ?,
               total_overtime_pay = ?,
               total_bonus_pay = ?,
               total_allowances = ?,
               total_gross_pay = ?,
               total_employee_tax = ?,
               total_employee_social_security = ?,
               total_other_deductions = ?,
               total_net_pay = ?,
               total_employer_tax = ?,
               total_employer_social_security = ?,
               raw_meta_json = ?,
               imported_by_user_id = ?,
               imported_at = CURRENT_TIMESTAMP,
               reviewed_by_user_id = NULL,
               reviewed_at = NULL,
               finalized_by_user_id = NULL,
               finalized_at = NULL,
               accrual_journal_entry_id = NULL,
               accrual_posted_by_user_id = NULL,
               accrual_posted_at = NULL
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?`,
          [
            payload.providerCode,
            entityCode,
            payload.payrollPeriod,
            payload.payDate,
            payload.currencyCode,
            payload.sourceBatchRef,
            payload.originalFilename,
            normalizedCsvChecksum,
            parsedRows.length,
            employeeCodes.size,
            roundedTotals.total_base_salary,
            roundedTotals.total_overtime_pay,
            roundedTotals.total_bonus_pay,
            roundedTotals.total_allowances,
            roundedTotals.total_gross_pay,
            roundedTotals.total_employee_tax,
            roundedTotals.total_employee_social_security,
            roundedTotals.total_other_deductions,
            roundedTotals.total_net_pay,
            roundedTotals.total_employer_tax,
            roundedTotals.total_employer_social_security,
            safeJson({
              parser: "payroll.parsers.csv",
              header_version: "v1",
              original_filename: payload.originalFilename,
              target_run_id: targetRunId,
              correction_shell_import: true,
            }),
            payload.userId,
            payload.tenantId,
            resolvedLegalEntityId,
            targetRunId,
          ]
        );

        runNo = String(targetRun.run_no || `RUN-${targetRunId}`);
        newRunId = targetRunId;
        importedIntoCorrectionShell = true;
      } else {
        const legalEntity = await assertLegalEntityBelongsToTenant(
          payload.tenantId,
          resolvedLegalEntityId,
          "legalEntityId"
        );
        assertScopeAccess(req, "legal_entity", resolvedLegalEntityId, "legalEntityId");
        entityCode = normalizeUpperText(legalEntity?.code);
        if (!entityCode) {
          throw badRequest("legalEntity code is required for payroll import");
        }

        const existing = await getExistingRunByChecksum({
          tenantId: payload.tenantId,
          legalEntityId: resolvedLegalEntityId,
          payrollPeriod: payload.payrollPeriod,
          providerCode: payload.providerCode,
          fileChecksum: normalizedCsvChecksum,
          runQuery: tx.query,
        });
        if (existing?.id) {
          throw conflictError("Payroll CSV already imported for this entity/period/provider (same checksum)");
        }

        runNo = await nextPayrollRunNo({
          tenantId: payload.tenantId,
          legalEntityId: resolvedLegalEntityId,
          payrollPeriod: payload.payrollPeriod,
          runQuery: tx.query,
        });

        const headerInsert = await tx.query(
          `INSERT INTO payroll_runs (
              tenant_id,
              legal_entity_id,
              run_no,
              provider_code,
              entity_code,
              payroll_period,
              pay_date,
              currency_code,
              source_batch_ref,
              original_filename,
              file_checksum,
              status,
              line_count_total,
              line_count_inserted,
              line_count_duplicates,
              employee_count,
              total_base_salary,
              total_overtime_pay,
              total_bonus_pay,
              total_allowances,
              total_gross_pay,
              total_employee_tax,
              total_employee_social_security,
              total_other_deductions,
              total_net_pay,
              total_employer_tax,
              total_employer_social_security,
              raw_meta_json,
              imported_by_user_id
            )
            VALUES (
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'IMPORTED',
              ?, ?, ?, ?,
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )`,
          [
            payload.tenantId,
            resolvedLegalEntityId,
            runNo,
            payload.providerCode,
            entityCode,
            payload.payrollPeriod,
            payload.payDate,
            payload.currencyCode,
            payload.sourceBatchRef,
            payload.originalFilename,
            normalizedCsvChecksum,
            parsedRows.length,
            0, // fill after line insert
            0, // fill after line insert
            employeeCodes.size,
            roundedTotals.total_base_salary,
            roundedTotals.total_overtime_pay,
            roundedTotals.total_bonus_pay,
            roundedTotals.total_allowances,
            roundedTotals.total_gross_pay,
            roundedTotals.total_employee_tax,
            roundedTotals.total_employee_social_security,
            roundedTotals.total_other_deductions,
            roundedTotals.total_net_pay,
            roundedTotals.total_employer_tax,
            roundedTotals.total_employer_social_security,
            safeJson({
              parser: "payroll.parsers.csv",
              header_version: "v1",
              original_filename: payload.originalFilename,
            }),
            payload.userId,
          ]
        );

        newRunId = parsePositiveInt(headerInsert.rows?.insertId);
        if (!newRunId) {
          throw new Error("Failed to create payroll run header");
        }
      }

      let insertedCount = 0;
      let duplicateCount = 0;
      const seenLineHashes = new Set();

      for (const row of parsedRows) {
        const lineHash = buildPayrollLineHash(
          {
            entity_code: entityCode,
            provider_code: payload.providerCode,
            payroll_period: payload.payrollPeriod,
          },
          row
        );

        if (seenLineHashes.has(lineHash)) {
          duplicateCount += 1;
          continue;
        }
        seenLineHashes.add(lineHash);

        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO payroll_run_lines (
              tenant_id,
              legal_entity_id,
              run_id,
              line_no,
              employee_code,
              employee_name,
              cost_center_code,
              base_salary,
              overtime_pay,
              bonus_pay,
              allowances_total,
              gross_pay,
              employee_tax,
              employee_social_security,
              other_deductions,
              net_pay,
              employer_tax,
              employer_social_security,
              line_hash,
              raw_row_json
            )
            VALUES (
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )`,
          [
            payload.tenantId,
            resolvedLegalEntityId,
            newRunId,
            row.line_no,
            row.employee_code,
            row.employee_name,
            row.cost_center_code,
            row.base_salary,
            row.overtime_pay,
            row.bonus_pay,
            row.allowances_total,
            row.gross_pay,
            row.employee_tax,
            row.employee_social_security,
            row.other_deductions,
            row.net_pay,
            row.employer_tax,
            row.employer_social_security,
            lineHash,
            safeJson(row.raw_row_json),
          ]
        );
        insertedCount += 1;
      }

      await tx.query(
        `UPDATE payroll_runs
         SET line_count_inserted = ?,
             line_count_duplicates = ?
         WHERE tenant_id = ?
           AND id = ?`,
        [insertedCount, duplicateCount, payload.tenantId, newRunId]
      );

      await writePayrollRunAudit({
        tenantId: payload.tenantId,
        legalEntityId: resolvedLegalEntityId,
        runId: newRunId,
        action: "VALIDATION",
        payload: {
          parser: "payroll.parsers.csv",
          checks: ["gross_pay_consistency", "net_pay_consistency"],
          rowCount: parsedRows.length,
        },
        userId: payload.userId,
        runQuery: tx.query,
      });

      await writePayrollRunAudit({
        tenantId: payload.tenantId,
        legalEntityId: resolvedLegalEntityId,
        runId: newRunId,
        action: "IMPORTED",
        payload: {
          runNo,
          providerCode: payload.providerCode,
          entityCode,
          payrollPeriod: payload.payrollPeriod,
          payDate: payload.payDate,
          currencyCode: payload.currencyCode,
          lineCountTotal: parsedRows.length,
          lineCountInserted: insertedCount,
          lineCountDuplicates: duplicateCount,
          employeeCount: employeeCodes.size,
        },
        userId: payload.userId,
        runQuery: tx.query,
      });

      if (importedIntoCorrectionShell) {
        await writePayrollRunAudit({
          tenantId: payload.tenantId,
          legalEntityId: resolvedLegalEntityId,
          runId: newRunId,
          action: "IMPORTED_TO_CORRECTION_SHELL",
          payload: {
            targetRunId: newRunId,
            runNo,
            correctionType: null,
          },
          userId: payload.userId,
          runQuery: tx.query,
        });
      }

      return newRunId;
    });

    return getPayrollRunByIdForTenant({
      req,
      tenantId: payload.tenantId,
      runId,
      assertScopeAccess,
    });
  } catch (err) {
    if (err?.status === 409) {
      throw err;
    }
    if (isDuplicateEntryError(err)) {
      const keyName = duplicateKeyName(err);
      if (keyName.includes("uk_payroll_runs_tenant_entity_checksum")) {
        throw conflictError("Payroll CSV already imported for this entity/period/provider (same checksum)");
      }
    }
    if (err?.message && /gross_pay mismatch|net_pay mismatch|Missing CSV column|CSV/i.test(err.message)) {
      throw badRequest(err.message);
    }
    throw err;
  }
}
