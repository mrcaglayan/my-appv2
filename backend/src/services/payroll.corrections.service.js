import crypto from "node:crypto";
import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { assertPayrollPeriodActionAllowed } from "./payroll.close.service.js";

function u(v) {
  return String(v || "").trim().toUpperCase();
}
function amt(v) {
  const n = Number(v);
  return Number.isFinite(n) ? Number(n.toFixed(6)) : 0;
}
function a(v) {
  return amt(v).toFixed(6);
}
function j(v) {
  return JSON.stringify(v ?? null);
}
function d(v) {
  const pad2 = (n) => String(n).padStart(2, "0");
  if (!v) return null;
  if (v instanceof Date) {
    if (Number.isNaN(v.getTime())) return null;
    return `${v.getFullYear()}-${pad2(v.getMonth() + 1)}-${pad2(v.getDate())}`;
  }
  const s = String(v).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  const p = new Date(s);
  return Number.isNaN(p.getTime()) ? null : `${p.getFullYear()}-${pad2(p.getMonth() + 1)}-${pad2(p.getDate())}`;
}
function t(v) {
  if (!v) return null;
  const s = String(v).trim();
  if (!s) return null;
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(s)) return s;
  const p = new Date(s);
  return Number.isNaN(p.getTime()) ? s : p.toISOString().slice(0, 19).replace("T", " ");
}
function hash(v) {
  return crypto.createHash("sha256").update(String(v ?? ""), "utf8").digest("hex");
}
function isDup(err) {
  return Number(err?.errno) === 1062 || u(err?.code) === "ER_DUP_ENTRY";
}
function dupKey(err) {
  const m = String(err?.sqlMessage || err?.message || "");
  return m.match(/for key ['`"]([^'"`]+)['`"]/i)?.[1] || "";
}
function conflict(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}
function notFound(message) {
  const err = new Error(message);
  err.status = 404;
  return err;
}
function clip(v, n) {
  const s = String(v ?? "");
  return s.length <= n ? s : `${s.slice(0, Math.max(0, n - 3))}...`;
}

async function findLegalEntityByCode({ tenantId, entityCode, runQuery = query }) {
  const r = await runQuery(
    `SELECT id, tenant_id, code, name, functional_currency_code
     FROM legal_entities
     WHERE tenant_id = ? AND code = ?
     LIMIT 1`,
    [tenantId, u(entityCode)]
  );
  return r.rows?.[0] || null;
}

async function findRun({ tenantId, runId, runQuery = query, forUpdate = false }) {
  const r = await runQuery(
    `SELECT
        pr.*,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM payroll_runs pr
     JOIN legal_entities le ON le.id = pr.legal_entity_id AND le.tenant_id = pr.tenant_id
     WHERE pr.tenant_id = ? AND pr.id = ?
     LIMIT 1${forUpdate ? " FOR UPDATE" : ""}`,
    [tenantId, runId]
  );
  const row = r.rows?.[0] || null;
  if (!row) return null;
  return {
    ...row,
    payroll_period: d(row.payroll_period),
    pay_date: d(row.pay_date),
    imported_at: row.imported_at ? String(row.imported_at) : null,
    reviewed_at: row.reviewed_at ? String(row.reviewed_at) : null,
    finalized_at: row.finalized_at ? String(row.finalized_at) : null,
    reversed_at: row.reversed_at ? String(row.reversed_at) : null,
    is_reversed: row.is_reversed === 1 || row.is_reversed === true || row.is_reversed === "1",
  };
}

async function listRunLines({ tenantId, legalEntityId, runId, runQuery = query }) {
  const r = await runQuery(
    `SELECT id, line_no, employee_code, employee_name, cost_center_code,
            base_salary, overtime_pay, bonus_pay, allowances_total, gross_pay,
            employee_tax, employee_social_security, other_deductions, net_pay,
            employer_tax, employer_social_security, raw_row_json
     FROM payroll_run_lines
     WHERE tenant_id = ? AND legal_entity_id = ? AND run_id = ?
     ORDER BY line_no ASC, id ASC`,
    [tenantId, legalEntityId, runId]
  );
  return r.rows || [];
}

async function writeRunAudit({
  tenantId,
  legalEntityId,
  runId,
  action,
  payload = null,
  userId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO payroll_run_audit
      (tenant_id, legal_entity_id, run_id, action, payload_json, acted_by_user_id)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, runId, action, j(payload), userId]
  );
}

async function writeLiabilityAudit({
  tenantId,
  legalEntityId,
  runId,
  liabilityId = null,
  action,
  payload = null,
  userId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO payroll_liability_audit
      (tenant_id, legal_entity_id, run_id, payroll_liability_id, action, payload_json, acted_by_user_id)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, runId, liabilityId, action, j(payload), userId]
  );
}

async function listCorrectionRows({ tenantId, legalEntityId, runId, runQuery = query }) {
  const r = await runQuery(
    `SELECT
        c.id, c.original_run_id, c.correction_run_id, c.correction_type, c.status,
        c.idempotency_key, c.notes, c.created_by_user_id, c.created_at, c.updated_at,
        o.run_no AS original_run_no, o.status AS original_run_status, o.run_type AS original_run_type,
        cr.run_no AS correction_run_no, cr.status AS correction_run_status, cr.run_type AS correction_run_type,
        cr.payroll_period AS correction_payroll_period, cr.pay_date AS correction_pay_date
     FROM payroll_run_corrections c
     LEFT JOIN payroll_runs o
       ON o.tenant_id = c.tenant_id AND o.legal_entity_id = c.legal_entity_id AND o.id = c.original_run_id
     JOIN payroll_runs cr
       ON cr.tenant_id = c.tenant_id AND cr.legal_entity_id = c.legal_entity_id AND cr.id = c.correction_run_id
     WHERE c.tenant_id = ? AND c.legal_entity_id = ? AND (c.original_run_id = ? OR c.correction_run_id = ?)
     ORDER BY c.id DESC`,
    [tenantId, legalEntityId, runId, runId]
  );
  return (r.rows || []).map((row) => ({
    ...row,
    correction_payroll_period: d(row.correction_payroll_period),
    correction_pay_date: d(row.correction_pay_date),
  }));
}

async function liabilitySummary({ tenantId, legalEntityId, runId, runQuery = query }) {
  const r = await runQuery(
    `SELECT
        COALESCE(SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END),0) AS open_count,
        COALESCE(SUM(CASE WHEN status = 'IN_BATCH' THEN 1 ELSE 0 END),0) AS in_batch_count,
        COALESCE(SUM(CASE WHEN status = 'PARTIALLY_PAID' THEN 1 ELSE 0 END),0) AS partially_paid_count,
        COALESCE(SUM(CASE WHEN status = 'PAID' THEN 1 ELSE 0 END),0) AS paid_count,
        COALESCE(SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END),0) AS cancelled_count
     FROM payroll_run_liabilities
     WHERE tenant_id = ? AND legal_entity_id = ? AND run_id = ?`,
    [tenantId, legalEntityId, runId]
  );
  return r.rows?.[0] || {
    open_count: 0,
    in_batch_count: 0,
    partially_paid_count: 0,
    paid_count: 0,
    cancelled_count: 0,
  };
}

async function cancelOpenLiabilitiesForReversalTx({ tenantId, legalEntityId, runId, userId, runQuery }) {
  const r = await runQuery(
    `SELECT id, status, amount
     FROM payroll_run_liabilities
     WHERE tenant_id = ? AND legal_entity_id = ? AND run_id = ?
     ORDER BY id ASC
     FOR UPDATE`,
    [tenantId, legalEntityId, runId]
  );
  const rows = r.rows || [];
  const inBatch = rows.filter((x) => u(x.status) === "IN_BATCH").length;
  const partiallyPaid = rows.filter((x) => u(x.status) === "PARTIALLY_PAID").length;
  const paid = rows.filter((x) => u(x.status) === "PAID").length;
  if (inBatch > 0) {
    throw conflict("Cannot reverse: some payroll liabilities are IN_BATCH. Release/cancel payment batch first.");
  }
  if (partiallyPaid > 0) {
    throw conflict(
      "Cannot reverse: some payroll liabilities are PARTIALLY_PAID. Use RETRO/OFF_CYCLE correction."
    );
  }
  if (paid > 0) {
    throw conflict("Cannot reverse: some payroll liabilities are already PAID. Use RETRO/OFF_CYCLE correction.");
  }
  let cancelledOpen = 0;
  for (const row of rows) {
    if (u(row.status) !== "OPEN") continue;
    // eslint-disable-next-line no-await-in-loop
    await runQuery(
      `UPDATE payroll_run_liabilities
       SET status = 'CANCELLED',
           cancelled_at = CURRENT_TIMESTAMP,
           cancelled_reason = 'reversed_run',
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ? AND status = 'OPEN'`,
      [tenantId, legalEntityId, row.id]
    );
    // eslint-disable-next-line no-await-in-loop
    await writeLiabilityAudit({
      tenantId,
      legalEntityId,
      runId,
      liabilityId: row.id,
      action: "CANCELLED_BY_REVERSAL",
      payload: { amount: amt(row.amount) },
      userId,
      runQuery,
    });
    cancelledOpen += 1;
  }
  return { cancelledOpen, inBatch, partiallyPaid, paid };
}

async function reverseJournalTx({ tenantId, journalId, userId, reason, runQuery }) {
  const h = await runQuery(
    `SELECT id, tenant_id, legal_entity_id, book_id, fiscal_period_id, journal_no, source_type, status,
            entry_date, document_date, currency_code, description, reference_no,
            total_debit_base, total_credit_base, reversal_journal_entry_id
     FROM journal_entries
     WHERE tenant_id = ? AND id = ?
     LIMIT 1
     FOR UPDATE`,
    [tenantId, journalId]
  );
  const orig = h.rows?.[0] || null;
  if (!orig) throw badRequest("Accrual journal not found");
  const existing = parsePositiveInt(orig.reversal_journal_entry_id);
  if (u(orig.status) === "REVERSED" && existing) {
    return { journalEntryId: existing, idempotentReplay: true };
  }
  if (u(orig.status) !== "POSTED") {
    throw badRequest(`Accrual journal ${journalId} is not POSTED and cannot be reversed`);
  }
  const l = await runQuery(
    `SELECT line_no, account_id, operating_unit_id, counterparty_legal_entity_id, description,
            subledger_reference_no, currency_code, amount_txn, debit_base, credit_base, tax_code
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no ASC`,
    [journalId]
  );
  if (!Array.isArray(l.rows) || l.rows.length === 0) {
    throw badRequest("Original accrual journal has no lines to reverse");
  }
  const reversalNo = `PRREV-${journalId}`.slice(0, 40);
  const ins = await runQuery(
    `INSERT INTO journal_entries (
        tenant_id, legal_entity_id, book_id, fiscal_period_id, journal_no,
        source_type, status, entry_date, document_date, currency_code,
        description, reference_no, total_debit_base, total_credit_base,
        created_by_user_id, posted_by_user_id, posted_at, reverse_reason
     ) VALUES (?, ?, ?, ?, ?, ?, 'POSTED', ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)`,
    [
      tenantId,
      orig.legal_entity_id,
      orig.book_id,
      orig.fiscal_period_id,
      reversalNo,
      u(orig.source_type || "SYSTEM") || "SYSTEM",
      d(orig.entry_date),
      d(orig.document_date),
      u(orig.currency_code),
      clip(`Payroll reversal of ${orig.journal_no}`, 500),
      orig.reference_no || null,
      amt(orig.total_credit_base),
      amt(orig.total_debit_base),
      userId,
      userId,
      clip(reason || "Payroll run reversal", 255),
    ]
  );
  const reversalId = parsePositiveInt(ins.rows?.insertId);
  if (!reversalId) throw new Error("Failed to create payroll reversal journal");
  await runQuery(
    `INSERT INTO journal_lines (
        journal_entry_id, line_no, account_id, operating_unit_id, counterparty_legal_entity_id,
        description, subledger_reference_no, currency_code, amount_txn, debit_base, credit_base, tax_code
     )
     SELECT
       ?, line_no, account_id, operating_unit_id, counterparty_legal_entity_id,
       description, subledger_reference_no, currency_code,
       amount_txn * -1, credit_base, debit_base, tax_code
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no ASC`,
    [reversalId, journalId]
  );
  await runQuery(
    `UPDATE journal_entries
     SET status = 'REVERSED',
         reversed_by_user_id = ?,
         reversed_at = CURRENT_TIMESTAMP,
         reversal_journal_entry_id = ?,
         reverse_reason = ?
     WHERE tenant_id = ? AND id = ?`,
    [userId, reversalId, clip(reason || "Payroll run reversal", 255), tenantId, journalId]
  );
  return { journalEntryId: reversalId, idempotentReplay: false };
}

async function createReversalRunTx({
  tenantId,
  legalEntityId,
  originalRun,
  reversalAccrualJeId,
  userId,
  reason,
  runQuery,
}) {
  const runNo = clip(`REV-${String(originalRun.run_no || originalRun.id)}`, 60);
  const fileChecksum = hash(`PRREV|T:${tenantId}|LE:${legalEntityId}|RUN:${originalRun.id}|JE:${reversalAccrualJeId}`);
  const originalFilename = clip(`reversal-${originalRun.run_no || originalRun.id}.virtual.csv`, 255);

  const ins = await runQuery(
    `INSERT INTO payroll_runs (
        tenant_id, legal_entity_id, run_no, provider_code, entity_code, payroll_period, pay_date, currency_code,
        source_batch_ref, original_filename, file_checksum, status, run_type, correction_of_run_id, correction_reason,
        line_count_total, line_count_inserted, line_count_duplicates, employee_count,
        total_base_salary, total_overtime_pay, total_bonus_pay, total_allowances, total_gross_pay,
        total_employee_tax, total_employee_social_security, total_other_deductions, total_net_pay,
        total_employer_tax, total_employer_social_security, raw_meta_json,
        imported_by_user_id, reviewed_by_user_id, reviewed_at, finalized_by_user_id, finalized_at,
        accrual_journal_entry_id, accrual_posted_by_user_id, accrual_posted_at
     )
     SELECT
        tenant_id, legal_entity_id, ?, provider_code, entity_code, payroll_period, pay_date, currency_code,
        ?, ?, ?, 'FINALIZED', 'REVERSAL', id, ?,
        line_count_total, line_count_inserted, 0, employee_count,
        total_base_salary * -1, total_overtime_pay * -1, total_bonus_pay * -1, total_allowances * -1, total_gross_pay * -1,
        total_employee_tax * -1, total_employee_social_security * -1, total_other_deductions * -1, total_net_pay * -1,
        total_employer_tax * -1, total_employer_social_security * -1,
        JSON_OBJECT('source','PR-P05','reversal_of_run_id',id,'reversal_of_run_no',run_no),
        ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP, ?, ?, CURRENT_TIMESTAMP
     FROM payroll_runs
     WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?
     LIMIT 1`,
    [
      runNo,
      clip(`REVERSAL:${originalRun.id}`, 120),
      originalFilename,
      fileChecksum,
      reason || null,
      userId,
      userId,
      userId,
      reversalAccrualJeId,
      userId,
      tenantId,
      legalEntityId,
      originalRun.id,
    ]
  );
  const reversalRunId = parsePositiveInt(ins.rows?.insertId);
  if (!reversalRunId) throw new Error("Failed to create reversal payroll run");

  await runQuery(
    `INSERT INTO payroll_run_lines (
        tenant_id, legal_entity_id, run_id, line_no, employee_code, employee_name, cost_center_code,
        base_salary, overtime_pay, bonus_pay, allowances_total, gross_pay,
        employee_tax, employee_social_security, other_deductions, net_pay,
        employer_tax, employer_social_security, line_hash, raw_row_json
     )
     SELECT
        tenant_id, legal_entity_id, ?, line_no, employee_code, employee_name, cost_center_code,
        base_salary * -1, overtime_pay * -1, bonus_pay * -1, allowances_total * -1, gross_pay * -1,
        employee_tax * -1, employee_social_security * -1, other_deductions * -1, net_pay * -1,
        employer_tax * -1, employer_social_security * -1,
        SHA2(CONCAT('PRREVLINE|', ?, '|', id), 256),
        raw_row_json
     FROM payroll_run_lines
     WHERE tenant_id = ? AND legal_entity_id = ? AND run_id = ?
     ORDER BY line_no ASC, id ASC`,
    [reversalRunId, reversalRunId, tenantId, legalEntityId, originalRun.id]
  );

  return reversalRunId;
}

async function nextCorrectionRunNo({ tenantId, legalEntityId, payrollPeriod, correctionType, runQuery = query }) {
  const r = await runQuery(
    `SELECT COALESCE(COUNT(*), 0) + 1 AS next_seq
     FROM payroll_runs
     WHERE tenant_id = ? AND legal_entity_id = ? AND payroll_period = ? AND run_type = ?`,
    [tenantId, legalEntityId, payrollPeriod, correctionType]
  );
  const seq = Number(r.rows?.[0]?.next_seq || 1);
  const prefix = correctionType === "RETRO" ? "RET" : "OFC";
  const yyyymm = String(payrollPeriod || "").slice(0, 7).replace("-", "");
  return clip(`${prefix}-${yyyymm}-${String(legalEntityId)}-${String(seq).padStart(4, "0")}`, 60);
}

async function findCorrectionByIdempotency({ tenantId, legalEntityId, idempotencyKey, runQuery = query }) {
  if (!idempotencyKey) return null;
  const r = await runQuery(
    `SELECT * FROM payroll_run_corrections
     WHERE tenant_id = ? AND legal_entity_id = ? AND idempotency_key = ?
     LIMIT 1`,
    [tenantId, legalEntityId, idempotencyKey]
  );
  return r.rows?.[0] || null;
}

async function insertCorrectionRowTx({
  tenantId,
  legalEntityId,
  originalRunId = null,
  correctionRunId,
  correctionType,
  status,
  idempotencyKey = null,
  notes = null,
  userId = null,
  runQuery,
}) {
  try {
    await runQuery(
      `INSERT INTO payroll_run_corrections (
          tenant_id, legal_entity_id, original_run_id, correction_run_id, correction_type,
          status, idempotency_key, notes, created_by_user_id
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [tenantId, legalEntityId, originalRunId, correctionRunId, correctionType, status, idempotencyKey, notes, userId]
    );
  } catch (err) {
    if (!isDup(err) || !dupKey(err).includes("idempotency")) throw err;
  }
}

export async function resolvePayrollCorrectionShellScope(input, tenantId) {
  const tId = parsePositiveInt(tenantId);
  if (!tId) return null;
  const originalRunId = parsePositiveInt(input?.originalRunId ?? input?.original_run_id);
  if (originalRunId) {
    const run = await findRun({ tenantId: tId, runId: originalRunId });
    return run ? { scopeType: "LEGAL_ENTITY", scopeId: parsePositiveInt(run.legal_entity_id) } : null;
  }
  const entityCode = input?.entityCode ?? input?.entity_code;
  if (!entityCode) return null;
  const le = await findLegalEntityByCode({ tenantId: tId, entityCode });
  return le ? { scopeType: "LEGAL_ENTITY", scopeId: parsePositiveInt(le.id) } : null;
}

export async function listPayrollRunCorrections({ req, tenantId, runId, assertScopeAccess }) {
  const run = await findRun({ tenantId, runId });
  if (!run) throw notFound("Payroll run not found");
  const legalEntityId = parsePositiveInt(run.legal_entity_id);
  assertScopeAccess(req, "legal_entity", legalEntityId, "runId");
  const items = await listCorrectionRows({ tenantId, legalEntityId, runId });
  return { run, items };
}

export async function reversePayrollRunWithCorrection({
  req,
  tenantId,
  runId,
  userId,
  reason,
  note = null,
  idempotencyKey = null,
  assertScopeAccess,
}) {
  return withTransaction(async (tx) => {
    const run = await findRun({ tenantId, runId, runQuery: tx.query, forUpdate: true });
    if (!run) throw notFound("Payroll run not found");
    const legalEntityId = parsePositiveInt(run.legal_entity_id);
    assertScopeAccess(req, "legal_entity", legalEntityId, "runId");

    if (u(run.run_type || "REGULAR") === "REVERSAL") {
      throw badRequest("Cannot reverse a reversal payroll run");
    }
    if (u(run.status) !== "FINALIZED" || !parsePositiveInt(run.accrual_journal_entry_id)) {
      throw badRequest("Only FINALIZED payroll runs with accrual journal can be reversed");
    }

    if (run.is_reversed && parsePositiveInt(run.reversed_by_run_id)) {
      const existing = await findRun({ tenantId, runId: parsePositiveInt(run.reversed_by_run_id), runQuery: tx.query });
      return {
        original_run: run,
        reversal_run: existing,
        idempotent: true,
        reversal_accrual_journal_entry_id: parsePositiveInt(existing?.accrual_journal_entry_id),
      };
    }

    await assertPayrollPeriodActionAllowed({
      tenantId,
      legalEntityId,
      payrollPeriod: run.payroll_period,
      actionType: "RUN_REVERSE",
      runQuery: tx.query,
    });

    const summary = await liabilitySummary({ tenantId, legalEntityId, runId, runQuery: tx.query });
    if (Number(summary.in_batch_count || 0) > 0) {
      throw conflict("Cannot reverse: some payroll liabilities are IN_BATCH. Release/cancel payment batch first.");
    }
    if (Number(summary.partially_paid_count || 0) > 0) {
      throw conflict("Cannot reverse: some payroll liabilities are PARTIALLY_PAID. Use RETRO/OFF_CYCLE correction.");
    }
    if (Number(summary.paid_count || 0) > 0) {
      throw conflict("Cannot reverse: some payroll liabilities are already PAID. Use RETRO/OFF_CYCLE correction.");
    }

    const reversalJournal = await reverseJournalTx({
      tenantId,
      journalId: parsePositiveInt(run.accrual_journal_entry_id),
      userId,
      reason,
      runQuery: tx.query,
    });
    const reversalRunId = await createReversalRunTx({
      tenantId,
      legalEntityId,
      originalRun: run,
      reversalAccrualJeId: reversalJournal.journalEntryId,
      userId,
      reason,
      runQuery: tx.query,
    });

    const cancelResult = await cancelOpenLiabilitiesForReversalTx({
      tenantId,
      legalEntityId,
      runId,
      userId,
      runQuery: tx.query,
    });

    await tx.query(
      `UPDATE payroll_runs
       SET is_reversed = 1,
           reversed_by_run_id = ?,
           reversed_at = CURRENT_TIMESTAMP,
           correction_reason = COALESCE(correction_reason, ?)
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
      [reversalRunId, reason || null, tenantId, legalEntityId, runId]
    );

    await insertCorrectionRowTx({
      tenantId,
      legalEntityId,
      originalRunId: runId,
      correctionRunId: reversalRunId,
      correctionType: "REVERSAL",
      status: "APPLIED",
      idempotencyKey,
      notes: note || reason || null,
      userId,
      runQuery: tx.query,
    });

    await writeRunAudit({
      tenantId,
      legalEntityId,
      runId,
      action: "REVERSED",
      payload: {
        reversal_run_id: reversalRunId,
        reversal_accrual_journal_entry_id: reversalJournal.journalEntryId,
        cancelled_open_liabilities: cancelResult.cancelledOpen,
        reason: reason || null,
        note: note || null,
      },
      userId,
      runQuery: tx.query,
    });
    await writeRunAudit({
      tenantId,
      legalEntityId,
      runId: reversalRunId,
      action: "CREATED_AS_REVERSAL",
      payload: {
        original_run_id: runId,
        reversal_accrual_journal_entry_id: reversalJournal.journalEntryId,
        reason: reason || null,
      },
      userId,
      runQuery: tx.query,
    });

    const originalRun = await findRun({ tenantId, runId, runQuery: tx.query });
    const reversalRun = await findRun({ tenantId, runId: reversalRunId, runQuery: tx.query });
    return {
      original_run: originalRun,
      reversal_run: reversalRun,
      idempotent: false,
      reversal_accrual_journal_entry_id: reversalJournal.journalEntryId,
    };
  });
}

export async function createPayrollCorrectionShell({
  req,
  tenantId,
  userId,
  input,
  assertScopeAccess,
}) {
  return withTransaction(async (tx) => {
    const correctionType = u(input.correctionType);
    if (!["OFF_CYCLE", "RETRO"].includes(correctionType)) {
      throw badRequest("correctionType must be OFF_CYCLE or RETRO");
    }

    let originalRun = null;
    let legalEntityId = null;
    let entityCode = u(input.entityCode);
    let providerCode = u(input.providerCode);
    let payrollPeriod = d(input.payrollPeriod);
    let payDate = d(input.payDate);
    let currencyCode = u(input.currencyCode);

    if (parsePositiveInt(input.originalRunId)) {
      originalRun = await findRun({
        tenantId,
        runId: parsePositiveInt(input.originalRunId),
        runQuery: tx.query,
        forUpdate: true,
      });
      if (!originalRun) throw notFound("Original payroll run not found");
      legalEntityId = parsePositiveInt(originalRun.legal_entity_id);
      assertScopeAccess(req, "legal_entity", legalEntityId, "originalRunId");

      if (correctionType === "RETRO" && u(originalRun.status) !== "FINALIZED") {
        throw badRequest("RETRO correction can only target a FINALIZED payroll run");
      }
      if (entityCode && entityCode !== u(originalRun.entity_code)) {
        throw badRequest("entityCode must match original payroll run");
      }
      if (providerCode && providerCode !== u(originalRun.provider_code)) {
        throw badRequest("providerCode must match original payroll run");
      }
      if (currencyCode && currencyCode !== u(originalRun.currency_code)) {
        throw badRequest("currencyCode must match original payroll run");
      }
      if (payrollPeriod && payrollPeriod !== d(originalRun.payroll_period)) {
        throw badRequest("payrollPeriod must match original payroll run");
      }
      if (payDate && payDate !== d(originalRun.pay_date)) {
        throw badRequest("payDate must match original payroll run");
      }

      entityCode = u(originalRun.entity_code);
      providerCode = providerCode || u(originalRun.provider_code);
      payrollPeriod = payrollPeriod || d(originalRun.payroll_period);
      payDate = payDate || d(originalRun.pay_date);
      currencyCode = currencyCode || u(originalRun.currency_code);
    } else {
      if (correctionType === "RETRO") throw badRequest("RETRO correction requires originalRunId");
      if (!entityCode) throw badRequest("entityCode is required");
      const le = await findLegalEntityByCode({ tenantId, entityCode, runQuery: tx.query });
      if (!le) throw badRequest("entityCode not found for tenant");
      legalEntityId = parsePositiveInt(le.id);
      assertScopeAccess(req, "legal_entity", legalEntityId, "entityCode");
      entityCode = u(le.code);
    }

    if (!legalEntityId) throw badRequest("Unable to resolve legal entity");
    if (!providerCode) throw badRequest("providerCode is required");
    if (!payrollPeriod) throw badRequest("payrollPeriod is required");
    if (!payDate) throw badRequest("payDate is required");
    if (!currencyCode) throw badRequest("currencyCode is required");

    if (input.idempotencyKey) {
      const existingCorrection = await findCorrectionByIdempotency({
        tenantId,
        legalEntityId,
        idempotencyKey: input.idempotencyKey,
        runQuery: tx.query,
      });
      if (existingCorrection?.correction_run_id) {
        const existingRun = await findRun({
          tenantId,
          runId: parsePositiveInt(existingCorrection.correction_run_id),
          runQuery: tx.query,
        });
        return { correction_run: existingRun, correction: existingCorrection, idempotent: true };
      }
    }

    await assertPayrollPeriodActionAllowed({
      tenantId,
      legalEntityId,
      payrollPeriod,
      actionType: "RUN_CORRECTION_SHELL_CREATE",
      runQuery: tx.query,
    });

    const runNo = await nextCorrectionRunNo({
      tenantId,
      legalEntityId,
      payrollPeriod,
      correctionType,
      runQuery: tx.query,
    });
    const fileChecksum = hash(`PRCORR-SHELL|${tenantId}|${legalEntityId}|${correctionType}|${runNo}`);
    const originalFilename = clip(`${correctionType.toLowerCase()}-shell-${runNo}.virtual.csv`, 255);

    const ins = await tx.query(
      `INSERT INTO payroll_runs (
          tenant_id, legal_entity_id, run_no, provider_code, entity_code, payroll_period, pay_date, currency_code,
          source_batch_ref, original_filename, file_checksum, status, run_type, correction_of_run_id, correction_reason,
          line_count_total, line_count_inserted, line_count_duplicates, employee_count,
          total_base_salary, total_overtime_pay, total_bonus_pay, total_allowances, total_gross_pay,
          total_employee_tax, total_employee_social_security, total_other_deductions, total_net_pay,
          total_employer_tax, total_employer_social_security, raw_meta_json, imported_by_user_id
       ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'DRAFT', ?, ?, ?,
          0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          ?, NULL
       )`,
      [
        tenantId,
        legalEntityId,
        runNo,
        providerCode,
        entityCode,
        payrollPeriod,
        payDate,
        currencyCode,
        clip(`${correctionType}-SHELL`, 120),
        originalFilename,
        fileChecksum,
        correctionType,
        parsePositiveInt(input.originalRunId),
        input.reason || null,
        j({
          source: "PR-P05",
          correction_shell: true,
          correction_type: correctionType,
          original_run_id: parsePositiveInt(input.originalRunId),
        }),
      ]
    );
    const correctionRunId = parsePositiveInt(ins.rows?.insertId);
    if (!correctionRunId) throw new Error("Failed to create correction shell");

    await insertCorrectionRowTx({
      tenantId,
      legalEntityId,
      originalRunId: parsePositiveInt(input.originalRunId),
      correctionRunId,
      correctionType,
      status: "CREATED",
      idempotencyKey: input.idempotencyKey || null,
      notes: input.reason || null,
      userId,
      runQuery: tx.query,
    });

    await writeRunAudit({
      tenantId,
      legalEntityId,
      runId: correctionRunId,
      action: "CORRECTION_SHELL_CREATED",
      payload: {
        correction_type: correctionType,
        original_run_id: parsePositiveInt(input.originalRunId),
        reason: input.reason || null,
        idempotency_key: input.idempotencyKey || null,
      },
      userId,
      runQuery: tx.query,
    });

    const correctionRun = await findRun({ tenantId, runId: correctionRunId, runQuery: tx.query });
    const rows = await listCorrectionRows({ tenantId, legalEntityId, runId: correctionRunId, runQuery: tx.query });
    return {
      correction_run: correctionRun,
      correction: rows.find((x) => parsePositiveInt(x.correction_run_id) === correctionRunId) || null,
      original_run: originalRun
        ? {
            id: parsePositiveInt(originalRun.id),
            run_no: originalRun.run_no,
            status: u(originalRun.status),
            run_type: u(originalRun.run_type || "REGULAR"),
          }
        : null,
      idempotent: false,
    };
  });
}
