import crypto from "node:crypto";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  getOpsBankPaymentBatchesHealth,
  getOpsBankReconciliationSummary,
  getOpsJobsHealth,
  getOpsPayrollCloseStatus,
  getOpsPayrollImportHealth,
} from "../src/services/ops.dashboard.service.js";

function assert(c, m) {
  if (!c) throw new Error(m);
}
const toInt = (v) => Math.trunc(Number(v) || 0);
const sha256 = (v) => crypto.createHash("sha256").update(String(v || ""), "utf8").digest("hex");
const noScopeGuard = () => true;
const allowAllScopeFilter = () => "1 = 1";
function denyScopeGuard() {
  const e = new Error("Scope access denied");
  e.status = 403;
  throw e;
}

async function expectFailure(work, { status, includes }) {
  try {
    await work();
  } catch (error) {
    if (status !== undefined && Number(error?.status || 0) !== Number(status)) {
      throw new Error(`Expected ${status}, got ${String(error?.status)} ${String(error?.message || "")}`);
    }
    if (includes && !String(error?.message || "").includes(includes)) {
      throw new Error(`Expected message containing "${includes}", got "${String(error?.message || "")}"`);
    }
    return;
  }
  throw new Error("Expected operation to fail, but it succeeded");
}

async function setupFixture(stamp) {
  await seedCore({ ensureDefaultTenantIfMissing: true });
  await query(`INSERT INTO tenants (code, name) VALUES (?, ?)`, [`PRH05_T_${stamp}`, `PRH05 Tenant ${stamp}`]);
  const tenantId = toInt((await query(`SELECT id FROM tenants WHERE code = ? LIMIT 1`, [`PRH05_T_${stamp}`])).rows?.[0]?.id);
  const country = (await query(`SELECT id, default_currency_code FROM countries WHERE iso2='TR' LIMIT 1`)).rows?.[0];
  const countryId = toInt(country?.id);
  const currencyCode = String(country?.default_currency_code || "TRY");

  await query(`INSERT INTO group_companies (tenant_id, code, name) VALUES (?, ?, ?)`, [tenantId, `PRH05_G_${stamp}`, `PRH05 Group ${stamp}`]);
  const groupCompanyId = toInt((await query(`SELECT id FROM group_companies WHERE tenant_id=? AND code=? LIMIT 1`, [tenantId, `PRH05_G_${stamp}`])).rows?.[0]?.id);
  const legalEntityCode = `PRH05_LE_${stamp}`;
  await query(
    `INSERT INTO legal_entities (tenant_id, group_company_id, code, name, country_id, functional_currency_code, status)
     VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, groupCompanyId, legalEntityCode, `PRH05 LE ${stamp}`, countryId, currencyCode]
  );
  const legalEntityId = toInt((await query(`SELECT id FROM legal_entities WHERE tenant_id=? AND code=? LIMIT 1`, [tenantId, legalEntityCode])).rows?.[0]?.id);

  const passwordHash = await bcrypt.hash("PRH05#Smoke123", 10);
  await query(`INSERT INTO users (tenant_id, email, password_hash, name, status) VALUES (?, ?, ?, ?, 'ACTIVE')`, [tenantId, `prh05_${stamp}@x.com`, passwordHash, "PRH05 User"]);
  const userId = toInt((await query(`SELECT id FROM users WHERE tenant_id=? AND email=? LIMIT 1`, [tenantId, `prh05_${stamp}@x.com`])).rows?.[0]?.id);

  await query(`INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name) VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`, [tenantId, legalEntityId, `PRH05_COA_${stamp}`, `PRH05 CoA ${stamp}`]);
  const coaId = toInt((await query(`SELECT id FROM charts_of_accounts WHERE tenant_id=? AND legal_entity_id=? AND code=? LIMIT 1`, [tenantId, legalEntityId, `PRH05_COA_${stamp}`])).rows?.[0]?.id);
  await query(`INSERT INTO accounts (coa_id, code, name, account_type, normal_side, allow_posting, is_active) VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, TRUE)`, [coaId, `PRH05_BANK_${stamp}`, `PRH05 Bank GL ${stamp}`]);
  await query(`INSERT INTO accounts (coa_id, code, name, account_type, normal_side, allow_posting, is_active) VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, TRUE)`, [coaId, `PRH05_PAY_${stamp}`, `PRH05 Pay GL ${stamp}`]);
  const accRows = (await query(`SELECT id, code FROM accounts WHERE coa_id=? AND code IN (?, ?)`, [coaId, `PRH05_BANK_${stamp}`, `PRH05_PAY_${stamp}`])).rows || [];
  const bankGl = toInt(accRows.find((r) => String(r.code) === `PRH05_BANK_${stamp}`)?.id);
  const payGl = toInt(accRows.find((r) => String(r.code) === `PRH05_PAY_${stamp}`)?.id);

  await query(
    `INSERT INTO bank_accounts (tenant_id, legal_entity_id, code, name, currency_code, gl_account_id, bank_name, branch_name, iban, account_no, is_active, created_by_user_id)
     VALUES (?, ?, ?, ?, ?, ?, 'PRH05 Bank', 'Main', ?, ?, TRUE, ?)`,
    [tenantId, legalEntityId, `PRH05_BA_${stamp}`, `PRH05 BA ${stamp}`, currencyCode, bankGl, `TR${String(stamp).slice(-20)}`, String(stamp), userId]
  );
  const bankAccountId = toInt((await query(`SELECT id FROM bank_accounts WHERE tenant_id=? AND legal_entity_id=? AND code=? LIMIT 1`, [tenantId, legalEntityId, `PRH05_BA_${stamp}`])).rows?.[0]?.id);

  // Bank statement + recon exceptions
  await query(
    `INSERT INTO bank_statement_imports (tenant_id, legal_entity_id, bank_account_id, original_filename, file_checksum, imported_by_user_id)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, bankAccountId, `prh05-${stamp}.csv`, sha256(`PRH05-IMPORT-${stamp}`), userId]
  );
  const importId = toInt((await query(`SELECT id FROM bank_statement_imports WHERE tenant_id=? AND legal_entity_id=? ORDER BY id DESC LIMIT 1`, [tenantId, legalEntityId])).rows?.[0]?.id);
  await query(
    `INSERT INTO bank_statement_lines (tenant_id, legal_entity_id, import_id, bank_account_id, line_no, txn_date, description, amount, currency_code, line_hash, recon_status)
     VALUES
     (?, ?, ?, ?, 1, DATE_SUB(CURDATE(), INTERVAL 1 DAY), 'L1', 120.55, ?, ?, 'UNMATCHED'),
     (?, ?, ?, ?, 2, DATE_SUB(CURDATE(), INTERVAL 5 DAY), 'L2', -80.10, ?, ?, 'MATCHED'),
     (?, ?, ?, ?, 3, DATE_SUB(CURDATE(), INTERVAL 10 DAY), 'L3', 42.00, ?, ?, 'PARTIAL')`,
    [
      tenantId, legalEntityId, importId, bankAccountId, currencyCode, sha256(`L1-${stamp}`),
      tenantId, legalEntityId, importId, bankAccountId, currencyCode, sha256(`L2-${stamp}`),
      tenantId, legalEntityId, importId, bankAccountId, currencyCode, sha256(`L3-${stamp}`),
    ]
  );
  const lines = (await query(`SELECT id, line_no FROM bank_statement_lines WHERE tenant_id=? AND legal_entity_id=? AND import_id=?`, [tenantId, legalEntityId, importId])).rows || [];
  const l1 = toInt(lines.find((r) => toInt(r.line_no) === 1)?.id);
  const l2 = toInt(lines.find((r) => toInt(r.line_no) === 2)?.id);
  const l3 = toInt(lines.find((r) => toInt(r.line_no) === 3)?.id);
  await query(
    `INSERT INTO bank_reconciliation_exceptions (tenant_id, legal_entity_id, statement_line_id, bank_account_id, reason_code, status, severity, first_seen_at, reason_message)
     VALUES
     (?, ?, ?, ?, 'RULE_MISS', 'OPEN', 'HIGH', DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 30 HOUR), 'O'),
     (?, ?, ?, ?, 'PARTIAL_DOC', 'ASSIGNED', 'MEDIUM', DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 100 HOUR), 'A'),
     (?, ?, ?, ?, 'IGNORE_DUP', 'RESOLVED', 'LOW', DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 HOUR), 'R')`,
    [tenantId, legalEntityId, l1, bankAccountId, tenantId, legalEntityId, l3, bankAccountId, tenantId, legalEntityId, l2, bankAccountId]
  );

  // Payment batches + lines + ack imports
  await query(
    `INSERT INTO payment_batches (tenant_id, legal_entity_id, batch_no, source_type, bank_account_id, currency_code, status, bank_export_status, bank_ack_status, created_by_user_id, exported_at)
     VALUES
     (?, ?, ?, 'MANUAL', ?, ?, 'APPROVED', 'NOT_EXPORTED', 'NOT_ACKED', ?, NULL),
     (?, ?, ?, 'MANUAL', ?, ?, 'EXPORTED', 'EXPORTED', 'NOT_ACKED', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 30 HOUR)),
     (?, ?, ?, 'MANUAL', ?, ?, 'FAILED', 'FAILED', 'FAILED', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 6 HOUR))`,
    [
      tenantId, legalEntityId, `PRH05_BA_${stamp}`, bankAccountId, currencyCode, userId,
      tenantId, legalEntityId, `PRH05_BB_${stamp}`, bankAccountId, currencyCode, userId,
      tenantId, legalEntityId, `PRH05_BC_${stamp}`, bankAccountId, currencyCode, userId,
    ]
  );
  const batches = (await query(`SELECT id, batch_no FROM payment_batches WHERE tenant_id=? AND legal_entity_id=?`, [tenantId, legalEntityId])).rows || [];
  const b1 = toInt(batches.find((r) => String(r.batch_no) === `PRH05_BA_${stamp}`)?.id);
  const b2 = toInt(batches.find((r) => String(r.batch_no) === `PRH05_BB_${stamp}`)?.id);
  const b3 = toInt(batches.find((r) => String(r.batch_no) === `PRH05_BC_${stamp}`)?.id);
  await query(
    `INSERT INTO payment_batch_lines (tenant_id, legal_entity_id, batch_id, line_no, beneficiary_type, beneficiary_name, payable_entity_type, payable_gl_account_id, amount, bank_execution_status, ack_status)
     VALUES
     (?, ?, ?, 1, 'VENDOR', 'Ben1', 'MANUAL', ?, 150.25, 'PENDING', 'NONE'),
     (?, ?, ?, 1, 'VENDOR', 'Ben2', 'MANUAL', ?, 300.75, 'EXECUTED', 'ACCEPTED'),
     (?, ?, ?, 1, 'VENDOR', 'Ben3', 'MANUAL', ?, 90.40, 'FAILED', 'REJECTED')`,
    [tenantId, legalEntityId, b1, payGl, tenantId, legalEntityId, b2, payGl, tenantId, legalEntityId, b3, payGl]
  );
  await query(
    `INSERT INTO payment_batch_ack_imports (tenant_id, legal_entity_id, batch_id, file_format_code, status, created_by_user_id)
     VALUES
     (?, ?, ?, 'GENERIC_CSV_V1', 'PROCESSED', ?),
     (?, ?, ?, 'GENERIC_CSV_V1', 'FAILED', ?)`,
    [tenantId, legalEntityId, b2, userId, tenantId, legalEntityId, b3, userId]
  );

  // Payroll provider imports + run
  await query(
    `INSERT INTO payroll_provider_connections (tenant_id, legal_entity_id, provider_code, provider_name, status, is_default, created_by_user_id, updated_by_user_id)
     VALUES (?, ?, 'GENERIC_JSON', ?, 'ACTIVE', 1, ?, ?)`,
    [tenantId, legalEntityId, `PRH05 Provider ${stamp}`, userId, userId]
  );
  const connId = toInt((await query(`SELECT id FROM payroll_provider_connections WHERE tenant_id=? AND legal_entity_id=? ORDER BY id DESC LIMIT 1`, [tenantId, legalEntityId])).rows?.[0]?.id);
  await query(
    `INSERT INTO payroll_provider_import_jobs (
        tenant_id, legal_entity_id, payroll_provider_connection_id, provider_code, adapter_version, payroll_period, period_start, period_end, pay_date, currency_code,
        import_key, raw_payload_hash, normalized_payload_hash, source_format, source_filename, status, preview_summary_json, validation_errors_json, match_errors_json, match_warnings_json, raw_payload_text, normalized_payload_json, requested_by_user_id, requested_at, applied_at
      ) VALUES
      (?, ?, ?, 'GENERIC_JSON', 'v1', '2026-02-01','2026-02-01','2026-02-01','2026-02-01', ?, ?, ?, ?, 'JSON', ?, 'PREVIEWED', '{}', '[]', '[]', '[]', '{}', '{}', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 HOUR), NULL),
      (?, ?, ?, 'GENERIC_JSON', 'v1', '2026-03-01','2026-03-01','2026-03-01','2026-03-01', ?, ?, ?, ?, 'JSON', ?, 'FAILED', '{}', '[]', '[]', '[]', '{}', '{}', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 30 HOUR), NULL),
      (?, ?, ?, 'GENERIC_JSON', 'v1', '2026-01-01','2026-01-01','2026-01-01','2026-01-01', ?, ?, ?, ?, 'JSON', ?, 'APPLIED', '{}', '[]', '[]', '[]', '{}', '{}', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 6 HOUR), DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 5 HOUR)),
      (?, ?, ?, 'GENERIC_JSON', 'v1', '2026-04-01','2026-04-01','2026-04-01','2026-04-01', ?, ?, ?, ?, 'JSON', ?, 'APPLYING', '{}', '[]', '[]', '[]', '{}', '{}', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 40 MINUTE), NULL)`,
    [
      tenantId, legalEntityId, connId, currencyCode, `PRH05_I1_${stamp}`, sha256(`i1-${stamp}`), sha256(`n1-${stamp}`), `i1-${stamp}.json`, userId,
      tenantId, legalEntityId, connId, currencyCode, `PRH05_I2_${stamp}`, sha256(`i2-${stamp}`), sha256(`n2-${stamp}`), `i2-${stamp}.json`, userId,
      tenantId, legalEntityId, connId, currencyCode, `PRH05_I3_${stamp}`, sha256(`i3-${stamp}`), sha256(`n3-${stamp}`), `i3-${stamp}.json`, userId,
      tenantId, legalEntityId, connId, currencyCode, `PRH05_I4_${stamp}`, sha256(`i4-${stamp}`), sha256(`n4-${stamp}`), `i4-${stamp}.json`, userId,
    ]
  );
  const appliedImportId = toInt((await query(`SELECT id FROM payroll_provider_import_jobs WHERE tenant_id=? AND legal_entity_id=? AND import_key=? LIMIT 1`, [tenantId, legalEntityId, `PRH05_I3_${stamp}`])).rows?.[0]?.id);
  await query(
    `INSERT INTO payroll_runs (
        tenant_id, legal_entity_id, run_no, provider_code, entity_code, payroll_period, pay_date, currency_code, source_batch_ref, original_filename, file_checksum, status, source_type, source_provider_code, source_provider_import_job_id, imported_by_user_id, imported_at
      ) VALUES (?, ?, ?, 'GENERIC_JSON', ?, '2026-01-01', '2026-01-31', ?, ?, ?, ?, 'IMPORTED', 'PROVIDER_IMPORT', 'GENERIC_JSON', ?, ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 4 HOUR))`,
    [tenantId, legalEntityId, `PRH05_RUN_${stamp}`, legalEntityCode, currencyCode, `SRC-${stamp}`, `run-${stamp}.csv`, sha256(`run-${stamp}`), appliedImportId, userId]
  );
  await query(`UPDATE payroll_provider_import_jobs SET applied_payroll_run_id=(SELECT id FROM payroll_runs WHERE tenant_id=? AND legal_entity_id=? AND run_no=? LIMIT 1) WHERE tenant_id=? AND legal_entity_id=? AND id=?`, [tenantId, legalEntityId, `PRH05_RUN_${stamp}`, tenantId, legalEntityId, appliedImportId]);

  // Payroll close controls
  await query(
    `INSERT INTO payroll_period_closes (tenant_id, legal_entity_id, period_start, period_end, status, failed_checks, warning_checks, requested_at, requested_by_user_id, closed_at, closed_by_user_id)
     VALUES
     (?, ?, DATE_SUB(CURDATE(), INTERVAL 60 DAY), DATE_SUB(CURDATE(), INTERVAL 31 DAY), 'DRAFT', 1, 0, NULL, NULL, NULL, NULL),
     (?, ?, DATE_SUB(CURDATE(), INTERVAL 30 DAY), DATE_SUB(CURDATE(), INTERVAL 1 DAY), 'CLOSED', 0, 0, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 48 HOUR), ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 24 HOUR), ?),
     (?, ?, DATE_SUB(CURDATE(), INTERVAL 15 DAY), DATE_ADD(CURDATE(), INTERVAL 14 DAY), 'REQUESTED', 2, 1, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 30 HOUR), ?, NULL, NULL)`,
    [tenantId, legalEntityId, tenantId, legalEntityId, userId, userId, tenantId, legalEntityId, userId]
  );
  const closeRows = (await query(`SELECT id, status FROM payroll_period_closes WHERE tenant_id=? AND legal_entity_id=?`, [tenantId, legalEntityId])).rows || [];
  const draftId = toInt(closeRows.find((r) => String(r.status) === "DRAFT")?.id);
  const reqId = toInt(closeRows.find((r) => String(r.status) === "REQUESTED")?.id);
  await query(
    `INSERT INTO payroll_period_close_checks (tenant_id, legal_entity_id, payroll_period_close_id, check_code, check_name, severity, status, sort_order)
     VALUES
     (?, ?, ?, 'RUNS_NO_NON_FINALIZED', 'No non-finalized', 'ERROR', 'FAIL', 10),
     (?, ?, ?, 'OFF_CYCLE_DRAFTS_WARN', 'Off-cycle warn', 'WARN', 'WARN', 20),
     (?, ?, ?, 'RUN_COUNT_INFO', 'Info', 'INFO', 'PASS', 30)`,
    [tenantId, legalEntityId, reqId, tenantId, legalEntityId, reqId, tenantId, legalEntityId, draftId]
  );

  // Jobs + attempts
  await query(
    `INSERT INTO app_jobs (tenant_id, queue_name, module_code, job_type, status, priority, run_after_at, idempotency_key, payload_json, payload_hash, max_attempts, created_by, created_at, started_at)
     VALUES
     (?, 'ops.default', 'OPS', 'PRH05_QUEUED', 'QUEUED', 100, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 20 MINUTE), ?, '{}', ?, 5, ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 10 MINUTE), NULL),
     (?, 'ops.default', 'OPS', 'PRH05_RETRY_DUE', 'FAILED_RETRYABLE', 90, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 40 MINUTE), ?, '{}', ?, 5, ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 30 MINUTE), NULL),
     (?, 'ops.default', 'OPS', 'PRH05_RETRY_DELAY', 'FAILED_RETRYABLE', 90, DATE_ADD(CURRENT_TIMESTAMP, INTERVAL 60 MINUTE), ?, '{}', ?, 5, ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 15 MINUTE), NULL),
     (?, 'payroll.imports', 'PAYROLL', 'PRH05_RUNNING', 'RUNNING', 50, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 5 MINUTE), ?, '{}', ?, 5, ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 HOUR), DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 HOUR)),
     (?, 'ops.default', 'OPS', 'PRH05_FAILED_FINAL', 'FAILED_FINAL', 100, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 50 MINUTE), ?, '{}', ?, 3, ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 HOUR), NULL),
     (?, 'ops.default', 'OPS', 'PRH05_SUCCEEDED', 'SUCCEEDED', 100, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 80 MINUTE), ?, '{}', ?, 3, ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 70 MINUTE), DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 65 MINUTE))`,
    [
      tenantId, `j1-${stamp}`, sha256(`j1-${stamp}`), userId,
      tenantId, `j2-${stamp}`, sha256(`j2-${stamp}`), userId,
      tenantId, `j3-${stamp}`, sha256(`j3-${stamp}`), userId,
      tenantId, `j4-${stamp}`, sha256(`j4-${stamp}`), userId,
      tenantId, `j5-${stamp}`, sha256(`j5-${stamp}`), userId,
      tenantId, `j6-${stamp}`, sha256(`j6-${stamp}`), userId,
    ]
  );
  const jobs = (await query(`SELECT id, job_type FROM app_jobs WHERE tenant_id=? AND job_type IN ('PRH05_RUNNING','PRH05_SUCCEEDED','PRH05_FAILED_FINAL')`, [tenantId])).rows || [];
  const jr = toInt(jobs.find((r) => String(r.job_type) === "PRH05_RUNNING")?.id);
  const js = toInt(jobs.find((r) => String(r.job_type) === "PRH05_SUCCEEDED")?.id);
  const jf = toInt(jobs.find((r) => String(r.job_type) === "PRH05_FAILED_FINAL")?.id);
  await query(
    `INSERT INTO app_job_attempts (tenant_id, app_job_id, attempt_no, worker_id, status, started_at, finished_at)
     VALUES
     (?, ?, 1, 'w', 'RUNNING', DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 30 MINUTE), NULL),
     (?, ?, 1, 'w', 'SUCCEEDED', DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 80 MINUTE), DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 75 MINUTE)),
     (?, ?, 1, 'w', 'FAILED_FINAL', DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 HOUR), DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 110 MINUTE))`,
    [tenantId, jr, tenantId, js, tenantId, jf]
  );

  return { tenantId, legalEntityId, bankAccountId };
}

async function main() {
  const stamp = Date.now();
  const fx = await setupFixture(stamp);

  const bankRecon = await getOpsBankReconciliationSummary({
    req: null,
    tenantId: fx.tenantId,
    filters: { legalEntityId: fx.legalEntityId, bankAccountId: fx.bankAccountId, days: 30 },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(toInt(bankRecon.statement_lines.total_in_window) === 3, "bank recon lines total");
  assert(toInt(bankRecon.sla.unmatched_open_total) === 2, "bank recon unmatched total");
  assert(toInt(bankRecon.exceptions.by_status.OPEN) === 1, "bank recon open exceptions");

  const bankBatches = await getOpsBankPaymentBatchesHealth({
    req: null,
    tenantId: fx.tenantId,
    filters: { legalEntityId: fx.legalEntityId, bankAccountId: fx.bankAccountId, days: 30 },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(toInt(bankBatches.batches.total_in_window) === 3, "batch total");
  assert(toInt(bankBatches.sla.pending_export_batches) === 2, "pending export");
  assert(toInt(bankBatches.sla.awaiting_ack_gt_24h) === 1, "awaiting ack gt 24h");

  const payrollImport = await getOpsPayrollImportHealth({
    req: null,
    tenantId: fx.tenantId,
    filters: { legalEntityId: fx.legalEntityId, providerCode: "GENERIC_JSON", days: 30 },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(toInt(payrollImport.provider_import_jobs.total_in_window) === 4, "provider import jobs total");
  assert(toInt(payrollImport.sla.previewed_jobs) === 1, "previewed jobs");
  assert(toInt(payrollImport.sla.failed_jobs) === 1, "failed jobs");
  assert(toInt(payrollImport.payroll_runs_from_provider_imports.by_status.IMPORTED) === 1, "provider imported runs");

  const payrollClose = await getOpsPayrollCloseStatus({
    req: null,
    tenantId: fx.tenantId,
    filters: { legalEntityId: fx.legalEntityId, days: 365 },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(toInt(payrollClose.periods.total_in_window) === 3, "close periods total");
  assert(toInt(payrollClose.periods.closed_periods) === 1, "closed periods");
  assert(toInt(payrollClose.sla.requested_gt_24h) === 1, "requested gt 24h");
  assert(toInt(payrollClose.checks.by_status.FAIL) >= 1, "close checks fail bucket");

  const jobs = await getOpsJobsHealth({ tenantId: fx.tenantId, filters: { days: 30 } });
  assert(toInt(jobs.jobs.total_in_window) === 6, "jobs total");
  assert(toInt(jobs.sla.queued_due_now) === 1, "jobs queued due");
  assert(toInt(jobs.sla.retries_due_now) === 1, "jobs retries due");
  assert(toInt(jobs.sla.running_gt_60m) === 1, "jobs running >60m");
  assert(toInt(jobs.attempts.total_in_window) === 3, "job attempts total");

  await expectFailure(
    () =>
      getOpsBankReconciliationSummary({
        req: null,
        tenantId: fx.tenantId,
        filters: { legalEntityId: fx.legalEntityId, bankAccountId: fx.bankAccountId, days: 30 },
        buildScopeFilter: allowAllScopeFilter,
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );
  await expectFailure(
    () =>
      getOpsPayrollImportHealth({
        req: null,
        tenantId: fx.tenantId,
        filters: { legalEntityId: fx.legalEntityId, days: 30 },
        buildScopeFilter: allowAllScopeFilter,
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  console.log("PR-H05 smoke test passed (ops dashboard bank/payroll/jobs KPIs + scope checks).");
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
