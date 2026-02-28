import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  finalizePayrollRunAccrual,
  getPayrollRunAccrualPreview,
  markPayrollRunReviewed,
} from "../src/services/payroll.accruals.service.js";
import { createPayrollEmployeeBeneficiaryBankAccount } from "../src/services/payroll.beneficiaries.service.js";
import {
  buildPayrollRunLiabilities,
  createPayrollRunPaymentBatchFromLiabilities,
  getPayrollRunLiabilitiesDetail,
} from "../src/services/payroll.liabilities.service.js";
import { upsertPayrollComponentMapping } from "../src/services/payroll.mappings.service.js";
import { applyPayrollRunPaymentSync, getPayrollRunPaymentSyncPreview } from "../src/services/payroll.paymentSync.service.js";
import { cancelPaymentBatch } from "../src/services/payments.service.js";
import { importPayrollRunCsv } from "../src/services/payroll.runs.service.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function toAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Number(parsed.toFixed(6));
}

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function noScopeGuard() {
  return true;
}

function denyScopeGuard() {
  const err = new Error("Scope access denied");
  err.status = 403;
  throw err;
}

async function expectFailure(work, { status, code, includes }) {
  try {
    await work();
  } catch (error) {
    if (status !== undefined && Number(error?.status || 0) !== Number(status)) {
      throw new Error(
        `Expected error status ${status} but got ${String(error?.status)} message=${String(
          error?.message || ""
        )}`
      );
    }
    if (code !== undefined && String(error?.code || "") !== String(code)) {
      throw new Error(`Expected error code ${code} but got ${String(error?.code || "")}`);
    }
    if (includes && !String(error?.message || "").includes(includes)) {
      throw new Error(
        `Expected error message to include "${includes}" but got "${String(error?.message || "")}"`
      );
    }
    return;
  }
  throw new Error("Expected operation to fail, but it succeeded");
}

function buildValidCsv() {
  return [
    "employee_code,employee_name,cost_center_code,base_salary,overtime_pay,bonus_pay,allowances_total,gross_pay,employee_tax,employee_social_security,other_deductions,employer_tax,employer_social_security,net_pay",
    "E001,Alpha User,CC-01,1000,100,50,50,1200,120,80,20,150,100,980",
    "E002,Beta User,CC-02,900,0,0,100,1000,100,50,10,120,90,840",
  ].join("\n");
}

async function createTenantWithP04Fixtures(stamp) {
  const tenantCode = `PRP04_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRP04 Tenant ${stamp}`]
  );
  const tenantRows = await query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [tenantCode]
  );
  const tenantId = toNumber(tenantRows.rows?.[0]?.id);
  assert(tenantId > 0, "Failed to create tenant fixture");

  const countryRows = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'TR'
     LIMIT 1`
  );
  const countryId = toNumber(countryRows.rows?.[0]?.id);
  const currencyCode = String(countryRows.rows?.[0]?.default_currency_code || "TRY");
  assert(countryId > 0, "Missing country seed row (TR)");

  await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `PRP04_G_${stamp}`, `PRP04 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP04_G_${stamp}`]
  );
  const groupCompanyId = toNumber(groupRows.rows?.[0]?.id);
  assert(groupCompanyId > 0, "Failed to create group company fixture");

  await query(
    `INSERT INTO legal_entities (
        tenant_id,
        group_company_id,
        code,
        name,
        country_id,
        functional_currency_code,
        status
      )
      VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      groupCompanyId,
      `PRP04_LE_${stamp}`,
      `PRP04 Legal Entity ${stamp}`,
      countryId,
      currencyCode,
    ]
  );
  const legalEntityRows = await query(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP04_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
      )
      VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `PRP04_CAL_${stamp}`, `PRP04 Calendar ${stamp}`]
  );
  const calendarRows = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP04_CAL_${stamp}`]
  );
  const calendarId = toNumber(calendarRows.rows?.[0]?.id);
  assert(calendarId > 0, "Failed to create fiscal calendar fixture");

  await query(
    `INSERT INTO fiscal_periods (
        calendar_id, fiscal_year, period_no, period_name, start_date, end_date, is_adjustment
      )
      VALUES (?, 2026, 2, '2026-02', '2026-02-01', '2026-02-28', FALSE)`,
    [calendarId]
  );

  await query(
    `INSERT INTO books (
        tenant_id, legal_entity_id, calendar_id, code, name, book_type, base_currency_code
      )
      VALUES (?, ?, ?, ?, ?, 'LOCAL', ?)`,
    [
      tenantId,
      legalEntityId,
      calendarId,
      `PRP04_BOOK_${stamp}`,
      `PRP04 Book ${stamp}`,
      currencyCode,
    ]
  );

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `PRP04_COA_${stamp}`, `PRP04 Chart ${stamp}`]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP04_COA_${stamp}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts fixture");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP04BANK${stamp}`, `PRP04 Bank GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'EXPENSE', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP04EXP${stamp}`, `PRP04 Expense GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP04LIA${stamp}`, `PRP04 Liability GL ${stamp}`]
  );

  const bankGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP04 Bank GL ${stamp}`]
  );
  const expenseRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP04 Expense GL ${stamp}`]
  );
  const liabilityRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP04 Liability GL ${stamp}`]
  );
  const bankGlAccountId = toNumber(bankGlRows.rows?.[0]?.id);
  const expenseGlAccountId = toNumber(expenseRows.rows?.[0]?.id);
  const liabilityGlAccountId = toNumber(liabilityRows.rows?.[0]?.id);
  assert(bankGlAccountId > 0, "Failed to create bank GL fixture");
  assert(expenseGlAccountId > 0, "Failed to create expense GL fixture");
  assert(liabilityGlAccountId > 0, "Failed to create liability GL fixture");

  const passwordHash = await bcrypt.hash("PRP04#Smoke123", 10);
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, `prp04_user_${stamp}@example.com`, passwordHash, "PRP04 User"]
  );
  const userRows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, `prp04_user_${stamp}@example.com`]
  );
  const userId = toNumber(userRows.rows?.[0]?.id);
  assert(userId > 0, "Failed to create user fixture");

  await query(
    `INSERT INTO bank_accounts (
        tenant_id,
        legal_entity_id,
        code,
        name,
        currency_code,
        gl_account_id,
        bank_name,
        branch_name,
        iban,
        account_no,
        is_active,
        created_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, ?)`,
    [
      tenantId,
      legalEntityId,
      `PRP04_BA_${stamp}`,
      `PRP04 Bank Account ${stamp}`,
      currencyCode,
      bankGlAccountId,
      "Smoke Bank",
      "Main",
      `TR${String(stamp).slice(-20)}`,
      String(stamp),
      userId,
    ]
  );
  const bankAccountRows = await query(
    `SELECT id
     FROM bank_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, `PRP04_BA_${stamp}`]
  );
  const bankAccountId = toNumber(bankAccountRows.rows?.[0]?.id);
  assert(bankAccountId > 0, "Failed to create bank account fixture");

  return {
    tenantId,
    legalEntityId,
    userId,
    currencyCode,
    bankAccountId,
    expenseGlAccountId,
    liabilityGlAccountId,
  };
}

async function ensureEmployeeBeneficiaryAccounts({ fixture, stamp }) {
  await createPayrollEmployeeBeneficiaryBankAccount({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    input: {
      legalEntityId: fixture.legalEntityId,
      employeeCode: "E001",
      employeeName: "Alpha User",
      accountHolderName: "Alpha User",
      bankName: "Smoke Bank",
      bankBranchName: "Main",
      countryCode: "TR",
      currencyCode: fixture.currencyCode,
      iban: `TR00E001${String(stamp).slice(-8)}`,
      accountNumber: null,
      routingNumber: null,
      swiftBic: null,
      isPrimary: true,
      effectiveFrom: "2026-01-01",
      effectiveTo: null,
      verificationStatus: "VERIFIED",
      sourceType: "MANUAL",
      externalRef: null,
      reason: "PRP04 beneficiary setup",
    },
    assertScopeAccess: noScopeGuard,
  });

  await createPayrollEmployeeBeneficiaryBankAccount({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    input: {
      legalEntityId: fixture.legalEntityId,
      employeeCode: "E002",
      employeeName: "Beta User",
      accountHolderName: "Beta User",
      bankName: "Smoke Bank",
      bankBranchName: "Main",
      countryCode: "TR",
      currencyCode: fixture.currencyCode,
      iban: `TR00E002${String(stamp).slice(-8)}`,
      accountNumber: null,
      routingNumber: null,
      swiftBic: null,
      isPrimary: true,
      effectiveFrom: "2026-01-01",
      effectiveTo: null,
      verificationStatus: "VERIFIED",
      sourceType: "MANUAL",
      externalRef: null,
      reason: "PRP04 beneficiary setup",
    },
    assertScopeAccess: noScopeGuard,
  });
}

async function createPreparedNetPayrollBatch({
  fixture,
  providerCode,
  sourceRef,
  idempotencyKey,
}) {
  const imported = await importPayrollRunCsv({
    req: null,
    payload: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      legalEntityId: fixture.legalEntityId,
      providerCode,
      payrollPeriod: "2026-02-01",
      payDate: "2026-02-15",
      currencyCode: fixture.currencyCode,
      sourceBatchRef: sourceRef,
      originalFilename: `${sourceRef}.csv`,
      csvText: buildValidCsv(),
    },
    assertScopeAccess: noScopeGuard,
  });
  const runId = toNumber(imported?.id);
  assert(runId > 0, "Payroll run import should return run id");

  const accrualPreview = await getPayrollRunAccrualPreview({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    assertScopeAccess: noScopeGuard,
  });
  assert((accrualPreview?.component_totals || []).length > 0, "Accrual preview should have components");

  for (const component of accrualPreview.component_totals || []) {
    const entrySide = normalizeUpperText(component?.entry_side);
    await upsertPayrollComponentMapping({
      req: null,
      payload: {
        tenantId: fixture.tenantId,
        userId: fixture.userId,
        legalEntityId: fixture.legalEntityId,
        entityCodeInput: null,
        providerCode,
        currencyCode: fixture.currencyCode,
        componentCode: normalizeUpperText(component?.component_code),
        entrySide,
        glAccountId:
          entrySide === "DEBIT" ? fixture.expenseGlAccountId : fixture.liabilityGlAccountId,
        effectiveFrom: "2026-01-01",
        effectiveTo: null,
        closePreviousOpenMapping: true,
        notes: "PRP04 mapping setup",
      },
      assertScopeAccess: noScopeGuard,
    });
  }

  await markPayrollRunReviewed({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "review for PRP04",
    assertScopeAccess: noScopeGuard,
  });

  const finalized = await finalizePayrollRunAccrual({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "finalize for PRP04",
    forceFromImported: false,
    assertScopeAccess: noScopeGuard,
  });
  assert(toNumber(finalized?.accrualJournalEntryId) > 0, "Finalize should produce accrual journal");

  const built = await buildPayrollRunLiabilities({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "build liabilities for PRP04",
    assertScopeAccess: noScopeGuard,
  });
  assert((built?.items || []).length === 7, "Liability build should create 7 liabilities");

  const prepared = await createPayrollRunPaymentBatchFromLiabilities({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    input: {
      scope: "NET_PAY",
      bankAccountId: fixture.bankAccountId,
      idempotencyKey,
      notes: "PRP04 NET payment prep",
    },
    assertScopeAccess: noScopeGuard,
  });
  const batchId = toNumber(prepared?.batch?.id);
  assert(batchId > 0, "Payment batch should be created");
  assert((prepared?.batch?.lines || []).length === 2, "NET scope should prepare 2 lines");

  return {
    runId,
    batchId,
    batchTotal: toAmount(prepared?.batch?.total_amount),
  };
}

async function createBankReconciliationEvidenceForBatch({
  fixture,
  batchId,
  matchedAmount,
  stamp,
}) {
  const checksum = `prp04-sync-${stamp}`.padEnd(64, "0").slice(0, 64);
  await query(
    `INSERT INTO bank_statement_imports (
        tenant_id,
        legal_entity_id,
        bank_account_id,
        import_source,
        original_filename,
        file_checksum,
        status,
        line_count_total,
        line_count_inserted,
        line_count_duplicates,
        raw_meta_json,
        imported_by_user_id
      )
      VALUES (?, ?, ?, 'CSV', ?, ?, 'IMPORTED', 1, 1, 0, ?, ?)`,
    [
      fixture.tenantId,
      fixture.legalEntityId,
      fixture.bankAccountId,
      `prp04-sync-${stamp}.csv`,
      checksum,
      JSON.stringify({ source: "prp04-smoke" }),
      fixture.userId,
    ]
  );
  const importRows = await query(
    `SELECT id
     FROM bank_statement_imports
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND bank_account_id = ?
       AND file_checksum = ?
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, fixture.bankAccountId, checksum]
  );
  const importId = toNumber(importRows.rows?.[0]?.id);
  assert(importId > 0, "Statement import should exist");

  const lineHash = `PRP04-LINE-${stamp}`.padEnd(64, "0").slice(0, 64);
  await query(
    `INSERT INTO bank_statement_lines (
        tenant_id,
        legal_entity_id,
        import_id,
        bank_account_id,
        line_no,
        txn_date,
        value_date,
        description,
        reference_no,
        amount,
        currency_code,
        balance_after,
        line_hash,
        recon_status,
        raw_row_json
      )
      VALUES (?, ?, ?, ?, 1, '2026-02-20', '2026-02-20', ?, ?, ?, ?, NULL, ?, 'UNMATCHED', ?)`,
    [
      fixture.tenantId,
      fixture.legalEntityId,
      importId,
      fixture.bankAccountId,
      `PRP04 settlement for batch ${batchId}`,
      `PRP04-B${batchId}`,
      toAmount(matchedAmount),
      fixture.currencyCode,
      lineHash,
      JSON.stringify({ batchId }),
    ]
  );
  const lineRows = await query(
    `SELECT id
     FROM bank_statement_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND import_id = ?
       AND line_no = 1
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, importId]
  );
  const statementLineId = toNumber(lineRows.rows?.[0]?.id);
  assert(statementLineId > 0, "Statement line should exist");

  await query(
    `INSERT INTO bank_reconciliation_matches (
        tenant_id,
        legal_entity_id,
        statement_line_id,
        match_type,
        matched_entity_type,
        matched_entity_id,
        matched_amount,
        status,
        notes,
        matched_by_user_id
      )
      VALUES (?, ?, ?, 'MANUAL', 'PAYMENT_BATCH', ?, ?, 'ACTIVE', ?, ?)`,
    [
      fixture.tenantId,
      fixture.legalEntityId,
      statementLineId,
      batchId,
      toAmount(matchedAmount),
      "PRP04 smoke bank evidence",
      fixture.userId,
    ]
  );

  return { statementLineId };
}

async function countRunSettlementRows(tenantId, legalEntityId, runId) {
  const rows = await query(
    `SELECT COUNT(*) AS total
     FROM payroll_liability_settlements
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND run_id = ?`,
    [tenantId, legalEntityId, runId]
  );
  return toNumber(rows.rows?.[0]?.total);
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithP04Fixtures(stamp);
  await ensureEmployeeBeneficiaryAccounts({ fixture, stamp });

  const run1 = await createPreparedNetPayrollBatch({
    fixture,
    providerCode: `PRP04_A_${stamp}`,
    sourceRef: `PRP04A-${stamp}`,
    idempotencyKey: `PRP04-BATCH-A-${stamp}`,
  });
  await createBankReconciliationEvidenceForBatch({
    fixture,
    batchId: run1.batchId,
    matchedAmount: run1.batchTotal,
    stamp: `${stamp}-A`,
  });

  const preview1 = await getPayrollRunPaymentSyncPreview({
    req: null,
    tenantId: fixture.tenantId,
    runId: run1.runId,
    scope: "NET_PAY",
    allowB04OnlySettlement: false,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(preview1?.summary?.mark_paid_count) === 2,
    "Preview should classify 2 NET liabilities as MARK_PAID"
  );
  assert(
    toAmount(preview1?.summary?.mark_paid_amount) === 1820,
    "Preview MARK_PAID amount should equal 1820"
  );

  const apply1 = await applyPayrollRunPaymentSync({
    req: null,
    tenantId: fixture.tenantId,
    runId: run1.runId,
    userId: fixture.userId,
    scope: "NET_PAY",
    allowB04OnlySettlement: false,
    note: "apply sync with B03 evidence",
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(apply1?.applied?.mark_paid_count) === 2,
    "Apply should mark 2 NET liabilities as PAID"
  );
  assert(
    toAmount(apply1?.applied?.mark_paid_amount) === 1820,
    "Apply MARK_PAID amount should equal 1820"
  );
  assert(
    toNumber(apply1?.applied?.mark_partial_count) === 0 &&
      toNumber(apply1?.applied?.release_count) === 0,
    "Apply should not produce partial/release for run1"
  );

  const run1Detail = await getPayrollRunLiabilitiesDetail({
    req: null,
    tenantId: fixture.tenantId,
    runId: run1.runId,
    assertScopeAccess: noScopeGuard,
  });
  const run1NetLiabilities = (run1Detail.items || []).filter(
    (row) => normalizeUpperText(row.liability_group) === "EMPLOYEE_NET"
  );
  assert(run1NetLiabilities.length === 2, "Run1 should have 2 NET liabilities");
  assert(
    run1NetLiabilities.every((row) => normalizeUpperText(row.status) === "PAID"),
    "Run1 NET liabilities should be PAID after sync apply"
  );
  assert(
    run1NetLiabilities.every((row) => toAmount(row.outstanding_amount) === 0),
    "Run1 NET liabilities outstanding should be zero"
  );
  assert(
    run1NetLiabilities.every((row) => normalizeUpperText(row.beneficiary_snapshot_status) === "CAPTURED"),
    "Run1 NET links should keep CAPTURED beneficiary snapshot status"
  );

  const run1SettleCountBeforeReplay = await countRunSettlementRows(
    fixture.tenantId,
    fixture.legalEntityId,
    run1.runId
  );
  assert(run1SettleCountBeforeReplay === 2, "Run1 should have 2 settlement rows after first apply");

  const apply1Replay = await applyPayrollRunPaymentSync({
    req: null,
    tenantId: fixture.tenantId,
    runId: run1.runId,
    userId: fixture.userId,
    scope: "NET_PAY",
    allowB04OnlySettlement: false,
    note: "idempotent re-apply",
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(apply1Replay?.applied?.mark_paid_count) === 0 &&
      toNumber(apply1Replay?.applied?.mark_partial_count) === 0 &&
      toNumber(apply1Replay?.applied?.release_count) === 0,
    "Re-apply should be idempotent with no new operations"
  );
  const run1SettleCountAfterReplay = await countRunSettlementRows(
    fixture.tenantId,
    fixture.legalEntityId,
    run1.runId
  );
  assert(
    run1SettleCountAfterReplay === run1SettleCountBeforeReplay,
    "Re-apply should not create duplicate settlement rows"
  );

  const run2 = await createPreparedNetPayrollBatch({
    fixture,
    providerCode: `PRP04_B_${stamp}`,
    sourceRef: `PRP04B-${stamp}`,
    idempotencyKey: `PRP04-BATCH-B-${stamp}`,
  });

  await cancelPaymentBatch({
    req: null,
    tenantId: fixture.tenantId,
    batchId: run2.batchId,
    userId: fixture.userId,
    cancelInput: { reason: "PRP04 cancellation release test" },
    assertScopeAccess: noScopeGuard,
  });

  const preview2 = await getPayrollRunPaymentSyncPreview({
    req: null,
    tenantId: fixture.tenantId,
    runId: run2.runId,
    scope: "NET_PAY",
    allowB04OnlySettlement: false,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(preview2?.summary?.release_count) === 2,
    "Cancelled batch preview should classify 2 liabilities as RELEASE_TO_OPEN"
  );
  assert(
    toAmount(preview2?.summary?.release_amount) === 1820,
    "Release amount should equal 1820"
  );

  const apply2 = await applyPayrollRunPaymentSync({
    req: null,
    tenantId: fixture.tenantId,
    runId: run2.runId,
    userId: fixture.userId,
    scope: "NET_PAY",
    allowB04OnlySettlement: false,
    note: "release to open from cancelled batch",
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(apply2?.applied?.release_count) === 2,
    "Apply should release 2 liabilities back to OPEN"
  );
  assert(
    toAmount(apply2?.applied?.release_amount) === 1820,
    "Apply release amount should equal 1820"
  );

  const run2Detail = await getPayrollRunLiabilitiesDetail({
    req: null,
    tenantId: fixture.tenantId,
    runId: run2.runId,
    assertScopeAccess: noScopeGuard,
  });
  const run2NetLiabilities = (run2Detail.items || []).filter(
    (row) => normalizeUpperText(row.liability_group) === "EMPLOYEE_NET"
  );
  assert(run2NetLiabilities.length === 2, "Run2 should have 2 NET liabilities");
  assert(
    run2NetLiabilities.every((row) => normalizeUpperText(row.status) === "OPEN"),
    "Run2 NET liabilities should return to OPEN after release apply"
  );
  assert(
    run2NetLiabilities.every((row) => normalizeUpperText(row.beneficiary_snapshot_status) === "CAPTURED"),
    "Run2 NET liabilities should keep captured snapshots after release"
  );

  const run2LinkRows = await query(
    `SELECT COUNT(*) AS total_links,
            SUM(CASE WHEN status = 'RELEASED' THEN 1 ELSE 0 END) AS released_links
     FROM payroll_liability_payment_links
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND run_id = ?
       AND payment_batch_id = ?`,
    [fixture.tenantId, fixture.legalEntityId, run2.runId, run2.batchId]
  );
  assert(toNumber(run2LinkRows.rows?.[0]?.total_links) === 2, "Run2 should keep 2 payment links");
  assert(
    toNumber(run2LinkRows.rows?.[0]?.released_links) === 2,
    "Run2 links should be RELEASED after release apply"
  );

  await expectFailure(
    () =>
      getPayrollRunPaymentSyncPreview({
        req: null,
        tenantId: fixture.tenantId,
        runId: run1.runId,
        scope: "NET_PAY",
        allowB04OnlySettlement: false,
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );
  await expectFailure(
    () =>
      applyPayrollRunPaymentSync({
        req: null,
        tenantId: fixture.tenantId,
        runId: run1.runId,
        userId: fixture.userId,
        scope: "NET_PAY",
        allowB04OnlySettlement: false,
        note: "should fail permission",
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  console.log(
    "PR-P04 smoke test passed (MARK_PAID from B03 evidence + idempotent replay + RELEASE_TO_OPEN on cancellation + permission checks)."
  );
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
