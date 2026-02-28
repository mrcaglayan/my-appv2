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
  approveAndClosePayrollPeriod,
  preparePayrollPeriodClose,
  requestPayrollPeriodClose,
} from "../src/services/payroll.close.service.js";
import {
  buildPayrollRunLiabilities,
  createPayrollRunPaymentBatchFromLiabilities,
  getPayrollRunLiabilitiesDetail,
} from "../src/services/payroll.liabilities.service.js";
import { upsertPayrollComponentMapping } from "../src/services/payroll.mappings.service.js";
import { applyPayrollRunPaymentSync, getPayrollRunPaymentSyncPreview } from "../src/services/payroll.paymentSync.service.js";
import { importPayrollRunCsv } from "../src/services/payroll.runs.service.js";
import {
  approveApplyPayrollManualSettlementRequest,
  createPayrollManualSettlementRequest,
  listPayrollManualSettlementRequests,
  rejectPayrollManualSettlementRequest,
} from "../src/services/payroll.settlementOverrides.service.js";

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

async function createUser({ tenantId, email, name, passwordHash }) {
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, email, passwordHash, name]
  );
  const rows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, email]
  );
  const userId = toNumber(rows.rows?.[0]?.id);
  assert(userId > 0, `Failed to create user fixture: ${email}`);
  return userId;
}

async function createTenantWithP06Fixtures(stamp) {
  const tenantCode = `PRP06_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRP06 Tenant ${stamp}`]
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
    [tenantId, `PRP06_G_${stamp}`, `PRP06 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP06_G_${stamp}`]
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
      `PRP06_LE_${stamp}`,
      `PRP06 Legal Entity ${stamp}`,
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
    [tenantId, `PRP06_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
      )
      VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `PRP06_CAL_${stamp}`, `PRP06 Calendar ${stamp}`]
  );
  const calendarRows = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP06_CAL_${stamp}`]
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
      `PRP06_BOOK_${stamp}`,
      `PRP06 Book ${stamp}`,
      currencyCode,
    ]
  );

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `PRP06_COA_${stamp}`, `PRP06 Chart ${stamp}`]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP06_COA_${stamp}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts fixture");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP06BANK${stamp}`, `PRP06 Bank GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'EXPENSE', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP06EXP${stamp}`, `PRP06 Expense GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP06LIA${stamp}`, `PRP06 Liability GL ${stamp}`]
  );

  const bankGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP06 Bank GL ${stamp}`]
  );
  const expenseRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP06 Expense GL ${stamp}`]
  );
  const liabilityRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP06 Liability GL ${stamp}`]
  );
  const bankGlAccountId = toNumber(bankGlRows.rows?.[0]?.id);
  const expenseGlAccountId = toNumber(expenseRows.rows?.[0]?.id);
  const liabilityGlAccountId = toNumber(liabilityRows.rows?.[0]?.id);
  assert(bankGlAccountId > 0, "Failed to create bank GL fixture");
  assert(expenseGlAccountId > 0, "Failed to create expense GL fixture");
  assert(liabilityGlAccountId > 0, "Failed to create liability GL fixture");

  const passwordHash = await bcrypt.hash("PRP06#Smoke123", 10);
  const makerUserId = await createUser({
    tenantId,
    email: `prp06_maker_${stamp}@example.com`,
    name: "PRP06 Maker",
    passwordHash,
  });
  const checkerUserId = await createUser({
    tenantId,
    email: `prp06_checker_${stamp}@example.com`,
    name: "PRP06 Checker",
    passwordHash,
  });

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
      `PRP06_BA_${stamp}`,
      `PRP06 Bank Account ${stamp}`,
      currencyCode,
      bankGlAccountId,
      "Smoke Bank",
      "Main",
      `TR${String(stamp).slice(-20)}`,
      String(stamp),
      makerUserId,
    ]
  );
  const bankAccountRows = await query(
    `SELECT id
     FROM bank_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, `PRP06_BA_${stamp}`]
  );
  const bankAccountId = toNumber(bankAccountRows.rows?.[0]?.id);
  assert(bankAccountId > 0, "Failed to create bank account fixture");

  return {
    tenantId,
    legalEntityId,
    makerUserId,
    checkerUserId,
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
    userId: fixture.makerUserId,
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
      reason: "PRP06 beneficiary setup",
    },
    assertScopeAccess: noScopeGuard,
  });

  await createPayrollEmployeeBeneficiaryBankAccount({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.makerUserId,
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
      reason: "PRP06 beneficiary setup",
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
      userId: fixture.makerUserId,
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
        userId: fixture.makerUserId,
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
        notes: "PRP06 mapping setup",
      },
      assertScopeAccess: noScopeGuard,
    });
  }

  await markPayrollRunReviewed({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.makerUserId,
    note: "review for PRP06",
    assertScopeAccess: noScopeGuard,
  });

  const finalized = await finalizePayrollRunAccrual({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.makerUserId,
    note: "finalize for PRP06",
    forceFromImported: false,
    assertScopeAccess: noScopeGuard,
  });
  assert(toNumber(finalized?.accrualJournalEntryId) > 0, "Finalize should produce accrual journal");

  const built = await buildPayrollRunLiabilities({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.makerUserId,
    note: "build liabilities for PRP06",
    assertScopeAccess: noScopeGuard,
  });
  assert((built?.items || []).length === 7, "Liability build should create 7 liabilities");

  const prepared = await createPayrollRunPaymentBatchFromLiabilities({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.makerUserId,
    input: {
      scope: "NET_PAY",
      bankAccountId: fixture.bankAccountId,
      idempotencyKey,
      notes: "PRP06 NET payment prep",
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
  const checksum = `prp06-sync-${stamp}`.padEnd(64, "0").slice(0, 64);
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
      `prp06-sync-${stamp}.csv`,
      checksum,
      JSON.stringify({ source: "prp06-smoke" }),
      fixture.makerUserId,
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

  const lineHash = `PRP06-LINE-${stamp}`.padEnd(64, "0").slice(0, 64);
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
      `PRP06 partial settlement for batch ${batchId}`,
      `PRP06-B${batchId}`,
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
      "PRP06 partial settlement evidence",
      fixture.makerUserId,
    ]
  );

  return { statementLineId };
}

function findNetLiability(items, employeeCode) {
  return (items || []).find(
    (row) =>
      normalizeUpperText(row?.liability_group) === "EMPLOYEE_NET" &&
      normalizeUpperText(row?.employee_code) === normalizeUpperText(employeeCode)
  );
}

async function countManualOverrideSettlements({
  tenantId,
  legalEntityId,
  liabilityId,
}) {
  const rows = await query(
    `SELECT COUNT(*) AS total
     FROM payroll_liability_settlements
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND payroll_liability_id = ?
       AND settlement_source = 'MANUAL_OVERRIDE'`,
    [tenantId, legalEntityId, liabilityId]
  );
  return toNumber(rows.rows?.[0]?.total);
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithP06Fixtures(stamp);
  await ensureEmployeeBeneficiaryAccounts({ fixture, stamp });

  const run = await createPreparedNetPayrollBatch({
    fixture,
    providerCode: `PRP06_${stamp}`,
    sourceRef: `PRP06-${stamp}`,
    idempotencyKey: `PRP06-BATCH-${stamp}`,
  });

  const partialMatchedAmount = toAmount(run.batchTotal / 2);
  await createBankReconciliationEvidenceForBatch({
    fixture,
    batchId: run.batchId,
    matchedAmount: partialMatchedAmount,
    stamp,
  });

  const syncPreview = await getPayrollRunPaymentSyncPreview({
    req: null,
    tenantId: fixture.tenantId,
    runId: run.runId,
    scope: "NET_PAY",
    allowB04OnlySettlement: false,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(syncPreview?.summary?.mark_partial_count) === 2,
    "Preview should classify 2 NET liabilities as MARK_PARTIAL"
  );
  assert(
    toAmount(syncPreview?.summary?.mark_partial_amount) === partialMatchedAmount,
    "Preview MARK_PARTIAL amount should equal matched evidence amount"
  );
  assert(
    toNumber(syncPreview?.summary?.mark_paid_count) === 0,
    "Preview should not classify MARK_PAID for partial evidence"
  );

  const syncApply = await applyPayrollRunPaymentSync({
    req: null,
    tenantId: fixture.tenantId,
    runId: run.runId,
    userId: fixture.makerUserId,
    scope: "NET_PAY",
    allowB04OnlySettlement: false,
    note: "apply partial sync for PRP06",
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(syncApply?.applied?.mark_partial_count) === 2,
    "Apply should mark 2 NET liabilities as PARTIALLY_PAID"
  );
  assert(
    toAmount(syncApply?.applied?.mark_partial_amount) === partialMatchedAmount,
    "Apply MARK_PARTIAL amount should equal matched evidence amount"
  );
  assert(
    toNumber(syncApply?.applied?.mark_paid_count) === 0 &&
      toNumber(syncApply?.applied?.release_count) === 0,
    "Apply should not mark full-paid or release on partial evidence"
  );

  const detailAfterPartial = await getPayrollRunLiabilitiesDetail({
    req: null,
    tenantId: fixture.tenantId,
    runId: run.runId,
    assertScopeAccess: noScopeGuard,
  });
  const e001Partial = findNetLiability(detailAfterPartial?.items, "E001");
  const e002Partial = findNetLiability(detailAfterPartial?.items, "E002");
  assert(e001Partial, "E001 NET liability should exist");
  assert(e002Partial, "E002 NET liability should exist");
  assert(
    normalizeUpperText(e001Partial?.status) === "PARTIALLY_PAID" &&
      normalizeUpperText(e002Partial?.status) === "PARTIALLY_PAID",
    "Both NET liabilities should become PARTIALLY_PAID after partial sync"
  );
  assert(
    toAmount(e001Partial?.outstanding_amount) > 0 && toAmount(e002Partial?.outstanding_amount) > 0,
    "Both NET liabilities should retain outstanding amount after partial sync"
  );

  await expectFailure(
    () =>
      createPayrollManualSettlementRequest({
        req: null,
        tenantId: fixture.tenantId,
        liabilityId: toNumber(e001Partial.id),
        userId: fixture.makerUserId,
        input: {
          amount: toAmount(e001Partial?.outstanding_amount) + 1,
          settledAt: "2026-02-20 10:30:00",
          reason: "Excess amount should fail",
          externalRef: "PRP06-OVER",
          idempotencyKey: `PRP06-OVER-${stamp}`,
        },
        assertScopeAccess: noScopeGuard,
      }),
    { status: 409, includes: "exceeds remaining settleable amount" }
  );

  const req1Key = `PRP06-REQ1-${stamp}`;
  const req1Create = await createPayrollManualSettlementRequest({
    req: null,
    tenantId: fixture.tenantId,
    liabilityId: toNumber(e001Partial.id),
    userId: fixture.makerUserId,
    input: {
      amount: toAmount(e001Partial?.outstanding_amount),
      settledAt: "2026-02-20 11:00:00",
      reason: "Manual settlement approval for E001",
      externalRef: "PRP06-E001",
      idempotencyKey: req1Key,
    },
    assertScopeAccess: noScopeGuard,
  });
  const req1Id = toNumber(req1Create?.request?.id);
  assert(req1Id > 0, "Manual override request #1 should be created");
  assert(req1Create?.idempotent === false, "First request create should not be idempotent replay");
  assert(
    normalizeUpperText(req1Create?.request?.status) === "REQUESTED",
    "Manual override request #1 should be REQUESTED"
  );

  const req1Replay = await createPayrollManualSettlementRequest({
    req: null,
    tenantId: fixture.tenantId,
    liabilityId: toNumber(e001Partial.id),
    userId: fixture.makerUserId,
    input: {
      amount: toAmount(e001Partial?.outstanding_amount),
      settledAt: "2026-02-20 11:00:00",
      reason: "Manual settlement approval for E001 replay",
      externalRef: "PRP06-E001-REPLAY",
      idempotencyKey: req1Key,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(req1Replay?.idempotent === true, "Request #1 replay should be idempotent");
  assert(
    toNumber(req1Replay?.request?.id) === req1Id,
    "Request #1 idempotent replay should return same request id"
  );

  const req2Create = await createPayrollManualSettlementRequest({
    req: null,
    tenantId: fixture.tenantId,
    liabilityId: toNumber(e002Partial.id),
    userId: fixture.makerUserId,
    input: {
      amount: toAmount(Math.min(10, toAmount(e002Partial?.outstanding_amount))),
      settledAt: "2026-02-20 11:30:00",
      reason: "Manual settlement request to reject",
      externalRef: "PRP06-E002",
      idempotencyKey: `PRP06-REQ2-${stamp}`,
    },
    assertScopeAccess: noScopeGuard,
  });
  const req2Id = toNumber(req2Create?.request?.id);
  assert(req2Id > 0, "Manual override request #2 should be created");
  assert(
    normalizeUpperText(req2Create?.request?.status) === "REQUESTED",
    "Manual override request #2 should be REQUESTED"
  );

  const listedE001 = await listPayrollManualSettlementRequests({
    req: null,
    tenantId: fixture.tenantId,
    liabilityId: toNumber(e001Partial.id),
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(listedE001?.liability?.id) === toNumber(e001Partial.id),
    "List requests should return liability context"
  );
  assert(
    (listedE001?.items || []).some((row) => toNumber(row?.id) === req1Id),
    "List requests should include request #1"
  );

  await expectFailure(
    () =>
      approveApplyPayrollManualSettlementRequest({
        req: null,
        tenantId: fixture.tenantId,
        requestId: req1Id,
        userId: fixture.makerUserId,
        decisionNote: "self approve should fail",
        assertScopeAccess: noScopeGuard,
        skipUnifiedApprovalGate: true,
      }),
    { status: 403, includes: "Maker-checker violation" }
  );

  await expectFailure(
    () =>
      rejectPayrollManualSettlementRequest({
        req: null,
        tenantId: fixture.tenantId,
        requestId: req2Id,
        userId: fixture.makerUserId,
        decisionNote: "self reject should fail",
        assertScopeAccess: noScopeGuard,
      }),
    { status: 403, includes: "Maker-checker violation" }
  );

  const reject2 = await rejectPayrollManualSettlementRequest({
    req: null,
    tenantId: fixture.tenantId,
    requestId: req2Id,
    userId: fixture.checkerUserId,
    decisionNote: "Rejected by checker",
    assertScopeAccess: noScopeGuard,
  });
  assert(
    normalizeUpperText(reject2?.request?.status) === "REJECTED",
    "Request #2 should be REJECTED by checker"
  );

  const reject2Replay = await rejectPayrollManualSettlementRequest({
    req: null,
    tenantId: fixture.tenantId,
    requestId: req2Id,
    userId: fixture.checkerUserId,
    decisionNote: "Rejected replay",
    assertScopeAccess: noScopeGuard,
  });
  assert(reject2Replay?.idempotent === true, "Reject replay should be idempotent");

  const manualSettlementCountBeforeApply = await countManualOverrideSettlements({
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
    liabilityId: toNumber(e001Partial.id),
  });
  assert(
    manualSettlementCountBeforeApply === 0,
    "No MANUAL_OVERRIDE settlement should exist before approve/apply"
  );

  const approve1 = await approveApplyPayrollManualSettlementRequest({
    req: null,
    tenantId: fixture.tenantId,
    requestId: req1Id,
    userId: fixture.checkerUserId,
    decisionNote: "Approved by checker",
    assertScopeAccess: noScopeGuard,
    skipUnifiedApprovalGate: true,
  });
  const appliedSettlementId = toNumber(approve1?.request?.applied_settlement_id);
  assert(
    normalizeUpperText(approve1?.request?.status) === "APPLIED",
    "Request #1 should be APPLIED by checker"
  );
  assert(appliedSettlementId > 0, "APPLIED request should reference settlement row");

  const manualSettlementCountAfterApply = await countManualOverrideSettlements({
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
    liabilityId: toNumber(e001Partial.id),
  });
  assert(
    manualSettlementCountAfterApply === 1,
    "Approve/apply should create one MANUAL_OVERRIDE settlement row"
  );

  const approve1Replay = await approveApplyPayrollManualSettlementRequest({
    req: null,
    tenantId: fixture.tenantId,
    requestId: req1Id,
    userId: fixture.checkerUserId,
    decisionNote: "Approved replay",
    assertScopeAccess: noScopeGuard,
    skipUnifiedApprovalGate: true,
  });
  assert(approve1Replay?.idempotent === true, "Approve/apply replay should be idempotent");
  assert(
    toNumber(approve1Replay?.request?.applied_settlement_id) === appliedSettlementId,
    "Approve/apply replay should return same settlement id"
  );

  const manualSettlementCountAfterReplay = await countManualOverrideSettlements({
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
    liabilityId: toNumber(e001Partial.id),
  });
  assert(
    manualSettlementCountAfterReplay === manualSettlementCountAfterApply,
    "Approve/apply replay should not create duplicate MANUAL_OVERRIDE settlements"
  );

  const e001SettlementRows = await query(
    `SELECT id, settlement_source, settled_amount
     FROM payroll_liability_settlements
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, appliedSettlementId]
  );
  const e001Settlement = e001SettlementRows.rows?.[0] || null;
  assert(e001Settlement, "Applied settlement row should be queryable");
  assert(
    normalizeUpperText(e001Settlement?.settlement_source) === "MANUAL_OVERRIDE",
    "Applied settlement row should have MANUAL_OVERRIDE source"
  );

  const detailAfterApply = await getPayrollRunLiabilitiesDetail({
    req: null,
    tenantId: fixture.tenantId,
    runId: run.runId,
    assertScopeAccess: noScopeGuard,
  });
  const e001AfterApply = findNetLiability(detailAfterApply?.items, "E001");
  const e002AfterApply = findNetLiability(detailAfterApply?.items, "E002");
  assert(e001AfterApply, "E001 liability should still exist");
  assert(e002AfterApply, "E002 liability should still exist");
  assert(
    normalizeUpperText(e001AfterApply?.status) === "PAID",
    "E001 should become PAID after manual override apply"
  );
  assert(
    toAmount(e001AfterApply?.outstanding_amount) === 0,
    "E001 outstanding should be zero after manual override apply"
  );
  assert(
    normalizeUpperText(e002AfterApply?.status) === "PARTIALLY_PAID",
    "E002 should remain PARTIALLY_PAID after request #2 rejection"
  );

  const closePrepare = await preparePayrollPeriodClose({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.makerUserId,
    input: {
      legalEntityId: fixture.legalEntityId,
      periodStart: "2026-02-01",
      periodEnd: "2026-02-28",
      lockRunChanges: false,
      lockManualSettlements: true,
      lockPaymentPrep: false,
      note: "Close period for PRP06 lock check",
    },
    assertScopeAccess: noScopeGuard,
  });
  const closeId = toNumber(closePrepare?.close?.id);
  assert(closeId > 0, "Period close prepare should return close id");
  assert(
    normalizeUpperText(closePrepare?.close?.status) === "READY",
    "Period close should be READY when checks pass"
  );

  const closeRequested = await requestPayrollPeriodClose({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.makerUserId,
    closeId,
    note: "Request close for PRP06",
    requestIdempotencyKey: `PRP06-CLOSE-REQ-${stamp}`,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    normalizeUpperText(closeRequested?.close?.status) === "REQUESTED",
    "Close request should move close to REQUESTED"
  );

  const closeApproved = await approveAndClosePayrollPeriod({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.checkerUserId,
    closeId,
    note: "Approve close for PRP06",
    closeIdempotencyKey: `PRP06-CLOSE-${stamp}`,
    assertScopeAccess: noScopeGuard,
    skipUnifiedApprovalGate: true,
  });
  assert(
    normalizeUpperText(closeApproved?.close?.status) === "CLOSED",
    "Close approve should move close to CLOSED"
  );

  await expectFailure(
    () =>
      createPayrollManualSettlementRequest({
        req: null,
        tenantId: fixture.tenantId,
        liabilityId: toNumber(e002AfterApply.id),
        userId: fixture.makerUserId,
        input: {
          amount: 10,
          settledAt: "2026-02-25 12:00:00",
          reason: "Should fail due to period lock",
          externalRef: "PRP06-LOCK",
          idempotencyKey: `PRP06-LOCK-${stamp}`,
        },
        assertScopeAccess: noScopeGuard,
      }),
    { status: 409, code: "PAYROLL_PERIOD_LOCKED", includes: "locked for action MANUAL_SETTLEMENT_REQUEST" }
  );

  await expectFailure(
    () =>
      listPayrollManualSettlementRequests({
        req: null,
        tenantId: fixture.tenantId,
        liabilityId: toNumber(e001AfterApply.id),
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  await expectFailure(
    () =>
      createPayrollManualSettlementRequest({
        req: null,
        tenantId: fixture.tenantId,
        liabilityId: toNumber(e001AfterApply.id),
        userId: fixture.makerUserId,
        input: {
          amount: 1,
          settledAt: "2026-02-25 12:30:00",
          reason: "permission check",
          externalRef: "PRP06-PERM",
          idempotencyKey: `PRP06-PERM-${stamp}`,
        },
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  console.log(
    "PR-P06 smoke test passed (partial settlement sync + manual override request/approve/reject + maker-checker + idempotency + period lock + permission checks)."
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
