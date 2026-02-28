import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  finalizePayrollRunAccrual,
  getPayrollRunAccrualPreview,
  markPayrollRunReviewed,
} from "../src/services/payroll.accruals.service.js";
import {
  createPayrollEmployeeBeneficiaryBankAccount,
  getPayrollLiabilityBeneficiarySnapshot,
  listPayrollEmployeeBeneficiaryBankAccounts,
  setPrimaryPayrollEmployeeBeneficiaryBankAccount,
} from "../src/services/payroll.beneficiaries.service.js";
import {
  buildPayrollRunLiabilities,
  createPayrollRunPaymentBatchFromLiabilities,
  getPayrollRunLiabilitiesDetail,
} from "../src/services/payroll.liabilities.service.js";
import { upsertPayrollComponentMapping } from "../src/services/payroll.mappings.service.js";
import { approvePaymentBatch, exportPaymentBatch } from "../src/services/payments.service.js";
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

function buildPayrollCsv(rows) {
  const header =
    "employee_code,employee_name,cost_center_code,base_salary,overtime_pay,bonus_pay,allowances_total,gross_pay,employee_tax,employee_social_security,other_deductions,employer_tax,employer_social_security,net_pay";
  const body = rows.map((row) =>
    [
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
      row.employer_tax,
      row.employer_social_security,
      row.net_pay,
    ].join(",")
  );
  return [header, ...body].join("\n");
}

function buildCsvStandardRun(nameSuffix = "") {
  const suffix = String(nameSuffix || "").trim();
  const alphaName = suffix ? `Alpha User ${suffix}` : "Alpha User";
  const betaName = suffix ? `Beta User ${suffix}` : "Beta User";
  return buildPayrollCsv([
    {
      employee_code: "E001",
      employee_name: alphaName,
      cost_center_code: "CC-01",
      base_salary: 1000,
      overtime_pay: 100,
      bonus_pay: 50,
      allowances_total: 50,
      gross_pay: 1200,
      employee_tax: 120,
      employee_social_security: 80,
      other_deductions: 20,
      employer_tax: 150,
      employer_social_security: 100,
      net_pay: 980,
    },
    {
      employee_code: "E002",
      employee_name: betaName,
      cost_center_code: "CC-02",
      base_salary: 900,
      overtime_pay: 0,
      bonus_pay: 0,
      allowances_total: 100,
      gross_pay: 1000,
      employee_tax: 100,
      employee_social_security: 50,
      other_deductions: 10,
      employer_tax: 120,
      employer_social_security: 90,
      net_pay: 840,
    },
  ]);
}

function buildCsvMissingBeneficiaryRun() {
  return buildPayrollCsv([
    {
      employee_code: "E001",
      employee_name: "Alpha User",
      cost_center_code: "CC-01",
      base_salary: 1000,
      overtime_pay: 100,
      bonus_pay: 50,
      allowances_total: 50,
      gross_pay: 1200,
      employee_tax: 120,
      employee_social_security: 80,
      other_deductions: 20,
      employer_tax: 150,
      employer_social_security: 100,
      net_pay: 980,
    },
    {
      employee_code: "E003",
      employee_name: "Gamma User",
      cost_center_code: "CC-03",
      base_salary: 800,
      overtime_pay: 0,
      bonus_pay: 0,
      allowances_total: 100,
      gross_pay: 900,
      employee_tax: 90,
      employee_social_security: 45,
      other_deductions: 10,
      employer_tax: 100,
      employer_social_security: 80,
      net_pay: 755,
    },
  ]);
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

async function createTenantWithP07Fixtures(stamp) {
  const tenantCode = `PRP07_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRP07 Tenant ${stamp}`]
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
    [tenantId, `PRP07_G_${stamp}`, `PRP07 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP07_G_${stamp}`]
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
      `PRP07_LE_${stamp}`,
      `PRP07 Legal Entity ${stamp}`,
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
    [tenantId, `PRP07_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
      )
      VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `PRP07_CAL_${stamp}`, `PRP07 Calendar ${stamp}`]
  );
  const calendarRows = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP07_CAL_${stamp}`]
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
      `PRP07_BOOK_${stamp}`,
      `PRP07 Book ${stamp}`,
      currencyCode,
    ]
  );

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `PRP07_COA_${stamp}`, `PRP07 Chart ${stamp}`]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP07_COA_${stamp}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts fixture");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP07BANK${stamp}`, `PRP07 Bank GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'EXPENSE', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP07EXP${stamp}`, `PRP07 Expense GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP07LIA${stamp}`, `PRP07 Liability GL ${stamp}`]
  );

  const bankGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP07 Bank GL ${stamp}`]
  );
  const expenseRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP07 Expense GL ${stamp}`]
  );
  const liabilityRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP07 Liability GL ${stamp}`]
  );
  const bankGlAccountId = toNumber(bankGlRows.rows?.[0]?.id);
  const expenseGlAccountId = toNumber(expenseRows.rows?.[0]?.id);
  const liabilityGlAccountId = toNumber(liabilityRows.rows?.[0]?.id);
  assert(bankGlAccountId > 0, "Failed to create bank GL fixture");
  assert(expenseGlAccountId > 0, "Failed to create expense GL fixture");
  assert(liabilityGlAccountId > 0, "Failed to create liability GL fixture");

  const passwordHash = await bcrypt.hash("PRP07#Smoke123", 10);
  const userId = await createUser({
    tenantId,
    email: `prp07_user_${stamp}@example.com`,
    name: "PRP07 User",
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
      `PRP07_BA_${stamp}`,
      `PRP07 Bank Account ${stamp}`,
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
    [tenantId, legalEntityId, `PRP07_BA_${stamp}`]
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

async function importRun({
  fixture,
  providerCode,
  sourceRef,
  csvText,
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
      csvText,
    },
    assertScopeAccess: noScopeGuard,
  });
  const runId = toNumber(imported?.id);
  assert(runId > 0, "Payroll run import should return run id");
  return runId;
}

async function ensureMappingsForRun({ fixture, runId, providerCode }) {
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
        notes: "PRP07 mapping setup",
      },
      assertScopeAccess: noScopeGuard,
    });
  }
}

async function finalizeRunAndBuildLiabilities({ fixture, runId }) {
  await markPayrollRunReviewed({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "review for PRP07",
    assertScopeAccess: noScopeGuard,
  });

  const finalized = await finalizePayrollRunAccrual({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "finalize for PRP07",
    forceFromImported: false,
    assertScopeAccess: noScopeGuard,
  });
  assert(toNumber(finalized?.accrualJournalEntryId) > 0, "Finalize should produce accrual journal");

  const built = await buildPayrollRunLiabilities({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "build liabilities for PRP07",
    assertScopeAccess: noScopeGuard,
  });
  assert((built?.items || []).length === 7, "Liability build should create 7 liabilities");
}

async function prepareNetPaymentBatch({ fixture, runId, idempotencyKey }) {
  const prepared = await createPayrollRunPaymentBatchFromLiabilities({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    input: {
      scope: "NET_PAY",
      bankAccountId: fixture.bankAccountId,
      idempotencyKey,
      notes: "PRP07 NET payment prep",
    },
    assertScopeAccess: noScopeGuard,
  });
  const batchId = toNumber(prepared?.batch?.id);
  assert(batchId > 0, "Payment batch should be created");
  assert((prepared?.batch?.lines || []).length === 2, "NET scope should prepare 2 lines");
  return prepared;
}

function findNetLiability(items, employeeCode) {
  return (items || []).find(
    (row) =>
      normalizeUpperText(row?.liability_group) === "EMPLOYEE_NET" &&
      normalizeUpperText(row?.employee_code) === normalizeUpperText(employeeCode)
  );
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithP07Fixtures(stamp);
  const providerCode = `PRP07_${stamp}`;

  const oldIban = `TR00OLD${String(stamp).slice(-10)}E001`;
  const newIban = `TR00NEW${String(stamp).slice(-10)}E001`;
  const e002Iban = `TR00E002${String(stamp).slice(-10)}ZZ`;

  const e001Primary = await createPayrollEmployeeBeneficiaryBankAccount({
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
      iban: oldIban,
      accountNumber: null,
      routingNumber: null,
      swiftBic: null,
      isPrimary: true,
      effectiveFrom: "2026-01-01",
      effectiveTo: null,
      verificationStatus: "VERIFIED",
      sourceType: "MANUAL",
      externalRef: null,
      reason: "PRP07 E001 primary setup",
    },
    assertScopeAccess: noScopeGuard,
  });
  const e001PrimaryId = toNumber(e001Primary?.item?.id);
  assert(e001PrimaryId > 0, "E001 primary beneficiary should be created");

  const e002Primary = await createPayrollEmployeeBeneficiaryBankAccount({
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
      iban: e002Iban,
      accountNumber: null,
      routingNumber: null,
      swiftBic: null,
      isPrimary: true,
      effectiveFrom: "2026-01-01",
      effectiveTo: null,
      verificationStatus: "VERIFIED",
      sourceType: "MANUAL",
      externalRef: null,
      reason: "PRP07 E002 primary setup",
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(toNumber(e002Primary?.item?.id) > 0, "E002 primary beneficiary should be created");

  const e001ListInitial = await listPayrollEmployeeBeneficiaryBankAccounts({
    req: null,
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
    employeeCode: "E001",
    currencyCode: fixture.currencyCode,
    status: "ACTIVE",
    assertScopeAccess: noScopeGuard,
  });
  assert((e001ListInitial?.items || []).length === 1, "E001 should start with one active account");
  const initialAccount = e001ListInitial.items[0];
  assert(initialAccount.is_primary === true, "E001 initial account should be primary");
  assert(
    String(initialAccount.account_last4 || "") === oldIban.slice(-4),
    "E001 initial account_last4 should match old IBAN"
  );

  const run1Id = await importRun({
    fixture,
    providerCode,
    sourceRef: `PRP07-RUN1-${stamp}`,
    csvText: buildCsvStandardRun(),
  });
  await ensureMappingsForRun({ fixture, runId: run1Id, providerCode });
  await finalizeRunAndBuildLiabilities({ fixture, runId: run1Id });
  const batch1 = await prepareNetPaymentBatch({
    fixture,
    runId: run1Id,
    idempotencyKey: `PRP07-BATCH1-${stamp}`,
  });
  const batch1Id = toNumber(batch1?.batch?.id);

  const run1Detail = await getPayrollRunLiabilitiesDetail({
    req: null,
    tenantId: fixture.tenantId,
    runId: run1Id,
    assertScopeAccess: noScopeGuard,
  });
  const run1E001 = findNetLiability(run1Detail?.items, "E001");
  const run1E002 = findNetLiability(run1Detail?.items, "E002");
  assert(run1E001 && run1E002, "Run1 should include E001/E002 NET liabilities");

  const linkAggRun1 = await query(
    `SELECT
        COUNT(*) AS total_links,
        SUM(CASE WHEN beneficiary_bank_snapshot_id IS NOT NULL THEN 1 ELSE 0 END) AS with_snapshot,
        SUM(CASE WHEN beneficiary_snapshot_status = 'CAPTURED' THEN 1 ELSE 0 END) AS captured_count
     FROM payroll_liability_payment_links
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND run_id = ?
       AND payment_batch_id = ?`,
    [fixture.tenantId, fixture.legalEntityId, run1Id, batch1Id]
  );
  assert(toNumber(linkAggRun1.rows?.[0]?.total_links) === 2, "Run1 should have 2 payment links");
  assert(
    toNumber(linkAggRun1.rows?.[0]?.with_snapshot) === 2,
    "Run1 links should all have beneficiary snapshots"
  );
  assert(
    toNumber(linkAggRun1.rows?.[0]?.captured_count) === 2,
    "Run1 link snapshot statuses should all be CAPTURED"
  );

  const run1E001Snapshot = await getPayrollLiabilityBeneficiarySnapshot({
    req: null,
    tenantId: fixture.tenantId,
    liabilityId: toNumber(run1E001.id),
    assertScopeAccess: noScopeGuard,
  });
  const run1SnapshotId = toNumber(run1E001Snapshot?.item?.beneficiary_bank_snapshot_id);
  assert(run1SnapshotId > 0, "Run1 E001 snapshot id should exist");
  assert(
    normalizeUpperText(run1E001Snapshot?.item?.beneficiary_snapshot_status) === "CAPTURED",
    "Run1 E001 snapshot status should be CAPTURED"
  );
  assert(
    String(run1E001Snapshot?.item?.snapshot?.iban || "") === oldIban,
    "Run1 E001 snapshot IBAN should match old beneficiary master"
  );

  const e001SecondAccount = await createPayrollEmployeeBeneficiaryBankAccount({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    input: {
      legalEntityId: fixture.legalEntityId,
      employeeCode: "E001",
      employeeName: "Alpha User",
      accountHolderName: "Alpha User",
      bankName: "Smoke Bank 2",
      bankBranchName: "Branch 2",
      countryCode: "TR",
      currencyCode: fixture.currencyCode,
      iban: newIban,
      accountNumber: null,
      routingNumber: null,
      swiftBic: null,
      isPrimary: false,
      effectiveFrom: "2026-01-01",
      effectiveTo: null,
      verificationStatus: "VERIFIED",
      sourceType: "MANUAL",
      externalRef: null,
      reason: "PRP07 E001 second account",
    },
    assertScopeAccess: noScopeGuard,
  });
  const e001SecondAccountId = toNumber(e001SecondAccount?.item?.id);
  assert(e001SecondAccountId > 0, "E001 second account should be created");
  assert(e001SecondAccount?.item?.is_primary === false, "Second account should start non-primary");

  const e001SetPrimary = await setPrimaryPayrollEmployeeBeneficiaryBankAccount({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    accountId: e001SecondAccountId,
    reason: "Switch payout account",
    assertScopeAccess: noScopeGuard,
  });
  assert(e001SetPrimary?.item?.is_primary === true, "Second account should become primary");

  const e001ListAfterSwitch = await listPayrollEmployeeBeneficiaryBankAccounts({
    req: null,
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
    employeeCode: "E001",
    currencyCode: fixture.currencyCode,
    status: "ACTIVE",
    assertScopeAccess: noScopeGuard,
  });
  const activeAccounts = e001ListAfterSwitch?.items || [];
  assert(activeAccounts.length === 2, "E001 should have two active accounts after switch");
  const primaryAfterSwitch = activeAccounts.find((row) => row.is_primary === true);
  assert(primaryAfterSwitch, "One active primary account should exist after switch");
  assert(
    toNumber(primaryAfterSwitch.id) === e001SecondAccountId,
    "New primary account id should match set-primary target"
  );
  const oldAccountAfterSwitch = activeAccounts.find((row) => toNumber(row.id) === e001PrimaryId);
  assert(oldAccountAfterSwitch?.is_primary === false, "Old primary should be unset after switch");

  await approvePaymentBatch({
    req: null,
    tenantId: fixture.tenantId,
    batchId: batch1Id,
    userId: fixture.userId,
    approveInput: { note: "approve run1 batch for P07 export check" },
    assertScopeAccess: noScopeGuard,
  });
  const export1 = await exportPaymentBatch({
    req: null,
    tenantId: fixture.tenantId,
    batchId: batch1Id,
    userId: fixture.userId,
    exportInput: { format: "CSV" },
    assertScopeAccess: noScopeGuard,
  });
  const csv1 = String(export1?.export?.csv || "");
  assert(csv1.length > 0, "Run1 export CSV should be generated");
  assert(
    csv1.includes(oldIban),
    "Run1 export should use old snapshot IBAN (historical immutability)"
  );
  assert(
    !csv1.includes(newIban),
    "Run1 export should not use new beneficiary master IBAN"
  );

  const run2Id = await importRun({
    fixture,
    providerCode,
    sourceRef: `PRP07-RUN2-${stamp}`,
    csvText: buildCsvStandardRun("R2"),
  });
  await finalizeRunAndBuildLiabilities({ fixture, runId: run2Id });
  await prepareNetPaymentBatch({
    fixture,
    runId: run2Id,
    idempotencyKey: `PRP07-BATCH2-${stamp}`,
  });
  const run2Detail = await getPayrollRunLiabilitiesDetail({
    req: null,
    tenantId: fixture.tenantId,
    runId: run2Id,
    assertScopeAccess: noScopeGuard,
  });
  const run2E001 = findNetLiability(run2Detail?.items, "E001");
  assert(run2E001, "Run2 E001 liability should exist");
  const run2E001Snapshot = await getPayrollLiabilityBeneficiarySnapshot({
    req: null,
    tenantId: fixture.tenantId,
    liabilityId: toNumber(run2E001.id),
    assertScopeAccess: noScopeGuard,
  });
  const run2SnapshotId = toNumber(run2E001Snapshot?.item?.beneficiary_bank_snapshot_id);
  assert(run2SnapshotId > 0, "Run2 E001 snapshot id should exist");
  assert(
    run2SnapshotId !== run1SnapshotId,
    "Run2 E001 snapshot id should differ after beneficiary primary switch"
  );
  assert(
    String(run2E001Snapshot?.item?.snapshot?.iban || "") === newIban,
    "Run2 E001 snapshot should capture new primary IBAN"
  );

  const run3Id = await importRun({
    fixture,
    providerCode,
    sourceRef: `PRP07-RUN3-${stamp}`,
    csvText: buildCsvMissingBeneficiaryRun(),
  });
  await finalizeRunAndBuildLiabilities({ fixture, runId: run3Id });

  await expectFailure(
    () =>
      createPayrollRunPaymentBatchFromLiabilities({
        req: null,
        tenantId: fixture.tenantId,
        runId: run3Id,
        userId: fixture.userId,
        input: {
          scope: "NET_PAY",
          bankAccountId: fixture.bankAccountId,
          idempotencyKey: `PRP07-BATCH3-${stamp}`,
          notes: "Should fail due to missing E003 beneficiary",
        },
        assertScopeAccess: noScopeGuard,
      }),
    { status: 409, code: "PAYROLL_BENEFICIARY_MISSING", includes: "missing beneficiary bank setup" }
  );

  await expectFailure(
    () =>
      listPayrollEmployeeBeneficiaryBankAccounts({
        req: null,
        tenantId: fixture.tenantId,
        legalEntityId: fixture.legalEntityId,
        employeeCode: "E001",
        currencyCode: fixture.currencyCode,
        status: "ACTIVE",
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  await expectFailure(
    () =>
      createPayrollEmployeeBeneficiaryBankAccount({
        req: null,
        tenantId: fixture.tenantId,
        userId: fixture.userId,
        input: {
          legalEntityId: fixture.legalEntityId,
          employeeCode: "E099",
          employeeName: "Blocked User",
          accountHolderName: "Blocked User",
          bankName: "Blocked Bank",
          bankBranchName: "Main",
          countryCode: "TR",
          currencyCode: fixture.currencyCode,
          iban: `TR00BLK${String(stamp).slice(-10)}9999`,
          accountNumber: null,
          routingNumber: null,
          swiftBic: null,
          isPrimary: true,
          effectiveFrom: "2026-01-01",
          effectiveTo: null,
          verificationStatus: "VERIFIED",
          sourceType: "MANUAL",
          externalRef: null,
          reason: "permission check",
        },
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  await expectFailure(
    () =>
      setPrimaryPayrollEmployeeBeneficiaryBankAccount({
        req: null,
        tenantId: fixture.tenantId,
        userId: fixture.userId,
        accountId: e001SecondAccountId,
        reason: "permission check",
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  await expectFailure(
    () =>
      getPayrollLiabilityBeneficiarySnapshot({
        req: null,
        tenantId: fixture.tenantId,
        liabilityId: toNumber(run2E001.id),
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  console.log(
    "PR-P07 smoke test passed (beneficiary master + primary switching + immutable snapshots on export + new snapshot after switch + missing setup block + permission checks)."
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
