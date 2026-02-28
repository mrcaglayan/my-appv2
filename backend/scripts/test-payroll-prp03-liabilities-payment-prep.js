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
  getPayrollRunLiabilityPaymentBatchPreview,
  listPayrollLiabilityRows,
} from "../src/services/payroll.liabilities.service.js";
import { upsertPayrollComponentMapping } from "../src/services/payroll.mappings.service.js";
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

function allowAllScopeFilter() {
  return "1 = 1";
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

async function createTenantWithP03Fixtures(stamp) {
  const tenantCode = `PRP03_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRP03 Tenant ${stamp}`]
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
    [tenantId, `PRP03_G_${stamp}`, `PRP03 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP03_G_${stamp}`]
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
      `PRP03_LE_${stamp}`,
      `PRP03 Legal Entity ${stamp}`,
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
    [tenantId, `PRP03_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
      )
      VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `PRP03_CAL_${stamp}`, `PRP03 Calendar ${stamp}`]
  );
  const calendarRows = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP03_CAL_${stamp}`]
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
      `PRP03_BOOK_${stamp}`,
      `PRP03 Book ${stamp}`,
      currencyCode,
    ]
  );

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `PRP03_COA_${stamp}`, `PRP03 Chart ${stamp}`]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP03_COA_${stamp}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts fixture");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP03BANK${stamp}`, `PRP03 Bank GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'EXPENSE', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP03EXP${stamp}`, `PRP03 Expense GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP03LIA${stamp}`, `PRP03 Liability GL ${stamp}`]
  );

  const bankGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP03 Bank GL ${stamp}`]
  );
  const expenseRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP03 Expense GL ${stamp}`]
  );
  const liabilityRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP03 Liability GL ${stamp}`]
  );
  const bankGlAccountId = toNumber(bankGlRows.rows?.[0]?.id);
  const expenseGlAccountId = toNumber(expenseRows.rows?.[0]?.id);
  const liabilityGlAccountId = toNumber(liabilityRows.rows?.[0]?.id);
  assert(bankGlAccountId > 0, "Failed to create bank GL fixture");
  assert(expenseGlAccountId > 0, "Failed to create expense GL fixture");
  assert(liabilityGlAccountId > 0, "Failed to create liability GL fixture");

  const passwordHash = await bcrypt.hash("PRP03#Smoke123", 10);
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, `prp03_user_${stamp}@example.com`, passwordHash, "PRP03 User"]
  );
  const userRows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, `prp03_user_${stamp}@example.com`]
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
      `PRP03_BA_${stamp}`,
      `PRP03 Bank Account ${stamp}`,
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
    [tenantId, legalEntityId, `PRP03_BA_${stamp}`]
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

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithP03Fixtures(stamp);
  const providerCode = `PRP03_${stamp}`;

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
      sourceBatchRef: `PRP03-SRC-${stamp}`,
      originalFilename: `prp03-${stamp}.csv`,
      csvText: buildValidCsv(),
    },
    assertScopeAccess: noScopeGuard,
  });
  const runId = toNumber(imported?.id);
  assert(runId > 0, "importPayrollRunCsv should return run id");

  await expectFailure(
    () =>
      buildPayrollRunLiabilities({
        req: null,
        tenantId: fixture.tenantId,
        runId,
        userId: fixture.userId,
        note: "should fail before finalize",
        assertScopeAccess: noScopeGuard,
      }),
    { status: 400, includes: "must be FINALIZED with accrual journal" }
  );

  const accrualPreviewBeforeMappings = await getPayrollRunAccrualPreview({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    (accrualPreviewBeforeMappings?.component_totals || []).length > 0,
    "Accrual preview should expose component totals"
  );

  for (const component of accrualPreviewBeforeMappings.component_totals || []) {
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
        notes: "PRP03 smoke mapping",
      },
      assertScopeAccess: noScopeGuard,
    });
  }

  const reviewed = await markPayrollRunReviewed({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "review for P03",
    assertScopeAccess: noScopeGuard,
  });
  assert(reviewed?.idempotentReplay === false, "First review should not be idempotent replay");

  const finalized = await finalizePayrollRunAccrual({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "finalize for P03",
    forceFromImported: false,
    assertScopeAccess: noScopeGuard,
  });
  assert(toNumber(finalized?.accrualJournalEntryId) > 0, "Finalize should set accrual journal");

  const built1 = await buildPayrollRunLiabilities({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "build liabilities",
    assertScopeAccess: noScopeGuard,
  });
  assert(built1?.alreadyBuilt === false, "First liability build should not be alreadyBuilt");
  assert((built1?.items || []).length === 7, "Expected 7 liabilities (2 net + 5 statutory)");
  assert(toAmount(built1?.summary?.total_employee_net) === 1820, "Employee net total should be 1820");
  assert(toAmount(built1?.summary?.total_statutory) === 840, "Statutory total should be 840");
  assert(toAmount(built1?.summary?.total_open) === 2660, "Total OPEN amount should be 2660 before prep");

  const built2 = await buildPayrollRunLiabilities({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "build liabilities idempotent replay",
    assertScopeAccess: noScopeGuard,
  });
  assert(built2?.alreadyBuilt === true, "Second liability build should be alreadyBuilt");

  const previewNet = await getPayrollRunLiabilityPaymentBatchPreview({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    scope: "NET_PAY",
    assertScopeAccess: noScopeGuard,
  });
  assert(toNumber(previewNet?.eligible_liability_count) === 2, "NET_PAY preview should include 2 liabilities");
  assert(toAmount(previewNet?.total_amount) === 1820, "NET_PAY preview total should be 1820");
  assert(previewNet?.can_prepare_payment_batch === true, "NET_PAY preview should be preparable");

  const previewStat = await getPayrollRunLiabilityPaymentBatchPreview({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    scope: "STATUTORY",
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(previewStat?.eligible_liability_count) === 5,
    "STATUTORY preview should include 5 liabilities"
  );
  assert(toAmount(previewStat?.total_amount) === 840, "STATUTORY preview total should be 840");

  const previewAll = await getPayrollRunLiabilityPaymentBatchPreview({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    scope: "ALL",
    assertScopeAccess: noScopeGuard,
  });
  assert(toNumber(previewAll?.eligible_liability_count) === 7, "ALL preview should include all liabilities");
  assert(toAmount(previewAll?.total_amount) === 2660, "ALL preview total should be 2660");

  await expectFailure(
    () =>
      createPayrollRunPaymentBatchFromLiabilities({
        req: null,
        tenantId: fixture.tenantId,
        runId,
        userId: fixture.userId,
        input: {
          scope: "NET_PAY",
          bankAccountId: fixture.bankAccountId,
          idempotencyKey: `PRP03-NET-${stamp}`,
          notes: "should fail without beneficiaries",
        },
        assertScopeAccess: noScopeGuard,
      }),
    {
      status: 409,
      code: "PAYROLL_BENEFICIARY_MISSING",
      includes: "missing beneficiary bank setup",
    }
  );

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
      reason: "PRP03 setup",
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
      reason: "PRP03 setup",
    },
    assertScopeAccess: noScopeGuard,
  });

  const prepared = await createPayrollRunPaymentBatchFromLiabilities({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    input: {
      scope: "NET_PAY",
      bankAccountId: fixture.bankAccountId,
      idempotencyKey: `PRP03-NET-${stamp}`,
      notes: "prepare net payroll batch",
    },
    assertScopeAccess: noScopeGuard,
  });
  const batchId = toNumber(prepared?.batch?.id);
  assert(batchId > 0, "Payment batch should be created");
  assert(normalizeUpperText(prepared?.batch?.source_type) === "PAYROLL", "Batch source_type should be PAYROLL");
  assert(toNumber(prepared?.batch?.source_id) === runId, "Batch source_id should match runId");
  assert(normalizeUpperText(prepared?.batch?.status) === "DRAFT", "Prepared batch should be DRAFT");
  assert((prepared?.batch?.lines || []).length === 2, "NET_PAY batch should include 2 lines");
  assert(toAmount(prepared?.batch?.total_amount) === 1820, "NET_PAY batch total should be 1820");
  assert(toNumber(prepared?.linkSummary?.linkedCount) === 2, "Two liabilities should be linked to batch");
  assert(toNumber(prepared?.linkSummary?.statusUpdatedCount) === 2, "Two liabilities should move to IN_BATCH");
  assert(
    toNumber(prepared?.preview_after_prepare?.eligible_liability_count) === 0,
    "After NET_PAY prepare there should be no remaining NET_PAY eligible liabilities"
  );

  const detailAfter = await getPayrollRunLiabilitiesDetail({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    assertScopeAccess: noScopeGuard,
  });
  assert(toAmount(detailAfter?.summary?.total_in_batch) === 1820, "IN_BATCH amount should be 1820 after prepare");
  assert(toAmount(detailAfter?.summary?.total_open) === 840, "OPEN amount should be 840 after NET_PAY prepare");

  const listedInBatch = await listPayrollLiabilityRows({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      tenantId: fixture.tenantId,
      runId,
      legalEntityId: fixture.legalEntityId,
      status: "IN_BATCH",
      liabilityType: null,
      scope: "NET_PAY",
      q: null,
      cursor: null,
      limit: 200,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert((listedInBatch?.rows || []).length === 2, "List IN_BATCH NET_PAY should return 2 liabilities");
  assert(
    (listedInBatch?.rows || []).every((row) => row.beneficiary_ready_for_payment === true),
    "Prepared NET_PAY liabilities should be beneficiary-ready"
  );

  const snapshotLinkRows = await query(
    `SELECT
        COUNT(*) AS total_links,
        SUM(CASE WHEN beneficiary_bank_snapshot_id IS NOT NULL THEN 1 ELSE 0 END) AS with_snapshot,
        SUM(CASE WHEN beneficiary_snapshot_status = 'CAPTURED' THEN 1 ELSE 0 END) AS captured_count
     FROM payroll_liability_payment_links
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND run_id = ?
       AND payment_batch_id = ?`,
    [fixture.tenantId, fixture.legalEntityId, runId, batchId]
  );
  const snapAgg = snapshotLinkRows.rows?.[0] || null;
  assert(snapAgg, "Snapshot links aggregate should exist");
  assert(toNumber(snapAgg?.total_links) === 2, "There should be 2 liability payment links");
  assert(toNumber(snapAgg?.with_snapshot) === 2, "All NET_PAY links should carry snapshot id");
  assert(toNumber(snapAgg?.captured_count) === 2, "All NET_PAY links should have CAPTURED status");

  await expectFailure(
    () =>
      createPayrollRunPaymentBatchFromLiabilities({
        req: null,
        tenantId: fixture.tenantId,
        runId,
        userId: fixture.userId,
        input: {
          scope: "NET_PAY",
          bankAccountId: fixture.bankAccountId,
          idempotencyKey: `PRP03-NET-${stamp}`,
          notes: "idempotent retry with no eligible liabilities",
        },
        assertScopeAccess: noScopeGuard,
      }),
    { status: 400, includes: "No eligible payroll liabilities" }
  );

  console.log(
    "PR-P03 smoke test passed (build liabilities + previews + beneficiary gate + NET payment batch prepare + links/snapshots/audit-safe transitions)."
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
