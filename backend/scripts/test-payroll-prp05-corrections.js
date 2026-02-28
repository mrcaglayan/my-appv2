import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  finalizePayrollRunAccrual,
  getPayrollRunAccrualPreview,
  markPayrollRunReviewed,
} from "../src/services/payroll.accruals.service.js";
import {
  createPayrollCorrectionShell,
  listPayrollRunCorrections,
  reversePayrollRunWithCorrection,
} from "../src/services/payroll.corrections.service.js";
import { buildPayrollRunLiabilities, getPayrollRunLiabilitiesDetail } from "../src/services/payroll.liabilities.service.js";
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

function denyScopeGuard() {
  const err = new Error("Scope access denied");
  err.status = 403;
  throw err;
}

async function expectFailure(work, { status, includes }) {
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
    if (includes && !String(error?.message || "").includes(includes)) {
      throw new Error(
        `Expected error message to include "${includes}" but got "${String(error?.message || "")}"`
      );
    }
    return;
  }
  throw new Error("Expected operation to fail, but it succeeded");
}

function buildOriginalCsv() {
  return [
    "employee_code,employee_name,cost_center_code,base_salary,overtime_pay,bonus_pay,allowances_total,gross_pay,employee_tax,employee_social_security,other_deductions,employer_tax,employer_social_security,net_pay",
    "E001,Alpha User,CC-01,1000,100,50,50,1200,120,80,20,150,100,980",
    "E002,Beta User,CC-02,900,0,0,100,1000,100,50,10,120,90,840",
  ].join("\n");
}

function buildCorrectionCsv() {
  return [
    "employee_code,employee_name,cost_center_code,base_salary,overtime_pay,bonus_pay,allowances_total,gross_pay,employee_tax,employee_social_security,other_deductions,employer_tax,employer_social_security,net_pay",
    "E001,Alpha User,CC-01,1100,100,50,50,1300,130,80,20,160,110,1070",
    "E002,Beta User,CC-02,900,0,0,100,1000,100,50,10,120,90,840",
  ].join("\n");
}

async function createTenantWithP05Fixtures(stamp) {
  const tenantCode = `PRP05_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRP05 Tenant ${stamp}`]
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
    [tenantId, `PRP05_G_${stamp}`, `PRP05 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP05_G_${stamp}`]
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
      `PRP05_LE_${stamp}`,
      `PRP05 Legal Entity ${stamp}`,
      countryId,
      currencyCode,
    ]
  );
  const legalEntityRows = await query(
    `SELECT id, code
     FROM legal_entities
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP05_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  const legalEntityCode = String(legalEntityRows.rows?.[0]?.code || "");
  assert(legalEntityId > 0, "Failed to create legal entity fixture");
  assert(Boolean(legalEntityCode), "Legal entity code should exist");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
      )
      VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `PRP05_CAL_${stamp}`, `PRP05 Calendar ${stamp}`]
  );
  const calendarRows = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP05_CAL_${stamp}`]
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
      `PRP05_BOOK_${stamp}`,
      `PRP05 Book ${stamp}`,
      currencyCode,
    ]
  );

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `PRP05_COA_${stamp}`, `PRP05 Chart ${stamp}`]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP05_COA_${stamp}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts fixture");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'EXPENSE', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP05EXP${stamp}`, `PRP05 Expense GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP05LIA${stamp}`, `PRP05 Liability GL ${stamp}`]
  );
  const expenseRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP05 Expense GL ${stamp}`]
  );
  const liabilityRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP05 Liability GL ${stamp}`]
  );
  const expenseGlAccountId = toNumber(expenseRows.rows?.[0]?.id);
  const liabilityGlAccountId = toNumber(liabilityRows.rows?.[0]?.id);
  assert(expenseGlAccountId > 0, "Failed to create expense GL fixture");
  assert(liabilityGlAccountId > 0, "Failed to create liability GL fixture");

  const passwordHash = await bcrypt.hash("PRP05#Smoke123", 10);
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, `prp05_user_${stamp}@example.com`, passwordHash, "PRP05 User"]
  );
  const userRows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, `prp05_user_${stamp}@example.com`]
  );
  const userId = toNumber(userRows.rows?.[0]?.id);
  assert(userId > 0, "Failed to create user fixture");

  return {
    tenantId,
    legalEntityId,
    legalEntityCode,
    userId,
    currencyCode,
    expenseGlAccountId,
    liabilityGlAccountId,
  };
}

async function createFinalizedRunWithLiabilities({
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

  const preview = await getPayrollRunAccrualPreview({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    assertScopeAccess: noScopeGuard,
  });
  assert((preview?.component_totals || []).length > 0, "Accrual preview should contain component totals");

  for (const component of preview.component_totals || []) {
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
        notes: "PRP05 mapping setup",
      },
      assertScopeAccess: noScopeGuard,
    });
  }

  const reviewed = await markPayrollRunReviewed({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "review for PRP05",
    assertScopeAccess: noScopeGuard,
  });
  assert(reviewed?.idempotentReplay === false, "Run review should not be replay");

  const finalized = await finalizePayrollRunAccrual({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "finalize for PRP05",
    forceFromImported: false,
    assertScopeAccess: noScopeGuard,
  });
  const accrualJournalEntryId = toNumber(finalized?.accrualJournalEntryId);
  assert(accrualJournalEntryId > 0, "Finalize should produce accrual journal entry");

  const built = await buildPayrollRunLiabilities({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "build liabilities for PRP05",
    assertScopeAccess: noScopeGuard,
  });
  assert((built?.items || []).length === 7, "Liability build should create 7 rows");

  return {
    runId,
    accrualJournalEntryId,
    runDetail: built?.run || null,
    liabilitySummary: built?.summary || null,
  };
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithP05Fixtures(stamp);
  const providerCode = `PRP05_${stamp}`;

  const original = await createFinalizedRunWithLiabilities({
    fixture,
    providerCode,
    sourceRef: `PRP05-ORIG-${stamp}`,
    csvText: buildOriginalCsv(),
  });
  assert(toAmount(original?.liabilitySummary?.total_open) === 2660, "Original run OPEN liabilities should total 2660");
  assert(toNumber(original?.liabilitySummary?.cancelled_count) === 0, "Original run should not have cancelled liabilities before reverse");

  const correctionShellKey = `PRP05-SHELL-${stamp}`;
  const correctionShell1 = await createPayrollCorrectionShell({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    input: {
      correctionType: "RETRO",
      originalRunId: original.runId,
      entityCode: null,
      providerCode: null,
      payrollPeriod: null,
      payDate: null,
      currencyCode: null,
      reason: "Retro correction shell for payroll adjustments",
      idempotencyKey: correctionShellKey,
    },
    assertScopeAccess: noScopeGuard,
  });
  const correctionRunId = toNumber(correctionShell1?.correction_run?.id);
  assert(correctionRunId > 0, "Correction shell should create correction run");
  assert(normalizeUpperText(correctionShell1?.correction_run?.status) === "DRAFT", "Correction shell run should start DRAFT");
  assert(normalizeUpperText(correctionShell1?.correction_run?.run_type) === "RETRO", "Correction shell run_type should be RETRO");
  assert(
    toNumber(correctionShell1?.correction_run?.correction_of_run_id) === original.runId,
    "Correction shell should link to original run"
  );
  assert(normalizeUpperText(correctionShell1?.correction?.status) === "CREATED", "Correction row should be CREATED");

  const correctionShell2 = await createPayrollCorrectionShell({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    input: {
      correctionType: "RETRO",
      originalRunId: original.runId,
      entityCode: null,
      providerCode: null,
      payrollPeriod: null,
      payDate: null,
      currencyCode: null,
      reason: "Retro correction shell idempotent replay",
      idempotencyKey: correctionShellKey,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(correctionShell2?.idempotent === true, "Correction shell replay should be idempotent");
  assert(
    toNumber(correctionShell2?.correction_run?.id) === correctionRunId,
    "Idempotent correction shell should return same correction run"
  );

  const importedCorrection = await importPayrollRunCsv({
    req: null,
    payload: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      legalEntityId: null,
      targetRunId: correctionRunId,
      providerCode,
      payrollPeriod: "2026-02-01",
      payDate: "2026-02-15",
      currencyCode: fixture.currencyCode,
      sourceBatchRef: `PRP05-CORR-${stamp}`,
      originalFilename: `prp05-correction-${stamp}.csv`,
      csvText: buildCorrectionCsv(),
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(toNumber(importedCorrection?.id) === correctionRunId, "Correction import should target the shell run");
  assert(normalizeUpperText(importedCorrection?.status) === "IMPORTED", "Correction run should become IMPORTED");
  assert(
    toNumber(importedCorrection?.line_count_inserted) === 2,
    "Correction run should insert two lines"
  );
  assert(
    toNumber(importedCorrection?.line_count_duplicates) === 0,
    "Correction run should not have duplicates"
  );

  await markPayrollRunReviewed({
    req: null,
    tenantId: fixture.tenantId,
    runId: correctionRunId,
    userId: fixture.userId,
    note: "review correction run",
    assertScopeAccess: noScopeGuard,
  });
  const correctionFinalize = await finalizePayrollRunAccrual({
    req: null,
    tenantId: fixture.tenantId,
    runId: correctionRunId,
    userId: fixture.userId,
    note: "finalize correction run",
    forceFromImported: false,
    assertScopeAccess: noScopeGuard,
  });
  const correctionAccrualJeId = toNumber(correctionFinalize?.accrualJournalEntryId);
  assert(correctionAccrualJeId > 0, "Correction finalize should create accrual JE");

  const reverseKey = `PRP05-REVERSE-${stamp}`;
  const reverse1 = await reversePayrollRunWithCorrection({
    req: null,
    tenantId: fixture.tenantId,
    runId: original.runId,
    userId: fixture.userId,
    reason: "Original payroll run should be reversed after correction",
    note: "PRP05 reverse smoke",
    idempotencyKey: reverseKey,
    assertScopeAccess: noScopeGuard,
  });
  const reversalRunId = toNumber(reverse1?.reversal_run?.id);
  const reversalAccrualJeId = toNumber(reverse1?.reversal_accrual_journal_entry_id);
  assert(reverse1?.idempotent === false, "First reverse should not be idempotent replay");
  assert(reversalRunId > 0, "Reverse should create reversal run");
  assert(reversalAccrualJeId > 0, "Reverse should create reversal accrual JE");
  assert(normalizeUpperText(reverse1?.original_run?.is_reversed ? "YES" : "NO") === "YES", "Original run should be marked reversed");
  assert(
    toNumber(reverse1?.original_run?.reversed_by_run_id) === reversalRunId,
    "Original run should point to reversal run"
  );
  assert(normalizeUpperText(reverse1?.reversal_run?.run_type) === "REVERSAL", "Reversal run type should be REVERSAL");
  assert(normalizeUpperText(reverse1?.reversal_run?.status) === "FINALIZED", "Reversal run should be FINALIZED");
  assert(
    toNumber(reverse1?.reversal_run?.correction_of_run_id) === original.runId,
    "Reversal run correction_of_run_id should point to original run"
  );

  const origLiabilitiesAfterReverse = await getPayrollRunLiabilitiesDetail({
    req: null,
    tenantId: fixture.tenantId,
    runId: original.runId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toAmount(origLiabilitiesAfterReverse?.summary?.total_cancelled) === 2660,
    "Original run cancelled liability amount should be 2660"
  );
  assert(
    (origLiabilitiesAfterReverse?.items || []).length === 7 &&
      (origLiabilitiesAfterReverse?.items || []).every(
        (row) => normalizeUpperText(row?.status) === "CANCELLED"
      ),
    "Original run liabilities should all be CANCELLED by reversal"
  );
  assert(
    toNumber(origLiabilitiesAfterReverse?.summary?.total_open) === 0 &&
      toNumber(origLiabilitiesAfterReverse?.summary?.total_in_batch) === 0 &&
      toNumber(origLiabilitiesAfterReverse?.summary?.total_paid) === 0 &&
      toNumber(origLiabilitiesAfterReverse?.summary?.total_partially_paid) === 0,
    "Original run should have no OPEN/IN_BATCH/PAID/PARTIALLY_PAID liabilities after reversal"
  );

  const journalRows = await query(
    `SELECT id, status, reversal_journal_entry_id
     FROM journal_entries
     WHERE tenant_id = ?
       AND id IN (?, ?)
     ORDER BY id ASC`,
    [fixture.tenantId, original.accrualJournalEntryId, reversalAccrualJeId]
  );
  const journalById = new Map((journalRows.rows || []).map((row) => [toNumber(row.id), row]));
  const originalAccrualJe = journalById.get(original.accrualJournalEntryId);
  const reversalAccrualJe = journalById.get(reversalAccrualJeId);
  assert(originalAccrualJe, "Original accrual journal should exist");
  assert(reversalAccrualJe, "Reversal accrual journal should exist");
  assert(normalizeUpperText(originalAccrualJe?.status) === "REVERSED", "Original accrual JE should be REVERSED");
  assert(
    toNumber(originalAccrualJe?.reversal_journal_entry_id) === reversalAccrualJeId,
    "Original accrual JE should point to reversal journal id"
  );
  assert(normalizeUpperText(reversalAccrualJe?.status) === "POSTED", "Reversal accrual JE should be POSTED");

  const correctionsList = await listPayrollRunCorrections({
    req: null,
    tenantId: fixture.tenantId,
    runId: original.runId,
    assertScopeAccess: noScopeGuard,
  });
  const correctionRows = correctionsList?.items || [];
  const retroRow = correctionRows.find(
    (row) =>
      toNumber(row?.correction_run_id) === correctionRunId &&
      normalizeUpperText(row?.correction_type) === "RETRO"
  );
  const reversalRow = correctionRows.find(
    (row) =>
      toNumber(row?.correction_run_id) === reversalRunId &&
      normalizeUpperText(row?.correction_type) === "REVERSAL"
  );
  assert(retroRow, "Corrections list should include RETRO correction shell row");
  assert(reversalRow, "Corrections list should include REVERSAL row");
  assert(normalizeUpperText(reversalRow?.status) === "APPLIED", "REVERSAL correction row should be APPLIED");

  const reverse2 = await reversePayrollRunWithCorrection({
    req: null,
    tenantId: fixture.tenantId,
    runId: original.runId,
    userId: fixture.userId,
    reason: "Idempotent reverse replay",
    note: "PRP05 reverse replay",
    idempotencyKey: reverseKey,
    assertScopeAccess: noScopeGuard,
  });
  assert(reverse2?.idempotent === true, "Second reverse call should be idempotent");
  assert(
    toNumber(reverse2?.reversal_run?.id) === reversalRunId,
    "Idempotent reverse should return existing reversal run"
  );

  await expectFailure(
    () =>
      listPayrollRunCorrections({
        req: null,
        tenantId: fixture.tenantId,
        runId: original.runId,
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  await expectFailure(
    () =>
      createPayrollCorrectionShell({
        req: null,
        tenantId: fixture.tenantId,
        userId: fixture.userId,
        input: {
          correctionType: "OFF_CYCLE",
          originalRunId: null,
          entityCode: fixture.legalEntityCode,
          providerCode,
          payrollPeriod: "2026-02-01",
          payDate: "2026-02-15",
          currencyCode: fixture.currencyCode,
          reason: "Permission check",
          idempotencyKey: `PRP05-PERM-${stamp}`,
        },
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  console.log(
    "PR-P05 smoke test passed (correction shell + import to target shell + finalize + reversal + liabilities cancelled + corrections list + idempotent reverse + permission checks)."
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
