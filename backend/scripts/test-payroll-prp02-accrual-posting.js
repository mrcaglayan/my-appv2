import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  finalizePayrollRunAccrual,
  getPayrollRunAccrualPreview,
  markPayrollRunReviewed,
} from "../src/services/payroll.accruals.service.js";
import {
  listPayrollComponentMappingRows,
  upsertPayrollComponentMapping,
} from "../src/services/payroll.mappings.service.js";
import { getPayrollRunByIdForTenant, importPayrollRunCsv } from "../src/services/payroll.runs.service.js";

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

function noScopeGuard() {
  return true;
}

function allowAllScopeFilter() {
  return "1 = 1";
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

function buildValidCsv() {
  return [
    "employee_code,employee_name,cost_center_code,base_salary,overtime_pay,bonus_pay,allowances_total,gross_pay,employee_tax,employee_social_security,other_deductions,employer_tax,employer_social_security,net_pay",
    "E001,Alpha User,CC-01,1000,100,50,50,1200,120,80,20,150,100,980",
    "E002,Beta User,CC-02,900,0,0,100,1000,100,50,10,120,90,840",
  ].join("\n");
}

async function createTenantWithAccrualFixtures(stamp) {
  const tenantCode = `PRP02_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRP02 Tenant ${stamp}`]
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
    [tenantId, `PRP02_G_${stamp}`, `PRP02 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP02_G_${stamp}`]
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
      `PRP02_LE_${stamp}`,
      `PRP02 Legal Entity ${stamp}`,
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
    [tenantId, `PRP02_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
      )
      VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `PRP02_CAL_${stamp}`, `PRP02 Calendar ${stamp}`]
  );
  const calendarRows = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP02_CAL_${stamp}`]
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
      `PRP02_BOOK_${stamp}`,
      `PRP02 Book ${stamp}`,
      currencyCode,
    ]
  );

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `PRP02_COA_${stamp}`, `PRP02 Chart ${stamp}`]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP02_COA_${stamp}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts fixture");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'EXPENSE', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP02EXP${stamp}`, `PRP02 Expense GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, NULL, TRUE)`,
    [coaId, `PRP02LIA${stamp}`, `PRP02 Liability GL ${stamp}`]
  );
  const expenseRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP02 Expense GL ${stamp}`]
  );
  const liabilityRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRP02 Liability GL ${stamp}`]
  );
  const expenseGlAccountId = toNumber(expenseRows.rows?.[0]?.id);
  const liabilityGlAccountId = toNumber(liabilityRows.rows?.[0]?.id);
  assert(expenseGlAccountId > 0, "Failed to create expense account fixture");
  assert(liabilityGlAccountId > 0, "Failed to create liability account fixture");

  const passwordHash = await bcrypt.hash("PRP02#Smoke123", 10);
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, `prp02_user_${stamp}@example.com`, passwordHash, "PRP02 User"]
  );
  const userRows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, `prp02_user_${stamp}@example.com`]
  );
  const userId = toNumber(userRows.rows?.[0]?.id);
  assert(userId > 0, "Failed to create user fixture");

  return {
    tenantId,
    legalEntityId,
    userId,
    currencyCode,
    expenseGlAccountId,
    liabilityGlAccountId,
  };
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithAccrualFixtures(stamp);
  const providerCode = `PRP02_${stamp}`;

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
      sourceBatchRef: `PRP02-SRC-${stamp}`,
      originalFilename: `prp02-${stamp}.csv`,
      csvText: buildValidCsv(),
    },
    assertScopeAccess: noScopeGuard,
  });
  const runId = toNumber(imported?.id);
  assert(runId > 0, "importPayrollRunCsv should return run id");
  assert(String(imported?.status || "").toUpperCase() === "IMPORTED", "Imported run should be IMPORTED");

  const previewBeforeMappings = await getPayrollRunAccrualPreview({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    (previewBeforeMappings?.component_totals || []).length > 0,
    "Accrual preview should expose component totals"
  );
  assert(
    (previewBeforeMappings?.missing_mappings || []).length > 0,
    "Accrual preview should report missing mappings before mapping setup"
  );
  assert(
    (previewBeforeMappings?.posting_lines || []).length === 0,
    "Accrual preview posting lines should be empty when mappings are missing"
  );
  assert(previewBeforeMappings?.can_finalize === false, "Run cannot finalize before mappings + review");

  await expectFailure(
    () =>
      finalizePayrollRunAccrual({
        req: null,
        tenantId: fixture.tenantId,
        runId,
        userId: fixture.userId,
        note: "should fail before review",
        forceFromImported: false,
        assertScopeAccess: noScopeGuard,
      }),
    { status: 400, includes: "must be REVIEWED before finalize" }
  );

  const mappingIds = [];
  for (const component of previewBeforeMappings.component_totals || []) {
    const entrySide = String(component?.entry_side || "").toUpperCase();
    const row = await upsertPayrollComponentMapping({
      req: null,
      payload: {
        tenantId: fixture.tenantId,
        userId: fixture.userId,
        legalEntityId: fixture.legalEntityId,
        entityCodeInput: null,
        providerCode,
        currencyCode: fixture.currencyCode,
        componentCode: String(component?.component_code || "").toUpperCase(),
        entrySide,
        glAccountId: entrySide === "DEBIT" ? fixture.expenseGlAccountId : fixture.liabilityGlAccountId,
        effectiveFrom: "2026-01-01",
        effectiveTo: null,
        closePreviousOpenMapping: true,
        notes: "PRP02 smoke mapping",
      },
      assertScopeAccess: noScopeGuard,
    });
    const mappingId = toNumber(row?.id);
    assert(mappingId > 0, `Mapping upsert should return id for ${component?.component_code}`);
    mappingIds.push(mappingId);
  }

  const listedMappings = await listPayrollComponentMappingRows({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      tenantId: fixture.tenantId,
      legalEntityId: fixture.legalEntityId,
      entityCode: null,
      providerCode,
      currencyCode: fixture.currencyCode,
      componentCode: null,
      asOfDate: "2026-02-15",
      activeOnly: true,
      limit: 200,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    (listedMappings.rows || []).length === (previewBeforeMappings.component_totals || []).length,
    "Mapping list should contain one active mapping per component"
  );

  const previewAfterMappings = await getPayrollRunAccrualPreview({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    (previewAfterMappings?.missing_mappings || []).length === 0,
    "Accrual preview should have no missing mappings after setup"
  );
  assert(
    (previewAfterMappings?.posting_lines || []).length === (previewBeforeMappings?.component_totals || []).length,
    "Accrual preview posting lines should be generated for all components"
  );
  assert(previewAfterMappings?.is_balanced === true, "Accrual preview should be balanced");
  assert(previewAfterMappings?.can_finalize === false, "Run still cannot finalize before review");

  const reviewed1 = await markPayrollRunReviewed({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "review run",
    assertScopeAccess: noScopeGuard,
  });
  assert(reviewed1?.idempotentReplay === false, "First review should not be idempotent replay");
  assert(String(reviewed1?.status || "").toUpperCase() === "REVIEWED", "Run status should become REVIEWED");

  const reviewed2 = await markPayrollRunReviewed({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "review run idempotent replay",
    assertScopeAccess: noScopeGuard,
  });
  assert(reviewed2?.idempotentReplay === true, "Second review should be idempotent replay");
  assert(String(reviewed2?.status || "").toUpperCase() === "REVIEWED", "Run status should stay REVIEWED");

  const previewReviewed = await getPayrollRunAccrualPreview({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    assertScopeAccess: noScopeGuard,
  });
  assert(previewReviewed?.can_finalize === true, "Run should be finalizable after review");

  const finalized1 = await finalizePayrollRunAccrual({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "finalize accrual",
    forceFromImported: false,
    assertScopeAccess: noScopeGuard,
  });
  const accrualJournalEntryId = toNumber(finalized1?.accrualJournalEntryId);
  assert(accrualJournalEntryId > 0, "Finalize should return accrual journal entry id");
  assert(finalized1?.idempotentReplay === false, "First finalize should not be idempotent replay");

  const runAfterFinalize = await getPayrollRunByIdForTenant({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    assertScopeAccess: noScopeGuard,
  });
  assert(String(runAfterFinalize?.status || "").toUpperCase() === "FINALIZED", "Run should be FINALIZED");
  assert(
    toNumber(runAfterFinalize?.accrual_journal_entry_id) === accrualJournalEntryId,
    "Run accrual_journal_entry_id should match finalize result"
  );

  const finalized2 = await finalizePayrollRunAccrual({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    userId: fixture.userId,
    note: "finalize idempotent replay",
    forceFromImported: false,
    assertScopeAccess: noScopeGuard,
  });
  assert(finalized2?.idempotentReplay === true, "Second finalize should be idempotent replay");
  assert(
    toNumber(finalized2?.accrualJournalEntryId) === accrualJournalEntryId,
    "Idempotent finalize should keep same accrual journal entry id"
  );

  const journalRows = await query(
    `SELECT id, status, total_debit_base, total_credit_base
     FROM journal_entries
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, accrualJournalEntryId]
  );
  const journal = journalRows.rows?.[0] || null;
  assert(journal, "Accrual journal should exist");
  assert(String(journal?.status || "").toUpperCase() === "POSTED", "Accrual journal should be POSTED");
  assert(
    toAmount(journal?.total_debit_base) === toAmount(previewReviewed?.debit_total),
    "Accrual journal debit total should match preview"
  );
  assert(
    toAmount(journal?.total_credit_base) === toAmount(previewReviewed?.credit_total),
    "Accrual journal credit total should match preview"
  );

  const journalLineAggRows = await query(
    `SELECT
        COUNT(*) AS line_count,
        COALESCE(SUM(debit_base), 0) AS debit_total,
        COALESCE(SUM(credit_base), 0) AS credit_total
     FROM journal_lines
     WHERE journal_entry_id = ?`,
    [accrualJournalEntryId]
  );
  const journalLineAgg = journalLineAggRows.rows?.[0] || null;
  assert(journalLineAgg, "Accrual journal lines aggregate should exist");
  assert(
    toNumber(journalLineAgg?.line_count) === (previewReviewed?.posting_lines || []).length,
    "Accrual journal line count should match posting preview lines"
  );
  assert(
    toAmount(journalLineAgg?.debit_total) === toAmount(previewReviewed?.debit_total),
    "Accrual journal lines debit sum should match preview debit total"
  );
  assert(
    toAmount(journalLineAgg?.credit_total) === toAmount(previewReviewed?.credit_total),
    "Accrual journal lines credit sum should match preview credit total"
  );

  const runAuditRows = await query(
    `SELECT action, COUNT(*) AS row_count
     FROM payroll_run_audit
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND run_id = ?
     GROUP BY action`,
    [fixture.tenantId, fixture.legalEntityId, runId]
  );
  const runAuditMap = new Map(
    (runAuditRows.rows || []).map((row) => [String(row.action || "").toUpperCase(), toNumber(row.row_count)])
  );
  assert((runAuditMap.get("VALIDATION") || 0) >= 1, "Run audit should include VALIDATION action");
  assert((runAuditMap.get("IMPORTED") || 0) >= 1, "Run audit should include IMPORTED action");
  assert((runAuditMap.get("STATUS") || 0) >= 2, "Run audit should include REVIEWED and FINALIZED status actions");

  const mappingAuditRows = await query(
    `SELECT COUNT(*) AS row_count
     FROM payroll_component_gl_mapping_audit
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND action = 'CREATED'
       AND mapping_id IN (${mappingIds.map(() => "?").join(",")})`,
    [fixture.tenantId, fixture.legalEntityId, ...mappingIds]
  );
  assert(
    toNumber(mappingAuditRows.rows?.[0]?.row_count) === mappingIds.length,
    "Mapping audit should contain CREATED rows for inserted mappings"
  );

  console.log(
    "PR-P02 smoke test passed (preview missing mappings -> map components -> review -> finalize + idempotent finalize + journal/audit assertions)."
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
