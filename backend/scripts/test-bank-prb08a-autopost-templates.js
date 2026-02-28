import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  createPostingTemplate,
  listPostingTemplateRows,
  updatePostingTemplate,
} from "../src/services/bank.reconciliationPostingTemplates.service.js";
import { createReconciliationRule } from "../src/services/bank.reconciliationRules.service.js";
import {
  applyBankReconciliationAutoRun,
  previewBankReconciliationAutoRun,
} from "../src/services/bank.reconciliationEngine.service.js";
import { autoPostTemplateAndReconcileStatementLine } from "../src/services/bank.reconciliationAutoPosting.service.js";

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
  return Number.isFinite(parsed) ? Number(parsed.toFixed(6)) : 0;
}

function allowAllScopeFilter() {
  return "1 = 1";
}

function noScopeGuard() {
  return true;
}

async function createTenantWithB08AFixtures(stamp) {
  const tenantCode = `PRB08A_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRB08A Tenant ${stamp}`]
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
    [tenantId, `PRB08A_G_${stamp}`, `PRB08A Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB08A_G_${stamp}`]
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
      `PRB08A_LE_${stamp}`,
      `PRB08A Legal Entity ${stamp}`,
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
    [tenantId, `PRB08A_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
      )
      VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `PRB08A_CAL_${stamp}`, `PRB08A Calendar ${stamp}`]
  );
  const calendarRows = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB08A_CAL_${stamp}`]
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
      `PRB08A_BOOK_${stamp}`,
      `PRB08A Book ${stamp}`,
      currencyCode,
    ]
  );
  const bookRows = await query(
    `SELECT id
     FROM books
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, `PRB08A_BOOK_${stamp}`]
  );
  const bookId = toNumber(bookRows.rows?.[0]?.id);
  assert(bookId > 0, "Failed to create book fixture");

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `PRB08A_COA_${stamp}`, `PRB08A Chart ${stamp}`]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB08A_COA_${stamp}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts fixture");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB08ABANK${stamp}`, `PRB08A Bank GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'EXPENSE', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB08ACNT${stamp}`, `PRB08A Counter GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB08ATAX${stamp}`, `PRB08A Tax GL ${stamp}`]
  );
  const bankGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB08A Bank GL ${stamp}`]
  );
  const counterGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB08A Counter GL ${stamp}`]
  );
  const taxGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB08A Tax GL ${stamp}`]
  );
  const bankGlAccountId = toNumber(bankGlRows.rows?.[0]?.id);
  const counterGlAccountId = toNumber(counterGlRows.rows?.[0]?.id);
  const taxGlAccountId = toNumber(taxGlRows.rows?.[0]?.id);
  assert(bankGlAccountId > 0, "Failed to create bank GL account fixture");
  assert(counterGlAccountId > 0, "Failed to create counter GL account fixture");
  assert(taxGlAccountId > 0, "Failed to create tax GL account fixture");

  const passwordHash = await bcrypt.hash("PRB08A#Smoke123", 10);
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, `prb08a_user_${stamp}@example.com`, passwordHash, "PRB08A User"]
  );
  const userRows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, `prb08a_user_${stamp}@example.com`]
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
      `PRB08A_BA_${stamp}`,
      `PRB08A Bank Account ${stamp}`,
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
    [tenantId, legalEntityId, `PRB08A_BA_${stamp}`]
  );
  const bankAccountId = toNumber(bankAccountRows.rows?.[0]?.id);
  assert(bankAccountId > 0, "Failed to create bank account fixture");

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
      tenantId,
      legalEntityId,
      bankAccountId,
      `prb08a-${stamp}.csv`,
      `prb08a-checksum-${stamp}`,
      JSON.stringify({ source: "smoke-test" }),
      userId,
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
    [tenantId, legalEntityId, bankAccountId, `prb08a-checksum-${stamp}`]
  );
  const importId = toNumber(importRows.rows?.[0]?.id);
  assert(importId > 0, "Failed to create statement import fixture");

  const lineHash = `PRB08A-LINE-${stamp}`.padEnd(64, "0").slice(0, 64);
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
      VALUES (?, ?, ?, ?, 1, '2026-02-12', '2026-02-12', ?, ?, -120.000000, ?, 880.000000, ?, 'UNMATCHED', ?)`,
    [
      tenantId,
      legalEntityId,
      importId,
      bankAccountId,
      "Office expense payment for PRB08A",
      "PRB08A-REF-001",
      currencyCode,
      lineHash,
      JSON.stringify({ rowNo: 1 }),
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
    [tenantId, legalEntityId, importId]
  );
  const lineId = toNumber(lineRows.rows?.[0]?.id);
  assert(lineId > 0, "Failed to create statement line fixture");

  return {
    tenantId,
    legalEntityId,
    currencyCode,
    userId,
    bankAccountId,
    importId,
    lineId,
    bookId,
    bankGlAccountId,
    counterGlAccountId,
    taxGlAccountId,
  };
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithB08AFixtures(stamp);

  const createdTemplate = await createPostingTemplate({
    req: null,
    input: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      templateCode: `PRB08A_TPL_${stamp}`,
      templateName: "PRB08A outflow template",
      status: "ACTIVE",
      scopeType: "LEGAL_ENTITY",
      legalEntityId: fixture.legalEntityId,
      bankAccountId: null,
      entryKind: "BANK_MISC",
      directionPolicy: "OUTFLOW_ONLY",
      counterAccountId: fixture.counterGlAccountId,
      taxAccountId: null,
      taxMode: "NONE",
      taxRate: null,
      currencyCode: fixture.currencyCode,
      minAmountAbs: 50,
      maxAmountAbs: 500,
      descriptionMode: "PREFIXED",
      fixedDescription: null,
      descriptionPrefix: "AUTO-POST",
      journalSourceCode: "BANK_AUTO_POST",
      journalDocType: "BANK_AUTO",
      effectiveFrom: null,
      effectiveTo: null,
    },
    assertScopeAccess: noScopeGuard,
  });
  const templateId = toNumber(createdTemplate?.row?.id);
  assert(templateId > 0, "Failed to create posting template");
  assert(
    createdTemplate?.approval_required === false,
    "Posting template create should not require approval in smoke tenant"
  );

  const listedTemplates = await listPostingTemplateRows({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      legalEntityId: fixture.legalEntityId,
      bankAccountId: null,
      status: "ACTIVE",
      q: "PRB08A_TPL_",
      limit: 50,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    (listedTemplates.rows || []).some((row) => toNumber(row?.id) === templateId),
    "Posting template list should include created template"
  );

  const updatedTemplate = await updatePostingTemplate({
    req: null,
    input: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      templateId,
      templateName: "PRB08A outflow template updated",
      descriptionMode: "FIXED_TEXT",
      fixedDescription: "PRB08A AUTOPOST",
      descriptionPrefix: null,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(updatedTemplate?.row?.template_name || "").includes("updated"),
    "Posting template update should persist updated template_name"
  );
  assert(
    String(updatedTemplate?.row?.description_mode || "").toUpperCase() === "FIXED_TEXT",
    "Posting template update should persist description_mode=FIXED_TEXT"
  );

  const createdRule = await createReconciliationRule({
    req: null,
    input: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      ruleCode: `PRB08A_RULE_${stamp}`,
      ruleName: "PRB08A auto post by reference",
      status: "ACTIVE",
      priority: 1,
      scopeType: "GLOBAL",
      legalEntityId: null,
      bankAccountId: null,
      matchType: "PAYMENT_BY_TEXT_AND_AMOUNT",
      conditions: {
        referenceIncludesAny: ["PRB08A-REF-001"],
      },
      actionType: "AUTO_POST_TEMPLATE",
      actionPayload: {
        postingTemplateId: templateId,
      },
      stopOnMatch: true,
      effectiveFrom: null,
      effectiveTo: null,
    },
    assertScopeAccess: noScopeGuard,
  });
  const ruleId = toNumber(createdRule?.row?.id);
  assert(ruleId > 0, "Failed to create reconciliation rule for AUTO_POST_TEMPLATE");

  const preview = await previewBankReconciliationAutoRun({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      legalEntityId: fixture.legalEntityId,
      bankAccountId: fixture.bankAccountId,
      dateFrom: "2026-02-01",
      dateTo: "2026-02-28",
      limit: 100,
      userId: fixture.userId,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(preview?.summary?.scannedCount) === 1,
    "B08A preview should scan one statement line"
  );
  assert(
    String(preview?.rows?.[0]?.outcome || "").toUpperCase() === "AUTO_POST_READY",
    "B08A preview should produce AUTO_POST_READY outcome"
  );
  assert(
    String(preview?.rows?.[0]?.target?.entityType || "").toUpperCase() === "POSTING_TEMPLATE",
    "B08A preview target entity should be POSTING_TEMPLATE"
  );
  assert(
    toNumber(preview?.rows?.[0]?.target?.entityId) === templateId,
    "B08A preview target template id should match created template"
  );

  const runRequestId = `PRB08A_APPLY_${stamp}`;
  const apply1 = await applyBankReconciliationAutoRun({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      legalEntityId: fixture.legalEntityId,
      bankAccountId: fixture.bankAccountId,
      dateFrom: "2026-02-01",
      dateTo: "2026-02-28",
      limit: 100,
      userId: fixture.userId,
    },
    runRequestId,
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(apply1?.replay === false, "First B08A apply run should not be replay");
  assert(
    String(apply1?.run?.status || "").toUpperCase() === "SUCCESS",
    "B08A apply status should be SUCCESS"
  );
  assert(
    toNumber(apply1?.summary?.reconciledCount) === 1,
    "B08A apply should reconcile one statement line"
  );
  assert(
    String(apply1?.rows?.[0]?.outcome || "").toUpperCase() === "AUTO_POSTED_RECONCILED",
    "B08A apply row should be AUTO_POSTED_RECONCILED"
  );
  assert(
    toNumber(apply1?.rows?.[0]?.autoPosting?.templateId) === templateId,
    "B08A apply autoPosting templateId should match"
  );

  const apply2 = await applyBankReconciliationAutoRun({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      legalEntityId: fixture.legalEntityId,
      bankAccountId: fixture.bankAccountId,
      dateFrom: "2026-02-01",
      dateTo: "2026-02-28",
      limit: 100,
      userId: fixture.userId,
    },
    runRequestId,
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(apply2?.replay === true, "Second B08A apply with same request should replay");
  assert(
    toNumber(apply2?.run?.id) === toNumber(apply1?.run?.id),
    "B08A replay run should return the same run id"
  );

  const lineRows = await query(
    `SELECT
        id,
        recon_status,
        reconciliation_method,
        reconciliation_rule_id,
        auto_post_template_id,
        auto_post_journal_entry_id
     FROM bank_statement_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, fixture.lineId]
  );
  const line = lineRows.rows?.[0] || null;
  assert(line, "Statement line should exist after B08A apply");
  assert(
    String(line?.recon_status || "").toUpperCase() === "MATCHED",
    "Statement line should be MATCHED after B08A auto-post apply"
  );
  assert(
    String(line?.reconciliation_method || "").toUpperCase() === "RULE_AUTO_POST",
    "Statement line reconciliation_method should be RULE_AUTO_POST"
  );
  assert(
    toNumber(line?.reconciliation_rule_id) === ruleId,
    "Statement line reconciliation_rule_id should match created rule"
  );
  assert(
    toNumber(line?.auto_post_template_id) === templateId,
    "Statement line auto_post_template_id should match template"
  );
  const journalEntryId = toNumber(line?.auto_post_journal_entry_id);
  assert(journalEntryId > 0, "Statement line should store auto_post_journal_entry_id");

  const journalRows = await query(
    `SELECT id, journal_no, status, total_debit_base, total_credit_base
     FROM journal_entries
     WHERE id = ?
       AND tenant_id = ?
       AND legal_entity_id = ?
     LIMIT 1`,
    [journalEntryId, fixture.tenantId, fixture.legalEntityId]
  );
  const journal = journalRows.rows?.[0] || null;
  assert(journal, "Auto-post journal entry should exist");
  assert(
    String(journal?.journal_no || "") === `BAP-${fixture.lineId}`,
    "Auto-post journal_no should follow BAP-{lineId}"
  );
  assert(
    String(journal?.status || "").toUpperCase() === "POSTED",
    "Auto-post journal must be POSTED"
  );
  assert(
    toAmount(journal?.total_debit_base) === 120 && toAmount(journal?.total_credit_base) === 120,
    "Auto-post journal totals should be balanced at 120/120"
  );

  const journalLines = await query(
    `SELECT line_no, account_id, debit_base, credit_base
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no ASC`,
    [journalEntryId]
  );
  assert(journalLines.rows.length === 2, "Auto-post journal should create exactly 2 journal lines");
  assert(
    toNumber(journalLines.rows?.[0]?.account_id) === fixture.counterGlAccountId &&
      toAmount(journalLines.rows?.[0]?.debit_base) === 120 &&
      toAmount(journalLines.rows?.[0]?.credit_base) === 0,
    "Outflow auto-post line 1 should debit counter account"
  );
  assert(
    toNumber(journalLines.rows?.[1]?.account_id) === fixture.bankGlAccountId &&
      toAmount(journalLines.rows?.[1]?.debit_base) === 0 &&
      toAmount(journalLines.rows?.[1]?.credit_base) === 120,
    "Outflow auto-post line 2 should credit bank GL account"
  );

  const autoPostingRows = await query(
    `SELECT
        bank_reconciliation_posting_template_id,
        journal_entry_id,
        status,
        posted_amount,
        currency_code
     FROM bank_reconciliation_auto_postings
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND statement_line_id = ?
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, fixture.lineId]
  );
  const autoPosting = autoPostingRows.rows?.[0] || null;
  assert(autoPosting, "Auto-post trace should be stored in bank_reconciliation_auto_postings");
  assert(
    toNumber(autoPosting?.bank_reconciliation_posting_template_id) === templateId,
    "Auto-post trace should reference template id"
  );
  assert(
    toNumber(autoPosting?.journal_entry_id) === journalEntryId,
    "Auto-post trace should reference created journal entry"
  );
  assert(
    String(autoPosting?.status || "").toUpperCase() === "POSTED",
    "Auto-post trace status should be POSTED"
  );
  assert(
    toAmount(autoPosting?.posted_amount) === 120 &&
      String(autoPosting?.currency_code || "").toUpperCase() === fixture.currencyCode,
    "Auto-post trace amount/currency should match statement line"
  );

  const matchRows = await query(
    `SELECT
        match_type,
        matched_entity_type,
        matched_entity_id,
        reconciliation_rule_id,
        matched_amount,
        status
     FROM bank_reconciliation_matches
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND statement_line_id = ?
       AND status = 'ACTIVE'`,
    [fixture.tenantId, fixture.legalEntityId, fixture.lineId]
  );
  assert(matchRows.rows.length === 1, "B08A apply should create exactly one active reconciliation match");
  const match = matchRows.rows[0];
  assert(
    String(match?.match_type || "").toUpperCase() === "AUTO_RULE",
    "B08A match_type should be AUTO_RULE"
  );
  assert(
    String(match?.matched_entity_type || "").toUpperCase() === "JOURNAL" &&
      toNumber(match?.matched_entity_id) === journalEntryId,
    "B08A match target should be the created journal entry"
  );
  assert(
    toNumber(match?.reconciliation_rule_id) === ruleId,
    "B08A match reconciliation_rule_id should match created rule"
  );
  assert(
    toAmount(match?.matched_amount) === 120,
    "B08A matched amount should be full statement amount abs(120)"
  );

  const directIdempotent = await autoPostTemplateAndReconcileStatementLine({
    req: null,
    tenantId: fixture.tenantId,
    lineId: fixture.lineId,
    templateId,
    userId: fixture.userId,
    ruleId,
    confidence: 95,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    directIdempotent?.idempotent === true,
    "Direct auto-post rerun should be idempotent after first apply"
  );
  assert(
    toNumber(directIdempotent?.journal?.id) === journalEntryId,
    "Idempotent rerun should reuse the same journal entry"
  );

  const autoPostingCountRows = await query(
    `SELECT COUNT(*) AS total
     FROM bank_reconciliation_auto_postings
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND statement_line_id = ?`,
    [fixture.tenantId, fixture.legalEntityId, fixture.lineId]
  );
  assert(
    toNumber(autoPostingCountRows.rows?.[0]?.total) === 1,
    "Idempotent rerun must not create duplicate auto-post trace rows"
  );

  const journalCountRows = await query(
    `SELECT COUNT(*) AS total
     FROM journal_entries
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND journal_no = ?`,
    [fixture.tenantId, fixture.legalEntityId, `BAP-${fixture.lineId}`]
  );
  assert(
    toNumber(journalCountRows.rows?.[0]?.total) === 1,
    "Idempotent rerun must not create duplicate auto-post journals"
  );

  const taxLineHash = `PRB08A-LINE-TAX-${stamp}`.padEnd(64, "0").slice(0, 64);
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
      VALUES (?, ?, ?, ?, 2, '2026-02-13', '2026-02-13', ?, ?, -118.000000, ?, 762.000000, ?, 'UNMATCHED', ?)`,
    [
      fixture.tenantId,
      fixture.legalEntityId,
      fixture.importId,
      fixture.bankAccountId,
      "PRB08A tax-included fee",
      "PRB08A-TAX-001",
      fixture.currencyCode,
      taxLineHash,
      JSON.stringify({ rowNo: 2 }),
    ]
  );
  const taxLineRows = await query(
    `SELECT id
     FROM bank_statement_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND import_id = ?
       AND line_no = 2
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, fixture.importId]
  );
  const taxLineId = toNumber(taxLineRows.rows?.[0]?.id);
  assert(taxLineId > 0, "Failed to create tax-included statement line fixture");

  const taxTemplate = await createPostingTemplate({
    req: null,
    input: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      templateCode: `PRB08A_TAXTPL_${stamp}`,
      templateName: "PRB08A tax-included outflow template",
      status: "ACTIVE",
      scopeType: "LEGAL_ENTITY",
      legalEntityId: fixture.legalEntityId,
      bankAccountId: null,
      entryKind: "BANK_MISC",
      directionPolicy: "OUTFLOW_ONLY",
      counterAccountId: fixture.counterGlAccountId,
      taxAccountId: fixture.taxGlAccountId,
      taxMode: "INCLUDED",
      taxRate: 18,
      currencyCode: fixture.currencyCode,
      minAmountAbs: 1,
      maxAmountAbs: 1000,
      descriptionMode: "FIXED_TEXT",
      fixedDescription: "PRB08A TAX INCLUDED",
      descriptionPrefix: null,
      journalSourceCode: "BANK_AUTO_POST",
      journalDocType: "BANK_AUTO",
      effectiveFrom: null,
      effectiveTo: null,
    },
    assertScopeAccess: noScopeGuard,
  });
  const taxTemplateId = toNumber(taxTemplate?.row?.id);
  assert(taxTemplateId > 0, "Failed to create tax-included posting template");

  const taxAutoPost = await autoPostTemplateAndReconcileStatementLine({
    req: null,
    tenantId: fixture.tenantId,
    lineId: taxLineId,
    templateId: taxTemplateId,
    userId: fixture.userId,
    ruleId: null,
    confidence: 97,
    assertScopeAccess: noScopeGuard,
  });
  const taxJournalEntryId = toNumber(taxAutoPost?.journal?.id);
  assert(taxJournalEntryId > 0, "Tax-included auto-post should create a journal entry");
  assert(taxAutoPost?.idempotent === false, "First tax-included auto-post should not be idempotent");

  const taxAutoLineRows = await query(
    `SELECT recon_status, auto_post_template_id, auto_post_journal_entry_id
     FROM bank_statement_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, taxLineId]
  );
  const taxAutoLine = taxAutoLineRows.rows?.[0] || null;
  assert(
    String(taxAutoLine?.recon_status || "").toUpperCase() === "MATCHED",
    "Tax-included line should be MATCHED after auto-post+reconcile"
  );
  assert(
    toNumber(taxAutoLine?.auto_post_template_id) === taxTemplateId,
    "Tax-included line should store matching auto_post_template_id"
  );
  assert(
    toNumber(taxAutoLine?.auto_post_journal_entry_id) === taxJournalEntryId,
    "Tax-included line should store matching auto_post_journal_entry_id"
  );

  const taxJournalLines = await query(
    `SELECT line_no, account_id, debit_base, credit_base
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no ASC`,
    [taxJournalEntryId]
  );
  assert(taxJournalLines.rows.length === 3, "Tax-included auto-post journal should create 3 lines");
  assert(
    toNumber(taxJournalLines.rows?.[0]?.account_id) === fixture.counterGlAccountId &&
      toAmount(taxJournalLines.rows?.[0]?.debit_base) === 100 &&
      toAmount(taxJournalLines.rows?.[0]?.credit_base) === 0,
    "Tax-included line 1 should debit counter account with net amount"
  );
  assert(
    toNumber(taxJournalLines.rows?.[1]?.account_id) === fixture.taxGlAccountId &&
      toAmount(taxJournalLines.rows?.[1]?.debit_base) === 18 &&
      toAmount(taxJournalLines.rows?.[1]?.credit_base) === 0,
    "Tax-included line 2 should debit tax account with tax amount"
  );
  assert(
    toNumber(taxJournalLines.rows?.[2]?.account_id) === fixture.bankGlAccountId &&
      toAmount(taxJournalLines.rows?.[2]?.debit_base) === 0 &&
      toAmount(taxJournalLines.rows?.[2]?.credit_base) === 118,
    "Tax-included line 3 should credit bank account with gross amount"
  );

  console.log(
    "PR-B08-A smoke test passed (template CRUD + AUTO_POST_TEMPLATE engine apply + idempotent auto-post)."
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
