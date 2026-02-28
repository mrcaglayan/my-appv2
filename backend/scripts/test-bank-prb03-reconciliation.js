import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  getReconciliationSuggestionsForLine,
  ignoreReconciliationLine,
  listReconciliationAuditRows,
  listReconciliationQueueRows,
  matchReconciliationLine,
  unignoreReconciliationLine,
  unmatchReconciliationLine,
} from "../src/services/bank.reconciliation.service.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
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

async function createTenantWithReconFixtures(stamp) {
  const tenantCode = `PRB03_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRB03 Tenant ${stamp}`]
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
    [tenantId, `PRB03_G_${stamp}`, `PRB03 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB03_G_${stamp}`]
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
      `PRB03_LE_${stamp}`,
      `PRB03 Legal Entity ${stamp}`,
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
    [tenantId, `PRB03_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
      )
      VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `PRB03_CAL_${stamp}`, `PRB03 Calendar ${stamp}`]
  );
  const calendarRows = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB03_CAL_${stamp}`]
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
  const periodRows = await query(
    `SELECT id
     FROM fiscal_periods
     WHERE calendar_id = ?
       AND fiscal_year = 2026
       AND period_no = 2
       AND is_adjustment = FALSE
     LIMIT 1`,
    [calendarId]
  );
  const fiscalPeriodId = toNumber(periodRows.rows?.[0]?.id);
  assert(fiscalPeriodId > 0, "Failed to create fiscal period fixture");

  await query(
    `INSERT INTO books (
        tenant_id, legal_entity_id, calendar_id, code, name, book_type, base_currency_code
      )
      VALUES (?, ?, ?, ?, ?, 'LOCAL', ?)`,
    [
      tenantId,
      legalEntityId,
      calendarId,
      `PRB03_BOOK_${stamp}`,
      `PRB03 Book ${stamp}`,
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
    [tenantId, legalEntityId, `PRB03_BOOK_${stamp}`]
  );
  const bookId = toNumber(bookRows.rows?.[0]?.id);
  assert(bookId > 0, "Failed to create book fixture");

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `PRB03_COA_${stamp}`, `PRB03 Chart ${stamp}`]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB03_COA_${stamp}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts fixture");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB03BANK${stamp}`, `PRB03 Bank GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB03OFF${stamp}`, `PRB03 Offset GL ${stamp}`]
  );
  const bankGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB03 Bank GL ${stamp}`]
  );
  const offsetGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB03 Offset GL ${stamp}`]
  );
  const bankGlAccountId = toNumber(bankGlRows.rows?.[0]?.id);
  const offsetGlAccountId = toNumber(offsetGlRows.rows?.[0]?.id);
  assert(bankGlAccountId > 0, "Failed to create bank GL account fixture");
  assert(offsetGlAccountId > 0, "Failed to create offset GL account fixture");

  const passwordHash = await bcrypt.hash("PRB03#Smoke123", 10);
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, `prb03_user_${stamp}@example.com`, passwordHash, "PRB03 User"]
  );
  const userRows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, `prb03_user_${stamp}@example.com`]
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
      `PRB03_BA_${stamp}`,
      `PRB03 Bank Account ${stamp}`,
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
    [tenantId, legalEntityId, `PRB03_BA_${stamp}`]
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
      `prb03-${stamp}.csv`,
      `prb03-checksum-${stamp}`,
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
    [tenantId, legalEntityId, bankAccountId, `prb03-checksum-${stamp}`]
  );
  const importId = toNumber(importRows.rows?.[0]?.id);
  assert(importId > 0, "Failed to create statement import fixture");

  const lineHash = `PRB03-LINE-${stamp}`.padEnd(64, "0").slice(0, 64);
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
      VALUES (?, ?, ?, ?, 1, '2026-02-10', '2026-02-10', ?, ?, 100.000000, ?, 1000.000000, ?, 'UNMATCHED', ?)`,
    [
      tenantId,
      legalEntityId,
      importId,
      bankAccountId,
      "Incoming transfer INV-001",
      "INV-001",
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

  await query(
    `INSERT INTO journal_entries (
        tenant_id,
        legal_entity_id,
        book_id,
        fiscal_period_id,
        journal_no,
        source_type,
        status,
        entry_date,
        document_date,
        currency_code,
        description,
        reference_no,
        total_debit_base,
        total_credit_base,
        created_by_user_id,
        posted_by_user_id,
        posted_at
      )
      VALUES (?, ?, ?, ?, ?, 'SYSTEM', 'POSTED', '2026-02-10', '2026-02-10', ?, ?, ?, 100.000000, 100.000000, ?, ?, CURRENT_TIMESTAMP)`,
    [
      tenantId,
      legalEntityId,
      bookId,
      fiscalPeriodId,
      `PRB03-JE-${stamp}`,
      currencyCode,
      "Bank settlement for INV-001",
      "INV-001",
      userId,
      userId,
    ]
  );
  const journalRows = await query(
    `SELECT id
     FROM journal_entries
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND journal_no = ?
     LIMIT 1`,
    [tenantId, legalEntityId, `PRB03-JE-${stamp}`]
  );
  const journalId = toNumber(journalRows.rows?.[0]?.id);
  assert(journalId > 0, "Failed to create journal entry fixture");

  await query(
    `INSERT INTO journal_lines (
        journal_entry_id,
        line_no,
        account_id,
        operating_unit_id,
        counterparty_legal_entity_id,
        description,
        currency_code,
        amount_txn,
        debit_base,
        credit_base,
        tax_code
      )
      VALUES (?, 1, ?, NULL, NULL, 'Bank debit', ?, 100.000000, 100.000000, 0.000000, NULL)`,
    [journalId, bankGlAccountId, currencyCode]
  );
  await query(
    `INSERT INTO journal_lines (
        journal_entry_id,
        line_no,
        account_id,
        operating_unit_id,
        counterparty_legal_entity_id,
        description,
        currency_code,
        amount_txn,
        debit_base,
        credit_base,
        tax_code
      )
      VALUES (?, 2, ?, NULL, NULL, 'Offset credit', ?, -100.000000, 0.000000, 100.000000, NULL)`,
    [journalId, offsetGlAccountId, currencyCode]
  );

  return {
    tenantId,
    legalEntityId,
    bankAccountId,
    userId,
    lineId,
    journalId,
  };
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithReconFixtures(stamp);

  const queueInitial = await listReconciliationQueueRows({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      tenantId: fixture.tenantId,
      legalEntityId: fixture.legalEntityId,
      bankAccountId: fixture.bankAccountId,
      reconStatus: null,
      q: null,
      cursor: null,
      limit: 100,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert((queueInitial.rows || []).length === 1, "Queue should include the created statement line");
  assert(
    String(queueInitial.rows?.[0]?.recon_status || "").toUpperCase() === "UNMATCHED",
    "Initial reconciliation status should be UNMATCHED"
  );

  const suggestions = await getReconciliationSuggestionsForLine({
    req: null,
    tenantId: fixture.tenantId,
    lineId: fixture.lineId,
    userId: fixture.userId,
    assertScopeAccess: noScopeGuard,
  });
  const journalSuggestion = (suggestions.suggestions || []).find(
    (row) => toNumber(row?.matchedEntityId) === fixture.journalId
  );
  assert(Boolean(journalSuggestion), "Suggestion list should contain the matching posted journal");

  const partialMatch = await matchReconciliationLine({
    req: null,
    tenantId: fixture.tenantId,
    lineId: fixture.lineId,
    matchInput: {
      matchType: "MANUAL",
      matchedEntityType: "JOURNAL",
      matchedEntityId: fixture.journalId,
      matchedAmount: 40,
      notes: "partial match",
    },
    userId: fixture.userId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(partialMatch?.line?.recon_status || "").toUpperCase() === "PARTIAL",
    "After first partial match, recon_status should be PARTIAL"
  );
  assert((partialMatch?.matches || []).length === 1, "Expected one active match after first match");
  const firstMatchId = toNumber(partialMatch?.matches?.[0]?.id);
  assert(firstMatchId > 0, "First match id should exist");

  await expectFailure(
    () =>
      matchReconciliationLine({
        req: null,
        tenantId: fixture.tenantId,
        lineId: fixture.lineId,
        matchInput: {
          matchType: "MANUAL",
          matchedEntityType: "JOURNAL",
          matchedEntityId: fixture.journalId,
          matchedAmount: 70,
          notes: "should fail due to over-match",
        },
        userId: fixture.userId,
        assertScopeAccess: noScopeGuard,
      }),
    { status: 400, includes: "exceeds statement line amount" }
  );

  const fullMatch = await matchReconciliationLine({
    req: null,
    tenantId: fixture.tenantId,
    lineId: fixture.lineId,
    matchInput: {
      matchType: "MANUAL",
      matchedEntityType: "JOURNAL",
      matchedEntityId: fixture.journalId,
      matchedAmount: 60,
      notes: "complete match",
    },
    userId: fixture.userId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(fullMatch?.line?.recon_status || "").toUpperCase() === "MATCHED",
    "After complete match, recon_status should be MATCHED"
  );
  assert((fullMatch?.matches || []).length === 2, "Expected two active matches after full matching");

  const queueMatched = await listReconciliationQueueRows({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      tenantId: fixture.tenantId,
      legalEntityId: fixture.legalEntityId,
      bankAccountId: fixture.bankAccountId,
      reconStatus: "MATCHED",
      q: null,
      cursor: null,
      limit: 100,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert((queueMatched.rows || []).length === 1, "Queue MATCHED filter should return the line");

  const unmatchOne = await unmatchReconciliationLine({
    req: null,
    tenantId: fixture.tenantId,
    lineId: fixture.lineId,
    unmatchInput: {
      matchId: firstMatchId,
      notes: "reverse first partial match",
    },
    userId: fixture.userId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(unmatchOne?.line?.recon_status || "").toUpperCase() === "PARTIAL",
    "Unmatching one of two active matches should leave line in PARTIAL status"
  );
  assert((unmatchOne?.matches || []).length === 1, "One active match should remain after unmatching one");

  const unmatchAll = await unmatchReconciliationLine({
    req: null,
    tenantId: fixture.tenantId,
    lineId: fixture.lineId,
    unmatchInput: {
      matchId: null,
      notes: "reverse remaining match",
    },
    userId: fixture.userId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(unmatchAll?.line?.recon_status || "").toUpperCase() === "UNMATCHED",
    "Unmatching all active matches should bring line back to UNMATCHED"
  );
  assert((unmatchAll?.matches || []).length === 0, "No active matches should remain after unmatch-all");

  const ignored = await ignoreReconciliationLine({
    req: null,
    tenantId: fixture.tenantId,
    lineId: fixture.lineId,
    ignoreInput: { reason: "bank fee line" },
    userId: fixture.userId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(ignored?.line?.recon_status || "").toUpperCase() === "IGNORED",
    "Ignoring line should set recon_status=IGNORED"
  );

  await expectFailure(
    () =>
      matchReconciliationLine({
        req: null,
        tenantId: fixture.tenantId,
        lineId: fixture.lineId,
        matchInput: {
          matchType: "MANUAL",
          matchedEntityType: "JOURNAL",
          matchedEntityId: fixture.journalId,
          matchedAmount: 10,
          notes: "should fail because line is ignored",
        },
        userId: fixture.userId,
        assertScopeAccess: noScopeGuard,
      }),
    { status: 400, includes: "Ignored line cannot be matched" }
  );

  const queueIgnored = await listReconciliationQueueRows({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      tenantId: fixture.tenantId,
      legalEntityId: fixture.legalEntityId,
      bankAccountId: fixture.bankAccountId,
      reconStatus: "IGNORED",
      q: null,
      cursor: null,
      limit: 100,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert((queueIgnored.rows || []).length === 1, "Queue IGNORED filter should return the ignored line");

  const unignored = await unignoreReconciliationLine({
    req: null,
    tenantId: fixture.tenantId,
    lineId: fixture.lineId,
    unignoreInput: { reason: "re-open for matching" },
    userId: fixture.userId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(unignored?.line?.recon_status || "").toUpperCase() === "UNMATCHED",
    "Unignore should move line back to UNMATCHED when no active matches exist"
  );

  const rematchAfterUnignore = await matchReconciliationLine({
    req: null,
    tenantId: fixture.tenantId,
    lineId: fixture.lineId,
    matchInput: {
      matchType: "MANUAL",
      matchedEntityType: "JOURNAL",
      matchedEntityId: fixture.journalId,
      matchedAmount: 10,
      notes: "match after unignore",
    },
    userId: fixture.userId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(rematchAfterUnignore?.line?.recon_status || "").toUpperCase() === "PARTIAL",
    "Line should be matchable again after unignore"
  );

  await unmatchReconciliationLine({
    req: null,
    tenantId: fixture.tenantId,
    lineId: fixture.lineId,
    unmatchInput: {
      matchId: null,
      notes: "cleanup rematch after unignore",
    },
    userId: fixture.userId,
    assertScopeAccess: noScopeGuard,
  });

  const audit = await listReconciliationAuditRows({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      tenantId: fixture.tenantId,
      legalEntityId: fixture.legalEntityId,
      statementLineId: fixture.lineId,
      action: null,
      limit: 200,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  const actionSet = new Set((audit.rows || []).map((row) => String(row?.action || "").toUpperCase()));
  for (const requiredAction of [
    "SUGGESTED",
    "MATCHED",
    "UNMATCHED",
    "IGNORE",
    "UNIGNORE",
    "AUTO_STATUS",
  ]) {
    assert(actionSet.has(requiredAction), `Audit should contain action ${requiredAction}`);
  }

  console.log(
    "PR-B03 smoke test passed (queue/suggestions/match/unmatch/ignore/audit with status transitions)."
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
