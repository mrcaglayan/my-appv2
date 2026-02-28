import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  approvePaymentBatch,
  createPaymentBatch,
  postPaymentBatch,
} from "../src/services/payments.service.js";
import { createReconciliationRule } from "../src/services/bank.reconciliationRules.service.js";
import {
  applyBankReconciliationAutoRun,
  previewBankReconciliationAutoRun,
} from "../src/services/bank.reconciliationEngine.service.js";
import {
  createDifferenceProfile,
  listDifferenceProfiles,
  updateDifferenceProfile,
} from "../src/services/bank.reconciliationDifferenceProfiles.service.js";
import {
  createManualPaymentReturnEvent,
  listPaymentReturnEventRows,
} from "../src/services/bank.paymentReturns.service.js";

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

async function createTenantWithB08BFixtures(stamp) {
  const returnRef = "RETXZA91";
  const diffRef = "DIFQVB72";
  const rejectRef = "REJMWC63";

  const tenantCode = `PRB08B_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRB08B Tenant ${stamp}`]
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
    [tenantId, `PRB08B_G_${stamp}`, `PRB08B Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB08B_G_${stamp}`]
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
      `PRB08B_LE_${stamp}`,
      `PRB08B Legal Entity ${stamp}`,
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
    [tenantId, `PRB08B_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
      )
      VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `PRB08B_CAL_${stamp}`, `PRB08B Calendar ${stamp}`]
  );
  const calendarRows = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB08B_CAL_${stamp}`]
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
      `PRB08B_BOOK_${stamp}`,
      `PRB08B Book ${stamp}`,
      currencyCode,
    ]
  );

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `PRB08B_COA_${stamp}`, `PRB08B Chart ${stamp}`]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB08B_COA_${stamp}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts fixture");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB08BBANK${stamp}`, `PRB08B Bank GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB08BPAY${stamp}`, `PRB08B Payable GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'REVENUE', 'CREDIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB08BFXG${stamp}`, `PRB08B FX Gain GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'EXPENSE', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB08BFXL${stamp}`, `PRB08B FX Loss GL ${stamp}`]
  );

  const bankGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB08B Bank GL ${stamp}`]
  );
  const payableGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB08B Payable GL ${stamp}`]
  );
  const fxGainGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB08B FX Gain GL ${stamp}`]
  );
  const fxLossGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB08B FX Loss GL ${stamp}`]
  );

  const bankGlAccountId = toNumber(bankGlRows.rows?.[0]?.id);
  const payableGlAccountId = toNumber(payableGlRows.rows?.[0]?.id);
  const fxGainAccountId = toNumber(fxGainGlRows.rows?.[0]?.id);
  const fxLossAccountId = toNumber(fxLossGlRows.rows?.[0]?.id);
  assert(bankGlAccountId > 0, "Failed to create bank GL account fixture");
  assert(payableGlAccountId > 0, "Failed to create payable GL account fixture");
  assert(fxGainAccountId > 0, "Failed to create FX gain GL account fixture");
  assert(fxLossAccountId > 0, "Failed to create FX loss GL account fixture");

  const passwordHash = await bcrypt.hash("PRB08B#Smoke123", 10);
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, `prb08b_user_${stamp}@example.com`, passwordHash, "PRB08B User"]
  );
  const userRows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, `prb08b_user_${stamp}@example.com`]
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
      `PRB08B_BA_${stamp}`,
      `PRB08B Bank Account ${stamp}`,
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
    [tenantId, legalEntityId, `PRB08B_BA_${stamp}`]
  );
  const bankAccountId = toNumber(bankAccountRows.rows?.[0]?.id);
  assert(bankAccountId > 0, "Failed to create bank account fixture");

  const createdBatch = await createPaymentBatch({
    req: null,
    payload: {
      tenantId,
      userId,
      sourceType: "MANUAL",
      sourceId: null,
      bankAccountId,
      currencyCode,
      idempotencyKey: `PRB08B_CREATE_${stamp}`,
      notes: "PR-B08-B smoke create",
      lines: [
        {
          beneficiaryType: "VENDOR",
          beneficiaryId: 8101,
          beneficiaryName: "PRB08B Return Vendor",
          beneficiaryBankRef: "TR00PRB08BRET",
          payableEntityType: "AP",
          payableEntityId: 9101,
          payableGlAccountId,
          payableRef: returnRef,
          amount: 100,
          notes: "return line",
        },
        {
          beneficiaryType: "VENDOR",
          beneficiaryId: 8102,
          beneficiaryName: "PRB08B Diff Vendor",
          beneficiaryBankRef: "TR00PRB08BDIFF",
          payableEntityType: "AP",
          payableEntityId: 9102,
          payableGlAccountId,
          payableRef: diffRef,
          amount: 90,
          notes: "difference line",
        },
        {
          beneficiaryType: "VENDOR",
          beneficiaryId: 8103,
          beneficiaryName: "PRB08B Reject Vendor",
          beneficiaryBankRef: "TR00PRB08BREJ",
          payableEntityType: "AP",
          payableEntityId: 9103,
          payableGlAccountId,
          payableRef: rejectRef,
          amount: 80,
          notes: "rejection line",
        },
      ],
    },
    assertScopeAccess: noScopeGuard,
  });
  const batchId = toNumber(createdBatch?.id);
  assert(batchId > 0, "Failed to create payment batch fixture");

  await approvePaymentBatch({
    req: null,
    tenantId,
    batchId,
    userId,
    approveInput: { note: "approve B08B batch" },
    assertScopeAccess: noScopeGuard,
  });

  const postedBatch = await postPaymentBatch({
    req: null,
    tenantId,
    batchId,
    userId,
    postInput: {
      postingDate: "2026-02-20",
      note: "post B08B batch",
      externalPaymentRefPrefix: "PRB08B",
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(postedBatch?.status || "").toUpperCase() === "POSTED",
    "Fixture payment batch should be POSTED"
  );

  const lineByNo = new Map((postedBatch?.lines || []).map((line) => [Number(line?.line_no), line]));
  const returnPaymentLine = lineByNo.get(1);
  const diffPaymentLine = lineByNo.get(2);
  const rejectPaymentLine = lineByNo.get(3);
  assert(returnPaymentLine && diffPaymentLine && rejectPaymentLine, "Posted batch lines are incomplete");

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
      VALUES (?, ?, ?, 'CSV', ?, ?, 'IMPORTED', 2, 2, 0, ?, ?)`,
    [
      tenantId,
      legalEntityId,
      bankAccountId,
      `prb08b-${stamp}.csv`,
      `prb08b-checksum-${stamp}`,
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
    [tenantId, legalEntityId, bankAccountId, `prb08b-checksum-${stamp}`]
  );
  const importId = toNumber(importRows.rows?.[0]?.id);
  assert(importId > 0, "Failed to create statement import fixture");

  const returnLineHash = `PRB08B-RET-LINE-${stamp}`.padEnd(64, "0").slice(0, 64);
  const diffLineHash = `PRB08B-DIFF-LINE-${stamp}`.padEnd(64, "1").slice(0, 64);

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
      VALUES (?, ?, ?, ?, 1, '2026-02-20', '2026-02-20', ?, ?, 100.000000, ?, 1100.000000, ?, 'UNMATCHED', ?)`,
    [
      tenantId,
      legalEntityId,
      importId,
      bankAccountId,
      returnRef,
      returnRef,
      currencyCode,
      returnLineHash,
      JSON.stringify({ rowNo: 1 }),
    ]
  );
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
      VALUES (?, ?, ?, ?, 2, '2026-02-20', '2026-02-20', ?, ?, -95.000000, ?, 1005.000000, ?, 'UNMATCHED', ?)`,
    [
      tenantId,
      legalEntityId,
      importId,
      bankAccountId,
      diffRef,
      diffRef,
      currencyCode,
      diffLineHash,
      JSON.stringify({ rowNo: 2 }),
    ]
  );

  const statementLines = await query(
    `SELECT id, line_no
     FROM bank_statement_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND import_id = ?
     ORDER BY line_no ASC`,
    [tenantId, legalEntityId, importId]
  );
  const returnStatementLineId = toNumber(statementLines.rows?.find((r) => Number(r.line_no) === 1)?.id);
  const diffStatementLineId = toNumber(statementLines.rows?.find((r) => Number(r.line_no) === 2)?.id);
  assert(returnStatementLineId > 0 && diffStatementLineId > 0, "Failed to create statement line fixtures");

  return {
    tenantId,
    legalEntityId,
    currencyCode,
    userId,
    bankAccountId,
    batchId,
    bankGlAccountId,
    fxGainAccountId,
    fxLossAccountId,
    returnPaymentLineId: toNumber(returnPaymentLine?.id),
    diffPaymentLineId: toNumber(diffPaymentLine?.id),
    rejectPaymentLineId: toNumber(rejectPaymentLine?.id),
    returnStatementLineId,
    diffStatementLineId,
    returnRef,
    diffRef,
    rejectRef,
  };
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithB08BFixtures(stamp);

  const createdProfile = await createDifferenceProfile({
    req: null,
    input: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      profileCode: `PRB08B_FX_${stamp}`,
      profileName: "PRB08B FX Difference Profile",
      status: "ACTIVE",
      scopeType: "LEGAL_ENTITY",
      legalEntityId: fixture.legalEntityId,
      bankAccountId: null,
      differenceType: "FX",
      directionPolicy: "BOTH",
      maxAbsDifference: 10,
      expenseAccountId: null,
      fxGainAccountId: fixture.fxGainAccountId,
      fxLossAccountId: fixture.fxLossAccountId,
      currencyCode: fixture.currencyCode,
      descriptionPrefix: "PRB08B FX",
      effectiveFrom: null,
      effectiveTo: null,
    },
    assertScopeAccess: noScopeGuard,
  });
  const profileId = toNumber(createdProfile?.row?.id);
  assert(profileId > 0, "Failed to create FX difference profile");
  assert(
    createdProfile?.approval_required === false,
    "Difference profile should not require approval in smoke tenant"
  );

  const listedProfiles = await listDifferenceProfiles({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      legalEntityId: fixture.legalEntityId,
      bankAccountId: null,
      status: "ACTIVE",
      differenceType: "FX",
      q: "PRB08B_FX_",
      limit: 50,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    (listedProfiles.rows || []).some((row) => toNumber(row?.id) === profileId),
    "Difference profile list should include created FX profile"
  );

  const updatedProfile = await updateDifferenceProfile({
    req: null,
    input: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      profileId,
      profileName: "PRB08B FX Difference Profile Updated",
      maxAbsDifference: 20,
      descriptionPrefix: "PRB08B FX UPDATED",
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(updatedProfile?.row?.profile_name || "").includes("Updated"),
    "Difference profile update should persist profile_name"
  );
  assert(
    toAmount(updatedProfile?.row?.max_abs_difference) === 20,
    "Difference profile update should persist max_abs_difference"
  );

  const returnRule = await createReconciliationRule({
    req: null,
    input: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      ruleCode: `PRB08B_RET_RULE_${stamp}`,
      ruleName: "PRB08B process payment return",
      status: "ACTIVE",
      priority: 1,
      scopeType: "GLOBAL",
      legalEntityId: null,
      bankAccountId: null,
      matchType: "PAYMENT_BY_TEXT_AND_AMOUNT",
      conditions: {
        referenceIncludesAny: [fixture.returnRef],
      },
      actionType: "PROCESS_PAYMENT_RETURN",
      actionPayload: {
        eventType: "PAYMENT_RETURNED",
      },
      stopOnMatch: true,
      effectiveFrom: null,
      effectiveTo: null,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(toNumber(returnRule?.row?.id) > 0, "Failed to create B08B return rule");

  const diffRule = await createReconciliationRule({
    req: null,
    input: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      ruleCode: `PRB08B_DIFF_RULE_${stamp}`,
      ruleName: "PRB08B auto match with FX difference",
      status: "ACTIVE",
      priority: 2,
      scopeType: "GLOBAL",
      legalEntityId: null,
      bankAccountId: null,
      matchType: "PAYMENT_BY_TEXT_AND_AMOUNT",
      conditions: {
        referenceIncludesAny: [fixture.diffRef],
        dateLagDays: 60,
      },
      actionType: "AUTO_MATCH_PAYMENT_LINE_WITH_DIFFERENCE",
      actionPayload: {
        differenceProfileId: profileId,
      },
      stopOnMatch: true,
      effectiveFrom: null,
      effectiveTo: null,
    },
    assertScopeAccess: noScopeGuard,
  });
  const diffRuleId = toNumber(diffRule?.row?.id);
  assert(diffRuleId > 0, "Failed to create B08B difference rule");

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
    toNumber(preview?.summary?.scannedCount) === 2,
    "B08B preview should scan two statement lines"
  );
  const previewOutcomes = new Set((preview?.rows || []).map((row) => String(row?.outcome || "").toUpperCase()));
  assert(previewOutcomes.has("AUTO_RETURN_READY"), "B08B preview should include AUTO_RETURN_READY");
  assert(previewOutcomes.has("AUTO_DIFF_READY"), "B08B preview should include AUTO_DIFF_READY");

  const runRequestId = `PRB08B_APPLY_${stamp}`;
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
  assert(apply1?.replay === false, "First B08B apply should not be replay");
  assert(
    String(apply1?.run?.status || "").toUpperCase() === "SUCCESS",
    "B08B apply status should be SUCCESS"
  );
  assert(
    toNumber(apply1?.summary?.reconciledCount) === 2,
    "B08B apply should reconcile two statement lines"
  );

  const applyOutcomes = new Set((apply1?.rows || []).map((row) => String(row?.outcome || "").toUpperCase()));
  assert(
    applyOutcomes.has("RETURN_PROCESSED_RECONCILED"),
    "B08B apply should include RETURN_PROCESSED_RECONCILED"
  );
  assert(applyOutcomes.has("DIFFERENCE_RECONCILED"), "B08B apply should include DIFFERENCE_RECONCILED");

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
  assert(apply2?.replay === true, "Second B08B apply with same runRequestId should replay");
  assert(
    toNumber(apply2?.run?.id) === toNumber(apply1?.run?.id),
    "B08B apply replay should return same run id"
  );

  const returnEventsRows = await query(
    `SELECT id, event_type, event_status, amount
     FROM bank_payment_return_events
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND payment_batch_line_id = ?
       AND event_type = 'PAYMENT_RETURNED'`,
    [fixture.tenantId, fixture.legalEntityId, fixture.returnPaymentLineId]
  );
  assert(returnEventsRows.rows.length === 1, "B08B return flow should create one PAYMENT_RETURNED event");
  assert(
    String(returnEventsRows.rows?.[0]?.event_status || "").toUpperCase() === "CONFIRMED",
    "Return event status should be CONFIRMED"
  );
  assert(
    toAmount(returnEventsRows.rows?.[0]?.amount) === 100,
    "Return event amount should be 100"
  );

  const returnLineStateRows = await query(
    `SELECT return_status, returned_amount, bank_execution_status
     FROM payment_batch_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, fixture.returnPaymentLineId]
  );
  const returnLineState = returnLineStateRows.rows?.[0] || null;
  assert(returnLineState, "Return payment line should exist");
  assert(
    String(returnLineState?.return_status || "").toUpperCase() === "RETURNED",
    "Return payment line should be RETURNED"
  );
  assert(
    toAmount(returnLineState?.returned_amount) === 100,
    "Return payment line returned_amount should be 100"
  );
  assert(
    String(returnLineState?.bank_execution_status || "").toUpperCase() === "RETURNED",
    "Return payment line bank_execution_status should be RETURNED"
  );

  const returnStmtRows = await query(
    `SELECT recon_status
     FROM bank_statement_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, fixture.returnStatementLineId]
  );
  assert(
    String(returnStmtRows.rows?.[0]?.recon_status || "").toUpperCase() === "MATCHED",
    "Return statement line should be MATCHED"
  );

  const diffAdjRows = await query(
    `SELECT
        difference_type,
        difference_amount,
        difference_profile_id,
        journal_entry_id,
        currency_code
     FROM bank_reconciliation_difference_adjustments
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND bank_statement_line_id = ?
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, fixture.diffStatementLineId]
  );
  const diffAdj = diffAdjRows.rows?.[0] || null;
  assert(diffAdj, "Difference flow should create adjustment row");
  assert(String(diffAdj?.difference_type || "").toUpperCase() === "FX", "Difference type should be FX");
  assert(toAmount(diffAdj?.difference_amount) === 5, "Difference amount should be +5");
  assert(
    toNumber(diffAdj?.difference_profile_id) === profileId,
    "Difference adjustment should reference created profile"
  );
  const diffJournalId = toNumber(diffAdj?.journal_entry_id);
  assert(diffJournalId > 0, "Difference adjustment should have journal_entry_id");

  const diffStmtRows = await query(
    `SELECT
        recon_status,
        reconciliation_difference_type,
        reconciliation_difference_amount,
        reconciliation_difference_profile_id,
        reconciliation_difference_journal_entry_id
     FROM bank_statement_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, fixture.diffStatementLineId]
  );
  const diffStmt = diffStmtRows.rows?.[0] || null;
  assert(diffStmt, "Difference statement line should exist");
  assert(String(diffStmt?.recon_status || "").toUpperCase() === "MATCHED", "Difference line should be MATCHED");
  assert(
    String(diffStmt?.reconciliation_difference_type || "").toUpperCase() === "FX",
    "Statement line difference type should be FX"
  );
  assert(
    toAmount(diffStmt?.reconciliation_difference_amount) === 5,
    "Statement line difference amount should be +5"
  );
  assert(
    toNumber(diffStmt?.reconciliation_difference_profile_id) === profileId,
    "Statement line difference profile id should match profile"
  );
  assert(
    toNumber(diffStmt?.reconciliation_difference_journal_entry_id) === diffJournalId,
    "Statement line difference journal id should match adjustment journal"
  );

  const diffJournalLineRows = await query(
    `SELECT account_id, debit_base, credit_base
     FROM journal_lines
     WHERE journal_entry_id = ?`,
    [diffJournalId]
  );
  assert(diffJournalLineRows.rows.length === 2, "Difference journal should have 2 lines");
  assert(
    diffJournalLineRows.rows.some(
      (row) =>
        toNumber(row?.account_id) === fixture.bankGlAccountId &&
        toAmount(row?.debit_base) === 0 &&
        toAmount(row?.credit_base) === 5
    ),
    "Difference journal should credit bank GL for outflow increase"
  );
  assert(
    diffJournalLineRows.rows.some(
      (row) =>
        toNumber(row?.account_id) === fixture.fxLossAccountId &&
        toAmount(row?.debit_base) === 5 &&
        toAmount(row?.credit_base) === 0
    ),
    "Difference journal should debit FX loss account for outflow increase"
  );

  const diffMatchesRows = await query(
    `SELECT matched_entity_type, matched_entity_id, matched_amount, status
     FROM bank_reconciliation_matches
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND statement_line_id = ?
       AND status = 'ACTIVE'`,
    [fixture.tenantId, fixture.legalEntityId, fixture.diffStatementLineId]
  );
  assert(diffMatchesRows.rows.length === 2, "Difference line should have 2 active matches");
  assert(
    diffMatchesRows.rows.some((row) => String(row?.matched_entity_type || "").toUpperCase() === "PAYMENT_BATCH"),
    "Difference line should have PAYMENT_BATCH match"
  );
  assert(
    diffMatchesRows.rows.some(
      (row) =>
        String(row?.matched_entity_type || "").toUpperCase() === "JOURNAL" &&
        toNumber(row?.matched_entity_id) === diffJournalId
    ),
    "Difference line should have JOURNAL match to the difference journal"
  );

  const manualRejection = await createManualPaymentReturnEvent({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    input: {
      eventRequestId: `PRB08B_REJ_${stamp}`,
      sourceType: "MANUAL",
      sourceRef: "PRB08B_MANUAL",
      paymentBatchLineId: fixture.rejectPaymentLineId,
      bankStatementLineId: null,
      eventType: "PAYMENT_REJECTED",
      amount: 0,
      currencyCode: fixture.currencyCode,
      bankReference: "PRB08B-REJ-BANKREF",
      reasonCode: "ACK_REJECTED",
      reasonMessage: "Manual rejection for smoke coverage",
      payload: { smoke: true },
      _b09SkipApprovalGate: true,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(manualRejection?.row?.id) > 0 && manualRejection?.idempotent === false,
    "Manual rejection should create a non-idempotent return event on first call"
  );
  assert(
    String(manualRejection?.row?.event_type || "").toUpperCase() === "PAYMENT_REJECTED",
    "Manual rejection event_type should be PAYMENT_REJECTED"
  );

  const rejectLineStateRows = await query(
    `SELECT return_status, returned_amount, bank_execution_status, status
     FROM payment_batch_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [fixture.tenantId, fixture.legalEntityId, fixture.rejectPaymentLineId]
  );
  const rejectLineState = rejectLineStateRows.rows?.[0] || null;
  assert(rejectLineState, "Reject payment line should exist");
  assert(
    String(rejectLineState?.return_status || "").toUpperCase() === "REJECTED_POST_ACK",
    "Reject payment line should be REJECTED_POST_ACK"
  );
  assert(
    String(rejectLineState?.bank_execution_status || "").toUpperCase() === "REJECTED",
    "Reject payment line bank_execution_status should be REJECTED"
  );
  assert(
    toAmount(rejectLineState?.returned_amount) === 0,
    "Reject payment line returned_amount should stay 0"
  );

  const listedReturnEvents = await listPaymentReturnEventRows({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      legalEntityId: fixture.legalEntityId,
      bankAccountId: fixture.bankAccountId,
      paymentBatchId: fixture.batchId,
      paymentBatchLineId: null,
      bankStatementLineId: null,
      eventType: null,
      eventStatus: null,
      limit: 100,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(listedReturnEvents?.total) >= 2,
    "Return event listing should include both auto-return and manual rejection events"
  );

  console.log(
    "PR-B08-B smoke test passed (return processing + manual rejection + FX difference auto-reconciliation)."
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
