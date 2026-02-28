import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  createReconciliationRule,
  listReconciliationRuleRows,
  updateReconciliationRule,
} from "../src/services/bank.reconciliationRules.service.js";
import {
  applyBankReconciliationAutoRun,
  previewBankReconciliationAutoRun,
} from "../src/services/bank.reconciliationEngine.service.js";
import {
  assignReconciliationException,
  listReconciliationExceptionRows,
  resolveReconciliationException,
  retryReconciliationException,
} from "../src/services/bank.reconciliationExceptions.service.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function allowAllScopeFilter() {
  return "1 = 1";
}

function noScopeGuard() {
  return true;
}

async function createTenantWithReconRuleFixtures(stamp) {
  const tenantCode = `PRB07_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRB07 Tenant ${stamp}`]
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
    [tenantId, `PRB07_G_${stamp}`, `PRB07 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB07_G_${stamp}`]
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
      `PRB07_LE_${stamp}`,
      `PRB07 Legal Entity ${stamp}`,
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
    [tenantId, `PRB07_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `PRB07_COA_${stamp}`, `PRB07 Chart ${stamp}`]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB07_COA_${stamp}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts fixture");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB07BANK${stamp}`, `PRB07 Bank GL ${stamp}`]
  );
  const bankGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB07 Bank GL ${stamp}`]
  );
  const bankGlAccountId = toNumber(bankGlRows.rows?.[0]?.id);
  assert(bankGlAccountId > 0, "Failed to create bank GL account fixture");

  const passwordHash = await bcrypt.hash("PRB07#Smoke123", 10);
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, `prb07_user_${stamp}@example.com`, passwordHash, "PRB07 User"]
  );
  const userRows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, `prb07_user_${stamp}@example.com`]
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
      `PRB07_BA_${stamp}`,
      `PRB07 Bank Account ${stamp}`,
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
    [tenantId, legalEntityId, `PRB07_BA_${stamp}`]
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
      `prb07-${stamp}.csv`,
      `prb07-checksum-${stamp}`,
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
    [tenantId, legalEntityId, bankAccountId, `prb07-checksum-${stamp}`]
  );
  const importId = toNumber(importRows.rows?.[0]?.id);
  assert(importId > 0, "Failed to create statement import fixture");

  const lineHash = `PRB07-LINE-${stamp}`.padEnd(64, "0").slice(0, 64);
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
      VALUES (?, ?, ?, ?, 1, '2026-02-11', '2026-02-11', ?, ?, 175.000000, ?, 1175.000000, ?, 'UNMATCHED', ?)`,
    [
      tenantId,
      legalEntityId,
      importId,
      bankAccountId,
      "Incoming transfer for PRB07 smoke",
      "PRB07-REF-001",
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
    bankAccountId,
    userId,
    lineId,
  };
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithReconRuleFixtures(stamp);

  const createdRule = await createReconciliationRule({
    req: null,
    input: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      ruleCode: `PRB07_RULE_${stamp}`,
      ruleName: "Queue exception on known reference",
      status: "ACTIVE",
      priority: 10,
      scopeType: "GLOBAL",
      legalEntityId: null,
      bankAccountId: null,
      matchType: "PAYMENT_BY_TEXT_AND_AMOUNT",
      conditions: {
        referenceIncludesAny: ["PRB07-REF-001"],
      },
      actionType: "QUEUE_EXCEPTION",
      actionPayload: {
        reason: "smoke-queue",
      },
      stopOnMatch: true,
      effectiveFrom: null,
      effectiveTo: null,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(createdRule?.row?.id) > 0,
    "Failed to create reconciliation rule"
  );
  assert(
    String(createdRule?.row?.status || "").toUpperCase() === "ACTIVE",
    "Created reconciliation rule should be ACTIVE"
  );
  assert(
    createdRule?.approval_required === false,
    "Smoke tenant should not require approval for rule creation"
  );
  const ruleId = toNumber(createdRule?.row?.id);

  const listedRules = await listReconciliationRuleRows({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      legalEntityId: null,
      bankAccountId: null,
      status: "ACTIVE",
      q: "PRB07_RULE",
      limit: 50,
      offset: 0,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(
    (listedRules.rows || []).some((row) => toNumber(row?.id) === ruleId),
    "Rule list should contain the created global rule"
  );

  const updatedRule = await updateReconciliationRule({
    req: null,
    input: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      ruleId,
      ruleName: "Queue exception on known reference (updated)",
      actionPayload: { reason: "smoke-queue-updated" },
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(updatedRule?.row?.rule_name || "").includes("(updated)"),
    "Rule update should persist updated rule_name"
  );

  const exceptionCountBeforeApplyRows = await query(
    `SELECT COUNT(*) AS total
     FROM bank_reconciliation_exceptions
     WHERE tenant_id = ?
       AND legal_entity_id = ?`,
    [fixture.tenantId, fixture.legalEntityId]
  );
  const exceptionCountBeforeApply = toNumber(
    exceptionCountBeforeApplyRows.rows?.[0]?.total
  );
  assert(
    exceptionCountBeforeApply === 0,
    "There should be no exceptions before apply run"
  );

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
    String(preview?.run?.run_mode || "").toUpperCase() === "PREVIEW",
    "Preview run mode should be PREVIEW"
  );
  assert(
    toNumber(preview?.summary?.scannedCount) === 1,
    "Preview should scan one statement line"
  );
  assert(
    toNumber(preview?.summary?.exceptionCount) === 1,
    "Preview should flag one exception candidate"
  );
  assert(
    String(preview?.rows?.[0]?.outcome || "").toUpperCase() === "RULE_QUEUE_EXCEPTION",
    "Preview outcome should be RULE_QUEUE_EXCEPTION"
  );

  const runRequestId = `PRB07_APPLY_${stamp}`;
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
  assert(apply1?.replay === false, "First apply run should not be replay");
  assert(
    String(apply1?.run?.run_mode || "").toUpperCase() === "APPLY",
    "Apply run mode should be APPLY"
  );
  assert(
    String(apply1?.run?.status || "").toUpperCase() === "PARTIAL",
    "Apply status should be PARTIAL when exceptions are queued"
  );
  assert(
    toNumber(apply1?.summary?.exceptionCount) === 1,
    "Apply should queue one exception"
  );
  assert(
    toNumber(apply1?.rows?.[0]?.exceptionId) > 0,
    "Apply row should include created exceptionId"
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
  assert(apply2?.replay === true, "Second apply with same runRequestId should replay");
  assert(
    toNumber(apply2?.run?.id) === toNumber(apply1?.run?.id),
    "Replay apply should return the same run row"
  );

  const listedExceptions = await listReconciliationExceptionRows({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      legalEntityId: fixture.legalEntityId,
      bankAccountId: fixture.bankAccountId,
      statementLineId: fixture.lineId,
      status: null,
      reasonCode: "RULE_QUEUE_EXCEPTION",
      q: null,
      cursor: null,
      limit: 50,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(listedExceptions?.total) >= 1,
    "Exception list should include the queued exception"
  );
  const exception = listedExceptions.rows?.[0];
  const exceptionId = toNumber(exception?.id);
  assert(exceptionId > 0, "Listed exception id should be present");
  assert(
    String(exception?.status || "").toUpperCase() === "OPEN",
    "Newly queued exception should be OPEN"
  );
  assert(
    toNumber(exception?.matched_rule_id) === ruleId,
    "Exception should reference matched rule id"
  );

  const assigned = await assignReconciliationException({
    req: null,
    tenantId: fixture.tenantId,
    exceptionId,
    assignedToUserId: fixture.userId,
    userId: fixture.userId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(assigned?.status || "").toUpperCase() === "ASSIGNED",
    "Assigning exception should set status=ASSIGNED"
  );
  assert(
    toNumber(assigned?.assigned_to_user_id) === fixture.userId,
    "Assigned exception should store assigned_to_user_id"
  );

  const resolved = await resolveReconciliationException({
    req: null,
    tenantId: fixture.tenantId,
    exceptionId,
    resolutionCode: "RESOLVED_MANUALLY",
    resolutionNote: "PRB07 smoke resolved",
    userId: fixture.userId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(resolved?.status || "").toUpperCase() === "RESOLVED",
    "Resolving exception should set status=RESOLVED"
  );
  assert(
    String(resolved?.resolution_code || "").toUpperCase() === "RESOLVED_MANUALLY",
    "Resolve should persist resolution_code"
  );

  const retried = await retryReconciliationException({
    req: null,
    tenantId: fixture.tenantId,
    exceptionId,
    note: "re-open for smoke",
    userId: fixture.userId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(retried?.exception?.status || "").toUpperCase() === "OPEN",
    "Retry should reopen exception to OPEN"
  );
  assert(
    toNumber(retried?.statementLine?.id) === fixture.lineId,
    "Retry response should include related statement line"
  );

  const exceptionEventsRows = await query(
    `SELECT event_type
     FROM bank_reconciliation_exception_events
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND bank_reconciliation_exception_id = ?`,
    [fixture.tenantId, fixture.legalEntityId, exceptionId]
  );
  const eventTypes = new Set(
    (exceptionEventsRows.rows || []).map((row) =>
      String(row?.event_type || "").toUpperCase()
    )
  );
  for (const expectedEvent of [
    "CREATED",
    "ASSIGNED",
    "RESOLVED",
    "RETRIED",
  ]) {
    assert(
      eventTypes.has(expectedEvent),
      `Exception event log should include ${expectedEvent}`
    );
  }

  console.log(
    "PR-B07 smoke test passed (rule CRUD + preview/apply + exception queue lifecycle + idempotent apply)."
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
