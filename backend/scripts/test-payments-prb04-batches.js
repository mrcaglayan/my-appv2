import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  approvePaymentBatch,
  cancelPaymentBatch,
  createPaymentBatch,
  exportPaymentBatch,
  postPaymentBatch,
  resolvePaymentBatchScope,
} from "../src/services/payments.service.js";

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

async function createTenantWithPaymentFixtures(stamp) {
  const tenantCode = `PRB04_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRB04 Tenant ${stamp}`]
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
    [tenantId, `PRB04_G_${stamp}`, `PRB04 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB04_G_${stamp}`]
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
      `PRB04_LE_${stamp}`,
      `PRB04 Legal Entity ${stamp}`,
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
    [tenantId, `PRB04_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
      )
      VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `PRB04_CAL_${stamp}`, `PRB04 Calendar ${stamp}`]
  );
  const calendarRows = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB04_CAL_${stamp}`]
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
      `PRB04_BOOK_${stamp}`,
      `PRB04 Book ${stamp}`,
      currencyCode,
    ]
  );

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [
      tenantId,
      legalEntityId,
      `PRB04_COA_${stamp}`,
      `PRB04 Chart ${stamp}`,
    ]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB04_COA_${stamp}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts fixture");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB04BANK${stamp}`, `PRB04 Bank GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB04PAY${stamp}`, `PRB04 Payable GL ${stamp}`]
  );

  const bankGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB04 Bank GL ${stamp}`]
  );
  const payableGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB04 Payable GL ${stamp}`]
  );
  const bankGlAccountId = toNumber(bankGlRows.rows?.[0]?.id);
  const payableGlAccountId = toNumber(payableGlRows.rows?.[0]?.id);
  assert(bankGlAccountId > 0, "Failed to create bank GL account fixture");
  assert(payableGlAccountId > 0, "Failed to create payable GL account fixture");

  const passwordHash = await bcrypt.hash("PRB04#Smoke123", 10);
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, `prb04_user_${stamp}@example.com`, passwordHash, "PRB04 User"]
  );
  const userRows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, `prb04_user_${stamp}@example.com`]
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
      `PRB04_BA_${stamp}`,
      `PRB04 Bank Account ${stamp}`,
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
    [tenantId, legalEntityId, `PRB04_BA_${stamp}`]
  );
  const bankAccountId = toNumber(bankAccountRows.rows?.[0]?.id);
  assert(bankAccountId > 0, "Failed to create bank account fixture");

  return {
    tenantId,
    legalEntityId,
    currencyCode,
    userId,
    bankAccountId,
    payableGlAccountId,
  };
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithPaymentFixtures(stamp);
  const idempotencyKey = `PRB04_CREATE_${stamp}`;

  const createPayload = {
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    sourceType: "MANUAL",
    sourceId: null,
    bankAccountId: fixture.bankAccountId,
    currencyCode: fixture.currencyCode,
    idempotencyKey,
    notes: "PR-B04 smoke create",
    lines: [
      {
        beneficiaryType: "VENDOR",
        beneficiaryId: 9001,
        beneficiaryName: "Smoke Vendor A",
        beneficiaryBankRef: "TR00SMOKEVENDOR",
        payableEntityType: "AP",
        payableEntityId: 7001,
        payableGlAccountId: fixture.payableGlAccountId,
        payableRef: "INV-7001",
        amount: 1250.75,
        notes: "line one",
      },
    ],
  };

  const created1 = await createPaymentBatch({
    req: null,
    payload: createPayload,
    assertScopeAccess: noScopeGuard,
  });
  const batchId = toNumber(created1?.id);
  assert(batchId > 0, "createPaymentBatch did not return batch id");
  assert(String(created1?.status || "").toUpperCase() === "DRAFT", "Created batch should be DRAFT");
  assert(Array.isArray(created1?.lines) && created1.lines.length === 1, "Created batch should have one line");

  const created2 = await createPaymentBatch({
    req: null,
    payload: { ...createPayload, notes: "idempotent retry should return same batch" },
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(created2?.id) === batchId,
    "Idempotent create should return existing batch when idempotencyKey matches"
  );

  const scope = await resolvePaymentBatchScope(batchId, fixture.tenantId);
  assert(scope?.scopeType === "LEGAL_ENTITY", "resolvePaymentBatchScope should return LEGAL_ENTITY scope");
  assert(toNumber(scope?.scopeId) === fixture.legalEntityId, "resolvePaymentBatchScope scopeId mismatch");

  const approved = await approvePaymentBatch({
    req: null,
    tenantId: fixture.tenantId,
    batchId,
    userId: fixture.userId,
    approveInput: { note: "approve batch" },
    assertScopeAccess: noScopeGuard,
  });
  assert(String(approved?.status || "").toUpperCase() === "APPROVED", "Batch should be APPROVED after approve");

  const exported = await exportPaymentBatch({
    req: null,
    tenantId: fixture.tenantId,
    batchId,
    userId: fixture.userId,
    exportInput: { format: "CSV" },
    assertScopeAccess: noScopeGuard,
  });
  assert(String(exported?.row?.status || "").toUpperCase() === "EXPORTED", "Batch should be EXPORTED after export");
  assert(toNumber(exported?.export?.id) > 0, "Export record id should exist");
  assert(String(exported?.export?.file_name || "").endsWith(".csv"), "Export file_name should be csv");
  assert(
    String(exported?.export?.csv || "").includes(String(approved?.batch_no || "")),
    "Export csv should include batch number"
  );

  const posted1 = await postPaymentBatch({
    req: null,
    tenantId: fixture.tenantId,
    batchId,
    userId: fixture.userId,
    postInput: {
      postingDate: "2026-02-15",
      note: "post batch",
      externalPaymentRefPrefix: "PRB04",
    },
    assertScopeAccess: noScopeGuard,
  });
  const postedJournalEntryId = toNumber(posted1?.posted_journal_entry_id);
  assert(String(posted1?.status || "").toUpperCase() === "POSTED", "Batch should be POSTED after post");
  assert(postedJournalEntryId > 0, "posted_journal_entry_id should be populated after posting");
  assert(
    (posted1?.lines || []).every((line) => String(line?.status || "").toUpperCase() === "PAID"),
    "All payment lines should become PAID after posting"
  );

  const posted2 = await postPaymentBatch({
    req: null,
    tenantId: fixture.tenantId,
    batchId,
    userId: fixture.userId,
    postInput: {
      postingDate: "2026-02-15",
      note: "idempotent re-post",
      externalPaymentRefPrefix: "PRB04",
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(
    toNumber(posted2?.posted_journal_entry_id) === postedJournalEntryId,
    "Repeated post should be idempotent and keep same posted_journal_entry_id"
  );

  const journalRows = await query(
    `SELECT id, status, total_debit_base, total_credit_base
     FROM journal_entries
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [fixture.tenantId, postedJournalEntryId]
  );
  const journal = journalRows.rows?.[0] || null;
  assert(journal, "Posted journal entry should exist");
  assert(String(journal.status || "").toUpperCase() === "POSTED", "Settlement journal should be POSTED");

  const auditRows = await query(
    `SELECT action, COUNT(*) AS row_count
     FROM payment_batch_audit
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND batch_id = ?
     GROUP BY action`,
    [fixture.tenantId, fixture.legalEntityId, batchId]
  );
  const auditMap = new Map(
    (auditRows.rows || []).map((row) => [String(row.action || "").toUpperCase(), toNumber(row.row_count)])
  );
  for (const action of ["CREATED", "APPROVED", "EXPORTED", "POSTED"]) {
    assert((auditMap.get(action) || 0) >= 1, `Missing payment batch audit action: ${action}`);
  }

  await expectFailure(
    () =>
      cancelPaymentBatch({
        req: null,
        tenantId: fixture.tenantId,
        batchId,
        userId: fixture.userId,
        cancelInput: { reason: "should fail once posted" },
        assertScopeAccess: noScopeGuard,
      }),
    { status: 400, includes: "cannot be cancelled" }
  );

  console.log(
    "PR-B04 smoke test passed (create + idempotent create + approve + export + post + idempotent post + audit)."
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
