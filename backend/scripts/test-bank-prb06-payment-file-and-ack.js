import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  approvePaymentBatch,
  createPaymentBatch,
} from "../src/services/payments.service.js";
import {
  exportPaymentBatchFile,
  importPaymentBatchAck,
} from "../src/services/bank.paymentFiles.service.js";

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

function noScopeGuard() {
  return true;
}

async function createTenantWithPaymentFixtures(stamp) {
  const tenantCode = `PRB06_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRB06 Tenant ${stamp}`]
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
    [tenantId, `PRB06_G_${stamp}`, `PRB06 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB06_G_${stamp}`]
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
      `PRB06_LE_${stamp}`,
      `PRB06 Legal Entity ${stamp}`,
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
    [tenantId, `PRB06_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
      )
      VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `PRB06_CAL_${stamp}`, `PRB06 Calendar ${stamp}`]
  );
  const calendarRows = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB06_CAL_${stamp}`]
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
      `PRB06_BOOK_${stamp}`,
      `PRB06 Book ${stamp}`,
      currencyCode,
    ]
  );

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `PRB06_COA_${stamp}`, `PRB06 Chart ${stamp}`]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRB06_COA_${stamp}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts fixture");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB06BANK${stamp}`, `PRB06 Bank GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, NULL, TRUE)`,
    [coaId, `PRB06PAY${stamp}`, `PRB06 Payable GL ${stamp}`]
  );

  const bankGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB06 Bank GL ${stamp}`]
  );
  const payableGlRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND name = ?
     LIMIT 1`,
    [coaId, `PRB06 Payable GL ${stamp}`]
  );
  const bankGlAccountId = toNumber(bankGlRows.rows?.[0]?.id);
  const payableGlAccountId = toNumber(payableGlRows.rows?.[0]?.id);
  assert(bankGlAccountId > 0, "Failed to create bank GL account fixture");
  assert(payableGlAccountId > 0, "Failed to create payable GL account fixture");

  const passwordHash = await bcrypt.hash("PRB06#Smoke123", 10);
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, `prb06_user_${stamp}@example.com`, passwordHash, "PRB06 User"]
  );
  const userRows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, `prb06_user_${stamp}@example.com`]
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
      `PRB06_BA_${stamp}`,
      `PRB06 Bank Account ${stamp}`,
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
    [tenantId, legalEntityId, `PRB06_BA_${stamp}`]
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

function buildAckCsv(rows) {
  const header =
    "line_ref,ack_status,ack_amount,bank_reference,ack_code,ack_message,executed_at,currency_code";
  const body = rows.map((row) =>
    [
      row.line_ref ?? "",
      row.ack_status ?? "",
      row.ack_amount ?? "",
      row.bank_reference ?? "",
      row.ack_code ?? "",
      row.ack_message ?? "",
      row.executed_at ?? "",
      row.currency_code ?? "",
    ].join(",")
  );
  return [header, ...body].join("\n");
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithPaymentFixtures(stamp);

  const createRes = await createPaymentBatch({
    req: null,
    payload: {
      tenantId: fixture.tenantId,
      userId: fixture.userId,
      sourceType: "MANUAL",
      sourceId: null,
      bankAccountId: fixture.bankAccountId,
      currencyCode: fixture.currencyCode,
      idempotencyKey: `PRB06_CREATE_${stamp}`,
      notes: "PR-B06 smoke create",
      lines: [
        {
          beneficiaryType: "VENDOR",
          beneficiaryId: 1001,
          beneficiaryName: "Vendor A",
          beneficiaryBankRef: "TR00VENDORA",
          payableEntityType: "AP",
          payableEntityId: 2001,
          payableGlAccountId: fixture.payableGlAccountId,
          payableRef: "INV-A",
          amount: 100,
          notes: "line-1",
        },
        {
          beneficiaryType: "VENDOR",
          beneficiaryId: 1002,
          beneficiaryName: "Vendor B",
          beneficiaryBankRef: "TR00VENDORB",
          payableEntityType: "AP",
          payableEntityId: 2002,
          payableGlAccountId: fixture.payableGlAccountId,
          payableRef: "INV-B",
          amount: 200,
          notes: "line-2",
        },
        {
          beneficiaryType: "VENDOR",
          beneficiaryId: 1003,
          beneficiaryName: "Vendor C",
          beneficiaryBankRef: "TR00VENDORC",
          payableEntityType: "AP",
          payableEntityId: 2003,
          payableGlAccountId: fixture.payableGlAccountId,
          payableRef: "INV-C",
          amount: 300,
          notes: "line-3",
        },
        {
          beneficiaryType: "VENDOR",
          beneficiaryId: 1004,
          beneficiaryName: "Vendor D",
          beneficiaryBankRef: "TR00VENDORD",
          payableEntityType: "AP",
          payableEntityId: 2004,
          payableGlAccountId: fixture.payableGlAccountId,
          payableRef: "INV-D",
          amount: 400,
          notes: "line-4",
        },
      ],
    },
    assertScopeAccess: noScopeGuard,
  });

  const batchId = toNumber(createRes?.id);
  assert(batchId > 0, "Failed to create payment batch for B06");

  const approved = await approvePaymentBatch({
    req: null,
    tenantId: fixture.tenantId,
    batchId,
    userId: fixture.userId,
    approveInput: { note: "approve for bank file export" },
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(approved?.status || "").toUpperCase() === "APPROVED",
    "Batch should be APPROVED before B06 export"
  );

  const exportRequestId = `PRB06_EXP_REQ_${stamp}`;
  const export1 = await exportPaymentBatchFile({
    req: null,
    tenantId: fixture.tenantId,
    batchId,
    userId: fixture.userId,
    input: {
      fileFormatCode: "GENERIC_CSV_V1",
      exportRequestId,
      markSent: true,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(export1?.idempotent === false, "First B06 export should not be idempotent");
  assert(
    String(export1?.row?.bank_export_status || "").toUpperCase() === "SENT",
    "B06 export with markSent should set bank_export_status=SENT"
  );
  const exportId = toNumber(export1?.export?.id);
  assert(exportId > 0, "Export id missing from B06 export result");

  const snapshotRows = await query(
    `SELECT payment_batch_line_id, line_ref, amount
     FROM payment_batch_export_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND payment_batch_export_id = ?
     ORDER BY payment_batch_line_id ASC`,
    [fixture.tenantId, fixture.legalEntityId, exportId]
  );
  assert(snapshotRows.rows.length === 4, "B06 export should create one snapshot line per payment line");

  const export2 = await exportPaymentBatchFile({
    req: null,
    tenantId: fixture.tenantId,
    batchId,
    userId: fixture.userId,
    input: {
      fileFormatCode: "GENERIC_CSV_V1",
      exportRequestId,
      markSent: true,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(export2?.idempotent === true, "Second B06 export with same exportRequestId should be idempotent");
  assert(
    toNumber(export2?.export?.id) === exportId,
    "Idempotent B06 export should return same export record"
  );

  const lineRefs = snapshotRows.rows.map((row) => String(row.line_ref));
  const ackRequestId = `PRB06_ACK_REQ_${stamp}`;
  const ackText = buildAckCsv([
    {
      line_ref: lineRefs[0],
      ack_status: "ACCEPTED",
      ack_amount: "",
      bank_reference: "BR-ACCEPTED",
      ack_code: "",
      ack_message: "",
      executed_at: "2026-02-15 10:00:00",
      currency_code: fixture.currencyCode,
    },
    {
      line_ref: lineRefs[1],
      ack_status: "PARTIAL",
      ack_amount: "50",
      bank_reference: "BR-PARTIAL",
      ack_code: "P01",
      ack_message: "partial payment",
      executed_at: "2026-02-15 10:01:00",
      currency_code: fixture.currencyCode,
    },
    {
      line_ref: lineRefs[2],
      ack_status: "PAID",
      ack_amount: "300",
      bank_reference: "BR-PAID",
      ack_code: "",
      ack_message: "",
      executed_at: "2026-02-15 10:02:00",
      currency_code: fixture.currencyCode,
    },
    {
      line_ref: lineRefs[3],
      ack_status: "REJECTED",
      ack_amount: "",
      bank_reference: "BR-REJECTED",
      ack_code: "R01",
      ack_message: "invalid beneficiary",
      executed_at: "2026-02-15 10:03:00",
      currency_code: fixture.currencyCode,
    },
  ]);

  const ack1 = await importPaymentBatchAck({
    req: null,
    tenantId: fixture.tenantId,
    batchId,
    userId: fixture.userId,
    input: {
      fileFormatCode: "GENERIC_CSV_V1",
      ackRequestId,
      fileName: "ack-main.csv",
      exportId,
      ackText,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(ack1?.idempotent === false, "First ack import should not be idempotent");
  assert(
    String(ack1?.ack_import?.status || "").toUpperCase() === "APPLIED",
    "Ack import should be APPLIED when all rows are valid"
  );
  assert(toNumber(ack1?.ack_import?.applied_rows) === 4, "Ack import should apply all 4 rows");
  assert(toNumber(ack1?.ack_import?.error_rows) === 0, "Ack import should have zero errors");
  assert(
    String(ack1?.row?.bank_ack_status || "").toUpperCase() === "PARTIAL",
    "Mixed ACK statuses should yield batch bank_ack_status=PARTIAL"
  );

  const linesAfterAck1 = new Map(
    (ack1?.row?.lines || []).map((line) => [String(line?.line_no), line])
  );
  assert(
    String(linesAfterAck1.get("1")?.bank_execution_status || "").toUpperCase() === "EXECUTED",
    "Line 1 ACCEPTED should map to EXECUTED"
  );
  assert(
    toAmount(linesAfterAck1.get("1")?.executed_amount) === 0,
    "Line 1 ACCEPTED should keep executed_amount at 0 in v1 mapping"
  );
  assert(
    String(linesAfterAck1.get("2")?.bank_execution_status || "").toUpperCase() === "PARTIALLY_PAID",
    "Line 2 PARTIAL should map to PARTIALLY_PAID"
  );
  assert(
    toAmount(linesAfterAck1.get("2")?.executed_amount) === 50,
    "Line 2 PARTIAL should set executed_amount=50"
  );
  assert(
    String(linesAfterAck1.get("3")?.bank_execution_status || "").toUpperCase() === "PAID",
    "Line 3 PAID should map to PAID"
  );
  assert(
    toAmount(linesAfterAck1.get("3")?.executed_amount) === 300,
    "Line 3 PAID should set executed_amount=300"
  );
  assert(
    String(linesAfterAck1.get("4")?.bank_execution_status || "").toUpperCase() === "REJECTED",
    "Line 4 REJECTED should map to REJECTED"
  );

  const ack2 = await importPaymentBatchAck({
    req: null,
    tenantId: fixture.tenantId,
    batchId,
    userId: fixture.userId,
    input: {
      fileFormatCode: "GENERIC_CSV_V1",
      ackRequestId,
      fileName: "ack-main-duplicate.csv",
      exportId,
      ackText,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(ack2?.idempotent === true, "Ack import should be idempotent for same ackRequestId");
  assert(
    toNumber(ack2?.ack_import?.id) === toNumber(ack1?.ack_import?.id),
    "Idempotent ack import should return same ack import row"
  );

  const overAckText = buildAckCsv([
    {
      line_ref: lineRefs[1],
      ack_status: "PAID",
      ack_amount: "999",
      bank_reference: "BR-OVERACK",
      ack_code: "",
      ack_message: "too high",
      executed_at: "2026-02-15 11:00:00",
      currency_code: fixture.currencyCode,
    },
  ]);

  const overAck = await importPaymentBatchAck({
    req: null,
    tenantId: fixture.tenantId,
    batchId,
    userId: fixture.userId,
    input: {
      fileFormatCode: "GENERIC_CSV_V1",
      ackRequestId: `PRB06_ACK_OVER_${stamp}`,
      fileName: "ack-over.csv",
      exportId,
      ackText: overAckText,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(overAck?.ack_import?.status || "").toUpperCase() === "FAILED",
    "Over-ack only file should produce FAILED ack import status"
  );
  assert(toNumber(overAck?.ack_import?.error_rows) === 1, "Over-ack import should have one error row");
  assert(toNumber(overAck?.ack_import?.applied_rows) === 0, "Over-ack import should apply zero rows");

  const linesAfterOverAck = new Map(
    (overAck?.row?.lines || []).map((line) => [String(line?.line_no), line])
  );
  assert(
    toAmount(linesAfterOverAck.get("2")?.executed_amount) === 50,
    "Over-ack attempt must not increase executed_amount beyond previously applied value"
  );

  const overAckImportLineRows = await query(
    `SELECT ack_code, ack_message
     FROM payment_batch_ack_import_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND ack_import_id = ?`,
    [fixture.tenantId, fixture.legalEntityId, toNumber(overAck?.ack_import?.id)]
  );
  assert(
    String(overAckImportLineRows.rows?.[0]?.ack_code || "").toUpperCase() === "ACK_AMOUNT_OVER_LINE",
    "Over-ack import line should be tagged with ACK_AMOUNT_OVER_LINE"
  );

  console.log(
    "PR-B06 smoke test passed (wrapper export, ack statuses, idempotency, over-ack protection)."
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
