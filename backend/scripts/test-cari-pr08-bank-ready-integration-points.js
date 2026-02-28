import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CARI_PR08_TEST_PORT || 3127);
const BASE_URL = process.env.CARI_PR08_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "CariPR08#12345";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function toUpper(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

async function apiRequest({
  token,
  method = "GET",
  requestPath,
  body,
  expectedStatus,
}) {
  const headers = { "Content-Type": "application/json" };
  if (token) {
    headers.Cookie = token;
  }

  const response = await fetch(`${BASE_URL}${requestPath}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  let json = null;
  try {
    json = await response.json();
  } catch {
    json = null;
  }

  const setCookieHeader = response.headers.get("set-cookie");
  const cookie = setCookieHeader ? String(setCookieHeader).split(";")[0].trim() : null;

  if (expectedStatus !== undefined && response.status !== expectedStatus) {
    throw new Error(
      `${method} ${requestPath} expected ${expectedStatus}, got ${response.status}. response=${JSON.stringify(
        json
      )}`
    );
  }

  return { status: response.status, json, cookie };
}

function startServerProcess() {
  const child = spawn(process.execPath, ["src/index.js"], {
    cwd: process.cwd(),
    env: { ...process.env, PORT: String(PORT) },
    stdio: ["ignore", "pipe", "pipe"],
  });

  child.stdout.on("data", (chunk) => {
    process.stdout.write(`[server] ${chunk}`);
  });
  child.stderr.on("data", (chunk) => {
    process.stderr.write(`[server] ${chunk}`);
  });

  return child;
}

async function waitForServer() {
  const startedAt = Date.now();
  while (Date.now() - startedAt < SERVER_START_TIMEOUT_MS) {
    try {
      const response = await fetch(`${BASE_URL}/health`);
      if (response.ok) {
        return;
      }
    } catch {
      // wait for startup
    }
    await sleep(350);
  }
  throw new Error(`Server did not start within ${SERVER_START_TIMEOUT_MS}ms`);
}

async function login(email, password) {
  const response = await apiRequest({
    method: "POST",
    requestPath: "/auth/login",
    body: { email, password },
    expectedStatus: 200,
  });
  const sessionCookie = response.cookie;
  assert(Boolean(sessionCookie), `Login cookie missing for ${email}`);
  return sessionCookie;
}

async function createTenant(code, name) {
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE name = VALUES(name)`,
    [code, name]
  );
  const result = await query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [code]
  );
  const tenantId = toNumber(result.rows?.[0]?.id);
  assert(tenantId > 0, `Failed to resolve tenant id for ${code}`);
  return tenantId;
}

async function createUserWithRole({
  tenantId,
  roleCode,
  email,
  passwordHash,
  name,
}) {
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, email, passwordHash, name]
  );

  const userResult = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, email]
  );
  const userId = toNumber(userResult.rows?.[0]?.id);
  assert(userId > 0, `Failed to resolve user id for ${email}`);

  const roleResult = await query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, roleCode]
  );
  const roleId = toNumber(roleResult.rows?.[0]?.id);
  assert(roleId > 0, `Role not found: ${roleCode}`);

  await query(
    `INSERT INTO user_role_scopes (
        tenant_id,
        user_id,
        role_id,
        scope_type,
        scope_id,
        effect
     )
     VALUES (?, ?, ?, 'TENANT', ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE effect = VALUES(effect)`,
    [tenantId, userId, roleId, tenantId]
  );

  return { userId, email };
}

async function createOrgFixtures({ tenantId, stamp }) {
  const countryResult = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(countryResult.rows?.[0]?.id);
  const functionalCurrencyCode = toUpper(
    countryResult.rows?.[0]?.default_currency_code || "USD"
  );
  assert(countryId > 0, "US country row is required");

  await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `CARI08GC${stamp}`, `CARI08 Group ${stamp}`]
  );
  const groupResult = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI08GC${stamp}`]
  );
  const groupId = toNumber(groupResult.rows?.[0]?.id);
  assert(groupId > 0, "Group company create failed");

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
      groupId,
      `CARI08LE${stamp}`,
      `CARI08 Legal Entity ${stamp}`,
      countryId,
      functionalCurrencyCode,
    ]
  );
  const legalEntityResult = await query(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI08LE${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityResult.rows?.[0]?.id);
  assert(legalEntityId > 0, "Legal entity create failed");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id,
        code,
        name,
        year_start_month,
        year_start_day
     )
     VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `CARI08CAL_${stamp}`, `CARI08 Calendar ${stamp}`]
  );
  const calendarResult = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI08CAL_${stamp}`]
  );
  const calendarId = toNumber(calendarResult.rows?.[0]?.id);
  assert(calendarId > 0, "Fiscal calendar create failed");

  await query(
    `INSERT INTO fiscal_periods (
        calendar_id,
        fiscal_year,
        period_no,
        period_name,
        start_date,
        end_date,
        is_adjustment
     )
     VALUES (?, 2026, 1, 'FY2026', '2026-01-01', '2026-12-31', FALSE)
     ON DUPLICATE KEY UPDATE period_name = VALUES(period_name)`,
    [calendarId]
  );

  await query(
    `INSERT INTO books (
        tenant_id,
        legal_entity_id,
        calendar_id,
        code,
        name,
        book_type,
        base_currency_code
     )
     VALUES (?, ?, ?, ?, ?, 'LOCAL', ?)`,
    [
      tenantId,
      legalEntityId,
      calendarId,
      `CARI08BOOK_${stamp}`,
      `CARI08 Book ${stamp}`,
      functionalCurrencyCode,
    ]
  );

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id,
        legal_entity_id,
        scope,
        code,
        name
     )
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `CARI08COA_${stamp}`, `CARI08 COA ${stamp}`]
  );
  const coaResult = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI08COA_${stamp}`]
  );
  const coaId = toNumber(coaResult.rows?.[0]?.id);
  assert(coaId > 0, "COA create failed");

  const accountPrefix = `C8${String(stamp).slice(-5)}`;
  await query(
    `INSERT INTO accounts (
        coa_id,
        code,
        name,
        account_type,
        normal_side,
        allow_posting,
        parent_account_id,
        is_active
     )
     VALUES
       (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE),
       (?, ?, ?, 'REVENUE', 'CREDIT', TRUE, NULL, TRUE),
       (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, NULL, TRUE),
       (?, ?, ?, 'EXPENSE', 'DEBIT', TRUE, NULL, TRUE)`,
    [
      coaId,
      `${accountPrefix}01`,
      "CARI08 AR Control",
      coaId,
      `${accountPrefix}02`,
      "CARI08 AR Offset",
      coaId,
      `${accountPrefix}03`,
      "CARI08 AP Control",
      coaId,
      `${accountPrefix}04`,
      "CARI08 AP Offset",
    ]
  );
  const accountRows = await query(
    `SELECT id, code
     FROM accounts
     WHERE coa_id = ?
       AND code IN (?, ?, ?, ?)
     ORDER BY code`,
    [
      coaId,
      `${accountPrefix}01`,
      `${accountPrefix}02`,
      `${accountPrefix}03`,
      `${accountPrefix}04`,
    ]
  );
  const accountByCode = new Map(
    accountRows.rows.map((row) => [String(row.code), toNumber(row.id)])
  );
  const arControlAccountId = accountByCode.get(`${accountPrefix}01`);
  const arOffsetAccountId = accountByCode.get(`${accountPrefix}02`);
  const apControlAccountId = accountByCode.get(`${accountPrefix}03`);
  const apOffsetAccountId = accountByCode.get(`${accountPrefix}04`);
  assert(arControlAccountId > 0, "AR control account missing");
  assert(arOffsetAccountId > 0, "AR offset account missing");
  assert(apControlAccountId > 0, "AP control account missing");
  assert(apOffsetAccountId > 0, "AP offset account missing");

  await query(
    `INSERT INTO journal_purpose_accounts (
        tenant_id,
        legal_entity_id,
        purpose_code,
        account_id
     )
     VALUES
       (?, ?, 'CARI_AR_CONTROL', ?),
       (?, ?, 'CARI_AR_OFFSET', ?),
       (?, ?, 'CARI_AP_CONTROL', ?),
       (?, ?, 'CARI_AP_OFFSET', ?)
     ON DUPLICATE KEY UPDATE account_id = VALUES(account_id)`,
    [
      tenantId,
      legalEntityId,
      arControlAccountId,
      tenantId,
      legalEntityId,
      arOffsetAccountId,
      tenantId,
      legalEntityId,
      apControlAccountId,
      tenantId,
      legalEntityId,
      apOffsetAccountId,
    ]
  );

  await query(
    `INSERT INTO payment_terms (
        tenant_id,
        legal_entity_id,
        code,
        name,
        due_days,
        grace_days,
        status
     )
     VALUES (?, ?, ?, ?, 30, 0, 'ACTIVE')`,
    [tenantId, legalEntityId, `CARI08TERM_${stamp}`, `CARI08 Term ${stamp}`]
  );
  const paymentTermResult = await query(
    `SELECT id
     FROM payment_terms
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, `CARI08TERM_${stamp}`]
  );
  const paymentTermId = toNumber(paymentTermResult.rows?.[0]?.id);
  assert(paymentTermId > 0, "Payment term create failed");

  await query(
    `INSERT INTO counterparties (
        tenant_id,
        legal_entity_id,
        code,
        name,
        is_customer,
        is_vendor,
        default_currency_code,
        default_payment_term_id,
        status
     )
     VALUES (?, ?, ?, ?, TRUE, FALSE, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      legalEntityId,
      `CARI08CP_${stamp}`,
      `CARI08 Counterparty ${stamp}`,
      functionalCurrencyCode,
      paymentTermId,
    ]
  );
  const counterpartyResult = await query(
    `SELECT id
     FROM counterparties
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, `CARI08CP_${stamp}`]
  );
  const counterpartyId = toNumber(counterpartyResult.rows?.[0]?.id);
  assert(counterpartyId > 0, "Counterparty create failed");

  return {
    tenantId,
    legalEntityId,
    paymentTermId,
    counterpartyId,
    functionalCurrencyCode,
  };
}

async function createAndPostDocument({
  token,
  fixtures,
  documentDate,
  dueDate,
  amountTxn,
  amountBase,
  currencyCode = "USD",
}) {
  const createResponse = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/cari/documents",
    body: {
      legalEntityId: fixtures.legalEntityId,
      counterpartyId: fixtures.counterpartyId,
      paymentTermId: fixtures.paymentTermId,
      direction: "AR",
      documentType: "INVOICE",
      documentDate,
      dueDate,
      amountTxn,
      amountBase,
      currencyCode,
      fxRate: 1,
    },
    expectedStatus: 201,
  });
  const documentId = toNumber(createResponse.json?.row?.id);
  assert(documentId > 0, "Draft document id missing");

  await apiRequest({
    token,
    method: "POST",
    requestPath: `/api/v1/cari/documents/${documentId}/post`,
    body: {},
    expectedStatus: 200,
  });

  const openItemResult = await query(
    `SELECT id
     FROM cari_open_items
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND document_id = ?
     ORDER BY id ASC
     LIMIT 1`,
    [fixtures.tenantId, fixtures.legalEntityId, documentId]
  );
  const openItemId = toNumber(openItemResult.rows?.[0]?.id);
  assert(openItemId > 0, "Open item missing after post");

  return { documentId, openItemId };
}

async function getOpenItem(tenantId, openItemId) {
  const result = await query(
    `SELECT *
     FROM cari_open_items
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, openItemId]
  );
  return result.rows?.[0] || null;
}

async function getSettlementBatch(tenantId, settlementBatchId) {
  const result = await query(
    `SELECT *
     FROM cari_settlement_batches
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, settlementBatchId]
  );
  return result.rows?.[0] || null;
}

async function getUnappliedCash(tenantId, unappliedCashId) {
  const result = await query(
    `SELECT *
     FROM cari_unapplied_cash
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, unappliedCashId]
  );
  return result.rows?.[0] || null;
}

async function getAuditRows({ tenantId, action }) {
  const result = await query(
    `SELECT id, action, resource_type, resource_id, payload_json
     FROM audit_logs
     WHERE tenant_id = ?
       AND action = ?
     ORDER BY id DESC`,
    [tenantId, action]
  );
  return result.rows || [];
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantId = await createTenant(`CARI08_${stamp}`, `CARI PR08 ${stamp}`);
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const fixtures = await createOrgFixtures({ tenantId, stamp });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const user = await createUserWithRole({
    tenantId,
    roleCode: "EntityAccountant",
    email: `cari08_${stamp}@example.com`,
    passwordHash,
    name: "CARI08 Accountant",
  });

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const token = await login(user.email, TEST_PASSWORD);

    const baseApplyBody = {
      legalEntityId: fixtures.legalEntityId,
      counterpartyId: fixtures.counterpartyId,
      direction: "AR",
      currencyCode: "USD",
    };

    const manualDocA = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-05-01",
      dueDate: "2026-05-15",
      amountTxn: 50,
      amountBase: 50,
    });
    const manualApplyA = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        ...baseApplyBody,
        settlementDate: "2026-05-16",
        paymentAmountTxn: 50,
        autoAllocate: false,
        allocations: [{ openItemId: manualDocA.openItemId, amountTxn: 50 }],
        idempotencyKey: `PR08-MANUAL-A-${stamp}`,
      },
      expectedStatus: 201,
    });
    const manualSettlementBatchId = toNumber(manualApplyA.json?.row?.id);
    assert(manualSettlementBatchId > 0, "Manual settlement should still create batch");
    const manualOpenA = await getOpenItem(tenantId, manualDocA.openItemId);
    assert(
      toUpper(manualOpenA?.status) === "SETTLED",
      "Manual settlement flow should remain unchanged after bank endpoints"
    );

    const attachSettlementKey = `PR08-ATTACH-SETT-${stamp}`;
    const attachSettlementFirst = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/bank/attach",
      body: {
        legalEntityId: fixtures.legalEntityId,
        targetType: "SETTLEMENT",
        settlementBatchId: manualSettlementBatchId,
        bankStatementLineId: 900001,
        bankTransactionRef: `PR08-SETT-${stamp}`,
        idempotencyKey: attachSettlementKey,
      },
      expectedStatus: 201,
    });
    assert(
      toNumber(attachSettlementFirst.json?.settlement?.bankStatementLineId) === 900001,
      "Settlement bank attach should persist bankStatementLineId"
    );
    assert(
      String(attachSettlementFirst.json?.settlement?.bankTransactionRef || "") ===
        `PR08-SETT-${stamp}`,
      "Settlement bank attach should persist bankTransactionRef"
    );
    const attachSettlementReplay = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/bank/attach",
      body: {
        legalEntityId: fixtures.legalEntityId,
        targetType: "SETTLEMENT",
        settlementBatchId: manualSettlementBatchId,
        bankStatementLineId: 900001,
        bankTransactionRef: `PR08-SETT-${stamp}`,
        idempotencyKey: attachSettlementKey,
      },
      expectedStatus: 200,
    });
    assert(
      attachSettlementReplay.json?.idempotentReplay === true,
      "Duplicate settlement attach should replay safely"
    );

    const overpayDoc = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-05-02",
      dueDate: "2026-05-16",
      amountTxn: 40,
      amountBase: 40,
    });
    const overpayApply = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        ...baseApplyBody,
        settlementDate: "2026-05-17",
        paymentAmountTxn: 100,
        autoAllocate: false,
        allocations: [{ openItemId: overpayDoc.openItemId, amountTxn: 40 }],
        idempotencyKey: `PR08-OVERPAY-${stamp}`,
      },
      expectedStatus: 201,
    });
    const createdUnappliedCashId = toNumber(overpayApply.json?.unapplied?.createdUnappliedCashId);
    assert(createdUnappliedCashId > 0, "Overpayment should create unapplied cash row");

    const attachUnappliedKey = `PR08-ATTACH-UNAP-${stamp}`;
    const attachUnappliedFirst = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/bank/attach",
      body: {
        legalEntityId: fixtures.legalEntityId,
        targetType: "UNAPPLIED_CASH",
        unappliedCashId: createdUnappliedCashId,
        bankStatementLineId: 900002,
        bankTransactionRef: `PR08-UNAP-${stamp}`,
        idempotencyKey: attachUnappliedKey,
      },
      expectedStatus: 201,
    });
    assert(
      toNumber(attachUnappliedFirst.json?.unappliedCash?.bankStatementLineId) === 900002,
      "Unapplied attach should persist bankStatementLineId"
    );
    const attachUnappliedReplay = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/bank/attach",
      body: {
        legalEntityId: fixtures.legalEntityId,
        targetType: "UNAPPLIED_CASH",
        unappliedCashId: createdUnappliedCashId,
        bankStatementLineId: 900002,
        bankTransactionRef: `PR08-UNAP-${stamp}`,
        idempotencyKey: attachUnappliedKey,
      },
      expectedStatus: 200,
    });
    assert(
      attachUnappliedReplay.json?.idempotentReplay === true,
      "Duplicate unapplied attach should replay safely"
    );

    const invalidBankApplyDoc = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-05-03",
      dueDate: "2026-05-17",
      amountTxn: 20,
      amountBase: 20,
    });
    const invalidBankApply = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/bank/apply",
      body: {
        ...baseApplyBody,
        settlementDate: "2026-05-18",
        paymentAmountTxn: 25,
        autoAllocate: false,
        allocations: [{ openItemId: invalidBankApplyDoc.openItemId, amountTxn: 25 }],
        idempotencyKey: `PR08-BANK-INVALID-${stamp}`,
        bankStatementLineId: 900003,
        bankTransactionRef: `PR08-BANK-INVALID-${stamp}`,
      },
    });
    assert(
      invalidBankApply.status === 400 &&
        String(invalidBankApply.json?.message || "").includes("allocation exceeds residual"),
      "Bank apply should use same settlement allocation validations"
    );

    const bankApplyDoc = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-05-04",
      dueDate: "2026-05-18",
      amountTxn: 30,
      amountBase: 30,
    });
    const bankApplyKey = `PR08-BANK-APPLY-${stamp}`;
    const bankApplyFirst = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/bank/apply",
      body: {
        ...baseApplyBody,
        settlementDate: "2026-05-19",
        paymentAmountTxn: 30,
        autoAllocate: false,
        allocations: [{ openItemId: bankApplyDoc.openItemId, amountTxn: 30 }],
        idempotencyKey: bankApplyKey,
        bankStatementLineId: 900004,
        bankTransactionRef: `PR08-BANK-APPLY-${stamp}`,
      },
      expectedStatus: 201,
    });
    const bankApplyBatchId = toNumber(bankApplyFirst.json?.row?.id);
    assert(bankApplyBatchId > 0, "Bank apply should create settlement batch");
    assert(
      String(bankApplyFirst.json?.row?.bankApplyIdempotencyKey || "") === bankApplyKey,
      "Bank apply should persist bank_apply_idempotency_key on settlement batch"
    );
    assert(
      toNumber(bankApplyFirst.json?.row?.bankStatementLineId) === 900004,
      "Bank apply should persist bank_statement_line_id on settlement batch"
    );
    assert(
      String(bankApplyFirst.json?.row?.bankTransactionRef || "") ===
        `PR08-BANK-APPLY-${stamp}`,
      "Bank apply should persist bank_transaction_ref on settlement batch"
    );
    const bankApplyReplay = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/bank/apply",
      body: {
        ...baseApplyBody,
        settlementDate: "2026-05-19",
        paymentAmountTxn: 30,
        autoAllocate: false,
        allocations: [{ openItemId: bankApplyDoc.openItemId, amountTxn: 30 }],
        idempotencyKey: bankApplyKey,
        bankStatementLineId: 900004,
        bankTransactionRef: `PR08-BANK-APPLY-${stamp}`,
      },
      expectedStatus: 200,
    });
    assert(
      bankApplyReplay.json?.idempotentReplay === true,
      "Duplicate bank apply should replay safely"
    );
    assert(
      toNumber(bankApplyReplay.json?.row?.id) === bankApplyBatchId,
      "Duplicate bank apply should return same settlement batch"
    );

    const settlementRow = await getSettlementBatch(tenantId, bankApplyBatchId);
    assert(
      String(settlementRow?.bank_apply_idempotency_key || "") === bankApplyKey,
      "DB settlement row should keep bank_apply_idempotency_key"
    );
    const unappliedRow = await getUnappliedCash(tenantId, createdUnappliedCashId);
    assert(
      String(unappliedRow?.bank_attach_idempotency_key || "") === attachUnappliedKey,
      "DB unapplied row should keep bank_attach_idempotency_key"
    );

    const bankAttachAuditRows = await getAuditRows({
      tenantId,
      action: "cari.bank.attach",
    });
    assert(bankAttachAuditRows.length >= 2, "Bank attach actions should be audited");
    assert(
      bankAttachAuditRows.some(
        (row) =>
          String(row.resource_type) === "cari_settlement_batch" &&
          toNumber(row.resource_id) === manualSettlementBatchId
      ),
      "Settlement bank attach should produce audit log row"
    );
    assert(
      bankAttachAuditRows.some(
        (row) =>
          String(row.resource_type) === "cari_unapplied_cash" &&
          toNumber(row.resource_id) === createdUnappliedCashId
      ),
      "Unapplied cash attach should produce audit log row"
    );

    const bankApplyAuditRows = await getAuditRows({
      tenantId,
      action: "cari.bank.apply",
    });
    assert(bankApplyAuditRows.length >= 1, "Bank apply action should be audited");
    assert(
      bankApplyAuditRows.some(
        (row) =>
          String(row.resource_type) === "cari_settlement_batch" &&
          toNumber(row.resource_id) === bankApplyBatchId
      ),
      "Bank apply audit should link to settlement batch"
    );

    const manualDocB = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-05-05",
      dueDate: "2026-05-19",
      amountTxn: 35,
      amountBase: 35,
    });
    const manualApplyB = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        ...baseApplyBody,
        settlementDate: "2026-05-20",
        paymentAmountTxn: 35,
        autoAllocate: false,
        allocations: [{ openItemId: manualDocB.openItemId, amountTxn: 35 }],
        idempotencyKey: `PR08-MANUAL-B-${stamp}`,
      },
      expectedStatus: 201,
    });
    const manualBatchB = toNumber(manualApplyB.json?.row?.id);
    assert(manualBatchB > 0, "Manual settlement should keep working after bank endpoints");
    assert(
      manualApplyB.json?.row?.bankApplyIdempotencyKey === null,
      "Manual settlement should not set bank apply idempotency linkage"
    );

    console.log("CARI PR-08 bank-ready integration points test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId,
          legalEntityId: fixtures.legalEntityId,
          counterpartyId: fixtures.counterpartyId,
          settlementAttachAuditRows: bankAttachAuditRows.length,
          bankApplyAuditRows: bankApplyAuditRows.length,
        },
        null,
        2
      )
    );
  } finally {
    if (!serverStopped) {
      server.kill("SIGTERM");
      serverStopped = true;
    }
  }
}

main()
  .then(async () => {
    await closePool();
    process.exit(0);
  })
  .catch(async (err) => {
    console.error(err);
    try {
      await closePool();
    } catch {
      // ignore close failures
    }
    process.exit(1);
  });
