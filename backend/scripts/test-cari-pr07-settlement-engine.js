import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CARI_PR07_TEST_PORT || 3126);
const BASE_URL =
  process.env.CARI_PR07_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "CariPR07#12345";

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

function amountsEqual(left, right, epsilon = 0.000001) {
  return Math.abs(toNumber(left) - toNumber(right)) <= epsilon;
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
  const cookie = setCookieHeader
    ? String(setCookieHeader).split(";")[0].trim()
    : null;

  if (expectedStatus !== undefined && response.status !== expectedStatus) {
    throw new Error(
      `${method} ${requestPath} expected ${expectedStatus}, got ${response.status}. response=${JSON.stringify(
        json
      )}`
    );
  }

  return {
    status: response.status,
    json,
    cookie,
  };
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
  assert(Boolean(response.cookie), `Login cookie missing for ${email}`);
  return response.cookie;
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
    [tenantId, `CARI07GC${stamp}`, `CARI07 Group ${stamp}`]
  );
  const groupResult = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI07GC${stamp}`]
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
      `CARI07LE${stamp}`,
      `CARI07 Legal Entity ${stamp}`,
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
    [tenantId, `CARI07LE${stamp}`]
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
    [tenantId, `CARI07CAL_${stamp}`, `CARI07 Calendar ${stamp}`]
  );
  const calendarResult = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI07CAL_${stamp}`]
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
      `CARI07BOOK_${stamp}`,
      `CARI07 Book ${stamp}`,
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
    [tenantId, legalEntityId, `CARI07COA_${stamp}`, `CARI07 COA ${stamp}`]
  );
  const coaResult = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI07COA_${stamp}`]
  );
  const coaId = toNumber(coaResult.rows?.[0]?.id);
  assert(coaId > 0, "COA create failed");

  const accountPrefix = `C7${String(stamp).slice(-5)}`;
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
      "CARI07 AR Control",
      coaId,
      `${accountPrefix}02`,
      "CARI07 AR Offset",
      coaId,
      `${accountPrefix}03`,
      "CARI07 AP Control",
      coaId,
      `${accountPrefix}04`,
      "CARI07 AP Offset",
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
    [tenantId, legalEntityId, `CARI07TERM_${stamp}`, `CARI07 Term ${stamp}`]
  );
  const paymentTermResult = await query(
    `SELECT id
     FROM payment_terms
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, `CARI07TERM_${stamp}`]
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
      `CARI07CP_${stamp}`,
      `CARI07 Counterparty ${stamp}`,
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
    [tenantId, legalEntityId, `CARI07CP_${stamp}`]
  );
  const counterpartyId = toNumber(counterpartyResult.rows?.[0]?.id);
  assert(counterpartyId > 0, "Counterparty create failed");

  return {
    tenantId,
    legalEntityId,
    paymentTermId,
    counterpartyId,
    functionalCurrencyCode,
    arControlAccountId,
    arOffsetAccountId,
    apControlAccountId,
    apOffsetAccountId,
  };
}

async function createAndPostDocument({
  token,
  fixtures,
  direction = "AR",
  documentType = "INVOICE",
  documentDate,
  dueDate,
  amountTxn,
  amountBase,
  currencyCode,
  fxRate,
}) {
  const createResponse = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/cari/documents",
    body: {
      legalEntityId: fixtures.legalEntityId,
      counterpartyId: fixtures.counterpartyId,
      paymentTermId: fixtures.paymentTermId,
      direction,
      documentType,
      documentDate,
      dueDate,
      amountTxn,
      amountBase,
      currencyCode,
      fxRate,
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

async function getDocument(tenantId, documentId) {
  const result = await query(
    `SELECT *
     FROM cari_documents
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, documentId]
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

async function countAllocationsByIdempotency({
  tenantId,
  legalEntityId,
  idempotencyKey,
}) {
  const result = await query(
    `SELECT COUNT(*) AS row_count
     FROM cari_settlement_allocations
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND apply_idempotency_key = ?`,
    [tenantId, legalEntityId, idempotencyKey]
  );
  return toNumber(result.rows?.[0]?.row_count);
}

async function getUnappliedRows({ tenantId, legalEntityId, counterpartyId }) {
  const result = await query(
    `SELECT *
     FROM cari_unapplied_cash
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND counterparty_id = ?
     ORDER BY id ASC`,
    [tenantId, legalEntityId, counterpartyId]
  );
  return result.rows || [];
}

async function getJournalLines(journalEntryId) {
  const result = await query(
    `SELECT *
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no ASC`,
    [journalEntryId]
  );
  return result.rows || [];
}

async function getAuditRows({ tenantId, action, resourceId }) {
  const result = await query(
    `SELECT id, payload_json
     FROM audit_logs
     WHERE tenant_id = ?
       AND action = ?
       AND resource_type = 'cari_settlement_batch'
       AND resource_id = ?
     ORDER BY id DESC`,
    [tenantId, action, String(resourceId)]
  );
  return result.rows || [];
}

async function insertFxRate({
  tenantId,
  rateDate,
  fromCurrencyCode,
  toCurrencyCode,
  rate,
}) {
  await query(
    `INSERT INTO fx_rates (
        tenant_id,
        rate_date,
        from_currency_code,
        to_currency_code,
        rate_type,
        rate,
        source,
        is_locked
     )
     VALUES (?, ?, ?, ?, 'SPOT', ?, 'TEST', FALSE)
     ON DUPLICATE KEY UPDATE rate = VALUES(rate), source = VALUES(source), is_locked = VALUES(is_locked)`,
    [tenantId, rateDate, fromCurrencyCode, toCurrencyCode, rate]
  );
}
async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantId = await createTenant(`CARI07_${stamp}`, `CARI PR07 ${stamp}`);
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const fixtures = await createOrgFixtures({ tenantId, stamp });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const user = await createUserWithRole({
    tenantId,
    roleCode: "EntityAccountant",
    email: `cari07_${stamp}@example.com`,
    passwordHash,
    name: "CARI07 Accountant",
  });

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const token = await login(user.email, TEST_PASSWORD);

    const docFull = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-02-01",
      dueDate: "2026-02-10",
      amountTxn: 1000,
      amountBase: 1000,
      currencyCode: "USD",
      fxRate: 1,
    });
    const fullApply = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixtures.legalEntityId,
        counterpartyId: fixtures.counterpartyId,
        settlementDate: "2026-03-01",
        currencyCode: "USD",
        incomingAmountTxn: 1000,
        idempotencyKey: `PR07-FULL-${stamp}`,
        autoAllocate: false,
        allocations: [{ openItemId: docFull.openItemId, amountTxn: 1000 }],
      },
      expectedStatus: 201,
    });
    assert(toUpper(fullApply.json?.row?.status) === "POSTED", "Full apply must be POSTED");
    const fullOpenItem = await getOpenItem(tenantId, docFull.openItemId);
    assert(amountsEqual(fullOpenItem?.residual_amount_txn, 0), "Full settle residual must be 0");
    assert(toUpper(fullOpenItem?.status) === "SETTLED", "Full settle status must be SETTLED");
    const fullDocument = await getDocument(tenantId, docFull.documentId);
    assert(toUpper(fullDocument?.status) === "SETTLED", "Document must move to SETTLED");

    const docPartial = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-02-05",
      dueDate: "2026-02-12",
      amountTxn: 500,
      amountBase: 500,
      currencyCode: "USD",
      fxRate: 1,
    });
    const partialApply = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixtures.legalEntityId,
        counterpartyId: fixtures.counterpartyId,
        settlementDate: "2026-03-02",
        currencyCode: "USD",
        incomingAmountTxn: 200,
        idempotencyKey: `PR07-PART-${stamp}`,
        autoAllocate: false,
        allocations: [{ openItemId: docPartial.openItemId, amountTxn: 200 }],
      },
      expectedStatus: 201,
    });
    const partialSettlementId = toNumber(partialApply.json?.row?.id);
    assert(partialSettlementId > 0, "Partial settlement id missing");
    const partialOpenItem = await getOpenItem(tenantId, docPartial.openItemId);
    assert(
      amountsEqual(partialOpenItem?.residual_amount_txn, 300),
      "Partial residual should be 300"
    );
    assert(
      toUpper(partialOpenItem?.status) === "PARTIALLY_SETTLED",
      "Partial status should be PARTIALLY_SETTLED"
    );

    const docAutoA = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-01-01",
      dueDate: "2026-01-05",
      amountTxn: 300,
      amountBase: 300,
      currencyCode: "USD",
      fxRate: 1,
    });
    const docAutoB = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-01-10",
      dueDate: "2026-02-15",
      amountTxn: 400,
      amountBase: 400,
      currencyCode: "USD",
      fxRate: 1,
    });
    const autoApply = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixtures.legalEntityId,
        counterpartyId: fixtures.counterpartyId,
        settlementDate: "2026-03-03",
        currencyCode: "USD",
        incomingAmountTxn: 350,
        idempotencyKey: `PR07-AUTO-${stamp}`,
        autoAllocate: true,
      },
      expectedStatus: 201,
    });
    const autoAllocations = autoApply.json?.allocations || [];
    assert(autoAllocations.length >= 2, "Auto allocation should produce two lines");
    assert(
      toNumber(autoAllocations[0]?.openItemId) === docAutoA.openItemId,
      "Auto allocation must prioritize oldest due date"
    );
    const autoTotalAllocatedTxn = autoAllocations.reduce(
      (sum, row) => sum + toNumber(row?.allocationAmountTxn),
      0
    );
    assert(amountsEqual(autoTotalAllocatedTxn, 350), "Auto allocation total should match funds");
    const autoAOpen = await getOpenItem(tenantId, docAutoA.openItemId);
    assert(amountsEqual(autoAOpen?.residual_amount_txn, 0), "Oldest due item should settle first");

    const docOverpay = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-02-08",
      dueDate: "2026-02-20",
      amountTxn: 100,
      amountBase: 100,
      currencyCode: "USD",
      fxRate: 1,
    });
    await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixtures.legalEntityId,
        counterpartyId: fixtures.counterpartyId,
        settlementDate: "2026-03-04",
        currencyCode: "USD",
        incomingAmountTxn: 150,
        idempotencyKey: `PR07-OVERPAY-${stamp}`,
        autoAllocate: false,
        allocations: [{ openItemId: docOverpay.openItemId, amountTxn: 100 }],
      },
      expectedStatus: 201,
    });
    const unappliedRowsAfterOverpay = await getUnappliedRows({
      tenantId,
      legalEntityId: fixtures.legalEntityId,
      counterpartyId: fixtures.counterpartyId,
    });
    const newestUnapplied = unappliedRowsAfterOverpay[unappliedRowsAfterOverpay.length - 1];
    assert(
      amountsEqual(newestUnapplied?.residual_amount_txn, 50),
      "Overpayment should create 50 unapplied residual"
    );

    const docConsumeUnapplied = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-02-09",
      dueDate: "2026-02-22",
      amountTxn: 40,
      amountBase: 40,
      currencyCode: "USD",
      fxRate: 1,
    });
    await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixtures.legalEntityId,
        counterpartyId: fixtures.counterpartyId,
        settlementDate: "2026-03-05",
        currencyCode: "USD",
        incomingAmountTxn: 0,
        idempotencyKey: `PR07-CONSUME-${stamp}`,
        autoAllocate: false,
        useUnappliedCash: true,
        allocations: [{ openItemId: docConsumeUnapplied.openItemId, amountTxn: 40 }],
      },
      expectedStatus: 201,
    });
    const consumedUnapplied = await query(
      `SELECT *
       FROM cari_unapplied_cash
       WHERE tenant_id = ?
         AND id = ?
       LIMIT 1`,
      [tenantId, toNumber(newestUnapplied?.id)]
    );
    const consumedRow = consumedUnapplied.rows?.[0];
    assert(
      amountsEqual(consumedRow?.residual_amount_txn, 10),
      "Later apply should consume unapplied down to 10"
    );

    const docFx = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-02-12",
      dueDate: "2026-02-25",
      amountTxn: 1000,
      amountBase: 1100,
      currencyCode: "EUR",
      fxRate: 1.1,
    });
    await insertFxRate({
      tenantId,
      rateDate: "2026-03-20",
      fromCurrencyCode: "EUR",
      toCurrencyCode: "USD",
      rate: 1.2,
    });
    const fxApply = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixtures.legalEntityId,
        counterpartyId: fixtures.counterpartyId,
        settlementDate: "2026-03-20",
        currencyCode: "EUR",
        incomingAmountTxn: 1000,
        idempotencyKey: `PR07-FX-${stamp}`,
        autoAllocate: false,
        allocations: [{ openItemId: docFx.openItemId, amountTxn: 1000 }],
      },
      expectedStatus: 201,
    });
    const fxSettlementId = toNumber(fxApply.json?.row?.id);
    assert(fxSettlementId > 0, "FX settlement id missing");
    assert(
      amountsEqual(fxApply.json?.metrics?.realizedFxNetBase, 100),
      "Realized FX net base should be 100 (1200-1100)"
    );
    assert(
      Array.isArray(fxApply.json?.followUpRisks) && fxApply.json.followUpRisks.length >= 3,
      "Follow-up risk notes should be returned"
    );
    const fxJournalEntryId = toNumber(fxApply.json?.journal?.journalEntryId);
    assert(fxJournalEntryId > 0, "FX settlement journal id missing");
    const fxJournalLines = await getJournalLines(fxJournalEntryId);
    assert(fxJournalLines.length === 2, "Settlement posting must use 2-line model");
    const fxAccountIds = new Set(fxJournalLines.map((line) => toNumber(line.account_id)));
    assert(
      fxAccountIds.has(fixtures.arControlAccountId) &&
        fxAccountIds.has(fixtures.arOffsetAccountId),
      "Settlement journal accounts must come from journal_purpose_accounts"
    );

    const residualBeforeReverseCheck = await query(
      `SELECT residual_amount_txn
       FROM cari_open_items
       WHERE tenant_id = ?
         AND id = ?
       LIMIT 1`,
      [tenantId, docPartial.openItemId]
    );
    const residualBeforeReverse = toNumber(
      residualBeforeReverseCheck.rows?.[0]?.residual_amount_txn
    );
    const reverseResponse = await apiRequest({
      token,
      method: "POST",
      requestPath: `/api/v1/cari/settlements/${partialSettlementId}/reverse`,
      body: { reason: "Test reversal", reversalDate: "2026-03-06" },
      expectedStatus: 201,
    });
    const reversalBatchId = toNumber(reverseResponse.json?.row?.id);
    assert(reversalBatchId > 0, "Reversal settlement batch missing");
    const originalBatchAfterReverse = await getSettlementBatch(tenantId, partialSettlementId);
    assert(
      toUpper(originalBatchAfterReverse?.status) === "REVERSED",
      "Original settlement batch must be REVERSED"
    );
    assert(
      toNumber(reverseResponse.json?.row?.reversalOfSettlementBatchId) === partialSettlementId,
      "Reversal linkage should point to original settlement"
    );
    const openItemAfterReverse = await getOpenItem(tenantId, docPartial.openItemId);
    const expectedResidualAfterReverse = Math.min(500, residualBeforeReverse + 200);
    assert(
      amountsEqual(openItemAfterReverse?.residual_amount_txn, expectedResidualAfterReverse),
      "Open item residual must restore after reversal"
    );
    const documentAfterReverse = await getDocument(tenantId, docPartial.documentId);
    assert(
      ["POSTED", "PARTIALLY_SETTLED"].includes(toUpper(documentAfterReverse?.status)),
      "Document status should stay valid after reversal"
    );

    const reverseTwice = await apiRequest({
      token,
      method: "POST",
      requestPath: `/api/v1/cari/settlements/${partialSettlementId}/reverse`,
      body: { reason: "Second reversal should fail" },
    });
    assert(reverseTwice.status === 400, "Second reversal must be blocked");

    const docIdem = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-02-13",
      dueDate: "2026-02-28",
      amountTxn: 120,
      amountBase: 120,
      currencyCode: "USD",
      fxRate: 1,
    });
    const idemRequest = {
      legalEntityId: fixtures.legalEntityId,
      counterpartyId: fixtures.counterpartyId,
      settlementDate: "2026-03-07",
      currencyCode: "USD",
      incomingAmountTxn: 120,
      idempotencyKey: `PR07-IDEM-${stamp}`,
      autoAllocate: false,
      allocations: [{ openItemId: docIdem.openItemId, amountTxn: 120 }],
    };
    const idemFirst = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: idemRequest,
      expectedStatus: 201,
    });
    const idemSecond = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: idemRequest,
      expectedStatus: 200,
    });
    assert(Boolean(idemSecond.json?.idempotentReplay), "Second call should be idempotent replay");
    assert(
      toNumber(idemFirst.json?.row?.id) === toNumber(idemSecond.json?.row?.id),
      "Idempotent replay should return same settlement batch id"
    );
    const idemAllocationCount = await countAllocationsByIdempotency({
      tenantId,
      legalEntityId: fixtures.legalEntityId,
      idempotencyKey: idemRequest.idempotencyKey,
    });
    assert(idemAllocationCount === 1, "Idempotency marker allocation row should exist once");
    const docConcurrent = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-02-14",
      dueDate: "2026-03-01",
      amountTxn: 100,
      amountBase: 100,
      currencyCode: "USD",
      fxRate: 1,
    });
    const concurrentA = apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixtures.legalEntityId,
        counterpartyId: fixtures.counterpartyId,
        settlementDate: "2026-03-08",
        currencyCode: "USD",
        incomingAmountTxn: 100,
        idempotencyKey: `PR07-CONC-A-${stamp}`,
        autoAllocate: false,
        allocations: [{ openItemId: docConcurrent.openItemId, amountTxn: 100 }],
      },
    });
    const concurrentB = apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixtures.legalEntityId,
        counterpartyId: fixtures.counterpartyId,
        settlementDate: "2026-03-08",
        currencyCode: "USD",
        incomingAmountTxn: 100,
        idempotencyKey: `PR07-CONC-B-${stamp}`,
        autoAllocate: false,
        allocations: [{ openItemId: docConcurrent.openItemId, amountTxn: 100 }],
      },
    });
    const [concAResult, concBResult] = await Promise.all([concurrentA, concurrentB]);
    const successfulConcurrent = [concAResult.status, concBResult.status].filter(
      (status) => status === 201
    ).length;
    assert(successfulConcurrent === 1, "Only one concurrent full allocation should succeed");

    const docDeadA = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-02-15",
      dueDate: "2026-03-03",
      amountTxn: 100,
      amountBase: 100,
      currencyCode: "USD",
      fxRate: 1,
    });
    const docDeadB = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-02-16",
      dueDate: "2026-03-04",
      amountTxn: 100,
      amountBase: 100,
      currencyCode: "USD",
      fxRate: 1,
    });
    const deadlockA = apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixtures.legalEntityId,
        counterpartyId: fixtures.counterpartyId,
        settlementDate: "2026-03-09",
        currencyCode: "USD",
        incomingAmountTxn: 100,
        idempotencyKey: `PR07-DEAD-A-${stamp}`,
        autoAllocate: false,
        allocations: [
          { openItemId: docDeadA.openItemId, amountTxn: 50 },
          { openItemId: docDeadB.openItemId, amountTxn: 50 },
        ],
      },
    });
    const deadlockB = apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixtures.legalEntityId,
        counterpartyId: fixtures.counterpartyId,
        settlementDate: "2026-03-09",
        currencyCode: "USD",
        incomingAmountTxn: 100,
        idempotencyKey: `PR07-DEAD-B-${stamp}`,
        autoAllocate: false,
        allocations: [
          { openItemId: docDeadB.openItemId, amountTxn: 50 },
          { openItemId: docDeadA.openItemId, amountTxn: 50 },
        ],
      },
    });
    const [deadAResult, deadBResult] = await Promise.all([deadlockA, deadlockB]);
    assert(deadAResult.status === 201, "Deadlock test request A should succeed");
    assert(deadBResult.status === 201, "Deadlock test request B should succeed");

    const docRiskMapping = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-02-17",
      dueDate: "2026-03-10",
      amountTxn: 100,
      amountBase: 100,
      currencyCode: "USD",
      fxRate: 1,
    });
    await query(
      `DELETE FROM journal_purpose_accounts
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND purpose_code = 'CARI_AR_OFFSET'`,
      [tenantId, fixtures.legalEntityId]
    );
    const missingMappingApply = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixtures.legalEntityId,
        counterpartyId: fixtures.counterpartyId,
        settlementDate: "2026-03-10",
        currencyCode: "USD",
        incomingAmountTxn: 100,
        idempotencyKey: `PR07-RISK-MAP-${stamp}`,
        autoAllocate: false,
        allocations: [{ openItemId: docRiskMapping.openItemId, amountTxn: 100 }],
      },
    });
    assert(missingMappingApply.status === 400, "Missing mapping must block posting");
    assert(
      String(missingMappingApply.json?.message || "").includes("Setup required"),
      "Missing mapping error should indicate setup requirement"
    );
    await query(
      `INSERT INTO journal_purpose_accounts (
          tenant_id,
          legal_entity_id,
          purpose_code,
          account_id
       )
       VALUES
         (?, ?, 'CARI_AR_CONTROL', ?),
         (?, ?, 'CARI_AR_OFFSET', ?)
       ON DUPLICATE KEY UPDATE account_id = VALUES(account_id)`,
      [
        tenantId,
        fixtures.legalEntityId,
        fixtures.arControlAccountId,
        tenantId,
        fixtures.legalEntityId,
        fixtures.arOffsetAccountId,
      ]
    );
    const restoredMappings = await query(
      `SELECT COUNT(*) AS row_count
       FROM journal_purpose_accounts
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND purpose_code IN ('CARI_AR_CONTROL', 'CARI_AR_OFFSET')`,
      [tenantId, fixtures.legalEntityId]
    );
    assert(
      toNumber(restoredMappings.rows?.[0]?.row_count) === 2,
      "AR purpose mappings should be restored"
    );

    const docRiskFx = await createAndPostDocument({
      token,
      fixtures,
      documentDate: "2026-02-18",
      dueDate: "2026-03-11",
      amountTxn: 200,
      amountBase: 220,
      currencyCode: "EUR",
      fxRate: 1.1,
    });
    const missingExactFxApply = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixtures.legalEntityId,
        counterpartyId: fixtures.counterpartyId,
        settlementDate: "2026-04-10",
        currencyCode: "EUR",
        incomingAmountTxn: 200,
        idempotencyKey: `PR07-RISK-FX-${stamp}`,
        autoAllocate: false,
        allocations: [{ openItemId: docRiskFx.openItemId, amountTxn: 200 }],
      },
    });
    assert(missingExactFxApply.status === 400, "Missing exact-date SPOT rate must fail");
    assert(
      String(missingExactFxApply.json?.message || "").includes("exact-date SPOT"),
      "FX risk error should mention exact-date SPOT behavior"
    );

    const applyAuditRows = await getAuditRows({
      tenantId,
      action: "cari.settlement.apply",
      resourceId: fxSettlementId,
    });
    assert(applyAuditRows.length > 0, "Apply audit log should exist");
    const reverseAuditRows = await getAuditRows({
      tenantId,
      action: "cari.settlement.reverse",
      resourceId: partialSettlementId,
    });
    assert(reverseAuditRows.length > 0, "Reverse audit log should exist");

    console.log("PR07 settlement + unapplied cash engine test passed.");
  } finally {
    if (!serverStopped) {
      server.kill();
      serverStopped = true;
    }
    await closePool();
  }
}

main().catch((err) => {
  console.error("PR07 settlement test failed:", err);
  closePool()
    .catch(() => {})
    .finally(() => {
      process.exit(1);
    });
});
