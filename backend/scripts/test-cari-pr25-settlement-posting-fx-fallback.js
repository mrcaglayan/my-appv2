import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CARI_PR25_TEST_PORT || 3137);
const BASE_URL = process.env.CARI_PR25_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "CariPR25#12345";
const TEST_FISCAL_YEAR = 2026;

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
    env: {
      ...process.env,
      PORT: String(PORT),
      CARI_SETTLEMENT_FX_FALLBACK_MODE: "PRIOR_DATE",
      CARI_SETTLEMENT_FX_FALLBACK_MAX_DAYS: "7",
    },
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

async function bootstrapOrgAndGlBase(token, stamp) {
  const countryResult = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(countryResult.rows?.[0]?.id);
  const currencyCode = String(
    countryResult.rows?.[0]?.default_currency_code || "USD"
  ).toUpperCase();
  assert(countryId > 0, "US country row is required");

  const groupRes = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/org/group-companies",
    body: {
      code: `PR25GC${stamp}`,
      name: `PR25 Group ${stamp}`,
    },
    expectedStatus: 201,
  });
  const groupCompanyId = toNumber(groupRes.json?.id);
  assert(groupCompanyId > 0, "groupCompanyId not created");

  const calendarRes = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/org/fiscal-calendars",
    body: {
      code: `PR25CAL${stamp}`,
      name: `PR25 Calendar ${stamp}`,
      yearStartMonth: 1,
      yearStartDay: 1,
    },
    expectedStatus: 201,
  });
  const calendarId = toNumber(calendarRes.json?.id);
  assert(calendarId > 0, "calendarId not created");

  await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/org/fiscal-periods/generate",
    body: {
      calendarId,
      fiscalYear: TEST_FISCAL_YEAR,
    },
    expectedStatus: 201,
  });

  const entityRes = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `PR25LE${stamp}`,
      name: `PR25 Legal Entity ${stamp}`,
      countryId,
      functionalCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const legalEntityId = toNumber(entityRes.json?.id);
  assert(legalEntityId > 0, "legalEntityId not created");

  const bookRes = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/gl/books",
    body: {
      legalEntityId,
      calendarId,
      code: `PR25BOOK${stamp}`,
      name: `PR25 Book ${stamp}`,
      bookType: "LOCAL",
      baseCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const bookId = toNumber(bookRes.json?.id);
  assert(bookId > 0, "bookId not created");

  const coaRes = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/gl/coas",
    body: {
      scope: "LEGAL_ENTITY",
      legalEntityId,
      code: `PR25COA${stamp}`,
      name: `PR25 CoA ${stamp}`,
    },
    expectedStatus: 201,
  });
  const coaId = toNumber(coaRes.json?.id);
  assert(coaId > 0, "coaId not created");

  return {
    legalEntityId,
    currencyCode,
    bookId,
    coaId,
  };
}

async function createAccount({
  token,
  coaId,
  code,
  name,
  accountType,
  normalSide,
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/gl/accounts",
    body: {
      coaId,
      code,
      name,
      accountType,
      normalSide,
      allowPosting: true,
    },
    expectedStatus: 201,
  });
  const accountId = toNumber(response.json?.id);
  assert(accountId > 0, `Account create failed for ${code}`);
  return accountId;
}

async function createRegister({
  token,
  tenantId,
  legalEntityId,
  accountId,
  code,
  name,
  currencyCode,
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/cash/registers",
    body: {
      tenantId,
      legalEntityId,
      accountId,
      code,
      name,
      registerType: "DRAWER",
      sessionMode: "OPTIONAL",
      currencyCode,
      status: "ACTIVE",
    },
    expectedStatus: 200,
  });
  const registerId = toNumber(response.json?.row?.id);
  assert(registerId > 0, "Cash register create failed");
  return registerId;
}

async function createPaymentTerm({ tenantId, legalEntityId, code, name }) {
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
    [tenantId, legalEntityId, code, name]
  );
  const result = await query(
    `SELECT id
     FROM payment_terms
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, code]
  );
  const paymentTermId = toNumber(result.rows?.[0]?.id);
  assert(paymentTermId > 0, "Payment term create failed");
  return paymentTermId;
}

async function createCounterparty({
  tenantId,
  legalEntityId,
  code,
  name,
  paymentTermId,
  currencyCode,
  isCustomer,
  isVendor,
}) {
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
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      legalEntityId,
      code,
      name,
      isCustomer ? 1 : 0,
      isVendor ? 1 : 0,
      currencyCode,
      paymentTermId,
    ]
  );
  const result = await query(
    `SELECT id
     FROM counterparties
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, code]
  );
  const counterpartyId = toNumber(result.rows?.[0]?.id);
  assert(counterpartyId > 0, `Counterparty create failed for ${code}`);
  return counterpartyId;
}

async function upsertPurposeAccount({
  tenantId,
  legalEntityId,
  purposeCode,
  accountId,
}) {
  await query(
    `INSERT INTO journal_purpose_accounts (
        tenant_id,
        legal_entity_id,
        purpose_code,
        account_id
     )
     VALUES (?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE account_id = VALUES(account_id)`,
    [tenantId, legalEntityId, purposeCode, accountId]
  );
}

async function upsertCariPostingAccounts({
  tenantId,
  legalEntityId,
  arControlAccountId,
  arOffsetAccountId,
  apControlAccountId,
  apOffsetAccountId,
}) {
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
}

async function createAndPostDocument({
  token,
  tenantId,
  legalEntityId,
  counterpartyId,
  paymentTermId,
  direction,
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
      legalEntityId,
      counterpartyId,
      paymentTermId,
      direction,
      documentType: "INVOICE",
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
    [tenantId, legalEntityId, documentId]
  );
  const openItemId = toNumber(openItemResult.rows?.[0]?.id);
  assert(openItemId > 0, "Open item missing after post");

  return { documentId, openItemId };
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

async function getJournalAccountIds(journalEntryId) {
  const result = await query(
    `SELECT account_id
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no ASC`,
    [journalEntryId]
  );
  return (result.rows || []).map((row) => toNumber(row.account_id)).filter((id) => id > 0);
}

async function applySettlement({
  token,
  legalEntityId,
  counterpartyId,
  settlementDate,
  currencyCode,
  incomingAmountTxn,
  idempotencyKey,
  autoAllocate = false,
  useUnappliedCash = true,
  allocations = [],
  paymentChannel = "MANUAL",
  linkedCashTransaction = null,
  fxFallbackMode = null,
  fxFallbackMaxDays = null,
  expectedStatus = 201,
}) {
  const payload = {
    legalEntityId,
    counterpartyId,
    settlementDate,
    currencyCode,
    incomingAmountTxn,
    idempotencyKey,
    autoAllocate,
    useUnappliedCash,
    allocations,
    paymentChannel,
  };
  if (linkedCashTransaction) {
    payload.linkedCashTransaction = linkedCashTransaction;
  }
  if (fxFallbackMode) {
    payload.fxFallbackMode = fxFallbackMode;
  }
  if (fxFallbackMaxDays !== null && fxFallbackMaxDays !== undefined) {
    payload.fxFallbackMaxDays = fxFallbackMaxDays;
  }

  return apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/cari/settlements/apply",
    body: payload,
    expectedStatus,
  });
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantId = await createTenant(`PR25_${stamp}`, `PR25 Tenant ${stamp}`);
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const user = await createUserWithRole({
    tenantId,
    roleCode: "TenantAdmin",
    email: `pr25_admin_${stamp}@example.com`,
    passwordHash,
    name: "PR25 Admin",
  });

  const server = startServerProcess();
  let serverStopped = false;
  try {
    await waitForServer();
    const token = await login(user.email, TEST_PASSWORD);

    const base = await bootstrapOrgAndGlBase(token, stamp);

    const counterAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR25CNT${String(stamp).slice(-5)}`,
      name: "PR25 Counter",
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    });
    const cashRegisterAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR25CASH${String(stamp).slice(-5)}`,
      name: "PR25 Cash Register",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    const arControlAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR25ARCTL${String(stamp).slice(-5)}`,
      name: "PR25 AR Control",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    const arOffsetAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR25AROFF${String(stamp).slice(-5)}`,
      name: "PR25 AR Offset Generic",
      accountType: "REVENUE",
      normalSide: "CREDIT",
    });
    const arCashOffsetAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR25ARCAS${String(stamp).slice(-5)}`,
      name: "PR25 AR Offset Cash Context",
      accountType: "REVENUE",
      normalSide: "CREDIT",
    });
    const arOnAccountOffsetAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR25ARONA${String(stamp).slice(-5)}`,
      name: "PR25 AR Offset OnAccount Context",
      accountType: "REVENUE",
      normalSide: "CREDIT",
    });
    const apControlAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR25APCTL${String(stamp).slice(-5)}`,
      name: "PR25 AP Control",
      accountType: "LIABILITY",
      normalSide: "CREDIT",
    });
    const apOffsetAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR25APOFF${String(stamp).slice(-5)}`,
      name: "PR25 AP Offset",
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    });

    await upsertCariPostingAccounts({
      tenantId,
      legalEntityId: base.legalEntityId,
      arControlAccountId,
      arOffsetAccountId,
      apControlAccountId,
      apOffsetAccountId,
    });
    await upsertPurposeAccount({
      tenantId,
      legalEntityId: base.legalEntityId,
      purposeCode: "CARI_AR_OFFSET_CASH",
      accountId: arCashOffsetAccountId,
    });
    await upsertPurposeAccount({
      tenantId,
      legalEntityId: base.legalEntityId,
      purposeCode: "CARI_AR_OFFSET_ON_ACCOUNT",
      accountId: arOnAccountOffsetAccountId,
    });

    const registerId = await createRegister({
      token,
      tenantId,
      legalEntityId: base.legalEntityId,
      accountId: cashRegisterAccountId,
      code: `PR25REG${stamp}`,
      name: `PR25 Register ${stamp}`,
      currencyCode: "EUR",
    });

    const paymentTermId = await createPaymentTerm({
      tenantId,
      legalEntityId: base.legalEntityId,
      code: `PR25TERM${stamp}`,
      name: `PR25 Term ${stamp}`,
    });
    const counterpartyId = await createCounterparty({
      tenantId,
      legalEntityId: base.legalEntityId,
      code: `PR25CP${stamp}`,
      name: `PR25 Counterparty ${stamp}`,
      paymentTermId,
      currencyCode: base.currencyCode,
      isCustomer: true,
      isVendor: false,
    });

    await insertFxRate({
      tenantId,
      rateDate: "2026-06-10",
      fromCurrencyCode: "EUR",
      toCurrencyCode: "USD",
      rate: 1.2,
    });
    const fxCashDoc = await createAndPostDocument({
      token,
      tenantId,
      legalEntityId: base.legalEntityId,
      counterpartyId,
      paymentTermId,
      direction: "AR",
      documentDate: "2026-06-01",
      dueDate: "2026-06-10",
      amountTxn: 1000,
      amountBase: 1100,
      currencyCode: "EUR",
      fxRate: 1.1,
    });
    const cashFxSettlement = await applySettlement({
      token,
      legalEntityId: base.legalEntityId,
      counterpartyId,
      settlementDate: "2026-06-10",
      currencyCode: "EUR",
      incomingAmountTxn: 1000,
      idempotencyKey: `PR25-CASH-FX-${stamp}`,
      paymentChannel: "CASH",
      linkedCashTransaction: {
        registerId,
        counterAccountId,
        bookDate: "2026-06-10",
        idempotencyKey: `PR25-CASH-TXN-${stamp}`,
        integrationEventUid: `PR25-CASH-EVT-${stamp}`,
      },
      autoAllocate: false,
      allocations: [{ openItemId: fxCashDoc.openItemId, amountTxn: 1000 }],
      expectedStatus: 201,
    });
    const cashFxSettlementId = toNumber(cashFxSettlement.json?.row?.id);
    assert(cashFxSettlementId > 0, "Cash-linked FX settlement id missing");
    assert(
      toNumber(cashFxSettlement.json?.row?.cashTransactionId) > 0,
      "Cash-linked settlement should create/link a cash transaction"
    );
    assert(
      toUpper(cashFxSettlement.json?.fx?.settlementFxSource) === "FX_TABLE_EXACT_SPOT",
      "Cash-linked FX settlement should use exact-date table rate when available"
    );
    assert(
      amountsEqual(cashFxSettlement.json?.fx?.settlementFxRate, 1.2),
      "Cash-linked FX settlement should use exact-date rate=1.2"
    );
    assert(
      toUpper(cashFxSettlement.json?.metrics?.journalPurposeAccounts?.offsetPurposeCode) ===
        "CARI_AR_OFFSET_CASH",
      "Cash-linked settlement should resolve cash-specific offset purpose code"
    );
    const cashFxJournalEntryId = toNumber(cashFxSettlement.json?.journal?.journalEntryId);
    const cashFxJournalAccounts = new Set(await getJournalAccountIds(cashFxJournalEntryId));
    assert(
      cashFxJournalAccounts.has(arControlAccountId) &&
        cashFxJournalAccounts.has(arCashOffsetAccountId),
      "Cash-linked settlement journal should use AR control + cash-specific AR offset"
    );

    await insertFxRate({
      tenantId,
      rateDate: "2026-06-15",
      fromCurrencyCode: "EUR",
      toCurrencyCode: "USD",
      rate: 1.25,
    });
    const fxPriorDoc = await createAndPostDocument({
      token,
      tenantId,
      legalEntityId: base.legalEntityId,
      counterpartyId,
      paymentTermId,
      direction: "AR",
      documentDate: "2026-06-05",
      dueDate: "2026-06-16",
      amountTxn: 500,
      amountBase: 550,
      currencyCode: "EUR",
      fxRate: 1.1,
    });
    const priorFxSettlement = await applySettlement({
      token,
      legalEntityId: base.legalEntityId,
      counterpartyId,
      settlementDate: "2026-06-16",
      currencyCode: "EUR",
      incomingAmountTxn: 500,
      idempotencyKey: `PR25-FX-FALLBACK-${stamp}`,
      paymentChannel: "MANUAL",
      fxFallbackMode: "PRIOR_DATE",
      fxFallbackMaxDays: 7,
      autoAllocate: false,
      allocations: [{ openItemId: fxPriorDoc.openItemId, amountTxn: 500 }],
      expectedStatus: 201,
    });
    assert(
      toUpper(priorFxSettlement.json?.fx?.settlementFxSource) === "FX_TABLE_PRIOR_SPOT",
      "Missing exact-date FX should use nearest prior SPOT when fallback enabled"
    );
    assert(
      String(priorFxSettlement.json?.fx?.fxRateDate || "") === "2026-06-15",
      "Fallback FX rate date should be nearest prior date"
    );
    assert(
      amountsEqual(priorFxSettlement.json?.fx?.settlementFxRate, 1.25),
      "Fallback FX settlement should use prior-date rate=1.25"
    );

    const manualLocalDoc = await createAndPostDocument({
      token,
      tenantId,
      legalEntityId: base.legalEntityId,
      counterpartyId,
      paymentTermId,
      direction: "AR",
      documentDate: "2026-06-07",
      dueDate: "2026-06-17",
      amountTxn: 300,
      amountBase: 300,
      currencyCode: "USD",
      fxRate: 1,
    });
    const manualLocalSettlement = await applySettlement({
      token,
      legalEntityId: base.legalEntityId,
      counterpartyId,
      settlementDate: "2026-06-17",
      currencyCode: "USD",
      incomingAmountTxn: 300,
      idempotencyKey: `PR25-MANUAL-LOCAL-${stamp}`,
      paymentChannel: "MANUAL",
      autoAllocate: false,
      allocations: [{ openItemId: manualLocalDoc.openItemId, amountTxn: 300 }],
      expectedStatus: 201,
    });
    assert(
      toUpper(manualLocalSettlement.json?.fx?.settlementFxSource) === "PARITY",
      "Manual local-currency settlement should remain parity-based"
    );
    assert(
      toUpper(manualLocalSettlement.json?.metrics?.journalPurposeAccounts?.offsetPurposeCode) ===
        "CARI_AR_OFFSET",
      "Manual local settlement should fallback to generic AR offset purpose"
    );
    const manualLocalJournalEntryId = toNumber(
      manualLocalSettlement.json?.journal?.journalEntryId
    );
    const manualLocalAccounts = new Set(await getJournalAccountIds(manualLocalJournalEntryId));
    assert(
      manualLocalAccounts.has(arControlAccountId) &&
        manualLocalAccounts.has(arOffsetAccountId),
      "Manual local settlement journal should use generic AR control/offset accounts"
    );

    const unappliedSourceDoc = await createAndPostDocument({
      token,
      tenantId,
      legalEntityId: base.legalEntityId,
      counterpartyId,
      paymentTermId,
      direction: "AR",
      documentDate: "2026-06-08",
      dueDate: "2026-06-18",
      amountTxn: 100,
      amountBase: 100,
      currencyCode: "USD",
      fxRate: 1,
    });
    const unappliedSourceSettlement = await applySettlement({
      token,
      legalEntityId: base.legalEntityId,
      counterpartyId,
      settlementDate: "2026-06-18",
      currencyCode: "USD",
      incomingAmountTxn: 150,
      idempotencyKey: `PR25-UNAP-SRC-${stamp}`,
      paymentChannel: "MANUAL",
      autoAllocate: false,
      allocations: [{ openItemId: unappliedSourceDoc.openItemId, amountTxn: 100 }],
      expectedStatus: 201,
    });
    const unappliedId = toNumber(unappliedSourceSettlement.json?.unapplied?.createdUnappliedCashId);
    assert(unappliedId > 0, "Over-collection should create unapplied cash row");

    const unappliedTargetDoc = await createAndPostDocument({
      token,
      tenantId,
      legalEntityId: base.legalEntityId,
      counterpartyId,
      paymentTermId,
      direction: "AR",
      documentDate: "2026-06-09",
      dueDate: "2026-06-19",
      amountTxn: 40,
      amountBase: 40,
      currencyCode: "USD",
      fxRate: 1,
    });
    const onAccountSettlement = await applySettlement({
      token,
      legalEntityId: base.legalEntityId,
      counterpartyId,
      settlementDate: "2026-06-19",
      currencyCode: "USD",
      incomingAmountTxn: 0,
      idempotencyKey: `PR25-ON-ACCOUNT-${stamp}`,
      paymentChannel: "MANUAL",
      autoAllocate: false,
      useUnappliedCash: true,
      allocations: [{ openItemId: unappliedTargetDoc.openItemId, amountTxn: 40 }],
      expectedStatus: 201,
    });
    assert(
      toUpper(onAccountSettlement.json?.metrics?.journalPurposeAccounts?.offsetPurposeCode) ===
        "CARI_AR_OFFSET_ON_ACCOUNT",
      "On-account settlement should resolve ON_ACCOUNT-specific purpose code"
    );
    const onAccountJournalEntryId = toNumber(onAccountSettlement.json?.journal?.journalEntryId);
    const onAccountAccounts = new Set(await getJournalAccountIds(onAccountJournalEntryId));
    assert(
      onAccountAccounts.has(arControlAccountId) &&
        onAccountAccounts.has(arOnAccountOffsetAccountId),
      "On-account settlement journal should use AR control + on-account offset"
    );

    const missingFxDoc = await createAndPostDocument({
      token,
      tenantId,
      legalEntityId: base.legalEntityId,
      counterpartyId,
      paymentTermId,
      direction: "AR",
      documentDate: "2026-07-01",
      dueDate: "2026-07-20",
      amountTxn: 100,
      amountBase: 110,
      currencyCode: "EUR",
      fxRate: 1.1,
    });
    const missingFxSettlement = await applySettlement({
      token,
      legalEntityId: base.legalEntityId,
      counterpartyId,
      settlementDate: "2026-07-20",
      currencyCode: "EUR",
      incomingAmountTxn: 100,
      idempotencyKey: `PR25-NO-FX-${stamp}`,
      paymentChannel: "MANUAL",
      autoAllocate: false,
      fxFallbackMode: "PRIOR_DATE",
      fxFallbackMaxDays: 3,
      allocations: [{ openItemId: missingFxDoc.openItemId, amountTxn: 100 }],
      expectedStatus: 400,
    });
    assert(
      String(missingFxSettlement.json?.message || "").includes("fxFallbackMaxDays"),
      "Missing fallback FX should return a clear fxFallbackMaxDays validation error"
    );

    console.log("CARI PR-25 settlement posting refinement + FX fallback test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId,
          legalEntityId: base.legalEntityId,
          counterpartyId,
          cashSettlementId: cashFxSettlementId,
          fallbackFxSource: priorFxSettlement.json?.fx?.settlementFxSource || null,
          manualOffsetPurpose:
            manualLocalSettlement.json?.metrics?.journalPurposeAccounts?.offsetPurposeCode || null,
          onAccountOffsetPurpose:
            onAccountSettlement.json?.metrics?.journalPurposeAccounts?.offsetPurposeCode || null,
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
    await closePool();
  }
}

main().catch((err) => {
  console.error("CARI PR-25 settlement posting refinement test failed:", err);
  process.exitCode = 1;
});
