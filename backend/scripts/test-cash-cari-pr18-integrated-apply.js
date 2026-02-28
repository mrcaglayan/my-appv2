import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CASH_CARI_PR18_TEST_PORT || 3136);
const BASE_URL =
  process.env.CASH_CARI_PR18_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "CashCariPR18#12345";
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
      code: `PR18GC${stamp}`,
      name: `PR18 Group ${stamp}`,
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
      code: `PR18CAL${stamp}`,
      name: `PR18 Calendar ${stamp}`,
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
      code: `PR18LE${stamp}`,
      name: `PR18 Legal Entity ${stamp}`,
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
      code: `PR18BOOK${stamp}`,
      name: `PR18 Book ${stamp}`,
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
      code: `PR18COA${stamp}`,
      name: `PR18 CoA ${stamp}`,
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
  legalEntityId,
  counterpartyId,
  paymentTermId,
  direction,
  documentDate,
  dueDate,
  amountTxn,
  currencyCode,
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
      amountBase: amountTxn,
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

  return documentId;
}

async function getOpenItemByDocument({
  tenantId,
  legalEntityId,
  documentId,
}) {
  const result = await query(
    `SELECT *
     FROM cari_open_items
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND document_id = ?
     ORDER BY id ASC
     LIMIT 1`,
    [tenantId, legalEntityId, documentId]
  );
  return result.rows?.[0] || null;
}

async function countAllocationsForBatch({ tenantId, settlementBatchId }) {
  const result = await query(
    `SELECT COUNT(*) AS row_count
     FROM cari_settlement_allocations
     WHERE tenant_id = ?
       AND settlement_batch_id = ?`,
    [tenantId, settlementBatchId]
  );
  return toNumber(result.rows?.[0]?.row_count);
}

async function getUnappliedById({ tenantId, unappliedCashId }) {
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

async function createAndPostCashTransaction({
  token,
  tenantId,
  registerId,
  txnType,
  amount,
  currencyCode,
  counterAccountId,
  counterpartyType,
  counterpartyId,
  idempotencyKey,
  bookDate,
}) {
  const createResponse = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/cash/transactions",
    body: {
      tenantId,
      registerId,
      txnType,
      amount,
      currencyCode,
      counterAccountId,
      counterpartyType,
      counterpartyId,
      idempotencyKey,
      bookDate,
      description: `PR18 ${txnType} ${idempotencyKey}`,
    },
    expectedStatus: 200,
  });
  const transactionId = toNumber(createResponse.json?.row?.id);
  assert(transactionId > 0, "Cash transaction create failed");

  await apiRequest({
    token,
    method: "POST",
    requestPath: `/api/v1/cash/transactions/${transactionId}/post`,
    body: { tenantId },
    expectedStatus: 200,
  });

  return transactionId;
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantId = await createTenant(`PR18_${stamp}`, `PR18 Tenant ${stamp}`);
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const user = await createUserWithRole({
    tenantId,
    roleCode: "TenantAdmin",
    email: `pr18_admin_${stamp}@example.com`,
    passwordHash,
    name: "PR18 Admin",
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
      code: `PR18CNT${String(stamp).slice(-5)}`,
      name: "PR18 Counter",
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    });
    const cashRegisterAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR18CASH${String(stamp).slice(-5)}`,
      name: "PR18 Cash",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    const arControlAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR18ARCTL${String(stamp).slice(-5)}`,
      name: "PR18 AR Control",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    const arOffsetAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR18AROFF${String(stamp).slice(-5)}`,
      name: "PR18 AR Offset",
      accountType: "REVENUE",
      normalSide: "CREDIT",
    });
    const apControlAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR18APCTL${String(stamp).slice(-5)}`,
      name: "PR18 AP Control",
      accountType: "LIABILITY",
      normalSide: "CREDIT",
    });
    const apOffsetAccountId = await createAccount({
      token,
      coaId: base.coaId,
      code: `PR18APOFF${String(stamp).slice(-5)}`,
      name: "PR18 AP Offset",
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

    const registerId = await createRegister({
      token,
      tenantId,
      legalEntityId: base.legalEntityId,
      accountId: cashRegisterAccountId,
      code: `PR18REG${stamp}`,
      name: `PR18 Register ${stamp}`,
      currencyCode: base.currencyCode,
    });

    const paymentTermId = await createPaymentTerm({
      tenantId,
      legalEntityId: base.legalEntityId,
      code: `PR18TERM${stamp}`,
      name: `PR18 Term ${stamp}`,
    });

    const customerCounterpartyId = await createCounterparty({
      tenantId,
      legalEntityId: base.legalEntityId,
      code: `PR18CUS${stamp}`,
      name: `PR18 Customer ${stamp}`,
      paymentTermId,
      currencyCode: base.currencyCode,
      isCustomer: true,
      isVendor: false,
    });
    const vendorCounterpartyId = await createCounterparty({
      tenantId,
      legalEntityId: base.legalEntityId,
      code: `PR18VEN${stamp}`,
      name: `PR18 Vendor ${stamp}`,
      paymentTermId,
      currencyCode: base.currencyCode,
      isCustomer: false,
      isVendor: true,
    });

    // Smoke 1: Receipt 10,000 -> apply 7,000 + 3,000.
    const docAId = await createAndPostDocument({
      token,
      legalEntityId: base.legalEntityId,
      counterpartyId: customerCounterpartyId,
      paymentTermId,
      direction: "AR",
      documentDate: "2026-05-01",
      dueDate: "2026-05-31",
      amountTxn: 7000,
      currencyCode: base.currencyCode,
    });
    const docBId = await createAndPostDocument({
      token,
      legalEntityId: base.legalEntityId,
      counterpartyId: customerCounterpartyId,
      paymentTermId,
      direction: "AR",
      documentDate: "2026-05-02",
      dueDate: "2026-05-31",
      amountTxn: 3000,
      currencyCode: base.currencyCode,
    });
    const receiptTxnId = await createAndPostCashTransaction({
      token,
      tenantId,
      registerId,
      txnType: "RECEIPT",
      amount: 10000,
      currencyCode: base.currencyCode,
      counterAccountId,
      counterpartyType: "CUSTOMER",
      counterpartyId: customerCounterpartyId,
      idempotencyKey: `PR18-REC-1-${stamp}`,
      bookDate: "2026-05-10",
    });
    const applyAKey = `PR18-APPLY-1-${stamp}`;
    const applyA = await apiRequest({
      token,
      method: "POST",
      requestPath: `/api/v1/cash/transactions/${receiptTxnId}/apply-cari`,
      body: {
        settlementDate: "2026-05-10",
        idempotencyKey: applyAKey,
        autoAllocate: false,
        applications: [
          { cariDocumentId: docAId, amount: 7000 },
          { cariDocumentId: docBId, amount: 3000 },
        ],
      },
      expectedStatus: 201,
    });
    const settlementAId = toNumber(applyA.json?.row?.id);
    assert(settlementAId > 0, "Smoke1 settlement id missing");
    assert(
      toNumber(applyA.json?.unapplied?.createdUnappliedCashId) === 0,
      "Smoke1 should not create unapplied residual"
    );
    const docAOpen = await getOpenItemByDocument({
      tenantId,
      legalEntityId: base.legalEntityId,
      documentId: docAId,
    });
    const docBOpen = await getOpenItemByDocument({
      tenantId,
      legalEntityId: base.legalEntityId,
      documentId: docBId,
    });
    assert(
      amountsEqual(docAOpen?.residual_amount_txn, 0) && toUpper(docAOpen?.status) === "SETTLED",
      "Smoke1 document A must be fully settled"
    );
    assert(
      amountsEqual(docBOpen?.residual_amount_txn, 0) && toUpper(docBOpen?.status) === "SETTLED",
      "Smoke1 document B must be fully settled"
    );
    const settlementAAllocCount = await countAllocationsForBatch({
      tenantId,
      settlementBatchId: settlementAId,
    });
    assert(settlementAAllocCount === 2, "Smoke1 should create exactly 2 allocations");

    const applyAReplay = await apiRequest({
      token,
      method: "POST",
      requestPath: `/api/v1/cash/transactions/${receiptTxnId}/apply-cari`,
      body: {
        settlementDate: "2026-05-10",
        idempotencyKey: applyAKey,
        autoAllocate: false,
        applications: [
          { cariDocumentId: docAId, amount: 7000 },
          { cariDocumentId: docBId, amount: 3000 },
        ],
      },
      expectedStatus: 200,
    });
    assert(
      Boolean(applyAReplay.json?.idempotentReplay),
      "Smoke1 duplicate apply should return idempotentReplay=true"
    );
    assert(
      toNumber(applyAReplay.json?.row?.id) === settlementAId,
      "Smoke1 duplicate apply should return the same settlement id"
    );
    const settlementAAllocCountAfterReplay = await countAllocationsForBatch({
      tenantId,
      settlementBatchId: settlementAId,
    });
    assert(
      settlementAAllocCountAfterReplay === 2,
      "Smoke1 duplicate apply must not create extra allocations"
    );

    const applyAReplayDifferentKey = await apiRequest({
      token,
      method: "POST",
      requestPath: `/api/v1/cash/transactions/${receiptTxnId}/apply-cari`,
      body: {
        settlementDate: "2026-05-10",
        idempotencyKey: `PR18-APPLY-1-RETRY-${stamp}`,
        autoAllocate: false,
        applications: [{ cariDocumentId: docAId, amount: 7000 }],
      },
      expectedStatus: 200,
    });
    assert(
      toNumber(applyAReplayDifferentKey.json?.row?.id) === settlementAId,
      "Smoke1 retry with different idempotency key should replay by cashTransactionId"
    );

    // Smoke 2: Receipt 10,000 -> apply 6,000 only, remaining 4,000 unapplied.
    const docCId = await createAndPostDocument({
      token,
      legalEntityId: base.legalEntityId,
      counterpartyId: customerCounterpartyId,
      paymentTermId,
      direction: "AR",
      documentDate: "2026-05-03",
      dueDate: "2026-05-31",
      amountTxn: 6000,
      currencyCode: base.currencyCode,
    });
    const receiptTxn2Id = await createAndPostCashTransaction({
      token,
      tenantId,
      registerId,
      txnType: "RECEIPT",
      amount: 10000,
      currencyCode: base.currencyCode,
      counterAccountId,
      counterpartyType: "CUSTOMER",
      counterpartyId: customerCounterpartyId,
      idempotencyKey: `PR18-REC-2-${stamp}`,
      bookDate: "2026-05-11",
    });
    const applyB = await apiRequest({
      token,
      method: "POST",
      requestPath: `/api/v1/cash/transactions/${receiptTxn2Id}/apply-cari`,
      body: {
        settlementDate: "2026-05-11",
        idempotencyKey: `PR18-APPLY-2-${stamp}`,
        autoAllocate: false,
        applications: [{ cariDocumentId: docCId, amount: 6000 }],
      },
      expectedStatus: 201,
    });
    const settlementBId = toNumber(applyB.json?.row?.id);
    assert(settlementBId > 0, "Smoke2 settlement id missing");
    const createdUnappliedBId = toNumber(applyB.json?.unapplied?.createdUnappliedCashId);
    assert(createdUnappliedBId > 0, "Smoke2 should create unapplied residual");
    const docCOpen = await getOpenItemByDocument({
      tenantId,
      legalEntityId: base.legalEntityId,
      documentId: docCId,
    });
    assert(
      amountsEqual(docCOpen?.residual_amount_txn, 0),
      "Smoke2 allocated document should be fully settled"
    );
    const unappliedB = await getUnappliedById({
      tenantId,
      unappliedCashId: createdUnappliedBId,
    });
    assert(Boolean(unappliedB), "Smoke2 unapplied residual row missing");
    assert(
      toNumber(unappliedB?.cash_transaction_id) === receiptTxn2Id,
      "Smoke2 unapplied residual must link to source cash transaction"
    );
    assert(
      amountsEqual(unappliedB?.residual_amount_txn, 4000),
      "Smoke2 residual unapplied must be 4,000"
    );

    // Smoke 3: Payout to vendor bill.
    const vendorDocId = await createAndPostDocument({
      token,
      legalEntityId: base.legalEntityId,
      counterpartyId: vendorCounterpartyId,
      paymentTermId,
      direction: "AP",
      documentDate: "2026-05-04",
      dueDate: "2026-05-31",
      amountTxn: 2500,
      currencyCode: base.currencyCode,
    });
    const vendorOpenBefore = await getOpenItemByDocument({
      tenantId,
      legalEntityId: base.legalEntityId,
      documentId: vendorDocId,
    });
    assert(toNumber(vendorOpenBefore?.id) > 0, "Smoke3 vendor open item missing");

    const payoutTxnId = await createAndPostCashTransaction({
      token,
      tenantId,
      registerId,
      txnType: "PAYOUT",
      amount: 2500,
      currencyCode: base.currencyCode,
      counterAccountId,
      counterpartyType: "VENDOR",
      counterpartyId: vendorCounterpartyId,
      idempotencyKey: `PR18-PAY-1-${stamp}`,
      bookDate: "2026-05-12",
    });
    const payoutApply = await apiRequest({
      token,
      method: "POST",
      requestPath: `/api/v1/cash/transactions/${payoutTxnId}/apply-cari`,
      body: {
        settlementDate: "2026-05-12",
        idempotencyKey: `PR18-APPLY-3-${stamp}`,
        autoAllocate: false,
        applications: [{ openItemId: toNumber(vendorOpenBefore?.id), amountTxn: 2500 }],
      },
      expectedStatus: 201,
    });
    assert(toNumber(payoutApply.json?.row?.id) > 0, "Smoke3 payout settlement id missing");
    const vendorOpenAfter = await getOpenItemByDocument({
      tenantId,
      legalEntityId: base.legalEntityId,
      documentId: vendorDocId,
    });
    assert(
      amountsEqual(vendorOpenAfter?.residual_amount_txn, 0) &&
        toUpper(vendorOpenAfter?.status) === "SETTLED",
      "Smoke3 vendor bill must be settled by payout"
    );

    // Smoke 4: No applications => create unapplied cash row.
    const receiptTxn3Id = await createAndPostCashTransaction({
      token,
      tenantId,
      registerId,
      txnType: "RECEIPT",
      amount: 5000,
      currencyCode: base.currencyCode,
      counterAccountId,
      counterpartyType: "CUSTOMER",
      counterpartyId: customerCounterpartyId,
      idempotencyKey: `PR18-REC-3-${stamp}`,
      bookDate: "2026-05-13",
    });
    const applyNoDocsKey = `PR18-APPLY-NODOCS-${stamp}`;
    const noDocsApply = await apiRequest({
      token,
      method: "POST",
      requestPath: `/api/v1/cash/transactions/${receiptTxn3Id}/apply-cari`,
      body: {
        settlementDate: "2026-05-13",
        idempotencyKey: applyNoDocsKey,
        autoAllocate: false,
      },
      expectedStatus: 201,
    });
    assert(noDocsApply.json?.row === null, "Smoke4 no-doc apply should not create settlement row");
    const noDocsUnappliedId = toNumber(noDocsApply.json?.unapplied?.createdUnappliedCashId);
    assert(noDocsUnappliedId > 0, "Smoke4 no-doc apply should create unapplied row");
    const noDocsUnapplied = await getUnappliedById({
      tenantId,
      unappliedCashId: noDocsUnappliedId,
    });
    assert(
      toNumber(noDocsUnapplied?.cash_transaction_id) === receiptTxn3Id,
      "Smoke4 no-doc unapplied must link to cash transaction"
    );
    assert(
      amountsEqual(noDocsUnapplied?.residual_amount_txn, 5000),
      "Smoke4 no-doc unapplied residual must equal cash amount"
    );

    const noDocsApplyReplay = await apiRequest({
      token,
      method: "POST",
      requestPath: `/api/v1/cash/transactions/${receiptTxn3Id}/apply-cari`,
      body: {
        settlementDate: "2026-05-13",
        idempotencyKey: applyNoDocsKey,
        autoAllocate: false,
      },
      expectedStatus: 200,
    });
    assert(
      Boolean(noDocsApplyReplay.json?.idempotentReplay),
      "Smoke4 duplicate no-doc apply must be idempotent"
    );
    assert(
      toNumber(noDocsApplyReplay.json?.unapplied?.createdUnappliedCashId) === noDocsUnappliedId,
      "Smoke4 duplicate no-doc apply should return same unapplied id"
    );

    console.log("PR18 cash->cari integrated apply smoke test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId,
          legalEntityId: base.legalEntityId,
          registerId,
          receiptTxnId,
          settlementAId,
          receiptTxn2Id,
          settlementBId,
          createdUnappliedBId,
          payoutTxnId,
          receiptTxn3Id,
          noDocsUnappliedId,
        },
        null,
        2
      )
    );
  } finally {
    if (!serverStopped) {
      server.kill();
      serverStopped = true;
    }
    await closePool();
  }
}

main().catch((err) => {
  console.error("PR18 cash->cari integrated apply smoke test failed.");
  console.error(err);
  process.exitCode = 1;
});
