import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CASH_PR07_TEST_PORT || 3112);
const BASE_URL = process.env.CASH_PR07_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
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

function toErrorText(jsonPayload) {
  if (jsonPayload === null || jsonPayload === undefined) {
    return "";
  }
  if (typeof jsonPayload === "string") {
    return jsonPayload;
  }
  if (typeof jsonPayload.error === "string") {
    return jsonPayload.error;
  }
  if (typeof jsonPayload.message === "string") {
    return jsonPayload.message;
  }
  try {
    return JSON.stringify(jsonPayload);
  } catch {
    return String(jsonPayload);
  }
}

function parseTxnNo(txnNo) {
  const match = String(txnNo || "").match(/^CASH-([A-Z0-9]+)-(\d{4})-(\d{6})$/);
  if (!match) {
    return null;
  }
  return {
    entitySegment: match[1],
    year: Number(match[2]),
    seq: Number(match[3]),
  };
}

async function apiRequest({
  token,
  method = "GET",
  path,
  body,
  expectedStatus,
}) {
  const headers = { "Content-Type": "application/json" };
  if (token) {
    headers.Cookie = token;
  }

  const response = await fetch(`${BASE_URL}${path}`, {
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
      `${method} ${path} expected ${expectedStatus}, got ${response.status}. response=${JSON.stringify(
        json
      )}`
    );
  }

  return { status: response.status, json, cookie };
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
      // ignore until timeout
    }
    await sleep(350);
  }
  throw new Error(`Server did not start within ${SERVER_START_TIMEOUT_MS}ms`);
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

async function login(email, password) {
  const response = await apiRequest({
    method: "POST",
    path: "/auth/login",
    body: { email, password },
    expectedStatus: 200,
  });
  const sessionCookie = response.cookie;
  assert(Boolean(sessionCookie), "Login cookie missing");
  return sessionCookie;
}

async function createTenantAndAdmin() {
  const stamp = Date.now() + Math.floor(Math.random() * 1000);
  const tenantCode = `CASH07_${stamp}`;
  const tenantName = `Cash PR07 ${stamp}`;
  const adminEmail = `cash_pr07_admin_${stamp}@example.com`;
  const password = "CashPR07#12345";
  const passwordHash = await bcrypt.hash(password, 10);

  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE name = VALUES(name)`,
    [tenantCode, tenantName]
  );

  await seedCore({
    ensureDefaultTenantIfMissing: true,
  });

  const tenantResult = await query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [tenantCode]
  );
  const tenantId = toNumber(tenantResult.rows[0]?.id);
  assert(tenantId > 0, "Failed to resolve tenant");

  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, adminEmail, passwordHash, "Cash PR07 Admin"]
  );

  const userResult = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, adminEmail]
  );
  const userId = toNumber(userResult.rows[0]?.id);
  assert(userId > 0, "Failed to resolve admin user");

  const roleResult = await query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = 'TenantAdmin'
     LIMIT 1`,
    [tenantId]
  );
  const roleId = toNumber(roleResult.rows[0]?.id);
  assert(roleId > 0, "Failed to resolve TenantAdmin role");

  await query(
    `INSERT INTO user_role_scopes (
        tenant_id, user_id, role_id, scope_type, scope_id, effect
     )
     VALUES (?, ?, ?, 'TENANT', ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE effect = VALUES(effect)`,
    [tenantId, userId, roleId, tenantId]
  );

  return {
    tenantId,
    userId,
    adminEmail,
    password,
    stamp,
  };
}

async function bootstrapOrgAndGlBase(token, stamp) {
  const countryResult = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(countryResult.rows[0]?.id);
  const currencyCode = String(countryResult.rows[0]?.default_currency_code || "USD").toUpperCase();
  assert(countryId > 0, "US country row is required");

  const groupRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/group-companies",
    body: {
      code: `CG${stamp}`,
      name: `Cash Group ${stamp}`,
    },
    expectedStatus: 201,
  });
  const groupCompanyId = toNumber(groupRes.json?.id);
  assert(groupCompanyId > 0, "groupCompanyId not created");

  const calendarRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/fiscal-calendars",
    body: {
      code: `CAL${stamp}`,
      name: `Cash Calendar ${stamp}`,
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
    path: "/api/v1/org/fiscal-periods/generate",
    body: {
      calendarId,
      fiscalYear: TEST_FISCAL_YEAR,
    },
    expectedStatus: 201,
  });

  await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/fiscal-periods/generate",
    body: {
      calendarId,
      fiscalYear: TEST_FISCAL_YEAR + 1,
    },
    expectedStatus: 201,
  });

  const entityRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `LE${stamp}`,
      name: `Cash Legal Entity ${stamp}`,
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
    path: "/api/v1/gl/books",
    body: {
      legalEntityId,
      calendarId,
      code: `BOOK${stamp}`,
      name: `Cash Book ${stamp}`,
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
    path: "/api/v1/gl/coas",
    body: {
      scope: "LEGAL_ENTITY",
      legalEntityId,
      code: `COA${stamp}`,
      name: `Cash CoA ${stamp}`,
    },
    expectedStatus: 201,
  });
  const coaId = toNumber(coaRes.json?.id);
  assert(coaId > 0, "coaId not created");

  return {
    calendarId,
    bookId,
    currencyCode,
    legalEntityId,
    coaId,
  };
}

async function createAccount({
  token,
  coaId,
  code,
  name,
  accountType = "ASSET",
  normalSide = "DEBIT",
  allowPosting = true,
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/accounts",
    body: {
      coaId,
      code,
      name,
      accountType,
      normalSide,
      allowPosting,
    },
    expectedStatus: 201,
  });
  const accountId = toNumber(response.json?.id);
  assert(accountId > 0, `Account not created for code=${code}`);
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
    path: "/api/v1/cash/registers",
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
  assert(registerId > 0, `Register not created for code=${code}`);
  return registerId;
}

async function createCashTxn({
  token,
  tenantId,
  registerId,
  currencyCode,
  counterAccountId = null,
  counterCashRegisterId = null,
  txnType = "RECEIPT",
  amount = "50.00",
  idempotencyKey,
  bookDate,
  expectedStatus = 200,
}) {
  return apiRequest({
    token,
    method: "POST",
    path: "/api/v1/cash/transactions",
    body: {
      tenantId,
      registerId,
      txnType,
      amount,
      currencyCode,
      counterAccountId,
      counterCashRegisterId,
      description: "PR07 lifecycle test txn",
      idempotencyKey,
      bookDate,
    },
    expectedStatus,
  });
}

async function main() {
  const identity = await createTenantAndAdmin();

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const adminToken = await login(identity.adminEmail, identity.password);
    const base = await bootstrapOrgAndGlBase(adminToken, identity.stamp);

    const counterAccountId = await createAccount({
      token: adminToken,
      coaId: base.coaId,
      code: `CNT${identity.stamp}`,
      name: "Counter Account",
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    });

    const cashAccountAId = await createAccount({
      token: adminToken,
      coaId: base.coaId,
      code: `CSA${identity.stamp}`,
      name: "Cash Account A",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    const cashAccountBId = await createAccount({
      token: adminToken,
      coaId: base.coaId,
      code: `CSB${identity.stamp}`,
      name: "Cash Account B",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });

    const registerAId = await createRegister({
      token: adminToken,
      tenantId: identity.tenantId,
      legalEntityId: base.legalEntityId,
      accountId: cashAccountAId,
      code: `RA${identity.stamp}`,
      name: "Register A",
      currencyCode: base.currencyCode,
    });
    const registerBId = await createRegister({
      token: adminToken,
      tenantId: identity.tenantId,
      legalEntityId: base.legalEntityId,
      accountId: cashAccountBId,
      code: `RB${identity.stamp}`,
      name: "Register B",
      currencyCode: base.currencyCode,
    });

    // Targeted validation coverage: receipt/payout now require counterAccountId at create time.
    const missingReceiptCounterAccount = await createCashTxn({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: registerAId,
      currencyCode: base.currencyCode,
      counterAccountId: null,
      txnType: "RECEIPT",
      idempotencyKey: `PR07-MISS-RECEIPT-${identity.stamp}`,
      bookDate: "2026-03-10",
      expectedStatus: 400,
    });
    assert(
      toErrorText(missingReceiptCounterAccount.json).includes("RECEIPT requires counterAccountId"),
      "RECEIPT create should fail with required counterAccountId message"
    );

    const missingPayoutCounterAccount = await createCashTxn({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: registerAId,
      currencyCode: base.currencyCode,
      counterAccountId: null,
      txnType: "PAYOUT",
      idempotencyKey: `PR07-MISS-PAYOUT-${identity.stamp}`,
      bookDate: "2026-03-11",
      expectedStatus: 400,
    });
    assert(
      toErrorText(missingPayoutCounterAccount.json).includes("PAYOUT requires counterAccountId"),
      "PAYOUT create should fail with required counterAccountId message"
    );

    const transferWithoutCounterAccount = await createCashTxn({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: registerAId,
      currencyCode: base.currencyCode,
      txnType: "TRANSFER_OUT",
      counterCashRegisterId: registerBId,
      counterAccountId: null,
      idempotencyKey: `PR07-TRANSFER-NO-COUNTER-${identity.stamp}`,
      bookDate: "2026-03-12",
      expectedStatus: 200,
    });
    assert(
      toNumber(transferWithoutCounterAccount.json?.row?.id) > 0,
      "TRANSFER_OUT create should work without counterAccountId when counterCashRegisterId is present"
    );

    // Cross-tenant guard coverage for counterAccountId.
    await sleep(5);
    const foreignIdentity = await createTenantAndAdmin();
    const foreignAdminToken = await login(foreignIdentity.adminEmail, foreignIdentity.password);
    const foreignBase = await bootstrapOrgAndGlBase(foreignAdminToken, foreignIdentity.stamp);
    const foreignTenantCounterAccountId = await createAccount({
      token: foreignAdminToken,
      coaId: foreignBase.coaId,
      code: `FCNT${foreignIdentity.stamp}`,
      name: "Foreign Tenant Counter Account",
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    });

    const crossTenantCounterAccountAttempt = await createCashTxn({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: registerAId,
      currencyCode: base.currencyCode,
      counterAccountId: foreignTenantCounterAccountId,
      txnType: "RECEIPT",
      idempotencyKey: `PR07-CROSS-TENANT-ACCOUNT-${identity.stamp}`,
      bookDate: "2026-03-13",
      expectedStatus: 400,
    });
    assert(
      toErrorText(crossTenantCounterAccountAttempt.json).includes(
        "counterAccountId not found for tenant"
      ),
      "Cross-tenant counterAccountId must fail with tenant-scope validation error"
    );

    // Txn no sequence should be per legal entity + year across registers.
    const tx1 = await createCashTxn({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: registerAId,
      currencyCode: base.currencyCode,
      counterAccountId,
      idempotencyKey: `PR07-TXN-1-${identity.stamp}`,
      bookDate: "2026-03-15",
      expectedStatus: 200,
    });
    const tx2 = await createCashTxn({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: registerBId,
      currencyCode: base.currencyCode,
      counterAccountId,
      idempotencyKey: `PR07-TXN-2-${identity.stamp}`,
      bookDate: "2026-03-16",
      expectedStatus: 200,
    });

    const tx1Id = toNumber(tx1.json?.row?.id);
    const tx2Id = toNumber(tx2.json?.row?.id);
    assert(tx1Id > 0 && tx2Id > 0, "Failed to create base txns");

    const tx1No = parseTxnNo(tx1.json?.row?.txn_no);
    const tx2No = parseTxnNo(tx2.json?.row?.txn_no);
    assert(Boolean(tx1No) && Boolean(tx2No), "txn_no format must be CASH-<entity>-<year>-<seq>");
    assert(tx1No.year === 2026 && tx2No.year === 2026, "txn_no year should follow book_date");
    assert(
      tx2No.seq === tx1No.seq + 1,
      "txn_no sequence must increment across registers for same legal entity/year"
    );

    const tx3 = await createCashTxn({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: registerAId,
      currencyCode: base.currencyCode,
      counterAccountId,
      idempotencyKey: `PR07-TXN-3-${identity.stamp}`,
      bookDate: "2027-01-05",
      expectedStatus: 200,
    });
    const tx3No = parseTxnNo(tx3.json?.row?.txn_no);
    assert(Boolean(tx3No), "txn_no must parse for year reset check");
    assert(tx3No.year === 2027, "txn_no year should follow 2027 book_date");
    assert(tx3No.seq === 1, "txn_no sequence should reset at new year");

    // State machine: reverse on DRAFT must fail.
    await apiRequest({
      token: adminToken,
      method: "POST",
      path: `/api/v1/cash/transactions/${tx2Id}/reverse`,
      body: {
        tenantId: identity.tenantId,
        reverseReason: "draft cannot reverse",
      },
      expectedStatus: 400,
    });

    // Post tx1.
    await apiRequest({
      token: adminToken,
      method: "POST",
      path: `/api/v1/cash/transactions/${tx1Id}/post`,
      body: {
        tenantId: identity.tenantId,
      },
      expectedStatus: 200,
    });

    // Posted txn cannot be cancelled.
    await apiRequest({
      token: adminToken,
      method: "POST",
      path: `/api/v1/cash/transactions/${tx1Id}/cancel`,
      body: {
        tenantId: identity.tenantId,
        cancelReason: "cannot cancel posted",
      },
      expectedStatus: 400,
    });

    // Reversal should create linked opposite txn and mark original reversed.
    const reverseRes = await apiRequest({
      token: adminToken,
      method: "POST",
      path: `/api/v1/cash/transactions/${tx1Id}/reverse`,
      body: {
        tenantId: identity.tenantId,
        reverseReason: "reverse posted txn",
      },
      expectedStatus: 200,
    });

    const originalAfterReverse = reverseRes.json?.original || null;
    const reversalRow = reverseRes.json?.reversal || null;
    const reversalId = toNumber(reversalRow?.id);
    assert(reversalId > 0, "Reversal txn not created");
    assert(
      toNumber(reversalRow?.reversal_of_transaction_id) === tx1Id,
      "Reversal txn must link to original via reversal_of_transaction_id"
    );
    assert(
      String(originalAfterReverse?.status || "").toUpperCase() === "REVERSED",
      "Original txn must become REVERSED after reversal"
    );
    assert(
      String(reversalRow?.status || "").toUpperCase() === "POSTED",
      "Reversal txn should be created as POSTED"
    );

    // Reversal txn cannot itself be reversed.
    await apiRequest({
      token: adminToken,
      method: "POST",
      path: `/api/v1/cash/transactions/${reversalId}/reverse`,
      body: {
        tenantId: identity.tenantId,
        reverseReason: "reversal of reversal should be blocked",
      },
      expectedStatus: 400,
    });

    console.log("Cash PR07 lifecycle test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          legalEntityId: base.legalEntityId,
          registerAId,
          registerBId,
          tx1Id,
          tx2Id,
          reversalId,
          tx1No: tx1.json?.row?.txn_no,
          tx2No: tx2.json?.row?.txn_no,
          tx3No: tx3.json?.row?.txn_no,
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
  console.error("Cash PR07 lifecycle test failed.");
  console.error(err);
  process.exitCode = 1;
});
