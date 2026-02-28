import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CASH_PR06_TEST_PORT || 3111);
const BASE_URL = process.env.CASH_PR06_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
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
  const stamp = Date.now();
  const tenantCode = `CASH06_${stamp}`;
  const tenantName = `Cash PR06 ${stamp}`;
  const adminEmail = `cash_pr06_admin_${stamp}@example.com`;
  const password = "CashPR06#12345";
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
    [tenantId, adminEmail, passwordHash, "Cash PR06 Admin"]
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
    countryId,
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
  varianceGainAccountId = null,
  varianceLossAccountId = null,
  code,
  name,
  currencyCode,
  sessionMode,
  status = "ACTIVE",
  registerType = "DRAWER",
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
      registerType,
      sessionMode,
      currencyCode,
      status,
      varianceGainAccountId,
      varianceLossAccountId,
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
  counterAccountId,
  idempotencyKey,
  expectedStatus = 200,
}) {
  return apiRequest({
    token,
    method: "POST",
    path: "/api/v1/cash/transactions",
    body: {
      tenantId,
      registerId,
      txnType: "RECEIPT",
      amount: "25.50",
      currencyCode,
      counterAccountId,
      description: "Test cash txn",
      idempotencyKey,
    },
    expectedStatus,
  });
}

async function main() {
  const stamp = Date.now();
  const identity = await createTenantAndAdmin();

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();

    const adminToken = await login(identity.adminEmail, identity.password);
    const base = await bootstrapOrgAndGlBase(adminToken, stamp);

    const counterAccountId = await createAccount({
      token: adminToken,
      coaId: base.coaId,
      code: `CNT${stamp}`,
      name: "Counter Account",
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    });

    const requiredAccountId = await createAccount({
      token: adminToken,
      coaId: base.coaId,
      code: `RQA${stamp}`,
      name: "Required Register Cash",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    const varianceGainAccountId = await createAccount({
      token: adminToken,
      coaId: base.coaId,
      code: `RGN${stamp}`,
      name: "Register Variance Gain",
      accountType: "REVENUE",
      normalSide: "CREDIT",
    });
    const varianceLossAccountId = await createAccount({
      token: adminToken,
      coaId: base.coaId,
      code: `RLS${stamp}`,
      name: "Register Variance Loss",
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    });

    const requiredRegisterId = await createRegister({
      token: adminToken,
      tenantId: identity.tenantId,
      legalEntityId: base.legalEntityId,
      accountId: requiredAccountId,
      varianceGainAccountId,
      varianceLossAccountId,
      code: `REQ${stamp}`,
      name: "Required Register",
      currencyCode: base.currencyCode,
      sessionMode: "REQUIRED",
    });

    // session_mode=REQUIRED blocks txn creation without open session.
    await createCashTxn({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: requiredRegisterId,
      currencyCode: base.currencyCode,
      counterAccountId,
      idempotencyKey: `REQ-NO-SESSION-${stamp}`,
      expectedStatus: 400,
    });

    const openSession = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/cash/sessions/open",
      body: {
        tenantId: identity.tenantId,
        registerId: requiredRegisterId,
        openingAmount: "100.00",
      },
      expectedStatus: 200,
    });
    const requiredSessionId = toNumber(openSession.json?.row?.id);
    assert(requiredSessionId > 0, "Failed to open REQUIRED register session");

    // Double-open must fail.
    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/cash/sessions/open",
      body: {
        tenantId: identity.tenantId,
        registerId: requiredRegisterId,
        openingAmount: "20.00",
      },
      expectedStatus: 400,
    });

    // Wrong currency/register linkage must fail.
    await createCashTxn({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: requiredRegisterId,
      currencyCode: base.currencyCode === "USD" ? "EUR" : "USD",
      counterAccountId,
      idempotencyKey: `REQ-CURRENCY-MISMATCH-${stamp}`,
      expectedStatus: 400,
    });

    // Create DRAFT txn tied to open session.
    const draftTxnRes = await createCashTxn({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: requiredRegisterId,
      currencyCode: base.currencyCode,
      counterAccountId,
      idempotencyKey: `REQ-DRAFT-${stamp}`,
      expectedStatus: 200,
    });
    const draftTxnId = toNumber(draftTxnRes.json?.row?.id);
    assert(draftTxnId > 0, "Draft transaction not created");

    // Close without counted amount must fail.
    await apiRequest({
      token: adminToken,
      method: "POST",
      path: `/api/v1/cash/sessions/${requiredSessionId}/close`,
      body: {
        tenantId: identity.tenantId,
      },
      expectedStatus: 400,
    });

    // Close with unposted transactions must fail.
    await apiRequest({
      token: adminToken,
      method: "POST",
      path: `/api/v1/cash/sessions/${requiredSessionId}/close`,
      body: {
        tenantId: identity.tenantId,
        countedClosingAmount: "120.00",
        closedReason: "END_SHIFT",
      },
      expectedStatus: 400,
    });

    // Cancel draft and close should pass.
    await apiRequest({
      token: adminToken,
      method: "POST",
      path: `/api/v1/cash/transactions/${draftTxnId}/cancel`,
      body: {
        tenantId: identity.tenantId,
        cancelReason: "cancel for session close test",
      },
      expectedStatus: 200,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: `/api/v1/cash/sessions/${requiredSessionId}/close`,
      body: {
        tenantId: identity.tenantId,
        countedClosingAmount: "120.00",
        closedReason: "END_SHIFT",
      },
      expectedStatus: 200,
    });

    // Cannot open session on inactive register.
    const inactiveAccountId = await createAccount({
      token: adminToken,
      coaId: base.coaId,
      code: `INA${stamp}`,
      name: "Inactive Register Cash",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    const inactiveRegisterId = await createRegister({
      token: adminToken,
      tenantId: identity.tenantId,
      legalEntityId: base.legalEntityId,
      accountId: inactiveAccountId,
      code: `INR${stamp}`,
      name: "Inactive Register",
      currencyCode: base.currencyCode,
      sessionMode: "OPTIONAL",
      status: "INACTIVE",
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/cash/sessions/open",
      body: {
        tenantId: identity.tenantId,
        registerId: inactiveRegisterId,
        openingAmount: "10.00",
      },
      expectedStatus: 400,
    });

    // Invalid config (account no longer cash-controlled) must block session open.
    const invalidCfgAccountId = await createAccount({
      token: adminToken,
      coaId: base.coaId,
      code: `BCA${stamp}`,
      name: "Broken Config Register Cash",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    const invalidCfgRegisterId = await createRegister({
      token: adminToken,
      tenantId: identity.tenantId,
      legalEntityId: base.legalEntityId,
      accountId: invalidCfgAccountId,
      code: `BCR${stamp}`,
      name: "Broken Config Register",
      currencyCode: base.currencyCode,
      sessionMode: "OPTIONAL",
    });

    await query(`UPDATE accounts SET is_cash_controlled = FALSE WHERE id = ?`, [invalidCfgAccountId]);

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/cash/sessions/open",
      body: {
        tenantId: identity.tenantId,
        registerId: invalidCfgRegisterId,
        openingAmount: "10.00",
      },
      expectedStatus: 400,
    });

    // session_mode=NONE allows txn creation without session.
    const noneModeAccountId = await createAccount({
      token: adminToken,
      coaId: base.coaId,
      code: `NON${stamp}`,
      name: "None-Session Register Cash",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    const noneModeRegisterId = await createRegister({
      token: adminToken,
      tenantId: identity.tenantId,
      legalEntityId: base.legalEntityId,
      accountId: noneModeAccountId,
      code: `NONR${stamp}`,
      name: "NONE Mode Register",
      currencyCode: base.currencyCode,
      sessionMode: "NONE",
    });

    const noneModeTxnRes = await createCashTxn({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: noneModeRegisterId,
      currencyCode: base.currencyCode,
      counterAccountId,
      idempotencyKey: `NONE-TXN-${stamp}`,
      expectedStatus: 200,
    });
    assert(
      !toNumber(noneModeTxnRes.json?.row?.cash_session_id),
      "session_mode=NONE should allow transaction without cash_session_id"
    );

    console.log("Cash PR06 register/session validation test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          legalEntityId: base.legalEntityId,
          requiredRegisterId,
          requiredSessionId,
          inactiveRegisterId,
          invalidCfgRegisterId,
          noneModeRegisterId,
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
  console.error("Cash PR06 register/session test failed.");
  console.error(err);
  process.exitCode = 1;
});
