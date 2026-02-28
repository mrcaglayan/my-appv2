import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CASH_PR10_TEST_PORT || 3115);
const BASE_URL = process.env.CASH_PR10_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
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
  if (!jsonPayload) {
    return "";
  }
  if (typeof jsonPayload === "string") {
    return jsonPayload;
  }
  if (typeof jsonPayload.message === "string") {
    return jsonPayload.message;
  }
  if (typeof jsonPayload.error === "string") {
    return jsonPayload.error;
  }
  try {
    return JSON.stringify(jsonPayload);
  } catch {
    return String(jsonPayload);
  }
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

async function createTenantAndUsers() {
  const stamp = Date.now();
  const tenantCode = `CASH10_${stamp}`;
  const tenantName = `Cash PR10 ${stamp}`;
  const adminEmail = `cash_pr10_admin_${stamp}@example.com`;
  const accountantEmail = `cash_pr10_accountant_${stamp}@example.com`;
  const password = "CashPR10#12345";
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
    [tenantId, adminEmail, passwordHash, "Cash PR10 Admin"]
  );
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, accountantEmail, passwordHash, "Cash PR10 Accountant"]
  );

  const usersResult = await query(
    `SELECT id, email
     FROM users
     WHERE tenant_id = ?
       AND email IN (?, ?)`,
    [tenantId, adminEmail, accountantEmail]
  );

  let adminUserId = 0;
  let accountantUserId = 0;
  for (const row of usersResult.rows || []) {
    if (String(row.email || "").toLowerCase() === adminEmail.toLowerCase()) {
      adminUserId = toNumber(row.id);
    } else if (String(row.email || "").toLowerCase() === accountantEmail.toLowerCase()) {
      accountantUserId = toNumber(row.id);
    }
  }
  assert(adminUserId > 0 && accountantUserId > 0, "Failed to resolve users");

  const roleRows = await query(
    `SELECT id, code
     FROM roles
     WHERE tenant_id = ?
       AND code IN ('TenantAdmin', 'EntityAccountant')`,
    [tenantId]
  );
  let tenantAdminRoleId = 0;
  let entityAccountantRoleId = 0;
  for (const row of roleRows.rows || []) {
    if (row.code === "TenantAdmin") {
      tenantAdminRoleId = toNumber(row.id);
    } else if (row.code === "EntityAccountant") {
      entityAccountantRoleId = toNumber(row.id);
    }
  }
  assert(tenantAdminRoleId > 0, "TenantAdmin role not found");
  assert(entityAccountantRoleId > 0, "EntityAccountant role not found");

  await query(
    `INSERT INTO user_role_scopes (
        tenant_id, user_id, role_id, scope_type, scope_id, effect
     )
     VALUES (?, ?, ?, 'TENANT', ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE effect = VALUES(effect)`,
    [tenantId, adminUserId, tenantAdminRoleId, tenantId]
  );
  await query(
    `INSERT INTO user_role_scopes (
        tenant_id, user_id, role_id, scope_type, scope_id, effect
     )
     VALUES (?, ?, ?, 'TENANT', ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE effect = VALUES(effect)`,
    [tenantId, accountantUserId, entityAccountantRoleId, tenantId]
  );

  return {
    tenantId,
    stamp,
    admin: {
      userId: adminUserId,
      email: adminEmail,
      password,
    },
    accountant: {
      userId: accountantUserId,
      email: accountantEmail,
      password,
    },
  };
}

async function createAccount({
  token,
  coaId,
  code,
  name,
  accountType = "ASSET",
  normalSide = "DEBIT",
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
    },
    expectedStatus: 201,
  });
  const accountId = toNumber(response.json?.id);
  assert(accountId > 0, `Account not created for code=${code}`);
  return accountId;
}

async function bootstrapContext(adminToken, identity) {
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
    token: adminToken,
    method: "POST",
    path: "/api/v1/org/group-companies",
    body: {
      code: `CG${identity.stamp}`,
      name: `Cash PR10 Group ${identity.stamp}`,
    },
    expectedStatus: 201,
  });
  const groupCompanyId = toNumber(groupRes.json?.id);
  assert(groupCompanyId > 0, "groupCompanyId not created");

  const calendarRes = await apiRequest({
    token: adminToken,
    method: "POST",
    path: "/api/v1/org/fiscal-calendars",
    body: {
      code: `CAL${identity.stamp}`,
      name: `Cash PR10 Calendar ${identity.stamp}`,
      yearStartMonth: 1,
      yearStartDay: 1,
    },
    expectedStatus: 201,
  });
  const calendarId = toNumber(calendarRes.json?.id);
  assert(calendarId > 0, "calendarId not created");

  await apiRequest({
    token: adminToken,
    method: "POST",
    path: "/api/v1/org/fiscal-periods/generate",
    body: {
      calendarId,
      fiscalYear: TEST_FISCAL_YEAR,
    },
    expectedStatus: 201,
  });

  const legalEntityRes = await apiRequest({
    token: adminToken,
    method: "POST",
    path: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `LE${identity.stamp}`,
      name: `Cash PR10 Entity ${identity.stamp}`,
      countryId,
      functionalCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const legalEntityId = toNumber(legalEntityRes.json?.id);
  assert(legalEntityId > 0, "legalEntityId not created");

  const operatingUnitRes = await apiRequest({
    token: adminToken,
    method: "POST",
    path: "/api/v1/org/operating-units",
    body: {
      legalEntityId,
      code: `OU${identity.stamp}`,
      name: "Cash PR10 Branch",
      unitType: "BRANCH",
      hasSubledger: true,
    },
    expectedStatus: 201,
  });
  const operatingUnitId = toNumber(operatingUnitRes.json?.id);
  assert(operatingUnitId > 0, "operatingUnitId not created");

  const bookRes = await apiRequest({
    token: adminToken,
    method: "POST",
    path: "/api/v1/gl/books",
    body: {
      legalEntityId,
      calendarId,
      code: `BOOK${identity.stamp}`,
      name: `Cash PR10 Book ${identity.stamp}`,
      bookType: "LOCAL",
      baseCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const bookId = toNumber(bookRes.json?.id);
  assert(bookId > 0, "bookId not created");

  const periodResult = await query(
    `SELECT id
     FROM fiscal_periods
     WHERE calendar_id = ?
       AND fiscal_year = ?
       AND period_no = 6
       AND is_adjustment = FALSE
     LIMIT 1`,
    [calendarId, TEST_FISCAL_YEAR]
  );
  const fiscalPeriodId = toNumber(periodResult.rows?.[0]?.id);
  assert(fiscalPeriodId > 0, "fiscalPeriodId not found");

  const coaRes = await apiRequest({
    token: adminToken,
    method: "POST",
    path: "/api/v1/gl/coas",
    body: {
      scope: "LEGAL_ENTITY",
      legalEntityId,
      code: `COA${identity.stamp}`,
      name: `Cash PR10 CoA ${identity.stamp}`,
    },
    expectedStatus: 201,
  });
  const coaId = toNumber(coaRes.json?.id);
  assert(coaId > 0, "coaId not created");

  return {
    calendarId,
    legalEntityId,
    operatingUnitId,
    bookId,
    fiscalPeriodId,
    coaId,
    currencyCode,
  };
}

async function createAndPostReceipt({
  token,
  tenantId,
  registerId,
  currencyCode,
  counterAccountId,
  idempotencyKey,
  amount,
}) {
  const createRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/cash/transactions",
    body: {
      tenantId,
      registerId,
      txnType: "RECEIPT",
      amount,
      currencyCode,
      counterAccountId,
      description: "PR10 test receipt",
      idempotencyKey,
      bookDate: `${TEST_FISCAL_YEAR}-06-15`,
    },
    expectedStatus: 200,
  });
  const txnId = toNumber(createRes.json?.row?.id);
  assert(txnId > 0, "Failed to create receipt transaction");

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/cash/transactions/${txnId}/post`,
    body: {
      tenantId,
    },
    expectedStatus: 200,
  });
}

async function loadSessionVarianceTransaction({
  tenantId,
  sessionId,
}) {
  const result = await query(
    `SELECT id, txn_type, status, amount, counter_account_id, posted_journal_entry_id
     FROM cash_transactions
     WHERE tenant_id = ?
       AND cash_session_id = ?
       AND txn_type = 'VARIANCE'
     ORDER BY id DESC
     LIMIT 1`,
    [tenantId, sessionId]
  );
  return result.rows?.[0] || null;
}

async function loadJournalLineTotals({
  journalEntryId,
  registerAccountId,
  varianceCounterAccountId,
}) {
  const result = await query(
    `SELECT
       SUM(CASE WHEN account_id = ? THEN debit_base ELSE 0 END) AS register_debit,
       SUM(CASE WHEN account_id = ? THEN credit_base ELSE 0 END) AS register_credit,
       SUM(CASE WHEN account_id = ? THEN debit_base ELSE 0 END) AS counter_debit,
       SUM(CASE WHEN account_id = ? THEN credit_base ELSE 0 END) AS counter_credit
     FROM journal_lines
     WHERE journal_entry_id = ?`,
    [
      registerAccountId,
      registerAccountId,
      varianceCounterAccountId,
      varianceCounterAccountId,
      journalEntryId,
    ]
  );
  return result.rows?.[0] || {};
}

async function main() {
  const identity = await createTenantAndUsers();
  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();

    const adminToken = await login(identity.admin.email, identity.admin.password);
    const accountantToken = await login(identity.accountant.email, identity.accountant.password);
    const context = await bootstrapContext(adminToken, identity);

    const registerAccountId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `CASH${identity.stamp}`,
      name: "Cash Register Account",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    const salesCounterAccountId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `SALE${identity.stamp}`,
      name: "Sales Counter Account",
      accountType: "REVENUE",
      normalSide: "CREDIT",
    });
    const varianceGainAccountId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `VARG${identity.stamp}`,
      name: "Cash Over Gain",
      accountType: "REVENUE",
      normalSide: "CREDIT",
    });
    const varianceLossAccountId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `VARL${identity.stamp}`,
      name: "Cash Short Loss",
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    });

    const registerRes = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/cash/registers",
      body: {
        tenantId: identity.tenantId,
        legalEntityId: context.legalEntityId,
        operatingUnitId: context.operatingUnitId,
        accountId: registerAccountId,
        code: `REG${identity.stamp}`,
        name: "PR10 Register",
        registerType: "DRAWER",
        sessionMode: "OPTIONAL",
        currencyCode: context.currencyCode,
        status: "ACTIVE",
        varianceGainAccountId,
        varianceLossAccountId,
        requiresApprovalOverAmount: "5.000000",
      },
      expectedStatus: 200,
    });
    const registerId = toNumber(registerRes.json?.row?.id);
    assert(registerId > 0, "Failed to create cash register");

    // Case 1: SHORT below threshold -> auto-post variance loss.
    const shortSessionOpen = await apiRequest({
      token: accountantToken,
      method: "POST",
      path: "/api/v1/cash/sessions/open",
      body: {
        tenantId: identity.tenantId,
        registerId,
        openingAmount: "100.00",
      },
      expectedStatus: 200,
    });
    const shortSessionId = toNumber(shortSessionOpen.json?.row?.id);
    assert(shortSessionId > 0, "Failed to open short test session");

    await createAndPostReceipt({
      token: accountantToken,
      tenantId: identity.tenantId,
      registerId,
      currencyCode: context.currencyCode,
      counterAccountId: salesCounterAccountId,
      idempotencyKey: `PR10-SHORT-${identity.stamp}`,
      amount: "20.00",
    });

    const shortClose = await apiRequest({
      token: accountantToken,
      method: "POST",
      path: `/api/v1/cash/sessions/${shortSessionId}/close`,
      body: {
        tenantId: identity.tenantId,
        countedClosingAmount: "118.00",
        closedReason: "END_SHIFT",
        closeNote: "Short test close",
      },
      expectedStatus: 200,
    });
    assert(
      String(shortClose.json?.row?.status || "").toUpperCase() === "CLOSED",
      "Short session should close successfully"
    );
    const shortVarianceTxn = await loadSessionVarianceTransaction({
      tenantId: identity.tenantId,
      sessionId: shortSessionId,
    });
    assert(Boolean(shortVarianceTxn), "Short session should create variance transaction");
    assert(
      String(shortVarianceTxn.status || "").toUpperCase() === "POSTED",
      "Short variance transaction should be POSTED"
    );
    assert(
      toNumber(shortVarianceTxn.counter_account_id) === varianceLossAccountId,
      "Short variance must post against variance loss account"
    );
    const shortLines = await loadJournalLineTotals({
      journalEntryId: toNumber(shortVarianceTxn.posted_journal_entry_id),
      registerAccountId,
      varianceCounterAccountId: varianceLossAccountId,
    });
    assert(
      Number(shortLines.counter_debit || 0) > 0 && Number(shortLines.register_credit || 0) > 0,
      "Short variance posting must be Dr loss / Cr register cash"
    );

    // Case 2: OVER below threshold -> auto-post variance gain.
    const overSessionOpen = await apiRequest({
      token: accountantToken,
      method: "POST",
      path: "/api/v1/cash/sessions/open",
      body: {
        tenantId: identity.tenantId,
        registerId,
        openingAmount: "100.00",
      },
      expectedStatus: 200,
    });
    const overSessionId = toNumber(overSessionOpen.json?.row?.id);
    assert(overSessionId > 0, "Failed to open over test session");

    await createAndPostReceipt({
      token: accountantToken,
      tenantId: identity.tenantId,
      registerId,
      currencyCode: context.currencyCode,
      counterAccountId: salesCounterAccountId,
      idempotencyKey: `PR10-OVER-${identity.stamp}`,
      amount: "10.00",
    });

    const overClose = await apiRequest({
      token: accountantToken,
      method: "POST",
      path: `/api/v1/cash/sessions/${overSessionId}/close`,
      body: {
        tenantId: identity.tenantId,
        countedClosingAmount: "113.00",
        closedReason: "END_SHIFT",
        closeNote: "Over test close",
      },
      expectedStatus: 200,
    });
    assert(
      String(overClose.json?.row?.status || "").toUpperCase() === "CLOSED",
      "Over session should close successfully"
    );
    const overVarianceTxn = await loadSessionVarianceTransaction({
      tenantId: identity.tenantId,
      sessionId: overSessionId,
    });
    assert(Boolean(overVarianceTxn), "Over session should create variance transaction");
    assert(
      String(overVarianceTxn.status || "").toUpperCase() === "POSTED",
      "Over variance transaction should be POSTED"
    );
    assert(
      toNumber(overVarianceTxn.counter_account_id) === varianceGainAccountId,
      "Over variance must post against variance gain account"
    );
    const overLines = await loadJournalLineTotals({
      journalEntryId: toNumber(overVarianceTxn.posted_journal_entry_id),
      registerAccountId,
      varianceCounterAccountId: varianceGainAccountId,
    });
    assert(
      Number(overLines.register_debit || 0) > 0 && Number(overLines.counter_credit || 0) > 0,
      "Over variance posting must be Dr register cash / Cr gain"
    );

    // Case 3: Above threshold -> blocked without approval, then privileged forced-close approval.
    const highSessionOpen = await apiRequest({
      token: accountantToken,
      method: "POST",
      path: "/api/v1/cash/sessions/open",
      body: {
        tenantId: identity.tenantId,
        registerId,
        openingAmount: "100.00",
      },
      expectedStatus: 200,
    });
    const highSessionId = toNumber(highSessionOpen.json?.row?.id);
    assert(highSessionId > 0, "Failed to open high variance session");

    const highCloseBlocked = await apiRequest({
      token: accountantToken,
      method: "POST",
      path: `/api/v1/cash/sessions/${highSessionId}/close`,
      body: {
        tenantId: identity.tenantId,
        countedClosingAmount: "90.00",
        closedReason: "END_SHIFT",
        closeNote: "High variance no approval",
      },
      expectedStatus: 400,
    });
    assert(
      toErrorText(highCloseBlocked.json).includes("approval is required"),
      "Above-threshold variance must be blocked without approval flag"
    );

    const highCloseForbidden = await apiRequest({
      token: accountantToken,
      method: "POST",
      path: `/api/v1/cash/sessions/${highSessionId}/close`,
      body: {
        tenantId: identity.tenantId,
        countedClosingAmount: "90.00",
        closedReason: "END_SHIFT",
        closeNote: "Try with approval flag but no permission",
        approveVariance: true,
      },
      expectedStatus: 403,
    });
    assert(
      toErrorText(highCloseForbidden.json).includes("cash.variance.approve"),
      "Above-threshold approval flag should require cash.variance.approve permission"
    );

    const forcedCloseApproved = await apiRequest({
      token: adminToken,
      method: "POST",
      path: `/api/v1/cash/sessions/${highSessionId}/close`,
      body: {
        tenantId: identity.tenantId,
        countedClosingAmount: "90.00",
        closedReason: "FORCED_CLOSE",
        closeNote: "Supervisor approved forced close for variance",
        approveVariance: true,
      },
      expectedStatus: 200,
    });
    assert(
      String(forcedCloseApproved.json?.row?.closed_reason || "").toUpperCase() === "FORCED_CLOSE",
      "Forced close reason should be captured"
    );
    assert(
      String(forcedCloseApproved.json?.row?.close_note || "").includes("Supervisor approved"),
      "Forced close note should be captured"
    );
    assert(
      toNumber(forcedCloseApproved.json?.row?.approved_by_user_id) === identity.admin.userId,
      "Above-threshold approved close should capture approved_by_user_id"
    );

    const highVarianceTxn = await loadSessionVarianceTransaction({
      tenantId: identity.tenantId,
      sessionId: highSessionId,
    });
    assert(Boolean(highVarianceTxn), "High variance session should create variance transaction");
    assert(
      String(highVarianceTxn.status || "").toUpperCase() === "POSTED",
      "High variance transaction should be auto-posted after approval"
    );

    console.log("PR10 variance policy checks passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          registerId,
          shortSessionId,
          overSessionId,
          highSessionId,
          shortVarianceTxnId: toNumber(shortVarianceTxn.id),
          overVarianceTxnId: toNumber(overVarianceTxn.id),
          highVarianceTxnId: toNumber(highVarianceTxn.id),
        },
        null,
        2
      )
    );
  } finally {
    if (!serverStopped) {
      server.kill("SIGINT");
      serverStopped = true;
    }
    await sleep(400);
    await closePool();
  }
}

main()
  .then(() => {
    process.exitCode = 0;
  })
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
