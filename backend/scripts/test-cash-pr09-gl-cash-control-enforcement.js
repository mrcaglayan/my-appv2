import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CASH_PR09_TEST_PORT || 3114);
const BASE_URL = process.env.CASH_PR09_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
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

function tryParseJson(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  if (typeof value === "object") {
    return value;
  }
  try {
    return JSON.parse(String(value));
  } catch {
    return null;
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
    env: {
      ...process.env,
      PORT: String(PORT),
      GL_CASH_CONTROL_MODE: "ENFORCE",
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
  const tenantCode = `CASH09_${stamp}`;
  const tenantName = `Cash PR09 ${stamp}`;
  const adminEmail = `cash_pr09_admin_${stamp}@example.com`;
  const password = "CashPR09#12345";
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
    [tenantId, adminEmail, passwordHash, "Cash PR09 Admin"]
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

async function createRegister({
  token,
  tenantId,
  legalEntityId,
  operatingUnitId,
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
      operatingUnitId,
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

async function createJournalDraft({
  token,
  legalEntityId,
  bookId,
  fiscalPeriodId,
  currencyCode,
  operatingUnitId,
  debitAccountId,
  creditAccountId,
  overrideCashControl = false,
  overrideReason = null,
  expectedStatus = 201,
}) {
  return apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/journals",
    body: {
      legalEntityId,
      bookId,
      fiscalPeriodId,
      sourceType: "MANUAL",
      entryDate: `${TEST_FISCAL_YEAR}-06-15`,
      documentDate: `${TEST_FISCAL_YEAR}-06-15`,
      currencyCode,
      description: "PR09 manual journal test",
      overrideCashControl,
      overrideReason,
      lines: [
        {
          accountId: debitAccountId,
          operatingUnitId,
          debitBase: 100,
          creditBase: 0,
        },
        {
          accountId: creditAccountId,
          operatingUnitId,
          debitBase: 0,
          creditBase: 100,
        },
      ],
    },
    expectedStatus,
  });
}

async function postJournal({
  token,
  journalId,
  overrideCashControl = false,
  overrideReason = null,
  expectedStatus = 200,
}) {
  return apiRequest({
    token,
    method: "POST",
    path: `/api/v1/gl/journals/${journalId}/post`,
    body: {
      overrideCashControl,
      overrideReason,
    },
    expectedStatus,
  });
}

async function findAuditLog({
  tenantId,
  action,
  resourceId,
}) {
  const result = await query(
    `SELECT id, action, resource_type, resource_id, payload_json
     FROM audit_logs
     WHERE tenant_id = ?
       AND action = ?
       AND resource_type = 'journal_entry'
       AND resource_id = ?
     ORDER BY id DESC
     LIMIT 1`,
    [tenantId, action, String(resourceId)]
  );
  return result.rows?.[0] || null;
}

async function bootstrapContext(token, identity) {
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
      code: `CG${identity.stamp}`,
      name: `Cash PR09 Group ${identity.stamp}`,
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
      code: `CAL${identity.stamp}`,
      name: `Cash PR09 Calendar ${identity.stamp}`,
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
  const fiscalPeriodId = toNumber(periodResult.rows[0]?.id);
  assert(fiscalPeriodId > 0, "fiscalPeriodId not found");

  const legalEntityRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `LE${identity.stamp}`,
      name: `Cash PR09 LE ${identity.stamp}`,
      countryId,
      functionalCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const legalEntityId = toNumber(legalEntityRes.json?.id);
  assert(legalEntityId > 0, "legalEntityId not created");

  const operatingUnitRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/operating-units",
    body: {
      legalEntityId,
      code: `OU${identity.stamp}`,
      name: "Cash PR09 OU",
      unitType: "BRANCH",
      hasSubledger: false,
    },
    expectedStatus: 201,
  });
  const operatingUnitId = toNumber(operatingUnitRes.json?.id);
  assert(operatingUnitId > 0, "operatingUnitId not created");

  const bookRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/books",
    body: {
      legalEntityId,
      calendarId,
      code: `BOOK${identity.stamp}`,
      name: `Cash PR09 Book ${identity.stamp}`,
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
      code: `COA${identity.stamp}`,
      name: `Cash PR09 CoA ${identity.stamp}`,
    },
    expectedStatus: 201,
  });
  const coaId = toNumber(coaRes.json?.id);
  assert(coaId > 0, "coaId not created");

  return {
    currencyCode,
    legalEntityId,
    operatingUnitId,
    bookId,
    fiscalPeriodId,
    coaId,
  };
}

async function main() {
  const identity = await createTenantAndAdmin();
  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const adminToken = await login(identity.adminEmail, identity.password);
    const context = await bootstrapContext(adminToken, identity);

    const counterAccountId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `CNT${identity.stamp}`,
      name: "Counter Expense",
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    });

    // Scenario A: CREATE_DRAFT enforcement
    const createBlockedCashAccountId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `CSA${identity.stamp}`,
      name: "Cash Controlled A",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    await createRegister({
      token: adminToken,
      tenantId: identity.tenantId,
      legalEntityId: context.legalEntityId,
      operatingUnitId: context.operatingUnitId,
      accountId: createBlockedCashAccountId,
      code: `RGA${identity.stamp}`,
      name: "Register A",
      currencyCode: context.currencyCode,
    });

    const createWithoutOverride = await createJournalDraft({
      token: adminToken,
      legalEntityId: context.legalEntityId,
      bookId: context.bookId,
      fiscalPeriodId: context.fiscalPeriodId,
      currencyCode: context.currencyCode,
      operatingUnitId: context.operatingUnitId,
      debitAccountId: counterAccountId,
      creditAccountId: createBlockedCashAccountId,
      expectedStatus: 400,
    });
    assert(
      toErrorText(createWithoutOverride.json).includes("cash-controlled"),
      "Create journal without override should be blocked for cash-controlled accounts"
    );

    const createWithOverrideMissingReason = await createJournalDraft({
      token: adminToken,
      legalEntityId: context.legalEntityId,
      bookId: context.bookId,
      fiscalPeriodId: context.fiscalPeriodId,
      currencyCode: context.currencyCode,
      operatingUnitId: context.operatingUnitId,
      debitAccountId: counterAccountId,
      creditAccountId: createBlockedCashAccountId,
      overrideCashControl: true,
      expectedStatus: 400,
    });
    assert(
      toErrorText(createWithOverrideMissingReason.json).includes("overrideReason"),
      "Create journal with overrideCashControl must require overrideReason"
    );

    const createWithOverride = await createJournalDraft({
      token: adminToken,
      legalEntityId: context.legalEntityId,
      bookId: context.bookId,
      fiscalPeriodId: context.fiscalPeriodId,
      currencyCode: context.currencyCode,
      operatingUnitId: context.operatingUnitId,
      debitAccountId: counterAccountId,
      creditAccountId: createBlockedCashAccountId,
      overrideCashControl: true,
      overrideReason: "Emergency correction required",
      expectedStatus: 201,
    });
    const createOverrideJournalId = toNumber(createWithOverride.json?.journalEntryId);
    assert(createOverrideJournalId > 0, "Override draft journal should be created");

    const createAudit = await findAuditLog({
      tenantId: identity.tenantId,
      action: "gl.cash_control.override",
      resourceId: createOverrideJournalId,
    });
    assert(Boolean(createAudit), "Override create should write audit log");
    const createAuditPayload = tryParseJson(createAudit?.payload_json);
    assert(
      String(createAuditPayload?.stage || "") === "CREATE_DRAFT",
      "Create override audit payload should include CREATE_DRAFT stage"
    );

    // Scenario B: POST_DRAFT enforcement
    const postBlockedCashAccountId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `CSB${identity.stamp}`,
      name: "Cash Controlled B",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });

    const preControlDraft = await createJournalDraft({
      token: adminToken,
      legalEntityId: context.legalEntityId,
      bookId: context.bookId,
      fiscalPeriodId: context.fiscalPeriodId,
      currencyCode: context.currencyCode,
      operatingUnitId: context.operatingUnitId,
      debitAccountId: counterAccountId,
      creditAccountId: postBlockedCashAccountId,
      expectedStatus: 201,
    });
    const preControlDraftJournalId = toNumber(preControlDraft.json?.journalEntryId);
    assert(preControlDraftJournalId > 0, "Pre-control draft journal should be created");

    await createRegister({
      token: adminToken,
      tenantId: identity.tenantId,
      legalEntityId: context.legalEntityId,
      operatingUnitId: context.operatingUnitId,
      accountId: postBlockedCashAccountId,
      code: `RGB${identity.stamp}`,
      name: "Register B",
      currencyCode: context.currencyCode,
    });

    const postWithoutOverride = await postJournal({
      token: adminToken,
      journalId: preControlDraftJournalId,
      expectedStatus: 400,
    });
    assert(
      toErrorText(postWithoutOverride.json).includes("cash-controlled"),
      "Post journal without override should be blocked for cash-controlled accounts"
    );

    const postWithOverride = await postJournal({
      token: adminToken,
      journalId: preControlDraftJournalId,
      overrideCashControl: true,
      overrideReason: "CFO emergency posting",
      expectedStatus: 200,
    });
    assert(postWithOverride.json?.posted === true, "Override post should succeed");

    const postAudit = await findAuditLog({
      tenantId: identity.tenantId,
      action: "gl.cash_control.override",
      resourceId: preControlDraftJournalId,
    });
    assert(Boolean(postAudit), "Override post should write audit log");
    const postAuditPayload = tryParseJson(postAudit?.payload_json);
    assert(
      String(postAuditPayload?.stage || "") === "POST_DRAFT",
      "Post override audit payload should include POST_DRAFT stage"
    );

    console.log("PR09 cash-control GL enforcement checks passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          createOverrideJournalId,
          postOverrideJournalId: preControlDraftJournalId,
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
