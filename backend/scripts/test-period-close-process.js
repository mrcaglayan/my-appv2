import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.PERIOD_CLOSE_TEST_PORT || 3109);
const BASE_URL =
  process.env.PERIOD_CLOSE_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;

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
  const cookie = setCookieHeader
    ? String(setCookieHeader).split(";")[0].trim()
    : null;

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
      // wait and retry
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

async function createTenantAndAdmin() {
  const stamp = Date.now();
  const tenantCode = `PCLOSE_${stamp}`;
  const tenantName = `Period Close ${stamp}`;
  const adminEmail = `period_close_admin_${stamp}@example.com`;
  const password = "PeriodClose#12345";
  const passwordHash = await bcrypt.hash(password, 10);

  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name)`,
    [tenantCode, tenantName]
  );

  await seedCore({ ensureDefaultTenantIfMissing: true });

  const tenantResult = await query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [tenantCode]
  );
  const tenantId = toNumber(tenantResult.rows[0]?.id);
  assert(tenantId > 0, "Failed to resolve tenantId");

  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, adminEmail, passwordHash, "Period Close Admin"]
  );

  const userResult = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, adminEmail]
  );
  const adminUserId = toNumber(userResult.rows[0]?.id);
  assert(adminUserId > 0, "Failed to resolve adminUserId");

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
     ON DUPLICATE KEY UPDATE
       effect = VALUES(effect)`,
    [tenantId, adminUserId, roleId, tenantId]
  );

  return { tenantId, adminUserId, adminEmail, password };
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

async function bootstrapCloseScenario(token) {
  const countryResult = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(countryResult.rows[0]?.id);
  const baseCurrencyCode = String(
    countryResult.rows[0]?.default_currency_code || "USD"
  ).toUpperCase();
  assert(countryId > 0, "US country row is required");

  const suffix = Date.now();
  const fiscalYear = 2026;

  const groupRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/group-companies",
    body: {
      code: `PCG_${suffix}`,
      name: `Period Close Group ${suffix}`,
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
      code: `PCCAL_${suffix}`,
      name: `Period Close Calendar ${suffix}`,
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
    body: { calendarId, fiscalYear },
    expectedStatus: 201,
  });
  await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/fiscal-periods/generate",
    body: { calendarId, fiscalYear: fiscalYear + 1 },
    expectedStatus: 201,
  });

  const periodsCurrentYearRes = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/org/fiscal-calendars/${calendarId}/periods?fiscalYear=${fiscalYear}`,
    expectedStatus: 200,
  });
  const periodsNextYearRes = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/org/fiscal-calendars/${calendarId}/periods?fiscalYear=${fiscalYear + 1}`,
    expectedStatus: 200,
  });

  const period12 = (periodsCurrentYearRes.json?.rows || []).find(
    (row) => toNumber(row.period_no) === 12 && !row.is_adjustment
  );
  const nextPeriod1 = (periodsNextYearRes.json?.rows || []).find(
    (row) => toNumber(row.period_no) === 1 && !row.is_adjustment
  );
  const closePeriodId = toNumber(period12?.id);
  const nextPeriodId = toNumber(nextPeriod1?.id);
  assert(closePeriodId > 0, "Current year period 12 not found");
  assert(nextPeriodId > 0, "Next year period 1 not found");

  const entityRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `PCLE_${suffix}`,
      name: `Period Close LE ${suffix}`,
      countryId,
      functionalCurrencyCode: baseCurrencyCode,
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
      code: `PCBOOK_${suffix}`,
      name: `Period Close Book ${suffix}`,
      bookType: "LOCAL",
      baseCurrencyCode,
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
      code: `PCCOA_${suffix}`,
      name: `Period Close CoA ${suffix}`,
    },
    expectedStatus: 201,
  });
  const coaId = toNumber(coaRes.json?.id);
  assert(coaId > 0, "coaId not created");

  async function createAccount(code, name, accountType, normalSide) {
    const accountRes = await apiRequest({
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
    const accountId = toNumber(accountRes.json?.id);
    assert(accountId > 0, `Account ${code} not created`);
    return accountId;
  }

  const cashAccountId = await createAccount("1000", "Cash", "ASSET", "DEBIT");
  const revenueAccountId = await createAccount("4000", "Revenue", "REVENUE", "CREDIT");
  const expenseAccountId = await createAccount("5000", "Expense", "EXPENSE", "DEBIT");
  const retainedAccountId = await createAccount(
    "3000",
    "Retained Earnings",
    "EQUITY",
    "CREDIT"
  );

  const entryDate = `${fiscalYear}-12-15`;
  const documentDate = entryDate;

  async function createAndPostJournal(lines) {
    const createRes = await apiRequest({
      token,
      method: "POST",
      path: "/api/v1/gl/journals",
      body: {
        legalEntityId,
        bookId,
        fiscalPeriodId: closePeriodId,
        entryDate,
        documentDate,
        currencyCode: baseCurrencyCode,
        sourceType: "MANUAL",
        description: "Period close test source journal",
        lines,
      },
      expectedStatus: 201,
    });
    const journalEntryId = toNumber(createRes.json?.journalEntryId);
    assert(journalEntryId > 0, "journalEntryId missing");

    await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/gl/journals/${journalEntryId}/post`,
      expectedStatus: 200,
    });
  }

  await createAndPostJournal([
    {
      accountId: cashAccountId,
      currencyCode: baseCurrencyCode,
      amountTxn: 100,
      debitBase: 100,
      creditBase: 0,
      description: "Revenue cash debit",
    },
    {
      accountId: revenueAccountId,
      currencyCode: baseCurrencyCode,
      amountTxn: -100,
      debitBase: 0,
      creditBase: 100,
      description: "Revenue credit",
    },
  ]);

  await createAndPostJournal([
    {
      accountId: expenseAccountId,
      currencyCode: baseCurrencyCode,
      amountTxn: 60,
      debitBase: 60,
      creditBase: 0,
      description: "Expense debit",
    },
    {
      accountId: cashAccountId,
      currencyCode: baseCurrencyCode,
      amountTxn: -60,
      debitBase: 0,
      creditBase: 60,
      description: "Cash credit",
    },
  ]);

  return {
    fiscalYear,
    bookId,
    closePeriodId,
    nextPeriodId,
    baseCurrencyCode,
    cashAccountId,
    retainedAccountId,
  };
}

async function verifyCloseAndReopenFlow(token, scenario) {
  const firstClose = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/gl/period-closing/${scenario.bookId}/${scenario.closePeriodId}/close-run`,
    body: {
      closeStatus: "SOFT_CLOSED",
      retainedEarningsAccountId: scenario.retainedAccountId,
      note: "period close integration test",
    },
    expectedStatus: 201,
  });

  const runId = toNumber(firstClose.json?.run?.id);
  assert(runId > 0, "close-run did not return run id");
  assert(firstClose.json?.idempotent === false, "first close should not be idempotent");

  const secondClose = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/gl/period-closing/${scenario.bookId}/${scenario.closePeriodId}/close-run`,
    body: {
      closeStatus: "SOFT_CLOSED",
      retainedEarningsAccountId: scenario.retainedAccountId,
      note: "period close integration test",
    },
    expectedStatus: 200,
  });
  assert(secondClose.json?.idempotent === true, "second close should be idempotent");
  assert(
    toNumber(secondClose.json?.run?.id) === runId,
    "idempotent close must return same run id"
  );

  const runRows = await query(
    `SELECT
       carry_forward_journal_entry_id,
       year_end_journal_entry_id,
       status
     FROM period_close_runs
     WHERE id = ?
     LIMIT 1`,
    [runId]
  );
  const runRow = runRows.rows[0];
  assert(runRow, "period_close_runs row missing");
  assert(String(runRow.status || "").toUpperCase() === "COMPLETED", "run must be COMPLETED");

  const carryJournalId = toNumber(runRow.carry_forward_journal_entry_id);
  const yearEndJournalId = toNumber(runRow.year_end_journal_entry_id);
  assert(carryJournalId > 0, "carry-forward journal id missing");
  assert(yearEndJournalId > 0, "year-end close journal id missing");

  const carryLinesResult = await query(
    `SELECT account_id, debit_base, credit_base
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no`,
    [carryJournalId]
  );
  const carryLines = carryLinesResult.rows || [];
  assert(carryLines.length >= 2, "carry-forward journal must include at least cash+retained");

  const cashCarry = carryLines.find(
    (line) => toNumber(line.account_id) === scenario.cashAccountId
  );
  const retainedCarry = carryLines.find(
    (line) => toNumber(line.account_id) === scenario.retainedAccountId
  );
  assert(cashCarry, "cash carry-forward line missing");
  assert(retainedCarry, "retained earnings carry-forward line missing");
  assert(
    Math.abs(toNumber(cashCarry.debit_base) - 40) < 0.0001,
    "cash carry-forward debit should be 40"
  );
  assert(
    Math.abs(toNumber(retainedCarry.credit_base) - 40) < 0.0001,
    "retained earnings carry-forward credit should be 40"
  );

  const reopen = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/gl/period-closing/${scenario.bookId}/${scenario.closePeriodId}/reopen`,
    body: {
      reason: "integration test reopen",
    },
    expectedStatus: 201,
  });
  const reversalIds = reopen.json?.reversalJournalEntryIds || [];
  assert(
    Array.isArray(reversalIds) && reversalIds.length >= 2,
    "reopen should produce reversal journals"
  );

  const reclose = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/gl/period-closing/${scenario.bookId}/${scenario.closePeriodId}/close-run`,
    body: {
      closeStatus: "HARD_CLOSED",
      retainedEarningsAccountId: scenario.retainedAccountId,
      note: "reclose after reopen",
    },
    expectedStatus: 201,
  });
  assert(reclose.json?.idempotent === false, "reclose after reopen must execute again");

  const finalStatusResult = await query(
    `SELECT status
     FROM period_statuses
     WHERE book_id = ?
       AND fiscal_period_id = ?
     LIMIT 1`,
    [scenario.bookId, scenario.closePeriodId]
  );
  const finalStatus = String(finalStatusResult.rows[0]?.status || "").toUpperCase();
  assert(finalStatus === "HARD_CLOSED", "final period status should be HARD_CLOSED");
}

async function main() {
  const identity = await createTenantAndAdmin();
  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const token = await login(identity.adminEmail, identity.password);

    const scenario = await bootstrapCloseScenario(token);
    await verifyCloseAndReopenFlow(token, scenario);

    console.log("");
    console.log("Period close process test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          bookId: scenario.bookId,
          closePeriodId: scenario.closePeriodId,
          nextPeriodId: scenario.nextPeriodId,
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

main().catch(async (err) => {
  console.error("Period close process test failed:", err);
  try {
    await closePool();
  } catch {
    // ignore close errors
  }
  process.exit(1);
});
