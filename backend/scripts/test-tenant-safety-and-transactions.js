import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.TENANT_SAFETY_TEST_PORT || 3108);
const BASE_URL =
  process.env.TENANT_SAFETY_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
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
  assert(Boolean(sessionCookie), `Login cookie missing for ${email}`);
  return sessionCookie;
}

async function createTenant(code, name) {
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name)`,
    [code, name]
  );

  const tenantResult = await query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [code]
  );
  const tenantId = toNumber(tenantResult.rows[0]?.id);
  assert(tenantId > 0, `Failed to resolve tenant for code=${code}`);
  return tenantId;
}

async function createUser({
  tenantId,
  email,
  name,
  passwordHash,
}) {
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, email, passwordHash, name]
  );

  const result = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, email]
  );
  const userId = toNumber(result.rows[0]?.id);
  assert(userId > 0, `Failed to resolve user id for ${email}`);
  return userId;
}

async function assignTenantAdminRole(tenantId, userId) {
  const roleResult = await query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = 'TenantAdmin'
     LIMIT 1`,
    [tenantId]
  );
  const roleId = toNumber(roleResult.rows[0]?.id);
  assert(roleId > 0, `TenantAdmin role not found for tenant ${tenantId}`);

  await query(
    `INSERT INTO user_role_scopes (
        tenant_id, user_id, role_id, scope_type, scope_id, effect
     )
     VALUES (?, ?, ?, 'TENANT', ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE
       effect = VALUES(effect)`,
    [tenantId, userId, roleId, tenantId]
  );

  return roleId;
}

async function getCountry(iso2) {
  const result = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = ?
     LIMIT 1`,
    [String(iso2).trim().toUpperCase()]
  );
  const row = result.rows[0];
  assert(row, `Country not found for iso2=${iso2}`);
  return {
    id: toNumber(row.id),
    currencyCode: String(row.default_currency_code || "USD").toUpperCase(),
  };
}

async function createTestIdentitySetup() {
  const now = Date.now();
  const password = "TenantSafe#12345";
  const passwordHash = await bcrypt.hash(password, 10);

  const tenantAId = await createTenant(
    `TENANT_A_${now}`,
    `Tenant Safety A ${now}`
  );
  const tenantBId = await createTenant(
    `TENANT_B_${now}`,
    `Tenant Safety B ${now}`
  );

  await seedCore({
    ensureDefaultTenantIfMissing: true,
  });

  const tenantAAdminEmail = `tenant_a_admin_${now}@example.com`;
  const tenantBAdminEmail = `tenant_b_admin_${now}@example.com`;
  const tenantAWorkerEmail = `tenant_a_worker_${now}@example.com`;

  const tenantAAdminUserId = await createUser({
    tenantId: tenantAId,
    email: tenantAAdminEmail,
    name: "Tenant A Admin",
    passwordHash,
  });
  const tenantBAdminUserId = await createUser({
    tenantId: tenantBId,
    email: tenantBAdminEmail,
    name: "Tenant B Admin",
    passwordHash,
  });
  const tenantAWorkerUserId = await createUser({
    tenantId: tenantAId,
    email: tenantAWorkerEmail,
    name: "Tenant A Worker",
    passwordHash,
  });

  await assignTenantAdminRole(tenantAId, tenantAAdminUserId);
  await assignTenantAdminRole(tenantBId, tenantBAdminUserId);

  return {
    password,
    tenantA: {
      tenantId: tenantAId,
      adminUserId: tenantAAdminUserId,
      workerUserId: tenantAWorkerUserId,
      adminEmail: tenantAAdminEmail,
    },
    tenantB: {
      tenantId: tenantBId,
      adminUserId: tenantBAdminUserId,
      adminEmail: tenantBAdminEmail,
    },
  };
}

async function bootstrapTenantData({ token, suffix, country, fiscalYear }) {
  const group = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/group-companies",
    body: {
      code: `G_${suffix}`,
      name: `Group ${suffix}`,
    },
    expectedStatus: 201,
  });
  const groupCompanyId = toNumber(group.json?.id);
  assert(groupCompanyId > 0, `Failed to create groupCompanyId for ${suffix}`);

  const calendar = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/fiscal-calendars",
    body: {
      code: `CAL_${suffix}`,
      name: `Calendar ${suffix}`,
      yearStartMonth: 1,
      yearStartDay: 1,
    },
    expectedStatus: 201,
  });
  const calendarId = toNumber(calendar.json?.id);
  assert(calendarId > 0, `Failed to create calendarId for ${suffix}`);

  await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/fiscal-periods/generate",
    body: {
      calendarId,
      fiscalYear,
    },
    expectedStatus: 201,
  });

  const periods = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/org/fiscal-calendars/${calendarId}/periods?fiscalYear=${fiscalYear}`,
    expectedStatus: 200,
  });
  const periodRow = (periods.json?.rows || []).find(
    (row) => toNumber(row.period_no) === 1 && !row.is_adjustment
  );
  const fiscalPeriodId = toNumber(periodRow?.id);
  assert(fiscalPeriodId > 0, `Failed to resolve fiscalPeriodId for ${suffix}`);

  const legalEntity = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `LE_${suffix}`,
      name: `Legal Entity ${suffix}`,
      countryId: country.id,
      functionalCurrencyCode: country.currencyCode,
    },
    expectedStatus: 201,
  });
  const legalEntityId = toNumber(legalEntity.json?.id);
  assert(legalEntityId > 0, `Failed to create legalEntityId for ${suffix}`);

  const book = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/books",
    body: {
      legalEntityId,
      calendarId,
      code: `BOOK_${suffix}`,
      name: `Book ${suffix}`,
      bookType: "LOCAL",
      baseCurrencyCode: country.currencyCode,
    },
    expectedStatus: 201,
  });
  const bookId = toNumber(book.json?.id);
  assert(bookId > 0, `Failed to create bookId for ${suffix}`);

  const coa = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/coas",
    body: {
      scope: "LEGAL_ENTITY",
      legalEntityId,
      code: `COA_${suffix}`,
      name: `CoA ${suffix}`,
    },
    expectedStatus: 201,
  });
  const coaId = toNumber(coa.json?.id);
  assert(coaId > 0, `Failed to create coaId for ${suffix}`);

  const account = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/accounts",
    body: {
      coaId,
      code: `1000_${suffix}`,
      name: `Cash ${suffix}`,
      accountType: "ASSET",
      normalSide: "DEBIT",
    },
    expectedStatus: 201,
  });
  const accountId = toNumber(account.json?.id);
  assert(accountId > 0, `Failed to create accountId for ${suffix}`);

  const consolidationGroup = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/consolidation/groups",
    body: {
      groupCompanyId,
      calendarId,
      code: `CONS_${suffix}`,
      name: `Consolidation Group ${suffix}`,
      presentationCurrencyCode: country.currencyCode,
    },
    expectedStatus: 201,
  });
  const consolidationGroupId = toNumber(consolidationGroup.json?.id);
  assert(
    consolidationGroupId > 0,
    `Failed to create consolidationGroupId for ${suffix}`
  );

  const run = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/consolidation/runs",
    body: {
      consolidationGroupId,
      fiscalPeriodId,
      runName: `RUN_${suffix}`,
      presentationCurrencyCode: country.currencyCode,
    },
    expectedStatus: 201,
  });
  const consolidationRunId = toNumber(run.json?.runId);
  assert(consolidationRunId > 0, `Failed to create consolidationRunId for ${suffix}`);

  return {
    groupCompanyId,
    calendarId,
    fiscalPeriodId,
    legalEntityId,
    bookId,
    coaId,
    accountId,
    consolidationGroupId,
    consolidationRunId,
    currencyCode: country.currencyCode,
  };
}

async function runCrossTenantInjectionAssertions({
  tenantAToken,
  tenantAData,
  tenantBData,
  fiscalYear,
}) {
  const today = `${fiscalYear}-01-15`;

  await apiRequest({
    token: tenantAToken,
    method: "POST",
    path: "/api/v1/intercompany/pairs",
    body: {
      fromLegalEntityId: tenantAData.legalEntityId,
      toLegalEntityId: tenantBData.legalEntityId,
      status: "ACTIVE",
    },
    expectedStatus: 400,
  });

  await apiRequest({
    token: tenantAToken,
    method: "POST",
    path: "/api/v1/gl/account-mappings",
    body: {
      sourceAccountId: tenantAData.accountId,
      targetAccountId: tenantBData.accountId,
      mappingType: "LOCAL_TO_GROUP",
    },
    expectedStatus: 400,
  });

  await apiRequest({
    token: tenantAToken,
    method: "POST",
    path: "/api/v1/gl/journals",
    body: {
      legalEntityId: tenantAData.legalEntityId,
      bookId: tenantBData.bookId,
      fiscalPeriodId: tenantAData.fiscalPeriodId,
      entryDate: today,
      documentDate: today,
      currencyCode: tenantAData.currencyCode,
      lines: [
        {
          accountId: tenantAData.accountId,
          debitBase: 100,
          creditBase: 0,
          currencyCode: tenantAData.currencyCode,
        },
        {
          accountId: tenantAData.accountId,
          debitBase: 0,
          creditBase: 100,
          currencyCode: tenantAData.currencyCode,
        },
      ],
    },
    expectedStatus: 400,
  });
}

async function runRollbackAssertions({
  tenantAToken,
  tenantAIdentity,
  tenantAData,
}) {
  await apiRequest({
    token: tenantAToken,
    method: "POST",
    path: "/api/v1/security/data-scopes",
    body: {
      userId: tenantAIdentity.workerUserId,
      scopeType: "GROUP",
      scopeId: tenantAData.groupCompanyId,
      effect: "ALLOW",
    },
    expectedStatus: 201,
  });

  const beforeReplace = await apiRequest({
    token: tenantAToken,
    method: "GET",
    path: `/api/v1/security/data-scopes?userId=${tenantAIdentity.workerUserId}`,
    expectedStatus: 200,
  });
  assert(
    (beforeReplace.json?.rows || []).length === 1,
    "Expected one baseline data scope row before rollback test"
  );

  await apiRequest({
    token: tenantAToken,
    method: "PUT",
    path: `/api/v1/security/data-scopes/users/${tenantAIdentity.workerUserId}/replace`,
    body: {
      scopes: [
        {
          scopeType: "GROUP",
          scopeId: tenantAData.groupCompanyId,
          effect: "ALLOW",
        },
        {
          scopeType: "GROUP",
          scopeId: tenantAData.groupCompanyId,
          effect: "ALLOW",
        },
      ],
    },
    expectedStatus: 400,
  });

  const afterReplace = await apiRequest({
    token: tenantAToken,
    method: "GET",
    path: `/api/v1/security/data-scopes?userId=${tenantAIdentity.workerUserId}`,
    expectedStatus: 200,
  });
  const afterRows = afterReplace.json?.rows || [];
  assert(afterRows.length === 1, "Data scope replace failure must rollback deletions");
  assert(
    toNumber(afterRows[0]?.scope_id) === tenantAData.groupCompanyId,
    "Data scope replace rollback did not preserve original scope_id"
  );

  const eliminationDescription = `ROLLBACK_ELIM_${Date.now()}`;

  await apiRequest({
    token: tenantAToken,
    method: "POST",
    path: `/api/v1/consolidation/runs/${tenantAData.consolidationRunId}/eliminations`,
    body: {
      description: eliminationDescription,
      lines: [
        {
          accountId: tenantAData.accountId,
          debitAmount: 250,
          creditAmount: 0,
          currencyCode: tenantAData.currencyCode,
          description: "valid line",
        },
        {
          accountId: tenantAData.accountId,
          debitAmount: 0,
          creditAmount: 250,
          currencyCode: "ZZZ",
          description: "invalid currency to force FK failure",
        },
      ],
    },
    expectedStatus: 500,
  });

  const eliminationEntryCountResult = await query(
    `SELECT COUNT(*) AS count
     FROM elimination_entries
     WHERE consolidation_run_id = ?
       AND description = ?`,
    [tenantAData.consolidationRunId, eliminationDescription]
  );
  const eliminationEntryCount = toNumber(eliminationEntryCountResult.rows[0]?.count);
  assert(
    eliminationEntryCount === 0,
    "Elimination entry/line transaction should rollback on line insert failure"
  );
}

async function main() {
  const fiscalYear = new Date().getUTCFullYear();
  const identities = await createTestIdentitySetup();

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();

    const tenantAToken = await login(
      identities.tenantA.adminEmail,
      identities.password
    );
    const tenantBToken = await login(
      identities.tenantB.adminEmail,
      identities.password
    );

    const usCountry = await getCountry("US");
    const trCountry = await getCountry("TR");

    const tenantAData = await bootstrapTenantData({
      token: tenantAToken,
      suffix: `A_${Date.now()}`,
      country: usCountry,
      fiscalYear,
    });
    const tenantBData = await bootstrapTenantData({
      token: tenantBToken,
      suffix: `B_${Date.now()}`,
      country: trCountry,
      fiscalYear,
    });

    await runCrossTenantInjectionAssertions({
      tenantAToken,
      tenantAData,
      tenantBData,
      fiscalYear,
    });

    await runRollbackAssertions({
      tenantAToken,
      tenantAIdentity: identities.tenantA,
      tenantAData,
    });

    console.log("");
    console.log("Tenant safety and transaction rollback test passed.");
    console.log(
      JSON.stringify(
        {
          tenantAId: identities.tenantA.tenantId,
          tenantBId: identities.tenantB.tenantId,
          tenantAData,
          tenantBData,
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
  console.error("Tenant safety and transaction rollback test failed:", err);
  try {
    await closePool();
  } catch {
    // ignore close errors
  }
  process.exit(1);
});
