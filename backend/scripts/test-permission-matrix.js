import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.PERMISSION_MATRIX_PORT || 3107);
const BASE_URL = process.env.PERMISSION_MATRIX_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
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

async function createTenantAndUsers() {
  const now = Date.now();
  const tenantCode = `PMAT_${now}`;
  const tenantName = `Permission Matrix ${now}`;
  const adminEmail = `pm_admin_${now}@example.com`;
  const scopedEmail = `pm_scoped_${now}@example.com`;
  const password = "Matrix#12345";

  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name)`,
    [tenantCode, tenantName]
  );

  // Ensure the new tenant also receives baseline roles/permissions.
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
  const tenantId = Number(tenantResult.rows[0]?.id || 0);
  assert(tenantId > 0, "Failed to resolve tenantId");

  const passwordHash = await bcrypt.hash(password, 10);

  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, adminEmail, passwordHash, "PMAT Admin"]
  );
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, scopedEmail, passwordHash, "PMAT Scoped User"]
  );

  const usersResult = await query(
    `SELECT id, email
     FROM users
     WHERE tenant_id = ?
       AND email IN (?, ?)`,
    [tenantId, adminEmail, scopedEmail]
  );
  const userIdByEmail = new Map(usersResult.rows.map((row) => [row.email, Number(row.id)]));
  const adminUserId = userIdByEmail.get(adminEmail);
  const scopedUserId = userIdByEmail.get(scopedEmail);
  assert(adminUserId, "Failed to resolve admin user");
  assert(scopedUserId, "Failed to resolve scoped user");

  const roleResult = await query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = 'TenantAdmin'
     LIMIT 1`,
    [tenantId]
  );
  const roleId = Number(roleResult.rows[0]?.id || 0);
  assert(roleId > 0, "Failed to resolve TenantAdmin role");

  await query(
    `INSERT INTO user_role_scopes (
        tenant_id, user_id, role_id, scope_type, scope_id, effect
     )
     VALUES
       (?, ?, ?, 'TENANT', ?, 'ALLOW'),
       (?, ?, ?, 'TENANT', ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE
       effect = VALUES(effect)`,
    [tenantId, adminUserId, roleId, tenantId, tenantId, scopedUserId, roleId, tenantId]
  );

  return {
    tenantId,
    adminEmail,
    scopedEmail,
    password,
    adminUserId,
    scopedUserId,
  };
}

async function getCountryIds() {
  const result = await query(
    `SELECT id, iso2
     FROM countries
     WHERE iso2 IN ('US', 'TR')`
  );
  const map = new Map(result.rows.map((row) => [String(row.iso2).toUpperCase(), Number(row.id)]));
  const us = map.get("US");
  const tr = map.get("TR");
  assert(us && tr, "Expected US and TR countries to exist");
  return { us, tr };
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

async function main() {
  const setup = await createTenantAndUsers();
  const countries = await getCountryIds();

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();

    const adminToken = await login(setup.adminEmail, setup.password);
    const scopedToken = await login(setup.scopedEmail, setup.password);

    const groupOne = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/org/group-companies",
      body: { code: "PMAT_G1", name: "PMAT Group 1" },
      expectedStatus: 201,
    });
    const groupTwo = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/org/group-companies",
      body: { code: "PMAT_G2", name: "PMAT Group 2" },
      expectedStatus: 201,
    });

    const groupOneId = Number(groupOne.json?.id || 0);
    const groupTwoId = Number(groupTwo.json?.id || 0);
    assert(groupOneId > 0 && groupTwoId > 0, "Failed to create group companies");

    const entityOne = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/org/legal-entities",
      body: {
        groupCompanyId: groupOneId,
        code: "PMAT_LE1",
        name: "PMAT Legal Entity 1",
        countryId: countries.us,
        functionalCurrencyCode: "USD",
      },
      expectedStatus: 201,
    });
    const entityTwo = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/org/legal-entities",
      body: {
        groupCompanyId: groupTwoId,
        code: "PMAT_LE2",
        name: "PMAT Legal Entity 2",
        countryId: countries.tr,
        functionalCurrencyCode: "TRY",
      },
      expectedStatus: 201,
    });

    const entityOneId = Number(entityOne.json?.id || 0);
    const entityTwoId = Number(entityTwo.json?.id || 0);
    assert(entityOneId > 0 && entityTwoId > 0, "Failed to create legal entities");

    const branchOne = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/org/operating-units",
      body: {
        legalEntityId: entityOneId,
        code: "PMAT_OU1",
        name: "PMAT Branch 1",
        unitType: "BRANCH",
      },
      expectedStatus: 201,
    });
    const branchTwo = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/org/operating-units",
      body: {
        legalEntityId: entityTwoId,
        code: "PMAT_OU2",
        name: "PMAT Branch 2",
        unitType: "BRANCH",
      },
      expectedStatus: 201,
    });

    const branchOneId = Number(branchOne.json?.id || 0);
    const branchTwoId = Number(branchTwo.json?.id || 0);
    assert(branchOneId > 0 && branchTwoId > 0, "Failed to create branches");

    await apiRequest({
      token: adminToken,
      method: "PUT",
      path: `/api/v1/security/data-scopes/users/${setup.scopedUserId}/replace`,
      body: {
        scopes: [
          { scopeType: "GROUP", scopeId: groupOneId, effect: "ALLOW" },
          { scopeType: "COUNTRY", scopeId: countries.us, effect: "ALLOW" },
        ],
      },
      expectedStatus: 200,
    });

    await apiRequest({
      token: scopedToken,
      method: "POST",
      path: "/api/v1/org/legal-entities",
      body: {
        groupCompanyId: groupOneId,
        code: `PMAT_LE_ALLOW_${Date.now()}`,
        name: "PMAT Allowed LE",
        countryId: countries.us,
        functionalCurrencyCode: "USD",
      },
      expectedStatus: 201,
    });
    await apiRequest({
      token: scopedToken,
      method: "POST",
      path: "/api/v1/org/legal-entities",
      body: {
        groupCompanyId: groupTwoId,
        code: `PMAT_LE_DENY_G_${Date.now()}`,
        name: "PMAT Denied LE Group",
        countryId: countries.tr,
        functionalCurrencyCode: "TRY",
      },
      expectedStatus: 403,
    });
    await apiRequest({
      token: scopedToken,
      method: "POST",
      path: "/api/v1/org/legal-entities",
      body: {
        groupCompanyId: groupOneId,
        code: `PMAT_LE_DENY_C_${Date.now()}`,
        name: "PMAT Denied LE Country",
        countryId: countries.tr,
        functionalCurrencyCode: "TRY",
      },
      expectedStatus: 403,
    });

    await apiRequest({
      token: adminToken,
      method: "PUT",
      path: `/api/v1/security/data-scopes/users/${setup.scopedUserId}/replace`,
      body: {
        scopes: [{ scopeType: "LEGAL_ENTITY", scopeId: entityOneId, effect: "ALLOW" }],
      },
      expectedStatus: 200,
    });

    await apiRequest({
      token: scopedToken,
      method: "POST",
      path: "/api/v1/org/operating-units",
      body: {
        legalEntityId: entityOneId,
        code: `PMAT_OU_ALLOW_${Date.now()}`,
        name: "PMAT Allowed OU",
      },
      expectedStatus: 201,
    });
    await apiRequest({
      token: scopedToken,
      method: "POST",
      path: "/api/v1/org/operating-units",
      body: {
        legalEntityId: entityTwoId,
        code: `PMAT_OU_DENY_${Date.now()}`,
        name: "PMAT Denied OU",
      },
      expectedStatus: 403,
    });

    await apiRequest({
      token: adminToken,
      method: "PUT",
      path: `/api/v1/security/data-scopes/users/${setup.scopedUserId}/replace`,
      body: {
        scopes: [{ scopeType: "OPERATING_UNIT", scopeId: branchOneId, effect: "ALLOW" }],
      },
      expectedStatus: 200,
    });

    const branchList = await apiRequest({
      token: scopedToken,
      method: "GET",
      path: "/api/v1/org/operating-units",
      expectedStatus: 200,
    });

    const branchIds = new Set(
      (branchList.json?.rows || []).map((row) => Number(row.id || 0)).filter(Boolean)
    );

    assert(
      branchIds.has(branchOneId),
      "Branch scope allow test failed: expected allowed branch to be visible"
    );
    assert(
      !branchIds.has(branchTwoId),
      "Branch scope deny test failed: expected denied branch to be hidden"
    );

    await apiRequest({
      token: scopedToken,
      method: "GET",
      path: `/api/v1/org/operating-units?legalEntityId=${entityOneId}`,
      expectedStatus: 403,
    });

    console.log("");
    console.log("Permission matrix test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: setup.tenantId,
          groupOneId,
          groupTwoId,
          entityOneId,
          entityTwoId,
          branchOneId,
          branchTwoId,
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
  console.error("Permission matrix test failed:", err);
  try {
    await closePool();
  } catch {
    // ignore close errors
  }
  process.exit(1);
});
