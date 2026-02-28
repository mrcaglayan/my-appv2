import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.POLICY_PACKS_READONLY_TEST_PORT || 3134);
const BASE_URL =
  process.env.POLICY_PACKS_READONLY_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "PolicyPacksReadonly#123";

const EXPECTED_PACK_IDS = Object.freeze([
  "TR_UNIFORM_V1",
  "AF_STARTER_V1",
  "US_GAAP_STARTER_V1",
]);

const CARI_PURPOSE_CODES = Object.freeze([
  "CARI_AR_CONTROL",
  "CARI_AR_OFFSET",
  "CARI_AP_CONTROL",
  "CARI_AP_OFFSET",
]);

const SHAREHOLDER_PURPOSE_CODES = Object.freeze([
  "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
  "SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
]);

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
    await sleep(300);
  }
  throw new Error(`Server did not start within ${SERVER_START_TIMEOUT_MS}ms`);
}

async function stopServerProcess(child) {
  if (!child || child.killed) {
    return;
  }
  child.kill("SIGTERM");
  await Promise.race([
    new Promise((resolve) => child.once("exit", resolve)),
    sleep(3_000),
  ]);
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

async function resolveRoleId(tenantId, roleCode) {
  const result = await query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, roleCode]
  );
  const roleId = toNumber(result.rows?.[0]?.id);
  assert(roleId > 0, `Role not found: ${roleCode}`);
  return roleId;
}

async function createRoleIfMissing(tenantId, roleCode, roleName) {
  await query(
    `INSERT INTO roles (tenant_id, code, name, is_system)
     VALUES (?, ?, ?, FALSE)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name)`,
    [tenantId, roleCode, roleName]
  );
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

  const roleId = await resolveRoleId(tenantId, roleCode);
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

function assertPackList(rows) {
  assert(Array.isArray(rows), "List response rows must be an array");
  assert(rows.length === 3, `Expected 3 packs, got ${rows.length}`);

  const seenIds = rows.map((row) => row?.packId);
  assert(
    JSON.stringify(seenIds) === JSON.stringify(EXPECTED_PACK_IDS),
    `Unexpected policy pack ids: ${JSON.stringify(seenIds)}`
  );

  for (const row of rows) {
    assert(typeof row.packId === "string", "packId must be a string");
    assert(typeof row.countryIso2 === "string", "countryIso2 must be a string");
    assert(typeof row.label === "string", "label must be a string");
    assert(row.locked === true, `pack ${row.packId} must be locked=true`);
  }
}

function assertPackDetailRow(row, expectedPackId) {
  assert(Boolean(row), "Detail response row is required");
  assert(row.packId === expectedPackId, `Expected packId ${expectedPackId}`);
  assert(row.locked === true, `${expectedPackId} must be locked=true`);
  assert(Array.isArray(row.modules), `${expectedPackId} modules must be an array`);

  const moduleKeys = new Set(row.modules.map((module) => module?.moduleKey));
  assert(moduleKeys.has("cariPosting"), `${expectedPackId} missing cariPosting module`);
  assert(
    moduleKeys.has("shareholderCommitment"),
    `${expectedPackId} missing shareholderCommitment module`
  );

  const purposeCodes = new Set();
  for (const module of row.modules) {
    for (const purposeCode of module?.requiredPurposeCodes || []) {
      purposeCodes.add(String(purposeCode));
    }
    for (const target of module?.purposeTargets || []) {
      purposeCodes.add(String(target?.purposeCode || ""));
    }
  }

  for (const purposeCode of [...CARI_PURPOSE_CODES, ...SHAREHOLDER_PURPOSE_CODES]) {
    assert(
      purposeCodes.has(purposeCode),
      `${expectedPackId} must include purpose ${purposeCode}`
    );
  }
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantId = await createTenant(
    `POLICY_PACKS_${stamp}`,
    `Policy Packs Test ${stamp}`
  );

  // Seed after tenant creation so system roles/permissions exist for this tenant.
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const adminUser = await createUserWithRole({
    tenantId,
    roleCode: "TenantAdmin",
    email: `policy_packs_admin_${stamp}@example.com`,
    passwordHash,
    name: "Policy Packs Admin",
  });

  const restrictedRoleCode = `NoPackRead_${stamp}`;
  await createRoleIfMissing(tenantId, restrictedRoleCode, "No Pack Read Role");
  const restrictedUser = await createUserWithRole({
    tenantId,
    roleCode: restrictedRoleCode,
    email: `policy_packs_limited_${stamp}@example.com`,
    passwordHash,
    name: "Policy Packs Limited",
  });

  let server = null;
  try {
    server = startServerProcess();
    await waitForServer();

    const adminToken = await login(adminUser.email, TEST_PASSWORD);
    const restrictedToken = await login(restrictedUser.email, TEST_PASSWORD);

    await apiRequest({
      method: "GET",
      path: "/api/v1/onboarding/policy-packs",
      expectedStatus: 401,
    });

    await apiRequest({
      token: restrictedToken,
      method: "GET",
      path: "/api/v1/onboarding/policy-packs",
      expectedStatus: 403,
    });

    const listResponse = await apiRequest({
      token: adminToken,
      method: "GET",
      path: "/api/v1/onboarding/policy-packs",
      expectedStatus: 200,
    });
    assertPackList(listResponse.json?.rows);

    for (const packId of EXPECTED_PACK_IDS) {
      const detailResponse = await apiRequest({
        token: adminToken,
        method: "GET",
        path: `/api/v1/onboarding/policy-packs/${packId}`,
        expectedStatus: 200,
      });
      assertPackDetailRow(detailResponse.json?.row, packId);
    }

    const missingPackResponse = await apiRequest({
      token: adminToken,
      method: "GET",
      path: "/api/v1/onboarding/policy-packs/NOT_FOUND_PACK",
      expectedStatus: 404,
    });
    assert(
      String(missingPackResponse.json?.message || "").length > 0,
      "Expected missing pack response message"
    );

    console.log("test-policy-packs-readonly: OK");
  } finally {
    await stopServerProcess(server);
    await closePool();
  }
}

main().catch((error) => {
  console.error("test-policy-packs-readonly: FAILED");
  console.error(error);
  process.exit(1);
});

