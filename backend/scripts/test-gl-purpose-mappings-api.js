import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.GL_PURPOSE_MAPPINGS_TEST_PORT || 3135);
const BASE_URL =
  process.env.GL_PURPOSE_MAPPINGS_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "GlPurposeMappings#123";

const CARI_AR_CONTROL = "CARI_AR_CONTROL";
const CARI_AR_OFFSET = "CARI_AR_OFFSET";
const EXPECTED_CARI_PURPOSE_ROW_COUNT = 16;

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

async function createRoleIfMissing(tenantId, roleCode, roleName) {
  await query(
    `INSERT INTO roles (tenant_id, code, name, is_system)
     VALUES (?, ?, ?, FALSE)
     ON DUPLICATE KEY UPDATE name = VALUES(name)`,
    [tenantId, roleCode, roleName]
  );
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

async function getCountryIdByIso2(iso2) {
  const result = await query(
    `SELECT id
     FROM countries
     WHERE iso2 = ?
     LIMIT 1`,
    [toUpper(iso2)]
  );
  const countryId = toNumber(result.rows?.[0]?.id);
  assert(countryId > 0, `Country not found: ${iso2}`);
  return countryId;
}

async function createGlFixtureData(tenantId, stamp) {
  const countryId = await getCountryIdByIso2("US");

  await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `GRP_${stamp}`, `Group ${stamp}`]
  );
  const groupCompanyResult = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `GRP_${stamp}`]
  );
  const groupCompanyId = toNumber(groupCompanyResult.rows?.[0]?.id);
  assert(groupCompanyId > 0, "Failed to create group company");

  await query(
    `INSERT INTO legal_entities (
        tenant_id,
        group_company_id,
        code,
        name,
        country_id,
        functional_currency_code
     )
     VALUES (?, ?, ?, ?, ?, 'USD'), (?, ?, ?, ?, ?, 'USD')`,
    [
      tenantId,
      groupCompanyId,
      `LE_A_${stamp}`,
      `Legal Entity A ${stamp}`,
      countryId,
      tenantId,
      groupCompanyId,
      `LE_B_${stamp}`,
      `Legal Entity B ${stamp}`,
      countryId,
    ]
  );

  const legalEntityRows = await query(
    `SELECT id, code
     FROM legal_entities
     WHERE tenant_id = ?
       AND code IN (?, ?)`,
    [tenantId, `LE_A_${stamp}`, `LE_B_${stamp}`]
  );
  const legalEntityIdByCode = new Map(
    (legalEntityRows.rows || []).map((row) => [String(row.code), toNumber(row.id)])
  );
  const legalEntityAId = legalEntityIdByCode.get(`LE_A_${stamp}`);
  const legalEntityBId = legalEntityIdByCode.get(`LE_B_${stamp}`);
  assert(legalEntityAId > 0, "Failed to create legalEntityA");
  assert(legalEntityBId > 0, "Failed to create legalEntityB");

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id,
        legal_entity_id,
        scope,
        code,
        name
     )
     VALUES
       (?, ?, 'LEGAL_ENTITY', ?, ?),
       (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [
      tenantId,
      legalEntityAId,
      `COA_A_${stamp}`,
      `COA A ${stamp}`,
      tenantId,
      legalEntityBId,
      `COA_B_${stamp}`,
      `COA B ${stamp}`,
    ]
  );

  const coaRows = await query(
    `SELECT id, code
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code IN (?, ?)`,
    [tenantId, `COA_A_${stamp}`, `COA_B_${stamp}`]
  );
  const coaIdByCode = new Map(
    (coaRows.rows || []).map((row) => [String(row.code), toNumber(row.id)])
  );
  const coaAId = coaIdByCode.get(`COA_A_${stamp}`);
  const coaBId = coaIdByCode.get(`COA_B_${stamp}`);
  assert(coaAId > 0, "Failed to create coaA");
  assert(coaBId > 0, "Failed to create coaB");

  await query(
    `INSERT INTO accounts (
        coa_id,
        code,
        name,
        account_type,
        normal_side,
        allow_posting,
        parent_account_id
     )
     VALUES
       (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL),
       (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL),
       (?, ?, ?, 'ASSET', 'DEBIT', FALSE, NULL),
       (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL),
       (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL)`,
    [
      coaAId,
      `120_${stamp}`,
      `AR Control A ${stamp}`,
      coaAId,
      `121_${stamp}`,
      `AR Offset A ${stamp}`,
      coaAId,
      `500_${stamp}`,
      `Non Postable A ${stamp}`,
      coaAId,
      `122_${stamp}`,
      `Inactive A ${stamp}`,
      coaBId,
      `220_${stamp}`,
      `Other Entity ${stamp}`,
    ]
  );

  await query(
    `UPDATE accounts
     SET is_active = FALSE
     WHERE coa_id = ?
       AND code = ?`,
    [coaAId, `122_${stamp}`]
  );

  const accountRows = await query(
    `SELECT a.id, a.code
     FROM accounts a
     WHERE a.coa_id IN (?, ?)
       AND a.code IN (?, ?, ?, ?, ?)`,
    [
      coaAId,
      coaBId,
      `120_${stamp}`,
      `121_${stamp}`,
      `500_${stamp}`,
      `122_${stamp}`,
      `220_${stamp}`,
    ]
  );
  const accountIdByCode = new Map(
    (accountRows.rows || []).map((row) => [String(row.code), toNumber(row.id)])
  );

  return {
    legalEntityAId,
    legalEntityBId,
    validArControlAccountId: accountIdByCode.get(`120_${stamp}`),
    validArOffsetAccountId: accountIdByCode.get(`121_${stamp}`),
    nonPostableAccountId: accountIdByCode.get(`500_${stamp}`),
    inactiveAccountId: accountIdByCode.get(`122_${stamp}`),
    otherEntityAccountId: accountIdByCode.get(`220_${stamp}`),
  };
}

function findPurposeRow(rows, purposeCode) {
  return (rows || []).find(
    (row) => toUpper(row?.purposeCode) === toUpper(purposeCode)
  );
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantId = await createTenant(`GL_PMAP_${stamp}`, `GL Purpose Map ${stamp}`);
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const adminUser = await createUserWithRole({
    tenantId,
    roleCode: "TenantAdmin",
    email: `gl_pmap_admin_${stamp}@example.com`,
    passwordHash,
    name: "GL Purpose Mapping Admin",
  });

  const restrictedRoleCode = `NoGlPurposeRead_${stamp}`;
  await createRoleIfMissing(tenantId, restrictedRoleCode, "No GL Purpose Read Role");
  const restrictedUser = await createUserWithRole({
    tenantId,
    roleCode: restrictedRoleCode,
    email: `gl_pmap_limited_${stamp}@example.com`,
    passwordHash,
    name: "GL Purpose Mapping Limited",
  });

  const fixture = await createGlFixtureData(tenantId, stamp);

  let server = null;
  try {
    server = startServerProcess();
    await waitForServer();

    const adminToken = await login(adminUser.email, TEST_PASSWORD);
    const restrictedToken = await login(restrictedUser.email, TEST_PASSWORD);

    await apiRequest({
      method: "GET",
      path: `/api/v1/gl/journal-purpose-accounts?legalEntityId=${fixture.legalEntityAId}`,
      expectedStatus: 401,
    });

    await apiRequest({
      token: restrictedToken,
      method: "GET",
      path: `/api/v1/gl/journal-purpose-accounts?legalEntityId=${fixture.legalEntityAId}`,
      expectedStatus: 403,
    });

    await apiRequest({
      token: adminToken,
      method: "GET",
      path: "/api/v1/gl/journal-purpose-accounts",
      expectedStatus: 400,
    });

    const initialList = await apiRequest({
      token: adminToken,
      method: "GET",
      path: `/api/v1/gl/journal-purpose-accounts?legalEntityId=${fixture.legalEntityAId}`,
      expectedStatus: 200,
    });
    assert(
      Array.isArray(initialList.json?.rows) &&
        initialList.json.rows.length === EXPECTED_CARI_PURPOSE_ROW_COUNT,
      `Initial list must include ${EXPECTED_CARI_PURPOSE_ROW_COUNT} CARI purpose rows`
    );
    assert(
      initialList.json.rows.every((row) => row.accountId === null),
      "Initial CARI mappings must be empty"
    );

    const shareholderAttempt = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/gl/journal-purpose-accounts",
      body: {
        legalEntityId: fixture.legalEntityAId,
        purposeCode: "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
        accountId: fixture.validArControlAccountId,
      },
      expectedStatus: 400,
    });
    assert(
      String(shareholderAttempt.json?.message || "").includes(
        "/api/v1/org/shareholder-journal-config"
      ),
      "Shareholder purpose rejection must direct to org shareholder config endpoint"
    );

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/gl/journal-purpose-accounts",
      body: {
        legalEntityId: fixture.legalEntityAId,
        purposeCode: "UNSUPPORTED_PURPOSE_CODE",
        accountId: fixture.validArControlAccountId,
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/gl/journal-purpose-accounts",
      body: {
        legalEntityId: fixture.legalEntityAId,
        purposeCode: CARI_AR_CONTROL,
        accountId: fixture.nonPostableAccountId,
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/gl/journal-purpose-accounts",
      body: {
        legalEntityId: fixture.legalEntityAId,
        purposeCode: CARI_AR_CONTROL,
        accountId: fixture.inactiveAccountId,
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/gl/journal-purpose-accounts",
      body: {
        legalEntityId: fixture.legalEntityAId,
        purposeCode: CARI_AR_CONTROL,
        accountId: fixture.otherEntityAccountId,
      },
      expectedStatus: 400,
    });

    const upsertControl = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/gl/journal-purpose-accounts",
      body: {
        legalEntityId: fixture.legalEntityAId,
        purposeCode: CARI_AR_CONTROL,
        accountId: fixture.validArControlAccountId,
      },
      expectedStatus: 201,
    });
    assert(upsertControl.json?.ok === true, "Valid upsert must return ok=true");
    assert(
      toUpper(upsertControl.json?.row?.purposeCode) === CARI_AR_CONTROL,
      "Upserted row must include CARI_AR_CONTROL"
    );

    const upsertOffset = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/gl/journal-purpose-accounts",
      body: {
        legalEntityId: fixture.legalEntityAId,
        purposeCode: CARI_AR_OFFSET,
        accountId: fixture.validArOffsetAccountId,
      },
      expectedStatus: 201,
    });
    assert(upsertOffset.json?.ok === true, "Second valid upsert must return ok=true");

    const afterUpsertList = await apiRequest({
      token: adminToken,
      method: "GET",
      path: `/api/v1/gl/journal-purpose-accounts?legalEntityId=${fixture.legalEntityAId}`,
      expectedStatus: 200,
    });
    const controlRow = findPurposeRow(afterUpsertList.json?.rows, CARI_AR_CONTROL);
    const offsetRow = findPurposeRow(afterUpsertList.json?.rows, CARI_AR_OFFSET);
    assert(
      toNumber(controlRow?.accountId) === fixture.validArControlAccountId,
      "CARI_AR_CONTROL must point to valid control account"
    );
    assert(
      toNumber(offsetRow?.accountId) === fixture.validArOffsetAccountId,
      "CARI_AR_OFFSET must point to valid offset account"
    );
    assert(
      controlRow?.validForCariPosting === true && offsetRow?.validForCariPosting === true,
      "Mapped rows must be valid for CARI posting"
    );

    const remapControl = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/gl/journal-purpose-accounts",
      body: {
        legalEntityId: fixture.legalEntityAId,
        purposeCode: CARI_AR_CONTROL,
        accountId: fixture.validArOffsetAccountId,
      },
      expectedStatus: 201,
    });
    assert(remapControl.json?.ok === true, "Remap upsert must return ok=true");

    const remapList = await apiRequest({
      token: adminToken,
      method: "GET",
      path: `/api/v1/gl/journal-purpose-accounts?legalEntityId=${fixture.legalEntityAId}`,
      expectedStatus: 200,
    });
    const remappedControlRow = findPurposeRow(remapList.json?.rows, CARI_AR_CONTROL);
    assert(
      toNumber(remappedControlRow?.accountId) === fixture.validArOffsetAccountId,
      "Remap should update existing purpose mapping (idempotent upsert behavior)"
    );

    console.log("test-gl-purpose-mappings-api: OK");
  } finally {
    await stopServerProcess(server);
    await closePool();
  }
}

main().catch((error) => {
  console.error("test-gl-purpose-mappings-api: FAILED");
  console.error(error);
  process.exit(1);
});
