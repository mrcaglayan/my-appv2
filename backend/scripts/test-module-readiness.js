import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.MODULE_READINESS_TEST_PORT || 3138);
const BASE_URL =
  process.env.MODULE_READINESS_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "ModuleReadiness#123";

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

async function createGroupCompany(tenantId, code, name) {
  await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, code, name]
  );
  const result = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, code]
  );
  const groupCompanyId = toNumber(result.rows?.[0]?.id);
  assert(groupCompanyId > 0, "Failed to create group company");
  return groupCompanyId;
}

async function createLegalEntity({
  tenantId,
  groupCompanyId,
  code,
  name,
  countryId,
}) {
  await query(
    `INSERT INTO legal_entities (
        tenant_id,
        group_company_id,
        code,
        name,
        country_id,
        functional_currency_code
     )
     VALUES (?, ?, ?, ?, ?, 'USD')`,
    [tenantId, groupCompanyId, code, name, countryId]
  );
  const result = await query(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, code]
  );
  const legalEntityId = toNumber(result.rows?.[0]?.id);
  assert(legalEntityId > 0, `Failed to create legal entity: ${code}`);
  return legalEntityId;
}

async function createCoa({ tenantId, legalEntityId, code, name }) {
  await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, code, name]
  );
  const result = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, code]
  );
  const coaId = toNumber(result.rows?.[0]?.id);
  assert(coaId > 0, `Failed to create CoA: ${code}`);
  return coaId;
}

async function createAccount({
  coaId,
  code,
  name,
  accountType,
  normalSide,
  allowPosting,
  isActive = true,
}) {
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
     VALUES (?, ?, ?, ?, ?, ?, NULL)`,
    [coaId, code, name, toUpper(accountType), toUpper(normalSide), Boolean(allowPosting)]
  );
  if (!isActive) {
    await query(
      `UPDATE accounts
       SET is_active = FALSE
       WHERE coa_id = ?
         AND code = ?`,
      [coaId, code]
    );
  }
}

async function resolveAccountIdByCode(coaId, code) {
  const result = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND code = ?
     LIMIT 1`,
    [coaId, code]
  );
  const accountId = toNumber(result.rows?.[0]?.id);
  assert(accountId > 0, `Account not found: ${code}`);
  return accountId;
}

function findModuleRow(payload, moduleKey, legalEntityId) {
  const rows = payload?.modules?.[moduleKey]?.byLegalEntity || [];
  return rows.find((row) => toNumber(row?.legalEntityId) === toNumber(legalEntityId)) || null;
}

function assertMissingPurposeCodes(row, expectedCodes, label) {
  const missing = new Set((row?.missingPurposeCodes || []).map((code) => toUpper(code)));
  for (const code of expectedCodes) {
    assert(missing.has(toUpper(code)), `${label} missing ${code}`);
  }
}

async function buildLegalEntityFixture({
  tenantId,
  groupCompanyId,
  countryId,
  code,
  name,
  coaCode,
  coaName,
}) {
  const legalEntityId = await createLegalEntity({
    tenantId,
    groupCompanyId,
    code,
    name,
    countryId,
  });
  const coaId = await createCoa({
    tenantId,
    legalEntityId,
    code: coaCode,
    name: coaName,
  });

  await createAccount({
    coaId,
    code: "120",
    name: "AR Control",
    accountType: "ASSET",
    normalSide: "DEBIT",
    allowPosting: true,
  });
  await createAccount({
    coaId,
    code: "600",
    name: "AR Offset",
    accountType: "REVENUE",
    normalSide: "CREDIT",
    allowPosting: true,
  });
  await createAccount({
    coaId,
    code: "320",
    name: "AP Control",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
    allowPosting: true,
  });
  await createAccount({
    coaId,
    code: "632",
    name: "AP Offset",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
    allowPosting: true,
  });
  await createAccount({
    coaId,
    code: "500",
    name: "Capital Parent",
    accountType: "EQUITY",
    normalSide: "CREDIT",
    allowPosting: false,
  });
  await createAccount({
    coaId,
    code: "501",
    name: "Commitment Parent",
    accountType: "EQUITY",
    normalSide: "DEBIT",
    allowPosting: false,
  });

  return {
    legalEntityId,
    accounts: {
      arControl: await resolveAccountIdByCode(coaId, "120"),
      arOffset: await resolveAccountIdByCode(coaId, "600"),
      apControl: await resolveAccountIdByCode(coaId, "320"),
      apOffset: await resolveAccountIdByCode(coaId, "632"),
      shareholderCapitalParent: await resolveAccountIdByCode(coaId, "500"),
      shareholderCommitmentParent: await resolveAccountIdByCode(coaId, "501"),
    },
  };
}

function buildAllPurposeRows(accounts) {
  return [
    { purposeCode: "CARI_AR_CONTROL", accountId: accounts.arControl },
    { purposeCode: "CARI_AR_OFFSET", accountId: accounts.arOffset },
    { purposeCode: "CARI_AP_CONTROL", accountId: accounts.apControl },
    { purposeCode: "CARI_AP_OFFSET", accountId: accounts.apOffset },
    {
      purposeCode: "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
      accountId: accounts.shareholderCapitalParent,
    },
    {
      purposeCode: "SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
      accountId: accounts.shareholderCommitmentParent,
    },
  ];
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantId = await createTenant(
    `MODULE_READINESS_${stamp}`,
    `Module Readiness ${stamp}`
  );
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const adminUser = await createUserWithRole({
    tenantId,
    roleCode: "TenantAdmin",
    email: `module_readiness_admin_${stamp}@example.com`,
    passwordHash,
    name: "Module Readiness Admin",
  });

  const restrictedRoleCode = `NoOrgTreeRead_${stamp}`;
  await createRoleIfMissing(tenantId, restrictedRoleCode, "No Org Tree Read");
  const restrictedUser = await createUserWithRole({
    tenantId,
    roleCode: restrictedRoleCode,
    email: `module_readiness_limited_${stamp}@example.com`,
    passwordHash,
    name: "Module Readiness Limited",
  });

  const countryId = await getCountryIdByIso2("TR");
  const groupCompanyId = await createGroupCompany(
    tenantId,
    `GRP_MODULE_${stamp}`,
    `Module Readiness Group ${stamp}`
  );

  const manualFixture = await buildLegalEntityFixture({
    tenantId,
    groupCompanyId,
    countryId,
    code: `LE_MANUAL_${stamp}`,
    name: `Manual LE ${stamp}`,
    coaCode: `COA_MANUAL_${stamp}`,
    coaName: `Manual CoA ${stamp}`,
  });
  const packFixture = await buildLegalEntityFixture({
    tenantId,
    groupCompanyId,
    countryId,
    code: `LE_PACK_${stamp}`,
    name: `Pack LE ${stamp}`,
    coaCode: `COA_PACK_${stamp}`,
    coaName: `Pack CoA ${stamp}`,
  });

  let server = null;
  try {
    server = startServerProcess();
    await waitForServer();

    const adminToken = await login(adminUser.email, TEST_PASSWORD);
    const restrictedToken = await login(restrictedUser.email, TEST_PASSWORD);

    await apiRequest({
      method: "GET",
      path: `/api/v1/onboarding/module-readiness?legalEntityId=${manualFixture.legalEntityId}`,
      expectedStatus: 401,
    });

    await apiRequest({
      token: restrictedToken,
      method: "GET",
      path: `/api/v1/onboarding/module-readiness?legalEntityId=${manualFixture.legalEntityId}`,
      expectedStatus: 403,
    });

    await apiRequest({
      token: adminToken,
      method: "GET",
      path: "/api/v1/onboarding/module-readiness?legalEntityId=abc",
      expectedStatus: 400,
    });

    const initialManualReadiness = await apiRequest({
      token: adminToken,
      method: "GET",
      path: `/api/v1/onboarding/module-readiness?legalEntityId=${manualFixture.legalEntityId}`,
      expectedStatus: 200,
    });
    const initialManualCari = findModuleRow(
      initialManualReadiness.json,
      "cariPosting",
      manualFixture.legalEntityId
    );
    const initialManualShareholder = findModuleRow(
      initialManualReadiness.json,
      "shareholderCommitment",
      manualFixture.legalEntityId
    );
    assert(initialManualCari, "Initial manual cari readiness row must exist");
    assert(
      initialManualCari.ready === false,
      "Initial manual cari readiness must be false"
    );
    assertMissingPurposeCodes(
      initialManualCari,
      ["CARI_AR_CONTROL", "CARI_AR_OFFSET", "CARI_AP_CONTROL", "CARI_AP_OFFSET"],
      "Initial manual cari"
    );
    assert(
      initialManualShareholder,
      "Initial manual shareholder readiness row must exist"
    );
    assert(
      initialManualShareholder.ready === false,
      "Initial manual shareholder readiness must be false"
    );
    assertMissingPurposeCodes(
      initialManualShareholder,
      [
        "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
        "SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
      ],
      "Initial manual shareholder"
    );

    const manualCariRows = buildAllPurposeRows(manualFixture.accounts).filter((row) =>
      row.purposeCode.startsWith("CARI_")
    );
    for (const row of manualCariRows) {
      // eslint-disable-next-line no-await-in-loop
      await apiRequest({
        token: adminToken,
        method: "POST",
        path: "/api/v1/gl/journal-purpose-accounts",
        body: {
          legalEntityId: manualFixture.legalEntityId,
          purposeCode: row.purposeCode,
          accountId: row.accountId,
        },
        expectedStatus: 201,
      });
    }

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/org/shareholder-journal-config",
      body: {
        legalEntityId: manualFixture.legalEntityId,
        capitalCreditParentAccountId: manualFixture.accounts.shareholderCapitalParent,
        commitmentDebitParentAccountId:
          manualFixture.accounts.shareholderCommitmentParent,
      },
      expectedStatus: 201,
    });

    const manualReady = await apiRequest({
      token: adminToken,
      method: "GET",
      path: `/api/v1/onboarding/module-readiness?legalEntityId=${manualFixture.legalEntityId}`,
      expectedStatus: 200,
    });
    const manualReadyCari = findModuleRow(
      manualReady.json,
      "cariPosting",
      manualFixture.legalEntityId
    );
    const manualReadyShareholder = findModuleRow(
      manualReady.json,
      "shareholderCommitment",
      manualFixture.legalEntityId
    );
    assert(
      manualReadyCari?.ready === true,
      "Manual path should make cari posting readiness true"
    );
    assert(
      (manualReadyCari?.missingPurposeCodes || []).length === 0,
      "Manual cari readiness should have no missing purpose codes"
    );
    assert(
      (manualReadyCari?.invalidMappings || []).length === 0,
      "Manual cari readiness should have no invalid mappings"
    );
    assert(
      manualReadyShareholder?.ready === true,
      "Manual path should make shareholder readiness true"
    );
    assert(
      (manualReadyShareholder?.missingPurposeCodes || []).length === 0,
      "Manual shareholder readiness should have no missing purpose codes"
    );
    assert(
      (manualReadyShareholder?.invalidMappings || []).length === 0,
      "Manual shareholder readiness should have no invalid mappings"
    );

    const packApply = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: {
        legalEntityId: packFixture.legalEntityId,
        mode: "MERGE",
        rows: buildAllPurposeRows(packFixture.accounts),
      },
      expectedStatus: 201,
    });
    assert(packApply.json?.ok === true, "Pack apply should succeed for pack fixture");

    const packReady = await apiRequest({
      token: adminToken,
      method: "GET",
      path: `/api/v1/onboarding/module-readiness?legalEntityId=${packFixture.legalEntityId}`,
      expectedStatus: 200,
    });
    const packReadyCari = findModuleRow(
      packReady.json,
      "cariPosting",
      packFixture.legalEntityId
    );
    const packReadyShareholder = findModuleRow(
      packReady.json,
      "shareholderCommitment",
      packFixture.legalEntityId
    );
    assert(
      packReadyCari?.ready === true,
      "Pack path should make cari posting readiness true"
    );
    assert(
      packReadyShareholder?.ready === true,
      "Pack path should make shareholder readiness true"
    );

    const allEntitiesReadiness = await apiRequest({
      token: adminToken,
      method: "GET",
      path: "/api/v1/onboarding/module-readiness",
      expectedStatus: 200,
    });
    const allCariRows = allEntitiesReadiness.json?.modules?.cariPosting?.byLegalEntity || [];
    const allShareholderRows =
      allEntitiesReadiness.json?.modules?.shareholderCommitment?.byLegalEntity || [];
    assert(
      allCariRows.length === 2,
      "Global module readiness should include both legal entities for cari module"
    );
    assert(
      allShareholderRows.length === 2,
      "Global module readiness should include both legal entities for shareholder module"
    );
    assert(
      allCariRows.every((row) => row.ready === true),
      "Global cari module rows should all be ready after setup"
    );
    assert(
      allShareholderRows.every((row) => row.ready === true),
      "Global shareholder module rows should all be ready after setup"
    );

    console.log("test-module-readiness: OK");
  } finally {
    await stopServerProcess(server);
    await closePool();
  }
}

main().catch((error) => {
  console.error("test-module-readiness: FAILED");
  console.error(error);
  process.exit(1);
});
