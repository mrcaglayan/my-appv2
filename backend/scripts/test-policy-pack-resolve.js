import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.POLICY_PACK_RESOLVE_TEST_PORT || 3136);
const BASE_URL =
  process.env.POLICY_PACK_RESOLVE_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "PolicyPackResolve#123";

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

async function createCoa({
  tenantId,
  legalEntityId,
  code,
  name,
}) {
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

function findRow(rows, purposeCode) {
  return (rows || []).find(
    (row) => toUpper(row?.purposeCode) === toUpper(purposeCode)
  );
}

async function buildResolveFixtures(tenantId, stamp) {
  const countryId = await getCountryIdByIso2("TR");
  const groupCompanyId = await createGroupCompany(
    tenantId,
    `GRP_RESOLVE_${stamp}`,
    `Resolve Group ${stamp}`
  );

  const goodLegalEntityId = await createLegalEntity({
    tenantId,
    groupCompanyId,
    code: `LE_GOOD_${stamp}`,
    name: `Resolve Good ${stamp}`,
    countryId,
  });
  const goodCoaId = await createCoa({
    tenantId,
    legalEntityId: goodLegalEntityId,
    code: `COA_GOOD_${stamp}`,
    name: `COA Good ${stamp}`,
  });
  await createAccount({
    coaId: goodCoaId,
    code: "120",
    name: "AR Control",
    accountType: "ASSET",
    normalSide: "DEBIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: goodCoaId,
    code: "100",
    name: "Cash",
    accountType: "ASSET",
    normalSide: "DEBIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: goodCoaId,
    code: "102",
    name: "Banks",
    accountType: "ASSET",
    normalSide: "DEBIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: goodCoaId,
    code: "600",
    name: "AR Offset",
    accountType: "REVENUE",
    normalSide: "CREDIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: goodCoaId,
    code: "320",
    name: "AP Control",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: goodCoaId,
    code: "340",
    name: "Customer Advances",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: goodCoaId,
    code: "632",
    name: "AP Offset Fallback",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: goodCoaId,
    code: "159",
    name: "Vendor Advances",
    accountType: "ASSET",
    normalSide: "DEBIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: goodCoaId,
    code: "500",
    name: "Capital Parent",
    accountType: "EQUITY",
    normalSide: "CREDIT",
    allowPosting: false,
  });
  await createAccount({
    coaId: goodCoaId,
    code: "501",
    name: "Commitment Parent",
    accountType: "EQUITY",
    normalSide: "DEBIT",
    allowPosting: false,
  });

  const issueLegalEntityId = await createLegalEntity({
    tenantId,
    groupCompanyId,
    code: `LE_ISSUE_${stamp}`,
    name: `Resolve Issue ${stamp}`,
    countryId,
  });
  const issueCoaAId = await createCoa({
    tenantId,
    legalEntityId: issueLegalEntityId,
    code: `COA_ISSUE_A_${stamp}`,
    name: `COA Issue A ${stamp}`,
  });
  const issueCoaBId = await createCoa({
    tenantId,
    legalEntityId: issueLegalEntityId,
    code: `COA_ISSUE_B_${stamp}`,
    name: `COA Issue B ${stamp}`,
  });

  await createAccount({
    coaId: issueCoaAId,
    code: "120",
    name: "AR Control",
    accountType: "ASSET",
    normalSide: "DEBIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: issueCoaAId,
    code: "600",
    name: "AR Offset",
    accountType: "REVENUE",
    normalSide: "CREDIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: issueCoaAId,
    code: "320",
    name: "AP Control",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: issueCoaAId,
    code: "770",
    name: "AP Offset Candidate A",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: issueCoaBId,
    code: "770",
    name: "AP Offset Candidate B",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: issueCoaAId,
    code: "500",
    name: "Capital Wrong (postable)",
    accountType: "EQUITY",
    normalSide: "CREDIT",
    allowPosting: true,
  });
  await createAccount({
    coaId: issueCoaAId,
    code: "501",
    name: "Commitment Parent",
    accountType: "EQUITY",
    normalSide: "DEBIT",
    allowPosting: false,
  });

  return {
    goodLegalEntityId,
    issueLegalEntityId,
  };
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantId = await createTenant(
    `POLICY_PACK_RESOLVE_${stamp}`,
    `Policy Pack Resolve ${stamp}`
  );
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const adminUser = await createUserWithRole({
    tenantId,
    roleCode: "TenantAdmin",
    email: `pack_resolve_admin_${stamp}@example.com`,
    passwordHash,
    name: "Policy Pack Resolve Admin",
  });

  const restrictedRoleCode = `NoOrgTreeRead_${stamp}`;
  await createRoleIfMissing(tenantId, restrictedRoleCode, "No Org Tree Read");
  const restrictedUser = await createUserWithRole({
    tenantId,
    roleCode: restrictedRoleCode,
    email: `pack_resolve_limited_${stamp}@example.com`,
    passwordHash,
    name: "Policy Pack Resolve Limited",
  });

  const fixtures = await buildResolveFixtures(tenantId, stamp);

  let server = null;
  try {
    server = startServerProcess();
    await waitForServer();

    const adminToken = await login(adminUser.email, TEST_PASSWORD);
    const restrictedToken = await login(restrictedUser.email, TEST_PASSWORD);

    await apiRequest({
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/resolve",
      body: { legalEntityId: fixtures.goodLegalEntityId },
      expectedStatus: 401,
    });

    await apiRequest({
      token: restrictedToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/resolve",
      body: { legalEntityId: fixtures.goodLegalEntityId },
      expectedStatus: 403,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/resolve",
      body: {},
      expectedStatus: 400,
    });

    const missingPack = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/NOT_FOUND/resolve",
      body: { legalEntityId: fixtures.goodLegalEntityId },
      expectedStatus: 404,
    });
    assert(
      String(missingPack.json?.message || "").includes("Policy pack not found"),
      "Unknown pack should return not found message"
    );

    const goodResolve = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/resolve",
      body: { legalEntityId: fixtures.goodLegalEntityId },
      expectedStatus: 200,
    });
    assert(
      toNumber(goodResolve.json?.summary?.total) === 18,
      "Good resolve should evaluate 18 purpose rows"
    );
    assert(
      toNumber(goodResolve.json?.summary?.missing) === 0,
      "Good resolve should have zero missing rows"
    );
    const goodRows = goodResolve.json?.rows || [];
    const goodApOffset = findRow(goodRows, "CARI_AP_OFFSET");
    assert(
      goodApOffset?.missing === false,
      "Good resolve CARI_AP_OFFSET must resolve"
    );
    assert(
      String(goodApOffset?.accountCode || "") === "632",
      "TR fallback must resolve CARI_AP_OFFSET to 632 when 770 is missing"
    );
    const goodArOffsetCash = findRow(goodRows, "CARI_AR_OFFSET_CASH");
    assert(
      goodArOffsetCash?.missing === false,
      "Good resolve CARI_AR_OFFSET_CASH must resolve"
    );
    assert(
      String(goodArOffsetCash?.accountCode || "") === "102",
      "CARI_AR_OFFSET_CASH should resolve to 102 when available"
    );
    const goodApOffsetOnAccount = findRow(goodRows, "CARI_AP_OFFSET_ON_ACCOUNT");
    assert(
      goodApOffsetOnAccount?.missing === false,
      "Good resolve CARI_AP_OFFSET_ON_ACCOUNT must resolve"
    );
    assert(
      String(goodApOffsetOnAccount?.accountCode || "") === "159",
      "CARI_AP_OFFSET_ON_ACCOUNT should resolve to 159 when available"
    );

    const issueResolve = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/resolve",
      body: { legalEntityId: fixtures.issueLegalEntityId },
      expectedStatus: 200,
    });
    const issueRows = issueResolve.json?.rows || [];

    const issueApOffset = findRow(issueRows, "CARI_AP_OFFSET");
    assert(issueApOffset?.missing === true, "Issue CARI_AP_OFFSET should be missing");
    assert(
      String(issueApOffset?.reason || "") === "ambiguous_match",
      "Issue CARI_AP_OFFSET should be ambiguous_match"
    );
    const candidateIds = issueApOffset?.details?.candidateAccountIds || [];
    assert(
      Array.isArray(candidateIds) && candidateIds.length === 2,
      "Ambiguous CARI_AP_OFFSET should include both candidate account ids"
    );

    const issueCapitalParent = findRow(
      issueRows,
      "SHAREHOLDER_CAPITAL_CREDIT_PARENT"
    );
    assert(
      issueCapitalParent?.missing === true,
      "Issue SHAREHOLDER_CAPITAL_CREDIT_PARENT should be missing"
    );
    assert(
      String(issueCapitalParent?.reason || "") === "unsuitable_match",
      "Issue SHAREHOLDER_CAPITAL_CREDIT_PARENT should be unsuitable_match"
    );
    const unsuitableIssues = issueCapitalParent?.details?.issues || [];
    assert(
      unsuitableIssues.includes("must_be_non_postable"),
      "Unsuitable shareholder parent should include must_be_non_postable issue"
    );

    const issueCommitmentParent = findRow(
      issueRows,
      "SHAREHOLDER_COMMITMENT_DEBIT_PARENT"
    );
    assert(
      issueCommitmentParent?.missing === false,
      "Issue SHAREHOLDER_COMMITMENT_DEBIT_PARENT should still resolve"
    );

    console.log("test-policy-pack-resolve: OK");
  } finally {
    await stopServerProcess(server);
    await closePool();
  }
}

main().catch((error) => {
  console.error("test-policy-pack-resolve: FAILED");
  console.error(error);
  process.exit(1);
});
