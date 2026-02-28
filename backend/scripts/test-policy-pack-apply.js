import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.POLICY_PACK_APPLY_TEST_PORT || 3137);
const BASE_URL =
  process.env.POLICY_PACK_APPLY_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "PolicyPackApply#123";
const TR_MANAGED_PURPOSE_CODES = Object.freeze([
  "CARI_AR_CONTROL",
  "CARI_AR_OFFSET",
  "CARI_AP_CONTROL",
  "CARI_AP_OFFSET",
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

async function getManagedPurposeMappings(tenantId, legalEntityId) {
  const placeholders = TR_MANAGED_PURPOSE_CODES.map(() => "?").join(", ");
  const result = await query(
    `SELECT purpose_code, account_id
     FROM journal_purpose_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND purpose_code IN (${placeholders})
     ORDER BY purpose_code`,
    [tenantId, legalEntityId, ...TR_MANAGED_PURPOSE_CODES]
  );
  return result.rows || [];
}

async function getCustomPurposeMapping(tenantId, legalEntityId, purposeCode) {
  const result = await query(
    `SELECT purpose_code, account_id
     FROM journal_purpose_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND purpose_code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, purposeCode]
  );
  return result.rows?.[0] || null;
}

async function getPolicyPackMetadataRows(tenantId, legalEntityId) {
  const result = await query(
    `SELECT id, pack_id, mode, applied_by_user_id, applied_at
     FROM legal_entity_policy_packs
     WHERE tenant_id = ?
       AND legal_entity_id = ?
     ORDER BY id`,
    [tenantId, legalEntityId]
  );
  return result.rows || [];
}

async function buildApplyFixture(tenantId, stamp) {
  const countryId = await getCountryIdByIso2("TR");
  const groupCompanyId = await createGroupCompany(
    tenantId,
    `GRP_APPLY_${stamp}`,
    `Apply Group ${stamp}`
  );

  const legalEntityId = await createLegalEntity({
    tenantId,
    groupCompanyId,
    code: `LE_APPLY_${stamp}`,
    name: `Apply Legal Entity ${stamp}`,
    countryId,
  });
  const otherLegalEntityId = await createLegalEntity({
    tenantId,
    groupCompanyId,
    code: `LE_OTHER_${stamp}`,
    name: `Apply Other Legal Entity ${stamp}`,
    countryId,
  });

  const coaId = await createCoa({
    tenantId,
    legalEntityId,
    code: `COA_APPLY_${stamp}`,
    name: `Apply CoA ${stamp}`,
  });
  const otherCoaId = await createCoa({
    tenantId,
    legalEntityId: otherLegalEntityId,
    code: `COA_OTHER_${stamp}`,
    name: `Other CoA ${stamp}`,
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
  await createAccount({
    coaId,
    code: "500P",
    name: "Capital Postable Invalid",
    accountType: "EQUITY",
    normalSide: "CREDIT",
    allowPosting: true,
  });
  await createAccount({
    coaId,
    code: "120NP",
    name: "AR Non Postable Invalid",
    accountType: "ASSET",
    normalSide: "DEBIT",
    allowPosting: false,
  });
  await createAccount({
    coaId: otherCoaId,
    code: "120",
    name: "Other LE Account",
    accountType: "ASSET",
    normalSide: "DEBIT",
    allowPosting: true,
  });

  return {
    legalEntityId,
    accounts: {
      arControl: await resolveAccountIdByCode(coaId, "120"),
      arOffset: await resolveAccountIdByCode(coaId, "600"),
      apControl: await resolveAccountIdByCode(coaId, "320"),
      apOffset: await resolveAccountIdByCode(coaId, "632"),
      shareholderCapital: await resolveAccountIdByCode(coaId, "500"),
      shareholderCommitment: await resolveAccountIdByCode(coaId, "501"),
      invalidShareholderPostable: await resolveAccountIdByCode(coaId, "500P"),
      invalidCariNonPostable: await resolveAccountIdByCode(coaId, "120NP"),
      otherLegalEntityAccount: await resolveAccountIdByCode(otherCoaId, "120"),
    },
  };
}

function buildValidTrRows(accounts) {
  return [
    { purposeCode: "CARI_AR_CONTROL", accountId: accounts.arControl },
    { purposeCode: "CARI_AR_OFFSET", accountId: accounts.arOffset },
    { purposeCode: "CARI_AP_CONTROL", accountId: accounts.apControl },
    { purposeCode: "CARI_AP_OFFSET", accountId: accounts.apOffset },
    {
      purposeCode: "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
      accountId: accounts.shareholderCapital,
    },
    {
      purposeCode: "SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
      accountId: accounts.shareholderCommitment,
    },
  ];
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantId = await createTenant(
    `POLICY_PACK_APPLY_${stamp}`,
    `Policy Pack Apply ${stamp}`
  );
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const adminUser = await createUserWithRole({
    tenantId,
    roleCode: "TenantAdmin",
    email: `pack_apply_admin_${stamp}@example.com`,
    passwordHash,
    name: "Policy Pack Apply Admin",
  });

  const restrictedRoleCode = `NoGlAccountUpsert_${stamp}`;
  await createRoleIfMissing(tenantId, restrictedRoleCode, "No GL Account Upsert");
  const restrictedUser = await createUserWithRole({
    tenantId,
    roleCode: restrictedRoleCode,
    email: `pack_apply_limited_${stamp}@example.com`,
    passwordHash,
    name: "Policy Pack Apply Limited",
  });

  const fixture = await buildApplyFixture(tenantId, stamp);
  const validRows = buildValidTrRows(fixture.accounts);

  let server = null;
  try {
    server = startServerProcess();
    await waitForServer();

    const adminToken = await login(adminUser.email, TEST_PASSWORD);
    const restrictedToken = await login(restrictedUser.email, TEST_PASSWORD);

    await apiRequest({
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: {
        legalEntityId: fixture.legalEntityId,
        mode: "MERGE",
        rows: validRows,
      },
      expectedStatus: 401,
    });

    await apiRequest({
      token: restrictedToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: {
        legalEntityId: fixture.legalEntityId,
        mode: "MERGE",
        rows: validRows,
      },
      expectedStatus: 403,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: { mode: "MERGE", rows: validRows },
      expectedStatus: 400,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/NOT_FOUND/apply",
      body: {
        legalEntityId: fixture.legalEntityId,
        mode: "MERGE",
        rows: validRows,
      },
      expectedStatus: 404,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: {
        legalEntityId: fixture.legalEntityId,
        mode: "INVALID_MODE",
        rows: validRows,
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: {
        legalEntityId: fixture.legalEntityId,
        mode: "MERGE",
        rows: [{ purposeCode: "UNKNOWN_PURPOSE", accountId: fixture.accounts.arControl }],
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: {
        legalEntityId: fixture.legalEntityId,
        mode: "MERGE",
        rows: [
          {
            purposeCode: "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
            accountId: fixture.accounts.invalidShareholderPostable,
          },
        ],
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: {
        legalEntityId: fixture.legalEntityId,
        mode: "MERGE",
        rows: [
          {
            purposeCode: "CARI_AR_CONTROL",
            accountId: fixture.accounts.invalidCariNonPostable,
          },
        ],
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: {
        legalEntityId: fixture.legalEntityId,
        mode: "MERGE",
        rows: [
          {
            purposeCode: "CARI_AR_CONTROL",
            accountId: fixture.accounts.otherLegalEntityAccount,
          },
        ],
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: {
        legalEntityId: fixture.legalEntityId,
        mode: "MERGE",
        rows: [
          {
            purposeCode: "CARI_AR_CONTROL",
            accountId: fixture.accounts.arControl,
          },
          {
            purposeCode: "CARI_AR_OFFSET",
            accountId: fixture.accounts.arControl,
          },
        ],
      },
      expectedStatus: 400,
    });

    const firstApply = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: {
        legalEntityId: fixture.legalEntityId,
        mode: "MERGE",
        rows: validRows,
      },
      expectedStatus: 201,
    });
    assert(firstApply.json?.ok === true, "First apply must return ok=true");
    assert(
      toUpper(firstApply.json?.mode) === "MERGE",
      "First apply must persist MERGE mode"
    );
    assert(
      toNumber(firstApply.json?.metadata?.metadataId) > 0,
      "First apply must return metadata id"
    );

    const firstManagedRows = await getManagedPurposeMappings(
      tenantId,
      fixture.legalEntityId
    );
    assert(
      firstManagedRows.length === TR_MANAGED_PURPOSE_CODES.length,
      "First MERGE should upsert all managed purpose rows"
    );
    const metadataAfterFirstApply = await getPolicyPackMetadataRows(
      tenantId,
      fixture.legalEntityId
    );
    assert(
      metadataAfterFirstApply.length === 1,
      "Metadata history should contain 1 row after first apply"
    );

    const secondApply = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: {
        legalEntityId: fixture.legalEntityId,
        mode: "MERGE",
        rows: validRows,
      },
      expectedStatus: 201,
    });
    assert(secondApply.json?.ok === true, "Second apply must return ok=true");

    const secondManagedRows = await getManagedPurposeMappings(
      tenantId,
      fixture.legalEntityId
    );
    assert(
      secondManagedRows.length === TR_MANAGED_PURPOSE_CODES.length,
      "Second MERGE must remain idempotent (no duplicate mappings)"
    );
    const metadataAfterSecondApply = await getPolicyPackMetadataRows(
      tenantId,
      fixture.legalEntityId
    );
    assert(
      metadataAfterSecondApply.length === 2,
      "Metadata history should append row per apply (count=2)"
    );

    await query(
      `INSERT INTO journal_purpose_accounts (
          tenant_id,
          legal_entity_id,
          purpose_code,
          account_id
       )
       VALUES (?, ?, 'CUSTOM_PURPOSE_KEEP', ?)
       ON DUPLICATE KEY UPDATE account_id = VALUES(account_id)`,
      [tenantId, fixture.legalEntityId, fixture.accounts.arControl]
    );

    const overwriteApply = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/onboarding/policy-packs/TR_UNIFORM_V1/apply",
      body: {
        legalEntityId: fixture.legalEntityId,
        mode: "OVERWRITE",
        rows: [
          {
            purposeCode: "CARI_AR_CONTROL",
            accountId: fixture.accounts.arControl,
          },
          {
            purposeCode: "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
            accountId: fixture.accounts.shareholderCapital,
          },
        ],
      },
      expectedStatus: 201,
    });
    assert(overwriteApply.json?.ok === true, "OVERWRITE apply must return ok=true");
    assert(
      toUpper(overwriteApply.json?.mode) === "OVERWRITE",
      "OVERWRITE apply must persist mode"
    );

    const managedAfterOverwrite = await getManagedPurposeMappings(
      tenantId,
      fixture.legalEntityId
    );
    assert(
      managedAfterOverwrite.length === 2,
      "OVERWRITE must replace managed mappings with provided subset only"
    );
    const mappedPurposeSet = new Set(
      managedAfterOverwrite.map((row) => toUpper(row.purpose_code))
    );
    assert(
      mappedPurposeSet.has("CARI_AR_CONTROL") &&
        mappedPurposeSet.has("SHAREHOLDER_CAPITAL_CREDIT_PARENT"),
      "OVERWRITE must keep only provided managed purposes"
    );

    const customPurposeRow = await getCustomPurposeMapping(
      tenantId,
      fixture.legalEntityId,
      "CUSTOM_PURPOSE_KEEP"
    );
    assert(
      toNumber(customPurposeRow?.account_id) === fixture.accounts.arControl,
      "OVERWRITE must not delete non-pack custom purpose mappings"
    );

    const metadataAfterOverwrite = await getPolicyPackMetadataRows(
      tenantId,
      fixture.legalEntityId
    );
    assert(
      metadataAfterOverwrite.length === 3,
      "Metadata history should append third row after OVERWRITE"
    );

    console.log("test-policy-pack-apply: OK");
  } finally {
    await stopServerProcess(server);
    await closePool();
  }
}

main().catch((error) => {
  console.error("test-policy-pack-apply: FAILED");
  console.error(error);
  process.exit(1);
});

