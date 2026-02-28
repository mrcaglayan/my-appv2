import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CARI_PAYMENT_TERMS_BOOTSTRAP_TEST_PORT || 3130);
const BASE_URL =
  process.env.CARI_PAYMENT_TERMS_BOOTSTRAP_TEST_BASE_URL ||
  `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "CariPayTermBootstrap#123";
const DEFAULT_TERM_COUNT = 5;

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
    await sleep(350);
  }
  throw new Error(`Server did not start within ${SERVER_START_TIMEOUT_MS}ms`);
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

  const roleResult = await query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, roleCode]
  );
  const roleId = toNumber(roleResult.rows?.[0]?.id);
  assert(roleId > 0, `Role not found: ${roleCode}`);

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

async function getTenantLegalEntityIds(tenantId) {
  const result = await query(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
     ORDER BY id`,
    [tenantId]
  );
  return result.rows.map((row) => toNumber(row.id)).filter((id) => id > 0);
}

async function getPaymentTermCount(tenantId, legalEntityId) {
  const result = await query(
    `SELECT COUNT(*) AS row_count
     FROM payment_terms
     WHERE tenant_id = ?
       AND legal_entity_id = ?`,
    [tenantId, legalEntityId]
  );
  return toNumber(result.rows?.[0]?.row_count);
}

async function getAnyGroupCompanyId(tenantId) {
  const result = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
     ORDER BY id
     LIMIT 1`,
    [tenantId]
  );
  return toNumber(result.rows?.[0]?.id);
}

async function getCountryIdByIso2(iso2) {
  const result = await query(
    `SELECT id
     FROM countries
     WHERE iso2 = ?
     LIMIT 1`,
    [String(iso2 || "").trim().toUpperCase()]
  );
  return toNumber(result.rows?.[0]?.id);
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantAId = await createTenant(
    `CARI_PTERM_BOOT_A_${stamp}`,
    `CARI PaymentTerm Bootstrap A ${stamp}`
  );
  const tenantBId = await createTenant(
    `CARI_PTERM_BOOT_B_${stamp}`,
    `CARI PaymentTerm Bootstrap B ${stamp}`
  );
  const tenantCId = await createTenant(
    `CARI_PTERM_BOOT_C_${stamp}`,
    `CARI PaymentTerm Bootstrap C ${stamp}`
  );
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const adminA = await createUserWithRole({
    tenantId: tenantAId,
    roleCode: "TenantAdmin",
    email: `cari_pterm_admin_a_${stamp}@example.com`,
    passwordHash,
    name: "Cari Payment Term Bootstrap Admin A",
  });
  const adminB = await createUserWithRole({
    tenantId: tenantBId,
    roleCode: "TenantAdmin",
    email: `cari_pterm_admin_b_${stamp}@example.com`,
    passwordHash,
    name: "Cari Payment Term Bootstrap Admin B",
  });
  const adminC = await createUserWithRole({
    tenantId: tenantCId,
    roleCode: "TenantAdmin",
    email: `cari_pterm_admin_c_${stamp}@example.com`,
    passwordHash,
    name: "Cari Payment Term Bootstrap Admin C",
  });
  const noSetupUser = await createUserWithRole({
    tenantId: tenantAId,
    roleCode: "EntityAccountant",
    email: `cari_pterm_nosetup_${stamp}@example.com`,
    passwordHash,
    name: "Cari Payment Term Bootstrap No Setup",
  });

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();

    const adminAToken = await login(adminA.email, TEST_PASSWORD);
    const adminBToken = await login(adminB.email, TEST_PASSWORD);
    const adminCToken = await login(adminC.email, TEST_PASSWORD);
    const noSetupToken = await login(noSetupUser.email, TEST_PASSWORD);

    await apiRequest({
      token: adminAToken,
      method: "POST",
      path: "/api/v1/onboarding/readiness/bootstrap-baseline",
      body: { fiscalYear: 2026 },
      expectedStatus: 201,
    });
    await apiRequest({
      token: adminBToken,
      method: "POST",
      path: "/api/v1/onboarding/readiness/bootstrap-baseline",
      body: { fiscalYear: 2026 },
      expectedStatus: 201,
    });

    const groupCompanyIdB = await getAnyGroupCompanyId(tenantBId);
    const usCountryId = await getCountryIdByIso2("US");
    assert(groupCompanyIdB > 0, "Tenant B should have a group company");
    assert(usCountryId > 0, "US country id should be available");

    const orgDefaultCode = `ORG_LE_DEF_${stamp}`;
    const orgDefaultCreate = await apiRequest({
      token: adminBToken,
      method: "POST",
      path: "/api/v1/org/legal-entities",
      body: {
        groupCompanyId: groupCompanyIdB,
        code: orgDefaultCode,
        name: `ORG LE Default ${stamp}`,
        countryId: usCountryId,
        functionalCurrencyCode: "USD",
        autoProvisionDefaults: true,
      },
      expectedStatus: 201,
    });
    const orgDefaultEntityId = toNumber(orgDefaultCreate.json?.legalEntityId);
    assert(orgDefaultEntityId > 0, "Org legal-entity upsert should create legalEntityId");
    assert(
      toNumber(orgDefaultCreate.json?.paymentTermsProvisioning?.createdCount) ===
        DEFAULT_TERM_COUNT,
      "Org legal-entity upsert with autoProvisionDefaults should seed default payment terms"
    );
    const orgDefaultEntityTermCount = await getPaymentTermCount(
      tenantBId,
      orgDefaultEntityId
    );
    assert(
      orgDefaultEntityTermCount === DEFAULT_TERM_COUNT,
      "Org legal-entity upsert should persist default payment terms for new entity"
    );

    const orgCustomCode = `ORG_LE_CUSTOM_${stamp}`;
    const orgCustomTermCode = `ORG_NET75_${stamp}`;
    const orgCustomCreate = await apiRequest({
      token: adminBToken,
      method: "POST",
      path: "/api/v1/org/legal-entities",
      body: {
        groupCompanyId: groupCompanyIdB,
        code: orgCustomCode,
        name: `ORG LE Custom ${stamp}`,
        countryId: usCountryId,
        functionalCurrencyCode: "USD",
        autoProvisionDefaults: false,
        paymentTerms: [
          {
            code: orgCustomTermCode,
            name: `Org Net 75 ${stamp}`,
            dueDays: 75,
            graceDays: 5,
            status: "ACTIVE",
          },
          {
            code: `ORG_EOM_${stamp}`,
            name: `Org EOM ${stamp}`,
            dueDays: 0,
            graceDays: 0,
            isEndOfMonth: true,
            status: "ACTIVE",
          },
        ],
      },
      expectedStatus: 201,
    });
    const orgCustomEntityId = toNumber(orgCustomCreate.json?.legalEntityId);
    assert(orgCustomEntityId > 0, "Org custom legal entity should be created");
    assert(
      orgCustomCreate.json?.paymentTermsProvisioning?.defaultsUsed === false,
      "Org custom legal-entity upsert should report custom payment terms"
    );
    assert(
      toNumber(orgCustomCreate.json?.paymentTermsProvisioning?.createdCount) === 2,
      "Org custom legal-entity upsert should create custom payment terms"
    );
    const orgCustomEntityTermCount = await getPaymentTermCount(
      tenantBId,
      orgCustomEntityId
    );
    assert(
      orgCustomEntityTermCount === 2,
      "Org custom legal-entity upsert should persist custom payment terms only"
    );

    const orgCustomReplay = await apiRequest({
      token: adminBToken,
      method: "POST",
      path: "/api/v1/org/legal-entities",
      body: {
        groupCompanyId: groupCompanyIdB,
        code: orgCustomCode,
        name: `ORG LE Custom ${stamp}`,
        countryId: usCountryId,
        functionalCurrencyCode: "USD",
        autoProvisionDefaults: false,
        paymentTerms: [
          {
            code: orgCustomTermCode,
            name: `Org Net 75 ${stamp}`,
            dueDays: 75,
            graceDays: 5,
            status: "ACTIVE",
          },
          {
            code: `ORG_EOM_${stamp}`,
            name: `Org EOM ${stamp}`,
            dueDays: 0,
            graceDays: 0,
            isEndOfMonth: true,
            status: "ACTIVE",
          },
        ],
      },
      expectedStatus: 201,
    });
    assert(
      toNumber(orgCustomReplay.json?.paymentTermsProvisioning?.createdCount) === 0,
      "Org custom legal-entity replay should be idempotent for payment terms"
    );
    assert(
      toNumber(orgCustomReplay.json?.paymentTermsProvisioning?.skippedCount) === 2,
      "Org custom legal-entity replay should skip existing payment terms"
    );

    const companyBootstrapPayload = {
      groupCompany: {
        code: `CARI_PTERM_GC_${stamp}`,
        name: `CARI PaymentTerm Group ${stamp}`,
      },
      fiscalCalendar: {
        code: "MAIN",
        name: "Main Calendar",
        yearStartMonth: 1,
        yearStartDay: 1,
      },
      fiscalYear: 2026,
      legalEntities: [
        {
          code: `CARI_PTERM_LE_${stamp}`,
          name: `CARI PaymentTerm Entity ${stamp}`,
          countryIso2: "US",
          functionalCurrencyCode: "USD",
        },
      ],
    };

    const companyBootstrap = await apiRequest({
      token: adminCToken,
      method: "POST",
      path: "/api/v1/onboarding/company-bootstrap",
      body: companyBootstrapPayload,
      expectedStatus: 201,
    });
    const companyBootstrapEntityId = toNumber(
      companyBootstrap.json?.legalEntities?.[0]?.legalEntityId
    );
    assert(
      companyBootstrapEntityId > 0,
      "Company bootstrap should return legalEntityId for created entity"
    );
    assert(
      toNumber(companyBootstrap.json?.paymentTerms?.templateCount) === DEFAULT_TERM_COUNT,
      "Company bootstrap response should report default payment-term template count"
    );
    assert(
      toNumber(companyBootstrap.json?.paymentTerms?.createdCount) === DEFAULT_TERM_COUNT,
      "Company bootstrap should auto-create default payment terms"
    );
    assert(
      toNumber(companyBootstrap.json?.paymentTerms?.skippedCount) === 0,
      "First company bootstrap run should not skip default payment terms"
    );
    const companyBootstrapTermCount = await getPaymentTermCount(
      tenantCId,
      companyBootstrapEntityId
    );
    assert(
      companyBootstrapTermCount === DEFAULT_TERM_COUNT,
      "Company bootstrap should persist default payment terms for the legal entity"
    );

    const companyBootstrapReplay = await apiRequest({
      token: adminCToken,
      method: "POST",
      path: "/api/v1/onboarding/company-bootstrap",
      body: companyBootstrapPayload,
      expectedStatus: 201,
    });
    assert(
      toNumber(companyBootstrapReplay.json?.paymentTerms?.createdCount) === 0,
      "Company bootstrap replay should not create duplicate payment terms"
    );
    assert(
      toNumber(companyBootstrapReplay.json?.paymentTerms?.skippedCount) >= DEFAULT_TERM_COUNT,
      "Company bootstrap replay should skip existing payment terms"
    );

    const legalEntityIdsA = await getTenantLegalEntityIds(tenantAId);
    const legalEntityIdsB = await getTenantLegalEntityIds(tenantBId);
    assert(legalEntityIdsA.length > 0, "Tenant A should have at least one legal entity");
    assert(legalEntityIdsB.length > 0, "Tenant B should have at least one legal entity");

    const initialCountA = await getPaymentTermCount(tenantAId, legalEntityIdsA[0]);
    assert(initialCountA === 0, "Baseline bootstrap should not pre-create payment terms");

    const defaultBootstrap = await apiRequest({
      token: adminAToken,
      method: "POST",
      path: "/api/v1/onboarding/payment-terms/bootstrap",
      expectedStatus: 201,
    });
    assert(defaultBootstrap.json?.ok === true, "Default payment-term bootstrap should return ok=true");
    assert(
      defaultBootstrap.json?.defaultsUsed === true,
      "Default payment-term bootstrap should report defaultsUsed=true"
    );
    assert(
      Array.isArray(defaultBootstrap.json?.legalEntityIds) &&
        defaultBootstrap.json.legalEntityIds.length === legalEntityIdsA.length,
      "Default payment-term bootstrap should target all tenant legal entities"
    );
    assert(
      toNumber(defaultBootstrap.json?.createdCount) ===
        DEFAULT_TERM_COUNT * legalEntityIdsA.length,
      "Default bootstrap should create expected number of payment terms"
    );
    assert(
      toNumber(defaultBootstrap.json?.skippedCount) === 0,
      "First bootstrap run should not skip terms"
    );

    for (const legalEntityId of legalEntityIdsA) {
      // eslint-disable-next-line no-await-in-loop
      const termCount = await getPaymentTermCount(tenantAId, legalEntityId);
      assert(
        termCount === DEFAULT_TERM_COUNT,
        `Legal entity ${legalEntityId} should have ${DEFAULT_TERM_COUNT} default terms after bootstrap`
      );
    }

    const idempotentBootstrap = await apiRequest({
      token: adminAToken,
      method: "POST",
      path: "/api/v1/onboarding/payment-terms/bootstrap",
      expectedStatus: 201,
    });
    assert(
      toNumber(idempotentBootstrap.json?.createdCount) === 0,
      "Second default bootstrap should not create duplicate terms"
    );
    assert(
      toNumber(idempotentBootstrap.json?.skippedCount) ===
        DEFAULT_TERM_COUNT * legalEntityIdsA.length,
      "Second default bootstrap should report skipped duplicates"
    );

    const customCode = `NET90_${stamp}`;
    const customBootstrap = await apiRequest({
      token: adminAToken,
      method: "POST",
      path: "/api/v1/onboarding/payment-terms/bootstrap",
      body: {
        legalEntityId: legalEntityIdsA[0],
        terms: [
          {
            code: customCode,
            name: `Net 90 ${stamp}`,
            dueDays: 90,
            graceDays: 3,
            isEndOfMonth: false,
            status: "ACTIVE",
          },
        ],
      },
      expectedStatus: 201,
    });
    assert(
      customBootstrap.json?.defaultsUsed === false,
      "Custom bootstrap should report defaultsUsed=false"
    );
    assert(
      toNumber(customBootstrap.json?.createdCount) === 1,
      "Custom bootstrap should create one custom term"
    );

    const customBootstrapReplay = await apiRequest({
      token: adminAToken,
      method: "POST",
      path: "/api/v1/onboarding/payment-terms/bootstrap",
      body: {
        legalEntityId: legalEntityIdsA[0],
        terms: [
          {
            code: customCode,
            name: `Net 90 ${stamp}`,
            dueDays: 90,
            graceDays: 3,
            isEndOfMonth: false,
            status: "ACTIVE",
          },
        ],
      },
      expectedStatus: 201,
    });
    assert(
      toNumber(customBootstrapReplay.json?.createdCount) === 0,
      "Custom bootstrap replay should be idempotent"
    );
    assert(
      toNumber(customBootstrapReplay.json?.skippedCount) === 1,
      "Custom bootstrap replay should skip the existing term"
    );

    const customTermRow = await query(
      `SELECT code, due_days, grace_days, status
       FROM payment_terms
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND code = ?
       LIMIT 1`,
      [tenantAId, legalEntityIdsA[0], customCode]
    );
    assert(customTermRow.rows?.[0], "Custom payment term should exist in DB");
    assert(
      toNumber(customTermRow.rows[0].due_days) === 90,
      "Custom payment term due days should be stored"
    );
    assert(
      toNumber(customTermRow.rows[0].grace_days) === 3,
      "Custom payment term grace days should be stored"
    );
    assert(
      String(customTermRow.rows[0].status || "").toUpperCase() === "ACTIVE",
      "Custom payment term status should be ACTIVE"
    );

    await apiRequest({
      token: noSetupToken,
      method: "POST",
      path: "/api/v1/onboarding/payment-terms/bootstrap",
      expectedStatus: 403,
    });

    await apiRequest({
      token: adminAToken,
      method: "POST",
      path: "/api/v1/onboarding/payment-terms/bootstrap",
      body: {
        legalEntityId: legalEntityIdsB[0],
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: adminAToken,
      method: "POST",
      path: "/api/v1/onboarding/payment-terms/bootstrap",
      body: {
        legalEntityId: legalEntityIdsA[0],
        terms: [
          { code: "DUP_CODE", name: "Dup A", dueDays: 10 },
          { code: "dup code", name: "Dup B", dueDays: 20 },
        ],
      },
      expectedStatus: 400,
    });

    const paymentTermLookup = await apiRequest({
      token: adminAToken,
      method: "GET",
      path: `/api/v1/cari/payment-terms?legalEntityId=${legalEntityIdsA[0]}`,
      expectedStatus: 200,
    });
    const termCodes = new Set(
      (paymentTermLookup.json?.rows || []).map((row) =>
        String(row.code || "").toUpperCase()
      )
    );
    assert(
      termCodes.has("NET_30"),
      "Payment-term lookup should include default NET_30 after bootstrap"
    );
    assert(
      termCodes.has(customCode.toUpperCase()),
      "Payment-term lookup should include custom term after bootstrap"
    );

    console.log("Cari payment-term onboarding bootstrap test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: tenantAId,
          legalEntityIds: legalEntityIdsA,
          createdDefaults: DEFAULT_TERM_COUNT * legalEntityIdsA.length,
          customCode,
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
  }
}

main()
  .then(async () => {
    await closePool();
    process.exit(0);
  })
  .catch(async (err) => {
    console.error(err);
    try {
      await closePool();
    } catch {
      // ignore close failures
    }
    process.exit(1);
  });
