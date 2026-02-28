import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CONTRACTS_PR21_TEST_PORT || 3121);
const BASE_URL =
  process.env.CONTRACTS_PR21_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const AMOUNT_EPSILON = 0.000001;

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function amountsEqual(left, right, epsilon = AMOUNT_EPSILON) {
  return Math.abs(Number(left || 0) - Number(right || 0)) <= epsilon;
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
      // keep polling
    }
    await sleep(300);
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
  assert(Boolean(response.cookie), `Login cookie missing for ${email}`);
  return response.cookie;
}

async function createTenantAndAdmin() {
  const stamp = Date.now();
  const tenantCode = `CTR_PR21_T_${stamp}`;
  const tenantName = `Contracts PR21 Tenant ${stamp}`;
  const email = `contracts_pr21_admin_${stamp}@example.com`;
  const password = "Contracts#12345";
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
  const tenantId = toNumber(tenantResult.rows?.[0]?.id);
  assert(tenantId > 0, "Failed to create tenant");

  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, email, passwordHash, "Contracts PR21 Admin"]
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
  assert(userId > 0, "Failed to create user");

  const roleResult = await query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = 'TenantAdmin'
     LIMIT 1`,
    [tenantId]
  );
  const roleId = toNumber(roleResult.rows?.[0]?.id);
  assert(roleId > 0, "TenantAdmin role not found");

  await query(
    `INSERT INTO user_role_scopes (tenant_id, user_id, role_id, scope_type, scope_id, effect)
     VALUES (?, ?, ?, 'TENANT', ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE
       effect = VALUES(effect)`,
    [tenantId, userId, roleId, tenantId]
  );

  return { stamp, tenantId, userId, email, password };
}

async function createFixture({ tenantId, stamp }) {
  const countryResult = await query(
    `SELECT id
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(countryResult.rows?.[0]?.id);
  assert(countryId > 0, "US country seed row missing");

  const groupCompanyResult = await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `CTR_PR21_G_${stamp}`, `Contracts PR21 Group ${stamp}`]
  );
  const groupCompanyId = toNumber(groupCompanyResult.rows?.insertId);
  assert(groupCompanyId > 0, "Failed to create group company");

  const legalEntityResult = await query(
    `INSERT INTO legal_entities (
        tenant_id,
        group_company_id,
        code,
        name,
        country_id,
        functional_currency_code,
        status
     )
     VALUES (?, ?, ?, ?, ?, 'USD', 'ACTIVE')`,
    [
      tenantId,
      groupCompanyId,
      `CTR_PR21_LE_${stamp}`,
      `Contracts PR21 Legal Entity ${stamp}`,
      countryId,
    ]
  );
  const legalEntityId = toNumber(legalEntityResult.rows?.insertId);
  assert(legalEntityId > 0, "Failed to create legal entity");

  const coaResult = await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `CTR_PR21_COA_${stamp}`, `Contracts PR21 CoA ${stamp}`]
  );
  const coaId = toNumber(coaResult.rows?.insertId);
  assert(coaId > 0, "Failed to create chart of accounts");

  async function createAccount(codeSuffix, accountType, normalSide) {
    const result = await query(
      `INSERT INTO accounts (
          coa_id,
          code,
          name,
          account_type,
          normal_side,
          allow_posting,
          is_active
       )
       VALUES (?, ?, ?, ?, ?, TRUE, TRUE)`,
      [
        coaId,
        `${codeSuffix}_${stamp}`,
        `${accountType} ${codeSuffix} ${stamp}`,
        accountType,
        normalSide,
      ]
    );
    return toNumber(result.rows?.insertId);
  }

  const deferredAccountId = await createAccount("2200", "LIABILITY", "CREDIT");
  const revenueAccountId = await createAccount("4000", "REVENUE", "CREDIT");
  assert(deferredAccountId > 0, "Failed to create deferred account");
  assert(revenueAccountId > 0, "Failed to create revenue account");

  const counterpartyResult = await query(
    `INSERT INTO counterparties (
        tenant_id,
        legal_entity_id,
        code,
        name,
        is_customer,
        is_vendor,
        status
     )
     VALUES (?, ?, ?, ?, TRUE, FALSE, 'ACTIVE')`,
    [tenantId, legalEntityId, `CTR_PR21_CP_${stamp}`, `Customer ${stamp}`]
  );
  const counterpartyId = toNumber(counterpartyResult.rows?.insertId);
  assert(counterpartyId > 0, "Failed to create counterparty");

  return {
    legalEntityId,
    counterpartyId,
    deferredAccountId,
    revenueAccountId,
  };
}

async function createDraftContract({ token, fixture, contractNo }) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/contracts",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.counterpartyId,
      contractNo,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      endDate: "2026-12-31",
      notes: "PR21 initial draft",
      lines: [
        {
          description: "Initial line A",
          lineAmountTxn: 100,
          lineAmountBase: 100,
          recognitionMethod: "STRAIGHT_LINE",
          recognitionStartDate: "2026-01-01",
          recognitionEndDate: "2026-12-31",
          deferredAccountId: fixture.deferredAccountId,
          revenueAccountId: fixture.revenueAccountId,
          status: "ACTIVE",
        },
        {
          description: "Initial line B",
          lineAmountTxn: 20,
          lineAmountBase: 20,
          recognitionMethod: "MANUAL",
          status: "INACTIVE",
        },
      ],
    },
  });
  const contractId = toNumber(response.json?.row?.id);
  assert(contractId > 0, "Contract create did not return row.id");
  return contractId;
}

async function runApiAssertions({ token, fixture, stamp }) {
  const contractId = await createDraftContract({
    token,
    fixture,
    contractNo: `CTR-PR21-${stamp}`,
  });

  const signedCreateResponse = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/contracts",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.counterpartyId,
      contractNo: `CTR-PR21-${stamp}-SIGNED`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      endDate: "2026-12-31",
      notes: "PR22 signed line support",
      lines: [
        {
          description: "Gross line",
          lineAmountTxn: 100,
          lineAmountBase: 100,
          recognitionMethod: "MANUAL",
          status: "ACTIVE",
        },
        {
          description: "Credit line",
          lineAmountTxn: -30,
          lineAmountBase: -30,
          recognitionMethod: "MANUAL",
          status: "ACTIVE",
        },
      ],
    },
  });
  const signedContractId = toNumber(signedCreateResponse.json?.row?.id);
  assert(signedContractId > 0, "Signed contract create should return row.id");
  assert(
    amountsEqual(signedCreateResponse.json?.row?.totalAmountTxn, 70),
    "Signed create totalAmountTxn should net to 70"
  );
  assert(
    amountsEqual(signedCreateResponse.json?.row?.totalAmountBase, 70),
    "Signed create totalAmountBase should net to 70"
  );

  const signedCreateZeroFail = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/contracts",
    expectedStatus: 400,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.counterpartyId,
      contractNo: `CTR-PR21-${stamp}-ZERO`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      lines: [
        {
          description: "Zero line should fail",
          lineAmountTxn: 0,
          lineAmountBase: 0,
          recognitionMethod: "MANUAL",
          status: "ACTIVE",
        },
      ],
    },
  });
  const zeroErrorMessage = String(signedCreateZeroFail.json?.message || "");
  assert(
    zeroErrorMessage.includes("must be non-zero"),
    "Zero line create should fail with non-zero validation message"
  );

  const activateResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractId}/activate`,
    expectedStatus: 200,
  });
  assert(activateResponse.json?.row?.status === "ACTIVE", "Contract should activate");
  const activatedVersion = toNumber(activateResponse.json?.row?.versionNo);
  assert(activatedVersion >= 1, "Activated contract should expose versionNo");

  await apiRequest({
    token,
    method: "PUT",
    path: `/api/v1/contracts/${contractId}`,
    expectedStatus: 400,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.counterpartyId,
      contractNo: `CTR-PR21-${stamp}-PUT`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      endDate: "2026-12-31",
      lines: [
        {
          description: "Should fail on ACTIVE",
          lineAmountTxn: 1,
          lineAmountBase: 1,
          recognitionMethod: "MANUAL",
        },
      ],
    },
  });

  const amendResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractId}/amend`,
    expectedStatus: 200,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.counterpartyId,
      contractNo: `CTR-PR21-${stamp}-AMD1`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      endDate: "2026-12-31",
      notes: "PR21 amendment full replace",
      reason: "Commercial terms revision",
      lines: [
        {
          description: "Amend line A",
          lineAmountTxn: 75,
          lineAmountBase: 75,
          recognitionMethod: "STRAIGHT_LINE",
          recognitionStartDate: "2026-01-01",
          recognitionEndDate: "2026-12-31",
          deferredAccountId: fixture.deferredAccountId,
          revenueAccountId: fixture.revenueAccountId,
          status: "ACTIVE",
        },
        {
          description: "Amend line B",
          lineAmountTxn: 25,
          lineAmountBase: 25,
          recognitionMethod: "MANUAL",
          status: "ACTIVE",
        },
      ],
    },
  });
  const amendedVersion = toNumber(amendResponse.json?.row?.versionNo);
  assert(amendedVersion > activatedVersion, "Amendment should increment versionNo");
  assert(
    amendResponse.json?.row?.contractNo === `CTR-PR21-${stamp}-AMD1`,
    "Amendment should update contractNo"
  );

  const detailAfterAmend = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/contracts/${contractId}`,
    expectedStatus: 200,
  });
  const amendedLines = detailAfterAmend.json?.row?.lines || [];
  assert(amendedLines.length === 2, "Amended contract should have two lines");
  const lineA = amendedLines.find((line) => String(line?.description || "").includes("line A"));
  assert(Boolean(lineA?.id), "Amended line A should have persisted id");
  assert(amountsEqual(detailAfterAmend.json?.row?.totalAmountTxn, 100), "Amended total should be 100");

  const invalidMilestonePatch = await apiRequest({
    token,
    method: "PATCH",
    path: `/api/v1/contracts/${contractId}/lines/${lineA.id}`,
    expectedStatus: 400,
    body: {
      recognitionMethod: "MILESTONE",
      recognitionStartDate: "2026-01-01",
      recognitionEndDate: "2026-06-01",
      reason: "Invalid milestone schedule window",
    },
  });
  assert(
    String(invalidMilestonePatch.json?.message || "").includes("must match for MILESTONE"),
    "Line patch should enforce MILESTONE same-date semantics"
  );

  const invalidManualPatch = await apiRequest({
    token,
    method: "PATCH",
    path: `/api/v1/contracts/${contractId}/lines/${lineA.id}`,
    expectedStatus: 400,
    body: {
      recognitionMethod: "MANUAL",
      reason: "Invalid manual dates carry-over",
    },
  });
  assert(
    String(invalidManualPatch.json?.message || "").includes("must be omitted for MANUAL"),
    "Line patch should enforce MANUAL date-free semantics"
  );

  const amendmentsAfterFullReplace = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/contracts/${contractId}/amendments`,
    expectedStatus: 200,
  });
  const fullReplaceRow = (amendmentsAfterFullReplace.json?.rows || []).find(
    (row) => row?.amendmentType === "FULL_REPLACE"
  );
  assert(Boolean(fullReplaceRow), "Amendment history must include FULL_REPLACE row");
  assert(
    toNumber(fullReplaceRow?.versionNo) === amendedVersion,
    "FULL_REPLACE versionNo mismatch"
  );

  const linePatchResponse = await apiRequest({
    token,
    method: "PATCH",
    path: `/api/v1/contracts/${contractId}/lines/${lineA.id}`,
    expectedStatus: 200,
    body: {
      lineAmountTxn: 55,
      lineAmountBase: 55,
      reason: "Partial line amount correction",
    },
  });
  const patchedVersion = toNumber(linePatchResponse.json?.row?.versionNo);
  assert(patchedVersion > amendedVersion, "Line patch should increment versionNo");
  assert(
    amountsEqual(linePatchResponse.json?.line?.lineAmountTxn, 55),
    "Line patch should set lineAmountTxn=55"
  );
  assert(
    amountsEqual(linePatchResponse.json?.line?.lineAmountBase, 55),
    "Line patch should set lineAmountBase=55"
  );

  const detailAfterPatch = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/contracts/${contractId}`,
    expectedStatus: 200,
  });
  assert(
    amountsEqual(detailAfterPatch.json?.row?.totalAmountTxn, 80),
    "Patched contract totalAmountTxn should recalc to 80"
  );
  assert(
    amountsEqual(detailAfterPatch.json?.row?.totalAmountBase, 80),
    "Patched contract totalAmountBase should recalc to 80"
  );

  const negativePatchResponse = await apiRequest({
    token,
    method: "PATCH",
    path: `/api/v1/contracts/${contractId}/lines/${lineA.id}`,
    expectedStatus: 200,
    body: {
      lineAmountTxn: -5,
      lineAmountBase: -5,
      reason: "Credit correction line",
    },
  });
  const negativePatchedVersion = toNumber(negativePatchResponse.json?.row?.versionNo);
  assert(
    negativePatchedVersion > patchedVersion,
    "Negative line patch should increment versionNo again"
  );
  assert(
    amountsEqual(negativePatchResponse.json?.line?.lineAmountTxn, -5),
    "Negative line patch should preserve signed txn amount"
  );
  assert(
    amountsEqual(negativePatchResponse.json?.line?.lineAmountBase, -5),
    "Negative line patch should preserve signed base amount"
  );

  const detailAfterNegativePatch = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/contracts/${contractId}`,
    expectedStatus: 200,
  });
  assert(
    amountsEqual(detailAfterNegativePatch.json?.row?.totalAmountTxn, 20),
    "Signed totals should recalc to 20 after negative patch"
  );
  assert(
    amountsEqual(detailAfterNegativePatch.json?.row?.totalAmountBase, 20),
    "Signed base totals should recalc to 20 after negative patch"
  );

  await apiRequest({
    token,
    method: "PATCH",
    path: `/api/v1/contracts/${contractId}/lines/${lineA.id}`,
    expectedStatus: 400,
    body: {
      lineAmountTxn: 0,
      lineAmountBase: 0,
      reason: "Zero amount should fail",
    },
  });

  const amendmentsAfterLinePatch = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/contracts/${contractId}/amendments`,
    expectedStatus: 200,
  });
  const linePatchRow = (amendmentsAfterLinePatch.json?.rows || []).find(
    (row) => row?.amendmentType === "LINE_PATCH"
  );
  assert(Boolean(linePatchRow), "Amendment history must include LINE_PATCH row");
  assert(
    toNumber(linePatchRow?.versionNo) === negativePatchedVersion,
    "LINE_PATCH versionNo mismatch"
  );

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractId}/close`,
    expectedStatus: 200,
  });

  await apiRequest({
    token,
    method: "PATCH",
    path: `/api/v1/contracts/${contractId}/lines/${lineA.id}`,
    expectedStatus: 400,
    body: {
      lineAmountTxn: 20,
      lineAmountBase: 20,
      reason: "Should fail for closed contract",
    },
  });

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractId}/amend`,
    expectedStatus: 400,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.counterpartyId,
      contractNo: `CTR-PR21-${stamp}-AMD2`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      endDate: "2026-12-31",
      reason: "Should fail for closed contract",
      lines: [
        {
          description: "Closed fail line",
          lineAmountTxn: 1,
          lineAmountBase: 1,
          recognitionMethod: "MANUAL",
        },
      ],
    },
  });
}

async function main() {
  await runMigrations();
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const identity = await createTenantAndAdmin();
  const fixture = await createFixture(identity);

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const token = await login(identity.email, identity.password);
    await runApiAssertions({
      token,
      fixture,
      stamp: identity.stamp,
    });

    console.log(
      "Contracts PR-21/PR-22/PR-24 amendment/versioning + signed line patch test passed."
    );
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          legalEntityId: fixture.legalEntityId,
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
    console.error("Contracts PR-21 test failed:", err);
    try {
      await closePool();
    } catch {
      // ignore close failures
    }
    process.exit(1);
  });
