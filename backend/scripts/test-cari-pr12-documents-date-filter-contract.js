import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CARI_PR12_FILTER_TEST_PORT || 3126);
const BASE_URL =
  process.env.CARI_PR12_FILTER_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "CariPR12#12345";

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
  requestPath,
  body,
  expectedStatus,
}) {
  const headers = { "Content-Type": "application/json" };
  if (token) {
    headers.Cookie = token;
  }

  const response = await fetch(`${BASE_URL}${requestPath}`, {
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
      `${method} ${requestPath} expected ${expectedStatus}, got ${
        response.status
      }. response=${JSON.stringify(json)}`
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
    requestPath: "/auth/login",
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

async function createFixtures({ tenantId, stamp }) {
  const countryResult = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(countryResult.rows?.[0]?.id);
  const currencyCode = String(
    countryResult.rows?.[0]?.default_currency_code || "USD"
  ).toUpperCase();
  assert(countryId > 0, "US country row is required");

  await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `CARI12GC${stamp}`, `CARI12 Group ${stamp}`]
  );
  const groupResult = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI12GC${stamp}`]
  );
  const groupId = toNumber(groupResult.rows?.[0]?.id);
  assert(groupId > 0, "Failed to create group company");

  await query(
    `INSERT INTO legal_entities (
        tenant_id,
        group_company_id,
        code,
        name,
        country_id,
        functional_currency_code,
        status
     )
     VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      groupId,
      `CARI12LE${stamp}`,
      `CARI12 Legal Entity ${stamp}`,
      countryId,
      currencyCode,
    ]
  );
  const legalEntityResult = await query(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI12LE${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityResult.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity");

  await query(
    `INSERT INTO payment_terms (
        tenant_id,
        legal_entity_id,
        code,
        name,
        due_days,
        grace_days,
        status
     )
     VALUES (?, ?, ?, ?, 30, 0, 'ACTIVE')`,
    [tenantId, legalEntityId, `CARI12TERM${stamp}`, `CARI12 Term ${stamp}`]
  );
  const paymentTermResult = await query(
    `SELECT id
     FROM payment_terms
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, `CARI12TERM${stamp}`]
  );
  const paymentTermId = toNumber(paymentTermResult.rows?.[0]?.id);
  assert(paymentTermId > 0, "Failed to create payment term");

  await query(
    `INSERT INTO counterparties (
        tenant_id,
        legal_entity_id,
        code,
        name,
        is_customer,
        is_vendor,
        default_currency_code,
        default_payment_term_id,
        status
     )
     VALUES (?, ?, ?, ?, TRUE, TRUE, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      legalEntityId,
      `CARI12CP${stamp}`,
      `CARI12 Counterparty ${stamp}`,
      currencyCode,
      paymentTermId,
    ]
  );
  const counterpartyResult = await query(
    `SELECT id
     FROM counterparties
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, `CARI12CP${stamp}`]
  );
  const counterpartyId = toNumber(counterpartyResult.rows?.[0]?.id);
  assert(counterpartyId > 0, "Failed to create counterparty");

  return {
    legalEntityId,
    counterpartyId,
    paymentTermId,
    currencyCode,
  };
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

function collectIds(responseJson) {
  const rows = Array.isArray(responseJson?.rows) ? responseJson.rows : [];
  return rows.map((row) => toNumber(row?.id)).filter((id) => id > 0);
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantId = await createTenant(`CARI12_${stamp}`, `CARI PR12 ${stamp}`);
  await seedCore({ ensureDefaultTenantIfMissing: true });
  const fixtures = await createFixtures({ tenantId, stamp });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const identity = await createUserWithRole({
    tenantId,
    roleCode: "EntityAccountant",
    email: `cari12_${stamp}@example.com`,
    passwordHash,
    name: "CARI12 Date Filter",
  });

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const token = await login(identity.email, TEST_PASSWORD);

    async function createDoc(documentDate) {
      const response = await apiRequest({
        token,
        method: "POST",
        requestPath: "/api/v1/cari/documents",
        body: {
          legalEntityId: fixtures.legalEntityId,
          counterpartyId: fixtures.counterpartyId,
          paymentTermId: fixtures.paymentTermId,
          direction: "AP",
          documentType: "PAYMENT",
          documentDate,
          amountTxn: 100,
          amountBase: 100,
          currencyCode: fixtures.currencyCode,
          fxRate: 1,
        },
        expectedStatus: 201,
      });
      return toNumber(response.json?.row?.id);
    }

    const docA = await createDoc("2026-01-05");
    const docB = await createDoc("2026-01-15");
    const docC = await createDoc("2026-02-02");
    assert(docA > 0 && docB > 0 && docC > 0, "failed to create date-filter fixtures");

    const primaryRange = await apiRequest({
      token,
      method: "GET",
      requestPath: "/api/v1/cari/documents?dateFrom=2026-01-10&dateTo=2026-01-31",
      expectedStatus: 200,
    });
    const aliasRange = await apiRequest({
      token,
      method: "GET",
      requestPath:
        "/api/v1/cari/documents?documentDateFrom=2026-01-10&documentDateTo=2026-01-31",
      expectedStatus: 200,
    });
    const mixedRange = await apiRequest({
      token,
      method: "GET",
      requestPath: "/api/v1/cari/documents?dateFrom=2026-01-10&documentDateTo=2026-01-31",
      expectedStatus: 200,
    });

    const primaryIds = collectIds(primaryRange.json);
    const aliasIds = collectIds(aliasRange.json);
    const mixedIds = collectIds(mixedRange.json);
    assert(
      primaryIds.includes(docB) && !primaryIds.includes(docA) && !primaryIds.includes(docC),
      "primary dateFrom/dateTo must filter by document_date"
    );
    assert(
      JSON.stringify(primaryIds) === JSON.stringify(aliasIds),
      "alias documentDateFrom/documentDateTo must normalize to same output"
    );
    assert(
      JSON.stringify(primaryIds) === JSON.stringify(mixedIds),
      "mixed primary+alias date params must normalize to same output"
    );

    const fromOnly = await apiRequest({
      token,
      method: "GET",
      requestPath: "/api/v1/cari/documents?dateFrom=2026-01-15",
      expectedStatus: 200,
    });
    const fromOnlyIds = collectIds(fromOnly.json);
    assert(
      fromOnlyIds.includes(docB) && fromOnlyIds.includes(docC) && !fromOnlyIds.includes(docA),
      "dateFrom-only filter must apply lower bound"
    );

    const toOnly = await apiRequest({
      token,
      method: "GET",
      requestPath: "/api/v1/cari/documents?dateTo=2026-01-31",
      expectedStatus: 200,
    });
    const toOnlyIds = collectIds(toOnly.json);
    assert(
      toOnlyIds.includes(docA) && toOnlyIds.includes(docB) && !toOnlyIds.includes(docC),
      "dateTo-only filter must apply upper bound"
    );

    await apiRequest({
      token,
      method: "GET",
      requestPath: "/api/v1/cari/documents?dateFrom=2026-02-01&dateTo=2026-01-01",
      expectedStatus: 400,
    });

    console.log("PR-12 documents date-filter contract passed.");
    console.log(
      JSON.stringify(
        {
          tenantId,
          documentIds: [docA, docB, docC],
          primaryIds,
          aliasIds,
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
