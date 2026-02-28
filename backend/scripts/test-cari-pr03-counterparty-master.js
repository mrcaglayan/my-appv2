import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CARI_PR03_TEST_PORT || 3122);
const BASE_URL =
  process.env.CARI_PR03_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "CariPR03#12345";

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

async function createOrgFixtures({ tenantId, stamp }) {
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
    [tenantId, `CARI03GC${stamp}`, `CARI03 Group ${stamp}`]
  );
  const groupResult = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI03GC${stamp}`]
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
     VALUES
       (?, ?, ?, ?, ?, ?, 'ACTIVE'),
       (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      groupId,
      `CARI03LEA${stamp}`,
      `CARI03 LE A ${stamp}`,
      countryId,
      currencyCode,
      tenantId,
      groupId,
      `CARI03LEB${stamp}`,
      `CARI03 LE B ${stamp}`,
      countryId,
      currencyCode,
    ]
  );

  const legalEntityResult = await query(
    `SELECT id, code
     FROM legal_entities
     WHERE tenant_id = ?
       AND code IN (?, ?)
     ORDER BY code`,
    [tenantId, `CARI03LEA${stamp}`, `CARI03LEB${stamp}`]
  );
  const legalEntityIdByCode = new Map(
    legalEntityResult.rows.map((row) => [String(row.code), toNumber(row.id)])
  );
  const legalEntityAId = legalEntityIdByCode.get(`CARI03LEA${stamp}`);
  const legalEntityBId = legalEntityIdByCode.get(`CARI03LEB${stamp}`);
  assert(legalEntityAId > 0, "Legal entity A missing");
  assert(legalEntityBId > 0, "Legal entity B missing");

  await query(
    `INSERT INTO payment_terms (
        tenant_id,
        legal_entity_id,
        code,
        name,
        due_days,
        status
     )
     VALUES
       (?, ?, ?, ?, 30, 'ACTIVE'),
       (?, ?, ?, ?, 45, 'ACTIVE')`,
    [
      tenantId,
      legalEntityAId,
      `NET30A${stamp}`,
      `Net 30 A ${stamp}`,
      tenantId,
      legalEntityBId,
      `NET45B${stamp}`,
      `Net 45 B ${stamp}`,
    ]
  );

  const termRows = await query(
    `SELECT id, legal_entity_id
     FROM payment_terms
     WHERE tenant_id = ?
       AND code IN (?, ?)`,
    [tenantId, `NET30A${stamp}`, `NET45B${stamp}`]
  );
  let paymentTermAId = 0;
  let paymentTermBId = 0;
  for (const row of termRows.rows) {
    const entityId = toNumber(row.legal_entity_id);
    const id = toNumber(row.id);
    if (entityId === legalEntityAId) {
      paymentTermAId = id;
    } else if (entityId === legalEntityBId) {
      paymentTermBId = id;
    }
  }
  assert(paymentTermAId > 0, "Payment term A missing");
  assert(paymentTermBId > 0, "Payment term B missing");

  return {
    countryId,
    currencyCode,
    legalEntityAId,
    legalEntityBId,
    paymentTermAId,
    paymentTermBId,
  };
}

async function createUserWithRole({
  tenantId,
  roleCode,
  email,
  passwordHash,
  name,
  permissionScopeType = "TENANT",
  permissionScopeId = tenantId,
  dataScopes = [],
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
     VALUES (?, ?, ?, ?, ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE effect = VALUES(effect)`,
    [tenantId, userId, roleId, permissionScopeType, permissionScopeId]
  );

  for (const scope of dataScopes) {
    await query(
      `INSERT INTO data_scopes (
          tenant_id,
          user_id,
          scope_type,
          scope_id,
          effect,
          created_by_user_id
       )
       VALUES (?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         effect = VALUES(effect),
         created_by_user_id = VALUES(created_by_user_id)`,
      [
        tenantId,
        userId,
        String(scope.scopeType || "").toUpperCase(),
        toNumber(scope.scopeId),
        String(scope.effect || "ALLOW").toUpperCase(),
        userId,
      ]
    );
  }

  return { userId, email };
}

async function assignRoleScope({
  tenantId,
  userId,
  roleCode,
  scopeType = "TENANT",
  scopeId = tenantId,
  effect = "ALLOW",
}) {
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
     VALUES (?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE effect = VALUES(effect)`,
    [tenantId, userId, roleId, scopeType, scopeId, effect]
  );
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantAId = await createTenant(
    `CARI03A_${stamp}`,
    `CARI PR03 A ${stamp}`
  );
  const tenantBId = await createTenant(
    `CARI03B_${stamp}`,
    `CARI PR03 B ${stamp}`
  );

  await seedCore({ ensureDefaultTenantIfMissing: true });

  const fixtures = await createOrgFixtures({ tenantId: tenantAId, stamp });
  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);

  const tenantWideIdentity = await createUserWithRole({
    tenantId: tenantAId,
    roleCode: "EntityAccountant",
    email: `cari03_tenantwide_${stamp}@example.com`,
    passwordHash,
    name: "CARI03 Tenant Wide",
  });

  const leScopedIdentity = await createUserWithRole({
    tenantId: tenantAId,
    roleCode: "EntityAccountant",
    email: `cari03_scoped_${stamp}@example.com`,
    passwordHash,
    name: "CARI03 Scoped",
    dataScopes: [
      {
        scopeType: "LEGAL_ENTITY",
        scopeId: fixtures.legalEntityAId,
        effect: "ALLOW",
      },
    ],
  });

  const otherTenantIdentity = await createUserWithRole({
    tenantId: tenantBId,
    roleCode: "EntityAccountant",
    email: `cari03_other_tenant_${stamp}@example.com`,
    passwordHash,
    name: "CARI03 Other Tenant",
  });

  await assignRoleScope({
    tenantId: tenantAId,
    userId: tenantWideIdentity.userId,
    roleCode: "AuditorReadOnly",
  });
  await assignRoleScope({
    tenantId: tenantAId,
    userId: leScopedIdentity.userId,
    roleCode: "AuditorReadOnly",
  });

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();

    const tenantWideToken = await login(tenantWideIdentity.email, TEST_PASSWORD);
    const leScopedToken = await login(leScopedIdentity.email, TEST_PASSWORD);
    const otherTenantToken = await login(otherTenantIdentity.email, TEST_PASSWORD);

    const createCustomer = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      path: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        code: `CP03CUS${stamp}`,
        name: "Counterparty Customer",
        isCustomer: true,
        isVendor: false,
        status: "ACTIVE",
        defaultPaymentTermId: fixtures.paymentTermAId,
        contacts: [
          {
            contactName: "Alice Customer",
            email: "alice.customer@example.com",
            phone: "+12025550101",
            title: "AR Lead",
            isPrimary: true,
            status: "ACTIVE",
          },
        ],
        addresses: [
          {
            addressType: "BILLING",
            addressLine1: "111 Billing St",
            city: "New York",
            stateRegion: "NY",
            postalCode: "10001",
            countryId: fixtures.countryId,
            isPrimary: true,
            status: "ACTIVE",
          },
        ],
      },
      expectedStatus: 201,
    });
    const customerRow = createCustomer.json?.row || {};
    const customerId = toNumber(customerRow.id);
    assert(customerId > 0, "Customer counterparty should be created");
    assert(customerRow.isCustomer === true, "Customer row should be customer-only");
    assert(customerRow.isVendor === false, "Customer row should not be vendor");

    const createVendor = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      path: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        code: `CP03VEN${stamp}`,
        name: "Counterparty Vendor",
        isCustomer: false,
        isVendor: true,
        status: "ACTIVE",
      },
      expectedStatus: 201,
    });
    const vendorId = toNumber(createVendor.json?.row?.id);
    assert(vendorId > 0, "Vendor counterparty should be created");

    const createDual = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      path: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixtures.legalEntityBId,
        code: `CP03BOTH${stamp}`,
        name: "Counterparty Both",
        isCustomer: true,
        isVendor: true,
        status: "ACTIVE",
      },
      expectedStatus: 201,
    });
    const dualId = toNumber(createDual.json?.row?.id);
    assert(dualId > 0, "Dual-role counterparty should be created");

    await apiRequest({
      token: tenantWideToken,
      method: "POST",
      path: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        code: `CP03NONE${stamp}`,
        name: "Invalid None",
        isCustomer: false,
        isVendor: false,
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: tenantWideToken,
      method: "POST",
      path: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        code: `CP03CUS${stamp}`,
        name: "Duplicate Code",
        isCustomer: true,
        isVendor: false,
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: tenantWideToken,
      method: "POST",
      path: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        code: `CP03BADTERM${stamp}`,
        name: "Bad Term Scope",
        isCustomer: true,
        isVendor: false,
        defaultPaymentTermId: fixtures.paymentTermBId,
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: tenantWideToken,
      method: "POST",
      path: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        code: `CP03BADDEF${stamp}`,
        name: "Bad Default Contact",
        isCustomer: true,
        isVendor: false,
        defaultContactId: 999999,
      },
      expectedStatus: 400,
    });

    const scopedList = await apiRequest({
      token: leScopedToken,
      method: "GET",
      path: "/api/v1/cari/counterparties",
      expectedStatus: 200,
    });
    const scopedRows = Array.isArray(scopedList.json?.rows)
      ? scopedList.json.rows
      : [];
    assert(scopedRows.length >= 2, "Scoped user should list tenant legal-entity A rows");
    assert(
      scopedRows.every(
        (row) => toNumber(row.legalEntityId) === fixtures.legalEntityAId
      ),
      "Scoped user must only see permitted legal entity rows"
    );

    await apiRequest({
      token: leScopedToken,
      method: "GET",
      path: `/api/v1/cari/counterparties?legalEntityId=${fixtures.legalEntityBId}`,
      expectedStatus: 403,
    });

    const detail = await apiRequest({
      token: tenantWideToken,
      method: "GET",
      path: `/api/v1/cari/counterparties/${customerId}`,
      expectedStatus: 200,
    });
    const detailRow = detail.json?.row || {};
    assert(
      Array.isArray(detailRow.contacts) && detailRow.contacts.length === 1,
      "Counterparty detail should return contacts"
    );
    assert(
      Array.isArray(detailRow.addresses) && detailRow.addresses.length === 1,
      "Counterparty detail should return addresses"
    );
    assert(
      toNumber(detailRow.defaults?.paymentTermId) === fixtures.paymentTermAId,
      "Counterparty detail should return default payment term"
    );
    assert(
      toNumber(detailRow.defaults?.contactId) > 0 &&
        toNumber(detailRow.defaults?.addressId) > 0,
      "Counterparty detail should return default contact/address ids"
    );

    await apiRequest({
      token: tenantWideToken,
      method: "PUT",
      path: `/api/v1/cari/counterparties/${customerId}`,
      body: { status: "INACTIVE" },
      expectedStatus: 200,
    });

    await apiRequest({
      token: tenantWideToken,
      method: "PUT",
      path: `/api/v1/cari/counterparties/${customerId}`,
      body: { defaultContactId: 99999999 },
      expectedStatus: 400,
    });

    await apiRequest({
      token: tenantWideToken,
      method: "PUT",
      path: `/api/v1/cari/counterparties/${customerId}`,
      body: { defaultAddressId: 99999999 },
      expectedStatus: 400,
    });

    await apiRequest({
      token: leScopedToken,
      method: "GET",
      path: `/api/v1/cari/counterparties/${dualId}`,
      expectedStatus: 403,
    });
    await apiRequest({
      token: leScopedToken,
      method: "PUT",
      path: `/api/v1/cari/counterparties/${dualId}`,
      body: { status: "INACTIVE" },
      expectedStatus: 403,
    });

    await apiRequest({
      token: otherTenantToken,
      method: "GET",
      path: `/api/v1/cari/counterparties/${customerId}`,
      expectedStatus: 400,
    });
    await apiRequest({
      token: otherTenantToken,
      method: "PUT",
      path: `/api/v1/cari/counterparties/${customerId}`,
      body: { status: "ACTIVE" },
      expectedStatus: 400,
    });

    const auditResult = await query(
      `SELECT action, COUNT(*) AS row_count
       FROM audit_logs
       WHERE tenant_id = ?
         AND resource_type = 'counterparty'
         AND resource_id = ?
         AND action IN ('cari.counterparty.create', 'cari.counterparty.update')
       GROUP BY action`,
      [tenantAId, String(customerId)]
    );
    const auditCounts = new Map(
      auditResult.rows.map((row) => [String(row.action), toNumber(row.row_count)])
    );
    assert(
      auditCounts.get("cari.counterparty.create") >= 1,
      "Create audit log should be written for counterparty"
    );
    assert(
      auditCounts.get("cari.counterparty.update") >= 1,
      "Update audit log should be written for counterparty"
    );

    const auditApiResponse = await apiRequest({
      token: tenantWideToken,
      method: "GET",
      path:
        `/api/v1/cari/audit?` +
        `legalEntityId=${fixtures.legalEntityAId}` +
        `&resourceType=counterparty` +
        `&resourceId=${customerId}` +
        `&includePayload=true`,
      expectedStatus: 200,
    });
    const auditApiRows = Array.isArray(auditApiResponse.json?.rows)
      ? auditApiResponse.json.rows
      : [];
    assert(
      auditApiRows.some((row) => String(row.action) === "cari.counterparty.create"),
      "Audit API should return create event"
    );
    assert(
      auditApiRows.some((row) => String(row.action) === "cari.counterparty.update"),
      "Audit API should return update event"
    );

    const scopedAudit = await apiRequest({
      token: leScopedToken,
      method: "GET",
      path: "/api/v1/cari/audit?resourceType=counterparty&includePayload=false",
      expectedStatus: 200,
    });
    const scopedAuditRows = Array.isArray(scopedAudit.json?.rows)
      ? scopedAudit.json.rows
      : [];
    assert(
      scopedAuditRows.every(
        (row) => toNumber(row.scopeId) === fixtures.legalEntityAId
      ),
      "Scoped audit endpoint must only return rows from allowed legal entities"
    );

    await apiRequest({
      token: leScopedToken,
      method: "GET",
      path: `/api/v1/cari/audit?legalEntityId=${fixtures.legalEntityBId}`,
      expectedStatus: 403,
    });

    console.log("CARI PR-03 counterparty master API test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: tenantAId,
          checkedCounterpartyIds: [customerId, vendorId, dualId],
          scopedUserLegalEntityId: fixtures.legalEntityAId,
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
