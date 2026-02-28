import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CARI_PR05_TEST_PORT || 3124);
const BASE_URL =
  process.env.CARI_PR05_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "CariPR05#12345";

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
      `${method} ${requestPath} expected ${expectedStatus}, got ${response.status}. response=${JSON.stringify(
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
    [tenantId, `CARI05GC${stamp}`, `CARI05 Group ${stamp}`]
  );
  const groupResult = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI05GC${stamp}`]
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
      `CARI05LEA${stamp}`,
      `CARI05 LE A ${stamp}`,
      countryId,
      currencyCode,
      tenantId,
      groupId,
      `CARI05LEB${stamp}`,
      `CARI05 LE B ${stamp}`,
      countryId,
      currencyCode,
    ]
  );

  const legalEntityRows = await query(
    `SELECT id, code
     FROM legal_entities
     WHERE tenant_id = ?
       AND code IN (?, ?)
     ORDER BY code`,
    [tenantId, `CARI05LEA${stamp}`, `CARI05LEB${stamp}`]
  );
  const idByCode = new Map(
    legalEntityRows.rows.map((row) => [String(row.code), toNumber(row.id)])
  );
  const legalEntityAId = idByCode.get(`CARI05LEA${stamp}`);
  const legalEntityBId = idByCode.get(`CARI05LEB${stamp}`);
  assert(legalEntityAId > 0, "Legal entity A missing");
  assert(legalEntityBId > 0, "Legal entity B missing");

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
     VALUES
       (?, ?, ?, ?, 30, 0, 'ACTIVE'),
       (?, ?, ?, ?, 45, 0, 'ACTIVE')`,
    [
      tenantId,
      legalEntityAId,
      `CARI05TERM_A_${stamp}`,
      `CARI05 Term A ${stamp}`,
      tenantId,
      legalEntityBId,
      `CARI05TERM_B_${stamp}`,
      `CARI05 Term B ${stamp}`,
    ]
  );

  const paymentTermRows = await query(
    `SELECT id, legal_entity_id
     FROM payment_terms
     WHERE tenant_id = ?
       AND code IN (?, ?)`,
    [tenantId, `CARI05TERM_A_${stamp}`, `CARI05TERM_B_${stamp}`]
  );
  let paymentTermAId = 0;
  let paymentTermBId = 0;
  for (const row of paymentTermRows.rows) {
    const legalEntityId = toNumber(row.legal_entity_id);
    const id = toNumber(row.id);
    if (legalEntityId === legalEntityAId) {
      paymentTermAId = id;
    } else if (legalEntityId === legalEntityBId) {
      paymentTermBId = id;
    }
  }
  assert(paymentTermAId > 0, "Payment term A missing");
  assert(paymentTermBId > 0, "Payment term B missing");

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
     VALUES
       (?, ?, ?, ?, TRUE, FALSE, ?, ?, 'ACTIVE'),
       (?, ?, ?, ?, FALSE, TRUE, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      legalEntityAId,
      `CARI05CPA${stamp}`,
      `CARI05 Counterparty A ${stamp}`,
      currencyCode,
      paymentTermAId,
      tenantId,
      legalEntityBId,
      `CARI05CPB${stamp}`,
      `CARI05 Counterparty B ${stamp}`,
      currencyCode,
      paymentTermBId,
    ]
  );

  const counterpartyRows = await query(
    `SELECT id, legal_entity_id
     FROM counterparties
     WHERE tenant_id = ?
       AND code IN (?, ?)`,
    [tenantId, `CARI05CPA${stamp}`, `CARI05CPB${stamp}`]
  );
  let counterpartyAId = 0;
  let counterpartyBId = 0;
  for (const row of counterpartyRows.rows) {
    const legalEntityId = toNumber(row.legal_entity_id);
    const id = toNumber(row.id);
    if (legalEntityId === legalEntityAId) {
      counterpartyAId = id;
    } else if (legalEntityId === legalEntityBId) {
      counterpartyBId = id;
    }
  }
  assert(counterpartyAId > 0, "Counterparty A missing");
  assert(counterpartyBId > 0, "Counterparty B missing");

  return {
    countryId,
    currencyCode,
    legalEntityAId,
    legalEntityBId,
    paymentTermAId,
    paymentTermBId,
    counterpartyAId,
    counterpartyBId,
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

async function runFrontendSmokeChecks() {
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.resolve(scriptDir, "..", "..");
  const paymentApiPath = path.resolve(repoRoot, "frontend/src/api/cariPaymentTerms.js");
  const counterpartyPagePath = path.resolve(
    repoRoot,
    "frontend/src/pages/cari/CariCounterpartyPage.jsx"
  );
  const counterpartyFormPath = path.resolve(
    repoRoot,
    "frontend/src/pages/cari/CounterpartyForm.jsx"
  );

  const [paymentApiSource, counterpartyPageSource, counterpartyFormSource] =
    await Promise.all([
      readFile(paymentApiPath, "utf8"),
      readFile(counterpartyPagePath, "utf8"),
      readFile(counterpartyFormPath, "utf8"),
    ]);

  assert(
    paymentApiSource.includes("/api/v1/cari/payment-terms"),
    "Payment-term API module should call /api/v1/cari/payment-terms"
  );
  assert(
    counterpartyPageSource.includes("listCariPaymentTerms"),
    "Counterparty page should request payment terms for lookup data"
  );
  assert(
    counterpartyPageSource.includes("paymentTerms={createPaymentTerms}") &&
      counterpartyPageSource.includes("paymentTerms={editPaymentTerms}"),
    "Counterparty page should pass payment term options to create and edit forms"
  );
  assert(
    counterpartyFormSource.includes("Default Payment Term"),
    "Counterparty form should render payment term lookup label"
  );
  assert(
    counterpartyFormSource.includes("No default payment term"),
    "Counterparty form should support clearing default payment term"
  );
  assert(
    counterpartyFormSource.includes("Loading payment terms..."),
    "Counterparty form should show payment-term lookup loading state"
  );
  assert(
    !counterpartyFormSource.includes("Default Payment Term Id"),
    "Counterparty form should not use numeric-only default payment term input label"
  );
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantAId = await createTenant(`CARI05A_${stamp}`, `CARI PR05 A ${stamp}`);
  const tenantBId = await createTenant(`CARI05B_${stamp}`, `CARI PR05 B ${stamp}`);

  await seedCore({ ensureDefaultTenantIfMissing: true });
  const fixtures = await createOrgFixtures({ tenantId: tenantAId, stamp });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const tenantWideIdentity = await createUserWithRole({
    tenantId: tenantAId,
    roleCode: "EntityAccountant",
    email: `cari05_tenantwide_${stamp}@example.com`,
    passwordHash,
    name: "CARI05 Tenant Wide",
  });
  const scopedIdentity = await createUserWithRole({
    tenantId: tenantAId,
    roleCode: "EntityAccountant",
    email: `cari05_scoped_${stamp}@example.com`,
    passwordHash,
    name: "CARI05 Scoped",
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
    email: `cari05_other_${stamp}@example.com`,
    passwordHash,
    name: "CARI05 Other Tenant",
  });

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();

    const tenantWideToken = await login(tenantWideIdentity.email, TEST_PASSWORD);
    const scopedToken = await login(scopedIdentity.email, TEST_PASSWORD);
    const otherTenantToken = await login(otherTenantIdentity.email, TEST_PASSWORD);

    const paymentTermList = await apiRequest({
      token: tenantWideToken,
      method: "GET",
      requestPath: "/api/v1/cari/payment-terms",
      expectedStatus: 200,
    });
    const paymentTermRows = Array.isArray(paymentTermList.json?.rows)
      ? paymentTermList.json.rows
      : [];
    assert(paymentTermRows.length >= 2, "Payment term list should return rows");
    assert(
      paymentTermRows.every((row) => toNumber(row.tenantId) === tenantAId),
      "Payment term list must be tenant-safe"
    );

    const paymentTermListByLe = await apiRequest({
      token: tenantWideToken,
      method: "GET",
      requestPath: `/api/v1/cari/payment-terms?legalEntityId=${fixtures.legalEntityAId}`,
      expectedStatus: 200,
    });
    const paymentTermRowsByLe = Array.isArray(paymentTermListByLe.json?.rows)
      ? paymentTermListByLe.json.rows
      : [];
    assert(
      paymentTermRowsByLe.every(
        (row) => toNumber(row.legalEntityId) === fixtures.legalEntityAId
      ),
      "Payment term legalEntity filter should work"
    );

    const scopedTermList = await apiRequest({
      token: scopedToken,
      method: "GET",
      requestPath: "/api/v1/cari/payment-terms",
      expectedStatus: 200,
    });
    const scopedPaymentTerms = Array.isArray(scopedTermList.json?.rows)
      ? scopedTermList.json.rows
      : [];
    assert(scopedPaymentTerms.length >= 1, "Scoped user should see in-scope payment terms");
    assert(
      scopedPaymentTerms.every(
        (row) => toNumber(row.legalEntityId) === fixtures.legalEntityAId
      ),
      "Scoped user should only see legal-entity scoped payment terms"
    );

    await apiRequest({
      token: scopedToken,
      method: "GET",
      requestPath: `/api/v1/cari/payment-terms?legalEntityId=${fixtures.legalEntityBId}`,
      expectedStatus: 403,
    });

    await apiRequest({
      token: scopedToken,
      method: "GET",
      requestPath: `/api/v1/cari/payment-terms/${fixtures.paymentTermBId}`,
      expectedStatus: 403,
    });

    await apiRequest({
      token: otherTenantToken,
      method: "GET",
      requestPath: `/api/v1/cari/payment-terms/${fixtures.paymentTermAId}`,
      expectedStatus: 400,
    });

    const createDraftArInvoice = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: "/api/v1/cari/documents",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        counterpartyId: fixtures.counterpartyAId,
        paymentTermId: fixtures.paymentTermAId,
        direction: "AR",
        documentType: "INVOICE",
        documentDate: "2026-01-15",
        dueDate: "2026-02-14",
        amountTxn: 1250.5,
        amountBase: 1250.5,
        currencyCode: fixtures.currencyCode,
        fxRate: 1,
      },
      expectedStatus: 201,
    });
    const invoiceDraftId = toNumber(createDraftArInvoice.json?.row?.id);
    assert(invoiceDraftId > 0, "AR invoice draft should be created");
    assert(
      createDraftArInvoice.json?.row?.status === "DRAFT",
      "Created draft document should have DRAFT status"
    );
    assert(
      String(createDraftArInvoice.json?.row?.sequenceNamespace || "").toUpperCase() ===
        "DRAFT",
      "Draft document should use DRAFT sequence namespace"
    );

    const createDraftApPayment = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: "/api/v1/cari/documents",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        counterpartyId: fixtures.counterpartyAId,
        direction: "AP",
        documentType: "PAYMENT",
        documentDate: "2026-01-18",
        amountTxn: 300,
        amountBase: 300,
        currencyCode: fixtures.currencyCode,
        fxRate: 1,
      },
      expectedStatus: 201,
    });
    const paymentDraftId = toNumber(createDraftApPayment.json?.row?.id);
    assert(paymentDraftId > 0, "AP payment draft should be created");

    await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: "/api/v1/cari/documents",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        counterpartyId: fixtures.counterpartyAId,
        direction: "AR",
        documentType: "OPENING_BALANCE",
        documentDate: "2026-01-19",
        amountTxn: 100,
        amountBase: 100,
        currencyCode: fixtures.currencyCode,
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: "/api/v1/cari/documents",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        counterpartyId: fixtures.counterpartyAId,
        direction: "AR",
        documentType: "INVOICE",
        documentDate: "2026-01-20",
        amountTxn: 100,
        amountBase: 100,
        currencyCode: fixtures.currencyCode,
      },
      expectedStatus: 400,
    });

    await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: "/api/v1/cari/documents",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        counterpartyId: fixtures.counterpartyBId,
        paymentTermId: fixtures.paymentTermAId,
        direction: "AR",
        documentType: "INVOICE",
        documentDate: "2026-01-21",
        dueDate: "2026-02-20",
        amountTxn: 100,
        amountBase: 100,
        currencyCode: fixtures.currencyCode,
      },
      expectedStatus: 400,
    });

    const createLeBDraft = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: "/api/v1/cari/documents",
      body: {
        legalEntityId: fixtures.legalEntityBId,
        counterpartyId: fixtures.counterpartyBId,
        paymentTermId: fixtures.paymentTermBId,
        direction: "AR",
        documentType: "INVOICE",
        documentDate: "2026-01-22",
        dueDate: "2026-02-21",
        amountTxn: 800,
        amountBase: 800,
        currencyCode: fixtures.currencyCode,
        fxRate: 1,
      },
      expectedStatus: 201,
    });
    const leBDraftId = toNumber(createLeBDraft.json?.row?.id);
    assert(leBDraftId > 0, "LE-B draft should be created");

    const updatedDraft = await apiRequest({
      token: tenantWideToken,
      method: "PUT",
      requestPath: `/api/v1/cari/documents/${invoiceDraftId}`,
      body: {
        amountTxn: 1300,
        amountBase: 1300,
        dueDate: "2026-02-18",
      },
      expectedStatus: 200,
    });
    assert(
      toNumber(updatedDraft.json?.row?.amountTxn) === 1300,
      "Draft update should apply new amountTxn"
    );
    assert(
      String(updatedDraft.json?.row?.dueDate || "").startsWith("2026-02-18"),
      "Draft update should apply dueDate"
    );

    const cancelDraft = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: `/api/v1/cari/documents/${paymentDraftId}/cancel`,
      expectedStatus: 200,
    });
    assert(
      String(cancelDraft.json?.row?.status || "") === "CANCELLED",
      "Draft cancel endpoint should transition status to CANCELLED"
    );

    await apiRequest({
      token: tenantWideToken,
      method: "PUT",
      requestPath: `/api/v1/cari/documents/${paymentDraftId}`,
      body: {
        amountTxn: 500,
        amountBase: 500,
      },
      expectedStatus: 400,
    });

    const scopedList = await apiRequest({
      token: scopedToken,
      method: "GET",
      requestPath: "/api/v1/cari/documents",
      expectedStatus: 200,
    });
    const scopedRows = Array.isArray(scopedList.json?.rows) ? scopedList.json.rows : [];
    assert(
      scopedRows.length >= 1,
      "Scoped user should list at least one in-scope draft document"
    );
    assert(
      scopedRows.every((row) => toNumber(row.legalEntityId) === fixtures.legalEntityAId),
      "Scoped user should only see in-scope legal entity documents"
    );

    await apiRequest({
      token: scopedToken,
      method: "GET",
      requestPath: `/api/v1/cari/documents/${leBDraftId}`,
      expectedStatus: 403,
    });

    await apiRequest({
      token: scopedToken,
      method: "GET",
      requestPath: `/api/v1/cari/documents?legalEntityId=${fixtures.legalEntityBId}`,
      expectedStatus: 403,
    });

    await apiRequest({
      token: otherTenantToken,
      method: "GET",
      requestPath: `/api/v1/cari/documents/${invoiceDraftId}`,
      expectedStatus: 400,
    });

    const auditRows = await query(
      `SELECT action, COUNT(*) AS row_count
       FROM audit_logs
       WHERE tenant_id = ?
         AND resource_type = 'cari_document'
         AND action IN (
           'cari.document.draft.create',
           'cari.document.draft.update',
           'cari.document.draft.cancel'
         )
       GROUP BY action`,
      [tenantAId]
    );
    const auditCounts = new Map(
      (auditRows.rows || []).map((row) => [String(row.action), toNumber(row.row_count)])
    );
    assert(
      toNumber(auditCounts.get("cari.document.draft.create")) >= 2,
      "Draft create audit logs should be written"
    );
    assert(
      toNumber(auditCounts.get("cari.document.draft.update")) >= 1,
      "Draft update audit log should be written"
    );
    assert(
      toNumber(auditCounts.get("cari.document.draft.cancel")) >= 1,
      "Draft cancel audit log should be written"
    );

    await runFrontendSmokeChecks();

    console.log("CARI PR-05 draft documents + payment-term lookup test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: tenantAId,
          checkedDocumentIds: [invoiceDraftId, paymentDraftId, leBDraftId],
          checkedPaymentTermIds: [fixtures.paymentTermAId, fixtures.paymentTermBId],
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
