import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CONTRACTS_PR21_BILLING_TEST_PORT || 3121);
const BASE_URL =
  process.env.CONTRACTS_PR21_BILLING_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "Contracts#12345";
const TEST_BILLING_DATE = "2026-02-25";
const EPSILON = 0.000001;

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

function amountsEqual(left, right, epsilon = EPSILON) {
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

  return {
    status: response.status,
    json,
    cookie,
  };
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
      // keep polling
    }
    await sleep(300);
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
  assert(Boolean(response.cookie), `Login cookie missing for ${email}`);
  return response.cookie;
}

async function createTenantAndAdmin() {
  const stamp = Date.now();
  const tenantCode = `CTR_PR21_BILL_T_${stamp}`;
  const tenantName = `Contracts PR21 Billing Tenant ${stamp}`;
  const email = `contracts_pr21_billing_admin_${stamp}@example.com`;
  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);

  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE name = VALUES(name)`,
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
    [tenantId, email, passwordHash, "Contracts PR21 Billing Admin"]
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
  assert(userId > 0, "Failed to create admin user");

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

  return {
    stamp,
    tenantId,
    userId,
    email,
  };
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

  const groupInsert = await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `CTR_PR21_BILL_G_${stamp}`, `Contracts PR21 Billing Group ${stamp}`]
  );
  const groupCompanyId = toNumber(groupInsert.rows?.insertId);
  assert(groupCompanyId > 0, "Failed to create group company");

  const legalEntityInsert = await query(
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
      `CTR_PR21_BILL_LE_${stamp}`,
      `Contracts PR21 Billing LE ${stamp}`,
      countryId,
    ]
  );
  const legalEntityId = toNumber(legalEntityInsert.rows?.insertId);
  assert(legalEntityId > 0, "Failed to create legal entity");

  const paymentTermInsert = await query(
    `INSERT INTO payment_terms (
        tenant_id,
        legal_entity_id,
        code,
        name,
        due_days,
        grace_days,
        is_end_of_month,
        status
     )
     VALUES (?, ?, ?, ?, 15, 0, FALSE, 'ACTIVE')`,
    [
      tenantId,
      legalEntityId,
      `CTR_PR21_TERM_${stamp}`,
      `Contracts PR21 Payment Term ${stamp}`,
    ]
  );
  const paymentTermId = toNumber(paymentTermInsert.rows?.insertId);
  assert(paymentTermId > 0, "Failed to create payment term");

  const counterpartyInsert = await query(
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
     VALUES (?, ?, ?, ?, TRUE, FALSE, 'USD', ?, 'ACTIVE')`,
    [
      tenantId,
      legalEntityId,
      `CTR_PR21_CP_${stamp}`,
      `Contracts PR21 Customer ${stamp}`,
      paymentTermId,
    ]
  );
  const counterpartyId = toNumber(counterpartyInsert.rows?.insertId);
  assert(counterpartyId > 0, "Failed to create counterparty");

  const coaInsert = await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [
      tenantId,
      legalEntityId,
      `CTR_PR21_BILL_COA_${stamp}`,
      `Contracts PR21 Billing CoA ${stamp}`,
    ]
  );
  const coaId = toNumber(coaInsert.rows?.insertId);
  assert(coaId > 0, "Failed to create CoA");

  const deferredInsert = await query(
    `INSERT INTO accounts (
        coa_id,
        code,
        name,
        account_type,
        normal_side,
        allow_posting,
        is_active
     )
     VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, TRUE)`,
    [coaId, `CTR_PR21_DEF_${stamp}`, `Deferred Liability ${stamp}`]
  );
  const deferredAccountId = toNumber(deferredInsert.rows?.insertId);
  assert(deferredAccountId > 0, "Failed to create deferred account");

  const revenueInsert = await query(
    `INSERT INTO accounts (
        coa_id,
        code,
        name,
        account_type,
        normal_side,
        allow_posting,
        is_active
     )
     VALUES (?, ?, ?, 'REVENUE', 'CREDIT', TRUE, TRUE)`,
    [coaId, `CTR_PR21_REV_${stamp}`, `Revenue ${stamp}`]
  );
  const revenueAccountId = toNumber(revenueInsert.rows?.insertId);
  assert(revenueAccountId > 0, "Failed to create revenue account");

  return {
    legalEntityId,
    counterpartyId,
    deferredAccountId,
    revenueAccountId,
  };
}

async function createContractForBilling({ token, fixture, stamp }) {
  const createPayload = {
    legalEntityId: fixture.legalEntityId,
    counterpartyId: fixture.counterpartyId,
    contractNo: `CTR_PR21_BILL_NO_${stamp}`,
    contractType: "CUSTOMER",
    currencyCode: "USD",
    startDate: "2026-01-01",
    endDate: "2026-12-31",
    notes: "PR21 billing generation smoke contract",
    lines: [
      {
        description: "Line A",
        lineAmountTxn: 7000,
        lineAmountBase: 7000,
        recognitionMethod: "STRAIGHT_LINE",
        recognitionStartDate: "2026-01-01",
        recognitionEndDate: "2026-12-31",
        deferredAccountId: fixture.deferredAccountId,
        revenueAccountId: fixture.revenueAccountId,
        status: "ACTIVE",
      },
      {
        description: "Line B",
        lineAmountTxn: 3000,
        lineAmountBase: 3000,
        recognitionMethod: "STRAIGHT_LINE",
        recognitionStartDate: "2026-01-01",
        recognitionEndDate: "2026-12-31",
        deferredAccountId: fixture.deferredAccountId,
        revenueAccountId: fixture.revenueAccountId,
        status: "ACTIVE",
      },
    ],
  };

  const createResponse = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/contracts",
    body: createPayload,
    expectedStatus: 201,
  });
  const contractId = toNumber(createResponse.json?.row?.id);
  assert(contractId > 0, "Contract create response missing id");

  const detailResponse = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/contracts/${contractId}`,
    expectedStatus: 200,
  });
  const detailRow = detailResponse.json?.row || null;
  assert(detailRow, "Contract detail row missing");
  const lineIds = (Array.isArray(detailRow.lines) ? detailRow.lines : [])
    .map((line) => toNumber(line?.id))
    .filter((id) => id > 0);
  assert(lineIds.length === 2, "Contract should have two lines");

  return { contractId, lineIds };
}

async function assertDatabaseState({
  tenantId,
  contractId,
  firstIdempotencyKey,
  secondIdempotencyKey,
  expectedDocumentId,
  expectedLinkId,
}) {
  const batchRows = await query(
    `SELECT
        id,
        idempotency_key,
        integration_event_uid,
        status,
        generated_document_id,
        generated_link_id
     FROM contract_billing_batches
     WHERE tenant_id = ?
       AND contract_id = ?
       AND idempotency_key IN (?, ?)
     ORDER BY id ASC`,
    [tenantId, contractId, firstIdempotencyKey, secondIdempotencyKey]
  );
  assert(batchRows.rows.length === 2, "Expected two contract_billing_batches rows");

  const firstBatch = batchRows.rows.find(
    (row) => String(row.idempotency_key) === firstIdempotencyKey
  );
  assert(firstBatch, "First batch not found");
  assert(toUpper(firstBatch.status) === "COMPLETED", "First batch status must be COMPLETED");
  assert(
    toNumber(firstBatch.generated_document_id) === expectedDocumentId,
    "First batch generated_document_id mismatch"
  );
  assert(
    toNumber(firstBatch.generated_link_id) === expectedLinkId,
    "First batch generated_link_id mismatch"
  );

  const documentRows = await query(
    `SELECT
        id,
        source_module,
        source_entity_type,
        integration_link_status,
        integration_event_uid
     FROM cari_documents
     WHERE tenant_id = ?
       AND id = ?`,
    [tenantId, expectedDocumentId]
  );
  const document = documentRows.rows?.[0] || null;
  assert(document, "Generated document row not found");
  assert(toUpper(document.source_module) === "CONTRACTS", "source_module must be CONTRACTS");
  assert(
    toUpper(document.source_entity_type) === "CONTRACT_BILLING",
    "source_entity_type must be CONTRACT_BILLING"
  );
  assert(
    toUpper(document.integration_link_status) === "LINKED",
    "integration_link_status must be LINKED"
  );
  assert(Boolean(document.integration_event_uid), "integration_event_uid must be populated");
}

async function assertBillingBatchRow({
  tenantId,
  contractId,
  idempotencyKey,
  expectedDocumentId,
  expectedLinkId,
}) {
  const batchResult = await query(
    `SELECT
        id,
        status,
        generated_document_id,
        generated_link_id
     FROM contract_billing_batches
     WHERE tenant_id = ?
       AND contract_id = ?
       AND idempotency_key = ?
     LIMIT 1`,
    [tenantId, contractId, idempotencyKey]
  );
  const batchRow = batchResult.rows?.[0] || null;
  assert(batchRow, `Batch row not found for idempotencyKey=${idempotencyKey}`);
  assert(toUpper(batchRow.status) === "COMPLETED", "Batch status must be COMPLETED");
  assert(
    toNumber(batchRow.generated_document_id) === expectedDocumentId,
    "Batch generated_document_id mismatch"
  );
  assert(
    toNumber(batchRow.generated_link_id) === expectedLinkId,
    "Batch generated_link_id mismatch"
  );
}

async function main() {
  await runMigrations();

  const server = startServerProcess();
  try {
    await waitForServer();

    const tenant = await createTenantAndAdmin();
    const fixture = await createFixture({
      tenantId: tenant.tenantId,
      stamp: tenant.stamp,
    });
    const token = await login(tenant.email, TEST_PASSWORD);

    const contract = await createContractForBilling({
      token,
      fixture,
      stamp: tenant.stamp,
    });

    const firstIdempotencyKey = `BILL-${tenant.stamp}-A`;
    const firstPayload = {
      docType: "INVOICE",
      amountStrategy: "FULL",
      billingDate: TEST_BILLING_DATE,
      selectedLineIds: contract.lineIds,
      idempotencyKey: firstIdempotencyKey,
      note: "Initial invoice generation",
    };

    const firstResponse = await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/contracts/${contract.contractId}/generate-billing`,
      body: firstPayload,
      expectedStatus: 201,
    });
    assert(
      firstResponse.json?.idempotentReplay === false,
      "First generate-billing call must not be replay"
    );
    const firstDocumentId = toNumber(firstResponse.json?.document?.id);
    const firstLinkId = toNumber(firstResponse.json?.link?.linkId);
    assert(firstDocumentId > 0, "First generated document id missing");
    assert(firstLinkId > 0, "First generated link id missing");
    assert(
      toUpper(firstResponse.json?.document?.documentType) === "INVOICE",
      "INVOICE generation must create INVOICE document type"
    );
    assert(
      toUpper(firstResponse.json?.link?.linkType) === "BILLING",
      "INVOICE generation must create BILLING link type"
    );
    assert(
      amountsEqual(firstResponse.json?.document?.amountTxn, 10000),
      "First generated document amountTxn should be 10000"
    );
    assert(
      amountsEqual(firstResponse.json?.document?.amountBase, 10000),
      "First generated document amountBase should be 10000"
    );

    const replayResponse = await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/contracts/${contract.contractId}/generate-billing`,
      body: firstPayload,
      expectedStatus: 200,
    });
    assert(
      replayResponse.json?.idempotentReplay === true,
      "Second identical call must return idempotent replay"
    );
    assert(
      toNumber(replayResponse.json?.document?.id) === firstDocumentId,
      "Replay should return same document id"
    );
    assert(
      toNumber(replayResponse.json?.link?.linkId) === firstLinkId,
      "Replay should return same link id"
    );

    await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/contracts/${contract.contractId}/generate-billing`,
      body: {
        ...firstPayload,
        docType: "ADVANCE",
      },
      expectedStatus: 400,
    });

    const secondIdempotencyKey = `BILL-${tenant.stamp}-B`;
    const advanceResponse = await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/contracts/${contract.contractId}/generate-billing`,
      body: {
        docType: "ADVANCE",
        amountStrategy: "FULL",
        billingDate: TEST_BILLING_DATE,
        selectedLineIds: [contract.lineIds[0]],
        idempotencyKey: secondIdempotencyKey,
        note: "Advance generation",
      },
      expectedStatus: 201,
    });
    assert(
      toUpper(advanceResponse.json?.document?.documentType) === "PAYMENT",
      "ADVANCE generation must create PAYMENT document type"
    );
    assert(
      toUpper(advanceResponse.json?.link?.linkType) === "ADVANCE",
      "ADVANCE generation must create ADVANCE link type"
    );

    const thirdIdempotencyKey = `BILL-${tenant.stamp}-C`;
    const adjustmentPayload = {
      docType: "ADJUSTMENT",
      amountStrategy: "PARTIAL",
      billingDate: TEST_BILLING_DATE,
      selectedLineIds: [contract.lineIds[1]],
      amountTxn: 1200,
      amountBase: 1200,
      idempotencyKey: thirdIdempotencyKey,
      note: "Adjustment generation",
    };

    const adjustmentResponse = await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/contracts/${contract.contractId}/generate-billing`,
      body: adjustmentPayload,
      expectedStatus: 201,
    });
    assert(
      adjustmentResponse.json?.idempotentReplay === false,
      "ADJUSTMENT first call must not be replay"
    );
    const adjustmentDocumentId = toNumber(adjustmentResponse.json?.document?.id);
    const adjustmentLinkId = toNumber(adjustmentResponse.json?.link?.linkId);
    assert(adjustmentDocumentId > 0, "ADJUSTMENT generated document id missing");
    assert(adjustmentLinkId > 0, "ADJUSTMENT generated link id missing");
    assert(
      toUpper(adjustmentResponse.json?.document?.documentType) === "ADJUSTMENT",
      "ADJUSTMENT generation must create ADJUSTMENT document type"
    );
    assert(
      toUpper(adjustmentResponse.json?.link?.linkType) === "ADJUSTMENT",
      "ADJUSTMENT generation must create ADJUSTMENT link type"
    );
    assert(
      amountsEqual(adjustmentResponse.json?.document?.amountTxn, 1200),
      "ADJUSTMENT generated document amountTxn should be 1200"
    );
    assert(
      amountsEqual(adjustmentResponse.json?.document?.amountBase, 1200),
      "ADJUSTMENT generated document amountBase should be 1200"
    );

    const adjustmentReplayResponse = await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/contracts/${contract.contractId}/generate-billing`,
      body: adjustmentPayload,
      expectedStatus: 200,
    });
    assert(
      adjustmentReplayResponse.json?.idempotentReplay === true,
      "ADJUSTMENT duplicate call must return idempotent replay"
    );
    assert(
      toNumber(adjustmentReplayResponse.json?.document?.id) === adjustmentDocumentId,
      "ADJUSTMENT replay should return same document id"
    );
    assert(
      toNumber(adjustmentReplayResponse.json?.link?.linkId) === adjustmentLinkId,
      "ADJUSTMENT replay should return same link id"
    );

    await assertDatabaseState({
      tenantId: tenant.tenantId,
      contractId: contract.contractId,
      firstIdempotencyKey,
      secondIdempotencyKey,
      expectedDocumentId: firstDocumentId,
      expectedLinkId: firstLinkId,
    });
    await assertBillingBatchRow({
      tenantId: tenant.tenantId,
      contractId: contract.contractId,
      idempotencyKey: thirdIdempotencyKey,
      expectedDocumentId: adjustmentDocumentId,
      expectedLinkId: adjustmentLinkId,
    });

    console.log("Contracts PR-21 billing generation integration test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: tenant.tenantId,
          legalEntityId: fixture.legalEntityId,
          contractId: contract.contractId,
          firstGeneratedDocumentId: firstDocumentId,
          firstGeneratedLinkId: firstLinkId,
          advanceGeneratedDocumentId: toNumber(advanceResponse.json?.document?.id),
          adjustmentGeneratedDocumentId: adjustmentDocumentId,
        },
        null,
        2
      )
    );
  } finally {
    server.kill();
    await closePool();
  }
}

main().catch((error) => {
  console.error("Contracts PR-21 billing generation integration test failed:", error);
  process.exitCode = 1;
});
