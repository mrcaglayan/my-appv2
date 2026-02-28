import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CONTRACTS_PR22_REVREC_TEST_PORT || 3123);
const BASE_URL =
  process.env.CONTRACTS_PR22_REVREC_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "Contracts#12345";
const TEST_DATE = "2026-02-25";
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
  const tenantCode = `CTR_PR22_T_${stamp}`;
  const tenantName = `Contracts PR22 Tenant ${stamp}`;
  const email = `contracts_pr22_admin_${stamp}@example.com`;
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
    [tenantId, email, passwordHash, "Contracts PR22 Admin"]
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

async function createFixture({ tenantId, userId, stamp }) {
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
    [tenantId, `CTR_PR22_G_${stamp}`, `Contracts PR22 Group ${stamp}`]
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
      `CTR_PR22_LE_${stamp}`,
      `Contracts PR22 LE ${stamp}`,
      countryId,
    ]
  );
  const legalEntityId = toNumber(legalEntityInsert.rows?.insertId);
  assert(legalEntityId > 0, "Failed to create legal entity");

  const calendarInsert = await query(
    `INSERT INTO fiscal_calendars (tenant_id, code, name, year_start_month, year_start_day)
     VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `CTR_PR22_CAL_${stamp}`, `Contracts PR22 Calendar ${stamp}`]
  );
  const calendarId = toNumber(calendarInsert.rows?.insertId);
  assert(calendarId > 0, "Failed to create fiscal calendar");

  const periodInsert = await query(
    `INSERT INTO fiscal_periods (
        calendar_id,
        fiscal_year,
        period_no,
        period_name,
        start_date,
        end_date,
        is_adjustment
     )
     VALUES (?, 2026, 1, ?, '2026-01-01', '2026-12-31', FALSE)`,
    [calendarId, `FY2026 P1 ${stamp}`]
  );
  const fiscalPeriodId = toNumber(periodInsert.rows?.insertId);
  assert(fiscalPeriodId > 0, "Failed to create fiscal period");

  const bookInsert = await query(
    `INSERT INTO books (
        tenant_id,
        legal_entity_id,
        calendar_id,
        code,
        name,
        book_type,
        base_currency_code
     )
     VALUES (?, ?, ?, ?, ?, 'LOCAL', 'USD')`,
    [
      tenantId,
      legalEntityId,
      calendarId,
      `CTR_PR22_BOOK_${stamp}`,
      `Contracts PR22 Book ${stamp}`,
    ]
  );
  const bookId = toNumber(bookInsert.rows?.insertId);
  assert(bookId > 0, "Failed to create book");

  await query(
    `INSERT INTO period_statuses (
        book_id,
        fiscal_period_id,
        status,
        closed_by_user_id,
        closed_at,
        note
     )
     VALUES (?, ?, 'OPEN', NULL, NULL, ?)
     ON DUPLICATE KEY UPDATE
       status = VALUES(status),
       closed_by_user_id = VALUES(closed_by_user_id),
       closed_at = VALUES(closed_at),
       note = VALUES(note)`,
    [bookId, fiscalPeriodId, "PR22 open period"]
  );

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
     VALUES (?, ?, ?, ?, 10, 0, FALSE, 'ACTIVE')`,
    [tenantId, legalEntityId, `CTR_PR22_TERM_${stamp}`, `Contracts PR22 Payment Term ${stamp}`]
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
      `CTR_PR22_CP_${stamp}`,
      `Contracts PR22 Customer ${stamp}`,
      paymentTermId,
    ]
  );
  const counterpartyId = toNumber(counterpartyInsert.rows?.insertId);
  assert(counterpartyId > 0, "Failed to create counterparty");

  const coaInsert = await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `CTR_PR22_COA_${stamp}`, `Contracts PR22 CoA ${stamp}`]
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
    [coaId, `CTR_PR22_DEF_${stamp}`, `Deferred Liability ${stamp}`]
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
    [coaId, `CTR_PR22_REV_${stamp}`, `Revenue ${stamp}`]
  );
  const revenueAccountId = toNumber(revenueInsert.rows?.insertId);
  assert(revenueAccountId > 0, "Failed to create revenue account");

  return {
    legalEntityId,
    fiscalPeriodId,
    counterpartyId,
    deferredAccountId,
    revenueAccountId,
    userId,
  };
}

async function createContract({ token, fixture, stamp }) {
  const createPayload = {
    legalEntityId: fixture.legalEntityId,
    counterpartyId: fixture.counterpartyId,
    contractNo: `CTR_PR22_NO_${stamp}`,
    contractType: "CUSTOMER",
    currencyCode: "USD",
    startDate: "2026-01-01",
    endDate: "2026-12-31",
    notes: "PR22 revrec generation contract",
    lines: [
      {
        description: "Straight line line",
        lineAmountTxn: 1200,
        lineAmountBase: 1200,
        recognitionMethod: "STRAIGHT_LINE",
        recognitionStartDate: "2026-01-01",
        recognitionEndDate: "2026-12-31",
        deferredAccountId: fixture.deferredAccountId,
        revenueAccountId: fixture.revenueAccountId,
        status: "ACTIVE",
      },
      {
        description: "Milestone line",
        lineAmountTxn: 500,
        lineAmountBase: 500,
        recognitionMethod: "MILESTONE",
        recognitionStartDate: "2026-03-15",
        recognitionEndDate: "2026-03-15",
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
  const lineRows = Array.isArray(detailRow.lines) ? detailRow.lines : [];
  assert(lineRows.length === 2, "Expected two contract lines");

  const lineAId = toNumber(lineRows[0]?.id);
  const lineBId = toNumber(lineRows[1]?.id);
  assert(lineAId > 0 && lineBId > 0, "Contract line IDs missing");
  return { contractId, lineAId, lineBId };
}

async function createLinkedBillingDocument({
  token,
  contractId,
  lineAId,
  stamp,
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractId}/generate-billing`,
    body: {
      docType: "INVOICE",
      amountStrategy: "FULL",
      billingDate: TEST_DATE,
      selectedLineIds: [lineAId],
      idempotencyKey: `PR22-BILL-${stamp}`,
      note: "PR22 billing for linked-doc revrec mode",
    },
    expectedStatus: 201,
  });
  const documentId = toNumber(response.json?.document?.id);
  assert(documentId > 0, "Billing document id missing");
  return documentId;
}

async function assertRevrecLineState({
  tenantId,
  legalEntityId,
  fiscalPeriodId,
  contractId,
  contractLineId,
  sourceCariDocumentId,
  expectedCount,
}) {
  const rowsResult = await query(
    `SELECT
        rrsl.id,
        rrsl.source_contract_id,
        rrsl.source_contract_line_id,
        rrsl.source_cari_document_id,
        rrsl.maturity_date,
        rrsl.amount_txn,
        rrsl.amount_base,
        rrsl.source_row_uid
     FROM revenue_recognition_schedule_lines rrsl
     WHERE rrsl.tenant_id = ?
       AND rrsl.legal_entity_id = ?
       AND rrsl.fiscal_period_id = ?
       AND rrsl.source_contract_id = ?
       AND rrsl.source_contract_line_id = ?
     ORDER BY rrsl.maturity_date ASC, rrsl.id ASC`,
    [tenantId, legalEntityId, fiscalPeriodId, contractId, contractLineId]
  );
  const rows = rowsResult.rows || [];
  assert(
    rows.length === expectedCount,
    `Expected ${expectedCount} RevRec lines, got ${rows.length}`
  );

  for (const row of rows) {
    assert(
      toNumber(row.source_contract_id) === contractId,
      "source_contract_id should match contractId"
    );
    assert(
      toNumber(row.source_contract_line_id) === contractLineId,
      "source_contract_line_id should match contractLineId"
    );
    if (sourceCariDocumentId) {
      assert(
        toNumber(row.source_cari_document_id) === sourceCariDocumentId,
        "source_cari_document_id should match linked document"
      );
    } else {
      assert(
        row.source_cari_document_id === null,
        "source_cari_document_id should be null for BY_CONTRACT_LINE mode"
      );
    }
    assert(Boolean(String(row.source_row_uid || "").trim()), "source_row_uid must be populated");
  }

  const distinctSourceResult = await query(
    `SELECT COUNT(DISTINCT rrsl.source_row_uid) AS total
     FROM revenue_recognition_schedule_lines rrsl
     WHERE rrsl.tenant_id = ?
       AND rrsl.legal_entity_id = ?
       AND rrsl.fiscal_period_id = ?
       AND rrsl.source_contract_id = ?
       AND rrsl.source_contract_line_id = ?`,
    [tenantId, legalEntityId, fiscalPeriodId, contractId, contractLineId]
  );
  assert(
    toNumber(distinctSourceResult.rows?.[0]?.total) === expectedCount,
    "source_row_uid values should be unique per generated bucket"
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
      userId: tenant.userId,
      stamp: tenant.stamp,
    });
    const token = await login(tenant.email, TEST_PASSWORD);

    const contract = await createContract({
      token,
      fixture,
      stamp: tenant.stamp,
    });
    const linkedBillingDocumentId = await createLinkedBillingDocument({
      token,
      contractId: contract.contractId,
      lineAId: contract.lineAId,
      stamp: tenant.stamp,
    });

    const linkedModePayload = {
      fiscalPeriodId: fixture.fiscalPeriodId,
      generationMode: "BY_LINKED_DOCUMENT",
      sourceCariDocumentId: linkedBillingDocumentId,
      regenerateMissingOnly: true,
      contractLineIds: [contract.lineAId],
    };

    const firstLinkedMode = await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/contracts/${contract.contractId}/generate-revrec`,
      body: linkedModePayload,
      expectedStatus: 201,
    });
    assert(
      firstLinkedMode.json?.idempotentReplay === false,
      "First linked-document revrec generation should not be replay"
    );
    assert(
      toUpper(firstLinkedMode.json?.accountFamily) === "DEFREV",
      "CUSTOMER contract should map RevRec accountFamily to DEFREV"
    );
    assert(
      toNumber(firstLinkedMode.json?.generatedLineCount) === 12,
      "Straight-line yearly contract line should generate 12 monthly RevRec lines"
    );
    assert(
      toNumber(firstLinkedMode.json?.generatedScheduleCount) >= 1,
      "Linked-document revrec should generate at least one schedule"
    );

    await assertRevrecLineState({
      tenantId: tenant.tenantId,
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.fiscalPeriodId,
      contractId: contract.contractId,
      contractLineId: contract.lineAId,
      sourceCariDocumentId: linkedBillingDocumentId,
      expectedCount: 12,
    });

    const replayLinkedMode = await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/contracts/${contract.contractId}/generate-revrec`,
      body: linkedModePayload,
      expectedStatus: 200,
    });
    assert(
      replayLinkedMode.json?.idempotentReplay === true,
      "Re-running linked-document revrec generation should be idempotent replay"
    );
    assert(
      toNumber(replayLinkedMode.json?.generatedLineCount) === 0,
      "Replay should not generate new lines"
    );

    const byLinePayload = {
      fiscalPeriodId: fixture.fiscalPeriodId,
      generationMode: "BY_CONTRACT_LINE",
      regenerateMissingOnly: true,
      contractLineIds: [contract.lineBId],
    };
    const firstByLine = await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/contracts/${contract.contractId}/generate-revrec`,
      body: byLinePayload,
      expectedStatus: 201,
    });
    assert(
      firstByLine.json?.idempotentReplay === false,
      "First BY_CONTRACT_LINE generation should not be replay"
    );
    assert(
      toNumber(firstByLine.json?.generatedLineCount) === 1,
      "Milestone contract line should generate one RevRec line"
    );

    await assertRevrecLineState({
      tenantId: tenant.tenantId,
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.fiscalPeriodId,
      contractId: contract.contractId,
      contractLineId: contract.lineBId,
      sourceCariDocumentId: null,
      expectedCount: 1,
    });

    const lineAmountRows = await query(
      `SELECT
          SUM(rrsl.amount_txn) AS total_amount_txn,
          SUM(rrsl.amount_base) AS total_amount_base
       FROM revenue_recognition_schedule_lines rrsl
       WHERE rrsl.tenant_id = ?
         AND rrsl.source_contract_id = ?
         AND rrsl.source_contract_line_id = ?`,
      [tenant.tenantId, contract.contractId, contract.lineAId]
    );
    assert(
      amountsEqual(lineAmountRows.rows?.[0]?.total_amount_txn, 1200),
      "Generated monthly line totals must reconcile to contract line amount_txn"
    );
    assert(
      amountsEqual(lineAmountRows.rows?.[0]?.total_amount_base, 1200),
      "Generated monthly line totals must reconcile to contract line amount_base"
    );

    const replayByLine = await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/contracts/${contract.contractId}/generate-revrec`,
      body: byLinePayload,
      expectedStatus: 200,
    });
    assert(
      replayByLine.json?.idempotentReplay === true,
      "Re-running BY_CONTRACT_LINE generation should be idempotent replay"
    );
    assert(
      toNumber(replayByLine.json?.generatedLineCount) === 0,
      "BY_CONTRACT_LINE replay should not generate new lines"
    );

    console.log("Contracts PR-22 RevRec generation integration test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: tenant.tenantId,
          legalEntityId: fixture.legalEntityId,
          fiscalPeriodId: fixture.fiscalPeriodId,
          contractId: contract.contractId,
          linkedBillingDocumentId,
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
  console.error("Contracts PR-22 RevRec generation integration test failed:", error);
  process.exitCode = 1;
});
