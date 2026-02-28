import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CONTRACTS_PR20_TEST_PORT || 3120);
const BASE_URL =
  process.env.CONTRACTS_PR20_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
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
  const tenantCode = `CTR_PR20_T_${stamp}`;
  const tenantName = `Contracts PR20 Tenant ${stamp}`;
  const email = `contracts_pr20_admin_${stamp}@example.com`;
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
    [tenantId, email, passwordHash, "Contracts PR20 Admin"]
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

async function createFixture({ tenantId, userId, stamp }) {
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
    [tenantId, `CTR_PR20_G_${stamp}`, `Contracts PR20 Group ${stamp}`]
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
      `CTR_PR20_LE_${stamp}`,
      `Contracts PR20 Legal Entity ${stamp}`,
      countryId,
    ]
  );
  const legalEntityId = toNumber(legalEntityResult.rows?.insertId);
  assert(legalEntityId > 0, "Failed to create legal entity");

  const calendarResult = await query(
    `INSERT INTO fiscal_calendars (tenant_id, code, name, year_start_month, year_start_day)
     VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `CTR_PR20_CAL_${stamp}`, `Contracts PR20 Calendar ${stamp}`]
  );
  const calendarId = toNumber(calendarResult.rows?.insertId);
  assert(calendarId > 0, "Failed to create fiscal calendar");

  const fiscalPeriodResult = await query(
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
    [calendarId, `FY2026 P1 PR20 ${stamp}`]
  );
  const fiscalPeriodId = toNumber(fiscalPeriodResult.rows?.insertId);
  assert(fiscalPeriodId > 0, "Failed to create fiscal period");

  const bookResult = await query(
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
      `CTR_PR20_BOOK_${stamp}`,
      `Contracts PR20 Book ${stamp}`,
    ]
  );
  const bookId = toNumber(bookResult.rows?.insertId);
  assert(bookId > 0, "Failed to create book");

  const coaResult = await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `CTR_PR20_COA_${stamp}`, `Contracts PR20 CoA ${stamp}`]
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
    [tenantId, legalEntityId, `CTR_PR20_CP_${stamp}`, `Customer ${stamp}`]
  );
  const counterpartyId = toNumber(counterpartyResult.rows?.insertId);
  assert(counterpartyId > 0, "Failed to create counterparty");

  async function createPostedJournal(journalNoSuffix) {
    const result = await query(
      `INSERT INTO journal_entries (
          tenant_id,
          legal_entity_id,
          book_id,
          fiscal_period_id,
          journal_no,
          source_type,
          status,
          entry_date,
          document_date,
          currency_code,
          description,
          total_debit_base,
          total_credit_base,
          created_by_user_id,
          posted_by_user_id,
          posted_at
       )
       VALUES (
         ?, ?, ?, ?, ?, 'SYSTEM', 'POSTED', '2026-01-15', '2026-01-15', 'USD', ?, 100, 100, ?, ?, CURRENT_TIMESTAMP
       )`,
      [
        tenantId,
        legalEntityId,
        bookId,
        fiscalPeriodId,
        `CTR_PR20_JE_${journalNoSuffix}_${stamp}`,
        `Fixture ${journalNoSuffix}`,
        userId,
        userId,
      ]
    );
    return toNumber(result.rows?.insertId);
  }

  const postedJournalIdA = await createPostedJournal("A");
  const postedJournalIdB = await createPostedJournal("B");
  assert(postedJournalIdA > 0 && postedJournalIdB > 0, "Failed to create posted journals");

  let sequenceNo = 1;
  async function insertPostedArDocument({ amountTxn, amountBase }) {
    const result = await query(
      `INSERT INTO cari_documents (
          tenant_id,
          legal_entity_id,
          counterparty_id,
          payment_term_id,
          direction,
          document_type,
          sequence_namespace,
          fiscal_year,
          sequence_no,
          document_no,
          status,
          document_date,
          due_date,
          amount_txn,
          amount_base,
          open_amount_txn,
          open_amount_base,
          currency_code,
          fx_rate,
          counterparty_code_snapshot,
          counterparty_name_snapshot,
          payment_term_snapshot,
          due_date_snapshot,
          currency_code_snapshot,
          fx_rate_snapshot,
          posted_journal_entry_id
       )
       VALUES (?, ?, ?, NULL, 'AR', 'INVOICE', 'CTR_PR20_AR', 2026, ?, ?, 'POSTED', '2026-01-15', '2026-02-14', ?, ?, ?, ?, 'USD', 1, ?, ?, NULL, '2026-02-14', 'USD', 1, ?)`,
      [
        tenantId,
        legalEntityId,
        counterpartyId,
        sequenceNo,
        `CTR-PR20-DOC-${stamp}-${sequenceNo}`,
        amountTxn,
        amountBase,
        amountTxn,
        amountBase,
        `CP-${stamp}`,
        `Counterparty ${stamp}`,
        sequenceNo === 1 ? postedJournalIdA : postedJournalIdB,
      ]
    );
    sequenceNo += 1;
    return toNumber(result.rows?.insertId);
  }

  const documentId = await insertPostedArDocument({ amountTxn: 100, amountBase: 100 });
  assert(documentId > 0, "Failed to create posted AR document");

  return {
    legalEntityId,
    counterpartyId,
    deferredAccountId,
    revenueAccountId,
    documentId,
  };
}

async function createContract({
  token,
  fixture,
  contractNo,
}) {
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
      lines: [
        {
          description: "Main line",
          lineAmountTxn: 100,
          lineAmountBase: 100,
          recognitionMethod: "STRAIGHT_LINE",
          recognitionStartDate: "2026-01-01",
          recognitionEndDate: "2026-12-31",
          deferredAccountId: fixture.deferredAccountId,
          revenueAccountId: fixture.revenueAccountId,
        },
      ],
    },
  });

  const contractId = toNumber(response.json?.row?.id);
  assert(contractId > 0, "Contract create did not return row.id");
  return contractId;
}

async function runApiAssertions({ token, fixture, tenantId, stamp }) {
  const contractAId = await createContract({
    token,
    fixture,
    contractNo: `CTR-PR20-A-${stamp}`,
  });
  const contractBId = await createContract({
    token,
    fixture,
    contractNo: `CTR-PR20-B-${stamp}`,
  });

  const linkCreateResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractAId}/link-document`,
    expectedStatus: 201,
    body: {
      cariDocumentId: fixture.documentId,
      linkType: "BILLING",
      linkedAmountTxn: 60,
      linkedAmountBase: 60,
    },
  });
  const linkId = toNumber(linkCreateResponse.json?.row?.linkId);
  assert(linkId > 0, "link-document response missing linkId");
  assert(
    String(linkCreateResponse.json?.row?.contractCurrencyCodeSnapshot || "") === "USD",
    "Create response must expose contractCurrencyCodeSnapshot"
  );
  assert(
    String(linkCreateResponse.json?.row?.documentCurrencyCodeSnapshot || "") === "USD",
    "Create response must expose documentCurrencyCodeSnapshot"
  );
  assert(
    amountsEqual(linkCreateResponse.json?.row?.linkFxRateSnapshot, 1),
    "Create response must expose linkFxRateSnapshot=1 for same currency"
  );

  const adjustResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractAId}/documents/${linkId}/adjust`,
    expectedStatus: 200,
    body: {
      nextLinkedAmountTxn: 30,
      nextLinkedAmountBase: 30,
      reason: "Reduce initial allocation",
    },
  });
  assert(
    amountsEqual(adjustResponse.json?.row?.linkedAmountTxn, 30),
    "Adjusted linkedAmountTxn must be 30"
  );
  assert(
    amountsEqual(adjustResponse.json?.row?.linkedAmountBase, 30),
    "Adjusted linkedAmountBase must be 30"
  );
  assert(
    Number(adjustResponse.json?.row?.adjustmentCount || 0) >= 1,
    "Adjustment count must increase after adjust"
  );
  assert(
    String(adjustResponse.json?.row?.contractCurrencyCodeSnapshot || "") === "USD" &&
      String(adjustResponse.json?.row?.documentCurrencyCodeSnapshot || "") === "USD",
    "Adjust response must preserve currency snapshots"
  );
  assert(
    amountsEqual(adjustResponse.json?.row?.linkFxRateSnapshot, 1),
    "Adjust response must preserve linkFxRateSnapshot"
  );
  assert(
    adjustResponse.json?.row?.isUnlinked === false,
    "Link must remain active after adjust"
  );

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractBId}/link-document`,
    expectedStatus: 201,
    body: {
      cariDocumentId: fixture.documentId,
      linkType: "ADVANCE",
      linkedAmountTxn: 60,
      linkedAmountBase: 60,
    },
  });

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractAId}/documents/${linkId}/adjust`,
    expectedStatus: 400,
    body: {
      nextLinkedAmountTxn: 50,
      nextLinkedAmountBase: 50,
      reason: "Would exceed document cap",
    },
  });

  const unlinkResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractAId}/documents/${linkId}/unlink`,
    expectedStatus: 200,
    body: {
      reason: "Wrong contract mapping",
    },
  });
  assert(
    amountsEqual(unlinkResponse.json?.row?.linkedAmountTxn, 0),
    "Unlink must zero effective linkedAmountTxn"
  );
  assert(
    amountsEqual(unlinkResponse.json?.row?.linkedAmountBase, 0),
    "Unlink must zero effective linkedAmountBase"
  );
  assert(unlinkResponse.json?.row?.isUnlinked === true, "Unlink must mark link as unlinked");

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractAId}/documents/${linkId}/adjust`,
    expectedStatus: 400,
    body: {
      nextLinkedAmountTxn: 10,
      nextLinkedAmountBase: 10,
      reason: "Cannot adjust unlinked row",
    },
  });

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractAId}/documents/${linkId}/unlink`,
    expectedStatus: 400,
    body: {
      reason: "Duplicate unlink",
    },
  });

  const docsResponse = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/contracts/${contractAId}/documents`,
    expectedStatus: 200,
  });
  const linkedRow = (docsResponse.json?.rows || []).find(
    (row) => toNumber(row?.linkId) === linkId
  );
  assert(Boolean(linkedRow), "Adjusted link not found in list endpoint");
  assert(linkedRow?.isUnlinked === true, "List endpoint must show unlinked state");
  assert(amountsEqual(linkedRow?.linkedAmountTxn, 0), "List endpoint must expose effective txn=0");
  assert(
    String(linkedRow?.contractCurrencyCodeSnapshot || "") === "USD" &&
      String(linkedRow?.documentCurrencyCodeSnapshot || "") === "USD",
    "List endpoint must expose link currency snapshots"
  );
  assert(
    amountsEqual(linkedRow?.linkFxRateSnapshot, 1),
    "List endpoint must expose linkFxRateSnapshot"
  );

  const auditRows = await query(
    `SELECT action
     FROM audit_logs
     WHERE tenant_id = ?
       AND resource_type = 'contract_document_link'
       AND resource_id = ?
       AND action IN (
         'contract.document_link.create',
         'contract.document_link.adjust',
         'contract.document_link.unlink'
       )`,
    [tenantId, String(linkId)]
  );
  const actions = new Set((auditRows.rows || []).map((row) => String(row.action || "")));
  assert(actions.has("contract.document_link.create"), "Missing audit action: link create");
  assert(actions.has("contract.document_link.adjust"), "Missing audit action: link adjust");
  assert(actions.has("contract.document_link.unlink"), "Missing audit action: link unlink");
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
      tenantId: identity.tenantId,
      stamp: identity.stamp,
    });

    console.log("Contracts PR-20 link correction integration test passed.");
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
    console.error("Contracts PR-20 link correction integration test failed:", err);
    try {
      await closePool();
    } catch {
      // ignore close failures
    }
    process.exit(1);
  });
