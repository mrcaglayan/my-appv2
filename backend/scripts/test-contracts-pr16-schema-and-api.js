import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CONTRACTS_PR16_TEST_PORT || 3116);
const BASE_URL =
  process.env.CONTRACTS_PR16_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function inClause(values) {
  if (!Array.isArray(values) || values.length === 0) {
    throw new Error("inClause values must be non-empty");
  }
  return values.map(() => "?").join(", ");
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
      // ignore until timeout
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

async function assertSchemaExists() {
  const requiredTables = [
    "contracts",
    "contract_lines",
    "contract_document_links",
  ];

  const tableRows = await query(
    `SELECT table_name AS table_name
     FROM information_schema.tables
     WHERE table_schema = DATABASE()
       AND table_name IN (${inClause(requiredTables)})`,
    requiredTables
  );
  const existingTables = new Set(tableRows.rows.map((row) => String(row.table_name)));
  const missingTables = requiredTables.filter((name) => !existingTables.has(name));
  assert(missingTables.length === 0, `Missing contracts tables: ${missingTables.join(", ")}`);

  const requiredIndexes = [
    "uk_contract_no",
    "uk_contracts_tenant_id_id",
    "uk_contracts_tenant_entity_id",
    "uk_contract_line_no",
    "uk_contract_doc_link",
  ];
  const indexRows = await query(
    `SELECT index_name AS index_name
     FROM information_schema.statistics
     WHERE table_schema = DATABASE()
       AND index_name IN (${inClause(requiredIndexes)})`,
    requiredIndexes
  );
  const existingIndexes = new Set(indexRows.rows.map((row) => String(row.index_name)));
  const missingIndexes = requiredIndexes.filter((name) => !existingIndexes.has(name));
  assert(missingIndexes.length === 0, `Missing contracts indexes: ${missingIndexes.join(", ")}`);

  const requiredFks = [
    "fk_contracts_entity_tenant",
    "fk_contracts_counterparty_tenant",
    "fk_contracts_creator_user",
    "fk_contract_lines_contract_tenant",
    "fk_contract_lines_deferred_account",
    "fk_contract_lines_revenue_account",
    "fk_contract_doc_links_contract_tenant",
    "fk_contract_doc_links_cari_doc_tenant",
    "fk_contract_doc_links_creator_user",
    "fk_contract_doc_links_contract_currency",
    "fk_contract_doc_links_document_currency",
  ];
  const fkRows = await query(
    `SELECT constraint_name AS constraint_name
     FROM information_schema.referential_constraints
     WHERE constraint_schema = DATABASE()
       AND constraint_name IN (${inClause(requiredFks)})`,
    requiredFks
  );
  const existingFks = new Set(fkRows.rows.map((row) => String(row.constraint_name)));
  const missingFks = requiredFks.filter((name) => !existingFks.has(name));
  assert(missingFks.length === 0, `Missing contracts FKs: ${missingFks.join(", ")}`);

  const columnRows = await query(
    `SELECT
        table_name AS table_name,
        column_name AS column_name,
        column_type AS column_type
     FROM information_schema.columns
     WHERE table_schema = DATABASE()
       AND (
          (table_name = 'users' AND column_name = 'id')
          OR (table_name = 'contracts' AND column_name = 'created_by_user_id')
          OR (table_name = 'contract_document_links' AND column_name = 'created_by_user_id')
          OR (table_name = 'contract_document_links' AND column_name = 'contract_currency_code_snapshot')
          OR (table_name = 'contract_document_links' AND column_name = 'document_currency_code_snapshot')
          OR (table_name = 'contract_document_links' AND column_name = 'link_fx_rate_snapshot')
        )`
  );
  const byKey = new Map(
    columnRows.rows.map((row) => [
      `${row.table_name}.${row.column_name}`,
      String(row.column_type || "").toLowerCase(),
    ])
  );
  const usersIdType = byKey.get("users.id");
  assert(Boolean(usersIdType), "users.id column type not found");
  assert(
    byKey.get("contracts.created_by_user_id") === usersIdType,
    `contracts.created_by_user_id type mismatch: expected ${usersIdType}`
  );
  assert(
    byKey.get("contract_document_links.created_by_user_id") === usersIdType,
    `contract_document_links.created_by_user_id type mismatch: expected ${usersIdType}`
  );
  assert(
    byKey.get("contract_document_links.contract_currency_code_snapshot") === "char(3)",
    "contract_currency_code_snapshot type mismatch: expected char(3)"
  );
  assert(
    byKey.get("contract_document_links.document_currency_code_snapshot") === "char(3)",
    "document_currency_code_snapshot type mismatch: expected char(3)"
  );
  assert(
    byKey.get("contract_document_links.link_fx_rate_snapshot") === "decimal(20,10)",
    "link_fx_rate_snapshot type mismatch: expected decimal(20,10)"
  );
}

async function createTenantAndAdmin() {
  const stamp = Date.now();
  const tenantCode = `CTR_PR16_T_${stamp}`;
  const tenantName = `Contracts PR16 Tenant ${stamp}`;
  const email = `contracts_admin_${stamp}@example.com`;
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
  const tenantId = toNumber(tenantResult.rows[0]?.id);
  assert(tenantId > 0, "Failed to create tenant");

  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, email, passwordHash, "Contracts Admin"]
  );
  const userResult = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, email]
  );
  const userId = toNumber(userResult.rows[0]?.id);
  assert(userId > 0, "Failed to create user");

  const roleResult = await query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = 'TenantAdmin'
     LIMIT 1`,
    [tenantId]
  );
  const roleId = toNumber(roleResult.rows[0]?.id);
  assert(roleId > 0, "TenantAdmin role not found");

  await query(
    `INSERT INTO user_role_scopes (tenant_id, user_id, role_id, scope_type, scope_id, effect)
     VALUES (?, ?, ?, 'TENANT', ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE
       effect = VALUES(effect)`,
    [tenantId, userId, roleId, tenantId]
  );

  return { tenantId, userId, email, password, stamp };
}

async function createCoreFixture({ tenantId, userId, stamp }) {
  const countryResult = await query(
    `SELECT id
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(countryResult.rows[0]?.id);
  assert(countryId > 0, "US country seed row missing");

  const groupInsert = await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `CTR_G_${stamp}`, `Contracts Group ${stamp}`]
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
      `CTR_LE_${stamp}`,
      `Contracts Legal Entity ${stamp}`,
      countryId,
    ]
  );
  const legalEntityId = toNumber(legalEntityInsert.rows?.insertId);
  assert(legalEntityId > 0, "Failed to create legal entity");

  const calendarInsert = await query(
    `INSERT INTO fiscal_calendars (tenant_id, code, name, year_start_month, year_start_day)
     VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `CTR_CAL_${stamp}`, `Contracts Calendar ${stamp}`]
  );
  const calendarId = toNumber(calendarInsert.rows?.insertId);
  assert(calendarId > 0, "Failed to create fiscal calendar");

  const fiscalYear = 2026;
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
     VALUES (?, ?, 1, ?, '2026-01-01', '2026-12-31', FALSE)`,
    [calendarId, fiscalYear, `FY${fiscalYear} P1`]
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
    [tenantId, legalEntityId, calendarId, `CTR_BOOK_${stamp}`, `Contracts Book ${stamp}`]
  );
  const bookId = toNumber(bookInsert.rows?.insertId);
  assert(bookId > 0, "Failed to create book");

  const coaInsert = await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `CTR_COA_${stamp}`, `Contracts CoA ${stamp}`]
  );
  const coaId = toNumber(coaInsert.rows?.insertId);
  assert(coaId > 0, "Failed to create CoA");

  async function createAccount(codeSuffix, accountType, normalSide) {
    const insert = await query(
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
    return toNumber(insert.rows?.insertId);
  }

  const liabilityDeferredAccountId = await createAccount("2200", "LIABILITY", "CREDIT");
  const revenueAccountId = await createAccount("4000", "REVENUE", "CREDIT");
  const assetDeferredAccountId = await createAccount("1500", "ASSET", "DEBIT");
  const expenseAccountId = await createAccount("6000", "EXPENSE", "DEBIT");
  assert(liabilityDeferredAccountId > 0, "Failed to create liability account");
  assert(revenueAccountId > 0, "Failed to create revenue account");
  assert(assetDeferredAccountId > 0, "Failed to create asset account");
  assert(expenseAccountId > 0, "Failed to create expense account");

  const customerCounterpartyInsert = await query(
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
    [tenantId, legalEntityId, `CTR_CP_C_${stamp}`, `Customer Counterparty ${stamp}`]
  );
  const customerCounterpartyId = toNumber(customerCounterpartyInsert.rows?.insertId);
  assert(customerCounterpartyId > 0, "Failed to create customer counterparty");

  const vendorCounterpartyInsert = await query(
    `INSERT INTO counterparties (
        tenant_id,
        legal_entity_id,
        code,
        name,
        is_customer,
        is_vendor,
        status
     )
     VALUES (?, ?, ?, ?, FALSE, TRUE, 'ACTIVE')`,
    [tenantId, legalEntityId, `CTR_CP_V_${stamp}`, `Vendor Counterparty ${stamp}`]
  );
  const vendorCounterpartyId = toNumber(vendorCounterpartyInsert.rows?.insertId);
  assert(vendorCounterpartyId > 0, "Failed to create vendor counterparty");

  async function createPostedJournal() {
    const insert = await query(
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
      [tenantId, legalEntityId, bookId, fiscalPeriodId, `CTR_JE_${Date.now()}`, "Fixture", userId, userId]
    );
    return toNumber(insert.rows?.insertId);
  }

  let sequenceNo = 1;
  async function insertCariDocument({
    direction,
    status,
    currencyCode,
    amountTxn,
    amountBase,
    postedJournalEntryId = null,
  }) {
    const documentNo = `CTR-DOC-${stamp}-${direction}-${sequenceNo}`;
    const sequenceNamespace = `CTR_${direction}`;
    const sequence = sequenceNo;
    sequenceNo += 1;

    const insertResult = await query(
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
       VALUES (?, ?, ?, NULL, ?, 'INVOICE', ?, 2026, ?, ?, ?, '2026-01-15', '2026-02-14', ?, ?, ?, ?, ?, 1, ?, ?, NULL, '2026-02-14', ?, 1, ?)`,
      [
        tenantId,
        legalEntityId,
        customerCounterpartyId,
        direction,
        sequenceNamespace,
        sequence,
        documentNo,
        status,
        amountTxn,
        amountBase,
        amountTxn,
        amountBase,
        currencyCode,
        `CP-${stamp}`,
        `Counterparty ${stamp}`,
        currencyCode,
        postedJournalEntryId,
      ]
    );
    return toNumber(insertResult.rows?.insertId);
  }

  const postedJournal1 = await createPostedJournal();
  const postedJournal2 = await createPostedJournal();
  const postedJournal3 = await createPostedJournal();

  const postedArDocumentId = await insertCariDocument({
    direction: "AR",
    status: "POSTED",
    currencyCode: "USD",
    amountTxn: 100,
    amountBase: 100,
    postedJournalEntryId: postedJournal1,
  });
  const postedApDocumentId = await insertCariDocument({
    direction: "AP",
    status: "POSTED",
    currencyCode: "USD",
    amountTxn: 100,
    amountBase: 100,
    postedJournalEntryId: postedJournal2,
  });
  const postedTryDocumentId = await insertCariDocument({
    direction: "AR",
    status: "POSTED",
    currencyCode: "TRY",
    amountTxn: 100,
    amountBase: 100,
    postedJournalEntryId: postedJournal3,
  });
  const draftArDocumentId = await insertCariDocument({
    direction: "AR",
    status: "DRAFT",
    currencyCode: "USD",
    amountTxn: 100,
    amountBase: 100,
    postedJournalEntryId: null,
  });

  assert(postedArDocumentId > 0, "Failed to create posted AR document");

  return {
    legalEntityId,
    customerCounterpartyId,
    vendorCounterpartyId,
    liabilityDeferredAccountId,
    revenueAccountId,
    assetDeferredAccountId,
    expenseAccountId,
    postedArDocumentId,
    postedApDocumentId,
    postedTryDocumentId,
    draftArDocumentId,
  };
}

async function runApiAssertions({ token, fixture, stamp }) {
  const createContractResponse = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/contracts",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.customerCounterpartyId,
      contractNo: `CTR-${stamp}-001`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      endDate: "2026-12-31",
      notes: "PR16 contract",
      lines: [
        {
          description: "Main line",
          lineAmountTxn: 100,
          lineAmountBase: 100,
          recognitionMethod: "STRAIGHT_LINE",
          recognitionStartDate: "2026-01-01",
          recognitionEndDate: "2026-12-31",
          deferredAccountId: fixture.liabilityDeferredAccountId,
          revenueAccountId: fixture.revenueAccountId,
        },
        {
          description: "Inactive line",
          lineAmountTxn: 25,
          lineAmountBase: 25,
          recognitionMethod: "MANUAL",
          status: "INACTIVE",
        },
      ],
    },
  });
  const contractId = toNumber(createContractResponse.json?.row?.id);
  assert(contractId > 0, "Contract create did not return row.id");
  assert(
    createContractResponse.json?.row?.totalAmountTxn === 100,
    "Header totals must be derived from ACTIVE lines only"
  );
  assert(
    createContractResponse.json?.row?.lines === undefined,
    "Mutation response must be summary-scoped (no lines[])"
  );

  const getContractResponse = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/contracts/${contractId}`,
    expectedStatus: 200,
  });
  const detailLines = getContractResponse.json?.row?.lines || [];
  assert(detailLines.length === 2, "Contract detail must include all persisted lines");
  assert(detailLines[0]?.lineNo === 1, "Line numbering must start from 1");
  assert(detailLines[1]?.lineNo === 2, "Line numbering must be sequential");
  assert(detailLines[1]?.status === "INACTIVE", "Line status must be returned in detail");

  const listResponse = await apiRequest({
    token,
    method: "GET",
    path: "/api/v1/contracts",
    expectedStatus: 200,
  });
  const listed = (listResponse.json?.rows || []).find((row) => toNumber(row.id) === contractId);
  assert(Boolean(listed), "Created contract missing from list endpoint");
  assert(toNumber(listed?.lineCount) === 2, "lineCount must include ACTIVE + INACTIVE lines");
  assert(listed?.lines === undefined, "List endpoint must be summary-only");

  const updateResponse = await apiRequest({
    token,
    method: "PUT",
    path: `/api/v1/contracts/${contractId}`,
    expectedStatus: 200,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.customerCounterpartyId,
      contractNo: `CTR-${stamp}-001-REV1`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      endDate: "2026-12-31",
      notes: "PR16 contract updated",
      lines: [
        {
          description: "Replacement line",
          lineAmountTxn: 80,
          lineAmountBase: 80,
          recognitionMethod: "MILESTONE",
          recognitionStartDate: "2026-02-01",
          recognitionEndDate: "2026-02-01",
          deferredAccountId: fixture.liabilityDeferredAccountId,
          revenueAccountId: fixture.revenueAccountId,
        },
        {
          description: "Replacement inactive",
          lineAmountTxn: 10,
          lineAmountBase: 10,
          recognitionMethod: "MANUAL",
          status: "INACTIVE",
        },
      ],
    },
  });
  assert(
    updateResponse.json?.row?.totalAmountTxn === 80,
    "Updated totals must still be ACTIVE-line derived"
  );

  const invalidMilestoneResponse = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/contracts",
    expectedStatus: 400,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.customerCounterpartyId,
      contractNo: `CTR-${stamp}-MILESTONE-ERR`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      lines: [
        {
          description: "Invalid milestone date range",
          lineAmountTxn: 10,
          lineAmountBase: 10,
          recognitionMethod: "MILESTONE",
          recognitionStartDate: "2026-03-01",
          recognitionEndDate: "2026-03-15",
        },
      ],
    },
  });
  assert(
    String(invalidMilestoneResponse.json?.message || "").includes("must match for MILESTONE"),
    "MILESTONE should require recognitionStartDate=recognitionEndDate"
  );

  const invalidManualResponse = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/contracts",
    expectedStatus: 400,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.customerCounterpartyId,
      contractNo: `CTR-${stamp}-MANUAL-ERR`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      lines: [
        {
          description: "Invalid manual dates",
          lineAmountTxn: 10,
          lineAmountBase: 10,
          recognitionMethod: "MANUAL",
          recognitionStartDate: "2026-03-01",
          recognitionEndDate: "2026-03-01",
        },
      ],
    },
  });
  assert(
    String(invalidManualResponse.json?.message || "").includes("must be omitted for MANUAL"),
    "MANUAL should reject recognitionStartDate/recognitionEndDate"
  );

  const activateResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractId}/activate`,
    expectedStatus: 200,
  });
  assert(activateResponse.json?.row?.status === "ACTIVE", "activate endpoint failed");

  await apiRequest({
    token,
    method: "PUT",
    path: `/api/v1/contracts/${contractId}`,
    expectedStatus: 400,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.customerCounterpartyId,
      contractNo: `CTR-${stamp}-001-INVALID`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      endDate: "2026-12-31",
      lines: [],
    },
  });

  const suspendResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractId}/suspend`,
    expectedStatus: 200,
  });
  assert(suspendResponse.json?.row?.status === "SUSPENDED", "suspend endpoint failed");

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractId}/activate`,
    expectedStatus: 200,
  });
  const closeResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractId}/close`,
    expectedStatus: 200,
  });
  assert(closeResponse.json?.row?.status === "CLOSED", "close endpoint failed");

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractId}/link-document`,
    expectedStatus: 400,
    body: {
      cariDocumentId: fixture.postedArDocumentId,
      linkType: "BILLING",
      linkedAmountTxn: 10,
      linkedAmountBase: 10,
    },
  });

  const linkableContractResponse = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/contracts",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.customerCounterpartyId,
      contractNo: `CTR-${stamp}-002`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      lines: [
        {
          description: "Link line",
          lineAmountTxn: 100,
          lineAmountBase: 100,
          recognitionMethod: "STRAIGHT_LINE",
          recognitionStartDate: "2026-01-01",
          recognitionEndDate: "2026-12-31",
          deferredAccountId: fixture.liabilityDeferredAccountId,
          revenueAccountId: fixture.revenueAccountId,
        },
      ],
    },
  });
  const linkableContractId = toNumber(linkableContractResponse.json?.row?.id);
  assert(linkableContractId > 0, "Failed to create linkable contract");

  const linkResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${linkableContractId}/link-document`,
    expectedStatus: 201,
    body: {
      cariDocumentId: fixture.postedArDocumentId,
      linkType: "BILLING",
      linkedAmountTxn: 60,
      linkedAmountBase: 60,
    },
  });
  assert(linkResponse.json?.row?.cariDocumentId === fixture.postedArDocumentId, "Link response shape mismatch");
  assert(linkResponse.json?.row?.linkType === "BILLING", "Link type mismatch");

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${linkableContractId}/link-document`,
    expectedStatus: 400,
    body: {
      cariDocumentId: fixture.postedArDocumentId,
      linkType: "BILLING",
      linkedAmountTxn: 10,
      linkedAmountBase: 10,
    },
  });

  const docsListResponse = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/contracts/${linkableContractId}/documents`,
    expectedStatus: 200,
  });
  assert((docsListResponse.json?.rows || []).length === 1, "Documents list must return one link row");

  const linkableDocumentsResponse = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/contracts/${linkableContractId}/linkable-documents?limit=100&offset=0`,
    expectedStatus: 200,
  });
  const linkableIds = new Set(
    (linkableDocumentsResponse.json?.rows || []).map((row) => toNumber(row?.id))
  );
  assert(
    linkableIds.has(fixture.postedArDocumentId),
    "linkable-documents should include posted AR document for CUSTOMER contract"
  );
  assert(
    linkableIds.has(fixture.postedTryDocumentId),
    "linkable-documents should include cross-currency AR document for CUSTOMER contract"
  );
  assert(
    !linkableIds.has(fixture.postedApDocumentId),
    "linkable-documents should exclude AP document for CUSTOMER contract"
  );
  assert(
    !linkableIds.has(fixture.draftArDocumentId),
    "linkable-documents should exclude DRAFT documents"
  );

  const capContractResponse = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/contracts",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.customerCounterpartyId,
      contractNo: `CTR-${stamp}-003`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      lines: [
        {
          description: "Cap line",
          lineAmountTxn: 100,
          lineAmountBase: 100,
          recognitionMethod: "STRAIGHT_LINE",
          recognitionStartDate: "2026-01-01",
          recognitionEndDate: "2026-12-31",
          deferredAccountId: fixture.liabilityDeferredAccountId,
          revenueAccountId: fixture.revenueAccountId,
        },
      ],
    },
  });
  const capContractId = toNumber(capContractResponse.json?.row?.id);
  assert(capContractId > 0, "Failed to create cap contract");

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${capContractId}/link-document`,
    expectedStatus: 201,
    body: {
      cariDocumentId: fixture.postedArDocumentId,
      linkType: "ADVANCE",
      linkedAmountTxn: 40,
      linkedAmountBase: 40,
    },
  });
  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${capContractId}/link-document`,
    expectedStatus: 400,
    body: {
      cariDocumentId: fixture.postedArDocumentId,
      linkType: "ADJUSTMENT",
      linkedAmountTxn: 1,
      linkedAmountBase: 1,
    },
  });

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${capContractId}/link-document`,
    expectedStatus: 400,
    body: {
      cariDocumentId: fixture.postedApDocumentId,
      linkType: "BILLING",
      linkedAmountTxn: 10,
      linkedAmountBase: 10,
    },
  });

  const crossCurrencyLinkResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${capContractId}/link-document`,
    expectedStatus: 201,
    body: {
      cariDocumentId: fixture.postedTryDocumentId,
      linkType: "BILLING",
      linkedAmountTxn: 10,
      linkedAmountBase: 10,
    },
  });
  assert(
    String(crossCurrencyLinkResponse.json?.row?.contractCurrencyCodeSnapshot || "") === "USD",
    "Cross-currency link must expose contractCurrencyCodeSnapshot"
  );
  assert(
    String(crossCurrencyLinkResponse.json?.row?.documentCurrencyCodeSnapshot || "") === "TRY",
    "Cross-currency link must expose documentCurrencyCodeSnapshot"
  );
  assert(
    toNumber(crossCurrencyLinkResponse.json?.row?.linkFxRateSnapshot) === 1,
    "Cross-currency link should fallback to document fx_rate snapshot when linkFxRate is not provided"
  );

  const explicitFxLinkResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${capContractId}/link-document`,
    expectedStatus: 201,
    body: {
      cariDocumentId: fixture.postedTryDocumentId,
      linkType: "ADJUSTMENT",
      linkedAmountTxn: 5,
      linkedAmountBase: 5,
      linkFxRate: 35.125,
    },
  });
  assert(
    toNumber(explicitFxLinkResponse.json?.row?.linkFxRateSnapshot) === 35.125,
    "Cross-currency link must persist explicit linkFxRate snapshot"
  );

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${capContractId}/link-document`,
    expectedStatus: 400,
    body: {
      cariDocumentId: fixture.draftArDocumentId,
      linkType: "BILLING",
      linkedAmountTxn: 10,
      linkedAmountBase: 10,
    },
  });

  await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/contracts",
    expectedStatus: 400,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.vendorCounterpartyId,
      contractNo: `CTR-${stamp}-ROLE-ERR`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      lines: [
        {
          description: "Role error",
          lineAmountTxn: 10,
          lineAmountBase: 10,
          recognitionMethod: "STRAIGHT_LINE",
          recognitionStartDate: "2026-01-01",
          recognitionEndDate: "2026-02-01",
          deferredAccountId: fixture.liabilityDeferredAccountId,
          revenueAccountId: fixture.revenueAccountId,
        },
      ],
    },
  });

  await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/contracts",
    expectedStatus: 400,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.customerCounterpartyId,
      contractNo: `CTR-${stamp}-ACC-ERR`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      lines: [
        {
          description: "Account type mismatch",
          lineAmountTxn: 10,
          lineAmountBase: 10,
          recognitionMethod: "STRAIGHT_LINE",
          recognitionStartDate: "2026-01-01",
          recognitionEndDate: "2026-02-01",
          deferredAccountId: fixture.assetDeferredAccountId,
          revenueAccountId: fixture.revenueAccountId,
        },
      ],
    },
  });

  const cancelContractResponse = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/contracts",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      counterpartyId: fixture.customerCounterpartyId,
      contractNo: `CTR-${stamp}-004`,
      contractType: "CUSTOMER",
      currencyCode: "USD",
      startDate: "2026-01-01",
      lines: [],
    },
  });
  const cancelContractId = toNumber(cancelContractResponse.json?.row?.id);
  assert(cancelContractId > 0, "Failed to create cancel test contract");

  const cancelResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${cancelContractId}/cancel`,
    expectedStatus: 200,
  });
  assert(cancelResponse.json?.row?.status === "CANCELLED", "cancel endpoint failed");

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${cancelContractId}/activate`,
    expectedStatus: 400,
  });
}

async function main() {
  await runMigrations();
  await seedCore({ ensureDefaultTenantIfMissing: true });
  await assertSchemaExists();

  const identity = await createTenantAndAdmin();
  const fixture = await createCoreFixture(identity);

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

    console.log("Contracts PR-16 schema + API integration test passed.");
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
    console.error("Contracts PR-16 schema + API integration test failed:", err);
    try {
      await closePool();
    } catch {
      // ignore close failures
    }
    process.exit(1);
  });
