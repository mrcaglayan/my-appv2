import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CONTRACTS_PR24_ROLLUP_TEST_PORT || 3125);
const BASE_URL =
  process.env.CONTRACTS_PR24_ROLLUP_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "Contracts#12345";
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

function toDateOnlyString(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      return null;
    }
    const year = value.getFullYear();
    const month = String(value.getMonth() + 1).padStart(2, "0");
    const day = String(value.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }
  const raw = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}(?:\b|T)/.test(raw)) {
    return raw.slice(0, 10);
  }
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  const year = parsed.getUTCFullYear();
  const month = String(parsed.getUTCMonth() + 1).padStart(2, "0");
  const day = String(parsed.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

async function apiRequest({ token, method = "GET", path, body, expectedStatus }) {
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
  const tenantCode = `CTR_PR24_T_${stamp}`;
  const tenantName = `Contracts PR24 Tenant ${stamp}`;
  const email = `contracts_pr24_admin_${stamp}@example.com`;
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
    [tenantId, email, passwordHash, "Contracts PR24 Admin"]
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

  return { stamp, tenantId, userId, email };
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
    [tenantId, `CTR_PR24_G_${stamp}`, `Contracts PR24 Group ${stamp}`]
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
      `CTR_PR24_LE_${stamp}`,
      `Contracts PR24 LE ${stamp}`,
      countryId,
    ]
  );
  const legalEntityId = toNumber(legalEntityInsert.rows?.insertId);
  assert(legalEntityId > 0, "Failed to create legal entity");

  const calendarInsert = await query(
    `INSERT INTO fiscal_calendars (tenant_id, code, name, year_start_month, year_start_day)
     VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `CTR_PR24_CAL_${stamp}`, `Contracts PR24 Calendar ${stamp}`]
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
      `CTR_PR24_BOOK_${stamp}`,
      `Contracts PR24 Book ${stamp}`,
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
    [bookId, fiscalPeriodId, "PR24 open period"]
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
     VALUES (?, ?, ?, ?, 15, 0, FALSE, 'ACTIVE')`,
    [tenantId, legalEntityId, `CTR_PR24_TERM_${stamp}`, `Contracts PR24 Payment Term ${stamp}`]
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
      `CTR_PR24_CP_${stamp}`,
      `Contracts PR24 Customer ${stamp}`,
      paymentTermId,
    ]
  );
  const counterpartyId = toNumber(counterpartyInsert.rows?.insertId);
  assert(counterpartyId > 0, "Failed to create counterparty");

  const coaInsert = await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `CTR_PR24_COA_${stamp}`, `Contracts PR24 CoA ${stamp}`]
  );
  const coaId = toNumber(coaInsert.rows?.insertId);
  assert(coaId > 0, "Failed to create CoA");

  async function createAccount({ code, name, accountType, normalSide }) {
    const insertResult = await query(
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
      [coaId, code, name, accountType, normalSide]
    );
    const accountId = toNumber(insertResult.rows?.insertId);
    assert(accountId > 0, `Failed to create account ${code}`);
    return accountId;
  }

  const deferredAccountId = await createAccount({
    code: `CTR24_DEF_${stamp}`,
    name: `PR24 deferred ${stamp}`,
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  });
  const revenueAccountId = await createAccount({
    code: `CTR24_REV_${stamp}`,
    name: `PR24 revenue ${stamp}`,
    accountType: "REVENUE",
    normalSide: "CREDIT",
  });
  const arControlAccountId = await createAccount({
    code: `CTR24_AR_CTL_${stamp}`,
    name: `PR24 AR control ${stamp}`,
    accountType: "ASSET",
    normalSide: "DEBIT",
  });
  const arOffsetAccountId = await createAccount({
    code: `CTR24_AR_OFF_${stamp}`,
    name: `PR24 AR offset ${stamp}`,
    accountType: "REVENUE",
    normalSide: "CREDIT",
  });

  await query(
    `INSERT INTO journal_purpose_accounts (
        tenant_id,
        legal_entity_id,
        purpose_code,
        account_id
     )
     VALUES (?, ?, 'CARI_AR_CONTROL', ?),
            (?, ?, 'CARI_AR_OFFSET', ?)
     ON DUPLICATE KEY UPDATE account_id = VALUES(account_id)`,
    [
      tenantId,
      legalEntityId,
      arControlAccountId,
      tenantId,
      legalEntityId,
      arOffsetAccountId,
    ]
  );

  return {
    tenantId,
    legalEntityId,
    fiscalPeriodId,
    counterpartyId,
    deferredAccountId,
    revenueAccountId,
  };
}

async function createContract({ token, fixture, stamp }) {
  const createPayload = {
    legalEntityId: fixture.legalEntityId,
    counterpartyId: fixture.counterpartyId,
    contractNo: `CTR_PR24_NO_${stamp}`,
    contractType: "CUSTOMER",
    currencyCode: "USD",
    startDate: "2026-01-01",
    endDate: "2026-12-31",
    notes: "PR24 rollup reconciliation contract",
    lines: [
      {
        description: "PR24 line A",
        lineAmountTxn: 7000,
        lineAmountBase: 7000,
        recognitionMethod: "MILESTONE",
        recognitionStartDate: "2026-03-15",
        recognitionEndDate: "2026-03-15",
        deferredAccountId: fixture.deferredAccountId,
        revenueAccountId: fixture.revenueAccountId,
        status: "ACTIVE",
      },
      {
        description: "PR24 line B",
        lineAmountTxn: 3000,
        lineAmountBase: 3000,
        recognitionMethod: "MILESTONE",
        recognitionStartDate: "2026-05-15",
        recognitionEndDate: "2026-05-15",
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

  const detail = await getContractDetail({ token, contractId });
  const lineRows = Array.isArray(detail?.lines) ? detail.lines : [];
  assert(lineRows.length === 2, "Contract should contain 2 lines");
  const lineAId = toNumber(lineRows[0]?.id);
  const lineBId = toNumber(lineRows[1]?.id);
  assert(lineAId > 0 && lineBId > 0, "Contract line IDs missing");

  return { contractId, lineAId, lineBId };
}

async function getContractDetail({ token, contractId }) {
  const response = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/contracts/${contractId}`,
    expectedStatus: 200,
  });
  assert(response.json?.row, "Contract detail response missing row");
  return response.json.row;
}

function assertRollupAmounts({ rollup, expected, stage }) {
  assert(rollup && typeof rollup === "object", `${stage}: financialRollup missing`);

  const amountFields = [
    "billedAmountTxn",
    "billedAmountBase",
    "collectedAmountTxn",
    "collectedAmountBase",
    "uncollectedAmountTxn",
    "uncollectedAmountBase",
    "revrecScheduledAmountTxn",
    "revrecScheduledAmountBase",
    "recognizedToDateTxn",
    "recognizedToDateBase",
    "deferredBalanceTxn",
    "deferredBalanceBase",
    "openReceivableTxn",
    "openReceivableBase",
    "openPayableTxn",
    "openPayableBase",
    "collectedCoveragePct",
    "recognizedCoveragePct",
  ];
  for (const field of amountFields) {
    if (Object.prototype.hasOwnProperty.call(expected, field)) {
      assert(
        amountsEqual(rollup[field], expected[field]),
        `${stage}: ${field} expected ${expected[field]}, got ${rollup[field]}`
      );
    }
  }

  const countFields = [
    "linkedDocumentCount",
    "activeLinkedDocumentCount",
    "revrecScheduleLineCount",
    "revrecRecognizedRunLineCount",
  ];
  for (const field of countFields) {
    if (Object.prototype.hasOwnProperty.call(expected, field)) {
      assert(
        toNumber(rollup[field]) === toNumber(expected[field]),
        `${stage}: ${field} expected ${expected[field]}, got ${rollup[field]}`
      );
    }
  }
}

async function generateBilling({ token, contractId, lineIds, stamp }) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractId}/generate-billing`,
    body: {
      docType: "INVOICE",
      amountStrategy: "FULL",
      billingDate: "2026-02-25",
      selectedLineIds: lineIds,
      idempotencyKey: `PR24-BILL-${stamp}`,
      note: "PR24 billed amount source",
    },
    expectedStatus: 201,
  });
  const documentId = toNumber(response.json?.document?.id);
  const linkId = toNumber(response.json?.link?.linkId);
  assert(documentId > 0, "generate-billing should return document id");
  assert(linkId > 0, "generate-billing should return link id");
  return { documentId, linkId };
}

async function postCariDocument({ token, documentId }) {
  const postResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/cari/documents/${documentId}/post`,
    body: {},
    expectedStatus: 200,
  });
  assert(
    toUpper(postResponse.json?.row?.status) === "POSTED",
    "Generated cari document should post successfully"
  );
}

async function setPartialCollectionState({
  tenantId,
  legalEntityId,
  documentId,
  remainingOpenAmountTxn,
  remainingOpenAmountBase,
}) {
  const openTxn = Number(remainingOpenAmountTxn);
  const openBase = Number(remainingOpenAmountBase);
  const documentResult = await query(
    `SELECT amount_txn, amount_base
     FROM cari_documents
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, documentId]
  );
  const document = documentResult.rows?.[0] || null;
  assert(document, "Partial collection update failed: document not found");
  const amountTxn = toNumber(document.amount_txn);
  const amountBase = toNumber(document.amount_base);
  const settledTxn = Number((amountTxn - openTxn).toFixed(6));
  const settledBase = Number((amountBase - openBase).toFixed(6));

  await query(
    `UPDATE cari_documents
     SET open_amount_txn = ?,
         open_amount_base = ?,
         status = ?
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?`,
    [openTxn, openBase, "PARTIALLY_SETTLED", tenantId, legalEntityId, documentId]
  );
  await query(
    `UPDATE cari_open_items
     SET status = ?,
         residual_amount_txn = ?,
         residual_amount_base = ?,
         settled_amount_txn = ?,
         settled_amount_base = ?
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND document_id = ?`,
    [ "PARTIALLY_SETTLED", openTxn, openBase, settledTxn, settledBase, tenantId, legalEntityId, documentId ]
  );
}

async function generateContractRevrec({ token, contractId, fiscalPeriodId, lineIds }) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/contracts/${contractId}/generate-revrec`,
    expectedStatus: 201,
    body: {
      fiscalPeriodId,
      generationMode: "BY_CONTRACT_LINE",
      regenerateMissingOnly: true,
      contractLineIds: lineIds,
    },
  });
  assert(
    toNumber(response.json?.generatedLineCount) === 2,
    "RevRec generation should create one schedule line per milestone contract line"
  );
}

async function loadScheduleIdByContractLineId({ tenantId, legalEntityId, contractLineId }) {
  const result = await query(
    `SELECT rrsl.schedule_id
     FROM revenue_recognition_schedule_lines rrsl
     WHERE rrsl.tenant_id = ?
       AND rrsl.legal_entity_id = ?
       AND rrsl.source_contract_line_id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, contractLineId]
  );
  return toNumber(result.rows?.[0]?.schedule_id);
}

async function createRunFromSchedule({
  token,
  tenantId,
  legalEntityId,
  scheduleId,
  fiscalPeriodId,
  sourceRunUid,
  runNo,
}) {
  const scheduleResult = await query(
    `SELECT
        id,
        account_family,
        maturity_bucket,
        maturity_date,
        reclass_required,
        currency_code,
        fx_rate,
        amount_txn,
        amount_base
     FROM revenue_recognition_schedules
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, scheduleId]
  );
  const schedule = scheduleResult.rows?.[0];
  assert(schedule, `Schedule ${scheduleId} not found`);

  const runCreate = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/runs",
    expectedStatus: 201,
    body: {
      legalEntityId,
      fiscalPeriodId,
      scheduleId,
      sourceRunUid,
      runNo,
      accountFamily: schedule.account_family,
      maturityBucket: schedule.maturity_bucket,
      maturityDate: toDateOnlyString(schedule.maturity_date),
      reclassRequired: Number(schedule.reclass_required || 0) > 0,
      currencyCode: schedule.currency_code,
      fxRate: toNumber(schedule.fx_rate || 1),
      totalAmountTxn: toNumber(schedule.amount_txn),
      totalAmountBase: toNumber(schedule.amount_base),
    },
  });
  const runId = toNumber(runCreate.json?.row?.id);
  assert(runId > 0, "Run create response missing id");
  return runId;
}

async function postRun({ token, runId }) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${runId}/post`,
    expectedStatus: 200,
    body: {},
  });
  assert(toUpper(response.json?.row?.status) === "POSTED", "Run should be POSTED");
}

async function main() {
  await runMigrations();

  const server = startServerProcess();
  try {
    await waitForServer();

    const identity = await createTenantAndAdmin();
    const fixture = await createFixture({
      tenantId: identity.tenantId,
      stamp: identity.stamp,
    });
    const token = await login(identity.email, TEST_PASSWORD);

    const contract = await createContract({
      token,
      fixture,
      stamp: identity.stamp,
    });

    const initialDetail = await getContractDetail({
      token,
      contractId: contract.contractId,
    });
    assertRollupAmounts({
      rollup: initialDetail.financialRollup,
      stage: "initial",
      expected: {
        billedAmountBase: 0,
        collectedAmountBase: 0,
        uncollectedAmountBase: 0,
        revrecScheduledAmountBase: 0,
        recognizedToDateBase: 0,
        deferredBalanceBase: 0,
        openReceivableBase: 0,
        openPayableBase: 0,
        linkedDocumentCount: 0,
        activeLinkedDocumentCount: 0,
        revrecScheduleLineCount: 0,
        revrecRecognizedRunLineCount: 0,
      },
    });

    const billing = await generateBilling({
      token,
      contractId: contract.contractId,
      lineIds: [contract.lineAId, contract.lineBId],
      stamp: identity.stamp,
    });
    await postCariDocument({ token, documentId: billing.documentId });

    const billedDetail = await getContractDetail({
      token,
      contractId: contract.contractId,
    });
    assertRollupAmounts({
      rollup: billedDetail.financialRollup,
      stage: "billed-only",
      expected: {
        billedAmountTxn: 10000,
        billedAmountBase: 10000,
        collectedAmountTxn: 0,
        collectedAmountBase: 0,
        uncollectedAmountTxn: 10000,
        uncollectedAmountBase: 10000,
        revrecScheduledAmountTxn: 0,
        revrecScheduledAmountBase: 0,
        recognizedToDateTxn: 0,
        recognizedToDateBase: 0,
        deferredBalanceTxn: 0,
        deferredBalanceBase: 0,
        openReceivableTxn: 10000,
        openReceivableBase: 10000,
        openPayableTxn: 0,
        openPayableBase: 0,
        collectedCoveragePct: 0,
        recognizedCoveragePct: 0,
        linkedDocumentCount: 1,
        activeLinkedDocumentCount: 1,
        revrecScheduleLineCount: 0,
        revrecRecognizedRunLineCount: 0,
      },
    });

    await setPartialCollectionState({
      tenantId: identity.tenantId,
      legalEntityId: fixture.legalEntityId,
      documentId: billing.documentId,
      remainingOpenAmountTxn: 4000,
      remainingOpenAmountBase: 4000,
    });

    await generateContractRevrec({
      token,
      contractId: contract.contractId,
      fiscalPeriodId: fixture.fiscalPeriodId,
      lineIds: [contract.lineAId, contract.lineBId],
    });

    const lineAScheduleId = await loadScheduleIdByContractLineId({
      tenantId: identity.tenantId,
      legalEntityId: fixture.legalEntityId,
      contractLineId: contract.lineAId,
    });
    assert(lineAScheduleId > 0, "Line A schedule id missing");

    const runId = await createRunFromSchedule({
      token,
      tenantId: identity.tenantId,
      legalEntityId: fixture.legalEntityId,
      scheduleId: lineAScheduleId,
      fiscalPeriodId: fixture.fiscalPeriodId,
      sourceRunUid: `PR24-RUN-${identity.stamp}`,
      runNo: `PR24-RUN-${identity.stamp}`,
    });
    await postRun({ token, runId });

    const finalDetail = await getContractDetail({
      token,
      contractId: contract.contractId,
    });
    assertRollupAmounts({
      rollup: finalDetail.financialRollup,
      stage: "final-rollup",
      expected: {
        billedAmountTxn: 10000,
        billedAmountBase: 10000,
        collectedAmountTxn: 6000,
        collectedAmountBase: 6000,
        uncollectedAmountTxn: 4000,
        uncollectedAmountBase: 4000,
        revrecScheduledAmountTxn: 10000,
        revrecScheduledAmountBase: 10000,
        recognizedToDateTxn: 7000,
        recognizedToDateBase: 7000,
        deferredBalanceTxn: 3000,
        deferredBalanceBase: 3000,
        openReceivableTxn: 4000,
        openReceivableBase: 4000,
        openPayableTxn: 0,
        openPayableBase: 0,
        collectedCoveragePct: 60,
        recognizedCoveragePct: 70,
        linkedDocumentCount: 1,
        activeLinkedDocumentCount: 1,
        revrecScheduleLineCount: 2,
        revrecRecognizedRunLineCount: 1,
      },
    });
    assert(
      toUpper(finalDetail.financialRollup?.currencyCode) === "USD",
      "financialRollup.currencyCode should be USD"
    );

    console.log("Contracts PR-24 financial rollups integration test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          legalEntityId: fixture.legalEntityId,
          contractId: contract.contractId,
          linkedDocumentId: billing.documentId,
          postedRunId: runId,
          finalRollup: finalDetail.financialRollup,
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
  console.error("Contracts PR-24 financial rollups integration test failed:", error);
  process.exitCode = 1;
});
