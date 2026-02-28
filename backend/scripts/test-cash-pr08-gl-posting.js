import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CASH_PR08_TEST_PORT || 3113);
const BASE_URL = process.env.CASH_PR08_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const BALANCE_EPSILON = 0.0001;
const TEST_FISCAL_YEAR = 2026;

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function toErrorText(jsonPayload) {
  if (jsonPayload === null || jsonPayload === undefined) {
    return "";
  }
  if (typeof jsonPayload === "string") {
    return jsonPayload;
  }
  if (typeof jsonPayload.error === "string") {
    return jsonPayload.error;
  }
  if (typeof jsonPayload.message === "string") {
    return jsonPayload.message;
  }
  try {
    return JSON.stringify(jsonPayload);
  } catch {
    return String(jsonPayload);
  }
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
  const cookie = setCookieHeader ? String(setCookieHeader).split(";")[0].trim() : null;

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
    await sleep(350);
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
  const sessionCookie = response.cookie;
  assert(Boolean(sessionCookie), "Login cookie missing");
  return sessionCookie;
}

async function createTenantAndAdmin() {
  const stamp = Date.now();
  const tenantCode = `CASH08_${stamp}`;
  const tenantName = `Cash PR08 ${stamp}`;
  const adminEmail = `cash_pr08_admin_${stamp}@example.com`;
  const password = "CashPR08#12345";
  const passwordHash = await bcrypt.hash(password, 10);

  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE name = VALUES(name)`,
    [tenantCode, tenantName]
  );

  await seedCore({
    ensureDefaultTenantIfMissing: true,
  });

  const tenantResult = await query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [tenantCode]
  );
  const tenantId = toNumber(tenantResult.rows[0]?.id);
  assert(tenantId > 0, "Failed to resolve tenant");

  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, adminEmail, passwordHash, "Cash PR08 Admin"]
  );

  const userResult = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, adminEmail]
  );
  const userId = toNumber(userResult.rows[0]?.id);
  assert(userId > 0, "Failed to resolve admin user");

  const roleResult = await query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = 'TenantAdmin'
     LIMIT 1`,
    [tenantId]
  );
  const roleId = toNumber(roleResult.rows[0]?.id);
  assert(roleId > 0, "Failed to resolve TenantAdmin role");

  await query(
    `INSERT INTO user_role_scopes (
        tenant_id, user_id, role_id, scope_type, scope_id, effect
     )
     VALUES (?, ?, ?, 'TENANT', ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE effect = VALUES(effect)`,
    [tenantId, userId, roleId, tenantId]
  );

  return {
    tenantId,
    userId,
    adminEmail,
    password,
    stamp,
  };
}

async function createAccount({
  token,
  coaId,
  code,
  name,
  accountType = "ASSET",
  normalSide = "DEBIT",
  allowPosting = true,
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/accounts",
    body: {
      coaId,
      code,
      name,
      accountType,
      normalSide,
      allowPosting,
    },
    expectedStatus: 201,
  });
  const accountId = toNumber(response.json?.id);
  assert(accountId > 0, `Account not created for code=${code}`);
  return accountId;
}

async function createRegister({
  token,
  tenantId,
  legalEntityId,
  operatingUnitId,
  accountId,
  code,
  name,
  currencyCode,
  sessionMode = "OPTIONAL",
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/cash/registers",
    body: {
      tenantId,
      legalEntityId,
      operatingUnitId,
      accountId,
      code,
      name,
      registerType: "DRAWER",
      sessionMode,
      currencyCode,
      status: "ACTIVE",
    },
    expectedStatus: 200,
  });

  const registerId = toNumber(response.json?.row?.id);
  assert(registerId > 0, `Register not created for code=${code}`);
  return registerId;
}

async function createCashTransaction({
  token,
  tenantId,
  registerId,
  txnType,
  amount,
  currencyCode,
  idempotencyKey,
  counterAccountId = null,
  counterCashRegisterId = null,
  description = null,
  bookDate = null,
  expectedStatus = 200,
}) {
  return apiRequest({
    token,
    method: "POST",
    path: "/api/v1/cash/transactions",
    body: {
      tenantId,
      registerId,
      txnType,
      amount,
      currencyCode,
      counterAccountId,
      counterCashRegisterId,
      description,
      idempotencyKey,
      bookDate,
    },
    expectedStatus,
  });
}

async function postCashTransaction({
  token,
  tenantId,
  transactionId,
  expectedStatus = 200,
}) {
  return apiRequest({
    token,
    method: "POST",
    path: `/api/v1/cash/transactions/${transactionId}/post`,
    body: {
      tenantId,
    },
    expectedStatus,
  });
}

async function resolvePeriodIdForDate({ calendarId, bookDate }) {
  const result = await query(
    `SELECT id
     FROM fiscal_periods
     WHERE calendar_id = ?
       AND ? BETWEEN start_date AND end_date
     ORDER BY is_adjustment ASC, id ASC
     LIMIT 1`,
    [calendarId, bookDate]
  );
  const periodId = toNumber(result.rows[0]?.id);
  assert(periodId > 0, `No fiscal period found for ${bookDate}`);
  return periodId;
}

async function markPeriodSoftClosed({ bookId, fiscalPeriodId, userId }) {
  await query(
    `INSERT INTO period_statuses (
       book_id, fiscal_period_id, status, closed_by_user_id, closed_at, note
     )
     VALUES (?, ?, 'SOFT_CLOSED', ?, UTC_TIMESTAMP(), ?)
     ON DUPLICATE KEY UPDATE
       status = VALUES(status),
       closed_by_user_id = VALUES(closed_by_user_id),
       closed_at = VALUES(closed_at),
       note = VALUES(note)`,
    [bookId, fiscalPeriodId, userId, "PR08 period-close guard test"]
  );
}

async function assertPostedJournalIntegrity({
  tenantId,
  transactionId,
  expectedReference,
}) {
  const cashTxnResult = await query(
    `SELECT id, status, posted_journal_entry_id
     FROM cash_transactions
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, transactionId]
  );
  const cashTxn = cashTxnResult.rows[0] || null;
  assert(Boolean(cashTxn), `cash_transactions row missing for id=${transactionId}`);
  assert(String(cashTxn.status || "").toUpperCase() === "POSTED", "Cash txn must be POSTED");

  const journalEntryId = toNumber(cashTxn.posted_journal_entry_id);
  assert(journalEntryId > 0, "posted_journal_entry_id must be populated");

  const journalResult = await query(
    `SELECT id, source_type, status, total_debit_base, total_credit_base
     FROM journal_entries
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [journalEntryId, tenantId]
  );
  const journal = journalResult.rows[0] || null;
  assert(Boolean(journal), `Journal not found for id=${journalEntryId}`);
  assert(String(journal.source_type || "").toUpperCase() === "CASH", "source_type must be CASH");
  assert(String(journal.status || "").toUpperCase() === "POSTED", "Journal status must be POSTED");

  const linesResult = await query(
    `SELECT
       COUNT(*) AS total_lines,
       COALESCE(SUM(debit_base), 0) AS total_debit,
       COALESCE(SUM(credit_base), 0) AS total_credit,
       SUM(CASE WHEN subledger_reference_no = ? THEN 1 ELSE 0 END) AS matching_subledger_refs
     FROM journal_lines
     WHERE journal_entry_id = ?`,
    [expectedReference, journalEntryId]
  );
  const totals = linesResult.rows[0] || {};
  const totalLines = toNumber(totals.total_lines);
  const totalDebit = Number(totals.total_debit || 0);
  const totalCredit = Number(totals.total_credit || 0);
  const matchingSubledgerRefs = toNumber(totals.matching_subledger_refs);

  assert(totalLines >= 2, "Cash journal must contain at least 2 lines");
  assert(
    Math.abs(totalDebit - totalCredit) <= BALANCE_EPSILON,
    `Cash journal must be balanced (debit=${totalDebit}, credit=${totalCredit})`
  );
  assert(
    matchingSubledgerRefs === totalLines,
    "All cash journal lines must include CASH_TXN subledger reference"
  );

  return {
    journalEntryId,
    totalLines,
    totalDebit,
    totalCredit,
  };
}

async function bootstrapCashPostingContext(token, identity) {
  const countryResult = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(countryResult.rows[0]?.id);
  const currencyCode = String(countryResult.rows[0]?.default_currency_code || "USD").toUpperCase();
  assert(countryId > 0, "US country row is required");

  const groupRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/group-companies",
    body: {
      code: `CG${identity.stamp}`,
      name: `Cash PR08 Group ${identity.stamp}`,
    },
    expectedStatus: 201,
  });
  const groupCompanyId = toNumber(groupRes.json?.id);
  assert(groupCompanyId > 0, "groupCompanyId not created");

  const calendarRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/fiscal-calendars",
    body: {
      code: `CAL${identity.stamp}`,
      name: `Cash PR08 Calendar ${identity.stamp}`,
      yearStartMonth: 1,
      yearStartDay: 1,
    },
    expectedStatus: 201,
  });
  const calendarId = toNumber(calendarRes.json?.id);
  assert(calendarId > 0, "calendarId not created");

  await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/fiscal-periods/generate",
    body: {
      calendarId,
      fiscalYear: TEST_FISCAL_YEAR,
    },
    expectedStatus: 201,
  });

  const legalEntityARes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `LEA${identity.stamp}`,
      name: `Cash LE A ${identity.stamp}`,
      countryId,
      functionalCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const legalEntityAId = toNumber(legalEntityARes.json?.id);
  assert(legalEntityAId > 0, "legalEntityAId not created");

  const legalEntityBRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `LEB${identity.stamp}`,
      name: `Cash LE B ${identity.stamp}`,
      countryId,
      functionalCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const legalEntityBId = toNumber(legalEntityBRes.json?.id);
  assert(legalEntityBId > 0, "legalEntityBId not created");

  const ouA1Res = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/operating-units",
    body: {
      legalEntityId: legalEntityAId,
      code: `OUA1${identity.stamp}`,
      name: "Cash OU A1",
      unitType: "BRANCH",
      hasSubledger: true,
    },
    expectedStatus: 201,
  });
  const operatingUnitA1Id = toNumber(ouA1Res.json?.id);
  assert(operatingUnitA1Id > 0, "operatingUnitA1Id not created");

  const ouA2Res = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/operating-units",
    body: {
      legalEntityId: legalEntityAId,
      code: `OUA2${identity.stamp}`,
      name: "Cash OU A2",
      unitType: "BRANCH",
      hasSubledger: true,
    },
    expectedStatus: 201,
  });
  const operatingUnitA2Id = toNumber(ouA2Res.json?.id);
  assert(operatingUnitA2Id > 0, "operatingUnitA2Id not created");

  const bookRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/books",
    body: {
      legalEntityId: legalEntityAId,
      calendarId,
      code: `BOOK${identity.stamp}`,
      name: `Cash Book ${identity.stamp}`,
      bookType: "LOCAL",
      baseCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const bookId = toNumber(bookRes.json?.id);
  assert(bookId > 0, "bookId not created");

  const coaARes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/coas",
    body: {
      scope: "LEGAL_ENTITY",
      legalEntityId: legalEntityAId,
      code: `COAA${identity.stamp}`,
      name: `Cash CoA A ${identity.stamp}`,
    },
    expectedStatus: 201,
  });
  const coaAId = toNumber(coaARes.json?.id);
  assert(coaAId > 0, "coaAId not created");

  const coaBRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/coas",
    body: {
      scope: "LEGAL_ENTITY",
      legalEntityId: legalEntityBId,
      code: `COAB${identity.stamp}`,
      name: `Cash CoA B ${identity.stamp}`,
    },
    expectedStatus: 201,
  });
  const coaBId = toNumber(coaBRes.json?.id);
  assert(coaBId > 0, "coaBId not created");

  const registerAccountAId = await createAccount({
    token,
    coaId: coaAId,
    code: `CRA${identity.stamp}`,
    name: "Cash Register A",
    accountType: "ASSET",
    normalSide: "DEBIT",
  });
  const registerAccountBId = await createAccount({
    token,
    coaId: coaAId,
    code: `CRB${identity.stamp}`,
    name: "Cash Register B",
    accountType: "ASSET",
    normalSide: "DEBIT",
  });
  const registerAccountCrossId = await createAccount({
    token,
    coaId: coaAId,
    code: `CRC${identity.stamp}`,
    name: "Cash Register Cross",
    accountType: "ASSET",
    normalSide: "DEBIT",
  });
  const counterExpenseAccountId = await createAccount({
    token,
    coaId: coaAId,
    code: `CEX${identity.stamp}`,
    name: "Cash Counter Expense",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  });
  const bankAccountId = await createAccount({
    token,
    coaId: coaAId,
    code: `CBK${identity.stamp}`,
    name: "Bank Account",
    accountType: "ASSET",
    normalSide: "DEBIT",
  });
  const foreignCounterAccountId = await createAccount({
    token,
    coaId: coaBId,
    code: `FCT${identity.stamp}`,
    name: "Foreign Counter Account",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  });

  const registerAId = await createRegister({
    token,
    tenantId: identity.tenantId,
    legalEntityId: legalEntityAId,
    operatingUnitId: operatingUnitA1Id,
    accountId: registerAccountAId,
    code: `RGA${identity.stamp}`,
    name: "Register A",
    currencyCode,
  });
  const registerBId = await createRegister({
    token,
    tenantId: identity.tenantId,
    legalEntityId: legalEntityAId,
    operatingUnitId: operatingUnitA1Id,
    accountId: registerAccountBId,
    code: `RGB${identity.stamp}`,
    name: "Register B",
    currencyCode,
  });
  const registerCrossOuId = await createRegister({
    token,
    tenantId: identity.tenantId,
    legalEntityId: legalEntityAId,
    operatingUnitId: operatingUnitA2Id,
    accountId: registerAccountCrossId,
    code: `RGC${identity.stamp}`,
    name: "Register Cross OU",
    currencyCode,
  });

  const openPeriodId = await resolvePeriodIdForDate({
    calendarId,
    bookDate: `${TEST_FISCAL_YEAR}-06-15`,
  });

  return {
    groupCompanyId,
    calendarId,
    legalEntityAId,
    legalEntityBId,
    operatingUnitA1Id,
    operatingUnitA2Id,
    bookId,
    openPeriodId,
    currencyCode,
    registerAId,
    registerBId,
    registerCrossOuId,
    counterExpenseAccountId,
    bankAccountId,
    foreignCounterAccountId,
  };
}

async function createAndPostTxnWithAssertions({
  token,
  identity,
  setup,
  txnType,
  amount,
  counterAccountId = null,
  counterCashRegisterId = null,
  bookDate = `${TEST_FISCAL_YEAR}-06-15`,
  idempotencySuffix,
}) {
  const createRes = await createCashTransaction({
    token,
    tenantId: identity.tenantId,
    registerId: setup.registerAId,
    txnType,
    amount,
    currencyCode: setup.currencyCode,
    counterAccountId,
    counterCashRegisterId,
    idempotencyKey: `PR08-${idempotencySuffix}-${identity.stamp}`,
    description: `PR08 ${txnType}`,
    bookDate,
    expectedStatus: 200,
  });
  const createdTxnId = toNumber(createRes.json?.row?.id);
  assert(createdTxnId > 0, `Failed to create ${txnType} transaction`);

  const postRes = await postCashTransaction({
    token,
    tenantId: identity.tenantId,
    transactionId: createdTxnId,
    expectedStatus: 200,
  });

  const postedRow = postRes.json?.row || null;
  assert(Boolean(postedRow), `${txnType} post response row missing`);
  assert(String(postedRow.status || "").toUpperCase() === "POSTED", `${txnType} must be POSTED`);
  assert(toNumber(postedRow.posted_journal_entry_id) > 0, `${txnType} must set posted_journal_entry_id`);

  await assertPostedJournalIntegrity({
    tenantId: identity.tenantId,
    transactionId: createdTxnId,
    expectedReference: `CASH_TXN:${createdTxnId}`,
  });

  return {
    txnId: createdTxnId,
    row: postedRow,
  };
}

async function main() {
  const identity = await createTenantAndAdmin();
  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const adminToken = await login(identity.adminEmail, identity.password);
    const setup = await bootstrapCashPostingContext(adminToken, identity);

    const postingSpecs = [
      {
        txnType: "RECEIPT",
        amount: "120.00",
        counterAccountId: setup.counterExpenseAccountId,
        idempotencySuffix: "RECEIPT",
      },
      {
        txnType: "PAYOUT",
        amount: "35.00",
        counterAccountId: setup.counterExpenseAccountId,
        idempotencySuffix: "PAYOUT",
      },
      {
        txnType: "DEPOSIT_TO_BANK",
        amount: "60.00",
        counterAccountId: setup.bankAccountId,
        idempotencySuffix: "DEPOSIT",
      },
      {
        txnType: "WITHDRAWAL_FROM_BANK",
        amount: "25.00",
        counterAccountId: setup.bankAccountId,
        idempotencySuffix: "WITHDRAW",
      },
      {
        txnType: "OPENING_FLOAT",
        amount: "40.00",
        counterAccountId: setup.counterExpenseAccountId,
        idempotencySuffix: "OPENING",
      },
      {
        txnType: "CLOSING_ADJUSTMENT",
        amount: "15.00",
        counterAccountId: setup.counterExpenseAccountId,
        idempotencySuffix: "CLOSING",
      },
      {
        txnType: "TRANSFER_OUT",
        amount: "18.00",
        counterCashRegisterId: setup.registerBId,
        idempotencySuffix: "TFOUT",
      },
      {
        txnType: "TRANSFER_IN",
        amount: "22.00",
        counterCashRegisterId: setup.registerBId,
        idempotencySuffix: "TFIN",
      },
    ];

    const postedRows = [];
    for (const spec of postingSpecs) {
      // eslint-disable-next-line no-await-in-loop
      const posted = await createAndPostTxnWithAssertions({
        token: adminToken,
        identity,
        setup,
        txnType: spec.txnType,
        amount: spec.amount,
        counterAccountId: spec.counterAccountId || null,
        counterCashRegisterId: spec.counterCashRegisterId || null,
        idempotencySuffix: spec.idempotencySuffix,
      });
      postedRows.push(posted);
    }

    // Manual CASH source in GL journal endpoint is blocked (reserved to cash module).
    const cashSourceManualJournalRes = await apiRequest({
      token: adminToken,
      method: "POST",
      path: "/api/v1/gl/journals",
      body: {
        legalEntityId: setup.legalEntityAId,
        bookId: setup.bookId,
        fiscalPeriodId: setup.openPeriodId,
        sourceType: "CASH",
        entryDate: `${TEST_FISCAL_YEAR}-06-15`,
        documentDate: `${TEST_FISCAL_YEAR}-06-15`,
        currencyCode: setup.currencyCode,
        lines: [
          {
            accountId: setup.counterExpenseAccountId,
            operatingUnitId: setup.operatingUnitA1Id,
            subledgerReferenceNo: "MANUAL-CASH-BLOCK-1",
            debitBase: 10,
            creditBase: 0,
          },
          {
            accountId: setup.counterExpenseAccountId,
            operatingUnitId: setup.operatingUnitA1Id,
            subledgerReferenceNo: "MANUAL-CASH-BLOCK-2",
            debitBase: 0,
            creditBase: 10,
          },
        ],
      },
      expectedStatus: 400,
    });
    assert(
      toErrorText(cashSourceManualJournalRes.json).includes("sourceType=CASH is reserved"),
      "Manual sourceType=CASH must be blocked on GL journal create endpoint"
    );

    // Cross-OU transfer is blocked in v1 (CASH_IN_TRANSIT planned for v2).
    const crossOuCreate = await createCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: setup.registerAId,
      txnType: "TRANSFER_OUT",
      amount: "11.00",
      currencyCode: setup.currencyCode,
      counterCashRegisterId: setup.registerCrossOuId,
      idempotencyKey: `PR08-CROSS-OU-${identity.stamp}`,
      description: "Cross OU transfer should fail in v1",
      expectedStatus: 200,
    });
    const crossOuTxnId = toNumber(crossOuCreate.json?.row?.id);
    assert(crossOuTxnId > 0, "Cross-OU transfer txn create must succeed before post checks");

    const crossOuPost = await postCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      transactionId: crossOuTxnId,
      expectedStatus: 400,
    });
    assert(
      toErrorText(crossOuPost.json).includes("CASH_IN_TRANSIT"),
      "Cross-OU transfer post must direct to CASH_IN_TRANSIT flow"
    );

    // GL line/scope validation is reused: wrong legal-entity account fails at post time.
    const wrongScopeCreate = await createCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: setup.registerAId,
      txnType: "RECEIPT",
      amount: "9.00",
      currencyCode: setup.currencyCode,
      counterAccountId: setup.foreignCounterAccountId,
      idempotencyKey: `PR08-SCOPE-${identity.stamp}`,
      description: "Wrong legal entity counter account",
      expectedStatus: 200,
    });
    const wrongScopeTxnId = toNumber(wrongScopeCreate.json?.row?.id);
    assert(wrongScopeTxnId > 0, "Wrong-scope txn create must succeed before post checks");

    const wrongScopePost = await postCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      transactionId: wrongScopeTxnId,
      expectedStatus: 400,
    });
    assert(
      toErrorText(wrongScopePost.json).includes("does not belong to legalEntityId"),
      "Wrong legal-entity counter account must fail with GL line scope validation"
    );

    // Period-open guard is reused: SOFT_CLOSED period blocks posting.
    const closedPeriodDate = `${TEST_FISCAL_YEAR}-12-15`;
    const closedPeriodTxnCreate = await createCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: setup.registerAId,
      txnType: "RECEIPT",
      amount: "13.00",
      currencyCode: setup.currencyCode,
      counterAccountId: setup.counterExpenseAccountId,
      idempotencyKey: `PR08-PERIOD-CLOSED-${identity.stamp}`,
      description: "Closed period post must fail",
      bookDate: closedPeriodDate,
      expectedStatus: 200,
    });
    const closedPeriodTxnId = toNumber(closedPeriodTxnCreate.json?.row?.id);
    assert(closedPeriodTxnId > 0, "Closed-period txn create must succeed before post checks");

    const closedPeriodId = await resolvePeriodIdForDate({
      calendarId: setup.calendarId,
      bookDate: closedPeriodDate,
    });
    await markPeriodSoftClosed({
      bookId: setup.bookId,
      fiscalPeriodId: closedPeriodId,
      userId: identity.userId,
    });

    const closedPeriodPost = await postCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      transactionId: closedPeriodTxnId,
      expectedStatus: 400,
    });
    assert(
      toErrorText(closedPeriodPost.json).includes("Period is SOFT_CLOSED"),
      "Posting must fail for SOFT_CLOSED period"
    );

    // Reversal creates linked posted opposite transaction + CASH journal.
    const firstPostedTxnId = toNumber(postedRows[0]?.txnId);
    assert(firstPostedTxnId > 0, "Need a posted transaction for reversal test");
    const reverseRes = await apiRequest({
      token: adminToken,
      method: "POST",
      path: `/api/v1/cash/transactions/${firstPostedTxnId}/reverse`,
      body: {
        tenantId: identity.tenantId,
        reverseReason: "PR08 reversal validation",
      },
      expectedStatus: 200,
    });

    const reversedOriginal = reverseRes.json?.original || null;
    const reversalRow = reverseRes.json?.reversal || null;
    assert(Boolean(reversalRow), "reverse endpoint must return reversal row");
    assert(
      String(reversedOriginal?.status || "").toUpperCase() === "REVERSED",
      "Original transaction must be marked REVERSED"
    );
    assert(
      String(reversalRow?.status || "").toUpperCase() === "POSTED",
      "Reversal transaction must be POSTED"
    );
    assert(
      toNumber(reversalRow?.reversal_of_transaction_id) === firstPostedTxnId,
      "Reversal must link to original transaction"
    );

    const reversalTxnId = toNumber(reversalRow?.id);
    assert(reversalTxnId > 0, "Reversal transaction id missing");
    await assertPostedJournalIntegrity({
      tenantId: identity.tenantId,
      transactionId: reversalTxnId,
      expectedReference: `CASH_TXN:${reversalTxnId}`,
    });

    console.log("PR08 cash GL posting integration checks passed.");
  } finally {
    if (!serverStopped) {
      server.kill("SIGINT");
      serverStopped = true;
    }
    await sleep(400);
    await closePool();
  }
}

main()
  .then(() => {
    process.exitCode = 0;
  })
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
