import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CASH_EXCEPTIONS_TEST_PORT || 3117);
const BASE_URL =
  process.env.CASH_EXCEPTIONS_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TODAY_DATE = new Date().toISOString().slice(0, 10);
const TEST_FISCAL_YEAR = Number(TODAY_DATE.slice(0, 4));

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
  if (!jsonPayload) {
    return "";
  }
  if (typeof jsonPayload === "string") {
    return jsonPayload;
  }
  if (typeof jsonPayload.message === "string") {
    return jsonPayload.message;
  }
  if (typeof jsonPayload.error === "string") {
    return jsonPayload.error;
  }
  try {
    return JSON.stringify(jsonPayload);
  } catch {
    return String(jsonPayload);
  }
}

function toQueryString(query = {}) {
  const pairs = Object.entries(query).filter(([, value]) => {
    return value !== undefined && value !== null && value !== "";
  });
  if (pairs.length === 0) {
    return "";
  }

  const searchParams = new URLSearchParams();
  for (const [key, value] of pairs) {
    if (Array.isArray(value)) {
      for (const item of value) {
        searchParams.append(key, String(item));
      }
    } else {
      searchParams.append(key, String(value));
    }
  }
  const serialized = searchParams.toString();
  return serialized ? `?${serialized}` : "";
}

async function apiRequest({
  token,
  method = "GET",
  path,
  queryParams,
  body,
  expectedStatus,
}) {
  const headers = { "Content-Type": "application/json" };
  if (token) {
    headers.Cookie = token;
  }

  const queryString = toQueryString(queryParams);
  const response = await fetch(`${BASE_URL}${path}${queryString}`, {
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

  return { status: response.status, json, cookie, headers: response.headers };
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
    env: {
      ...process.env,
      PORT: String(PORT),
      GL_CASH_CONTROL_MODE: "WARN",
    },
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
  const tenantCode = `CASHEXC_${stamp}`;
  const tenantName = `Cash Exceptions ${stamp}`;
  const adminEmail = `cash_exceptions_admin_${stamp}@example.com`;
  const password = "CashExc#12345";
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
    [tenantId, adminEmail, passwordHash, "Cash Exceptions Admin"]
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

async function bootstrapContext(token, identity) {
  const countryResult = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(countryResult.rows[0]?.id);
  const currencyCode = String(
    countryResult.rows[0]?.default_currency_code || "USD"
  ).toUpperCase();
  assert(countryId > 0, "US country row is required");

  const groupRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/group-companies",
    body: {
      code: `CG${identity.stamp}`,
      name: `Cash Exceptions Group ${identity.stamp}`,
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
      name: `Cash Exceptions Calendar ${identity.stamp}`,
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

  const periodResult = await query(
    `SELECT id
     FROM fiscal_periods
     WHERE calendar_id = ?
       AND ? BETWEEN start_date AND end_date
       AND is_adjustment = FALSE
     ORDER BY period_no ASC
     LIMIT 1`,
    [calendarId, TODAY_DATE]
  );
  const fiscalPeriodId = toNumber(periodResult.rows?.[0]?.id);
  assert(
    fiscalPeriodId > 0,
    `fiscalPeriodId not found for date=${TODAY_DATE} year=${TEST_FISCAL_YEAR}`
  );

  const legalEntityRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `LE${identity.stamp}`,
      name: `Cash Exceptions Entity ${identity.stamp}`,
      countryId,
      functionalCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const legalEntityId = toNumber(legalEntityRes.json?.id);
  assert(legalEntityId > 0, "legalEntityId not created");

  const operatingUnitRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/operating-units",
    body: {
      legalEntityId,
      code: `OU${identity.stamp}`,
      name: "Cash Exceptions Branch",
      unitType: "BRANCH",
      hasSubledger: true,
    },
    expectedStatus: 201,
  });
  const operatingUnitId = toNumber(operatingUnitRes.json?.id);
  assert(operatingUnitId > 0, "operatingUnitId not created");

  const bookRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/books",
    body: {
      legalEntityId,
      calendarId,
      code: `BOOK${identity.stamp}`,
      name: `Cash Exceptions Book ${identity.stamp}`,
      bookType: "LOCAL",
      baseCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const bookId = toNumber(bookRes.json?.id);
  assert(bookId > 0, "bookId not created");

  const coaRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/coas",
    body: {
      scope: "LEGAL_ENTITY",
      legalEntityId,
      code: `COA${identity.stamp}`,
      name: `Cash Exceptions CoA ${identity.stamp}`,
    },
    expectedStatus: 201,
  });
  const coaId = toNumber(coaRes.json?.id);
  assert(coaId > 0, "coaId not created");

  return {
    currencyCode,
    legalEntityId,
    operatingUnitId,
    bookId,
    fiscalPeriodId,
    coaId,
  };
}

async function createAccount({
  token,
  coaId,
  code,
  name,
  accountType = "ASSET",
  normalSide = "DEBIT",
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
  currencyCode,
  varianceGainAccountId,
  varianceLossAccountId,
  stamp,
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
      code: `REG${stamp}`,
      name: `Exceptions Register ${stamp}`,
      registerType: "DRAWER",
      sessionMode: "OPTIONAL",
      currencyCode,
      allowNegative: false,
      varianceGainAccountId,
      varianceLossAccountId,
      requiresApprovalOverAmount: "250.00",
      status: "ACTIVE",
    },
    expectedStatus: 200,
  });
  const registerId = toNumber(response.json?.row?.id);
  assert(registerId > 0, "Register not created");
  return registerId;
}

async function openCashSession({ token, tenantId, registerId, openingAmount }) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/cash/sessions/open",
    body: {
      tenantId,
      registerId,
      openingAmount,
    },
    expectedStatus: 200,
  });
  const sessionId = toNumber(response.json?.row?.id);
  assert(sessionId > 0, "Session did not open");
  return {
    sessionId,
    row: response.json?.row || null,
  };
}

async function closeCashSession({
  token,
  tenantId,
  sessionId,
  countedClosingAmount,
  closedReason = "END_SHIFT",
  closeNote = null,
  approveVariance = false,
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/cash/sessions/${sessionId}/close`,
    body: {
      tenantId,
      countedClosingAmount,
      closedReason,
      closeNote,
      approveVariance,
    },
    expectedStatus: 200,
  });
  return response.json?.row || null;
}

async function createCashTransaction({
  token,
  tenantId,
  registerId,
  counterAccountId,
  currencyCode,
  amount,
  idempotencyKey,
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/cash/transactions",
    body: {
      tenantId,
      registerId,
      txnType: "RECEIPT",
      amount,
      currencyCode,
      counterAccountId,
      description: "Cash exceptions test receipt",
      bookDate: TODAY_DATE,
      idempotencyKey,
    },
    expectedStatus: 200,
  });
  const transactionId = toNumber(response.json?.row?.id);
  assert(transactionId > 0, "Cash transaction not created");
  return response.json?.row;
}

async function postCashTransaction({
  token,
  tenantId,
  transactionId,
  overrideCashControl = false,
  overrideReason = null,
  expectedStatus = 200,
}) {
  return apiRequest({
    token,
    method: "POST",
    path: `/api/v1/cash/transactions/${transactionId}/post`,
    body: {
      tenantId,
      overrideCashControl,
      overrideReason,
    },
    expectedStatus,
  });
}

async function createManualJournalDraft({
  token,
  legalEntityId,
  bookId,
  fiscalPeriodId,
  currencyCode,
  operatingUnitId,
  debitAccountId,
  creditAccountId,
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/journals",
    body: {
      legalEntityId,
      bookId,
      fiscalPeriodId,
      sourceType: "MANUAL",
      entryDate: TODAY_DATE,
      documentDate: TODAY_DATE,
      currencyCode,
      description: "Cash exception GL warn event",
      lines: [
        {
          accountId: debitAccountId,
          operatingUnitId,
          subledgerReferenceNo: `EXC-GL-${Date.now()}-D`,
          debitBase: 55,
          creditBase: 0,
        },
        {
          accountId: creditAccountId,
          operatingUnitId,
          subledgerReferenceNo: `EXC-GL-${Date.now()}-C`,
          debitBase: 0,
          creditBase: 55,
        },
      ],
    },
    expectedStatus: 201,
  });
  const journalEntryId = toNumber(response.json?.journalEntryId);
  assert(journalEntryId > 0, "Manual draft journal was not created");
  return journalEntryId;
}

async function fetchExceptions({ token, queryParams, expectedStatus = 200 }) {
  return apiRequest({
    token,
    method: "GET",
    path: "/api/v1/cash/exceptions",
    queryParams,
    expectedStatus,
  });
}

function sectionTotal(section) {
  return toNumber(section?.total);
}

function sectionRows(section) {
  return Array.isArray(section?.rows) ? section.rows : [];
}

async function main() {
  const identity = await createTenantAndAdmin();
  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();

    const adminToken = await login(identity.adminEmail, identity.password);
    const context = await bootstrapContext(adminToken, identity);

    const registerAccountId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `CASH${identity.stamp}`,
      name: "Exceptions Cash Account",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    const counterAccountId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `CTR${identity.stamp}`,
      name: "Exceptions Counter Account",
      accountType: "REVENUE",
      normalSide: "CREDIT",
    });
    const varianceGainAccountId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `VGN${identity.stamp}`,
      name: "Exceptions Variance Gain",
      accountType: "REVENUE",
      normalSide: "CREDIT",
    });
    const varianceLossAccountId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `VLS${identity.stamp}`,
      name: "Exceptions Variance Loss",
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    });

    const registerId = await createRegister({
      token: adminToken,
      tenantId: identity.tenantId,
      legalEntityId: context.legalEntityId,
      operatingUnitId: context.operatingUnitId,
      accountId: registerAccountId,
      currencyCode: context.currencyCode,
      varianceGainAccountId,
      varianceLossAccountId,
      stamp: identity.stamp,
    });

    const openedSession = await openCashSession({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId,
      openingAmount: "100.00",
    });
    assert(
      String(openedSession.row?.status || "").toUpperCase() === "OPEN",
      "Session should be OPEN after creation"
    );

    const closedSessionRow = await closeCashSession({
      token: adminToken,
      tenantId: identity.tenantId,
      sessionId: openedSession.sessionId,
      countedClosingAmount: "90.00",
      closedReason: "FORCED_CLOSE",
      closeNote: "Physical count mismatch",
      approveVariance: false,
    });
    assert(
      String(closedSessionRow?.status || "").toUpperCase() === "CLOSED",
      "Session should be CLOSED"
    );
    assert(
      Number(closedSessionRow?.variance_amount || 0) < 0,
      "Closed session should have short variance"
    );

    const unpostedTxnRow = await createCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId,
      counterAccountId,
      currencyCode: context.currencyCode,
      amount: "25.00",
      idempotencyKey: `EXC-UNPOSTED-${identity.stamp}`,
    });
    assert(
      String(unpostedTxnRow?.status || "").toUpperCase() === "DRAFT",
      "Unposted transaction should remain DRAFT"
    );

    const overrideTxnRow = await createCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId,
      counterAccountId,
      currencyCode: context.currencyCode,
      amount: "40.00",
      idempotencyKey: `EXC-OVERRIDE-${identity.stamp}`,
    });
    const overrideTxnId = toNumber(overrideTxnRow?.id);
    assert(overrideTxnId > 0, "Override transaction was not created");

    const overridePost = await postCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      transactionId: overrideTxnId,
      overrideCashControl: true,
      overrideReason: "Supervisor override for test",
      expectedStatus: 200,
    });
    assert(
      String(overridePost.json?.row?.status || "").toUpperCase() === "POSTED",
      "Override transaction should be POSTED"
    );
    assert(
      String(overridePost.json?.row?.override_reason || "").length > 0,
      "Override reason should be persisted on posted transaction"
    );

    const warnJournalEntryId = await createManualJournalDraft({
      token: adminToken,
      legalEntityId: context.legalEntityId,
      bookId: context.bookId,
      fiscalPeriodId: context.fiscalPeriodId,
      currencyCode: context.currencyCode,
      operatingUnitId: context.operatingUnitId,
      debitAccountId: counterAccountId,
      creditAccountId: registerAccountId,
    });
    assert(warnJournalEntryId > 0, "WARN draft journal should be created");

    const warnAuditResult = await query(
      `SELECT COUNT(*) AS total
       FROM audit_logs
       WHERE tenant_id = ?
         AND action = 'gl.cash_control.warn'`,
      [identity.tenantId]
    );
    assert(
      toNumber(warnAuditResult.rows?.[0]?.total) > 0,
      "Expected at least one gl.cash_control.warn audit log"
    );

    const fullSnapshotRes = await fetchExceptions({
      token: adminToken,
      queryParams: {
        tenantId: identity.tenantId,
        registerId,
      },
      expectedStatus: 200,
    });
    const fullSections = fullSnapshotRes.json?.sections || {};
    const fullSummary = fullSnapshotRes.json?.summary || {};
    const fullNotes = Array.isArray(fullSnapshotRes.json?.notes)
      ? fullSnapshotRes.json.notes
      : [];

    assert(
      toNumber(fullSummary.highVarianceCount) >= 1,
      "Expected highVarianceCount >= 1"
    );
    assert(
      toNumber(fullSummary.forcedCloseCount) >= 1,
      "Expected forcedCloseCount >= 1"
    );
    assert(
      toNumber(fullSummary.overrideUsageCount) >= 1,
      "Expected overrideUsageCount >= 1"
    );
    assert(
      toNumber(fullSummary.unpostedCount) >= 1,
      "Expected unpostedCount >= 1"
    );
    assert(
      toNumber(fullSummary.glCashControlEventCount) >= 1,
      "Expected glCashControlEventCount >= 1"
    );

    assert(
      sectionTotal(fullSections.highVariance) >= 1 &&
        sectionRows(fullSections.highVariance).length >= 1,
      "highVariance section should include rows"
    );
    assert(
      sectionTotal(fullSections.forcedClose) >= 1 &&
        sectionRows(fullSections.forcedClose).length >= 1,
      "forcedClose section should include rows"
    );
    assert(
      sectionTotal(fullSections.overrideUsage) >= 1 &&
        sectionRows(fullSections.overrideUsage).length >= 1,
      "overrideUsage section should include rows"
    );
    assert(
      sectionTotal(fullSections.unposted) >= 1 &&
        sectionRows(fullSections.unposted).length >= 1,
      "unposted section should include rows"
    );
    assert(
      sectionTotal(fullSections.glCashControlEvents) >= 1 &&
        sectionRows(fullSections.glCashControlEvents).length >= 1,
      "glCashControlEvents section should include rows"
    );
    assert(
      fullNotes.length > 0,
      "Expected notes about register/account filtering on GL events"
    );

    const unpostedOnlyRes = await fetchExceptions({
      token: adminToken,
      queryParams: {
        tenantId: identity.tenantId,
        registerId,
        type: "UNPOSTED",
      },
      expectedStatus: 200,
    });
    const unpostedOnlySections = unpostedOnlyRes.json?.sections || {};
    assert(
      sectionTotal(unpostedOnlySections.unposted) >= 1,
      "UNPOSTED filter should return unposted section rows"
    );
    assert(
      sectionTotal(unpostedOnlySections.highVariance) === 0 &&
        sectionTotal(unpostedOnlySections.forcedClose) === 0 &&
        sectionTotal(unpostedOnlySections.overrideUsage) === 0 &&
        sectionTotal(unpostedOnlySections.glCashControlEvents) === 0,
      "UNPOSTED filter should zero-out non-requested sections"
    );

    const glAliasNoRowsRes = await fetchExceptions({
      token: adminToken,
      queryParams: {
        tenantId: identity.tenantId,
        type: "GL_CASH_CONTROL",
        includeRows: "false",
      },
      expectedStatus: 200,
    });
    const glAliasTypes = Array.isArray(glAliasNoRowsRes.json?.filters?.type)
      ? glAliasNoRowsRes.json.filters.type
      : [];
    const glAliasSections = glAliasNoRowsRes.json?.sections || {};
    assert(
      glAliasTypes.includes("GL_CASH_CONTROL_WARN") &&
        glAliasTypes.includes("GL_CASH_CONTROL_OVERRIDE"),
      "GL_CASH_CONTROL alias should expand to WARN and OVERRIDE filter types"
    );
    assert(
      sectionTotal(glAliasSections.glCashControlEvents) >= 1,
      "GL alias filter should count cash-control audit events"
    );
    assert(
      sectionRows(glAliasSections.glCashControlEvents).length === 0,
      "includeRows=false should return empty rows"
    );

    const invalidDateRangeRes = await fetchExceptions({
      token: adminToken,
      queryParams: {
        tenantId: identity.tenantId,
        fromDate: "2026-12-31",
        toDate: "2026-01-01",
      },
      expectedStatus: 400,
    });
    assert(
      toErrorText(invalidDateRangeRes.json).includes("fromDate cannot be after toDate"),
      "Invalid date range should return validator error"
    );

    console.log("Cash exceptions endpoint checks passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          registerId,
          warnJournalEntryId,
          summary: fullSummary,
        },
        null,
        2
      )
    );
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
