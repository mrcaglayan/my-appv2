import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.ICONS_TEST_PORT || 3110);
const BASE_URL =
  process.env.ICONS_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
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
      // wait and retry
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

async function createTenantAndAdmin() {
  const stamp = Date.now();
  const tenantCode = `ICONS_${stamp}`;
  const tenantName = `ICONS ${stamp}`;
  const adminEmail = `icons_admin_${stamp}@example.com`;
  const password = "Interco#12345";
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
  assert(tenantId > 0, "Failed to resolve tenantId");

  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, adminEmail, passwordHash, "ICONS Admin"]
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
  assert(userId > 0, "Failed to resolve userId");

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
     ON DUPLICATE KEY UPDATE
       effect = VALUES(effect)`,
    [tenantId, userId, roleId, tenantId]
  );

  return { tenantId, userId, adminEmail, password };
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

async function createAccount({
  token,
  coaId,
  code,
  name,
  accountType,
  normalSide,
}) {
  const result = await apiRequest({
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

  const accountId = toNumber(result.json?.id);
  assert(accountId > 0, `Failed to create account ${code}`);
  return accountId;
}

async function createAndPostJournal({
  token,
  legalEntityId,
  bookId,
  fiscalPeriodId,
  entryDate,
  currencyCode,
  lines,
}) {
  const createResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/journals",
    body: {
      legalEntityId,
      bookId,
      fiscalPeriodId,
      entryDate,
      documentDate: entryDate,
      currencyCode,
      sourceType: "INTERCOMPANY",
      description: "Intercompany test journal",
      lines,
    },
    expectedStatus: 201,
  });

  const journalEntryId = toNumber(createResult.json?.journalEntryId);
  assert(journalEntryId > 0, "journalEntryId missing");

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/gl/journals/${journalEntryId}/post`,
    expectedStatus: 200,
  });

  return journalEntryId;
}

async function createDraftJournal({
  token,
  legalEntityId,
  bookId,
  fiscalPeriodId,
  entryDate,
  currencyCode,
  sourceType = "MANUAL",
  lines,
  extraPayload = {},
  expectedStatus = 201,
}) {
  return apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/journals",
    body: {
      legalEntityId,
      bookId,
      fiscalPeriodId,
      entryDate,
      documentDate: entryDate,
      currencyCode,
      sourceType,
      description: "Intercompany policy check journal",
      lines,
      ...extraPayload,
    },
    expectedStatus,
  });
}

async function bootstrapScenario(token) {
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
  assert(countryId > 0, "US country row not found");

  const suffix = Date.now();
  const fiscalYear = 2026;

  const groupResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/group-companies",
    body: {
      code: `ICG_${suffix}`,
      name: `Interco Group ${suffix}`,
    },
    expectedStatus: 201,
  });
  const groupCompanyId = toNumber(groupResult.json?.id);
  assert(groupCompanyId > 0, "groupCompanyId not created");

  const calendarResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/fiscal-calendars",
    body: {
      code: `ICCAL_${suffix}`,
      name: `Interco Calendar ${suffix}`,
      yearStartMonth: 1,
      yearStartDay: 1,
    },
    expectedStatus: 201,
  });
  const calendarId = toNumber(calendarResult.json?.id);
  assert(calendarId > 0, "calendarId not created");

  await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/fiscal-periods/generate",
    body: { calendarId, fiscalYear },
    expectedStatus: 201,
  });

  const periodsResult = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/org/fiscal-calendars/${calendarId}/periods?fiscalYear=${fiscalYear}`,
    expectedStatus: 200,
  });
  const period1 = (periodsResult.json?.rows || []).find(
    (row) => toNumber(row.period_no) === 1 && !row.is_adjustment
  );
  const fiscalPeriodId = toNumber(period1?.id);
  assert(fiscalPeriodId > 0, "period 1 not found");
  const entryDate = `${fiscalYear}-01-15`;

  const entityAResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `ICA_${suffix}`,
      name: `Interco A ${suffix}`,
      countryId,
      functionalCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const legalEntityAId = toNumber(entityAResult.json?.id);
  assert(legalEntityAId > 0, "legalEntityAId not created");

  const entityBResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `ICB_${suffix}`,
      name: `Interco B ${suffix}`,
      countryId,
      functionalCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const legalEntityBId = toNumber(entityBResult.json?.id);
  assert(legalEntityBId > 0, "legalEntityBId not created");

  const entityCResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `ICC_${suffix}`,
      name: `Interco C ${suffix}`,
      countryId,
      functionalCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const legalEntityCId = toNumber(entityCResult.json?.id);
  assert(legalEntityCId > 0, "legalEntityCId not created");

  const bookAResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/books",
    body: {
      legalEntityId: legalEntityAId,
      calendarId,
      code: `ICBOOKA_${suffix}`,
      name: `Interco Book A ${suffix}`,
      bookType: "LOCAL",
      baseCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const bookAId = toNumber(bookAResult.json?.id);
  assert(bookAId > 0, "bookAId not created");

  const bookBResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/books",
    body: {
      legalEntityId: legalEntityBId,
      calendarId,
      code: `ICBOOKB_${suffix}`,
      name: `Interco Book B ${suffix}`,
      bookType: "LOCAL",
      baseCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const bookBId = toNumber(bookBResult.json?.id);
  assert(bookBId > 0, "bookBId not created");

  const localCoaAResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/coas",
    body: {
      scope: "LEGAL_ENTITY",
      legalEntityId: legalEntityAId,
      code: `ICCOAA_${suffix}`,
      name: `Local CoA A ${suffix}`,
    },
    expectedStatus: 201,
  });
  const localCoaAId = toNumber(localCoaAResult.json?.id);
  assert(localCoaAId > 0, "localCoaAId not created");

  const localCoaBResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/coas",
    body: {
      scope: "LEGAL_ENTITY",
      legalEntityId: legalEntityBId,
      code: `ICCOAB_${suffix}`,
      name: `Local CoA B ${suffix}`,
    },
    expectedStatus: 201,
  });
  const localCoaBId = toNumber(localCoaBResult.json?.id);
  assert(localCoaBId > 0, "localCoaBId not created");

  const groupCoaResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/coas",
    body: {
      scope: "GROUP",
      code: `ICCOAG_${suffix}`,
      name: `Group CoA ${suffix}`,
    },
    expectedStatus: 201,
  });
  const groupCoaId = toNumber(groupCoaResult.json?.id);
  assert(groupCoaId > 0, "groupCoaId not created");

  const accountARAId = await createAccount({
    token,
    coaId: localCoaAId,
    code: "1100",
    name: "IC Receivable",
    accountType: "ASSET",
    normalSide: "DEBIT",
  });
  const accountRevAId = await createAccount({
    token,
    coaId: localCoaAId,
    code: "4000",
    name: "Intercompany Revenue",
    accountType: "REVENUE",
    normalSide: "CREDIT",
  });
  const accountAPBId = await createAccount({
    token,
    coaId: localCoaBId,
    code: "2100",
    name: "IC Payable",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  });
  const accountExpBId = await createAccount({
    token,
    coaId: localCoaBId,
    code: "5000",
    name: "Intercompany Expense",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  });

  const groupARId = await createAccount({
    token,
    coaId: groupCoaId,
    code: "1100",
    name: "Group IC Receivable",
    accountType: "ASSET",
    normalSide: "DEBIT",
  });
  const groupAPId = await createAccount({
    token,
    coaId: groupCoaId,
    code: "2100",
    name: "Group IC Payable",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  });
  const groupRevId = await createAccount({
    token,
    coaId: groupCoaId,
    code: "4000",
    name: "Group Revenue",
    accountType: "REVENUE",
    normalSide: "CREDIT",
  });
  const groupExpId = await createAccount({
    token,
    coaId: groupCoaId,
    code: "5000",
    name: "Group Expense",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  });

  await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/intercompany/pairs",
    body: {
      fromLegalEntityId: legalEntityAId,
      toLegalEntityId: legalEntityBId,
      receivableAccountId: accountARAId,
      status: "ACTIVE",
    },
    expectedStatus: 201,
  });
  await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/intercompany/pairs",
    body: {
      fromLegalEntityId: legalEntityBId,
      toLegalEntityId: legalEntityAId,
      payableAccountId: accountAPBId,
      status: "ACTIVE",
    },
    expectedStatus: 201,
  });

  await createAndPostJournal({
    token,
    legalEntityId: legalEntityAId,
    bookId: bookAId,
    fiscalPeriodId,
    entryDate,
    currencyCode,
    lines: [
      {
        accountId: accountARAId,
        counterpartyLegalEntityId: legalEntityBId,
        currencyCode,
        amountTxn: 100,
        debitBase: 100,
        creditBase: 0,
        description: "A receivable",
      },
      {
        accountId: accountRevAId,
        counterpartyLegalEntityId: legalEntityBId,
        currencyCode,
        amountTxn: -100,
        debitBase: 0,
        creditBase: 100,
        description: "A revenue",
      },
    ],
  });

  await createAndPostJournal({
    token,
    legalEntityId: legalEntityBId,
    bookId: bookBId,
    fiscalPeriodId,
    entryDate,
    currencyCode,
    lines: [
      {
        accountId: accountExpBId,
        counterpartyLegalEntityId: legalEntityAId,
        currencyCode,
        amountTxn: 95,
        debitBase: 95,
        creditBase: 0,
        description: "B expense",
      },
      {
        accountId: accountAPBId,
        counterpartyLegalEntityId: legalEntityAId,
        currencyCode,
        amountTxn: -95,
        debitBase: 0,
        creditBase: 95,
        description: "B payable",
      },
    ],
  });

  const consolidationGroupResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/consolidation/groups",
    body: {
      groupCompanyId,
      calendarId,
      code: `ICCONS_${suffix}`,
      name: `Interco Consolidation ${suffix}`,
      presentationCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const consolidationGroupId = toNumber(consolidationGroupResult.json?.id);
  assert(consolidationGroupId > 0, "consolidationGroupId not created");

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/consolidation/groups/${consolidationGroupId}/members`,
    body: {
      legalEntityId: legalEntityAId,
      consolidationMethod: "FULL",
      ownershipPct: 1,
      effectiveFrom: `${fiscalYear}-01-01`,
    },
    expectedStatus: 201,
  });
  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/consolidation/groups/${consolidationGroupId}/members`,
    body: {
      legalEntityId: legalEntityBId,
      consolidationMethod: "FULL",
      ownershipPct: 1,
      effectiveFrom: `${fiscalYear}-01-01`,
    },
    expectedStatus: 201,
  });

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/consolidation/groups/${consolidationGroupId}/coa-mappings`,
    body: {
      legalEntityId: legalEntityAId,
      groupCoaId,
      localCoaId: localCoaAId,
      status: "ACTIVE",
    },
    expectedStatus: 201,
  });
  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/consolidation/groups/${consolidationGroupId}/coa-mappings`,
    body: {
      legalEntityId: legalEntityBId,
      groupCoaId,
      localCoaId: localCoaBId,
      status: "ACTIVE",
    },
    expectedStatus: 201,
  });

  const runResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/consolidation/runs",
    body: {
      consolidationGroupId,
      fiscalPeriodId,
      runName: `IC_RUN_${suffix}`,
      presentationCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const runId = toNumber(runResult.json?.runId);
  assert(runId > 0, "runId not created");

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/consolidation/runs/${runId}/execute`,
    body: { rateType: "CLOSING" },
    expectedStatus: 200,
  });

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/consolidation/runs/${runId}/adjustments`,
    body: {
      accountId: groupExpId,
      currencyCode,
      description: "Topside expense",
      debitAmount: 10,
      creditAmount: 0,
    },
    expectedStatus: 201,
  });
  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/consolidation/runs/${runId}/adjustments`,
    body: {
      accountId: groupRevId,
      currencyCode,
      description: "Topside revenue",
      debitAmount: 0,
      creditAmount: 10,
    },
    expectedStatus: 201,
  });

  await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/consolidation/runs/${runId}/eliminations`,
    body: {
      description: "Eliminate internal markup",
      lines: [
        {
          accountId: groupRevId,
          debitAmount: 5,
          creditAmount: 0,
          currencyCode,
          description: "Revenue elimination",
        },
        {
          accountId: groupExpId,
          debitAmount: 0,
          creditAmount: 5,
          currencyCode,
          description: "Expense elimination",
        },
      ],
    },
    expectedStatus: 201,
  });

  return {
    fiscalPeriodId,
    entryDate,
    currencyCode,
    bookAId,
    bookBId,
    accountARAId,
    accountRevAId,
    accountAPBId,
    accountExpBId,
    legalEntityAId,
    legalEntityBId,
    legalEntityCId,
    localCoaBId,
    runId,
    groupARCode: "1100",
    groupAPCode: "2100",
    groupRevenueCode: "4000",
    groupExpenseCode: "5000",
  };
}

async function verifyIntercompanyPolicyEnforcement(token, scenario) {
  await apiRequest({
    token,
    method: "PATCH",
    path: `/api/v1/intercompany/entity-flags/${scenario.legalEntityAId}`,
    body: { isIntercompanyEnabled: false },
    expectedStatus: 200,
  });

  const disabledEntityAttempt = await createDraftJournal({
    token,
    legalEntityId: scenario.legalEntityAId,
    bookId: scenario.bookAId,
    fiscalPeriodId: scenario.fiscalPeriodId,
    entryDate: scenario.entryDate,
    currencyCode: scenario.currencyCode,
    sourceType: "MANUAL",
    lines: [
      {
        accountId: scenario.accountARAId,
        counterpartyLegalEntityId: scenario.legalEntityBId,
        currencyCode: scenario.currencyCode,
        amountTxn: 10,
        debitBase: 10,
        creditBase: 0,
      },
      {
        accountId: scenario.accountRevAId,
        counterpartyLegalEntityId: scenario.legalEntityBId,
        currencyCode: scenario.currencyCode,
        amountTxn: -10,
        debitBase: 0,
        creditBase: 10,
      },
    ],
    expectedStatus: 400,
  });
  assert(
    String(disabledEntityAttempt.json?.message || "")
      .toLowerCase()
      .includes("intercompany disabled"),
    "Expected disabled-entity intercompany policy rejection"
  );

  await apiRequest({
    token,
    method: "PATCH",
    path: `/api/v1/intercompany/entity-flags/${scenario.legalEntityAId}`,
    body: { isIntercompanyEnabled: true },
    expectedStatus: 200,
  });

  await apiRequest({
    token,
    method: "PATCH",
    path: `/api/v1/intercompany/entity-flags/${scenario.legalEntityAId}`,
    body: { intercompanyPartnerRequired: true },
    expectedStatus: 200,
  });

  const missingPartnerAttempt = await createDraftJournal({
    token,
    legalEntityId: scenario.legalEntityAId,
    bookId: scenario.bookAId,
    fiscalPeriodId: scenario.fiscalPeriodId,
    entryDate: scenario.entryDate,
    currencyCode: scenario.currencyCode,
    sourceType: "INTERCOMPANY",
    lines: [
      {
        accountId: scenario.accountARAId,
        counterpartyLegalEntityId: scenario.legalEntityBId,
        currencyCode: scenario.currencyCode,
        amountTxn: 15,
        debitBase: 15,
        creditBase: 0,
      },
      {
        accountId: scenario.accountRevAId,
        currencyCode: scenario.currencyCode,
        amountTxn: -15,
        debitBase: 0,
        creditBase: 15,
      },
    ],
    expectedStatus: 400,
  });
  assert(
    String(missingPartnerAttempt.json?.message || "")
      .toLowerCase()
      .includes("requires intercompany partner"),
    "Expected partner-required policy rejection"
  );

  await apiRequest({
    token,
    method: "PATCH",
    path: `/api/v1/intercompany/entity-flags/${scenario.legalEntityAId}`,
    body: { intercompanyPartnerRequired: false },
    expectedStatus: 200,
  });

  const missingPairAttempt = await createDraftJournal({
    token,
    legalEntityId: scenario.legalEntityAId,
    bookId: scenario.bookAId,
    fiscalPeriodId: scenario.fiscalPeriodId,
    entryDate: scenario.entryDate,
    currencyCode: scenario.currencyCode,
    sourceType: "MANUAL",
    lines: [
      {
        accountId: scenario.accountARAId,
        counterpartyLegalEntityId: scenario.legalEntityCId,
        currencyCode: scenario.currencyCode,
        amountTxn: 12,
        debitBase: 12,
        creditBase: 0,
      },
      {
        accountId: scenario.accountRevAId,
        counterpartyLegalEntityId: scenario.legalEntityCId,
        currencyCode: scenario.currencyCode,
        amountTxn: -12,
        debitBase: 0,
        creditBase: 12,
      },
    ],
    expectedStatus: 400,
  });
  assert(
    String(missingPairAttempt.json?.message || "")
      .toLowerCase()
      .includes("pair mapping"),
    "Expected missing active pair mapping rejection"
  );

  await apiRequest({
    token,
    method: "PATCH",
    path: `/api/v1/intercompany/entity-flags/${scenario.legalEntityBId}`,
    body: { isIntercompanyEnabled: false },
    expectedStatus: 200,
  });

  const complianceResult = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/intercompany/compliance-issues?fiscalPeriodId=${scenario.fiscalPeriodId}&includeDraft=false&limit=200`,
    expectedStatus: 200,
  });
  const complianceRows = complianceResult.json?.rows || [];
  const disabledIssue = complianceRows.find(
    (row) =>
      String(row.issueCode || "") === "ENTITY_INTERCOMPANY_DISABLED" &&
      toNumber(row.fromLegalEntityId) === scenario.legalEntityBId
  );
  assert(disabledIssue, "Expected compliance issue row for disabled source legal entity");

  await apiRequest({
    token,
    method: "PATCH",
    path: `/api/v1/intercompany/entity-flags/${scenario.legalEntityBId}`,
    body: { isIntercompanyEnabled: true },
    expectedStatus: 200,
  });
}

async function verifyIntercompanyReconciliation(token, scenario) {
  const reconcileResult = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/intercompany/reconcile",
    body: {
      fiscalPeriodId: scenario.fiscalPeriodId,
      includeMatched: true,
      includeAccountBreakdown: true,
    },
    expectedStatus: 200,
  });

  const rows = reconcileResult.json?.rows || [];
  const row = rows.find((candidate) => {
    const aId = toNumber(candidate?.entityA?.id);
    const bId = toNumber(candidate?.entityB?.id);
    return (
      (aId === scenario.legalEntityAId && bId === scenario.legalEntityBId) ||
      (aId === scenario.legalEntityBId && bId === scenario.legalEntityAId)
    );
  });
  assert(row, "Reconciliation pair row not found");
  assert(
    String(row.status || "").toUpperCase() === "MISMATCHED",
    "Expected mismatched reconciliation status"
  );
  assert(
    Math.abs(Math.abs(toNumber(row.differenceBase)) - 5) < 0.0001,
    "Expected reconciliation difference magnitude of 5"
  );

  const accountBreakdown = Array.isArray(row.accountBreakdown)
    ? row.accountBreakdown
    : [];
  const icReceivableRow = accountBreakdown.find(
    (item) => String(item.accountCode || "") === scenario.groupARCode
  );
  assert(icReceivableRow, "Expected receivable account breakdown row");
}

async function verifyConsolidationReports(token, scenario) {
  const baseBsResult = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/consolidation/runs/${scenario.runId}/reports/balance-sheet?includeDraft=false`,
    expectedStatus: 200,
  });
  const baseBsTotals = baseBsResult.json?.totals || {};
  assert(
    Math.abs(toNumber(baseBsTotals.assetsTotal) - 100) < 0.0001,
    "Base balance sheet assets total must be 100"
  );
  assert(
    Math.abs(toNumber(baseBsTotals.liabilitiesTotal) - 95) < 0.0001,
    "Base balance sheet liabilities total must be 95"
  );
  assert(
    Math.abs(toNumber(baseBsTotals.currentPeriodEarnings) - 5) < 0.0001,
    "Base balance sheet current period earnings must be 5"
  );
  assert(
    Math.abs(toNumber(baseBsTotals.equationDelta)) < 0.0001,
    "Base balance sheet equation delta must be ~0"
  );

  const baseIsResult = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/consolidation/runs/${scenario.runId}/reports/income-statement?includeDraft=false`,
    expectedStatus: 200,
  });
  const baseIsTotals = baseIsResult.json?.totals || {};
  assert(
    Math.abs(toNumber(baseIsTotals.revenueTotal) - 100) < 0.0001,
    "Base income statement revenue total must be 100"
  );
  assert(
    Math.abs(toNumber(baseIsTotals.expenseTotal) - 95) < 0.0001,
    "Base income statement expense total must be 95"
  );
  assert(
    Math.abs(toNumber(baseIsTotals.netIncome) - 5) < 0.0001,
    "Base income statement net income must be 5"
  );

  const draftAdjustmentList = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/consolidation/runs/${scenario.runId}/adjustments?status=DRAFT`,
    expectedStatus: 200,
  });
  const draftAdjustments = draftAdjustmentList.json?.rows || [];
  assert(draftAdjustments.length >= 2, "Expected at least two draft adjustments");
  for (const adjustment of draftAdjustments) {
    const adjustmentId = toNumber(adjustment?.id);
    assert(adjustmentId > 0, "Invalid draft adjustment id");
    await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/consolidation/runs/${scenario.runId}/adjustments/${adjustmentId}/post`,
      expectedStatus: 200,
    });
  }

  const draftEliminationList = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/consolidation/runs/${scenario.runId}/eliminations?status=DRAFT`,
    expectedStatus: 200,
  });
  const draftEliminations = draftEliminationList.json?.rows || [];
  assert(draftEliminations.length >= 1, "Expected at least one draft elimination");
  for (const elimination of draftEliminations) {
    const eliminationEntryId = toNumber(elimination?.id);
    assert(eliminationEntryId > 0, "Invalid draft elimination id");
    await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/consolidation/runs/${scenario.runId}/eliminations/${eliminationEntryId}/post`,
      expectedStatus: 200,
    });
  }

  const postedAdjustmentList = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/consolidation/runs/${scenario.runId}/adjustments?status=POSTED`,
    expectedStatus: 200,
  });
  assert(
    (postedAdjustmentList.json?.rows || []).length >= 2,
    "Expected posted adjustments after posting flow"
  );

  const postedEliminationList = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/consolidation/runs/${scenario.runId}/eliminations?status=POSTED`,
    expectedStatus: 200,
  });
  assert(
    (postedEliminationList.json?.rows || []).length >= 1,
    "Expected posted eliminations after posting flow"
  );

  const postedBsResult = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/consolidation/runs/${scenario.runId}/reports/balance-sheet?includeDraft=false`,
    expectedStatus: 200,
  });
  const postedBsTotals = postedBsResult.json?.totals || {};
  assert(
    Math.abs(toNumber(postedBsTotals.assetsTotal) - 100) < 0.0001,
    "Posted balance sheet assets total must be 100"
  );
  assert(
    Math.abs(toNumber(postedBsTotals.liabilitiesTotal) - 95) < 0.0001,
    "Posted balance sheet liabilities total must be 95"
  );
  assert(
    Math.abs(toNumber(postedBsTotals.currentPeriodEarnings) - 5) < 0.0001,
    "Posted balance sheet current period earnings must be 5"
  );
  assert(
    Math.abs(toNumber(postedBsTotals.equationDelta)) < 0.0001,
    "Posted balance sheet equation delta must be ~0"
  );

  const postedBsRows = postedBsResult.json?.rows || [];
  const assetRow = postedBsRows.find(
    (row) => String(row.accountCode || "") === scenario.groupARCode
  );
  const liabilityRow = postedBsRows.find(
    (row) => String(row.accountCode || "") === scenario.groupAPCode
  );
  assert(assetRow, "Posted balance sheet asset row missing");
  assert(liabilityRow, "Posted balance sheet liability row missing");
  assert(
    Math.abs(toNumber(assetRow.normalizedFinalBalance) - 100) < 0.0001,
    "Posted asset normalized final balance must be 100"
  );
  assert(
    Math.abs(toNumber(liabilityRow.normalizedFinalBalance) - 95) < 0.0001,
    "Posted liability normalized final balance must be 95"
  );

  const postedIsResult = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/consolidation/runs/${scenario.runId}/reports/income-statement?includeDraft=false`,
    expectedStatus: 200,
  });
  const postedIsTotals = postedIsResult.json?.totals || {};
  assert(
    Math.abs(toNumber(postedIsTotals.revenueTotal) - 105) < 0.0001,
    "Posted income statement revenue total must be 105"
  );
  assert(
    Math.abs(toNumber(postedIsTotals.expenseTotal) - 100) < 0.0001,
    "Posted income statement expense total must be 100"
  );
  assert(
    Math.abs(toNumber(postedIsTotals.netIncome) - 5) < 0.0001,
    "Posted income statement net income must be 5"
  );

  const postedIsRows = postedIsResult.json?.rows || [];
  const revenueRow = postedIsRows.find(
    (row) => String(row.accountCode || "") === scenario.groupRevenueCode
  );
  const expenseRow = postedIsRows.find(
    (row) => String(row.accountCode || "") === scenario.groupExpenseCode
  );
  assert(revenueRow, "Posted income statement revenue row missing");
  assert(expenseRow, "Posted income statement expense row missing");
}

async function verifyIntercompanyAutoMirrorLinkedPosting(token, scenario) {
  await createAccount({
    token,
    coaId: scenario.localCoaBId,
    code: "1100",
    name: "Mirror IC Receivable",
    accountType: "ASSET",
    normalSide: "DEBIT",
  });
  await createAccount({
    token,
    coaId: scenario.localCoaBId,
    code: "4000",
    name: "Mirror Intercompany Revenue",
    accountType: "REVENUE",
    normalSide: "CREDIT",
  });

  const createResult = await createDraftJournal({
    token,
    legalEntityId: scenario.legalEntityAId,
    bookId: scenario.bookAId,
    fiscalPeriodId: scenario.fiscalPeriodId,
    entryDate: scenario.entryDate,
    currencyCode: scenario.currencyCode,
    sourceType: "INTERCOMPANY",
    lines: [
      {
        accountId: scenario.accountARAId,
        counterpartyLegalEntityId: scenario.legalEntityBId,
        currencyCode: scenario.currencyCode,
        amountTxn: 21,
        debitBase: 21,
        creditBase: 0,
      },
      {
        accountId: scenario.accountRevAId,
        counterpartyLegalEntityId: scenario.legalEntityBId,
        currencyCode: scenario.currencyCode,
        amountTxn: -21,
        debitBase: 0,
        creditBase: 21,
      },
    ],
    extraPayload: {
      autoMirror: true,
      description: "Auto mirror integration check",
    },
    expectedStatus: 201,
  });

  const sourceJournalId = toNumber(createResult.json?.journalEntryId);
  assert(sourceJournalId > 0, "Expected source draft journal id for auto-mirror flow");
  const mirrorJournalIds = Array.isArray(createResult.json?.mirrorJournalEntryIds)
    ? createResult.json.mirrorJournalEntryIds.map((id) => toNumber(id)).filter((id) => id > 0)
    : [];
  assert(mirrorJournalIds.length === 1, "Expected one mirror draft journal id");
  const mirrorJournalId = mirrorJournalIds[0];

  const mirrorDetailBeforePost = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/gl/journals/${mirrorJournalId}`,
    expectedStatus: 200,
  });
  assert(
    toNumber(mirrorDetailBeforePost.json?.row?.intercompany_source_journal_entry_id) ===
      sourceJournalId,
    "Mirror journal must link to source journal"
  );
  assert(
    String(mirrorDetailBeforePost.json?.row?.status || "").toUpperCase() === "DRAFT",
    "Mirror journal must be created as DRAFT"
  );

  const postResult = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/gl/journals/${sourceJournalId}/post`,
    body: { postLinkedMirrors: true },
    expectedStatus: 200,
  });

  const postedIds = Array.isArray(postResult.json?.postedJournalIds)
    ? postResult.json.postedJournalIds.map((id) => toNumber(id))
    : [];
  assert(
    postedIds.includes(sourceJournalId),
    "Linked post must include source journal id"
  );
  assert(
    postedIds.includes(mirrorJournalId),
    "Linked post must include mirror journal id"
  );

  const mirrorDetailAfterPost = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/gl/journals/${mirrorJournalId}`,
    expectedStatus: 200,
  });
  assert(
    String(mirrorDetailAfterPost.json?.row?.status || "").toUpperCase() === "POSTED",
    "Mirror journal should be POSTED by linked posting"
  );
}

async function main() {
  const identity = await createTenantAndAdmin();
  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const token = await login(identity.adminEmail, identity.password);

    const scenario = await bootstrapScenario(token);
    await verifyIntercompanyPolicyEnforcement(token, scenario);
    await verifyIntercompanyReconciliation(token, scenario);
    await verifyConsolidationReports(token, scenario);
    await verifyIntercompanyAutoMirrorLinkedPosting(token, scenario);

    console.log("");
    console.log("Intercompany reconciliation + consolidation reports test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          runId: scenario.runId,
          fiscalPeriodId: scenario.fiscalPeriodId,
          legalEntityAId: scenario.legalEntityAId,
          legalEntityBId: scenario.legalEntityBId,
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
    await closePool();
  }
}

main().catch(async (err) => {
  console.error("Intercompany reconciliation + consolidation reports test failed:", err);
  try {
    await closePool();
  } catch {
    // ignore close errors
  }
  process.exit(1);
});
