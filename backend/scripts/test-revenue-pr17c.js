import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.REVENUE_PR17C_TEST_PORT || 3123);
const BASE_URL =
  process.env.REVENUE_PR17C_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
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

function amountsEqual(left, right, epsilon = 0.000001) {
  return Math.abs(toNumber(left) - toNumber(right)) <= epsilon;
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
      // retry
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
  const tenantCode = `RR17C_T_${stamp}`;
  const tenantName = `Revenue PR17C Tenant ${stamp}`;
  const email = `revenue_pr17c_admin_${stamp}@example.com`;
  const password = "Revenue#12345";
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
    [tenantId, email, passwordHash, "Revenue PR17C Admin"]
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

async function upsertPeriodStatus({
  bookId,
  fiscalPeriodId,
  status,
  userId,
  note,
}) {
  const normalizedStatus = String(status || "").toUpperCase();
  const isOpen = normalizedStatus === "OPEN";
  await query(
    `INSERT INTO period_statuses (
        book_id,
        fiscal_period_id,
        status,
        closed_by_user_id,
        closed_at,
        note
     )
     VALUES (?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       status = VALUES(status),
       closed_by_user_id = VALUES(closed_by_user_id),
       closed_at = VALUES(closed_at),
       note = VALUES(note)`,
    [
      bookId,
      fiscalPeriodId,
      normalizedStatus,
      isOpen ? null : userId,
      isOpen ? null : new Date(),
      note || null,
    ]
  );
}

async function upsertPurposeMappings({
  tenantId,
  legalEntityId,
  mappings,
}) {
  for (const mapping of mappings) {
    await query(
      `INSERT INTO journal_purpose_accounts (
          tenant_id,
          legal_entity_id,
          purpose_code,
          account_id
       )
       VALUES (?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         account_id = VALUES(account_id)`,
      [tenantId, legalEntityId, mapping.purposeCode, mapping.accountId]
    );
  }
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
    [tenantId, `RR17C_G_${stamp}`, `Revenue Group ${stamp}`]
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
      `RR17C_LE_${stamp}`,
      `Revenue Legal Entity ${stamp}`,
      countryId,
    ]
  );
  const legalEntityId = toNumber(legalEntityInsert.rows?.insertId);
  assert(legalEntityId > 0, "Failed to create legal entity");

  const calendarInsert = await query(
    `INSERT INTO fiscal_calendars (tenant_id, code, name, year_start_month, year_start_day)
     VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `RR17C_CAL_${stamp}`, `Revenue Calendar ${stamp}`]
  );
  const calendarId = toNumber(calendarInsert.rows?.insertId);
  assert(calendarId > 0, "Failed to create fiscal calendar");

  const period1Insert = await query(
    `INSERT INTO fiscal_periods (
        calendar_id,
        fiscal_year,
        period_no,
        period_name,
        start_date,
        end_date,
        is_adjustment
     )
     VALUES (?, 2026, 1, ?, '2026-01-01', '2026-06-30', FALSE)`,
    [calendarId, `FY2026 P1 ${stamp}`]
  );
  const period1Id = toNumber(period1Insert.rows?.insertId);
  assert(period1Id > 0, "Failed to create period 1");

  const period2Insert = await query(
    `INSERT INTO fiscal_periods (
        calendar_id,
        fiscal_year,
        period_no,
        period_name,
        start_date,
        end_date,
        is_adjustment
     )
     VALUES (?, 2026, 2, ?, '2026-07-01', '2026-12-31', FALSE)`,
    [calendarId, `FY2026 P2 ${stamp}`]
  );
  const period2Id = toNumber(period2Insert.rows?.insertId);
  assert(period2Id > 0, "Failed to create period 2");

  const period3Insert = await query(
    `INSERT INTO fiscal_periods (
        calendar_id,
        fiscal_year,
        period_no,
        period_name,
        start_date,
        end_date,
        is_adjustment
     )
     VALUES (?, 2027, 1, ?, '2027-01-01', '2027-06-30', FALSE)`,
    [calendarId, `FY2027 P1 ${stamp}`]
  );
  const period3Id = toNumber(period3Insert.rows?.insertId);
  assert(period3Id > 0, "Failed to create period 3");

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
    [tenantId, legalEntityId, calendarId, `RR17C_BOOK_${stamp}`, `Revenue Book ${stamp}`]
  );
  const bookId = toNumber(bookInsert.rows?.insertId);
  assert(bookId > 0, "Failed to create book");

  await upsertPeriodStatus({
    bookId,
    fiscalPeriodId: period1Id,
    status: "OPEN",
    userId,
    note: "PR17C period1 open",
  });
  await upsertPeriodStatus({
    bookId,
    fiscalPeriodId: period2Id,
    status: "OPEN",
    userId,
    note: "PR17C period2 open",
  });
  await upsertPeriodStatus({
    bookId,
    fiscalPeriodId: period3Id,
    status: "HARD_CLOSED",
    userId,
    note: "PR17C period3 closed",
  });

  const coaInsert = await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `RR17C_COA_${stamp}`, `Revenue CoA ${stamp}`]
  );
  const coaId = toNumber(coaInsert.rows?.insertId);
  assert(coaId > 0, "Failed to create legal-entity chart");

  async function createAccount({ code, name, accountType, normalSide }) {
    const insertResult = await query(
      `INSERT INTO accounts (
          coa_id,
          code,
          name,
          account_type,
          normal_side,
          allow_posting,
          parent_account_id,
          is_active
       )
       VALUES (?, ?, ?, ?, ?, TRUE, NULL, TRUE)`,
      [coaId, code, name, accountType, normalSide]
    );
    const accountId = toNumber(insertResult.rows?.insertId);
    assert(accountId > 0, `Failed to create account ${code}`);
    return accountId;
  }

  const accountCodePrefix = `R17C${String(stamp).slice(-6)}`;
  const accounts = {
    accrRevShortAsset: await createAccount({
      code: `${accountCodePrefix}01`,
      name: `ACCR_REV Short Asset ${stamp}`,
      accountType: "ASSET",
      normalSide: "DEBIT",
    }),
    accrRevLongAsset: await createAccount({
      code: `${accountCodePrefix}02`,
      name: `ACCR_REV Long Asset ${stamp}`,
      accountType: "ASSET",
      normalSide: "DEBIT",
    }),
    accrRevRevenue: await createAccount({
      code: `${accountCodePrefix}03`,
      name: `ACCR_REV Revenue ${stamp}`,
      accountType: "REVENUE",
      normalSide: "CREDIT",
    }),
    accrRevReclass: await createAccount({
      code: `${accountCodePrefix}04`,
      name: `ACCR_REV Reclass ${stamp}`,
      accountType: "ASSET",
      normalSide: "DEBIT",
    }),
    accrExpShortLiability: await createAccount({
      code: `${accountCodePrefix}05`,
      name: `ACCR_EXP Short Liability ${stamp}`,
      accountType: "LIABILITY",
      normalSide: "CREDIT",
    }),
    accrExpLongLiability: await createAccount({
      code: `${accountCodePrefix}06`,
      name: `ACCR_EXP Long Liability ${stamp}`,
      accountType: "LIABILITY",
      normalSide: "CREDIT",
    }),
    accrExpExpense: await createAccount({
      code: `${accountCodePrefix}07`,
      name: `ACCR_EXP Expense ${stamp}`,
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    }),
    accrExpReclass: await createAccount({
      code: `${accountCodePrefix}08`,
      name: `ACCR_EXP Reclass ${stamp}`,
      accountType: "LIABILITY",
      normalSide: "CREDIT",
    }),
  };

  return {
    legalEntityId,
    period1Id,
    period2Id,
    period3Id,
    bookId,
    accounts,
  };
}

async function runApiAssertions({
  token,
  tenantId,
  fixture,
  stamp,
}) {
  const accrRevGenerate = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/accruals/generate",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.period1Id,
      accountFamily: "ACCRUED_REVENUE",
      maturityBucket: "LONG_TERM",
      maturityDate: "2026-10-01",
      reclassRequired: true,
      currencyCode: "USD",
      fxRate: 1,
      totalAmountTxn: 150,
      totalAmountBase: 150,
      sourceRunUid: `RR17C-ACCRREV-RUN-${stamp}`,
      runNo: `RR17C-ACCRREV-${stamp}`,
    },
  });
  const accrRevId = toNumber(accrRevGenerate.json?.row?.id);
  assert(accrRevId > 0, "Accrued-revenue accrual generate must return row.id");
  assert(String(accrRevGenerate.json?.row?.status) === "DRAFT", "Generated accrual should be DRAFT");

  const reverseBeforeSettle = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrRevId}/reverse`,
    expectedStatus: 400,
  });
  assert(
    String(reverseBeforeSettle.json?.message || "").includes("settled/posted accrual state"),
    "Reverse must fail for non-settled accrual"
  );

  const settleMissingMappings = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrRevId}/settle`,
    expectedStatus: 400,
    body: { settlementPeriodId: fixture.period2Id },
  });
  assert(
    String(settleMissingMappings.json?.message || "").includes("Setup required"),
    "Settle must fail when required ACCR_REV purpose-account mappings are missing"
  );

  await upsertPurposeMappings({
    tenantId,
    legalEntityId: fixture.legalEntityId,
    mappings: [
      {
        purposeCode: "ACCR_REV_SHORT_ASSET",
        accountId: fixture.accounts.accrRevShortAsset,
      },
      {
        purposeCode: "ACCR_REV_LONG_ASSET",
        accountId: fixture.accounts.accrRevLongAsset,
      },
      {
        purposeCode: "ACCR_REV_REVENUE",
        accountId: fixture.accounts.accrRevRevenue,
      },
      {
        purposeCode: "ACCR_REV_RECLASS",
        accountId: fixture.accounts.accrRevReclass,
      },
    ],
  });

  const settleBeforeMaturity = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrRevId}/settle`,
    expectedStatus: 400,
    body: { settlementPeriodId: fixture.period1Id },
  });
  assert(
    String(settleBeforeMaturity.json?.message || "").includes("maturity boundary"),
    "Settle must enforce due/maturity boundary"
  );

  const settleClosedPeriod = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrRevId}/settle`,
    expectedStatus: 400,
    body: { settlementPeriodId: fixture.period3Id },
  });
  assert(
    String(settleClosedPeriod.json?.message || "").includes("Period is HARD_CLOSED"),
    "Settle must fail when target period is not OPEN"
  );

  const settleAccrRev = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrRevId}/settle`,
    expectedStatus: 200,
    body: { settlementPeriodId: fixture.period2Id },
  });
  const settledAccrRevRow = settleAccrRev.json?.row || {};
  const settledAccrRevJournal = settleAccrRev.json?.journal || {};
  assert(
    String(settledAccrRevRow.status) === "POSTED",
    "Settled accrual should transition to POSTED"
  );
  assert(
    toNumber(settledAccrRevRow.fiscalPeriodId) === fixture.period2Id,
    "Accrual settle should update run fiscalPeriodId to settlement period"
  );
  assert(
    toNumber(settleAccrRev.json?.subledgerEntryCount) === 2,
    "Accrued-revenue long-term settle should create RECOGNITION + RECLASS entries"
  );
  const accrRevJournalEntryId = toNumber(settledAccrRevJournal.journalEntryId);
  assert(accrRevJournalEntryId > 0, "Accrual settle must return journalEntryId");

  const accrRevJournalLines = await query(
    `SELECT account_id, debit_base, credit_base
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no`,
    [accrRevJournalEntryId]
  );
  assert(
    accrRevJournalLines.rows.length === 4,
    "Accrued-revenue long-term settle should produce 4 journal lines"
  );
  const accrRevAccounts = new Set(
    accrRevJournalLines.rows.map((row) => toNumber(row.account_id))
  );
  assert(
    accrRevAccounts.has(fixture.accounts.accrRevLongAsset),
    "Accrued-revenue settle must use ACCR_REV_LONG_ASSET"
  );
  assert(
    accrRevAccounts.has(fixture.accounts.accrRevShortAsset),
    "Accrued-revenue settle must use ACCR_REV_SHORT_ASSET for 281->181 reclass"
  );
  assert(
    accrRevAccounts.has(fixture.accounts.accrRevRevenue),
    "Accrued-revenue settle must use ACCR_REV_REVENUE"
  );
  const accrRevDebit = accrRevJournalLines.rows.reduce(
    (sum, row) => sum + toNumber(row.debit_base),
    0
  );
  const accrRevCredit = accrRevJournalLines.rows.reduce(
    (sum, row) => sum + toNumber(row.credit_base),
    0
  );
  assert(amountsEqual(accrRevDebit, accrRevCredit), "Accrued-revenue journal must be balanced");

  const accrRevSubledgerRows = await query(
    `SELECT entry_kind
     FROM revenue_recognition_subledger_entries
     WHERE tenant_id = ?
       AND run_id = ?`,
    [tenantId, accrRevId]
  );
  const subledgerKindCounts = new Map();
  for (const row of accrRevSubledgerRows.rows || []) {
    const kind = String(row.entry_kind || "");
    subledgerKindCounts.set(kind, (subledgerKindCounts.get(kind) || 0) + 1);
  }
  assert(
    toNumber(subledgerKindCounts.get("RECOGNITION")) === 1,
    "Accrued-revenue settle must create one RECOGNITION entry"
  );
  assert(
    toNumber(subledgerKindCounts.get("RECLASS")) === 1,
    "Accrued-revenue settle must create one RECLASS entry"
  );

  const accrRevRunLineRows = await query(
    `SELECT status
     FROM revenue_recognition_run_lines
     WHERE tenant_id = ?
       AND run_id = ?`,
    [tenantId, accrRevId]
  );
  assert(
    accrRevRunLineRows.rows.every((row) => String(row.status) === "SETTLED"),
    "Accrual settle must mark run lines as SETTLED"
  );

  const settleAgain = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrRevId}/settle`,
    expectedStatus: 400,
    body: { settlementPeriodId: fixture.period2Id },
  });
  assert(
    String(settleAgain.json?.message || "").includes("already settled/posted"),
    "Double-settle must be blocked"
  );

  const reverseBeforeMaturityBoundary = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrRevId}/reverse`,
    expectedStatus: 400,
    body: { reversalPeriodId: fixture.period1Id },
  });
  assert(
    String(reverseBeforeMaturityBoundary.json?.message || "").includes("maturity boundary"),
    "Accrual reverse must enforce due/maturity boundary"
  );

  const reverseClosedPeriod = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrRevId}/reverse`,
    expectedStatus: 400,
    body: { reversalPeriodId: fixture.period3Id },
  });
  assert(
    String(reverseClosedPeriod.json?.message || "").includes("Period is HARD_CLOSED"),
    "Accrual reverse must fail when reversal period is not OPEN"
  );

  const reverseAccrRev = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrRevId}/reverse`,
    expectedStatus: 201,
    body: { reversalPeriodId: fixture.period2Id, reason: "PR17C accrued revenue reverse" },
  });
  const reversedAccrRevRow = reverseAccrRev.json?.row || {};
  const reversedAccrRevReversalRun = reverseAccrRev.json?.reversalRun || {};
  assert(
    String(reversedAccrRevRow.status) === "REVERSED",
    "Accrual reverse must set original accrual status to REVERSED"
  );
  assert(
    String(reversedAccrRevReversalRun.status) === "POSTED",
    "Accrual reverse must create posted reversal run"
  );
  assert(
    toNumber(reversedAccrRevReversalRun.reversalOfRunId) === accrRevId,
    "Accrual reversal run must link to original accrual"
  );

  const reverseAgain = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrRevId}/reverse`,
    expectedStatus: 400,
  });
  assert(
    String(reverseAgain.json?.message || "").includes("settled/posted accrual state"),
    "Reverse of already-reversed accrual must be blocked"
  );

  const accrExpGenerate = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/accruals/generate",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.period1Id,
      accountFamily: "ACCRUED_EXPENSE",
      maturityBucket: "LONG_TERM",
      maturityDate: "2026-09-15",
      reclassRequired: true,
      currencyCode: "USD",
      fxRate: 1,
      totalAmountTxn: 90,
      totalAmountBase: 90,
      sourceRunUid: `RR17C-ACCREXP-RUN-${stamp}`,
      runNo: `RR17C-ACCREXP-${stamp}`,
    },
  });
  const accrExpId = toNumber(accrExpGenerate.json?.row?.id);
  assert(accrExpId > 0, "Accrued-expense accrual generate must return row.id");

  const settleExpMissingMappings = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrExpId}/settle`,
    expectedStatus: 400,
    body: { settlementPeriodId: fixture.period2Id },
  });
  assert(
    String(settleExpMissingMappings.json?.message || "").includes("Setup required"),
    "Settle must fail when required ACCR_EXP purpose-account mappings are missing"
  );

  await upsertPurposeMappings({
    tenantId,
    legalEntityId: fixture.legalEntityId,
    mappings: [
      {
        purposeCode: "ACCR_EXP_SHORT_LIABILITY",
        accountId: fixture.accounts.accrExpShortLiability,
      },
      {
        purposeCode: "ACCR_EXP_LONG_LIABILITY",
        accountId: fixture.accounts.accrExpLongLiability,
      },
      {
        purposeCode: "ACCR_EXP_EXPENSE",
        accountId: fixture.accounts.accrExpExpense,
      },
      {
        purposeCode: "ACCR_EXP_RECLASS",
        accountId: fixture.accounts.accrExpReclass,
      },
    ],
  });

  const settleAccrExp = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrExpId}/settle`,
    expectedStatus: 200,
    body: { settlementPeriodId: fixture.period2Id },
  });
  const settledAccrExpRow = settleAccrExp.json?.row || {};
  const settledAccrExpJournal = settleAccrExp.json?.journal || {};
  assert(
    String(settledAccrExpRow.status) === "POSTED",
    "Accrued-expense settle should transition accrual to POSTED"
  );
  assert(
    toNumber(settleAccrExp.json?.subledgerEntryCount) === 2,
    "Accrued-expense long-term settle should create RECOGNITION + RECLASS entries"
  );

  const accrExpJournalEntryId = toNumber(settledAccrExpJournal.journalEntryId);
  assert(accrExpJournalEntryId > 0, "Accrued-expense settle must return journalEntryId");
  const accrExpJournalLines = await query(
    `SELECT account_id
     FROM journal_lines
     WHERE journal_entry_id = ?`,
    [accrExpJournalEntryId]
  );
  assert(
    accrExpJournalLines.rows.length === 4,
    "Accrued-expense long-term settle should produce 4 journal lines"
  );
  const accrExpAccounts = new Set(
    accrExpJournalLines.rows.map((row) => toNumber(row.account_id))
  );
  assert(
    accrExpAccounts.has(fixture.accounts.accrExpLongLiability),
    "Accrued-expense settle must use ACCR_EXP_LONG_LIABILITY"
  );
  assert(
    accrExpAccounts.has(fixture.accounts.accrExpShortLiability),
    "Accrued-expense settle must use ACCR_EXP_SHORT_LIABILITY for 481->381 reclass"
  );
  assert(
    accrExpAccounts.has(fixture.accounts.accrExpExpense),
    "Accrued-expense settle must use ACCR_EXP_EXPENSE"
  );

  const reverseAccrExp = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrExpId}/reverse`,
    expectedStatus: 201,
    body: { reversalPeriodId: fixture.period2Id, reason: "PR17C accrued expense reverse" },
  });
  assert(
    String(reverseAccrExp.json?.row?.status || "") === "REVERSED",
    "Accrued-expense reverse should transition accrual to REVERSED"
  );
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
      tenantId: identity.tenantId,
      fixture,
      stamp: identity.stamp,
    });

    console.log("Revenue PR-17C accrual lifecycle test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          legalEntityId: fixture.legalEntityId,
          period1Id: fixture.period1Id,
          period2Id: fixture.period2Id,
          period3Id: fixture.period3Id,
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
    console.error("Revenue PR-17C test failed:", err);
    try {
      await closePool();
    } catch {
      // ignore close failures
    }
    process.exit(1);
  });
