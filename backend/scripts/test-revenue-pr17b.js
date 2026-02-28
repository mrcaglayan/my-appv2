import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.REVENUE_PR17B_TEST_PORT || 3122);
const BASE_URL =
  process.env.REVENUE_PR17B_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
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
      // retry until timeout
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

async function assertPermissionsSeeded() {
  const requiredPermissions = [
    "revenue.schedule.read",
    "revenue.schedule.generate",
    "revenue.run.read",
    "revenue.run.create",
    "revenue.run.post",
    "revenue.run.reverse",
    "revenue.report.read",
  ];
  const rows = await query(
    `SELECT code
     FROM permissions
     WHERE code IN (${inClause(requiredPermissions)})`,
    requiredPermissions
  );
  const existing = new Set(rows.rows.map((row) => String(row.code)));
  const missing = requiredPermissions.filter((code) => !existing.has(code));
  assert(missing.length === 0, `Missing revenue permissions: ${missing.join(", ")}`);
}

async function createTenantAndAdmin() {
  const stamp = Date.now();
  const tenantCode = `RR17B_T_${stamp}`;
  const tenantName = `Revenue PR17B Tenant ${stamp}`;
  const email = `revenue_pr17b_admin_${stamp}@example.com`;
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
    [tenantId, email, passwordHash, "Revenue PR17B Admin"]
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

  const groupInsert = await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `RR17B_G_${stamp}`, `Revenue Group ${stamp}`]
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
      `RR17B_LE_${stamp}`,
      `Revenue Legal Entity ${stamp}`,
      countryId,
    ]
  );
  const legalEntityId = toNumber(legalEntityInsert.rows?.insertId);
  assert(legalEntityId > 0, "Failed to create legal entity");

  const calendarInsert = await query(
    `INSERT INTO fiscal_calendars (tenant_id, code, name, year_start_month, year_start_day)
     VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `RR17B_CAL_${stamp}`, `Revenue Calendar ${stamp}`]
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
    [tenantId, legalEntityId, calendarId, `RR17B_BOOK_${stamp}`, `Revenue Book ${stamp}`]
  );
  const bookId = toNumber(bookInsert.rows?.insertId);
  assert(bookId > 0, "Failed to create book");

  await upsertPeriodStatus({
    bookId,
    fiscalPeriodId: period1Id,
    status: "OPEN",
    userId,
    note: "PR17B open period",
  });
  await upsertPeriodStatus({
    bookId,
    fiscalPeriodId: period2Id,
    status: "HARD_CLOSED",
    userId,
    note: "PR17B closed period",
  });

  const coaInsert = await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `RR17B_COA_${stamp}`, `Revenue CoA ${stamp}`]
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

  const accountCodePrefix = `R17B${String(stamp).slice(-6)}`;
  const accounts = {
    defrevShortLiability: await createAccount({
      code: `${accountCodePrefix}01`,
      name: `DEFREV Short Liability ${stamp}`,
      accountType: "LIABILITY",
      normalSide: "CREDIT",
    }),
    defrevLongLiability: await createAccount({
      code: `${accountCodePrefix}02`,
      name: `DEFREV Long Liability ${stamp}`,
      accountType: "LIABILITY",
      normalSide: "CREDIT",
    }),
    defrevRevenue: await createAccount({
      code: `${accountCodePrefix}03`,
      name: `DEFREV Revenue ${stamp}`,
      accountType: "REVENUE",
      normalSide: "CREDIT",
    }),
    defrevReclass: await createAccount({
      code: `${accountCodePrefix}04`,
      name: `DEFREV Reclass ${stamp}`,
      accountType: "LIABILITY",
      normalSide: "CREDIT",
    }),
    prepaidShortAsset: await createAccount({
      code: `${accountCodePrefix}05`,
      name: `Prepaid Short Asset ${stamp}`,
      accountType: "ASSET",
      normalSide: "DEBIT",
    }),
    prepaidLongAsset: await createAccount({
      code: `${accountCodePrefix}06`,
      name: `Prepaid Long Asset ${stamp}`,
      accountType: "ASSET",
      normalSide: "DEBIT",
    }),
    prepaidExpense: await createAccount({
      code: `${accountCodePrefix}07`,
      name: `Prepaid Expense ${stamp}`,
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    }),
    prepaidReclass: await createAccount({
      code: `${accountCodePrefix}08`,
      name: `Prepaid Reclass ${stamp}`,
      accountType: "ASSET",
      normalSide: "DEBIT",
    }),
  };

  return {
    legalEntityId,
    period1Id,
    period2Id,
    bookId,
    accounts,
  };
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

async function runApiAssertions({
  token,
  tenantId,
  fixture,
  stamp,
}) {
  const defrevScheduleGenerate = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/schedules/generate",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.period1Id,
      accountFamily: "DEFREV",
      maturityBucket: "LONG_TERM",
      maturityDate: "2027-06-30",
      reclassRequired: true,
      currencyCode: "USD",
      fxRate: 1,
      amountTxn: 120,
      amountBase: 120,
      sourceEventUid: `RR17B-SCHED-DEF-${stamp}`,
    },
  });
  const defrevScheduleId = toNumber(defrevScheduleGenerate.json?.row?.id);
  assert(defrevScheduleId > 0, "DEFREV schedule generate should return row.id");

  const defrevRunCreate = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/runs",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.period1Id,
      scheduleId: defrevScheduleId,
      accountFamily: "DEFREV",
      maturityBucket: "LONG_TERM",
      maturityDate: "2027-06-30",
      reclassRequired: true,
      currencyCode: "USD",
      fxRate: 1,
      totalAmountTxn: 120,
      totalAmountBase: 120,
      sourceRunUid: `RR17B-RUN-DEF-${stamp}`,
      runNo: `RR17B-DEF-${stamp}`,
    },
  });
  const defrevRunId = toNumber(defrevRunCreate.json?.row?.id);
  assert(defrevRunId > 0, "DEFREV run create should return row.id");
  assert(
    String(defrevRunCreate.json?.row?.status) === "DRAFT",
    "New run should start as DRAFT"
  );

  const duplicateRunAttempt = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/runs",
    expectedStatus: 400,
    body: {
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.period1Id,
      scheduleId: defrevScheduleId,
      accountFamily: "DEFREV",
      maturityBucket: "LONG_TERM",
      maturityDate: "2027-06-30",
      reclassRequired: true,
      currencyCode: "USD",
      fxRate: 1,
      totalAmountTxn: 120,
      totalAmountBase: 120,
      sourceRunUid: `RR17B-RUN-DEF-DUP-${stamp}`,
      runNo: `RR17B-DEF-DUP-${stamp}`,
    },
  });
  assert(
    String(duplicateRunAttempt.json?.message || "").includes("Duplicate rerun guard"),
    "Create run must enforce duplicate open-line rerun guard"
  );

  const postWithoutDefrevMappings = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${defrevRunId}/post`,
    expectedStatus: 400,
  });
  assert(
    String(postWithoutDefrevMappings.json?.message || "").includes("Setup required"),
    "POST run must fail when DEFREV purpose-account mappings are missing"
  );

  await upsertPurposeMappings({
    tenantId,
    legalEntityId: fixture.legalEntityId,
    mappings: [
      {
        purposeCode: "DEFREV_SHORT_LIABILITY",
        accountId: fixture.accounts.defrevShortLiability,
      },
      {
        purposeCode: "DEFREV_LONG_LIABILITY",
        accountId: fixture.accounts.defrevLongLiability,
      },
      {
        purposeCode: "DEFREV_REVENUE",
        accountId: fixture.accounts.defrevRevenue,
      },
      {
        purposeCode: "DEFREV_RECLASS",
        accountId: fixture.accounts.defrevReclass,
      },
    ],
  });

  const defrevPostResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${defrevRunId}/post`,
    expectedStatus: 200,
  });
  const postedRunRow = defrevPostResponse.json?.row || {};
  const postedJournalSummary = defrevPostResponse.json?.journal || {};
  assert(
    String(postedRunRow.status) === "POSTED",
    "Run status must transition to POSTED on post"
  );
  assert(
    toNumber(postedRunRow.postedJournalEntryId) ===
      toNumber(postedJournalSummary.journalEntryId),
    "Run posted_journal_entry_id must match post response journal id"
  );
  assert(
    toNumber(defrevPostResponse.json?.subledgerEntryCount) === 2,
    "DEFREV long-term reclass post must create 2 subledger entries (RECOGNITION + RECLASS)"
  );

  const postedJournalEntryId = toNumber(postedJournalSummary.journalEntryId);
  assert(postedJournalEntryId > 0, "Post response must include journalEntryId");

  const postedJournalRows = await query(
    `SELECT status, total_debit_base, total_credit_base
     FROM journal_entries
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, postedJournalEntryId]
  );
  const postedJournal = postedJournalRows.rows?.[0] || null;
  assert(Boolean(postedJournal), "Posted journal row not found");
  assert(String(postedJournal.status) === "POSTED", "Posted journal status must be POSTED");
  assert(
    amountsEqual(postedJournal.total_debit_base, postedJournal.total_credit_base),
    "Posted journal totals must be balanced"
  );

  const postedJournalLineRows = await query(
    `SELECT account_id, debit_base, credit_base
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no ASC`,
    [postedJournalEntryId]
  );
  assert(
    postedJournalLineRows.rows.length === 4,
    "DEFREV long-term run should produce 4 journal lines (recognition + reclass)"
  );
  const accountIds = new Set(postedJournalLineRows.rows.map((row) => toNumber(row.account_id)));
  assert(
    accountIds.has(fixture.accounts.defrevLongLiability),
    "DEFREV journal should include long-liability account"
  );
  assert(
    accountIds.has(fixture.accounts.defrevShortLiability),
    "DEFREV journal should include short-liability account for 480->380 reclass"
  );
  assert(
    accountIds.has(fixture.accounts.defrevRevenue),
    "DEFREV journal should include revenue account"
  );

  const subledgerRows = await query(
    `SELECT entry_kind
     FROM revenue_recognition_subledger_entries
     WHERE tenant_id = ?
       AND run_id = ?`,
    [tenantId, defrevRunId]
  );
  const subledgerKinds = new Map();
  for (const row of subledgerRows.rows || []) {
    const key = String(row.entry_kind || "");
    subledgerKinds.set(key, (subledgerKinds.get(key) || 0) + 1);
  }
  assert(
    toNumber(subledgerKinds.get("RECOGNITION")) === 1,
    "DEFREV post must create one RECOGNITION subledger row"
  );
  assert(
    toNumber(subledgerKinds.get("RECLASS")) === 1,
    "DEFREV long-term reclass post must create one RECLASS subledger row"
  );

  const runLineStatusRows = await query(
    `SELECT status, posted_journal_entry_id
     FROM revenue_recognition_run_lines
     WHERE tenant_id = ?
       AND run_id = ?`,
    [tenantId, defrevRunId]
  );
  assert(runLineStatusRows.rows.length >= 1, "Run lines should exist for posted run");
  assert(
    runLineStatusRows.rows.every((row) => String(row.status) === "POSTED"),
    "All run lines must transition to POSTED"
  );
  assert(
    runLineStatusRows.rows.every(
      (row) => toNumber(row.posted_journal_entry_id) === postedJournalEntryId
    ),
    "All run lines must reference posted journal id"
  );

  const doublePostAttempt = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${defrevRunId}/post`,
    expectedStatus: 400,
  });
  assert(
    String(doublePostAttempt.json?.message || "").includes("already POSTED"),
    "Double-post must be blocked"
  );

  const reverseClosedPeriodAttempt = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${defrevRunId}/reverse`,
    expectedStatus: 400,
    body: {
      reversalPeriodId: fixture.period2Id,
      reason: "Closed-period reverse test",
    },
  });
  assert(
    String(reverseClosedPeriodAttempt.json?.message || "").includes("Period is HARD_CLOSED"),
    "Reverse must fail when reversal target period is not OPEN"
  );

  const reverseResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${defrevRunId}/reverse`,
    expectedStatus: 201,
    body: {
      reason: "PR17B reversal smoke",
    },
  });
  const reversedOriginal = reverseResponse.json?.row || {};
  const reversalRun = reverseResponse.json?.reversalRun || {};
  const reverseJournal = reverseResponse.json?.journal || {};
  assert(
    String(reversedOriginal.status) === "REVERSED",
    "Original run must transition to REVERSED on reverse"
  );
  assert(
    String(reversalRun.status) === "POSTED",
    "Reversal run must be persisted as POSTED"
  );
  assert(
    toNumber(reversalRun.reversalOfRunId) === defrevRunId,
    "Reversal run must keep reversal_of_run_id linkage"
  );
  assert(
    toNumber(reverseJournal.originalPostedJournalEntryId) === postedJournalEntryId,
    "Reverse response must expose original posted journal id"
  );
  assert(
    toNumber(reverseJournal.reversalJournalEntryId) > 0,
    "Reverse response must expose reversal journal id"
  );

  const originalRunRow = await query(
    `SELECT status, reversal_journal_entry_id
     FROM revenue_recognition_runs
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, defrevRunId]
  );
  assert(
    String(originalRunRow.rows?.[0]?.status || "") === "REVERSED",
    "Original run row must be REVERSED in DB"
  );
  assert(
    toNumber(originalRunRow.rows?.[0]?.reversal_journal_entry_id) ===
      toNumber(reverseJournal.reversalJournalEntryId),
    "Original run row must store reversal_journal_entry_id"
  );

  const originalJournalAfterReverse = await query(
    `SELECT status, reversal_journal_entry_id
     FROM journal_entries
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, postedJournalEntryId]
  );
  assert(
    String(originalJournalAfterReverse.rows?.[0]?.status || "") === "REVERSED",
    "Original posted journal must transition to REVERSED"
  );
  assert(
    toNumber(originalJournalAfterReverse.rows?.[0]?.reversal_journal_entry_id) ===
      toNumber(reverseJournal.reversalJournalEntryId),
    "Original posted journal must point to reversal journal"
  );

  const reversalRunId = toNumber(reversalRun.id);
  assert(reversalRunId > 0, "Reverse response must include reversal run id");
  const originalRunLineCountRows = await query(
    `SELECT COUNT(*) AS total
     FROM revenue_recognition_run_lines
     WHERE tenant_id = ?
       AND run_id = ?`,
    [tenantId, defrevRunId]
  );
  const originalRunLineCount = toNumber(originalRunLineCountRows.rows?.[0]?.total);
  const reversalRunLineRows = await query(
    `SELECT reversal_of_run_line_id
     FROM revenue_recognition_run_lines
     WHERE tenant_id = ?
       AND run_id = ?`,
    [tenantId, reversalRunId]
  );
  assert(
    reversalRunLineRows.rows.length === originalRunLineCount,
    "Reversal run must contain same number of run lines as original"
  );
  assert(
    reversalRunLineRows.rows.every((row) => toNumber(row.reversal_of_run_line_id) > 0),
    "Every reversal run line must keep reversal_of_run_line_id linkage"
  );

  const reverseAgainAttempt = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${defrevRunId}/reverse`,
    expectedStatus: 400,
  });
  assert(
    String(reverseAgainAttempt.json?.message || "").includes("allowed only from POSTED"),
    "Reverse of already-reversed run must be blocked"
  );

  const prepaidScheduleGenerate = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/schedules/generate",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.period2Id,
      accountFamily: "PREPAID_EXPENSE",
      maturityBucket: "LONG_TERM",
      maturityDate: "2027-12-31",
      reclassRequired: true,
      currencyCode: "USD",
      fxRate: 1,
      amountTxn: 80,
      amountBase: 80,
      sourceEventUid: `RR17B-SCHED-PRE-${stamp}`,
    },
  });
  const prepaidScheduleId = toNumber(prepaidScheduleGenerate.json?.row?.id);
  assert(prepaidScheduleId > 0, "PREPAID schedule generate should return row.id");

  const prepaidRunCreate = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/runs",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.period2Id,
      scheduleId: prepaidScheduleId,
      accountFamily: "PREPAID_EXPENSE",
      maturityBucket: "LONG_TERM",
      maturityDate: "2027-12-31",
      reclassRequired: true,
      currencyCode: "USD",
      fxRate: 1,
      totalAmountTxn: 80,
      totalAmountBase: 80,
      sourceRunUid: `RR17B-RUN-PRE-${stamp}`,
      runNo: `RR17B-PRE-${stamp}`,
    },
  });
  const prepaidRunId = toNumber(prepaidRunCreate.json?.row?.id);
  assert(prepaidRunId > 0, "PREPAID run create should return row.id");

  const reverseDraftAttempt = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${prepaidRunId}/reverse`,
    expectedStatus: 400,
  });
  assert(
    String(reverseDraftAttempt.json?.message || "").includes("allowed only from POSTED"),
    "Reverse must be blocked for non-posted run"
  );

  const postClosedPeriodAttempt = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${prepaidRunId}/post`,
    expectedStatus: 400,
  });
  assert(
    String(postClosedPeriodAttempt.json?.message || "").includes("Period is HARD_CLOSED"),
    "POST run must fail when target period is not OPEN"
  );

  await upsertPeriodStatus({
    bookId: fixture.bookId,
    fiscalPeriodId: fixture.period2Id,
    status: "OPEN",
    userId: 0,
    note: "PR17B reopened period for PREPAID post",
  });

  const postWithoutPrepaidMappings = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${prepaidRunId}/post`,
    expectedStatus: 400,
  });
  assert(
    String(postWithoutPrepaidMappings.json?.message || "").includes("Setup required"),
    "POST run must fail when PREPAID purpose-account mappings are missing"
  );

  await upsertPurposeMappings({
    tenantId,
    legalEntityId: fixture.legalEntityId,
    mappings: [
      {
        purposeCode: "PREPAID_EXP_SHORT_ASSET",
        accountId: fixture.accounts.prepaidShortAsset,
      },
      {
        purposeCode: "PREPAID_EXP_LONG_ASSET",
        accountId: fixture.accounts.prepaidLongAsset,
      },
      {
        purposeCode: "PREPAID_EXPENSE",
        accountId: fixture.accounts.prepaidExpense,
      },
      {
        purposeCode: "PREPAID_RECLASS",
        accountId: fixture.accounts.prepaidReclass,
      },
    ],
  });

  const prepaidPostResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${prepaidRunId}/post`,
    expectedStatus: 200,
  });
  const prepaidPostRow = prepaidPostResponse.json?.row || {};
  const prepaidJournal = prepaidPostResponse.json?.journal || {};
  assert(
    String(prepaidPostRow.status) === "POSTED",
    "PREPAID run must transition to POSTED on post"
  );
  assert(
    toNumber(prepaidPostResponse.json?.subledgerEntryCount) === 2,
    "PREPAID long-term reclass post must create 2 subledger entries"
  );

  const prepaidJournalEntryId = toNumber(prepaidJournal.journalEntryId);
  assert(prepaidJournalEntryId > 0, "PREPAID post must return journal entry id");
  const prepaidJournalLines = await query(
    `SELECT account_id
     FROM journal_lines
     WHERE journal_entry_id = ?`,
    [prepaidJournalEntryId]
  );
  const prepaidAccountIds = new Set(
    prepaidJournalLines.rows.map((row) => toNumber(row.account_id))
  );
  assert(
    prepaidAccountIds.has(fixture.accounts.prepaidLongAsset),
    "PREPAID journal should include long asset account"
  );
  assert(
    prepaidAccountIds.has(fixture.accounts.prepaidShortAsset),
    "PREPAID journal should include short asset account for 280->180 reclass"
  );
  assert(
    prepaidAccountIds.has(fixture.accounts.prepaidExpense),
    "PREPAID journal should include expense account"
  );

  const runListResponse = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/revenue-recognition/runs?legalEntityId=${fixture.legalEntityId}`,
    expectedStatus: 200,
  });
  const runIds = new Set((runListResponse.json?.rows || []).map((row) => toNumber(row.id)));
  assert(runIds.has(defrevRunId), "Run list should contain DEFREV run");
  assert(runIds.has(prepaidRunId), "Run list should contain PREPAID run");
}

async function main() {
  await runMigrations();
  await seedCore({ ensureDefaultTenantIfMissing: true });
  await assertPermissionsSeeded();

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

    console.log("Revenue PR-17B DEFREV + PREPAID test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          legalEntityId: fixture.legalEntityId,
          period1Id: fixture.period1Id,
          period2Id: fixture.period2Id,
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
    console.error("Revenue PR-17B test failed:", err);
    try {
      await closePool();
    } catch {
      // ignore close failures
    }
    process.exit(1);
  });
