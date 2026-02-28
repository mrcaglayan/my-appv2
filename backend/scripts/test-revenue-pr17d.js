import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.REVENUE_PR17D_TEST_PORT || 3124);
const BASE_URL =
  process.env.REVENUE_PR17D_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
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

function hasExpectedKey(explainRows, expectedPrefixes) {
  return (explainRows || []).some((row) =>
    (expectedPrefixes || []).some((prefix) =>
      String(row?.key || "").includes(prefix)
    )
  );
}

async function explainOne(sql, params = []) {
  const result = await query(`EXPLAIN ${sql}`, params);
  return result.rows || [];
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

async function createTenantAndAdmin() {
  const stamp = Date.now();
  const tenantCode = `RR17D_T_${stamp}`;
  const tenantName = `Revenue PR17D Tenant ${stamp}`;
  const email = `revenue_pr17d_admin_${stamp}@example.com`;
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
    [tenantId, email, passwordHash, "Revenue PR17D Admin"]
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
    [tenantId, `RR17D_G_${stamp}`, `Revenue Group ${stamp}`]
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
      `RR17D_LE_${stamp}`,
      `Revenue Legal Entity ${stamp}`,
      countryId,
    ]
  );
  const legalEntityId = toNumber(legalEntityInsert.rows?.insertId);
  assert(legalEntityId > 0, "Failed to create legal entity");

  const calendarInsert = await query(
    `INSERT INTO fiscal_calendars (tenant_id, code, name, year_start_month, year_start_day)
     VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `RR17D_CAL_${stamp}`, `Revenue Calendar ${stamp}`]
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
    [tenantId, legalEntityId, calendarId, `RR17D_BOOK_${stamp}`, `Revenue Book ${stamp}`]
  );
  const bookId = toNumber(bookInsert.rows?.insertId);
  assert(bookId > 0, "Failed to create book");

  await upsertPeriodStatus({
    bookId,
    fiscalPeriodId: period1Id,
    status: "OPEN",
    userId,
    note: "PR17D period1 open",
  });
  await upsertPeriodStatus({
    bookId,
    fiscalPeriodId: period2Id,
    status: "OPEN",
    userId,
    note: "PR17D period2 open",
  });

  const coaInsert = await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `RR17D_COA_${stamp}`, `Revenue CoA ${stamp}`]
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

  const accountCodePrefix = `R17D${String(stamp).slice(-6)}`;
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
    accrRevShortAsset: await createAccount({
      code: `${accountCodePrefix}09`,
      name: `ACCR_REV Short Asset ${stamp}`,
      accountType: "ASSET",
      normalSide: "DEBIT",
    }),
    accrRevLongAsset: await createAccount({
      code: `${accountCodePrefix}10`,
      name: `ACCR_REV Long Asset ${stamp}`,
      accountType: "ASSET",
      normalSide: "DEBIT",
    }),
    accrRevRevenue: await createAccount({
      code: `${accountCodePrefix}11`,
      name: `ACCR_REV Revenue ${stamp}`,
      accountType: "REVENUE",
      normalSide: "CREDIT",
    }),
    accrRevReclass: await createAccount({
      code: `${accountCodePrefix}12`,
      name: `ACCR_REV Reclass ${stamp}`,
      accountType: "ASSET",
      normalSide: "DEBIT",
    }),
    accrExpShortLiability: await createAccount({
      code: `${accountCodePrefix}13`,
      name: `ACCR_EXP Short Liability ${stamp}`,
      accountType: "LIABILITY",
      normalSide: "CREDIT",
    }),
    accrExpLongLiability: await createAccount({
      code: `${accountCodePrefix}14`,
      name: `ACCR_EXP Long Liability ${stamp}`,
      accountType: "LIABILITY",
      normalSide: "CREDIT",
    }),
    accrExpExpense: await createAccount({
      code: `${accountCodePrefix}15`,
      name: `ACCR_EXP Expense ${stamp}`,
      accountType: "EXPENSE",
      normalSide: "DEBIT",
    }),
    accrExpReclass: await createAccount({
      code: `${accountCodePrefix}16`,
      name: `ACCR_EXP Reclass ${stamp}`,
      accountType: "LIABILITY",
      normalSide: "CREDIT",
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

async function createAndPostRun({
  token,
  fixture,
  stamp,
  family,
  amount,
  sourcePrefix,
}) {
  const scheduleResponse = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/schedules/generate",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.period1Id,
      accountFamily: family,
      maturityBucket: "LONG_TERM",
      maturityDate: "2027-06-30",
      reclassRequired: true,
      currencyCode: "USD",
      fxRate: 1,
      amountTxn: amount,
      amountBase: amount,
      sourceEventUid: `${sourcePrefix}-SCHED-${stamp}`,
    },
  });
  const scheduleId = toNumber(scheduleResponse.json?.row?.id);
  assert(scheduleId > 0, `${family} schedule generate should return row.id`);

  const runResponse = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/runs",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.period1Id,
      scheduleId,
      accountFamily: family,
      maturityBucket: "LONG_TERM",
      maturityDate: "2027-06-30",
      reclassRequired: true,
      currencyCode: "USD",
      fxRate: 1,
      totalAmountTxn: amount,
      totalAmountBase: amount,
      sourceRunUid: `${sourcePrefix}-RUN-${stamp}`,
      runNo: `${sourcePrefix}-${stamp}`,
    },
  });
  const runId = toNumber(runResponse.json?.row?.id);
  assert(runId > 0, `${family} run create should return row.id`);

  const postResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${runId}/post`,
    expectedStatus: 200,
  });
  assert(
    String(postResponse.json?.row?.status || "") === "POSTED",
    `${family} run should be POSTED`
  );

  return {
    scheduleId,
    runId,
    postResponse: postResponse.json,
  };
}

async function createAndSettleAccrual({
  token,
  fixture,
  stamp,
  family,
  amount,
  sourcePrefix,
}) {
  const accrualGenerate = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/accruals/generate",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.period1Id,
      accountFamily: family,
      maturityBucket: "LONG_TERM",
      maturityDate: "2026-09-30",
      reclassRequired: true,
      currencyCode: "USD",
      fxRate: 1,
      totalAmountTxn: amount,
      totalAmountBase: amount,
      sourceRunUid: `${sourcePrefix}-RUN-${stamp}`,
      runNo: `${sourcePrefix}-${stamp}`,
    },
  });
  const accrualId = toNumber(accrualGenerate.json?.row?.id);
  assert(accrualId > 0, `${family} accrual generate should return row.id`);

  const settleResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/accruals/${accrualId}/settle`,
    expectedStatus: 200,
    body: { settlementPeriodId: fixture.period2Id },
  });
  assert(
    String(settleResponse.json?.row?.status || "") === "POSTED",
    `${family} accrual should settle to POSTED`
  );

  return {
    accrualId,
    settleResponse: settleResponse.json,
  };
}

async function runApiAssertions({
  token,
  tenantId,
  fixture,
  stamp,
}) {
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

  await createAndPostRun({
    token,
    fixture,
    stamp,
    family: "DEFREV",
    amount: 120,
    sourcePrefix: "RR17D-DEFREV",
  });
  await createAndPostRun({
    token,
    fixture,
    stamp,
    family: "PREPAID_EXPENSE",
    amount: 80,
    sourcePrefix: "RR17D-PREPAID",
  });
  await createAndSettleAccrual({
    token,
    fixture,
    stamp,
    family: "ACCRUED_REVENUE",
    amount: 150,
    sourcePrefix: "RR17D-ACCRREV",
  });
  await createAndSettleAccrual({
    token,
    fixture,
    stamp,
    family: "ACCRUED_EXPENSE",
    amount: 90,
    sourcePrefix: "RR17D-ACCREXP",
  });

  const deferredSplit = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/revenue-recognition/reports/deferred-revenue-split?legalEntityId=${fixture.legalEntityId}&asOfDate=2026-12-31`,
    expectedStatus: 200,
  });
  assert(
    String(deferredSplit.json?.reportCode || "") === "DEFERRED_REVENUE_SPLIT",
    "Deferred split reportCode mismatch"
  );
  assert(
    deferredSplit.json?.reconciled === true &&
      toNumber(deferredSplit.json?.reconciliation?.unmatchedGroups) === 0,
    "Deferred split reconciliation must be fully matched"
  );
  const deferredRow = (deferredSplit.json?.rows || []).find(
    (row) => String(row.accountFamily) === "DEFREV"
  );
  assert(Boolean(deferredRow), "Deferred split row for DEFREV missing");
  assert(
    amountsEqual(deferredRow.shortTermAmountBase, 120),
    "Deferred split short-term amount must be 120"
  );
  assert(
    amountsEqual(deferredRow.longTermAmountBase, 0),
    "Deferred split long-term amount must be 0 after reclass"
  );

  const prepaidSplit = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/revenue-recognition/reports/prepaid-expense-split?legalEntityId=${fixture.legalEntityId}&asOfDate=2026-12-31`,
    expectedStatus: 200,
  });
  assert(
    String(prepaidSplit.json?.reportCode || "") === "PREPAID_EXPENSE_SPLIT",
    "Prepaid split reportCode mismatch"
  );
  assert(
    prepaidSplit.json?.reconciled === true &&
      toNumber(prepaidSplit.json?.reconciliation?.unmatchedGroups) === 0,
    "Prepaid split reconciliation must be fully matched"
  );
  const prepaidRow = (prepaidSplit.json?.rows || []).find(
    (row) => String(row.accountFamily) === "PREPAID_EXPENSE"
  );
  assert(Boolean(prepaidRow), "Prepaid split row missing");
  assert(
    amountsEqual(prepaidRow.shortTermAmountBase, 80),
    "Prepaid split short-term amount must be 80"
  );
  assert(
    amountsEqual(prepaidRow.longTermAmountBase, 0),
    "Prepaid split long-term amount must be 0 after reclass"
  );

  const accrualSplit = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/revenue-recognition/reports/accrual-split?legalEntityId=${fixture.legalEntityId}&asOfDate=2026-12-31`,
    expectedStatus: 200,
  });
  assert(
    String(accrualSplit.json?.reportCode || "") === "ACCRUAL_SPLIT",
    "Accrual split reportCode mismatch"
  );
  assert(
    accrualSplit.json?.reconciled === true &&
      toNumber(accrualSplit.json?.reconciliation?.unmatchedGroups) === 0,
    "Accrual split reconciliation must be fully matched"
  );
  const accrualRowsByFamily = new Map(
    (accrualSplit.json?.rows || []).map((row) => [String(row.accountFamily), row])
  );
  assert(accrualRowsByFamily.has("ACCRUED_REVENUE"), "Accrued revenue split row missing");
  assert(accrualRowsByFamily.has("ACCRUED_EXPENSE"), "Accrued expense split row missing");
  assert(
    amountsEqual(accrualRowsByFamily.get("ACCRUED_REVENUE")?.shortTermAmountBase, 150),
    "Accrued-revenue short-term split must be 150"
  );
  assert(
    amountsEqual(accrualRowsByFamily.get("ACCRUED_REVENUE")?.longTermAmountBase, 0),
    "Accrued-revenue long-term split must be 0 after reclass"
  );
  assert(
    amountsEqual(accrualRowsByFamily.get("ACCRUED_EXPENSE")?.shortTermAmountBase, 90),
    "Accrued-expense short-term split must be 90"
  );
  assert(
    amountsEqual(accrualRowsByFamily.get("ACCRUED_EXPENSE")?.longTermAmountBase, 0),
    "Accrued-expense long-term split must be 0 after reclass"
  );

  const rollforward = await apiRequest({
    token,
    method: "GET",
    path:
      `/api/v1/revenue-recognition/reports/future-year-rollforward` +
      `?legalEntityId=${fixture.legalEntityId}&fiscalPeriodId=${fixture.period2Id}&asOfDate=2026-12-31`,
    expectedStatus: 200,
  });
  assert(
    String(rollforward.json?.reportCode || "") === "FUTURE_YEAR_ROLLFORWARD",
    "Rollforward reportCode mismatch"
  );
  assert(
    String(rollforward.json?.windowStartDate || "") === "2026-07-01",
    `Rollforward windowStartDate must equal selected period start, got ${String(
      rollforward.json?.windowStartDate
    )}`
  );
  assert(
    String(rollforward.json?.windowEndDate || "") === "2026-12-31",
    "Rollforward windowEndDate must equal selected period end/asOf"
  );
  assert(
    rollforward.json?.reconciled === true &&
      toNumber(rollforward.json?.reconciliation?.unmatchedGroups) === 0,
    "Rollforward reconciliation must be fully matched"
  );
  assert(
    Array.isArray(rollforward.json?.rows) && rollforward.json.rows.length >= 4,
    "Rollforward should return family rows for posted data"
  );
  assert(
    toNumber(rollforward.json?.summary?.movementAmountBase) > 0,
    "Rollforward summary movementAmountBase must be positive for selected period"
  );

  const explainSubledger = await explainOne(
    `SELECT rse.id
     FROM revenue_recognition_subledger_entries rse
     WHERE rse.tenant_id = ?
       AND rse.legal_entity_id = ?
       AND rse.fiscal_period_id = ?
       AND rse.account_family = 'PREPAID_EXPENSE'
       AND rse.maturity_bucket = 'LONG_TERM'
       AND rse.status = 'POSTED'`,
    [tenantId, fixture.legalEntityId, fixture.period1Id]
  );
  assert(
    hasExpectedKey(explainSubledger, ["ix_revrec_subledger_scope"]),
    "EXPLAIN subledger report query should use ix_revrec_subledger_scope"
  );

  const explainJournal = await explainOne(
    `SELECT je.id
     FROM journal_entries je
     WHERE je.tenant_id = ?
       AND je.legal_entity_id = ?
       AND je.fiscal_period_id = ?`,
    [tenantId, fixture.legalEntityId, fixture.period1Id]
  );
  assert(
    hasExpectedKey(explainJournal, ["ix_journal_tenant_entity_period"]),
    "EXPLAIN journal query should use ix_journal_tenant_entity_period"
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

    console.log("Revenue PR-17D reports + reconciliation test passed.");
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
    console.error("Revenue PR-17D test failed:", err);
    try {
      await closePool();
    } catch {
      // ignore close failures
    }
    process.exit(1);
  });
