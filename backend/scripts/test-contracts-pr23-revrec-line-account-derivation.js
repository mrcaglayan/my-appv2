import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CONTRACTS_PR23_REVREC_TEST_PORT || 3124);
const BASE_URL =
  process.env.CONTRACTS_PR23_REVREC_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
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
  const tenantCode = `CTR_PR23_T_${stamp}`;
  const tenantName = `Contracts PR23 Tenant ${stamp}`;
  const email = `contracts_pr23_admin_${stamp}@example.com`;
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
    [tenantId, email, passwordHash, "Contracts PR23 Admin"]
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

async function createFixture({ tenantId, stamp, userId }) {
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
    [tenantId, `CTR_PR23_G_${stamp}`, `Contracts PR23 Group ${stamp}`]
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
      `CTR_PR23_LE_${stamp}`,
      `Contracts PR23 LE ${stamp}`,
      countryId,
    ]
  );
  const legalEntityId = toNumber(legalEntityInsert.rows?.insertId);
  assert(legalEntityId > 0, "Failed to create legal entity");

  const calendarInsert = await query(
    `INSERT INTO fiscal_calendars (tenant_id, code, name, year_start_month, year_start_day)
     VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `CTR_PR23_CAL_${stamp}`, `Contracts PR23 Calendar ${stamp}`]
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
      `CTR_PR23_BOOK_${stamp}`,
      `Contracts PR23 Book ${stamp}`,
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
    [bookId, fiscalPeriodId, "PR23 open period"]
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
    [tenantId, legalEntityId, `CTR_PR23_TERM_${stamp}`, `Contracts PR23 Payment Term ${stamp}`]
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
      `CTR_PR23_CP_${stamp}`,
      `Contracts PR23 Customer ${stamp}`,
      paymentTermId,
    ]
  );
  const counterpartyId = toNumber(counterpartyInsert.rows?.insertId);
  assert(counterpartyId > 0, "Failed to create counterparty");

  const coaInsert = await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `CTR_PR23_COA_${stamp}`, `Contracts PR23 CoA ${stamp}`]
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

  const accounts = {
    customDeferred: await createAccount({
      code: `CTR23_DEF_CUSTOM_${stamp}`,
      name: `PR23 custom deferred ${stamp}`,
      accountType: "LIABILITY",
      normalSide: "CREDIT",
    }),
    customRevenue: await createAccount({
      code: `CTR23_REV_CUSTOM_${stamp}`,
      name: `PR23 custom revenue ${stamp}`,
      accountType: "REVENUE",
      normalSide: "CREDIT",
    }),
    fallbackShortLiability: await createAccount({
      code: `CTR23_DEF_SHORT_${stamp}`,
      name: `PR23 fallback short deferred ${stamp}`,
      accountType: "LIABILITY",
      normalSide: "CREDIT",
    }),
    fallbackRevenue: await createAccount({
      code: `CTR23_REV_FALLBACK_${stamp}`,
      name: `PR23 fallback revenue ${stamp}`,
      accountType: "REVENUE",
      normalSide: "CREDIT",
    }),
  };

  return {
    tenantId,
    userId,
    legalEntityId,
    fiscalPeriodId,
    counterpartyId,
    accounts,
  };
}

async function createContractWithThreeLines({ token, fixture, stamp }) {
  const createPayload = {
    legalEntityId: fixture.legalEntityId,
    counterpartyId: fixture.counterpartyId,
    contractNo: `CTR_PR23_NO_${stamp}`,
    contractType: "CUSTOMER",
    currencyCode: "USD",
    startDate: "2026-01-01",
    endDate: "2026-12-31",
    notes: "PR23 contract line account derivation",
    lines: [
      {
        description: "Custom account line",
        lineAmountTxn: 100,
        lineAmountBase: 100,
        recognitionMethod: "MILESTONE",
        recognitionStartDate: "2026-03-15",
        recognitionEndDate: "2026-03-15",
        deferredAccountId: fixture.accounts.customDeferred,
        revenueAccountId: fixture.accounts.customRevenue,
        status: "ACTIVE",
      },
      {
        description: "Fallback account line",
        lineAmountTxn: 200,
        lineAmountBase: 200,
        recognitionMethod: "MILESTONE",
        recognitionStartDate: "2026-04-15",
        recognitionEndDate: "2026-04-15",
        status: "ACTIVE",
      },
      {
        description: "Missing mapping line",
        lineAmountTxn: 300,
        lineAmountBase: 300,
        recognitionMethod: "MILESTONE",
        recognitionStartDate: "2026-05-15",
        recognitionEndDate: "2026-05-15",
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
  const lines = Array.isArray(detailResponse.json?.row?.lines)
    ? detailResponse.json.row.lines
    : [];
  assert(lines.length === 3, "Contract should contain 3 lines");

  const lineIdByDescription = new Map();
  for (const row of lines) {
    lineIdByDescription.set(String(row?.description || ""), toNumber(row?.id));
  }

  const customLineId = toNumber(lineIdByDescription.get("Custom account line"));
  const fallbackLineId = toNumber(lineIdByDescription.get("Fallback account line"));
  const missingLineId = toNumber(lineIdByDescription.get("Missing mapping line"));
  assert(customLineId > 0, "Custom line id missing");
  assert(fallbackLineId > 0, "Fallback line id missing");
  assert(missingLineId > 0, "Missing line id missing");

  return { contractId, customLineId, fallbackLineId, missingLineId };
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
    toNumber(response.json?.generatedLineCount) === 3,
    "Expected one generated RevRec line per selected milestone contract line"
  );
}

async function loadScheduleIdByContractLineId({
  tenantId,
  legalEntityId,
  contractId,
}) {
  const result = await query(
    `SELECT
        rrsl.source_contract_line_id,
        rrsl.schedule_id,
        COUNT(*) AS line_count
     FROM revenue_recognition_schedule_lines rrsl
     WHERE rrsl.tenant_id = ?
       AND rrsl.legal_entity_id = ?
       AND rrsl.source_contract_id = ?
     GROUP BY rrsl.source_contract_line_id, rrsl.schedule_id`,
    [tenantId, legalEntityId, contractId]
  );
  const map = new Map();
  for (const row of result.rows || []) {
    const contractLineId = toNumber(row.source_contract_line_id);
    const scheduleId = toNumber(row.schedule_id);
    const lineCount = toNumber(row.line_count);
    if (contractLineId > 0 && scheduleId > 0 && lineCount > 0) {
      map.set(contractLineId, scheduleId);
    }
  }
  return map;
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
       ON DUPLICATE KEY UPDATE account_id = VALUES(account_id)`,
      [tenantId, legalEntityId, mapping.purposeCode, mapping.accountId]
    );
  }
}

async function deletePurposeMappings({
  tenantId,
  legalEntityId,
  purposeCodes,
}) {
  if (!Array.isArray(purposeCodes) || purposeCodes.length === 0) {
    return;
  }
  const placeholders = purposeCodes.map(() => "?").join(", ");
  await query(
    `DELETE FROM journal_purpose_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND purpose_code IN (${placeholders})`,
    [tenantId, legalEntityId, ...purposeCodes]
  );
}

async function fetchJournalAccountIds(journalEntryId) {
  const result = await query(
    `SELECT account_id, debit_base, credit_base
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no ASC`,
    [journalEntryId]
  );
  const accountIds = new Set();
  let totalDebit = 0;
  let totalCredit = 0;
  for (const row of result.rows || []) {
    accountIds.add(toNumber(row.account_id));
    totalDebit += toNumber(row.debit_base);
    totalCredit += toNumber(row.credit_base);
  }
  return {
    accountIds,
    lineCount: (result.rows || []).length,
    totalDebit,
    totalCredit,
  };
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
      userId: identity.userId,
    });
    const token = await login(identity.email, TEST_PASSWORD);

    const contract = await createContractWithThreeLines({
      token,
      fixture,
      stamp: identity.stamp,
    });
    await generateContractRevrec({
      token,
      contractId: contract.contractId,
      fiscalPeriodId: fixture.fiscalPeriodId,
      lineIds: [contract.customLineId, contract.fallbackLineId, contract.missingLineId],
    });

    const scheduleByLineId = await loadScheduleIdByContractLineId({
      tenantId: identity.tenantId,
      legalEntityId: fixture.legalEntityId,
      contractId: contract.contractId,
    });
    const customScheduleId = toNumber(scheduleByLineId.get(contract.customLineId));
    const fallbackScheduleId = toNumber(scheduleByLineId.get(contract.fallbackLineId));
    const missingScheduleId = toNumber(scheduleByLineId.get(contract.missingLineId));
    assert(customScheduleId > 0, "Custom line schedule id missing");
    assert(fallbackScheduleId > 0, "Fallback line schedule id missing");
    assert(missingScheduleId > 0, "Missing line schedule id missing");

    await deletePurposeMappings({
      tenantId: identity.tenantId,
      legalEntityId: fixture.legalEntityId,
      purposeCodes: ["DEFREV_SHORT_LIABILITY", "DEFREV_REVENUE", "DEFREV_LONG_LIABILITY"],
    });

    const customRunId = await createRunFromSchedule({
      token,
      tenantId: identity.tenantId,
      legalEntityId: fixture.legalEntityId,
      scheduleId: customScheduleId,
      fiscalPeriodId: fixture.fiscalPeriodId,
      sourceRunUid: `CTR23-RUN-CUSTOM-${identity.stamp}`,
      runNo: `CTR23-CUSTOM-${identity.stamp}`,
    });
    const customPost = await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/revenue-recognition/runs/${customRunId}/post`,
      expectedStatus: 200,
    });
    assert(
      toUpper(customPost.json?.row?.status) === "POSTED",
      "Custom line run should post successfully without purpose mappings"
    );
    const customJournalId = toNumber(customPost.json?.journal?.journalEntryId);
    assert(customJournalId > 0, "Custom line run should return journalEntryId");
    const customJournal = await fetchJournalAccountIds(customJournalId);
    assert(customJournal.lineCount === 2, "Custom line should create 2 journal lines");
    assert(
      customJournal.accountIds.has(fixture.accounts.customDeferred),
      "Custom line should use contract-line deferred_account_id"
    );
    assert(
      customJournal.accountIds.has(fixture.accounts.customRevenue),
      "Custom line should use contract-line revenue_account_id"
    );
    assert(
      !customJournal.accountIds.has(fixture.accounts.fallbackShortLiability),
      "Custom line should not fall back to DEFREV_SHORT_LIABILITY mapping account"
    );
    assert(
      !customJournal.accountIds.has(fixture.accounts.fallbackRevenue),
      "Custom line should not fall back to DEFREV_REVENUE mapping account"
    );
    assert(
      amountsEqual(customJournal.totalDebit, customJournal.totalCredit),
      "Custom line journal should stay balanced"
    );

    await upsertPurposeMappings({
      tenantId: identity.tenantId,
      legalEntityId: fixture.legalEntityId,
      mappings: [
        {
          purposeCode: "DEFREV_SHORT_LIABILITY",
          accountId: fixture.accounts.fallbackShortLiability,
        },
        {
          purposeCode: "DEFREV_REVENUE",
          accountId: fixture.accounts.fallbackRevenue,
        },
      ],
    });

    const fallbackRunId = await createRunFromSchedule({
      token,
      tenantId: identity.tenantId,
      legalEntityId: fixture.legalEntityId,
      scheduleId: fallbackScheduleId,
      fiscalPeriodId: fixture.fiscalPeriodId,
      sourceRunUid: `CTR23-RUN-FALLBACK-${identity.stamp}`,
      runNo: `CTR23-FALLBACK-${identity.stamp}`,
    });
    const fallbackPost = await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/revenue-recognition/runs/${fallbackRunId}/post`,
      expectedStatus: 200,
    });
    assert(
      toUpper(fallbackPost.json?.row?.status) === "POSTED",
      "Fallback line run should post successfully"
    );
    const fallbackJournalId = toNumber(fallbackPost.json?.journal?.journalEntryId);
    assert(fallbackJournalId > 0, "Fallback line run should return journalEntryId");
    const fallbackJournal = await fetchJournalAccountIds(fallbackJournalId);
    assert(fallbackJournal.lineCount === 2, "Fallback line should create 2 journal lines");
    assert(
      fallbackJournal.accountIds.has(fixture.accounts.fallbackShortLiability),
      "Fallback line should use DEFREV_SHORT_LIABILITY purpose mapping"
    );
    assert(
      fallbackJournal.accountIds.has(fixture.accounts.fallbackRevenue),
      "Fallback line should use DEFREV_REVENUE purpose mapping"
    );
    assert(
      amountsEqual(fallbackJournal.totalDebit, fallbackJournal.totalCredit),
      "Fallback line journal should stay balanced"
    );

    await deletePurposeMappings({
      tenantId: identity.tenantId,
      legalEntityId: fixture.legalEntityId,
      purposeCodes: ["DEFREV_SHORT_LIABILITY", "DEFREV_REVENUE"],
    });

    const missingRunId = await createRunFromSchedule({
      token,
      tenantId: identity.tenantId,
      legalEntityId: fixture.legalEntityId,
      scheduleId: missingScheduleId,
      fiscalPeriodId: fixture.fiscalPeriodId,
      sourceRunUid: `CTR23-RUN-MISSING-${identity.stamp}`,
      runNo: `CTR23-MISSING-${identity.stamp}`,
    });
    const missingPost = await apiRequest({
      token,
      method: "POST",
      path: `/api/v1/revenue-recognition/runs/${missingRunId}/post`,
      expectedStatus: 400,
    });
    const missingMessage = String(missingPost.json?.message || "");
    assert(
      missingMessage.includes("Setup required"),
      "Missing both line-level and fallback mapping should return setup validation error"
    );
    assert(
      missingMessage.includes("sourceContractLineId"),
      "Missing mapping error should include source contract line context"
    );

    console.log("Contracts PR-23 RevRec line-level account derivation test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          legalEntityId: fixture.legalEntityId,
          contractId: contract.contractId,
          customScheduleId,
          fallbackScheduleId,
          missingScheduleId,
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
  console.error("Contracts PR-23 RevRec line-level account derivation test failed:", error);
  process.exitCode = 1;
});
