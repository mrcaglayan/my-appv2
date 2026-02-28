import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.REVENUE_PR17A_TEST_PORT || 3121);
const BASE_URL =
  process.env.REVENUE_PR17A_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;

const REQUIRED_REVENUE_PERMISSIONS = [
  "revenue.schedule.read",
  "revenue.schedule.generate",
  "revenue.run.read",
  "revenue.run.create",
  "revenue.run.post",
  "revenue.run.reverse",
  "revenue.report.read",
];

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

async function assertSchemaExists() {
  const requiredTables = [
    "revenue_recognition_schedules",
    "revenue_recognition_schedule_lines",
    "revenue_recognition_runs",
    "revenue_recognition_run_lines",
    "revenue_recognition_subledger_entries",
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
  assert(missingTables.length === 0, `Missing PR-17A tables: ${missingTables.join(", ")}`);

  const requiredIndexes = [
    "uk_revrec_sched_source_uid",
    "uk_revrec_sched_line_source_uid",
    "uk_revrec_runs_source_uid",
    "uk_revrec_run_line_source_uid",
    "uk_revrec_subledger_source_uid",
    "uk_revrec_runs_tenant_entity_id",
    "uk_revrec_subledger_tenant_entity_id",
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
  assert(missingIndexes.length === 0, `Missing PR-17A indexes: ${missingIndexes.join(", ")}`);

  const requiredFks = [
    "fk_revrec_sched_entity_tenant",
    "fk_revrec_sched_creator_user",
    "fk_revrec_sched_lines_schedule_tenant",
    "fk_revrec_runs_schedule_tenant",
    "fk_revrec_runs_creator_user",
    "fk_revrec_run_lines_run_tenant",
    "fk_revrec_subledger_run_tenant",
    "fk_revrec_subledger_creator_user",
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
  assert(missingFks.length === 0, `Missing PR-17A FKs: ${missingFks.join(", ")}`);

  const columnRows = await query(
    `SELECT
        table_name AS table_name,
        column_name AS column_name,
        column_type AS column_type
     FROM information_schema.columns
     WHERE table_schema = DATABASE()
       AND (
         (table_name = 'users' AND column_name = 'id')
         OR (table_name = 'revenue_recognition_schedules' AND column_name = 'created_by_user_id')
         OR (table_name = 'revenue_recognition_runs' AND column_name = 'created_by_user_id')
         OR (table_name = 'revenue_recognition_subledger_entries' AND column_name = 'created_by_user_id')
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
    byKey.get("revenue_recognition_schedules.created_by_user_id") === usersIdType,
    `revenue_recognition_schedules.created_by_user_id type mismatch: expected ${usersIdType}`
  );
  assert(
    byKey.get("revenue_recognition_runs.created_by_user_id") === usersIdType,
    `revenue_recognition_runs.created_by_user_id type mismatch: expected ${usersIdType}`
  );
  assert(
    byKey.get("revenue_recognition_subledger_entries.created_by_user_id") === usersIdType,
    `revenue_recognition_subledger_entries.created_by_user_id type mismatch: expected ${usersIdType}`
  );
}

async function assertPermissionsSeeded() {
  const permissionRows = await query(
    `SELECT code
     FROM permissions
     WHERE code IN (${inClause(REQUIRED_REVENUE_PERMISSIONS)})`,
    REQUIRED_REVENUE_PERMISSIONS
  );
  const existing = new Set(permissionRows.rows.map((row) => String(row.code)));
  const missing = REQUIRED_REVENUE_PERMISSIONS.filter((code) => !existing.has(code));
  assert(missing.length === 0, `Missing revenue permissions: ${missing.join(", ")}`);
}

async function createTenantAndAdmin() {
  const stamp = Date.now();
  const tenantCode = `RR17A_T_${stamp}`;
  const tenantName = `Revenue PR17A Tenant ${stamp}`;
  const email = `revenue_pr17a_admin_${stamp}@example.com`;
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
    [tenantId, email, passwordHash, "Revenue Admin"]
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
    [tenantId, `RR17A_G_${stamp}`, `Revenue Group ${stamp}`]
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
      `RR17A_LE_${stamp}`,
      `Revenue Legal Entity ${stamp}`,
      countryId,
    ]
  );
  const legalEntityId = toNumber(legalEntityInsert.rows?.insertId);
  assert(legalEntityId > 0, "Failed to create legal entity");

  const calendarInsert = await query(
    `INSERT INTO fiscal_calendars (tenant_id, code, name, year_start_month, year_start_day)
     VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `RR17A_CAL_${stamp}`, `Revenue Calendar ${stamp}`]
  );
  const calendarId = toNumber(calendarInsert.rows?.insertId);
  assert(calendarId > 0, "Failed to create fiscal calendar");

  const fiscalPeriodInsert = await query(
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
  const fiscalPeriodId = toNumber(fiscalPeriodInsert.rows?.insertId);
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
      `RR17A_BOOK_${stamp}`,
      `Revenue Book ${stamp}`,
    ]
  );
  const bookId = toNumber(bookInsert.rows?.insertId);
  assert(bookId > 0, "Failed to create book");

  return { legalEntityId, fiscalPeriodId, bookId };
}

async function runApiAssertions({ token, tenantId, fixture, stamp }) {
  const beforeJournalCountResult = await query(
    `SELECT COUNT(*) AS total
     FROM journal_entries
     WHERE tenant_id = ?`,
    [tenantId]
  );
  const beforeJournalCount = toNumber(beforeJournalCountResult.rows?.[0]?.total);

  const generateScheduleResponse = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/schedules/generate",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.fiscalPeriodId,
      accountFamily: "DEFREV",
      maturityBucket: "SHORT_TERM",
      maturityDate: "2026-06-30",
      reclassRequired: false,
      currencyCode: "USD",
      fxRate: 1,
      amountTxn: 120,
      amountBase: 120,
      sourceEventUid: `RR17A-SCHED-${stamp}`,
    },
  });
  const scheduleId = toNumber(generateScheduleResponse.json?.row?.id);
  assert(scheduleId > 0, "Schedule generate did not return row.id");
  assert(
    generateScheduleResponse.json?.row?.status === "READY",
    "Generated schedule status must be READY"
  );
  assert(
    generateScheduleResponse.json?.row?.postedJournalEntryId === null,
    "PR-17A must not set postedJournalEntryId"
  );

  const listSchedulesResponse = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/revenue-recognition/schedules?legalEntityId=${fixture.legalEntityId}`,
    expectedStatus: 200,
  });
  const listedSchedule = (listSchedulesResponse.json?.rows || []).find(
    (row) => toNumber(row?.id) === scheduleId
  );
  assert(Boolean(listedSchedule), "Generated schedule missing from list endpoint");
  assert(toNumber(listedSchedule?.lineCount) >= 1, "Generated schedule must include lineCount");

  const createRunResponse = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/revenue-recognition/runs",
    expectedStatus: 201,
    body: {
      legalEntityId: fixture.legalEntityId,
      fiscalPeriodId: fixture.fiscalPeriodId,
      scheduleId,
      accountFamily: "DEFREV",
      maturityBucket: "SHORT_TERM",
      maturityDate: "2026-06-30",
      reclassRequired: false,
      currencyCode: "USD",
      fxRate: 1,
      totalAmountTxn: 120,
      totalAmountBase: 120,
      sourceRunUid: `RR17A-RUN-${stamp}`,
      runNo: `RR17A-RUNNO-${stamp}`,
    },
  });
  const runId = toNumber(createRunResponse.json?.row?.id);
  assert(runId > 0, "Run create did not return row.id");
  assert(createRunResponse.json?.row?.status === "DRAFT", "Run status must start at DRAFT");
  assert(toNumber(createRunResponse.json?.row?.lineCount) >= 1, "Run lineCount must be >= 1");

  const listRunsResponse = await apiRequest({
    token,
    method: "GET",
    path: `/api/v1/revenue-recognition/runs?legalEntityId=${fixture.legalEntityId}`,
    expectedStatus: 200,
  });
  const listedRun = (listRunsResponse.json?.rows || []).find(
    (row) => toNumber(row?.id) === runId
  );
  assert(Boolean(listedRun), "Created run missing from list endpoint");

  const postRunResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${runId}/post`,
    expectedStatus: 400,
  });
  assert(
    String(postRunResponse.json?.message || "").includes("not active") ||
      String(postRunResponse.json?.message || "").includes("Setup required"),
    "PR-17A post endpoint must stay side-effect-free when posting setup is unavailable"
  );

  const reverseRunResponse = await apiRequest({
    token,
    method: "POST",
    path: `/api/v1/revenue-recognition/runs/${runId}/reverse`,
    expectedStatus: 400,
  });
  assert(
    String(reverseRunResponse.json?.message || "").includes("not active") ||
      String(reverseRunResponse.json?.message || "").includes("allowed only from POSTED"),
    "PR-17A reverse endpoint must stay side-effect-free for non-posted runs"
  );

  const reportPaths = [
    "/api/v1/revenue-recognition/reports/future-year-rollforward",
    "/api/v1/revenue-recognition/reports/deferred-revenue-split",
    "/api/v1/revenue-recognition/reports/accrual-split",
    "/api/v1/revenue-recognition/reports/prepaid-expense-split",
  ];
  for (const reportPath of reportPaths) {
    const reportResponse = await apiRequest({
      token,
      method: "GET",
      path: `${reportPath}?legalEntityId=${fixture.legalEntityId}`,
      expectedStatus: 200,
    });
    assert(Array.isArray(reportResponse.json?.rows), `${reportPath} must return rows[]`);
    assert(reportResponse.json?.reconciled === true, `${reportPath} must stay reconciled`);
  }

  const runRowResult = await query(
    `SELECT status, posted_journal_entry_id
     FROM revenue_recognition_runs
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, runId]
  );
  const runRow = runRowResult.rows?.[0] || null;
  assert(Boolean(runRow), "Run row not found in DB");
  assert(String(runRow.status) === "DRAFT", "PR-17A post/reverse must not change run status");
  assert(runRow.posted_journal_entry_id === null, "PR-17A must not persist posted journal references");

  const subledgerCountResult = await query(
    `SELECT COUNT(*) AS total
     FROM revenue_recognition_subledger_entries
     WHERE tenant_id = ?
       AND run_id = ?`,
    [tenantId, runId]
  );
  const subledgerCount = toNumber(subledgerCountResult.rows?.[0]?.total);
  assert(subledgerCount === 0, "PR-17A must not create subledger postings during foundation flow");

  const afterJournalCountResult = await query(
    `SELECT COUNT(*) AS total
     FROM journal_entries
     WHERE tenant_id = ?`,
    [tenantId]
  );
  const afterJournalCount = toNumber(afterJournalCountResult.rows?.[0]?.total);
  assert(
    afterJournalCount === beforeJournalCount,
    "PR-17A must not create journal entries from revenue-recognition endpoints"
  );
}

async function main() {
  await runMigrations();
  await seedCore({ ensureDefaultTenantIfMissing: true });
  await assertSchemaExists();
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

    console.log("Revenue PR-17A foundation test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          legalEntityId: fixture.legalEntityId,
          fiscalPeriodId: fixture.fiscalPeriodId,
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
    console.error("Revenue PR-17A foundation test failed:", err);
    try {
      await closePool();
    } catch {
      // ignore close failures
    }
    process.exit(1);
  });
