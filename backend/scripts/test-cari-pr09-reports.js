import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CARI_PR09_TEST_PORT || 3128);
const BASE_URL = process.env.CARI_PR09_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "CariPR09#12345";

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

function amountsEqual(left, right, epsilon = 0.000001) {
  return Math.abs(toNumber(left) - toNumber(right)) <= epsilon;
}

async function apiRequest({ token, method = "GET", requestPath, body, expectedStatus }) {
  const headers = { "Content-Type": "application/json" };
  if (token) {
    headers.Cookie = token;
  }

  const response = await fetch(`${BASE_URL}${requestPath}`, {
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
      `${method} ${requestPath} expected ${expectedStatus}, got ${response.status}. response=${JSON.stringify(
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
      // wait for startup
    }
    await sleep(350);
  }
  throw new Error(`Server did not start within ${SERVER_START_TIMEOUT_MS}ms`);
}

async function login(email, password) {
  const response = await apiRequest({
    method: "POST",
    requestPath: "/auth/login",
    body: { email, password },
    expectedStatus: 200,
  });
  assert(Boolean(response.cookie), `Login cookie missing for ${email}`);
  return response.cookie;
}

async function createTenant(code, name) {
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE name = VALUES(name)`,
    [code, name]
  );
  const result = await query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [code]
  );
  const tenantId = toNumber(result.rows?.[0]?.id);
  assert(tenantId > 0, `Failed to resolve tenant id for ${code}`);
  return tenantId;
}

async function createUserWithScopedRole({
  tenantId,
  roleCode,
  email,
  passwordHash,
  name,
  scopeType = "TENANT",
  scopeId,
}) {
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, email, passwordHash, name]
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
  assert(userId > 0, `Failed to resolve user id for ${email}`);

  const roleResult = await query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, roleCode]
  );
  const roleId = toNumber(roleResult.rows?.[0]?.id);
  assert(roleId > 0, `Role not found: ${roleCode}`);

  const resolvedScopeId = scopeType === "TENANT" ? tenantId : toNumber(scopeId);
  assert(resolvedScopeId > 0, `scopeId is required for scopeType=${scopeType}`);

  await query(
    `INSERT INTO user_role_scopes (
        tenant_id,
        user_id,
        role_id,
        scope_type,
        scope_id,
        effect
     )
     VALUES (?, ?, ?, ?, ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE effect = VALUES(effect)`,
    [tenantId, userId, roleId, scopeType, resolvedScopeId]
  );

  return { userId, email };
}

async function createLegalEntityFixture({ tenantId, groupId, stamp, suffix }) {
  const countryResult = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(countryResult.rows?.[0]?.id);
  const functionalCurrencyCode = toUpper(countryResult.rows?.[0]?.default_currency_code || "USD");
  assert(countryId > 0, "US country row is required");

  await query(
    `INSERT INTO legal_entities (
        tenant_id,
        group_company_id,
        code,
        name,
        country_id,
        functional_currency_code,
        status
     )
     VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      groupId,
      `CARI09LE${suffix}_${stamp}`,
      `CARI09 Legal Entity ${suffix} ${stamp}`,
      countryId,
      functionalCurrencyCode,
    ]
  );
  const legalEntityResult = await query(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI09LE${suffix}_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityResult.rows?.[0]?.id);
  assert(legalEntityId > 0, "Legal entity create failed");

  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id,
        code,
        name,
        year_start_month,
        year_start_day
     )
     VALUES (?, ?, ?, 1, 1)`,
    [tenantId, `CARI09CAL_${suffix}_${stamp}`, `CARI09 Calendar ${suffix} ${stamp}`]
  );
  const calendarResult = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI09CAL_${suffix}_${stamp}`]
  );
  const calendarId = toNumber(calendarResult.rows?.[0]?.id);
  assert(calendarId > 0, "Fiscal calendar create failed");

  await query(
    `INSERT INTO fiscal_periods (
        calendar_id,
        fiscal_year,
        period_no,
        period_name,
        start_date,
        end_date,
        is_adjustment
     )
     VALUES (?, 2026, 1, 'FY2026', '2026-01-01', '2026-12-31', FALSE)
     ON DUPLICATE KEY UPDATE period_name = VALUES(period_name)`,
    [calendarId]
  );

  await query(
    `INSERT INTO books (
        tenant_id,
        legal_entity_id,
        calendar_id,
        code,
        name,
        book_type,
        base_currency_code
     )
     VALUES (?, ?, ?, ?, ?, 'LOCAL', ?)`,
    [
      tenantId,
      legalEntityId,
      calendarId,
      `CARI09BOOK_${suffix}_${stamp}`,
      `CARI09 Book ${suffix} ${stamp}`,
      functionalCurrencyCode,
    ]
  );

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id,
        legal_entity_id,
        scope,
        code,
        name
     )
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `CARI09COA_${suffix}_${stamp}`, `CARI09 COA ${suffix} ${stamp}`]
  );
  const coaResult = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI09COA_${suffix}_${stamp}`]
  );
  const coaId = toNumber(coaResult.rows?.[0]?.id);
  assert(coaId > 0, "COA create failed");

  const accountPrefix = `C9${String(stamp).slice(-4)}${suffix}`;
  await query(
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
     VALUES
       (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE),
       (?, ?, ?, 'REVENUE', 'CREDIT', TRUE, NULL, TRUE),
       (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, NULL, TRUE),
       (?, ?, ?, 'EXPENSE', 'DEBIT', TRUE, NULL, TRUE)`,
    [
      coaId,
      `${accountPrefix}01`,
      `CARI09 AR Control ${suffix}`,
      coaId,
      `${accountPrefix}02`,
      `CARI09 AR Offset ${suffix}`,
      coaId,
      `${accountPrefix}03`,
      `CARI09 AP Control ${suffix}`,
      coaId,
      `${accountPrefix}04`,
      `CARI09 AP Offset ${suffix}`,
    ]
  );
  const accountRows = await query(
    `SELECT id, code
     FROM accounts
     WHERE coa_id = ?
       AND code IN (?, ?, ?, ?)
     ORDER BY code`,
    [coaId, `${accountPrefix}01`, `${accountPrefix}02`, `${accountPrefix}03`, `${accountPrefix}04`]
  );
  const accountByCode = new Map(accountRows.rows.map((row) => [String(row.code), toNumber(row.id)]));

  await query(
    `INSERT INTO journal_purpose_accounts (
        tenant_id,
        legal_entity_id,
        purpose_code,
        account_id
     )
     VALUES
       (?, ?, 'CARI_AR_CONTROL', ?),
       (?, ?, 'CARI_AR_OFFSET', ?),
       (?, ?, 'CARI_AP_CONTROL', ?),
       (?, ?, 'CARI_AP_OFFSET', ?)
     ON DUPLICATE KEY UPDATE account_id = VALUES(account_id)`,
    [
      tenantId,
      legalEntityId,
      accountByCode.get(`${accountPrefix}01`),
      tenantId,
      legalEntityId,
      accountByCode.get(`${accountPrefix}02`),
      tenantId,
      legalEntityId,
      accountByCode.get(`${accountPrefix}03`),
      tenantId,
      legalEntityId,
      accountByCode.get(`${accountPrefix}04`),
    ]
  );

  await query(
    `INSERT INTO payment_terms (
        tenant_id,
        legal_entity_id,
        code,
        name,
        due_days,
        grace_days,
        status
     )
     VALUES (?, ?, ?, ?, 30, 0, 'ACTIVE')`,
    [tenantId, legalEntityId, `CARI09TERM_${suffix}_${stamp}`, `CARI09 Term ${suffix} ${stamp}`]
  );
  const termResult = await query(
    `SELECT id
     FROM payment_terms
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, `CARI09TERM_${suffix}_${stamp}`]
  );
  const paymentTermId = toNumber(termResult.rows?.[0]?.id);
  assert(paymentTermId > 0, "Payment term create failed");

  return {
    legalEntityId,
    paymentTermId,
    functionalCurrencyCode,
  };
}

async function createCounterparty({
  tenantId,
  legalEntityId,
  paymentTermId,
  code,
  name,
  type,
  currencyCode,
}) {
  const normalizedType = String(type || "")
    .trim()
    .toUpperCase();
  const isCustomer = normalizedType === "CUSTOMER" || normalizedType === "BOTH";
  const isVendor = normalizedType === "VENDOR" || normalizedType === "BOTH";
  assert(isCustomer || isVendor, `Unsupported counterparty type: ${type}`);

  await query(
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
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, legalEntityId, code, name, isCustomer, isVendor, currencyCode, paymentTermId]
  );
  const result = await query(
    `SELECT id
     FROM counterparties
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, code]
  );
  const counterpartyId = toNumber(result.rows?.[0]?.id);
  assert(counterpartyId > 0, "Counterparty create failed");
  return counterpartyId;
}

async function createAndPostDocument({
  tenantId,
  token,
  legalEntityId,
  counterpartyId,
  paymentTermId,
  direction,
  documentDate,
  dueDate,
  amountTxn,
  amountBase,
  currencyCode = "USD",
}) {
  const createResponse = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/cari/documents",
    body: {
      legalEntityId,
      counterpartyId,
      paymentTermId,
      direction,
      documentType: "INVOICE",
      documentDate,
      dueDate,
      amountTxn,
      amountBase,
      currencyCode,
      fxRate: 1,
    },
    expectedStatus: 201,
  });
  const documentId = toNumber(createResponse.json?.row?.id);
  assert(documentId > 0, "Draft document id missing");

  await apiRequest({
    token,
    method: "POST",
    requestPath: `/api/v1/cari/documents/${documentId}/post`,
    body: {},
    expectedStatus: 200,
  });

  const openItemResult = await query(
    `SELECT id
     FROM cari_open_items
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND document_id = ?
     ORDER BY id ASC
     LIMIT 1`,
    [tenantId, legalEntityId, documentId]
  );
  const openItemId = toNumber(openItemResult.rows?.[0]?.id);
  assert(openItemId > 0, "Open item missing after post");
  return { documentId, openItemId };
}

async function applySettlement({
  token,
  legalEntityId,
  counterpartyId,
  settlementDate,
  direction,
  incomingAmountTxn,
  allocations,
  idempotencyKey,
  useUnappliedCash = true,
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/cari/settlements/apply",
    body: {
      legalEntityId,
      counterpartyId,
      settlementDate,
      direction,
      currencyCode: "USD",
      incomingAmountTxn,
      idempotencyKey,
      autoAllocate: false,
      useUnappliedCash,
      allocations,
    },
    expectedStatus: 201,
  });
  return response.json;
}

async function reverseSettlement({ token, settlementBatchId, reason, reversalDate }) {
  const response = await apiRequest({
    token,
    method: "POST",
    requestPath: `/api/v1/cari/settlements/${settlementBatchId}/reverse`,
    body: { reason, reversalDate },
    expectedStatus: 201,
  });
  return response.json;
}

async function reverseDocument({ token, documentId, reason, reversalDate }) {
  const response = await apiRequest({
    token,
    method: "POST",
    requestPath: `/api/v1/cari/documents/${documentId}/reverse`,
    body: { reason, reversalDate },
    expectedStatus: 201,
  });
  return response.json;
}

async function explainOne(sql, params = []) {
  const result = await query(`EXPLAIN ${sql}`, params);
  return result.rows || [];
}

function hasExpectedKey(explainRows, expectedPrefixes) {
  return explainRows.some((row) => {
    const selectedKey = String(row?.key || "");
    const possibleKeys = String(row?.possible_keys || "");
    return expectedPrefixes.some(
      (prefix) => selectedKey.includes(prefix) || possibleKeys.includes(prefix)
    );
  });
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantId = await createTenant(`CARI09_${stamp}`, `CARI PR09 ${stamp}`);
  await seedCore({ ensureDefaultTenantIfMissing: true });

  await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `CARI09GC${stamp}`, `CARI09 Group ${stamp}`]
  );
  const groupResult = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI09GC${stamp}`]
  );
  const groupId = toNumber(groupResult.rows?.[0]?.id);
  assert(groupId > 0, "Group company create failed");

  const le1 = await createLegalEntityFixture({
    tenantId,
    groupId,
    stamp,
    suffix: "A",
  });
  const le2 = await createLegalEntityFixture({
    tenantId,
    groupId,
    stamp,
    suffix: "B",
  });

  const cpAr = await createCounterparty({
    tenantId,
    legalEntityId: le1.legalEntityId,
    paymentTermId: le1.paymentTermId,
    code: `CARI09CP_AR_${stamp}`,
    name: `CARI09 Customer ${stamp}`,
    type: "CUSTOMER",
    currencyCode: le1.functionalCurrencyCode,
  });
  const cpAp = await createCounterparty({
    tenantId,
    legalEntityId: le1.legalEntityId,
    paymentTermId: le1.paymentTermId,
    code: `CARI09CP_AP_${stamp}`,
    name: `CARI09 Vendor ${stamp}`,
    type: "VENDOR",
    currencyCode: le1.functionalCurrencyCode,
  });
  const cpLe2 = await createCounterparty({
    tenantId,
    legalEntityId: le2.legalEntityId,
    paymentTermId: le2.paymentTermId,
    code: `CARI09CP_LE2_${stamp}`,
    name: `CARI09 LE2 Customer ${stamp}`,
    type: "CUSTOMER",
    currencyCode: le2.functionalCurrencyCode,
  });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const adminUser = await createUserWithScopedRole({
    tenantId,
    roleCode: "EntityAccountant",
    email: `cari09_admin_${stamp}@example.com`,
    passwordHash,
    name: "CARI09 Admin",
    scopeType: "TENANT",
  });
  const scopedUser = await createUserWithScopedRole({
    tenantId,
    roleCode: "EntityAccountant",
    email: `cari09_scope_${stamp}@example.com`,
    passwordHash,
    name: "CARI09 Scoped",
    scopeType: "LEGAL_ENTITY",
    scopeId: le1.legalEntityId,
  });

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const adminToken = await login(adminUser.email, TEST_PASSWORD);
    const scopedToken = await login(scopedUser.email, TEST_PASSWORD);

    const ar1 = await createAndPostDocument({
      tenantId,
      token: adminToken,
      legalEntityId: le1.legalEntityId,
      counterpartyId: cpAr,
      paymentTermId: le1.paymentTermId,
      direction: "AR",
      documentDate: "2026-01-10",
      dueDate: "2026-02-10",
      amountTxn: 100,
      amountBase: 100,
    });
    const ar2 = await createAndPostDocument({
      tenantId,
      token: adminToken,
      legalEntityId: le1.legalEntityId,
      counterpartyId: cpAr,
      paymentTermId: le1.paymentTermId,
      direction: "AR",
      documentDate: "2026-02-01",
      dueDate: "2026-03-01",
      amountTxn: 200,
      amountBase: 200,
    });
    const ar3 = await createAndPostDocument({
      tenantId,
      token: adminToken,
      legalEntityId: le1.legalEntityId,
      counterpartyId: cpAr,
      paymentTermId: le1.paymentTermId,
      direction: "AR",
      documentDate: "2026-03-10",
      dueDate: "2026-04-15",
      amountTxn: 150,
      amountBase: 150,
    });
    const ar4 = await createAndPostDocument({
      tenantId,
      token: adminToken,
      legalEntityId: le1.legalEntityId,
      counterpartyId: cpAr,
      paymentTermId: le1.paymentTermId,
      direction: "AR",
      documentDate: "2026-04-01",
      dueDate: "2026-05-01",
      amountTxn: 80,
      amountBase: 80,
    });
    const ap1 = await createAndPostDocument({
      tenantId,
      token: adminToken,
      legalEntityId: le1.legalEntityId,
      counterpartyId: cpAp,
      paymentTermId: le1.paymentTermId,
      direction: "AP",
      documentDate: "2026-01-15",
      dueDate: "2026-02-20",
      amountTxn: 120,
      amountBase: 120,
    });
    const ap2 = await createAndPostDocument({
      tenantId,
      token: adminToken,
      legalEntityId: le1.legalEntityId,
      counterpartyId: cpAp,
      paymentTermId: le1.paymentTermId,
      direction: "AP",
      documentDate: "2026-02-15",
      dueDate: "2026-03-20",
      amountTxn: 90,
      amountBase: 90,
    });
    const le2Doc = await createAndPostDocument({
      tenantId,
      token: adminToken,
      legalEntityId: le2.legalEntityId,
      counterpartyId: cpLe2,
      paymentTermId: le2.paymentTermId,
      direction: "AR",
      documentDate: "2026-01-20",
      dueDate: "2026-02-28",
      amountTxn: 500,
      amountBase: 500,
    });

    await applySettlement({
      token: adminToken,
      legalEntityId: le1.legalEntityId,
      counterpartyId: cpAr,
      direction: "AR",
      settlementDate: "2026-02-15",
      incomingAmountTxn: 40,
      allocations: [{ openItemId: ar1.openItemId, amountTxn: 40 }],
      idempotencyKey: `CARI09-AR-S1-${stamp}`,
    });
    const arS2 = await applySettlement({
      token: adminToken,
      legalEntityId: le1.legalEntityId,
      counterpartyId: cpAr,
      direction: "AR",
      settlementDate: "2026-03-10",
      incomingAmountTxn: 60,
      allocations: [{ openItemId: ar1.openItemId, amountTxn: 60 }],
      idempotencyKey: `CARI09-AR-S2-${stamp}`,
    });
    await applySettlement({
      token: adminToken,
      legalEntityId: le1.legalEntityId,
      counterpartyId: cpAr,
      direction: "AR",
      settlementDate: "2026-03-20",
      incomingAmountTxn: 300,
      allocations: [{ openItemId: ar2.openItemId, amountTxn: 200 }],
      idempotencyKey: `CARI09-AR-S3-${stamp}`,
    });
    await applySettlement({
      token: adminToken,
      legalEntityId: le1.legalEntityId,
      counterpartyId: cpAr,
      direction: "AR",
      settlementDate: "2026-04-10",
      incomingAmountTxn: 0,
      allocations: [{ openItemId: ar3.openItemId, amountTxn: 30 }],
      idempotencyKey: `CARI09-AR-S4-${stamp}`,
      useUnappliedCash: true,
    });
    await applySettlement({
      token: adminToken,
      legalEntityId: le1.legalEntityId,
      counterpartyId: cpAp,
      direction: "AP",
      settlementDate: "2026-02-25",
      incomingAmountTxn: 120,
      allocations: [{ openItemId: ap1.openItemId, amountTxn: 120 }],
      idempotencyKey: `CARI09-AP-S1-${stamp}`,
    });
    await applySettlement({
      token: adminToken,
      legalEntityId: le1.legalEntityId,
      counterpartyId: cpAp,
      direction: "AP",
      settlementDate: "2026-03-25",
      incomingAmountTxn: 30,
      allocations: [{ openItemId: ap2.openItemId, amountTxn: 30 }],
      idempotencyKey: `CARI09-AP-S2-${stamp}`,
    });
    await applySettlement({
      token: adminToken,
      legalEntityId: le2.legalEntityId,
      counterpartyId: cpLe2,
      direction: "AR",
      settlementDate: "2026-03-01",
      incomingAmountTxn: 100,
      allocations: [{ openItemId: le2Doc.openItemId, amountTxn: 100 }],
      idempotencyKey: `CARI09-LE2-S1-${stamp}`,
    });

    await reverseSettlement({
      token: adminToken,
      settlementBatchId: toNumber(arS2?.row?.id),
      reason: "PR09 reversal",
      reversalDate: "2026-04-05",
    });
    await reverseDocument({
      token: adminToken,
      documentId: ar4.documentId,
      reason: "PR09 document reversal",
      reversalDate: "2026-04-20",
    });

    const arAgingBefore = await apiRequest({
      token: adminToken,
      method: "GET",
      requestPath: `/api/v1/cari/reports/ar-aging?legalEntityId=${le1.legalEntityId}&counterpartyId=${cpAr}&asOfDate=2026-03-31&status=OPEN`,
      expectedStatus: 200,
    });
    assert(
      amountsEqual(arAgingBefore.json?.summary?.residualAmountTxnTotal, 150),
      "AR aging as-of 2026-03-31 should show residual 150"
    );

    const arAgingAfter = await apiRequest({
      token: adminToken,
      method: "GET",
      requestPath: `/api/v1/cari/reports/ar-aging?legalEntityId=${le1.legalEntityId}&counterpartyId=${cpAr}&asOfDate=2026-04-30&status=OPEN`,
      expectedStatus: 200,
    });
    assert(
      amountsEqual(arAgingAfter.json?.summary?.residualAmountTxnTotal, 180),
      "AR aging as-of 2026-04-30 should show residual 180 after reversal + consume"
    );
    const arBucketByCode = new Map(
      (arAgingAfter.json?.buckets || []).map((row) => [String(row.bucketCode), row])
    );
    assert(
      amountsEqual(arBucketByCode.get("DUE_61_90")?.residualAmountTxnTotal, 60),
      "AR bucket DUE_61_90 should include AR1 residual 60"
    );
    assert(
      amountsEqual(arBucketByCode.get("DUE_1_30")?.residualAmountTxnTotal, 120),
      "AR bucket DUE_1_30 should include AR3 residual 120"
    );

    const apAging = await apiRequest({
      token: adminToken,
      method: "GET",
      requestPath: `/api/v1/cari/reports/ap-aging?legalEntityId=${le1.legalEntityId}&counterpartyId=${cpAp}&asOfDate=2026-04-30&status=OPEN`,
      expectedStatus: 200,
    });
    assert(
      amountsEqual(apAging.json?.summary?.residualAmountTxnTotal, 60),
      "AP aging should show AP2 residual 60"
    );

    const openItemsAll = await apiRequest({
      token: adminToken,
      method: "GET",
      requestPath: `/api/v1/cari/reports/open-items?legalEntityId=${le1.legalEntityId}&counterpartyId=${cpAr}&asOfDate=2026-03-31&status=ALL&includeDetails=true`,
      expectedStatus: 200,
    });
    assert(
      amountsEqual(openItemsAll.json?.summary?.residualAmountTxnTotal, 150),
      "Open-items ALL as-of 2026-03-31 residual total should be 150"
    );
    assert(
      openItemsAll.json?.rows?.some((row) => String(row.asOfStatus) === "SETTLED"),
      "Open-items ALL should include settled rows"
    );

    const openItemsDefault = await apiRequest({
      token: adminToken,
      method: "GET",
      requestPath: `/api/v1/cari/reports/open-items?legalEntityId=${le1.legalEntityId}&counterpartyId=${cpAr}&asOfDate=2026-03-31`,
      expectedStatus: 200,
    });
    assert(
      toNumber(openItemsDefault.json?.rows?.length) === 1,
      "Open-items default OPEN filter should include only one AR row as-of 2026-03-31"
    );
    assert(
      amountsEqual(openItemsDefault.json?.unapplied?.summary?.residualAmountTxnTotal, 100),
      "Unapplied residual at 2026-03-31 should be 100"
    );

    const openItemsAfter = await apiRequest({
      token: adminToken,
      method: "GET",
      requestPath: `/api/v1/cari/reports/open-items?legalEntityId=${le1.legalEntityId}&counterpartyId=${cpAr}&asOfDate=2026-04-30`,
      expectedStatus: 200,
    });
    assert(
      amountsEqual(openItemsAfter.json?.summary?.residualAmountTxnTotal, 180),
      "Open-items residual at 2026-04-30 should be 180"
    );
    assert(
      amountsEqual(openItemsAfter.json?.unapplied?.summary?.residualAmountTxnTotal, 70),
      "Unapplied residual at 2026-04-30 should be 70"
    );

    const statement = await apiRequest({
      token: adminToken,
      method: "GET",
      requestPath: `/api/v1/cari/reports/statement?legalEntityId=${le1.legalEntityId}&counterpartyId=${cpAr}&asOfDate=2026-04-30&includeDetails=true`,
      expectedStatus: 200,
    });
    assert(
      statement.json?.settlements?.rows?.some(
        (row) =>
          toNumber(row.settlementBatchId) === toNumber(arS2?.row?.id) &&
          toNumber(row.reversedBySettlementBatchId) > 0
      ),
      "Statement should include reversed settlement linkage"
    );
    assert(
      statement.json?.allocations?.rows?.some(
        (row) =>
          toNumber(row.settlementBatchId) === toNumber(arS2?.row?.id) &&
          row.activeAsOf === false
      ),
      "Statement should mark reversed allocation as inactive as-of"
    );
    assert(
      statement.json?.documents?.rows?.some(
        (row) => toNumber(row.documentId) === ar4.documentId && String(row.asOfStatus) === "REVERSED"
      ),
      "Statement should include reversed document status"
    );

    const scopedOpenItems = await apiRequest({
      token: scopedToken,
      method: "GET",
      requestPath: `/api/v1/cari/reports/open-items?asOfDate=2026-04-30&status=ALL&includeDetails=true`,
      expectedStatus: 200,
    });
    assert(
      (scopedOpenItems.json?.rows || []).every(
        (row) => toNumber(row.legalEntityId) === le1.legalEntityId
      ),
      "Scoped user should only see allowed legal entity rows"
    );

    const scopedDenied = await apiRequest({
      token: scopedToken,
      method: "GET",
      requestPath: `/api/v1/cari/reports/open-items?legalEntityId=${le2.legalEntityId}&asOfDate=2026-04-30`,
    });
    assert(scopedDenied.status === 403, "Scoped user must be denied for out-of-scope legalEntityId");

    const explainOpenItemsDocs = await explainOne(
      `SELECT d.id
       FROM cari_documents d
       WHERE d.tenant_id = ?
         AND d.legal_entity_id = ?
         AND d.direction = 'AR'
         AND d.document_date <= ?`,
      [tenantId, le1.legalEntityId, "2026-04-30"]
    );
    assert(
      hasExpectedKey(explainOpenItemsDocs, [
        "ix_cari_docs_report_scope_date_direction",
        "ix_cari_docs_tenant_document_date",
      ]),
      "EXPLAIN document as-of query should use report/document date index"
    );

    const explainOpenItems = await explainOne(
      `SELECT oi.id
       FROM cari_open_items oi
       WHERE oi.tenant_id = ?
         AND oi.legal_entity_id = ?
         AND oi.due_date <= ?`,
      [tenantId, le1.legalEntityId, "2026-04-30"]
    );
    assert(
      hasExpectedKey(explainOpenItems, [
        "ix_cari_oi_report_scope_due_counterparty",
        "ix_cari_oi_tenant_due_date",
      ]),
      "EXPLAIN open-items query should use open-item tenant/entity index"
    );

    const explainSettlementBatches = await explainOne(
      `SELECT b.id
       FROM cari_settlement_batches b
       WHERE b.tenant_id = ?
         AND b.legal_entity_id = ?
         AND b.settlement_date <= ?`,
      [tenantId, le1.legalEntityId, "2026-04-30"]
    );
    assert(
      hasExpectedKey(explainSettlementBatches, [
        "ix_cari_settle_report_scope_date_counterparty",
        "ix_cari_settle_batches_tenant_date",
      ]),
      "EXPLAIN settlement batch query should use settlement date/scope index"
    );

    const explainAlloc = await explainOne(
      `SELECT a.open_item_id
       FROM cari_settlement_allocations a
       JOIN cari_settlement_batches b
         ON b.tenant_id = a.tenant_id
        AND b.legal_entity_id = a.legal_entity_id
        AND b.id = a.settlement_batch_id
       WHERE a.tenant_id = ?
         AND a.legal_entity_id = ?
         AND b.settlement_date <= ?`,
      [tenantId, le1.legalEntityId, "2026-04-30"]
    );
    assert(
      hasExpectedKey(explainAlloc, [
        "ix_cari_alloc_report_open_item_batch",
        "ix_cari_alloc_tenant_batch",
      ]),
      "EXPLAIN allocation query should use settlement allocation index"
    );

    const explainAudit = await explainOne(
      `SELECT payload_json
       FROM audit_logs
       WHERE tenant_id = ?
         AND action = 'cari.settlement.apply'
         AND resource_type = 'cari_settlement_batch'`,
      [tenantId]
    );
    assert(
      hasExpectedKey(explainAudit, ["ix_audit_tenant_action_resource", "ix_audit_tenant_time"]),
      "EXPLAIN audit query should use audit log index"
    );

    console.log("CARI PR-09 reports test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId,
          legalEntityA: le1.legalEntityId,
          legalEntityB: le2.legalEntityId,
          counterpartyAr: cpAr,
          counterpartyAp: cpAp,
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
    console.error(err);
    try {
      await closePool();
    } catch {
      // ignore close failures
    }
    process.exit(1);
  });
