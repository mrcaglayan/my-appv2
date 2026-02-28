
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CARI_PR19_TEST_PORT || 3129);
const BASE_URL =
  process.env.CARI_PR19_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "CariPR19#12345";

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

async function apiRequest({
  token,
  method = "GET",
  requestPath,
  body,
  expectedStatus,
}) {
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
  const cookie = setCookieHeader
    ? String(setCookieHeader).split(";")[0].trim()
    : null;

  if (expectedStatus !== undefined && response.status !== expectedStatus) {
    throw new Error(
      `${method} ${requestPath} expected ${expectedStatus}, got ${response.status}. response=${JSON.stringify(
        json
      )}`
    );
  }

  return {
    status: response.status,
    json,
    cookie,
  };
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

async function createUserWithRole({
  tenantId,
  roleCode,
  email,
  passwordHash,
  name,
  permissionScopeType = "TENANT",
  permissionScopeId = tenantId,
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
    [tenantId, userId, roleId, permissionScopeType, permissionScopeId]
  );

  return { userId, email };
}

async function createLegalEntityWithBook({
  tenantId,
  groupId,
  code,
  name,
  countryId,
  functionalCurrencyCode,
  stamp,
  ordinal,
}) {
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
    [tenantId, groupId, code, name, countryId, functionalCurrencyCode]
  );
  const legalEntityResult = await query(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, code]
  );
  const legalEntityId = toNumber(legalEntityResult.rows?.[0]?.id);
  assert(legalEntityId > 0, `Legal entity create failed: ${code}`);

  const calendarCode = `CARI19CAL_${ordinal}_${stamp}`;
  await query(
    `INSERT INTO fiscal_calendars (
        tenant_id,
        code,
        name,
        year_start_month,
        year_start_day
     )
     VALUES (?, ?, ?, 1, 1)`,
    [tenantId, calendarCode, `CARI19 Calendar ${ordinal} ${stamp}`]
  );
  const calendarResult = await query(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, calendarCode]
  );
  const calendarId = toNumber(calendarResult.rows?.[0]?.id);
  assert(calendarId > 0, `Fiscal calendar create failed: ${calendarCode}`);

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

  const bookCode = `CARI19BOOK_${ordinal}_${stamp}`;
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
      bookCode,
      `CARI19 Book ${ordinal} ${stamp}`,
      functionalCurrencyCode,
    ]
  );

  const coaCode = `CARI19COA_${ordinal}_${stamp}`;
  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id,
        legal_entity_id,
        scope,
        code,
        name
     )
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, coaCode, `CARI19 COA ${ordinal} ${stamp}`]
  );
  const coaResult = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, coaCode]
  );
  const coaId = toNumber(coaResult.rows?.[0]?.id);
  assert(coaId > 0, `COA create failed: ${coaCode}`);

  return { legalEntityId, coaId };
}

async function insertAccountsForCoa({ coaId, specs = [] }) {
  for (const spec of specs) {
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
       VALUES (?, ?, ?, ?, ?, ?, NULL, ?)`,
      [
        coaId,
        spec.code,
        spec.name,
        spec.accountType,
        spec.normalSide,
        spec.allowPosting ? 1 : 0,
        spec.isActive ? 1 : 0,
      ]
    );
  }

  const codes = specs.map((spec) => spec.code);
  if (codes.length === 0) {
    return {};
  }
  const placeholders = codes.map(() => "?").join(", ");
  const result = await query(
    `SELECT id, code
     FROM accounts
     WHERE coa_id = ?
       AND code IN (${placeholders})`,
    [coaId, ...codes]
  );
  const idByCode = new Map(
    (result.rows || []).map((row) => [String(row.code), toNumber(row.id)])
  );
  const byKey = {};
  for (const spec of specs) {
    byKey[spec.key] = idByCode.get(spec.code) || 0;
    assert(byKey[spec.key] > 0, `Account missing for key=${spec.key} code=${spec.code}`);
  }
  return byKey;
}

async function createTenantAccountingFixtures({ tenantId, stamp, prefix = "A" }) {
  const countryResult = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(countryResult.rows?.[0]?.id);
  const functionalCurrencyCode = toUpper(
    countryResult.rows?.[0]?.default_currency_code || "USD"
  );
  assert(countryId > 0, "US country row is required");

  const groupCode = `CARI19GC_${prefix}_${stamp}`;
  await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, groupCode, `CARI19 Group ${prefix} ${stamp}`]
  );
  const groupResult = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, groupCode]
  );
  const groupId = toNumber(groupResult.rows?.[0]?.id);
  assert(groupId > 0, "Group company create failed");

  const entityA = await createLegalEntityWithBook({
    tenantId,
    groupId,
    code: `CARI19LEA_${prefix}_${stamp}`,
    name: `CARI19 LE A ${prefix} ${stamp}`,
    countryId,
    functionalCurrencyCode,
    stamp,
    ordinal: `${prefix}1`,
  });
  const entityB = await createLegalEntityWithBook({
    tenantId,
    groupId,
    code: `CARI19LEB_${prefix}_${stamp}`,
    name: `CARI19 LE B ${prefix} ${stamp}`,
    countryId,
    functionalCurrencyCode,
    stamp,
    ordinal: `${prefix}2`,
  });

  const accountCodePrefixA = `C19${prefix}${String(stamp).slice(-4)}A`;
  const accountCodePrefixB = `C19${prefix}${String(stamp).slice(-4)}B`;
  const entityAAccounts = await insertAccountsForCoa({
    coaId: entityA.coaId,
    specs: [
      {
        key: "arControlDefault",
        code: `${accountCodePrefixA}01`,
        name: "PR19 AR Control Default",
        accountType: "ASSET",
        normalSide: "DEBIT",
        allowPosting: true,
        isActive: true,
      },
      {
        key: "arOffsetDefault",
        code: `${accountCodePrefixA}02`,
        name: "PR19 AR Offset Default",
        accountType: "REVENUE",
        normalSide: "CREDIT",
        allowPosting: true,
        isActive: true,
      },
      {
        key: "apControlDefault",
        code: `${accountCodePrefixA}03`,
        name: "PR19 AP Control Default",
        accountType: "LIABILITY",
        normalSide: "CREDIT",
        allowPosting: true,
        isActive: true,
      },
      {
        key: "apOffsetDefault",
        code: `${accountCodePrefixA}04`,
        name: "PR19 AP Offset Default",
        accountType: "EXPENSE",
        normalSide: "DEBIT",
        allowPosting: true,
        isActive: true,
      },
      {
        key: "arOverridePrimary",
        code: `${accountCodePrefixA}05`,
        name: "PR19 AR Override Primary",
        accountType: "ASSET",
        normalSide: "DEBIT",
        allowPosting: true,
        isActive: true,
      },
      {
        key: "apOverridePrimary",
        code: `${accountCodePrefixA}06`,
        name: "PR19 AP Override Primary",
        accountType: "LIABILITY",
        normalSide: "CREDIT",
        allowPosting: true,
        isActive: true,
      },
      {
        key: "arOverrideForDocInvalidation",
        code: `${accountCodePrefixA}07`,
        name: "PR19 AR Override For Document Revalidation",
        accountType: "ASSET",
        normalSide: "DEBIT",
        allowPosting: true,
        isActive: true,
      },
      {
        key: "arOverrideForSettlementInvalidation",
        code: `${accountCodePrefixA}08`,
        name: "PR19 AR Override For Settlement Revalidation",
        accountType: "ASSET",
        normalSide: "DEBIT",
        allowPosting: true,
        isActive: true,
      },
      {
        key: "arWrongTypeLiability",
        code: `${accountCodePrefixA}09`,
        name: "PR19 Wrong Type Liability",
        accountType: "LIABILITY",
        normalSide: "CREDIT",
        allowPosting: true,
        isActive: true,
      },
      {
        key: "apWrongTypeAsset",
        code: `${accountCodePrefixA}10`,
        name: "PR19 Wrong Type Asset",
        accountType: "ASSET",
        normalSide: "DEBIT",
        allowPosting: true,
        isActive: true,
      },
      {
        key: "arInactive",
        code: `${accountCodePrefixA}11`,
        name: "PR19 Inactive AR Account",
        accountType: "ASSET",
        normalSide: "DEBIT",
        allowPosting: true,
        isActive: false,
      },
      {
        key: "apNonPostable",
        code: `${accountCodePrefixA}12`,
        name: "PR19 Non-Postable AP Account",
        accountType: "LIABILITY",
        normalSide: "CREDIT",
        allowPosting: false,
        isActive: true,
      },
    ],
  });

  const entityBAccounts = await insertAccountsForCoa({
    coaId: entityB.coaId,
    specs: [
      {
        key: "arEntityBAsset",
        code: `${accountCodePrefixB}01`,
        name: "PR19 LE-B Asset Account",
        accountType: "ASSET",
        normalSide: "DEBIT",
        allowPosting: true,
        isActive: true,
      },
    ],
  });

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
      entityA.legalEntityId,
      entityAAccounts.arControlDefault,
      tenantId,
      entityA.legalEntityId,
      entityAAccounts.arOffsetDefault,
      tenantId,
      entityA.legalEntityId,
      entityAAccounts.apControlDefault,
      tenantId,
      entityA.legalEntityId,
      entityAAccounts.apOffsetDefault,
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
    [
      tenantId,
      entityA.legalEntityId,
      `CARI19TERM_${prefix}_${stamp}`,
      `CARI19 Term ${prefix} ${stamp}`,
    ]
  );
  const paymentTermResult = await query(
    `SELECT id
     FROM payment_terms
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [
      tenantId,
      entityA.legalEntityId,
      `CARI19TERM_${prefix}_${stamp}`,
    ]
  );
  const paymentTermAId = toNumber(paymentTermResult.rows?.[0]?.id);
  assert(paymentTermAId > 0, "Payment term create failed");

  return {
    tenantId,
    countryId,
    functionalCurrencyCode,
    legalEntityAId: entityA.legalEntityId,
    legalEntityBId: entityB.legalEntityId,
    paymentTermAId,
    accounts: {
      ...entityAAccounts,
      ...entityBAccounts,
    },
  };
}

async function createCounterparty({
  token,
  legalEntityId,
  code,
  name,
  isCustomer,
  isVendor,
  paymentTermId,
  arAccountId,
  apAccountId,
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/cari/counterparties",
    body: {
      legalEntityId,
      code,
      name,
      isCustomer,
      isVendor,
      status: "ACTIVE",
      defaultPaymentTermId: paymentTermId || null,
      arAccountId,
      apAccountId,
    },
    expectedStatus: 201,
  });
  return response.json?.row || {};
}

async function getCounterparty(token, counterpartyId) {
  const response = await apiRequest({
    token,
    method: "GET",
    requestPath: `/api/v1/cari/counterparties/${counterpartyId}`,
    expectedStatus: 200,
  });
  return response.json?.row || {};
}

async function listCounterparties(token, filters = {}) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(filters)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    searchParams.set(key, String(value));
  }
  const querySuffix = searchParams.toString();
  const requestPath = querySuffix
    ? `/api/v1/cari/counterparties?${querySuffix}`
    : "/api/v1/cari/counterparties";

  const response = await apiRequest({
    token,
    method: "GET",
    requestPath,
    expectedStatus: 200,
  });
  return response.json || {};
}

async function createAndPostDocument({
  token,
  tenantId,
  legalEntityId,
  counterpartyId,
  paymentTermId,
  currencyCode,
  amountTxn,
  amountBase,
  stamp,
  sequence,
}) {
  const day = String(10 + sequence).padStart(2, "0");
  const createResponse = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/cari/documents",
    body: {
      legalEntityId,
      counterpartyId,
      paymentTermId,
      direction: "AR",
      documentType: "INVOICE",
      documentDate: `2026-02-${day}`,
      dueDate: `2026-03-${day}`,
      amountTxn,
      amountBase,
      currencyCode,
      fxRate: 1,
      sourceDocNo: `PR19-DOC-${stamp}-${sequence}`,
    },
    expectedStatus: 201,
  });
  const documentId = toNumber(createResponse.json?.row?.id);
  assert(documentId > 0, "Draft document id missing");

  const postResponse = await apiRequest({
    token,
    method: "POST",
    requestPath: `/api/v1/cari/documents/${documentId}/post`,
    expectedStatus: 200,
  });
  const postedJournalEntryId = toNumber(postResponse.json?.row?.postedJournalEntryId);
  assert(postedJournalEntryId > 0, "postedJournalEntryId missing after post");

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
  assert(openItemId > 0, "Open item row missing after post");

  return {
    documentId,
    openItemId,
    postedJournalEntryId,
  };
}

async function applySettlement({
  token,
  legalEntityId,
  counterpartyId,
  openItemId,
  incomingAmountTxn,
  settlementDate,
  currencyCode,
  idempotencyKey,
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    requestPath: "/api/v1/cari/settlements/apply",
    body: {
      legalEntityId,
      counterpartyId,
      settlementDate,
      currencyCode,
      incomingAmountTxn,
      idempotencyKey,
      autoAllocate: false,
      allocations: [{ openItemId, amountTxn: incomingAmountTxn }],
    },
    expectedStatus: 201,
  });
  return response.json;
}

async function getJournalAccountIds(journalEntryId) {
  const result = await query(
    `SELECT account_id
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no ASC`,
    [journalEntryId]
  );
  return new Set((result.rows || []).map((row) => toNumber(row.account_id)));
}

function assertContainsAll(accountIdsSet, expectedIds, message) {
  for (const accountId of expectedIds) {
    if (!accountIdsSet.has(accountId)) {
      throw new Error(`${message}; missing account_id=${accountId}`);
    }
  }
}

function messageIncludes(response, expectedPart) {
  return String(response?.json?.message || "")
    .toLowerCase()
    .includes(String(expectedPart || "").toLowerCase());
}

async function main() {
  await runMigrations();
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantAId = await createTenant(`CARI19A_${stamp}`, `CARI PR19 A ${stamp}`);
  const tenantBId = await createTenant(`CARI19B_${stamp}`, `CARI PR19 B ${stamp}`);

  await seedCore({ ensureDefaultTenantIfMissing: true });
  const fixturesA = await createTenantAccountingFixtures({
    tenantId: tenantAId,
    stamp,
    prefix: "A",
  });
  const fixturesB = await createTenantAccountingFixtures({
    tenantId: tenantBId,
    stamp,
    prefix: "B",
  });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const user = await createUserWithRole({
    tenantId: tenantAId,
    roleCode: "EntityAccountant",
    email: `cari19_user_${stamp}@example.com`,
    passwordHash,
    name: "CARI19 Accountant",
  });

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const token = await login(user.email, TEST_PASSWORD);

    const mappedCustomer = await createCounterparty({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      code: `CP19_CUS_MAP_${stamp}`,
      name: "PR19 Customer Mapped",
      isCustomer: true,
      isVendor: false,
      paymentTermId: fixturesA.paymentTermAId,
      arAccountId: fixturesA.accounts.arOverridePrimary,
      apAccountId: null,
    });
    const mappedCustomerId = toNumber(mappedCustomer.id);
    assert(mappedCustomerId > 0, "Mapped customer should be created");
    assert(
      toNumber(mappedCustomer.arAccountId) === fixturesA.accounts.arOverridePrimary,
      "Mapped customer should return arAccountId"
    );
    assert(
      Boolean(mappedCustomer.arAccountCode) && Boolean(mappedCustomer.arAccountName),
      "Mapped customer should return AR enrichment fields"
    );

    const mappedVendor = await createCounterparty({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      code: `CP19_VEN_MAP_${stamp}`,
      name: "PR19 Vendor Mapped",
      isCustomer: false,
      isVendor: true,
      paymentTermId: fixturesA.paymentTermAId,
      arAccountId: null,
      apAccountId: fixturesA.accounts.apOverridePrimary,
    });
    const mappedVendorId = toNumber(mappedVendor.id);
    assert(mappedVendorId > 0, "Mapped vendor should be created");
    assert(
      toNumber(mappedVendor.apAccountId) === fixturesA.accounts.apOverridePrimary,
      "Mapped vendor should return apAccountId"
    );
    assert(
      Boolean(mappedVendor.apAccountCode) && Boolean(mappedVendor.apAccountName),
      "Mapped vendor should return AP enrichment fields"
    );

    const pr26ArAlpha = await createCounterparty({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      code: `CP26_AR_A_${stamp}`,
      name: "PR26 AR Alpha",
      isCustomer: true,
      isVendor: false,
      paymentTermId: fixturesA.paymentTermAId,
      arAccountId: fixturesA.accounts.arControlDefault,
      apAccountId: null,
    });
    const pr26ArBeta = await createCounterparty({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      code: `CP26_AR_B_${stamp}`,
      name: "PR26 AR Beta",
      isCustomer: true,
      isVendor: false,
      paymentTermId: fixturesA.paymentTermAId,
      arAccountId: fixturesA.accounts.arOverridePrimary,
      apAccountId: null,
    });
    const pr26ApAlpha = await createCounterparty({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      code: `CP26_AP_A_${stamp}`,
      name: "PR26 AP Alpha",
      isCustomer: false,
      isVendor: true,
      paymentTermId: fixturesA.paymentTermAId,
      arAccountId: null,
      apAccountId: fixturesA.accounts.apControlDefault,
    });
    const pr26NoMap = await createCounterparty({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      code: `CP26_NONE_${stamp}`,
      name: "PR26 No Mapping",
      isCustomer: true,
      isVendor: false,
      paymentTermId: fixturesA.paymentTermAId,
      arAccountId: null,
      apAccountId: null,
    });

    const pr26ArAlphaId = toNumber(pr26ArAlpha.id);
    const pr26ArBetaId = toNumber(pr26ArBeta.id);
    const pr26ApAlphaId = toNumber(pr26ApAlpha.id);
    const pr26NoMapId = toNumber(pr26NoMap.id);
    assert(
      pr26ArAlphaId > 0 && pr26ArBetaId > 0 && pr26ApAlphaId > 0 && pr26NoMapId > 0,
      "PR26 counterparties should be created"
    );

    const qByArCodeResponse = await listCounterparties(token, {
      q: pr26ArAlpha.arAccountCode,
      limit: 100,
      offset: 0,
    });
    const qByArCodeRows = Array.isArray(qByArCodeResponse.rows) ? qByArCodeResponse.rows : [];
    assert(
      qByArCodeRows.some((row) => toNumber(row.id) === pr26ArAlphaId),
      "q search should match AR account code enrichment"
    );

    const filterArCodeResponse = await listCounterparties(token, {
      q: `CP26_`,
      arAccountCode: pr26ArAlpha.arAccountCode,
      limit: 100,
      offset: 0,
    });
    const filterArCodeRows = Array.isArray(filterArCodeResponse.rows)
      ? filterArCodeResponse.rows
      : [];
    assert(
      filterArCodeRows.length === 1 && toNumber(filterArCodeRows[0]?.id) === pr26ArAlphaId,
      "arAccountCode filter should scope to matching enrichment row"
    );

    const filterApNameResponse = await listCounterparties(token, {
      q: `CP26_`,
      apAccountName: "AP Control",
      limit: 100,
      offset: 0,
    });
    const filterApNameRows = Array.isArray(filterApNameResponse.rows)
      ? filterApNameResponse.rows
      : [];
    assert(
      filterApNameRows.length === 1 && toNumber(filterApNameRows[0]?.id) === pr26ApAlphaId,
      "apAccountName filter should scope to matching enrichment row"
    );

    const sortedArResponse = await listCounterparties(token, {
      q: `CP26_`,
      role: "CUSTOMER",
      sortBy: "arAccountCode",
      sortDir: "asc",
      limit: 100,
      offset: 0,
    });
    const sortedArRows = Array.isArray(sortedArResponse.rows) ? sortedArResponse.rows : [];
    const sortedArIndexes = {
      alpha: sortedArRows.findIndex((row) => toNumber(row.id) === pr26ArAlphaId),
      beta: sortedArRows.findIndex((row) => toNumber(row.id) === pr26ArBetaId),
      noMap: sortedArRows.findIndex((row) => toNumber(row.id) === pr26NoMapId),
    };
    assert(
      sortedArIndexes.alpha >= 0 && sortedArIndexes.beta >= 0 && sortedArIndexes.noMap >= 0,
      "Sorted AR response should include all PR26 customer rows"
    );
    assert(
      sortedArIndexes.alpha < sortedArIndexes.beta,
      "sortBy=arAccountCode&sortDir=asc should sort mapped AR codes ascending"
    );
    assert(
      sortedArIndexes.noMap > sortedArIndexes.beta,
      "sortBy=arAccountCode should place null AR mappings after non-null rows"
    );

    const invalidSortBy = await apiRequest({
      token,
      method: "GET",
      requestPath: "/api/v1/cari/counterparties?sortBy=not_a_real_field",
      expectedStatus: 400,
    });
    assert(
      messageIncludes(invalidSortBy, "sortby must be one of"),
      "Invalid sortBy should fail with validation error"
    );

    const invalidSortDir = await apiRequest({
      token,
      method: "GET",
      requestPath: "/api/v1/cari/counterparties?sortDir=sideways",
      expectedStatus: 400,
    });
    assert(
      messageIncludes(invalidSortDir, "sortdir must be one of"),
      "Invalid sortDir should fail with validation error"
    );

    const roleMismatchAr = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixturesA.legalEntityAId,
        code: `CP19_ROLE_AR_${stamp}`,
        name: "Role Mismatch AR",
        isCustomer: false,
        isVendor: true,
        arAccountId: fixturesA.accounts.arOverridePrimary,
      },
      expectedStatus: 400,
    });
    assert(
      messageIncludes(roleMismatchAr, "araccountid requires iscustomer=true"),
      "Role mismatch for AR mapping should fail explicitly"
    );

    const roleMismatchAp = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixturesA.legalEntityAId,
        code: `CP19_ROLE_AP_${stamp}`,
        name: "Role Mismatch AP",
        isCustomer: true,
        isVendor: false,
        apAccountId: fixturesA.accounts.apOverridePrimary,
      },
      expectedStatus: 400,
    });
    assert(
      messageIncludes(roleMismatchAp, "apaccountid requires isvendor=true"),
      "Role mismatch for AP mapping should fail explicitly"
    );

    const crossTenantReject = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixturesA.legalEntityAId,
        code: `CP19_XTEN_${stamp}`,
        name: "Cross Tenant Mapping",
        isCustomer: true,
        isVendor: false,
        arAccountId: fixturesB.accounts.arOverridePrimary,
      },
      expectedStatus: 400,
    });
    assert(
      messageIncludes(crossTenantReject, "araccountid"),
      "Cross-tenant account mapping should be rejected"
    );

    const wrongEntityReject = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixturesA.legalEntityAId,
        code: `CP19_XLE_${stamp}`,
        name: "Wrong Legal Entity Mapping",
        isCustomer: true,
        isVendor: false,
        arAccountId: fixturesA.accounts.arEntityBAsset,
      },
      expectedStatus: 400,
    });
    assert(
      messageIncludes(wrongEntityReject, "belong to legalentityid"),
      "Wrong legal-entity account mapping should be rejected"
    );

    const wrongTypeArReject = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixturesA.legalEntityAId,
        code: `CP19_WRONG_AR_${stamp}`,
        name: "Wrong Type AR",
        isCustomer: true,
        isVendor: false,
        arAccountId: fixturesA.accounts.arWrongTypeLiability,
      },
      expectedStatus: 400,
    });
    assert(
      messageIncludes(wrongTypeArReject, "accounttype=asset"),
      "Wrong account type for AR should be rejected"
    );

    const wrongTypeApReject = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixturesA.legalEntityAId,
        code: `CP19_WRONG_AP_${stamp}`,
        name: "Wrong Type AP",
        isCustomer: false,
        isVendor: true,
        apAccountId: fixturesA.accounts.apWrongTypeAsset,
      },
      expectedStatus: 400,
    });
    assert(
      messageIncludes(wrongTypeApReject, "accounttype=liability"),
      "Wrong account type for AP should be rejected"
    );

    const inactiveReject = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixturesA.legalEntityAId,
        code: `CP19_INACTIVE_${stamp}`,
        name: "Inactive Account Mapping",
        isCustomer: true,
        isVendor: false,
        arAccountId: fixturesA.accounts.arInactive,
      },
      expectedStatus: 400,
    });
    assert(
      messageIncludes(inactiveReject, "active"),
      "Inactive mapped account should be rejected"
    );

    const nonPostableReject = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/counterparties",
      body: {
        legalEntityId: fixturesA.legalEntityAId,
        code: `CP19_NONPOST_${stamp}`,
        name: "Non Postable Account Mapping",
        isCustomer: false,
        isVendor: true,
        apAccountId: fixturesA.accounts.apNonPostable,
      },
      expectedStatus: 400,
    });
    assert(
      messageIncludes(nonPostableReject, "postable"),
      "Non-postable mapped account should be rejected"
    );

    const detailBeforeOmittedUpdate = await getCounterparty(token, mappedCustomerId);
    assert(
      toNumber(detailBeforeOmittedUpdate.arAccountId) === fixturesA.accounts.arOverridePrimary,
      "Precondition: mapped customer should keep AR mapping before omitted update check"
    );

    await apiRequest({
      token,
      method: "PUT",
      requestPath: `/api/v1/cari/counterparties/${mappedCustomerId}`,
      body: {
        name: "PR19 Customer Mapped Updated",
      },
      expectedStatus: 200,
    });
    const detailAfterOmittedUpdate = await getCounterparty(token, mappedCustomerId);
    assert(
      toNumber(detailAfterOmittedUpdate.arAccountId) === fixturesA.accounts.arOverridePrimary,
      "PUT omitted arAccountId must keep existing value"
    );

    await apiRequest({
      token,
      method: "PUT",
      requestPath: `/api/v1/cari/counterparties/${mappedCustomerId}`,
      body: {
        arAccountId: null,
      },
      expectedStatus: 200,
    });
    const detailAfterNullClear = await getCounterparty(token, mappedCustomerId);
    assert(
      detailAfterNullClear.arAccountId === null,
      "PUT explicit arAccountId:null must clear mapping"
    );
    assert(
      detailAfterNullClear.arAccountCode === null &&
        detailAfterNullClear.arAccountName === null,
      "AR enrichment fields should be null after clearing mapping"
    );

    const listAfterClear = await apiRequest({
      token,
      method: "GET",
      requestPath: `/api/v1/cari/counterparties?q=CP19_CUS_MAP_${stamp}&limit=20&offset=0`,
      expectedStatus: 200,
    });
    const listRows = Array.isArray(listAfterClear.json?.rows) ? listAfterClear.json.rows : [];
    const listRow = listRows.find((row) => toNumber(row.id) === mappedCustomerId) || null;
    assert(listRow, "List response should include updated mapped customer");
    assert(
      listRow.arAccountCode === null && listRow.arAccountName === null,
      "List enrichment fields should return null when AR mapping is null"
    );

    await apiRequest({
      token,
      method: "PUT",
      requestPath: `/api/v1/cari/counterparties/${mappedVendorId}`,
      body: {
        apAccountId: null,
      },
      expectedStatus: 200,
    });
    const vendorAfterNullClear = await getCounterparty(token, mappedVendorId);
    assert(vendorAfterNullClear.apAccountId === null, "PUT apAccountId:null must clear AP mapping");
    assert(
      vendorAfterNullClear.apAccountCode === null &&
        vendorAfterNullClear.apAccountName === null,
      "AP enrichment fields should be null after clearing AP mapping"
    );

    const documentMappedCounterparty = await createCounterparty({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      code: `CP19_DOC_MAP_${stamp}`,
      name: "PR19 Document Mapped Counterparty",
      isCustomer: true,
      isVendor: false,
      paymentTermId: fixturesA.paymentTermAId,
      arAccountId: fixturesA.accounts.arOverridePrimary,
      apAccountId: null,
    });

    const docMapped = await createAndPostDocument({
      token,
      tenantId: tenantAId,
      legalEntityId: fixturesA.legalEntityAId,
      counterpartyId: toNumber(documentMappedCounterparty.id),
      paymentTermId: fixturesA.paymentTermAId,
      currencyCode: fixturesA.functionalCurrencyCode,
      amountTxn: 120,
      amountBase: 120,
      stamp,
      sequence: 1,
    });
    const mappedDocAccounts = await getJournalAccountIds(docMapped.postedJournalEntryId);
    assertContainsAll(
      mappedDocAccounts,
      [fixturesA.accounts.arOverridePrimary, fixturesA.accounts.arOffsetDefault],
      "Document posting with AR mapping should use mapped control + purpose offset"
    );

    const documentFallbackCounterparty = await createCounterparty({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      code: `CP19_DOC_FALL_${stamp}`,
      name: "PR19 Document Fallback Counterparty",
      isCustomer: true,
      isVendor: false,
      paymentTermId: fixturesA.paymentTermAId,
      arAccountId: null,
      apAccountId: null,
    });
    const docFallback = await createAndPostDocument({
      token,
      tenantId: tenantAId,
      legalEntityId: fixturesA.legalEntityAId,
      counterpartyId: toNumber(documentFallbackCounterparty.id),
      paymentTermId: fixturesA.paymentTermAId,
      currencyCode: fixturesA.functionalCurrencyCode,
      amountTxn: 130,
      amountBase: 130,
      stamp,
      sequence: 2,
    });
    const fallbackDocAccounts = await getJournalAccountIds(docFallback.postedJournalEntryId);
    assertContainsAll(
      fallbackDocAccounts,
      [fixturesA.accounts.arControlDefault, fixturesA.accounts.arOffsetDefault],
      "Document posting without mapping should fallback to purpose accounts"
    );

    const invalidDocCounterparty = await createCounterparty({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      code: `CP19_DOC_INV_${stamp}`,
      name: "PR19 Document Revalidation Counterparty",
      isCustomer: true,
      isVendor: false,
      paymentTermId: fixturesA.paymentTermAId,
      arAccountId: fixturesA.accounts.arOverrideForDocInvalidation,
      apAccountId: null,
    });
    await query(
      `UPDATE accounts
       SET is_active = FALSE
       WHERE id = ?`,
      [fixturesA.accounts.arOverrideForDocInvalidation]
    );
    const invalidDocDraft = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/documents",
      body: {
        legalEntityId: fixturesA.legalEntityAId,
        counterpartyId: toNumber(invalidDocCounterparty.id),
        paymentTermId: fixturesA.paymentTermAId,
        direction: "AR",
        documentType: "INVOICE",
        documentDate: "2026-02-20",
        dueDate: "2026-03-20",
        amountTxn: 90,
        amountBase: 90,
        currencyCode: fixturesA.functionalCurrencyCode,
        fxRate: 1,
      },
      expectedStatus: 201,
    });
    const invalidDocDraftId = toNumber(invalidDocDraft.json?.row?.id);
    assert(invalidDocDraftId > 0, "Invalidation draft should be created");
    const invalidDocPost = await apiRequest({
      token,
      method: "POST",
      requestPath: `/api/v1/cari/documents/${invalidDocDraftId}/post`,
      expectedStatus: 400,
    });
    assert(
      messageIncludes(invalidDocPost, "active"),
      "Posting should fail when mapped AR account becomes inactive after save"
    );

    const settlementMappedCounterparty = await createCounterparty({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      code: `CP19_SETTLE_MAP_${stamp}`,
      name: "PR19 Settlement Mapped Counterparty",
      isCustomer: true,
      isVendor: false,
      paymentTermId: fixturesA.paymentTermAId,
      arAccountId: fixturesA.accounts.arOverridePrimary,
      apAccountId: null,
    });
    const settlementMappedDoc = await createAndPostDocument({
      token,
      tenantId: tenantAId,
      legalEntityId: fixturesA.legalEntityAId,
      counterpartyId: toNumber(settlementMappedCounterparty.id),
      paymentTermId: fixturesA.paymentTermAId,
      currencyCode: fixturesA.functionalCurrencyCode,
      amountTxn: 150,
      amountBase: 150,
      stamp,
      sequence: 3,
    });
    const settlementMapped = await applySettlement({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      counterpartyId: toNumber(settlementMappedCounterparty.id),
      openItemId: settlementMappedDoc.openItemId,
      incomingAmountTxn: 150,
      settlementDate: "2026-03-21",
      currencyCode: fixturesA.functionalCurrencyCode,
      idempotencyKey: `PR19-SETTLE-MAP-${stamp}`,
    });
    const settlementMappedJournalId = toNumber(settlementMapped?.journal?.journalEntryId);
    assert(settlementMappedJournalId > 0, "Mapped settlement should create journal entry");
    const mappedSettlementAccounts = await getJournalAccountIds(settlementMappedJournalId);
    assertContainsAll(
      mappedSettlementAccounts,
      [fixturesA.accounts.arOverridePrimary, fixturesA.accounts.arOffsetDefault],
      "Settlement posting with AR mapping should use mapped control + purpose offset"
    );

    const settlementFallbackCounterparty = await createCounterparty({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      code: `CP19_SETTLE_FALL_${stamp}`,
      name: "PR19 Settlement Fallback Counterparty",
      isCustomer: true,
      isVendor: false,
      paymentTermId: fixturesA.paymentTermAId,
      arAccountId: null,
      apAccountId: null,
    });
    const settlementFallbackDoc = await createAndPostDocument({
      token,
      tenantId: tenantAId,
      legalEntityId: fixturesA.legalEntityAId,
      counterpartyId: toNumber(settlementFallbackCounterparty.id),
      paymentTermId: fixturesA.paymentTermAId,
      currencyCode: fixturesA.functionalCurrencyCode,
      amountTxn: 160,
      amountBase: 160,
      stamp,
      sequence: 4,
    });
    const settlementFallback = await applySettlement({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      counterpartyId: toNumber(settlementFallbackCounterparty.id),
      openItemId: settlementFallbackDoc.openItemId,
      incomingAmountTxn: 160,
      settlementDate: "2026-03-22",
      currencyCode: fixturesA.functionalCurrencyCode,
      idempotencyKey: `PR19-SETTLE-FALL-${stamp}`,
    });
    const settlementFallbackJournalId = toNumber(settlementFallback?.journal?.journalEntryId);
    assert(settlementFallbackJournalId > 0, "Fallback settlement should create journal entry");
    const fallbackSettlementAccounts = await getJournalAccountIds(settlementFallbackJournalId);
    assertContainsAll(
      fallbackSettlementAccounts,
      [fixturesA.accounts.arControlDefault, fixturesA.accounts.arOffsetDefault],
      "Settlement posting without mapping should fallback to purpose accounts"
    );

    const invalidSettlementCounterparty = await createCounterparty({
      token,
      legalEntityId: fixturesA.legalEntityAId,
      code: `CP19_SETTLE_INV_${stamp}`,
      name: "PR19 Settlement Revalidation Counterparty",
      isCustomer: true,
      isVendor: false,
      paymentTermId: fixturesA.paymentTermAId,
      arAccountId: fixturesA.accounts.arOverrideForSettlementInvalidation,
      apAccountId: null,
    });
    const invalidSettlementDoc = await createAndPostDocument({
      token,
      tenantId: tenantAId,
      legalEntityId: fixturesA.legalEntityAId,
      counterpartyId: toNumber(invalidSettlementCounterparty.id),
      paymentTermId: fixturesA.paymentTermAId,
      currencyCode: fixturesA.functionalCurrencyCode,
      amountTxn: 170,
      amountBase: 170,
      stamp,
      sequence: 5,
    });
    await query(
      `UPDATE accounts
       SET allow_posting = FALSE
       WHERE id = ?`,
      [fixturesA.accounts.arOverrideForSettlementInvalidation]
    );
    const invalidSettlementApply = await apiRequest({
      token,
      method: "POST",
      requestPath: "/api/v1/cari/settlements/apply",
      body: {
        legalEntityId: fixturesA.legalEntityAId,
        counterpartyId: toNumber(invalidSettlementCounterparty.id),
        settlementDate: "2026-03-23",
        currencyCode: fixturesA.functionalCurrencyCode,
        incomingAmountTxn: 170,
        idempotencyKey: `PR19-SETTLE-INV-${stamp}`,
        autoAllocate: false,
        allocations: [{ openItemId: invalidSettlementDoc.openItemId, amountTxn: 170 }],
      },
      expectedStatus: 400,
    });
    assert(
      messageIncludes(invalidSettlementApply, "postable"),
      "Settlement should fail when mapped AR account becomes non-postable after save"
    );

    console.log("PR19/PR26 counterparty account mapping + enrichment list behavior test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: tenantAId,
          checkedCounterparties: [
            mappedCustomerId,
            mappedVendorId,
            toNumber(documentMappedCounterparty.id),
            toNumber(documentFallbackCounterparty.id),
            toNumber(settlementMappedCounterparty.id),
            toNumber(settlementFallbackCounterparty.id),
          ],
          checkedAccounts: {
            arControlDefault: fixturesA.accounts.arControlDefault,
            arOffsetDefault: fixturesA.accounts.arOffsetDefault,
            arOverridePrimary: fixturesA.accounts.arOverridePrimary,
          },
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
