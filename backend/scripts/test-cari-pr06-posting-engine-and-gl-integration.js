import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CARI_PR06_TEST_PORT || 3125);
const BASE_URL =
  process.env.CARI_PR06_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_PASSWORD = "CariPR06#12345";

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
  const sessionCookie = response.cookie;
  assert(Boolean(sessionCookie), `Login cookie missing for ${email}`);
  return sessionCookie;
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
  dataScopes = [],
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

  for (const scope of dataScopes) {
    await query(
      `INSERT INTO data_scopes (
          tenant_id,
          user_id,
          scope_type,
          scope_id,
          effect,
          created_by_user_id
       )
       VALUES (?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         effect = VALUES(effect),
         created_by_user_id = VALUES(created_by_user_id)`,
      [
        tenantId,
        userId,
        toUpper(scope.scopeType),
        toNumber(scope.scopeId),
        toUpper(scope.effect || "ALLOW"),
        userId,
      ]
    );
  }

  return { userId, email };
}

async function createOrgAccountingFixtures({ tenantId, stamp }) {
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

  await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `CARI06GC${stamp}`, `CARI06 Group ${stamp}`]
  );
  const groupResult = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `CARI06GC${stamp}`]
  );
  const groupId = toNumber(groupResult.rows?.[0]?.id);
  assert(groupId > 0, "Group company create failed");

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
     VALUES
       (?, ?, ?, ?, ?, ?, 'ACTIVE'),
       (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      groupId,
      `CARI06LEA${stamp}`,
      `CARI06 Legal Entity A ${stamp}`,
      countryId,
      functionalCurrencyCode,
      tenantId,
      groupId,
      `CARI06LEB${stamp}`,
      `CARI06 Legal Entity B ${stamp}`,
      countryId,
      functionalCurrencyCode,
    ]
  );

  const legalEntityRows = await query(
    `SELECT id, code
     FROM legal_entities
     WHERE tenant_id = ?
       AND code IN (?, ?)
     ORDER BY code`,
    [tenantId, `CARI06LEA${stamp}`, `CARI06LEB${stamp}`]
  );
  const legalEntityByCode = new Map(
    legalEntityRows.rows.map((row) => [String(row.code), toNumber(row.id)])
  );
  const legalEntityAId = legalEntityByCode.get(`CARI06LEA${stamp}`);
  const legalEntityBId = legalEntityByCode.get(`CARI06LEB${stamp}`);
  assert(legalEntityAId > 0, "Legal entity A missing");
  assert(legalEntityBId > 0, "Legal entity B missing");

  for (const [index, legalEntityId] of [legalEntityAId, legalEntityBId].entries()) {
    const calendarCode = `CARI06CAL${index + 1}_${stamp}`;
    await query(
      `INSERT INTO fiscal_calendars (
          tenant_id,
          code,
          name,
          year_start_month,
          year_start_day
       )
       VALUES (?, ?, ?, 1, 1)`,
      [tenantId, calendarCode, `CARI06 Calendar ${index + 1} ${stamp}`]
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
       ON DUPLICATE KEY UPDATE
         period_name = VALUES(period_name)`,
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
        `CARI06BOOK${index + 1}_${stamp}`,
        `CARI06 Book ${index + 1} ${stamp}`,
        functionalCurrencyCode,
      ]
    );
  }

  const bookRows = await query(
    `SELECT id, legal_entity_id
     FROM books
     WHERE tenant_id = ?
       AND legal_entity_id IN (?, ?)`,
    [tenantId, legalEntityAId, legalEntityBId]
  );
  const bookIdByEntity = new Map();
  for (const row of bookRows.rows || []) {
    bookIdByEntity.set(toNumber(row.legal_entity_id), toNumber(row.id));
  }
  assert(bookIdByEntity.get(legalEntityAId) > 0, "Book for LE-A missing");
  assert(bookIdByEntity.get(legalEntityBId) > 0, "Book for LE-B missing");

  const accountIdsByEntity = new Map();

  for (const [index, legalEntityId] of [legalEntityAId, legalEntityBId].entries()) {
    const coaCode = `CARI06COA${index + 1}_${stamp}`;
    await query(
      `INSERT INTO charts_of_accounts (
          tenant_id,
          legal_entity_id,
          scope,
          code,
          name
       )
       VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
      [tenantId, legalEntityId, coaCode, `CARI06 COA ${index + 1} ${stamp}`]
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
    assert(coaId > 0, "COA create failed");

    const accountCodePrefix = `C6${index + 1}${String(stamp).slice(-4)}`;
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
        `${accountCodePrefix}01`,
        `CARI AR Control ${index + 1}`,
        coaId,
        `${accountCodePrefix}02`,
        `CARI AR Offset ${index + 1}`,
        coaId,
        `${accountCodePrefix}03`,
        `CARI AP Control ${index + 1}`,
        coaId,
        `${accountCodePrefix}04`,
        `CARI AP Offset ${index + 1}`,
      ]
    );

    const accountRows = await query(
      `SELECT id, code
       FROM accounts
       WHERE coa_id = ?
         AND code IN (?, ?, ?, ?)
       ORDER BY code`,
      [
        coaId,
        `${accountCodePrefix}01`,
        `${accountCodePrefix}02`,
        `${accountCodePrefix}03`,
        `${accountCodePrefix}04`,
      ]
    );
    const accountByCode = new Map(
      accountRows.rows.map((row) => [String(row.code), toNumber(row.id)])
    );
    const arControlAccountId = accountByCode.get(`${accountCodePrefix}01`);
    const arOffsetAccountId = accountByCode.get(`${accountCodePrefix}02`);
    const apControlAccountId = accountByCode.get(`${accountCodePrefix}03`);
    const apOffsetAccountId = accountByCode.get(`${accountCodePrefix}04`);
    assert(arControlAccountId > 0, "AR control account missing");
    assert(arOffsetAccountId > 0, "AR offset account missing");
    assert(apControlAccountId > 0, "AP control account missing");
    assert(apOffsetAccountId > 0, "AP offset account missing");

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
        arControlAccountId,
        tenantId,
        legalEntityId,
        arOffsetAccountId,
        tenantId,
        legalEntityId,
        apControlAccountId,
        tenantId,
        legalEntityId,
        apOffsetAccountId,
      ]
    );

    accountIdsByEntity.set(legalEntityId, {
      arControlAccountId,
      arOffsetAccountId,
      apControlAccountId,
      apOffsetAccountId,
    });
  }

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
     VALUES
       (?, ?, ?, ?, 30, 0, 'ACTIVE'),
       (?, ?, ?, ?, 45, 0, 'ACTIVE')`,
    [
      tenantId,
      legalEntityAId,
      `CARI06TERM_A_${stamp}`,
      `CARI06 Term A ${stamp}`,
      tenantId,
      legalEntityBId,
      `CARI06TERM_B_${stamp}`,
      `CARI06 Term B ${stamp}`,
    ]
  );
  const paymentTermRows = await query(
    `SELECT id, legal_entity_id
     FROM payment_terms
     WHERE tenant_id = ?
       AND code IN (?, ?)`,
    [tenantId, `CARI06TERM_A_${stamp}`, `CARI06TERM_B_${stamp}`]
  );
  let paymentTermAId = 0;
  let paymentTermBId = 0;
  for (const row of paymentTermRows.rows || []) {
    const leId = toNumber(row.legal_entity_id);
    if (leId === legalEntityAId) {
      paymentTermAId = toNumber(row.id);
    } else if (leId === legalEntityBId) {
      paymentTermBId = toNumber(row.id);
    }
  }
  assert(paymentTermAId > 0, "Payment term A missing");
  assert(paymentTermBId > 0, "Payment term B missing");

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
     VALUES
       (?, ?, ?, ?, TRUE, FALSE, ?, ?, 'ACTIVE'),
       (?, ?, ?, ?, TRUE, FALSE, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      legalEntityAId,
      `CARI06CPA${stamp}`,
      `CARI06 Counterparty A ${stamp}`,
      functionalCurrencyCode,
      paymentTermAId,
      tenantId,
      legalEntityBId,
      `CARI06CPB${stamp}`,
      `CARI06 Counterparty B ${stamp}`,
      functionalCurrencyCode,
      paymentTermBId,
    ]
  );
  const counterpartyRows = await query(
    `SELECT id, legal_entity_id
     FROM counterparties
     WHERE tenant_id = ?
       AND code IN (?, ?)`,
    [tenantId, `CARI06CPA${stamp}`, `CARI06CPB${stamp}`]
  );
  let counterpartyAId = 0;
  let counterpartyBId = 0;
  for (const row of counterpartyRows.rows || []) {
    const leId = toNumber(row.legal_entity_id);
    if (leId === legalEntityAId) {
      counterpartyAId = toNumber(row.id);
    } else if (leId === legalEntityBId) {
      counterpartyBId = toNumber(row.id);
    }
  }
  assert(counterpartyAId > 0, "Counterparty A missing");
  assert(counterpartyBId > 0, "Counterparty B missing");

  return {
    functionalCurrencyCode,
    legalEntityAId,
    legalEntityBId,
    bookAId: bookIdByEntity.get(legalEntityAId),
    bookBId: bookIdByEntity.get(legalEntityBId),
    paymentTermAId,
    paymentTermBId,
    counterpartyAId,
    counterpartyBId,
    accountIdsByEntity,
  };
}

async function fetchDocumentFromApi(token, documentId) {
  const response = await apiRequest({
    token,
    method: "GET",
    requestPath: `/api/v1/cari/documents/${documentId}`,
    expectedStatus: 200,
  });
  return response.json?.row || null;
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const tenantAId = await createTenant(`CARI06A_${stamp}`, `CARI PR06 A ${stamp}`);
  const tenantBId = await createTenant(`CARI06B_${stamp}`, `CARI PR06 B ${stamp}`);

  await seedCore({ ensureDefaultTenantIfMissing: true });
  const fixtures = await createOrgAccountingFixtures({ tenantId: tenantAId, stamp });

  const passwordHash = await bcrypt.hash(TEST_PASSWORD, 10);
  const tenantWideIdentity = await createUserWithRole({
    tenantId: tenantAId,
    roleCode: "EntityAccountant",
    email: `cari06_tenantwide_${stamp}@example.com`,
    passwordHash,
    name: "CARI06 Tenant Wide",
  });
  const overrideIdentity = await createUserWithRole({
    tenantId: tenantAId,
    roleCode: "CountryController",
    email: `cari06_override_${stamp}@example.com`,
    passwordHash,
    name: "CARI06 FX Override",
  });
  const scopedIdentity = await createUserWithRole({
    tenantId: tenantAId,
    roleCode: "EntityAccountant",
    email: `cari06_scoped_${stamp}@example.com`,
    passwordHash,
    name: "CARI06 Scoped",
    dataScopes: [
      {
        scopeType: "LEGAL_ENTITY",
        scopeId: fixtures.legalEntityAId,
        effect: "ALLOW",
      },
    ],
  });
  const otherTenantIdentity = await createUserWithRole({
    tenantId: tenantBId,
    roleCode: "EntityAccountant",
    email: `cari06_other_${stamp}@example.com`,
    passwordHash,
    name: "CARI06 Other Tenant",
  });

  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();

    const tenantWideToken = await login(tenantWideIdentity.email, TEST_PASSWORD);
    const overrideToken = await login(overrideIdentity.email, TEST_PASSWORD);
    const scopedToken = await login(scopedIdentity.email, TEST_PASSWORD);
    const otherTenantToken = await login(otherTenantIdentity.email, TEST_PASSWORD);

    const createPrimaryDraft = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: "/api/v1/cari/documents",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        counterpartyId: fixtures.counterpartyAId,
        paymentTermId: fixtures.paymentTermAId,
        direction: "AR",
        documentType: "INVOICE",
        documentDate: "2026-02-10",
        dueDate: "2026-03-12",
        amountTxn: 1200,
        amountBase: 1200,
        currencyCode: fixtures.functionalCurrencyCode,
        fxRate: 1,
      },
      expectedStatus: 201,
    });
    const primaryDraft = createPrimaryDraft.json?.row || {};
    const primaryDraftId = toNumber(primaryDraft.id);
    assert(primaryDraftId > 0, "Primary draft should be created");
    assert(primaryDraft.status === "DRAFT", "Primary draft should be DRAFT");
    assert(
      toUpper(primaryDraft.sequenceNamespace) === "DRAFT",
      "Draft should use DRAFT sequence namespace before posting"
    );
    assert(
      String(primaryDraft.documentNo || "").startsWith("DRAFT-"),
      "Draft document number should be temporary before POST"
    );

    const snapshotNameAtPost = `CARI06 Snapshot At Post ${stamp}`;
    await query(
      `UPDATE counterparties
       SET name = ?
       WHERE tenant_id = ?
         AND id = ?`,
      [snapshotNameAtPost, tenantAId, fixtures.counterpartyAId]
    );

    const postPrimary = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: `/api/v1/cari/documents/${primaryDraftId}/post`,
      expectedStatus: 200,
    });
    const postedPrimary = postPrimary.json?.row || {};
    const postedPrimaryJournal = postPrimary.json?.journal || {};
    assert(
      postedPrimary.status === "POSTED",
      "Primary draft should transition to POSTED on post"
    );
    assert(
      toUpper(postedPrimary.sequenceNamespace) === "INVOICE",
      "Posted document should use posted sequence namespace by documentType"
    );
    assert(
      String(postedPrimary.documentNo || "").startsWith("DRAFT-") === false,
      "Posted document number should not use draft numbering"
    );
    assert(
      toNumber(postedPrimary.postedJournalEntryId) > 0,
      "posted_journal_entry_id should be populated on post"
    );
    assert(
      toNumber(postedPrimaryJournal.journalEntryId) ===
        toNumber(postedPrimary.postedJournalEntryId),
      "Post response should return created posted journal id"
    );
    assert(
      postedPrimary.counterpartyNameSnapshot === snapshotNameAtPost,
      "Snapshot should capture latest counterparty name at POST boundary"
    );

    const snapshotNameAfterPost = `CARI06 Snapshot After Post ${stamp}`;
    await query(
      `UPDATE counterparties
       SET name = ?
       WHERE tenant_id = ?
         AND id = ?`,
      [snapshotNameAfterPost, tenantAId, fixtures.counterpartyAId]
    );
    const postedPrimaryDetail = await fetchDocumentFromApi(tenantWideToken, primaryDraftId);
    assert(
      postedPrimaryDetail?.counterpartyNameSnapshot === snapshotNameAtPost,
      "Snapshot should remain immutable after post even if master data changes"
    );

    const openItemRows = await query(
      `SELECT
         id,
         status,
         residual_amount_txn,
         residual_amount_base,
         original_amount_txn,
         original_amount_base
       FROM cari_open_items
       WHERE tenant_id = ?
         AND document_id = ?`,
      [tenantAId, primaryDraftId]
    );
    assert(openItemRows.rows.length === 1, "Posting should create one open item row");
    const openItem = openItemRows.rows[0];
    assert(toUpper(openItem.status) === "OPEN", "Open item should be OPEN after posting");
    assert(
      amountsEqual(openItem.original_amount_txn, 1200) &&
        amountsEqual(openItem.residual_amount_txn, 1200),
      "Open item txn amounts should match posted document amount"
    );
    assert(
      amountsEqual(openItem.original_amount_base, 1200) &&
        amountsEqual(openItem.residual_amount_base, 1200),
      "Open item base amounts should match posted document amount"
    );

    const postedJournalRows = await query(
      `SELECT status, source_type
       FROM journal_entries
       WHERE tenant_id = ?
         AND id = ?`,
      [tenantAId, postedPrimary.postedJournalEntryId]
    );
    const postedJournal = postedJournalRows.rows?.[0] || null;
    assert(postedJournal, "Posted journal entry should exist");
    assert(
      toUpper(postedJournal.status) === "POSTED",
      "Created journal entry should be POSTED"
    );
    assert(
      toUpper(postedJournal.source_type) === "SYSTEM",
      "Cari posting should create SYSTEM source journal"
    );

    const postedLineRows = await query(
      `SELECT subledger_reference_no, debit_base, credit_base
       FROM journal_lines
       WHERE journal_entry_id = ?
       ORDER BY line_no`,
      [postedPrimary.postedJournalEntryId]
    );
    assert(postedLineRows.rows.length === 2, "Posted journal should have two lines");
    assert(
      postedLineRows.rows.every(
        (row) =>
          String(row.subledger_reference_no || "") === `CARI_DOC:${primaryDraftId}`
      ),
      "Posted journal lines should populate subledger_reference_no"
    );
    assert(
      amountsEqual(
        toNumber(postedLineRows.rows[0].debit_base) +
          toNumber(postedLineRows.rows[1].debit_base),
        toNumber(postedLineRows.rows[0].credit_base) +
          toNumber(postedLineRows.rows[1].credit_base)
      ),
      "Posted journal line totals should be balanced"
    );

    await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: `/api/v1/cari/documents/${primaryDraftId}/cancel`,
      expectedStatus: 400,
    });

    await apiRequest({
      token: tenantWideToken,
      method: "PUT",
      requestPath: `/api/v1/cari/documents/${primaryDraftId}`,
      body: {
        amountTxn: 1400,
        amountBase: 1400,
      },
      expectedStatus: 400,
    });

    const createDraftForReverseBoundary = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: "/api/v1/cari/documents",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        counterpartyId: fixtures.counterpartyAId,
        paymentTermId: fixtures.paymentTermAId,
        direction: "AP",
        documentType: "PAYMENT",
        documentDate: "2026-02-11",
        amountTxn: 200,
        amountBase: 200,
        currencyCode: fixtures.functionalCurrencyCode,
        fxRate: 1,
      },
      expectedStatus: 201,
    });
    const reverseBoundaryDraftId = toNumber(createDraftForReverseBoundary.json?.row?.id);
    assert(reverseBoundaryDraftId > 0, "Draft for reverse-boundary test should exist");

    await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: `/api/v1/cari/documents/${reverseBoundaryDraftId}/reverse`,
      body: { reason: "Should fail for draft" },
      expectedStatus: 400,
    });

    await query(
      `INSERT INTO fx_rates (
          tenant_id,
          rate_date,
          from_currency_code,
          to_currency_code,
          rate_type,
          rate,
          source,
          is_locked
       )
       VALUES (?, '2026-03-15', 'EUR', ?, 'SPOT', 1.1000000000, 'PR06-TEST', TRUE)
       ON DUPLICATE KEY UPDATE
         rate = VALUES(rate),
         source = VALUES(source),
         is_locked = VALUES(is_locked)`,
      [tenantAId, fixtures.functionalCurrencyCode]
    );

    const createLockedFxDraft = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: "/api/v1/cari/documents",
      body: {
        legalEntityId: fixtures.legalEntityAId,
        counterpartyId: fixtures.counterpartyAId,
        paymentTermId: fixtures.paymentTermAId,
        direction: "AR",
        documentType: "INVOICE",
        documentDate: "2026-03-15",
        dueDate: "2026-04-14",
        amountTxn: 100,
        amountBase: 115,
        currencyCode: "EUR",
        fxRate: 1.15,
      },
      expectedStatus: 201,
    });
    const lockedFxDraftId = toNumber(createLockedFxDraft.json?.row?.id);
    assert(lockedFxDraftId > 0, "Locked-FX draft should be created");

    await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: `/api/v1/cari/documents/${lockedFxDraftId}/post`,
      expectedStatus: 400,
    });

    const overridePost = await apiRequest({
      token: overrideToken,
      method: "POST",
      requestPath: `/api/v1/cari/documents/${lockedFxDraftId}/post`,
      body: {
        useFxOverride: true,
        fxOverrideReason: "Country controller approved locked FX override",
      },
      expectedStatus: 200,
    });
    const overridePostedRow = overridePost.json?.row || {};
    assert(
      overridePostedRow.status === "POSTED",
      "Override user should be able to post locked-FX document"
    );
    assert(
      amountsEqual(overridePostedRow.fxRate, 1.15),
      "Override post should keep overridden fxRate"
    );

    const fxOverrideAuditRows = await query(
      `SELECT COUNT(*) AS row_count
       FROM audit_logs
       WHERE tenant_id = ?
         AND action = 'cari.document.post.fx_override'
         AND resource_type = 'cari_document'
         AND resource_id = ?`,
      [tenantAId, String(lockedFxDraftId)]
    );
    assert(
      toNumber(fxOverrideAuditRows.rows?.[0]?.row_count) >= 1,
      "FX override post should write override audit log"
    );

    const reversePosted = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: `/api/v1/cari/documents/${primaryDraftId}/reverse`,
      body: {
        reason: "Customer dispute adjustment",
        reversalDate: "2026-03-20",
      },
      expectedStatus: 201,
    });
    const reversalRow = reversePosted.json?.row || {};
    const reversedOriginal = reversePosted.json?.original || {};
    const reversalJournal = reversePosted.json?.journal || {};
    const reversalDocumentId = toNumber(reversalRow.id);
    assert(reversalDocumentId > 0, "Reverse should create reversal document");
    assert(
      toUpper(reversalRow.status) === "REVERSED",
      "Reversal document status should be REVERSED"
    );
    assert(
      toNumber(reversalRow.reversalOfDocumentId) === primaryDraftId,
      "Reversal document should link to original document"
    );
    assert(
      toUpper(reversedOriginal.status) === "REVERSED",
      "Original document should transition to REVERSED"
    );
    assert(
      toNumber(reversalJournal.originalJournalEntryId) ===
        toNumber(postedPrimary.postedJournalEntryId),
      "Reverse response should link original journal id"
    );
    assert(
      toNumber(reversalJournal.reversalJournalEntryId) > 0,
      "Reverse should create reversal journal entry"
    );

    const originalAfterReverse = await fetchDocumentFromApi(tenantWideToken, primaryDraftId);
    assert(
      toUpper(originalAfterReverse?.status) === "REVERSED",
      "Original document detail should show REVERSED after reverse"
    );

    const journalReverseRows = await query(
      `SELECT status, reversal_journal_entry_id
       FROM journal_entries
       WHERE tenant_id = ?
         AND id = ?`,
      [tenantAId, postedPrimary.postedJournalEntryId]
    );
    assert(
      toUpper(journalReverseRows.rows?.[0]?.status) === "REVERSED",
      "Original journal should be marked REVERSED"
    );
    assert(
      toNumber(journalReverseRows.rows?.[0]?.reversal_journal_entry_id) ===
        toNumber(reversalJournal.reversalJournalEntryId),
      "Original journal should reference reversal journal entry id"
    );

    await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: `/api/v1/cari/documents/${primaryDraftId}/reverse`,
      body: { reason: "Second reverse attempt should fail" },
      expectedStatus: 400,
    });

    const createLeBDraft = await apiRequest({
      token: tenantWideToken,
      method: "POST",
      requestPath: "/api/v1/cari/documents",
      body: {
        legalEntityId: fixtures.legalEntityBId,
        counterpartyId: fixtures.counterpartyBId,
        paymentTermId: fixtures.paymentTermBId,
        direction: "AR",
        documentType: "INVOICE",
        documentDate: "2026-04-01",
        dueDate: "2026-05-01",
        amountTxn: 500,
        amountBase: 500,
        currencyCode: fixtures.functionalCurrencyCode,
        fxRate: 1,
      },
      expectedStatus: 201,
    });
    const leBDraftId = toNumber(createLeBDraft.json?.row?.id);
    assert(leBDraftId > 0, "LE-B draft should be created");

    await apiRequest({
      token: scopedToken,
      method: "POST",
      requestPath: `/api/v1/cari/documents/${leBDraftId}/post`,
      expectedStatus: 403,
    });

    await apiRequest({
      token: otherTenantToken,
      method: "POST",
      requestPath: `/api/v1/cari/documents/${leBDraftId}/post`,
      expectedStatus: 400,
    });

    console.log("CARI PR-06 posting engine + GL integration test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: tenantAId,
          checkedDocumentIds: [
            primaryDraftId,
            reverseBoundaryDraftId,
            lockedFxDraftId,
            reversalDocumentId,
            leBDraftId,
          ],
          checkedJournalIds: [
            toNumber(postedPrimary.postedJournalEntryId),
            toNumber(reversalJournal.reversalJournalEntryId),
          ],
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
