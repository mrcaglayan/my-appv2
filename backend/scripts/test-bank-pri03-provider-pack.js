#!/usr/bin/env node

import crypto from "node:crypto";
import http from "node:http";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  createBankConnector,
  runBankConnectorStatementSync,
  testBankConnectorConnection,
  upsertBankConnectorAccountLink,
} from "../src/services/bank.connectors.service.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function noScopeGuard() {
  return true;
}

function writeJson(res, statusCode, payload) {
  res.statusCode = statusCode;
  res.setHeader("content-type", "application/json; charset=utf-8");
  res.end(JSON.stringify(payload ?? {}));
}

function ensureEncryptionEnv() {
  const activeKid = String(process.env.APP_ENCRYPTION_ACTIVE_KEY_VERSION || "").trim() || "local-v1";
  process.env.APP_ENCRYPTION_ACTIVE_KEY_VERSION = activeKid;

  let parsed = {};
  const raw = String(process.env.APP_ENCRYPTION_KEYS_JSON || "").trim();
  if (raw) {
    try {
      parsed = JSON.parse(raw);
    } catch {
      parsed = {};
    }
  }

  if (!parsed[activeKid]) {
    parsed[activeKid] = crypto.randomBytes(32).toString("base64");
  }
  process.env.APP_ENCRYPTION_KEYS_JSON = JSON.stringify(parsed);
}

async function createTenantWithBankFixture(stamp) {
  const suffix = String(stamp).slice(-6);
  const tenantCode = `I03T${suffix}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRI03 Tenant ${suffix}`]
  );
  const tenantRows = await query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [tenantCode]
  );
  const tenantId = toNumber(tenantRows.rows?.[0]?.id);
  assert(tenantId > 0, "Failed to create tenant");

  const countryRows = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'TR'
     LIMIT 1`
  );
  const countryId = toNumber(countryRows.rows?.[0]?.id);
  const currencyCode = String(countryRows.rows?.[0]?.default_currency_code || "TRY")
    .trim()
    .toUpperCase();
  assert(countryId > 0, "Missing TR country seed row");

  await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `I03G${suffix}`, `PRI03 Group ${suffix}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `I03G${suffix}`]
  );
  const groupCompanyId = toNumber(groupRows.rows?.[0]?.id);
  assert(groupCompanyId > 0, "Failed to create group company");

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
    [tenantId, groupCompanyId, `I03LE${suffix}`, `PRI03 Legal Entity ${suffix}`, countryId, currencyCode]
  );
  const legalEntityRows = await query(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `I03LE${suffix}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity");

  const passwordHash = await bcrypt.hash("PRI03#Smoke123", 10);
  const email = `pri03_user_${suffix}@example.com`;
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, email, passwordHash, "PRI03 User"]
  );
  const userRows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, email]
  );
  const userId = toNumber(userRows.rows?.[0]?.id);
  assert(userId > 0, "Failed to create user");

  await query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
      )
      VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `I03COA${suffix}`, `PRI03 COA ${suffix}`]
  );
  const coaRows = await query(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, `I03COA${suffix}`]
  );
  const coaId = toNumber(coaRows.rows?.[0]?.id);
  assert(coaId > 0, "Failed to create chart of accounts");

  await query(
    `INSERT INTO accounts (
        coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id, is_active
      )
      VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, NULL, TRUE)`,
    [coaId, `I03BANK${suffix}`, `PRI03 Bank GL ${suffix}`]
  );
  const glRows = await query(
    `SELECT id
     FROM accounts
     WHERE coa_id = ?
       AND code = ?
     LIMIT 1`,
    [coaId, `I03BANK${suffix}`]
  );
  const glAccountId = toNumber(glRows.rows?.[0]?.id);
  assert(glAccountId > 0, "Failed to create bank GL account");

  const ibanSuffix = String(stamp).slice(-24).padStart(24, "0");
  await query(
    `INSERT INTO bank_accounts (
        tenant_id,
        legal_entity_id,
        code,
        name,
        currency_code,
        gl_account_id,
        bank_name,
        branch_name,
        iban,
        account_no,
        is_active,
        created_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, ?)`,
    [
      tenantId,
      legalEntityId,
      `I03BA${suffix}`,
      `PRI03 Bank Account ${suffix}`,
      currencyCode,
      glAccountId,
      "Sandbox Bank",
      "Main",
      `TR${ibanSuffix}`,
      `AC${suffix}`,
      userId,
    ]
  );
  const bankAccountRows = await query(
    `SELECT id
     FROM bank_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, `I03BA${suffix}`]
  );
  const bankAccountId = toNumber(bankAccountRows.rows?.[0]?.id);
  assert(bankAccountId > 0, "Failed to create bank account");

  return {
    tenantId,
    legalEntityId,
    userId,
    bankAccountId,
    currencyCode,
  };
}

async function startSandboxProviderServer({ token, externalAccountId, currencyCode }) {
  const requestLog = [];
  const server = http.createServer((req, res) => {
    const requestUrl = new URL(req.url || "/", "http://127.0.0.1");
    requestLog.push({
      method: String(req.method || "").toUpperCase(),
      path: requestUrl.pathname,
      query: Object.fromEntries(requestUrl.searchParams.entries()),
    });

    const auth = String(req.headers?.authorization || "");
    if (auth !== `Bearer ${token}`) {
      writeJson(res, 401, { message: "Unauthorized" });
      return;
    }

    if (String(req.method || "").toUpperCase() !== "GET") {
      writeJson(res, 405, { message: "Method not allowed" });
      return;
    }

    if (requestUrl.pathname === "/sandbox/v1/health") {
      writeJson(res, 200, {
        ok: true,
        bank_name: "Sandbox Partner Bank",
      });
      return;
    }

    if (requestUrl.pathname === "/sandbox/v1/statements") {
      const cursor = String(requestUrl.searchParams.get("cursor") || "").trim() || null;
      const bookingDate =
        String(requestUrl.searchParams.get("toDate") || "").trim() ||
        String(requestUrl.searchParams.get("fromDate") || "").trim() ||
        "2026-02-26";
      const txnSuffix = cursor ? "002" : "001";
      writeJson(res, 200, {
        accounts: [
          {
            external_account_id: externalAccountId,
            account_name: "Sandbox TRY Account",
            currency_code: currencyCode,
            lines: [
              {
                external_txn_id: `SBOX-${bookingDate}-${txnSuffix}`,
                booking_date: bookingDate,
                value_date: bookingDate,
                amount: -1250.25,
                currency_code: currencyCode,
                description: "Sandbox provider transfer",
                reference: `SBOXREF-${txnSuffix}`,
                counterparty_name: "Sandbox Counterparty",
                balance_after: 86000.75,
              },
            ],
          },
        ],
        next_cursor: cursor ? null : `SBOX-CURSOR-${bookingDate}`,
      });
      return;
    }

    writeJson(res, 404, { message: "Not found" });
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  const port =
    address && typeof address === "object" && "port" in address ? Number(address.port) : 0;
  assert(port > 0, "Failed to start sandbox provider server");

  return {
    server,
    baseUrl: `http://127.0.0.1:${port}`,
    requestLog,
  };
}

async function closeServer(server) {
  if (!server) return;
  await new Promise((resolve) => {
    server.close(() => resolve());
  });
}

async function main() {
  ensureEncryptionEnv();
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithBankFixture(stamp);
  const externalAccountId = "SANDBOX-EXT-001";
  const token = `sandbox-token-${stamp}`;

  const sandbox = await startSandboxProviderServer({
    token,
    externalAccountId,
    currencyCode: fixture.currencyCode,
  });

  let connectorId = 0;
  try {
    const created = await createBankConnector({
      req: null,
      assertScopeAccess: noScopeGuard,
      input: {
        tenantId: fixture.tenantId,
        legalEntityId: fixture.legalEntityId,
        userId: fixture.userId,
        connectorCode: `I03CONN${String(stamp).slice(-6)}`,
        connectorName: "PRI03 Sandbox Connector",
        providerCode: "SANDBOX_OB",
        connectorType: "OPEN_BANKING",
        status: "ACTIVE",
        adapterVersion: "v1",
        config: {
          baseUrl: sandbox.baseUrl,
          healthPath: "/sandbox/v1/health",
          statementsPath: "/sandbox/v1/statements",
          bankName: "Sandbox Partner Bank",
          timeoutMs: 7000,
        },
        credentials: {
          token,
        },
        syncMode: "MANUAL",
        syncFrequencyMinutes: null,
        nextSyncAt: null,
      },
    });

    connectorId = toNumber(created?.connector?.id);
    assert(connectorId > 0, "Failed to create SANDBOX_OB connector");

    await upsertBankConnectorAccountLink({
      req: null,
      assertScopeAccess: noScopeGuard,
      input: {
        tenantId: fixture.tenantId,
        userId: fixture.userId,
        connectorId,
        bankAccountId: fixture.bankAccountId,
        externalAccountId,
        externalAccountName: "Sandbox TRY Account",
        externalCurrencyCode: fixture.currencyCode,
        status: "ACTIVE",
      },
    });

    const connectionTest = await testBankConnectorConnection({
      req: null,
      tenantId: fixture.tenantId,
      connectorId,
      assertScopeAccess: noScopeGuard,
    });
    assert(connectionTest?.result?.ok === true, "SANDBOX_OB test connection should return ok=true");

    const requestId = `PRI03_REQ_${stamp}`;
    const syncResult = await runBankConnectorStatementSync({
      req: null,
      tenantId: fixture.tenantId,
      connectorId,
      userId: fixture.userId,
      input: {
        fromDate: "2026-02-01",
        toDate: "2026-02-28",
        requestId,
        forceFull: false,
      },
      assertScopeAccess: noScopeGuard,
    });

    const syncStatus = String(syncResult?.sync_run?.status || "").toUpperCase();
    assert(["SUCCESS", "PARTIAL"].includes(syncStatus), `Unexpected sync status: ${syncStatus}`);
    assert(Number(syncResult?.imports?.length || 0) >= 1, "Expected at least one imported account batch");

    const idemResult = await runBankConnectorStatementSync({
      req: null,
      tenantId: fixture.tenantId,
      connectorId,
      userId: fixture.userId,
      input: {
        fromDate: "2026-02-01",
        toDate: "2026-02-28",
        requestId,
        forceFull: false,
      },
      assertScopeAccess: noScopeGuard,
    });
    assert(idemResult?.idempotent === true, "Second sync with same requestId should be idempotent");

    const statementCountRows = await query(
      `SELECT COUNT(*) AS total
       FROM bank_statement_lines
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND bank_account_id = ?`,
      [fixture.tenantId, fixture.legalEntityId, fixture.bankAccountId]
    );
    const statementLineCount = toNumber(statementCountRows.rows?.[0]?.total);
    assert(statementLineCount >= 1, "Expected imported bank statement lines from SANDBOX_OB");

    const calledHealth = sandbox.requestLog.some((row) => row.path === "/sandbox/v1/health");
    const calledStatements = sandbox.requestLog.some((row) => row.path === "/sandbox/v1/statements");
    assert(calledHealth, "Sandbox health endpoint was not called");
    assert(calledStatements, "Sandbox statements endpoint was not called");

    console.log(
      JSON.stringify(
        {
          ok: true,
          step: "PR-I03",
          provider_code: "SANDBOX_OB",
          tenant_id: fixture.tenantId,
          legal_entity_id: fixture.legalEntityId,
          connector_id: connectorId,
          bank_account_id: fixture.bankAccountId,
          statement_line_count: statementLineCount,
          sync_status: syncStatus,
          request_log_count: sandbox.requestLog.length,
        },
        null,
        2
      )
    );
  } finally {
    await closeServer(sandbox.server);
  }
}

main()
  .catch((err) => {
    // eslint-disable-next-line no-console
    console.error("[PR-I03 smoke] failed", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await closePool();
    } catch {
      // ignore close errors during script shutdown
    }
  });
