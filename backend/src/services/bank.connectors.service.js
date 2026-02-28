import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  decryptJson,
  encryptJson,
  parseEnvelopeText,
  serializeEnvelope,
} from "../utils/cryptoEnvelope.js";
import { getBankConnectorAdapter } from "./bankConnectorAdapters/index.js";
import { importNormalizedBankStatementLines } from "./bank.statements.service.js";

function u(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function parseJson(value, fallback = null) {
  if (value === undefined || value === null) return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function isDuplicateEntryError(err) {
  return Number(err?.errno) === 1062 || u(err?.code) === "ER_DUP_ENTRY";
}

function duplicateKeyName(err) {
  const message = String(err?.sqlMessage || err?.message || "");
  const match = message.match(/for key ['`"]([^'"`]+)['`"]/i);
  return match?.[1] || "";
}

function conflictError(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function maskConnectorCredentials(row) {
  const envelope = parseEnvelopeText(row?.credentials_encrypted_json);
  return {
    has_credentials: Boolean(envelope),
    credentials_key_version: row?.credentials_key_version || envelope?.kid || null,
  };
}

function normalizeConnectorRow(row) {
  if (!row) return null;
  const encryptedCredentialsText =
    row?.credentials_encrypted_json === undefined ? null : row.credentials_encrypted_json;
  const normalized = {
    ...row,
    config_json: parseJson(row.config_json, {}),
    ...maskConnectorCredentials(row),
  };
  Object.defineProperty(normalized, "__credentials_encrypted_json", {
    value: encryptedCredentialsText,
    enumerable: false,
    configurable: false,
    writable: false,
  });
  delete normalized.credentials_encrypted_json;
  return normalized;
}

function normalizeConnectorLinkRow(row) {
  if (!row) return null;
  return row;
}

function normalizeSyncRunRow(row) {
  if (!row) return null;
  return {
    ...row,
    payload_json: parseJson(row.payload_json, null),
  };
}

async function getConnectorById({ tenantId, connectorId, runQuery = query }) {
  const result = await runQuery(
    `SELECT *
     FROM bank_connectors
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, connectorId]
  );
  return normalizeConnectorRow(result.rows?.[0] || null);
}

async function getConnectorAccountLinks({ tenantId, legalEntityId, connectorId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        l.*,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        ba.currency_code AS bank_account_currency_code
     FROM bank_connector_account_links l
     JOIN bank_accounts ba
       ON ba.tenant_id = l.tenant_id
      AND ba.legal_entity_id = l.legal_entity_id
      AND ba.id = l.bank_account_id
     WHERE l.tenant_id = ?
       AND l.legal_entity_id = ?
       AND l.bank_connector_id = ?
     ORDER BY l.id ASC`,
    [tenantId, legalEntityId, connectorId]
  );
  return (result.rows || []).map(normalizeConnectorLinkRow);
}

async function getConnectorSyncRunById({ tenantId, legalEntityId, syncRunId, runQuery = query }) {
  const result = await runQuery(
    `SELECT *
     FROM bank_connector_sync_runs
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, syncRunId]
  );
  return normalizeSyncRunRow(result.rows?.[0] || null);
}

async function getConnectorSyncRunByRequestId({
  tenantId,
  legalEntityId,
  connectorId,
  requestId,
  runQuery = query,
}) {
  if (!requestId) return null;
  const result = await runQuery(
    `SELECT *
     FROM bank_connector_sync_runs
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND bank_connector_id = ?
       AND request_id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, connectorId, requestId]
  );
  return normalizeSyncRunRow(result.rows?.[0] || null);
}

async function getBankAccountForLink({ tenantId, bankAccountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, tenant_id, legal_entity_id, currency_code, is_active, code, name
     FROM bank_accounts
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, bankAccountId]
  );
  return result.rows?.[0] || null;
}

function decryptConnectorCredentials(row) {
  const envelope = parseEnvelopeText(
    row?.credentials_encrypted_json ?? row?.__credentials_encrypted_json
  );
  if (!envelope) return {};
  return decryptJson(envelope);
}

async function assertConnectorScope(req, connectorRow, assertScopeAccess) {
  if (!connectorRow) throw badRequest("Connector not found");
  assertScopeAccess(req, "legal_entity", connectorRow.legal_entity_id, "connectorId");
}

async function createConnectorSyncRun({
  connectorRow,
  requestId = null,
  fromDate = null,
  toDate = null,
  cursorBefore = null,
  userId = null,
}) {
  try {
    const insertResult = await query(
      `INSERT INTO bank_connector_sync_runs (
          tenant_id,
          legal_entity_id,
          bank_connector_id,
          run_type,
          status,
          request_id,
          window_from,
          window_to,
          cursor_before,
          triggered_by_user_id
        ) VALUES (?, ?, ?, 'STATEMENT_PULL', 'RUNNING', ?, ?, ?, ?, ?)`,
      [
        connectorRow.tenant_id,
        connectorRow.legal_entity_id,
        connectorRow.id,
        requestId || null,
        fromDate || null,
        toDate || null,
        cursorBefore || null,
        userId || null,
      ]
    );
    return parsePositiveInt(insertResult.rows?.insertId);
  } catch (err) {
    if (isDuplicateEntryError(err) && duplicateKeyName(err).includes("uk_bank_connector_sync_runs_request")) {
      return null;
    }
    throw err;
  }
}

async function updateConnectorSyncRunFinal({
  tenantId,
  legalEntityId,
  syncRunId,
  status,
  cursorAfter = null,
  fetchedCount = 0,
  importedCount = 0,
  duplicateCount = 0,
  skippedUnmappedCount = 0,
  errorCount = 0,
  payload = null,
  errorMessage = null,
}) {
  await query(
    `UPDATE bank_connector_sync_runs
     SET status = ?,
         finished_at = CURRENT_TIMESTAMP,
         cursor_after = ?,
         fetched_count = ?,
         imported_count = ?,
         duplicate_count = ?,
         skipped_unmapped_count = ?,
         error_count = ?,
         payload_json = ?,
         error_message = ?
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?`,
    [
      status,
      cursorAfter || null,
      Number(fetchedCount || 0),
      Number(importedCount || 0),
      Number(duplicateCount || 0),
      Number(skippedUnmappedCount || 0),
      Number(errorCount || 0),
      safeJson(payload),
      errorMessage || null,
      tenantId,
      legalEntityId,
      syncRunId,
    ]
  );
}

async function updateConnectorAfterSync({
  connectorRow,
  status,
  cursorAfter = null,
  userId = null,
  errorMessage = null,
}) {
  await query(
    `UPDATE bank_connectors
     SET last_sync_at = CURRENT_TIMESTAMP,
         last_success_at = CASE WHEN ? IN ('SUCCESS','PARTIAL') THEN CURRENT_TIMESTAMP ELSE last_success_at END,
         last_error_at = CASE WHEN ? = 'FAILED' THEN CURRENT_TIMESTAMP ELSE last_error_at END,
         last_error_message = CASE WHEN ? = 'FAILED' THEN ? ELSE NULL END,
         last_cursor = ?,
         status = CASE
           WHEN status = 'DRAFT' AND ? IN ('SUCCESS','PARTIAL') THEN 'ACTIVE'
           WHEN ? = 'FAILED' THEN 'ERROR'
           ELSE status
         END,
         next_sync_at = CASE
           WHEN sync_mode = 'SCHEDULED' AND sync_frequency_minutes IS NOT NULL
             THEN DATE_ADD(CURRENT_TIMESTAMP, INTERVAL sync_frequency_minutes MINUTE)
           ELSE next_sync_at
         END,
         updated_by_user_id = ?,
         updated_at = CURRENT_TIMESTAMP
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?`,
    [
      status,
      status,
      status,
      errorMessage || "Connector sync failed",
      cursorAfter || null,
      status,
      status,
      userId || null,
      connectorRow.tenant_id,
      connectorRow.legal_entity_id,
      connectorRow.id,
    ]
  );
}

async function insertConnectorSyncRunImport({
  tenantId,
  legalEntityId,
  syncRunId,
  bankAccountId,
  externalAccountId,
  bankStatementImportId,
  importRef,
  importedCount,
  duplicateCount,
}) {
  await query(
    `INSERT INTO bank_connector_sync_run_imports (
        tenant_id,
        legal_entity_id,
        bank_connector_sync_run_id,
        bank_account_id,
        external_account_id,
        bank_statement_import_id,
        import_ref,
        imported_count,
        duplicate_count
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      tenantId,
      legalEntityId,
      syncRunId,
      bankAccountId,
      externalAccountId,
      bankStatementImportId,
      importRef,
      Number(importedCount || 0),
      Number(duplicateCount || 0),
    ]
  );
}

export async function resolveBankConnectorScope(connectorId, tenantId) {
  const parsedConnectorId = parsePositiveInt(connectorId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedConnectorId || !parsedTenantId) return null;
  const row = await getConnectorById({ tenantId: parsedTenantId, connectorId: parsedConnectorId });
  if (!row) return null;
  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: parsePositiveInt(row.legal_entity_id),
  };
}

export async function listBankConnectorRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["c.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "c.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("c.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }
  if (filters.status) {
    conditions.push("c.status = ?");
    params.push(filters.status);
  }
  if (filters.providerCode) {
    conditions.push("c.provider_code = ?");
    params.push(filters.providerCode);
  }
  if (filters.q) {
    const like = `%${filters.q}%`;
    conditions.push("(c.connector_code LIKE ? OR c.connector_name LIKE ?)");
    params.push(like, like);
  }

  const whereSql = conditions.join(" AND ");
  const countResult = await query(
    `SELECT COUNT(*) AS total FROM bank_connectors c WHERE ${whereSql}`,
    params
  );
  const total = Number(countResult.rows?.[0]?.total || 0);
  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset =
    Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;

  const listResult = await query(
    `SELECT
        c.*,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name,
        COALESCE(link_counts.link_count, 0) AS account_link_count
     FROM bank_connectors c
     JOIN legal_entities le
       ON le.tenant_id = c.tenant_id
      AND le.id = c.legal_entity_id
     LEFT JOIN (
       SELECT tenant_id, legal_entity_id, bank_connector_id, COUNT(*) AS link_count
       FROM bank_connector_account_links
       GROUP BY tenant_id, legal_entity_id, bank_connector_id
     ) link_counts
       ON link_counts.tenant_id = c.tenant_id
      AND link_counts.legal_entity_id = c.legal_entity_id
      AND link_counts.bank_connector_id = c.id
     WHERE ${whereSql}
     ORDER BY c.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: (listResult.rows || []).map(normalizeConnectorRow),
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getBankConnectorByIdForTenant({
  req,
  tenantId,
  connectorId,
  assertScopeAccess,
}) {
  const connector = await getConnectorById({ tenantId, connectorId });
  await assertConnectorScope(req, connector, assertScopeAccess);
  const links = await getConnectorAccountLinks({
    tenantId,
    legalEntityId: connector.legal_entity_id,
    connectorId: connector.id,
  });
  return {
    connector,
    account_links: links,
  };
}

export async function createBankConnector({ req, input, assertScopeAccess }) {
  assertScopeAccess(req, "legal_entity", input.legalEntityId, "legalEntityId");
  let credentialsEnvelope = null;
  if (input.credentials && Object.keys(input.credentials).length > 0) {
    credentialsEnvelope = encryptJson(input.credentials);
  }

  try {
    const connectorId = await withTransaction(async (tx) => {
      const insertResult = await tx.query(
        `INSERT INTO bank_connectors (
            tenant_id,
            legal_entity_id,
            connector_code,
            connector_name,
            provider_code,
            connector_type,
            status,
            adapter_version,
            config_json,
            credentials_encrypted_json,
            credentials_key_version,
            sync_mode,
            sync_frequency_minutes,
            next_sync_at,
            created_by_user_id,
            updated_by_user_id
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          input.tenantId,
          input.legalEntityId,
          input.connectorCode,
          input.connectorName,
          input.providerCode,
          input.connectorType,
          input.status,
          input.adapterVersion || "v1",
          safeJson(input.config || {}),
          credentialsEnvelope ? serializeEnvelope(credentialsEnvelope) : null,
          credentialsEnvelope?.kid || null,
          input.syncMode || "MANUAL",
          input.syncFrequencyMinutes || null,
          input.nextSyncAt || null,
          input.userId || null,
          input.userId || null,
        ]
      );
      return parsePositiveInt(insertResult.rows?.insertId);
    });

    if (!connectorId) throw new Error("Failed to create bank connector");
    return getBankConnectorByIdForTenant({
      req,
      tenantId: input.tenantId,
      connectorId,
      assertScopeAccess,
    });
  } catch (err) {
    if (isDuplicateEntryError(err)) {
      throw conflictError("connectorCode already exists");
    }
    throw err;
  }
}

export async function updateBankConnectorById({ req, input, assertScopeAccess }) {
  const existing = await getConnectorById({ tenantId: input.tenantId, connectorId: input.connectorId });
  await assertConnectorScope(req, existing, assertScopeAccess);

  let credentialsEnvelope = null;
  if (Object.prototype.hasOwnProperty.call(input, "credentials") && input.credentials) {
    credentialsEnvelope = encryptJson(input.credentials);
  }

  const updates = [];
  const params = [];
  if (Object.prototype.hasOwnProperty.call(input, "connectorName")) {
    updates.push("connector_name = ?");
    params.push(input.connectorName || existing.connector_name);
  }
  if (Object.prototype.hasOwnProperty.call(input, "status")) {
    updates.push("status = ?");
    params.push(input.status || existing.status);
  }
  if (Object.prototype.hasOwnProperty.call(input, "connectorType")) {
    updates.push("connector_type = ?");
    params.push(input.connectorType || existing.connector_type);
  }
  if (Object.prototype.hasOwnProperty.call(input, "config")) {
    updates.push("config_json = ?");
    params.push(safeJson(input.config || {}));
  }
  if (Object.prototype.hasOwnProperty.call(input, "credentials")) {
    updates.push("credentials_encrypted_json = ?");
    updates.push("credentials_key_version = ?");
    params.push(credentialsEnvelope ? serializeEnvelope(credentialsEnvelope) : null);
    params.push(credentialsEnvelope?.kid || null);
  }
  if (Object.prototype.hasOwnProperty.call(input, "syncMode")) {
    updates.push("sync_mode = ?");
    params.push(input.syncMode || existing.sync_mode);
  }
  if (Object.prototype.hasOwnProperty.call(input, "syncFrequencyMinutes")) {
    updates.push("sync_frequency_minutes = ?");
    params.push(input.syncFrequencyMinutes || null);
  }
  if (Object.prototype.hasOwnProperty.call(input, "nextSyncAt")) {
    updates.push("next_sync_at = ?");
    params.push(input.nextSyncAt || null);
  }
  if (updates.length === 0) {
    return getBankConnectorByIdForTenant({
      req,
      tenantId: input.tenantId,
      connectorId: input.connectorId,
      assertScopeAccess,
    });
  }
  updates.push("updated_by_user_id = ?");
  params.push(input.userId || null);
  params.push(input.tenantId, input.connectorId);

  await query(
    `UPDATE bank_connectors
     SET ${updates.join(", ")}
     WHERE tenant_id = ?
       AND id = ?`,
    params
  );

  return getBankConnectorByIdForTenant({
    req,
    tenantId: input.tenantId,
    connectorId: input.connectorId,
    assertScopeAccess,
  });
}

export async function upsertBankConnectorAccountLink({ req, input, assertScopeAccess }) {
  const connector = await getConnectorById({ tenantId: input.tenantId, connectorId: input.connectorId });
  await assertConnectorScope(req, connector, assertScopeAccess);

  const bankAccount = await getBankAccountForLink({
    tenantId: input.tenantId,
    bankAccountId: input.bankAccountId,
  });
  if (!bankAccount) throw badRequest("bankAccountId not found");
  if (parsePositiveInt(bankAccount.legal_entity_id) !== parsePositiveInt(connector.legal_entity_id)) {
    throw badRequest("bankAccountId must belong to the same legal entity as connector");
  }
  assertScopeAccess(req, "legal_entity", bankAccount.legal_entity_id, "bankAccountId");
  if (u(bankAccount.currency_code) !== u(input.externalCurrencyCode)) {
    throw badRequest(
      `Currency mismatch: bank account ${bankAccount.currency_code}, external ${input.externalCurrencyCode}`
    );
  }

  await withTransaction(async (tx) => {
    const existingResult = await tx.query(
      `SELECT id
       FROM bank_connector_account_links
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND bank_connector_id = ?
         AND external_account_id = ?
       LIMIT 1`,
      [
        input.tenantId,
        connector.legal_entity_id,
        connector.id,
        input.externalAccountId,
      ]
    );
    const existingId = parsePositiveInt(existingResult.rows?.[0]?.id);
    if (existingId) {
      await tx.query(
        `UPDATE bank_connector_account_links
         SET external_account_name = ?,
             external_currency_code = ?,
             bank_account_id = ?,
             status = ?,
             updated_by_user_id = ?
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND id = ?`,
        [
          input.externalAccountName || null,
          input.externalCurrencyCode,
          input.bankAccountId,
          input.status,
          input.userId || null,
          input.tenantId,
          connector.legal_entity_id,
          existingId,
        ]
      );
    } else {
      await tx.query(
        `INSERT INTO bank_connector_account_links (
            tenant_id,
            legal_entity_id,
            bank_connector_id,
            external_account_id,
            external_account_name,
            external_currency_code,
            bank_account_id,
            status,
            created_by_user_id,
            updated_by_user_id
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          input.tenantId,
          connector.legal_entity_id,
          connector.id,
          input.externalAccountId,
          input.externalAccountName || null,
          input.externalCurrencyCode,
          input.bankAccountId,
          input.status,
          input.userId || null,
          input.userId || null,
        ]
      );
    }
  });

  const links = await getConnectorAccountLinks({
    tenantId: input.tenantId,
    legalEntityId: connector.legal_entity_id,
    connectorId: connector.id,
  });
  return {
    connector: await getConnectorById({ tenantId: input.tenantId, connectorId: connector.id }),
    account_links: links,
  };
}

export async function listBankConnectorSyncRuns({
  req,
  tenantId,
  connectorId,
  limit = 50,
  offset = 0,
  assertScopeAccess,
}) {
  const connector = await getConnectorById({ tenantId, connectorId });
  await assertConnectorScope(req, connector, assertScopeAccess);
  const safeLimit = Number.isInteger(limit) && limit > 0 ? Math.min(limit, 200) : 50;
  const safeOffset = Number.isInteger(offset) && offset >= 0 ? offset : 0;

  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM bank_connector_sync_runs
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND bank_connector_id = ?`,
    [tenantId, connector.legal_entity_id, connector.id]
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const listResult = await query(
    `SELECT *
     FROM bank_connector_sync_runs
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND bank_connector_id = ?
     ORDER BY started_at DESC, id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    [tenantId, connector.legal_entity_id, connector.id]
  );

  return {
    connector,
    rows: (listResult.rows || []).map(normalizeSyncRunRow),
    total,
    limit: safeLimit,
    offset: safeOffset,
  };
}

export async function testBankConnectorConnection({
  req,
  tenantId,
  connectorId,
  assertScopeAccess,
}) {
  const connector = await getConnectorById({ tenantId, connectorId });
  await assertConnectorScope(req, connector, assertScopeAccess);
  const adapter = getBankConnectorAdapter(connector.provider_code);
  const result = await adapter.testConnection({
    config: connector.config_json || {},
    credentials: decryptConnectorCredentials(connector),
  });
  return {
    connector,
    result,
  };
}

async function listConnectorSyncRunImports({
  tenantId,
  legalEntityId,
  syncRunId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
        s.*,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        ba.currency_code AS bank_account_currency_code
     FROM bank_connector_sync_run_imports s
     LEFT JOIN bank_accounts ba
       ON ba.tenant_id = s.tenant_id
      AND ba.legal_entity_id = s.legal_entity_id
      AND ba.id = s.bank_account_id
     WHERE s.tenant_id = ?
       AND s.legal_entity_id = ?
       AND s.bank_connector_sync_run_id = ?
     ORDER BY s.id ASC`,
    [tenantId, legalEntityId, syncRunId]
  );
  return result.rows || [];
}

function normalizeConnectorAccountPayload(account, index) {
  if (!account || typeof account !== "object") {
    throw badRequest(`Adapter account ${index + 1} is invalid`);
  }
  const externalAccountId = String(
    account.external_account_id ?? account.externalAccountId ?? ""
  ).trim();
  if (!externalAccountId) {
    throw badRequest(`Adapter account ${index + 1} missing external_account_id`);
  }

  const lines = Array.isArray(account.lines) ? account.lines : [];
  const currencyCode = String(account.currency_code ?? account.currencyCode ?? "")
    .trim()
    .toUpperCase();

  return {
    external_account_id: externalAccountId,
    account_name: String(account.account_name ?? account.accountName ?? "").trim() || null,
    currency_code: currencyCode || null,
    lines,
  };
}

async function executeBankConnectorStatementSync({
  tenantId,
  connectorId,
  userId = null,
  fromDate = null,
  toDate = null,
  requestId = null,
  forceFull = false,
}) {
  const connector = await getConnectorById({ tenantId, connectorId });
  if (!connector) {
    throw badRequest("Connector not found");
  }

  if (u(connector.status) === "DISABLED") {
    throw badRequest("Connector is disabled");
  }

  const existingByRequest = await getConnectorSyncRunByRequestId({
    tenantId,
    legalEntityId: connector.legal_entity_id,
    connectorId: connector.id,
    requestId,
  });
  if (existingByRequest) {
    const imports = await listConnectorSyncRunImports({
      tenantId,
      legalEntityId: connector.legal_entity_id,
      syncRunId: existingByRequest.id,
    });
    return {
      connector: await getConnectorById({ tenantId, connectorId }),
      sync_run: existingByRequest,
      imports,
      idempotent: true,
    };
  }

  const syncRunId = await createConnectorSyncRun({
    connectorRow: connector,
    requestId,
    fromDate,
    toDate,
    cursorBefore: forceFull ? null : connector.last_cursor || null,
    userId,
  });

  if (!syncRunId) {
    // Race on duplicate request_id after pre-check
    const dup = await getConnectorSyncRunByRequestId({
      tenantId,
      legalEntityId: connector.legal_entity_id,
      connectorId: connector.id,
      requestId,
    });
    if (dup) {
      const imports = await listConnectorSyncRunImports({
        tenantId,
        legalEntityId: connector.legal_entity_id,
        syncRunId: dup.id,
      });
      return {
        connector: await getConnectorById({ tenantId, connectorId }),
        sync_run: dup,
        imports,
        idempotent: true,
      };
    }
    throw new Error("Failed to create connector sync run");
  }

  const adapter = getBankConnectorAdapter(connector.provider_code);
  const credentials = decryptConnectorCredentials(connector);
  const links = await getConnectorAccountLinks({
    tenantId,
    legalEntityId: connector.legal_entity_id,
    connectorId: connector.id,
  });
  const activeLinkByExternalAccount = new Map();
  for (const link of links) {
    if (u(link.status) !== "ACTIVE") continue;
    const key = u(link.external_account_id);
    if (!key) continue;
    activeLinkByExternalAccount.set(key, link);
  }

  let cursorAfter = forceFull ? null : connector.last_cursor || null;
  let fetchedCount = 0;
  let importedCount = 0;
  let duplicateCount = 0;
  let skippedUnmappedCount = 0;
  let errorCount = 0;
  const importRows = [];
  const errorItems = [];

  try {
    const adapterRes = await adapter.pullStatements({
      config: connector.config_json || {},
      credentials,
      cursor: forceFull ? null : connector.last_cursor || null,
      fromDate,
      toDate,
    });

    const accountPayloads = Array.isArray(adapterRes?.accounts)
      ? adapterRes.accounts.map(normalizeConnectorAccountPayload)
      : [];
    cursorAfter =
      adapterRes?.next_cursor === undefined
        ? cursorAfter
        : String(adapterRes?.next_cursor || "").trim() || null;

    for (let i = 0; i < accountPayloads.length; i += 1) {
      const account = normalizeConnectorAccountPayload(accountPayloads[i], i);
      const rawLines = Array.isArray(account.lines) ? account.lines : [];
      fetchedCount += rawLines.length;

      if (rawLines.length === 0) {
        continue;
      }

      const link = activeLinkByExternalAccount.get(u(account.external_account_id));
      if (!link) {
        skippedUnmappedCount += rawLines.length;
        errorItems.push({
          type: "UNMAPPED_EXTERNAL_ACCOUNT",
          external_account_id: account.external_account_id,
          line_count: rawLines.length,
        });
        continue;
      }

      const sourceRef = [
        "B05",
        `CONN:${connector.id}`,
        `RUN:${syncRunId}`,
        `EXT:${account.external_account_id}`,
      ].join("|");

      const normalizedLines = rawLines.map((line) => ({
        ...line,
        currency_code: line?.currency_code ?? line?.currencyCode ?? account.currency_code,
      }));

      try {
        const importResult = await importNormalizedBankStatementLines({
          payload: {
            tenantId,
            bankAccountId: link.bank_account_id,
            userId: userId || null,
            importSource: "API",
            sourceRef,
            sourceFilename: `${connector.connector_code || "connector"}-${account.external_account_id}.normalized`,
            sourceMeta: {
              source: "B05_CONNECTOR",
              connector_id: connector.id,
              connector_code: connector.connector_code,
              provider_code: connector.provider_code,
              sync_run_id: syncRunId,
              external_account_id: account.external_account_id,
              request_id: requestId || null,
              from_date: fromDate || null,
              to_date: toDate || null,
            },
            lines: normalizedLines,
          },
        });

        importedCount += Number(importResult.imported_count || 0);
        duplicateCount += Number(importResult.duplicate_count || 0);
        await insertConnectorSyncRunImport({
          tenantId,
          legalEntityId: connector.legal_entity_id,
          syncRunId,
          bankAccountId: link.bank_account_id,
          externalAccountId: account.external_account_id,
          bankStatementImportId: importResult.import_id,
          importRef: importResult.import_ref || sourceRef,
          importedCount: importResult.imported_count || 0,
          duplicateCount: importResult.duplicate_count || 0,
        });
        importRows.push({
          external_account_id: account.external_account_id,
          bank_account_id: link.bank_account_id,
          bank_statement_import_id: importResult.import_id,
          import_ref: importResult.import_ref || sourceRef,
          imported_count: Number(importResult.imported_count || 0),
          duplicate_count: Number(importResult.duplicate_count || 0),
        });
      } catch (err) {
        // B02 checksum conflict => duplicate import batch for same normalized payload
        if (Number(err?.status) === 409) {
          duplicateCount += rawLines.length;
          errorItems.push({
            type: "DUPLICATE_IMPORT_BATCH",
            external_account_id: account.external_account_id,
            line_count: rawLines.length,
            message: err?.message || "Duplicate normalized import batch",
          });
          continue;
        }
        errorCount += 1;
        errorItems.push({
          type: "ACCOUNT_IMPORT_ERROR",
          external_account_id: account.external_account_id,
          line_count: rawLines.length,
          message: String(err?.message || "Failed to import normalized statement lines"),
        });
      }
    }

    const finalStatus =
      errorCount > 0 || skippedUnmappedCount > 0 ? "PARTIAL" : "SUCCESS";

    await updateConnectorSyncRunFinal({
      tenantId,
      legalEntityId: connector.legal_entity_id,
      syncRunId,
      status: finalStatus,
      cursorAfter,
      fetchedCount,
      importedCount,
      duplicateCount,
      skippedUnmappedCount,
      errorCount,
      payload: {
        provider_code: connector.provider_code,
        account_count: accountPayloads.length,
        imported_batches: importRows.length,
        errors: errorItems.slice(0, 50),
      },
      errorMessage:
        finalStatus === "PARTIAL"
          ? errorItems[0]?.message || "Connector sync partially completed"
          : null,
    });

    await updateConnectorAfterSync({
      connectorRow: connector,
      status: finalStatus,
      cursorAfter,
      userId,
      errorMessage: errorItems[0]?.message || null,
    });

    const syncRun = await getConnectorSyncRunById({
      tenantId,
      legalEntityId: connector.legal_entity_id,
      syncRunId,
    });
    const imports = await listConnectorSyncRunImports({
      tenantId,
      legalEntityId: connector.legal_entity_id,
      syncRunId,
    });

    return {
      connector: await getConnectorById({ tenantId, connectorId }),
      sync_run: syncRun,
      imports,
      idempotent: false,
    };
  } catch (err) {
    await updateConnectorSyncRunFinal({
      tenantId,
      legalEntityId: connector.legal_entity_id,
      syncRunId,
      status: "FAILED",
      cursorAfter,
      fetchedCount,
      importedCount,
      duplicateCount,
      skippedUnmappedCount,
      errorCount: errorCount + 1,
      payload: {
        provider_code: connector.provider_code,
        imported_batches: importRows.length,
        errors: [
          ...errorItems.slice(0, 25),
          {
            type: "SYNC_FATAL",
            message: String(err?.message || "Connector sync failed"),
          },
        ],
      },
      errorMessage: String(err?.message || "Connector sync failed"),
    });

    await updateConnectorAfterSync({
      connectorRow: connector,
      status: "FAILED",
      cursorAfter,
      userId,
      errorMessage: String(err?.message || "Connector sync failed"),
    });
    throw err;
  }
}

export async function runBankConnectorStatementSync({
  req = null,
  tenantId,
  connectorId,
  userId = null,
  input = {},
  assertScopeAccess = null,
}) {
  const connector = await getConnectorById({ tenantId, connectorId });
  if (!connector) {
    throw badRequest("Connector not found");
  }
  if (req && typeof assertScopeAccess === "function") {
    await assertConnectorScope(req, connector, assertScopeAccess);
  }
  return executeBankConnectorStatementSync({
    tenantId,
    connectorId: connector.id,
    userId: userId || null,
    fromDate: input.fromDate || null,
    toDate: input.toDate || null,
    requestId: input.requestId || null,
    forceFull: Boolean(input.forceFull),
  });
}

export async function syncDueBankConnectors({
  tenantId = null,
  limit = 20,
  userId = null,
} = {}) {
  const conditions = [
    "status IN ('ACTIVE','ERROR')",
    "sync_mode = 'SCHEDULED'",
    "next_sync_at IS NOT NULL",
    "next_sync_at <= CURRENT_TIMESTAMP",
  ];
  const params = [];
  if (parsePositiveInt(tenantId)) {
    conditions.push("tenant_id = ?");
    params.push(parsePositiveInt(tenantId));
  }
  const safeLimit = Number.isInteger(limit) && limit > 0 ? Math.min(limit, 100) : 20;

  const dueRows = await query(
    `SELECT tenant_id, id AS connector_id
     FROM bank_connectors
     WHERE ${conditions.join(" AND ")}
     ORDER BY next_sync_at ASC, id ASC
     LIMIT ${safeLimit}`,
    params
  );

  const results = [];
  for (const row of dueRows.rows || []) {
    const rowTenantId = parsePositiveInt(row.tenant_id);
    const rowConnectorId = parsePositiveInt(row.connector_id);
    if (!rowTenantId || !rowConnectorId) continue;
    try {
      const minuteBucket = new Date().toISOString().slice(0, 16);
      const result = await executeBankConnectorStatementSync({
        tenantId: rowTenantId,
        connectorId: rowConnectorId,
        userId: userId || null,
        requestId: `AUTOSYNC|T:${rowTenantId}|C:${rowConnectorId}|${minuteBucket}`,
        forceFull: false,
      });
      results.push({
        tenant_id: rowTenantId,
        connector_id: rowConnectorId,
        ok: true,
        idempotent: Boolean(result.idempotent),
        sync_run_id: parsePositiveInt(result?.sync_run?.id) || null,
        sync_status: result?.sync_run?.status || null,
      });
    } catch (err) {
      results.push({
        tenant_id: rowTenantId,
        connector_id: rowConnectorId,
        ok: false,
        error: String(err?.message || "Connector scheduled sync failed"),
      });
    }
  }
  return results;
}

export default {
  resolveBankConnectorScope,
  listBankConnectorRows,
  getBankConnectorByIdForTenant,
  createBankConnector,
  updateBankConnectorById,
  upsertBankConnectorAccountLink,
  listBankConnectorSyncRuns,
  testBankConnectorConnection,
  runBankConnectorStatementSync,
  syncDueBankConnectors,
};
