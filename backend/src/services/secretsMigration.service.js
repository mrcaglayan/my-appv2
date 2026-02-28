import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  decryptJson,
  encryptJson,
  parseEnvelopeText,
  serializeEnvelope,
} from "../utils/cryptoEnvelope.js";

const MIGRATION_MODES = new Set(["BACKFILL", "REENCRYPT", "BOTH"]);

function up(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function parseJsonMaybe(value) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function toBool(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  if (value === true || value === false) return value;
  const text = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(text)) return true;
  if (["0", "false", "no", "off"].includes(text)) return false;
  return fallback;
}

function normalizeLimit(value, fallback = 200, maxLimit = 2000) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, maxLimit);
}

function truncate(value, maxLen) {
  const text = String(value ?? "");
  return text.length > maxLen ? text.slice(0, maxLen) : text;
}

function normalizeMode(value) {
  const mode = up(value || "BOTH");
  if (!MIGRATION_MODES.has(mode)) {
    throw badRequest(`mode must be one of: ${Array.from(MIGRATION_MODES).join(", ")}`);
  }
  return mode;
}

function requireActiveKeyVersion() {
  const keyVersion = String(process.env.APP_ENCRYPTION_ACTIVE_KEY_VERSION || "").trim();
  if (!keyVersion) {
    throw new Error("APP_ENCRYPTION_ACTIVE_KEY_VERSION is required");
  }
  return keyVersion;
}

function parseLegacySecretObject(rawValue) {
  const parsed = parseJsonMaybe(rawValue);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return null;
  }
  return parsed;
}

function makeErrorRow(row, err, step) {
  return {
    step,
    object_id: parsePositiveInt(row?.id) || null,
    tenant_id: parsePositiveInt(row?.tenant_id) || null,
    legal_entity_id: parsePositiveInt(row?.legal_entity_id) || null,
    message: truncate(String(err?.message || err || "Migration error"), 240),
  };
}

async function writeSensitiveDataAudit({
  tenantId,
  legalEntityId = null,
  moduleCode,
  objectType,
  objectId,
  action,
  payload = null,
  note = null,
  userId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO sensitive_data_audit (
        tenant_id,
        legal_entity_id,
        module_code,
        object_type,
        object_id,
        action,
        payload_json,
        note,
        acted_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      parsePositiveInt(tenantId),
      parsePositiveInt(legalEntityId),
      up(moduleCode),
      up(objectType),
      parsePositiveInt(objectId),
      up(action),
      payload === null ? null : safeJson(payload),
      note ? truncate(note, 500) : null,
      parsePositiveInt(userId),
    ]
  );
}

async function listLegacyPayrollProviderCandidates({
  tenantId = null,
  legalEntityId = null,
  limit = 200,
  runQuery = query,
}) {
  const params = [];
  const conditions = [
    "c.secrets_json IS NOT NULL",
    "(c.secrets_encrypted_json IS NULL OR c.secrets_encrypted_json = '')",
  ];
  if (parsePositiveInt(tenantId)) {
    conditions.push("c.tenant_id = ?");
    params.push(parsePositiveInt(tenantId));
  }
  if (parsePositiveInt(legalEntityId)) {
    conditions.push("c.legal_entity_id = ?");
    params.push(parsePositiveInt(legalEntityId));
  }

  const res = await runQuery(
    `SELECT
        c.id,
        c.tenant_id,
        c.legal_entity_id,
        c.provider_code,
        c.status,
        c.secrets_json,
        c.secrets_encrypted_json,
        c.secrets_key_version
     FROM payroll_provider_connections c
     WHERE ${conditions.join(" AND ")}
     ORDER BY c.id ASC
     LIMIT ${normalizeLimit(limit)}`,
    params
  );
  return res.rows || [];
}

async function listPayrollProviderEncryptedCandidates({
  tenantId = null,
  legalEntityId = null,
  limit = 200,
  activeKid = null,
  forceReencrypt = false,
  runQuery = query,
}) {
  const params = [];
  const conditions = ["c.secrets_encrypted_json IS NOT NULL", "c.secrets_encrypted_json != ''"];
  if (parsePositiveInt(tenantId)) {
    conditions.push("c.tenant_id = ?");
    params.push(parsePositiveInt(tenantId));
  }
  if (parsePositiveInt(legalEntityId)) {
    conditions.push("c.legal_entity_id = ?");
    params.push(parsePositiveInt(legalEntityId));
  }
  if (!forceReencrypt && activeKid) {
    conditions.push("(c.secrets_key_version IS NULL OR c.secrets_key_version != ?)");
    params.push(activeKid);
  }

  const res = await runQuery(
    `SELECT
        c.id,
        c.tenant_id,
        c.legal_entity_id,
        c.provider_code,
        c.status,
        c.secrets_encrypted_json,
        c.secrets_key_version
     FROM payroll_provider_connections c
     WHERE ${conditions.join(" AND ")}
     ORDER BY c.id ASC
     LIMIT ${normalizeLimit(limit)}`,
    params
  );
  return res.rows || [];
}

async function listBankConnectorEncryptedCandidates({
  tenantId = null,
  legalEntityId = null,
  limit = 200,
  activeKid = null,
  forceReencrypt = false,
  runQuery = query,
}) {
  const params = [];
  const conditions = ["c.credentials_encrypted_json IS NOT NULL", "c.credentials_encrypted_json != ''"];
  if (parsePositiveInt(tenantId)) {
    conditions.push("c.tenant_id = ?");
    params.push(parsePositiveInt(tenantId));
  }
  if (parsePositiveInt(legalEntityId)) {
    conditions.push("c.legal_entity_id = ?");
    params.push(parsePositiveInt(legalEntityId));
  }
  if (!forceReencrypt && activeKid) {
    conditions.push("(c.credentials_key_version IS NULL OR c.credentials_key_version != ?)");
    params.push(activeKid);
  }

  const res = await runQuery(
    `SELECT
        c.id,
        c.tenant_id,
        c.legal_entity_id,
        c.connector_code,
        c.provider_code,
        c.status,
        c.credentials_encrypted_json,
        c.credentials_key_version
     FROM bank_connectors c
     WHERE ${conditions.join(" AND ")}
     ORDER BY c.id ASC
     LIMIT ${normalizeLimit(limit)}`,
    params
  );
  return res.rows || [];
}

async function backfillPayrollProviderConnectionRow({ row, userId = null }) {
  return withTransaction(async (tx) => {
    const lockedRes = await tx.query(
      `SELECT *
       FROM payroll_provider_connections
       WHERE tenant_id = ?
         AND id = ?
       LIMIT 1
       FOR UPDATE`,
      [parsePositiveInt(row?.tenant_id), parsePositiveInt(row?.id)]
    );
    const locked = lockedRes.rows?.[0] || null;
    if (!locked) return { skipped: true, reason: "MISSING" };
    if (!locked.secrets_json) return { skipped: true, reason: "NO_LEGACY_SECRET" };
    if (locked.secrets_encrypted_json) return { skipped: true, reason: "ALREADY_ENCRYPTED" };

    const legacySecretObj = parseLegacySecretObject(locked.secrets_json);
    if (!legacySecretObj) {
      throw badRequest(`Invalid legacy secrets_json for payroll_provider_connections.id=${locked.id}`);
    }

    const envelope = encryptJson(legacySecretObj);
    await tx.query(
      `UPDATE payroll_provider_connections
       SET secrets_json = NULL,
           secrets_encrypted_json = ?,
           secrets_key_version = ?,
           secrets_migrated_at = CURRENT_TIMESTAMP,
           updated_by_user_id = COALESCE(?, updated_by_user_id),
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [
        serializeEnvelope(envelope),
        String(envelope.kid || "").trim() || null,
        parsePositiveInt(userId),
        parsePositiveInt(locked.tenant_id),
        parsePositiveInt(locked.id),
      ]
    );

    await writeSensitiveDataAudit({
      tenantId: locked.tenant_id,
      legalEntityId: locked.legal_entity_id,
      moduleCode: "PAYROLL",
      objectType: "PROVIDER_CONNECTION",
      objectId: locked.id,
      action: "BACKFILL_ENCRYPTED",
      payload: {
        provider_code: up(locked.provider_code),
        status: up(locked.status),
        key_version: String(envelope.kid || "").trim() || null,
        secret_field_count: Object.keys(legacySecretObj).length,
      },
      note: "Legacy provider secrets migrated to encrypted envelope",
      userId,
      runQuery: tx.query,
    });

    return {
      skipped: false,
      keyVersion: String(envelope.kid || "").trim() || null,
    };
  });
}

async function reencryptPayrollProviderConnectionRow({ row, userId = null, forceReencrypt = false }) {
  return withTransaction(async (tx) => {
    const lockedRes = await tx.query(
      `SELECT *
       FROM payroll_provider_connections
       WHERE tenant_id = ?
         AND id = ?
       LIMIT 1
       FOR UPDATE`,
      [parsePositiveInt(row?.tenant_id), parsePositiveInt(row?.id)]
    );
    const locked = lockedRes.rows?.[0] || null;
    if (!locked) return { skipped: true, reason: "MISSING" };
    if (!locked.secrets_encrypted_json) return { skipped: true, reason: "NO_ENCRYPTED_SECRET" };

    const oldEnvelope = parseEnvelopeText(locked.secrets_encrypted_json);
    if (!oldEnvelope) {
      throw badRequest(`Invalid secrets_encrypted_json envelope for payroll_provider_connections.id=${locked.id}`);
    }

    const oldKid = String(oldEnvelope.kid || "").trim() || null;
    const plainSecretObj = decryptJson(oldEnvelope);
    if (!plainSecretObj || typeof plainSecretObj !== "object" || Array.isArray(plainSecretObj)) {
      throw badRequest(`Decrypted provider secrets are invalid for payroll_provider_connections.id=${locked.id}`);
    }

    const newEnvelope = encryptJson(plainSecretObj);
    const newKid = String(newEnvelope.kid || "").trim() || null;

    if (!forceReencrypt && oldKid && newKid && oldKid === newKid) {
      return { skipped: true, reason: "ALREADY_ACTIVE_KEY" };
    }

    await tx.query(
      `UPDATE payroll_provider_connections
       SET secrets_encrypted_json = ?,
           secrets_key_version = ?,
           secrets_migrated_at = CURRENT_TIMESTAMP,
           secrets_json = CASE WHEN secrets_json IS NOT NULL THEN NULL ELSE secrets_json END,
           updated_by_user_id = COALESCE(?, updated_by_user_id),
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [
        serializeEnvelope(newEnvelope),
        newKid,
        parsePositiveInt(userId),
        parsePositiveInt(locked.tenant_id),
        parsePositiveInt(locked.id),
      ]
    );

    await writeSensitiveDataAudit({
      tenantId: locked.tenant_id,
      legalEntityId: locked.legal_entity_id,
      moduleCode: "PAYROLL",
      objectType: "PROVIDER_CONNECTION",
      objectId: locked.id,
      action: "REENCRYPTED_SECRET",
      payload: {
        provider_code: up(locked.provider_code),
        status: up(locked.status),
        from_key_version: oldKid,
        to_key_version: newKid,
      },
      note: "Provider secrets re-encrypted with active key version",
      userId,
      runQuery: tx.query,
    });

    return {
      skipped: false,
      fromKeyVersion: oldKid,
      toKeyVersion: newKid,
    };
  });
}

async function reencryptBankConnectorRow({ row, userId = null, forceReencrypt = false }) {
  return withTransaction(async (tx) => {
    const lockedRes = await tx.query(
      `SELECT *
       FROM bank_connectors
       WHERE tenant_id = ?
         AND id = ?
       LIMIT 1
       FOR UPDATE`,
      [parsePositiveInt(row?.tenant_id), parsePositiveInt(row?.id)]
    );
    const locked = lockedRes.rows?.[0] || null;
    if (!locked) return { skipped: true, reason: "MISSING" };
    if (!locked.credentials_encrypted_json) return { skipped: true, reason: "NO_ENCRYPTED_SECRET" };

    const oldEnvelope = parseEnvelopeText(locked.credentials_encrypted_json);
    if (!oldEnvelope) {
      throw badRequest(`Invalid credentials_encrypted_json envelope for bank_connectors.id=${locked.id}`);
    }

    const oldKid = String(oldEnvelope.kid || "").trim() || null;
    const plainSecretObj = decryptJson(oldEnvelope);
    if (!plainSecretObj || typeof plainSecretObj !== "object" || Array.isArray(plainSecretObj)) {
      throw badRequest(`Decrypted connector credentials are invalid for bank_connectors.id=${locked.id}`);
    }

    const newEnvelope = encryptJson(plainSecretObj);
    const newKid = String(newEnvelope.kid || "").trim() || null;

    if (!forceReencrypt && oldKid && newKid && oldKid === newKid) {
      return { skipped: true, reason: "ALREADY_ACTIVE_KEY" };
    }

    await tx.query(
      `UPDATE bank_connectors
       SET credentials_encrypted_json = ?,
           credentials_key_version = ?,
           updated_by_user_id = COALESCE(?, updated_by_user_id),
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [
        serializeEnvelope(newEnvelope),
        newKid,
        parsePositiveInt(userId),
        parsePositiveInt(locked.tenant_id),
        parsePositiveInt(locked.id),
      ]
    );

    await writeSensitiveDataAudit({
      tenantId: locked.tenant_id,
      legalEntityId: locked.legal_entity_id,
      moduleCode: "BANK",
      objectType: "CONNECTOR",
      objectId: locked.id,
      action: "REENCRYPTED_SECRET",
      payload: {
        connector_code: String(locked.connector_code || "").trim() || null,
        provider_code: up(locked.provider_code),
        status: up(locked.status),
        from_key_version: oldKid,
        to_key_version: newKid,
      },
      note: "Connector credentials re-encrypted with active key version",
      userId,
      runQuery: tx.query,
    });

    return {
      skipped: false,
      fromKeyVersion: oldKid,
      toKeyVersion: newKid,
    };
  });
}

export async function getSecretsMigrationDependencySummary({
  tenantId = null,
  legalEntityId = null,
  runQuery = query,
} = {}) {
  const where = [];
  const params = [];
  if (parsePositiveInt(tenantId)) {
    where.push("c.tenant_id = ?");
    params.push(parsePositiveInt(tenantId));
  }
  if (parsePositiveInt(legalEntityId)) {
    where.push("c.legal_entity_id = ?");
    params.push(parsePositiveInt(legalEntityId));
  }

  const providerPlainConditions = [
    "c.status = 'ACTIVE'",
    "c.secrets_json IS NOT NULL",
    "(c.secrets_encrypted_json IS NULL OR c.secrets_encrypted_json = '')",
    ...where,
  ];
  const providerPlainRes = await runQuery(
    `SELECT COUNT(*) AS total
     FROM payroll_provider_connections c
     WHERE ${providerPlainConditions.join(" AND ")}`,
    params
  );

  const providerLegacyAnyConditions = [
    "c.secrets_json IS NOT NULL",
    "(c.secrets_encrypted_json IS NULL OR c.secrets_encrypted_json = '')",
    ...where,
  ];
  const providerLegacyAnyRes = await runQuery(
    `SELECT COUNT(*) AS total
     FROM payroll_provider_connections c
     WHERE ${providerLegacyAnyConditions.join(" AND ")}`,
    params
  );

  return {
    active_provider_plaintext_dependencies: Number(providerPlainRes.rows?.[0]?.total || 0),
    provider_plaintext_dependencies_total: Number(providerLegacyAnyRes.rows?.[0]?.total || 0),
  };
}

export async function runSecretsMigrationPass({
  tenantId = null,
  legalEntityId = null,
  userId = null,
  limit = 200,
  mode = "BOTH",
  dryRun = false,
  forceReencrypt = false,
} = {}) {
  const normalizedMode = normalizeMode(mode);
  const safeLimit = normalizeLimit(limit);
  const activeKidRaw = String(process.env.APP_ENCRYPTION_ACTIVE_KEY_VERSION || "").trim();
  const activeKid = activeKidRaw || null;

  const runBackfill = normalizedMode === "BACKFILL" || normalizedMode === "BOTH";
  const runReencrypt = normalizedMode === "REENCRYPT" || normalizedMode === "BOTH";
  const isDryRun = toBool(dryRun, false);

  if (!isDryRun) {
    requireActiveKeyVersion();
  }

  const legacyProviderRows = runBackfill
    ? await listLegacyPayrollProviderCandidates({
        tenantId,
        legalEntityId,
        limit: safeLimit,
      })
    : [];
  const providerEncryptedRows = runReencrypt
    ? await listPayrollProviderEncryptedCandidates({
        tenantId,
        legalEntityId,
        limit: safeLimit,
        activeKid,
        forceReencrypt: toBool(forceReencrypt, false),
      })
    : [];
  const bankEncryptedRows = runReencrypt
    ? await listBankConnectorEncryptedCandidates({
        tenantId,
        legalEntityId,
        limit: safeLimit,
        activeKid,
        forceReencrypt: toBool(forceReencrypt, false),
      })
    : [];

  const summary = {
    mode: normalizedMode,
    dry_run: isDryRun,
    tenant_id: parsePositiveInt(tenantId),
    legal_entity_id: parsePositiveInt(legalEntityId),
    active_key_version: activeKid,
    candidates: {
      payroll_provider_legacy: legacyProviderRows.length,
      payroll_provider_encrypted: providerEncryptedRows.length,
      bank_connector_encrypted: bankEncryptedRows.length,
    },
    results: {
      payroll_provider_backfilled: 0,
      payroll_provider_reencrypted: 0,
      bank_connector_reencrypted: 0,
      skipped: 0,
      errors: [],
    },
  };

  if (isDryRun) {
    summary.post_check = await getSecretsMigrationDependencySummary({
      tenantId,
      legalEntityId,
    });
    return summary;
  }

  if (runBackfill) {
    for (const row of legacyProviderRows) {
      try {
        // eslint-disable-next-line no-await-in-loop
        const result = await backfillPayrollProviderConnectionRow({
          row,
          userId,
        });
        if (result?.skipped) {
          summary.results.skipped += 1;
        } else {
          summary.results.payroll_provider_backfilled += 1;
        }
      } catch (err) {
        summary.results.errors.push(makeErrorRow(row, err, "PAYROLL_PROVIDER_BACKFILL"));
      }
    }
  }

  if (runReencrypt) {
    for (const row of providerEncryptedRows) {
      try {
        // eslint-disable-next-line no-await-in-loop
        const result = await reencryptPayrollProviderConnectionRow({
          row,
          userId,
          forceReencrypt: toBool(forceReencrypt, false),
        });
        if (result?.skipped) {
          summary.results.skipped += 1;
        } else {
          summary.results.payroll_provider_reencrypted += 1;
        }
      } catch (err) {
        summary.results.errors.push(makeErrorRow(row, err, "PAYROLL_PROVIDER_REENCRYPT"));
      }
    }

    for (const row of bankEncryptedRows) {
      try {
        // eslint-disable-next-line no-await-in-loop
        const result = await reencryptBankConnectorRow({
          row,
          userId,
          forceReencrypt: toBool(forceReencrypt, false),
        });
        if (result?.skipped) {
          summary.results.skipped += 1;
        } else {
          summary.results.bank_connector_reencrypted += 1;
        }
      } catch (err) {
        summary.results.errors.push(makeErrorRow(row, err, "BANK_CONNECTOR_REENCRYPT"));
      }
    }
  }

  summary.post_check = await getSecretsMigrationDependencySummary({
    tenantId,
    legalEntityId,
  });
  return summary;
}

export async function runSecretsMigrationUntilStable({
  tenantId = null,
  legalEntityId = null,
  userId = null,
  limit = 200,
  mode = "BOTH",
  forceReencrypt = false,
  maxPasses = 20,
} = {}) {
  const safeMaxPasses = normalizeLimit(maxPasses, 20, 200);
  const passes = [];
  for (let i = 0; i < safeMaxPasses; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    const pass = await runSecretsMigrationPass({
      tenantId,
      legalEntityId,
      userId,
      limit,
      mode,
      dryRun: false,
      forceReencrypt,
    });
    passes.push(pass);
    const moved =
      Number(pass?.results?.payroll_provider_backfilled || 0) +
      Number(pass?.results?.payroll_provider_reencrypted || 0) +
      Number(pass?.results?.bank_connector_reencrypted || 0);
    if (moved <= 0) {
      break;
    }
  }

  const totals = {
    payroll_provider_backfilled: 0,
    payroll_provider_reencrypted: 0,
    bank_connector_reencrypted: 0,
    skipped: 0,
    error_count: 0,
  };
  const errors = [];
  for (const pass of passes) {
    totals.payroll_provider_backfilled += Number(
      pass?.results?.payroll_provider_backfilled || 0
    );
    totals.payroll_provider_reencrypted += Number(
      pass?.results?.payroll_provider_reencrypted || 0
    );
    totals.bank_connector_reencrypted += Number(
      pass?.results?.bank_connector_reencrypted || 0
    );
    totals.skipped += Number(pass?.results?.skipped || 0);
    const passErrors = Array.isArray(pass?.results?.errors) ? pass.results.errors : [];
    totals.error_count += passErrors.length;
    errors.push(...passErrors);
  }

  const finalPostCheck = passes.length
    ? passes[passes.length - 1]?.post_check || null
    : await getSecretsMigrationDependencySummary({ tenantId, legalEntityId });

  return {
    mode: normalizeMode(mode),
    tenant_id: parsePositiveInt(tenantId),
    legal_entity_id: parsePositiveInt(legalEntityId),
    pass_count: passes.length,
    passes,
    totals,
    errors,
    post_check: finalPostCheck,
  };
}

export default {
  getSecretsMigrationDependencySummary,
  runSecretsMigrationPass,
  runSecretsMigrationUntilStable,
};
