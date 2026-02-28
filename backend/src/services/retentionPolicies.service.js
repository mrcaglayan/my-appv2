import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";

const POLICY_STATUSES = new Set(["ACTIVE", "PAUSED", "DISABLED"]);
const RUN_STATUSES = new Set(["RUNNING", "COMPLETED", "PARTIAL", "FAILED"]);
const RUN_TRIGGER_MODES = new Set(["MANUAL", "SCHEDULED", "JOB"]);

const DATASET_DEFINITIONS = {
  PAYROLL_PROVIDER_IMPORT_RAW: {
    allowedActions: new Set(["MASK", "PURGE"]),
    supportsLegalEntity: true,
  },
  BANK_FEED_RAW_PAYLOAD: {
    allowedActions: new Set(["MASK", "PURGE"]),
    supportsLegalEntity: true,
  },
  BANK_WEBHOOK_RAW_PAYLOAD: {
    allowedActions: new Set(["MASK", "PURGE"]),
    supportsLegalEntity: true,
  },
  JOB_EXECUTION_LOG: {
    allowedActions: new Set(["MASK", "PURGE"]),
    supportsLegalEntity: false,
  },
  SENSITIVE_DATA_AUDIT: {
    allowedActions: new Set(["ARCHIVE"]),
    supportsLegalEntity: true,
  },
};

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
    return value;
  }
}

function toDateOnly(value) {
  if (!value) return null;
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return value.toISOString().slice(0, 10);
  }
  const raw = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) {
    return raw.slice(0, 10);
  }
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

function addDays(date, days) {
  const next = new Date(date.getTime());
  next.setUTCDate(next.getUTCDate() + Number(days || 0));
  return next;
}

function truncate(value, maxLen) {
  const text = String(value ?? "");
  return text.length > maxLen ? text.slice(0, maxLen) : text;
}

function notFound(message) {
  const err = new Error(message);
  err.status = 404;
  return err;
}

function conflict(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}

function parseObjectMaybe(value, label) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value !== "object" || Array.isArray(value)) {
    throw badRequest(`${label} must be a JSON object`);
  }
  return value;
}

function isDuplicateError(err) {
  return Number(err?.errno) === 1062 || up(err?.code) === "ER_DUP_ENTRY";
}

function normalizeDatasetCode(value) {
  const datasetCode = up(value);
  if (!datasetCode || !DATASET_DEFINITIONS[datasetCode]) {
    throw badRequest(
      `datasetCode must be one of: ${Object.keys(DATASET_DEFINITIONS).join(", ")}`
    );
  }
  return datasetCode;
}

function normalizeActionCode(value, datasetCode) {
  const actionCode = up(value);
  const def = DATASET_DEFINITIONS[datasetCode];
  if (!actionCode || !def?.allowedActions?.has(actionCode)) {
    throw badRequest(
      `actionCode must be one of: ${Array.from(def?.allowedActions || []).join(", ")} for dataset ${datasetCode}`
    );
  }
  return actionCode;
}

function normalizePolicyStatus(value, fallback = "ACTIVE") {
  const status = up(value || fallback);
  if (!POLICY_STATUSES.has(status)) {
    throw badRequest(`status must be one of: ${Array.from(POLICY_STATUSES).join(", ")}`);
  }
  return status;
}

function normalizeRunStatus(value) {
  const status = up(value);
  if (!status) return null;
  if (!RUN_STATUSES.has(status)) {
    throw badRequest(`status must be one of: ${Array.from(RUN_STATUSES).join(", ")}`);
  }
  return status;
}

function normalizeRunTriggerMode(value, fallback = "MANUAL") {
  const triggerMode = up(value || fallback);
  if (!RUN_TRIGGER_MODES.has(triggerMode)) {
    throw badRequest(
      `triggerMode must be one of: ${Array.from(RUN_TRIGGER_MODES).join(", ")}`
    );
  }
  return triggerMode;
}

function normalizeRetentionDays(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 36500) {
    throw badRequest("retentionDays must be an integer between 1 and 36500");
  }
  return parsed;
}

function mapPolicyRow(row) {
  if (!row) return null;
  return {
    ...row,
    dataset_code: up(row.dataset_code),
    action_code: up(row.action_code),
    scope_type: up(row.scope_type),
    status: up(row.status),
    config_json: parseJsonMaybe(row.config_json),
  };
}

function mapRunRow(row) {
  if (!row) return null;
  return {
    ...row,
    trigger_mode: up(row.trigger_mode),
    status: up(row.status),
    retention_cutoff_date: toDateOnly(row.retention_cutoff_date),
    payload_json: parseJsonMaybe(row.payload_json),
  };
}

function computeCutoffDate(retentionDays) {
  const today = new Date();
  const todayUtc = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
  return toDateOnly(addDays(todayUtc, -Number(retentionDays || 0)));
}

function computeCutoffTimestamp(cutoffDate) {
  return `${cutoffDate} 00:00:00`;
}

function buildScopedLegalEntityClause({
  req,
  tenantId,
  alias,
  legalEntityId,
  params,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const conditions = [`${alias}.tenant_id = ?`];
  params.push(tenantId);

  const leId = parsePositiveInt(legalEntityId);
  if (leId) {
    if (typeof assertScopeAccess === "function") {
      assertScopeAccess(req, "legal_entity", leId, "legalEntityId");
    }
    conditions.push(`${alias}.legal_entity_id = ?`);
    params.push(leId);
  } else if (typeof buildScopeFilter === "function") {
    if (req?.rbac?.scopeContext?.tenantWide) {
      conditions.push("1 = 1");
    } else {
      conditions.push(buildScopeFilter(req, "legal_entity", `${alias}.legal_entity_id`, params));
    }
  }

  return conditions;
}

async function findPolicyRow({ tenantId, policyId, runQuery = query, forUpdate = false }) {
  const res = await runQuery(
    `SELECT p.*, le.code AS legal_entity_code, le.name AS legal_entity_name,
            cu.email AS created_by_user_email, uu.email AS updated_by_user_email
     FROM data_retention_policies p
     LEFT JOIN legal_entities le
       ON le.tenant_id = p.tenant_id
      AND le.id = p.legal_entity_id
     LEFT JOIN users cu
       ON cu.tenant_id = p.tenant_id
      AND cu.id = p.created_by_user_id
     LEFT JOIN users uu
       ON uu.tenant_id = p.tenant_id
      AND uu.id = p.updated_by_user_id
     WHERE p.tenant_id = ?
       AND p.id = ?
     LIMIT 1
     ${forUpdate ? "FOR UPDATE" : ""}`,
    [tenantId, policyId]
  );
  return mapPolicyRow(res.rows?.[0] || null);
}

async function findRunRow({ tenantId, runId, runQuery = query }) {
  const res = await runQuery(
    `SELECT r.*, p.policy_code, p.policy_name, p.dataset_code, p.action_code
     FROM data_retention_runs r
     JOIN data_retention_policies p
       ON p.tenant_id = r.tenant_id
      AND p.id = r.data_retention_policy_id
     WHERE r.tenant_id = ?
       AND r.id = ?
     LIMIT 1`,
    [tenantId, runId]
  );
  return mapRunRow(res.rows?.[0] || null);
}

async function writeSensitiveAuditLog({
  tenantId,
  legalEntityId,
  action,
  objectId,
  payload,
  note,
  userId,
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
     ) VALUES (?, ?, 'OPS', 'RETENTION_POLICY', ?, ?, ?, ?, ?)`,
    [
      tenantId,
      parsePositiveInt(legalEntityId) || null,
      parsePositiveInt(objectId) || 0,
      truncate(action || "RETENTION_RUN", 30),
      safeJson(payload),
      note ? truncate(note, 500) : null,
      parsePositiveInt(userId) || null,
    ]
  );
}

async function tableExists(tableName, runQuery = query) {
  const res = await runQuery(
    `SELECT 1 AS ok
     FROM information_schema.tables
     WHERE table_schema = DATABASE()
       AND table_name = ?
     LIMIT 1`,
    [tableName]
  );
  return Array.isArray(res.rows) && res.rows.length > 0;
}

async function tableHasColumns(tableName, requiredColumns = [], runQuery = query) {
  if (!requiredColumns.length) return true;
  const res = await runQuery(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = DATABASE()
       AND table_name = ?`,
    [tableName]
  );
  const present = new Set((res.rows || []).map((row) => String(row.column_name || "").trim().toLowerCase()));
  return requiredColumns.every((col) => present.has(String(col).trim().toLowerCase()));
}
async function executePayrollProviderImportRetention({
  tenantId,
  legalEntityId,
  policy,
  cutoffDate,
  cutoffTimestamp,
  runQuery,
  userId,
}) {
  const paramsBase = [tenantId];
  let scopeSql = "j.tenant_id = ?";
  if (parsePositiveInt(legalEntityId)) {
    scopeSql += " AND j.legal_entity_id = ?";
    paramsBase.push(parsePositiveInt(legalEntityId));
  }

  const scannedRes = await runQuery(
    `SELECT COUNT(*) AS total
     FROM payroll_provider_import_jobs j
     WHERE ${scopeSql}
       AND COALESCE(j.requested_at, j.created_at) < ?`,
    [...paramsBase, cutoffTimestamp]
  );
  const scannedRows = Number(scannedRes.rows?.[0]?.total || 0);

  let affectedRows = 0;
  let maskedRows = 0;
  let purgedRows = 0;

  const action = up(policy.action_code);
  if (action === "MASK") {
    const upd = await runQuery(
      `UPDATE payroll_provider_import_jobs j
       SET j.raw_payload_text = '[MASKED_BY_RETENTION_POLICY]',
           j.raw_payload_retention_status = 'MASKED',
           j.raw_payload_masked_at = CURRENT_TIMESTAMP,
           j.raw_payload_redaction_note = ?,
           j.updated_at = CURRENT_TIMESTAMP
       WHERE ${scopeSql}
         AND COALESCE(j.requested_at, j.created_at) < ?
         AND j.raw_payload_retention_status = 'ACTIVE'
         AND j.raw_payload_text IS NOT NULL`,
      [
        truncate(`Retention policy ${policy.policy_code} applied`, 500),
        ...paramsBase,
        cutoffTimestamp,
      ]
    );
    maskedRows = Number(upd.rows?.affectedRows || 0);
    affectedRows = maskedRows;
  } else if (action === "PURGE") {
    const upd = await runQuery(
      `UPDATE payroll_provider_import_jobs j
       SET j.raw_payload_text = NULL,
           j.raw_payload_retention_status = 'PURGED',
           j.raw_payload_purged_at = CURRENT_TIMESTAMP,
           j.raw_payload_redaction_note = ?,
           j.updated_at = CURRENT_TIMESTAMP
       WHERE ${scopeSql}
         AND COALESCE(j.requested_at, j.created_at) < ?
         AND j.raw_payload_retention_status <> 'PURGED'`,
      [
        truncate(`Retention policy ${policy.policy_code} applied`, 500),
        ...paramsBase,
        cutoffTimestamp,
      ]
    );
    purgedRows = Number(upd.rows?.affectedRows || 0);
    affectedRows = purgedRows;
  } else {
    throw badRequest(`Unsupported action for PAYROLL_PROVIDER_IMPORT_RAW: ${action}`);
  }

  await writeSensitiveAuditLog({
    tenantId,
    legalEntityId,
    action: action === "MASK" ? "RETENTION_MASK" : "RETENTION_PURGE",
    objectId: policy.id,
    payload: {
      dataset_code: policy.dataset_code,
      cutoff_date: cutoffDate,
      scanned_rows: scannedRows,
      affected_rows: affectedRows,
    },
    note: `Retention run for policy ${policy.policy_code}`,
    userId,
    runQuery,
  });

  return {
    status: "COMPLETED",
    scannedRows,
    affectedRows,
    maskedRows,
    purgedRows,
    archivedRows: 0,
    errorRows: 0,
    payload: {
      dataset_code: policy.dataset_code,
      action_code: action,
      cutoff_date: cutoffDate,
    },
    note: `Applied ${action} to payroll provider raw payloads`,
  };
}

async function executeGenericRawPayloadRetention({
  tenantId,
  legalEntityId,
  policy,
  cutoffDate,
  cutoffTimestamp,
  tableName,
  alias,
  runQuery,
  userId,
}) {
  const exists = await tableExists(tableName, runQuery);
  if (!exists) {
    return {
      status: "PARTIAL",
      scannedRows: 0,
      affectedRows: 0,
      maskedRows: 0,
      purgedRows: 0,
      archivedRows: 0,
      errorRows: 0,
      payload: {
        dataset_code: policy.dataset_code,
        table_name: tableName,
        reason: "TABLE_NOT_INSTALLED",
      },
      note: `${tableName} table is not installed in this environment`,
    };
  }

  const hasRequiredColumns = await tableHasColumns(
    tableName,
    [
      "tenant_id",
      "raw_payload_text",
      "raw_payload_retention_status",
      "raw_payload_masked_at",
      "raw_payload_purged_at",
    ],
    runQuery
  );
  if (!hasRequiredColumns) {
    return {
      status: "PARTIAL",
      scannedRows: 0,
      affectedRows: 0,
      maskedRows: 0,
      purgedRows: 0,
      archivedRows: 0,
      errorRows: 0,
      payload: {
        dataset_code: policy.dataset_code,
        table_name: tableName,
        reason: "REQUIRED_COLUMNS_MISSING",
      },
      note: `${tableName} does not expose required retention columns`,
    };
  }

  const paramsBase = [tenantId];
  let scopeSql = `${alias}.tenant_id = ?`;
  const hasLegalEntityColumn = await tableHasColumns(tableName, ["legal_entity_id"], runQuery);
  if (parsePositiveInt(legalEntityId) && hasLegalEntityColumn) {
    scopeSql += ` AND ${alias}.legal_entity_id = ?`;
    paramsBase.push(parsePositiveInt(legalEntityId));
  }

  const hasCreatedAt = await tableHasColumns(tableName, ["created_at"], runQuery);
  const datePredicate = hasCreatedAt
    ? `AND ${alias}.created_at < ?`
    : "";
  const dateParams = hasCreatedAt ? [cutoffTimestamp] : [];

  const scannedRes = await runQuery(
    `SELECT COUNT(*) AS total
     FROM ${tableName} ${alias}
     WHERE ${scopeSql}
       ${datePredicate}`,
    [...paramsBase, ...dateParams]
  );
  const scannedRows = Number(scannedRes.rows?.[0]?.total || 0);

  const action = up(policy.action_code);
  let affectedRows = 0;
  let maskedRows = 0;
  let purgedRows = 0;

  if (action === "MASK") {
    const upd = await runQuery(
      `UPDATE ${tableName} ${alias}
       SET ${alias}.raw_payload_text = '[MASKED_BY_RETENTION_POLICY]',
           ${alias}.raw_payload_retention_status = 'MASKED',
           ${alias}.raw_payload_masked_at = CURRENT_TIMESTAMP
       WHERE ${scopeSql}
         ${datePredicate}
         AND ${alias}.raw_payload_retention_status = 'ACTIVE'
         AND ${alias}.raw_payload_text IS NOT NULL`,
      [...paramsBase, ...dateParams]
    );
    maskedRows = Number(upd.rows?.affectedRows || 0);
    affectedRows = maskedRows;
  } else if (action === "PURGE") {
    const upd = await runQuery(
      `UPDATE ${tableName} ${alias}
       SET ${alias}.raw_payload_text = NULL,
           ${alias}.raw_payload_retention_status = 'PURGED',
           ${alias}.raw_payload_purged_at = CURRENT_TIMESTAMP
       WHERE ${scopeSql}
         ${datePredicate}
         AND ${alias}.raw_payload_retention_status <> 'PURGED'`,
      [...paramsBase, ...dateParams]
    );
    purgedRows = Number(upd.rows?.affectedRows || 0);
    affectedRows = purgedRows;
  } else {
    throw badRequest(`Unsupported action for ${policy.dataset_code}: ${action}`);
  }

  await writeSensitiveAuditLog({
    tenantId,
    legalEntityId,
    action: action === "MASK" ? "RETENTION_MASK" : "RETENTION_PURGE",
    objectId: policy.id,
    payload: {
      dataset_code: policy.dataset_code,
      table_name: tableName,
      cutoff_date: cutoffDate,
      scanned_rows: scannedRows,
      affected_rows: affectedRows,
    },
    note: `Retention run for policy ${policy.policy_code}`,
    userId,
    runQuery,
  });

  return {
    status: "COMPLETED",
    scannedRows,
    affectedRows,
    maskedRows,
    purgedRows,
    archivedRows: 0,
    errorRows: 0,
    payload: {
      dataset_code: policy.dataset_code,
      table_name: tableName,
      action_code: action,
      cutoff_date: cutoffDate,
    },
    note: `Applied ${action} to ${tableName}`,
  };
}

async function executeJobExecutionLogRetention({
  tenantId,
  policy,
  cutoffDate,
  cutoffTimestamp,
  runQuery,
  userId,
}) {
  const action = up(policy.action_code);
  const scannedJobsRes = await runQuery(
    `SELECT COUNT(*) AS total
     FROM app_jobs j
     WHERE j.tenant_id = ?
       AND j.created_at < ?
       AND (
         j.payload_json IS NOT NULL OR
         j.last_error_json IS NOT NULL OR
         j.result_json IS NOT NULL
       )`,
    [tenantId, cutoffTimestamp]
  );
  const scannedAttemptsRes = await runQuery(
    `SELECT COUNT(*) AS total
     FROM app_job_attempts a
     WHERE a.tenant_id = ?
       AND a.started_at < ?
       AND (
         a.error_json IS NOT NULL OR
         a.result_json IS NOT NULL
       )`,
    [tenantId, cutoffTimestamp]
  );

  const scannedRows =
    Number(scannedJobsRes.rows?.[0]?.total || 0) +
    Number(scannedAttemptsRes.rows?.[0]?.total || 0);

  let affectedRows = 0;
  let maskedRows = 0;
  let purgedRows = 0;

  if (action === "MASK") {
    const maskedObject = safeJson({ masked: true, source: "RETENTION_POLICY", cutoff_date: cutoffDate });

    const jobsUpd = await runQuery(
      `UPDATE app_jobs j
       SET j.payload_json = CASE
             WHEN j.payload_json IS NULL THEN NULL
             ELSE CAST(? AS JSON)
           END,
           j.last_error_json = CASE
             WHEN j.last_error_json IS NULL THEN NULL
             ELSE CAST(? AS JSON)
           END,
           j.result_json = CASE
             WHEN j.result_json IS NULL THEN NULL
             ELSE CAST(? AS JSON)
           END,
           j.updated_at = CURRENT_TIMESTAMP
       WHERE j.tenant_id = ?
         AND j.created_at < ?
         AND (
           j.payload_json IS NOT NULL OR
           j.last_error_json IS NOT NULL OR
           j.result_json IS NOT NULL
         )`,
      [maskedObject, maskedObject, maskedObject, tenantId, cutoffTimestamp]
    );

    const attemptsUpd = await runQuery(
      `UPDATE app_job_attempts a
       SET a.error_json = CASE
             WHEN a.error_json IS NULL THEN NULL
             ELSE CAST(? AS JSON)
           END,
           a.result_json = CASE
             WHEN a.result_json IS NULL THEN NULL
             ELSE CAST(? AS JSON)
           END
       WHERE a.tenant_id = ?
         AND a.started_at < ?
         AND (
           a.error_json IS NOT NULL OR
           a.result_json IS NOT NULL
         )`,
      [maskedObject, maskedObject, tenantId, cutoffTimestamp]
    );

    affectedRows = Number(jobsUpd.rows?.affectedRows || 0) + Number(attemptsUpd.rows?.affectedRows || 0);
    maskedRows = affectedRows;
  } else if (action === "PURGE") {
    const jobsUpd = await runQuery(
      `UPDATE app_jobs j
       SET j.payload_json = NULL,
           j.last_error_json = NULL,
           j.result_json = NULL,
           j.updated_at = CURRENT_TIMESTAMP
       WHERE j.tenant_id = ?
         AND j.created_at < ?
         AND (
           j.payload_json IS NOT NULL OR
           j.last_error_json IS NOT NULL OR
           j.result_json IS NOT NULL
         )`,
      [tenantId, cutoffTimestamp]
    );

    const attemptsUpd = await runQuery(
      `UPDATE app_job_attempts a
       SET a.error_json = NULL,
           a.result_json = NULL
       WHERE a.tenant_id = ?
         AND a.started_at < ?
         AND (
           a.error_json IS NOT NULL OR
           a.result_json IS NOT NULL
         )`,
      [tenantId, cutoffTimestamp]
    );

    affectedRows = Number(jobsUpd.rows?.affectedRows || 0) + Number(attemptsUpd.rows?.affectedRows || 0);
    purgedRows = affectedRows;
  } else {
    throw badRequest(`Unsupported action for JOB_EXECUTION_LOG: ${action}`);
  }

  await writeSensitiveAuditLog({
    tenantId,
    legalEntityId: null,
    action: action === "MASK" ? "RETENTION_MASK" : "RETENTION_PURGE",
    objectId: policy.id,
    payload: {
      dataset_code: policy.dataset_code,
      cutoff_date: cutoffDate,
      scanned_rows: scannedRows,
      affected_rows: affectedRows,
    },
    note: `Retention run for policy ${policy.policy_code}`,
    userId,
    runQuery,
  });

  return {
    status: "COMPLETED",
    scannedRows,
    affectedRows,
    maskedRows,
    purgedRows,
    archivedRows: 0,
    errorRows: 0,
    payload: {
      dataset_code: policy.dataset_code,
      action_code: action,
      cutoff_date: cutoffDate,
    },
    note: `Applied ${action} to app_jobs and app_job_attempts payload columns`,
  };
}

async function executeSensitiveAuditArchive({
  tenantId,
  legalEntityId,
  policy,
  cutoffDate,
  cutoffTimestamp,
  runQuery,
  userId,
}) {
  const paramsBase = [tenantId];
  let scopeSql = "a.tenant_id = ?";
  if (parsePositiveInt(legalEntityId)) {
    scopeSql += " AND a.legal_entity_id = ?";
    paramsBase.push(parsePositiveInt(legalEntityId));
  }

  const scannedRes = await runQuery(
    `SELECT COUNT(*) AS total
     FROM sensitive_data_audit a
     WHERE ${scopeSql}
       AND a.acted_at < ?`,
    [...paramsBase, cutoffTimestamp]
  );
  const scannedRows = Number(scannedRes.rows?.[0]?.total || 0);

  const action = up(policy.action_code);
  if (action !== "ARCHIVE") {
    throw badRequest("SENSITIVE_DATA_AUDIT dataset only supports ARCHIVE action");
  }

  const ins = await runQuery(
    `INSERT INTO sensitive_data_audit_archive (
       tenant_id,
       source_sensitive_data_audit_id,
       legal_entity_id,
       module_code,
       object_type,
       object_id,
       action,
       payload_json,
       note,
       acted_by_user_id,
       acted_at,
       archived_by_user_id,
       archived_at
     )
     SELECT
       a.tenant_id,
       a.id,
       a.legal_entity_id,
       a.module_code,
       a.object_type,
       a.object_id,
       a.action,
       a.payload_json,
       a.note,
       a.acted_by_user_id,
       a.acted_at,
       ?,
       CURRENT_TIMESTAMP
     FROM sensitive_data_audit a
     LEFT JOIN sensitive_data_audit_archive ar
       ON ar.tenant_id = a.tenant_id
      AND ar.source_sensitive_data_audit_id = a.id
     WHERE ${scopeSql}
       AND a.acted_at < ?
       AND ar.id IS NULL`,
    [parsePositiveInt(userId) || null, ...paramsBase, cutoffTimestamp]
  );

  const archivedRows = Number(ins.rows?.affectedRows || 0);

  await writeSensitiveAuditLog({
    tenantId,
    legalEntityId,
    action: "RETENTION_ARCHIVE",
    objectId: policy.id,
    payload: {
      dataset_code: policy.dataset_code,
      cutoff_date: cutoffDate,
      scanned_rows: scannedRows,
      archived_rows: archivedRows,
    },
    note: `Retention archive run for policy ${policy.policy_code}`,
    userId,
    runQuery,
  });

  return {
    status: "COMPLETED",
    scannedRows,
    affectedRows: archivedRows,
    maskedRows: 0,
    purgedRows: 0,
    archivedRows,
    errorRows: 0,
    payload: {
      dataset_code: policy.dataset_code,
      action_code: action,
      cutoff_date: cutoffDate,
    },
    note: "Archived sensitive_data_audit rows to sensitive_data_audit_archive",
  };
}
async function executePolicyDataset({
  tenantId,
  legalEntityId,
  policy,
  cutoffDate,
  runQuery,
  userId,
}) {
  const cutoffTimestamp = computeCutoffTimestamp(cutoffDate);
  const datasetCode = up(policy.dataset_code);

  if (datasetCode === "PAYROLL_PROVIDER_IMPORT_RAW") {
    return executePayrollProviderImportRetention({
      tenantId,
      legalEntityId,
      policy,
      cutoffDate,
      cutoffTimestamp,
      runQuery,
      userId,
    });
  }
  if (datasetCode === "BANK_FEED_RAW_PAYLOAD") {
    return executeGenericRawPayloadRetention({
      tenantId,
      legalEntityId,
      policy,
      cutoffDate,
      cutoffTimestamp,
      tableName: "bank_feed_events",
      alias: "e",
      runQuery,
      userId,
    });
  }
  if (datasetCode === "BANK_WEBHOOK_RAW_PAYLOAD") {
    return executeGenericRawPayloadRetention({
      tenantId,
      legalEntityId,
      policy,
      cutoffDate,
      cutoffTimestamp,
      tableName: "bank_webhook_events",
      alias: "w",
      runQuery,
      userId,
    });
  }
  if (datasetCode === "JOB_EXECUTION_LOG") {
    return executeJobExecutionLogRetention({
      tenantId,
      policy,
      cutoffDate,
      cutoffTimestamp,
      runQuery,
      userId,
    });
  }
  if (datasetCode === "SENSITIVE_DATA_AUDIT") {
    return executeSensitiveAuditArchive({
      tenantId,
      legalEntityId,
      policy,
      cutoffDate,
      cutoffTimestamp,
      runQuery,
      userId,
    });
  }

  throw badRequest(`Unsupported datasetCode: ${datasetCode}`);
}

function assertDatasetPolicyCompatibility({ datasetCode, actionCode, legalEntityId }) {
  const definition = DATASET_DEFINITIONS[datasetCode];
  if (!definition) {
    throw badRequest(`Unsupported datasetCode: ${datasetCode}`);
  }
  if (!definition.allowedActions.has(actionCode)) {
    throw badRequest(
      `actionCode ${actionCode} is not allowed for datasetCode ${datasetCode}`
    );
  }
  if (!definition.supportsLegalEntity && parsePositiveInt(legalEntityId)) {
    throw badRequest(`datasetCode ${datasetCode} does not support legalEntity scope`);
  }
}

function normalizeRetentionPolicyInput(input = {}) {
  const datasetCode = normalizeDatasetCode(input.datasetCode ?? input.dataset_code);
  const actionCode = normalizeActionCode(input.actionCode ?? input.action_code, datasetCode);
  const legalEntityId = parsePositiveInt(input.legalEntityId ?? input.legal_entity_id) || null;
  assertDatasetPolicyCompatibility({ datasetCode, actionCode, legalEntityId });

  const policyCode = String((input.policyCode ?? input.policy_code) || "").trim().toUpperCase();
  if (!policyCode) throw badRequest("policyCode is required");
  if (policyCode.length > 60) throw badRequest("policyCode cannot exceed 60 characters");

  const policyName = String((input.policyName ?? input.policy_name) || "").trim();
  if (!policyName) throw badRequest("policyName is required");
  if (policyName.length > 190) throw badRequest("policyName cannot exceed 190 characters");

  return {
    policyCode,
    policyName,
    datasetCode,
    actionCode,
    retentionDays: normalizeRetentionDays(input.retentionDays ?? input.retention_days),
    legalEntityId,
    scopeType: legalEntityId ? "LEGAL_ENTITY" : "TENANT",
    status: normalizePolicyStatus(input.status, "ACTIVE"),
    configJson: parseObjectMaybe(input.configJson ?? input.config_json, "configJson"),
  };
}

export async function resolveDataRetentionPolicyScope(policyId, tenantId) {
  const pId = parsePositiveInt(policyId);
  const tId = parsePositiveInt(tenantId);
  if (!pId || !tId) return null;

  const row = await query(
    `SELECT legal_entity_id
     FROM data_retention_policies
     WHERE tenant_id = ? AND id = ?
     LIMIT 1`,
    [tId, pId]
  );
  const legalEntityId = parsePositiveInt(row.rows?.[0]?.legal_entity_id);
  if (!legalEntityId) return null;
  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: legalEntityId,
  };
}

export async function resolveDataRetentionRunScope(runId, tenantId) {
  const rId = parsePositiveInt(runId);
  const tId = parsePositiveInt(tenantId);
  if (!rId || !tId) return null;

  const row = await query(
    `SELECT legal_entity_id
     FROM data_retention_runs
     WHERE tenant_id = ? AND id = ?
     LIMIT 1`,
    [tId, rId]
  );
  const legalEntityId = parsePositiveInt(row.rows?.[0]?.legal_entity_id);
  if (!legalEntityId) return null;
  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: legalEntityId,
  };
}

export async function listRetentionPolicyRows({
  req,
  tenantId,
  filters = {},
  buildScopeFilter,
  assertScopeAccess,
}) {
  const tId = parsePositiveInt(tenantId);
  if (!tId) throw badRequest("tenantId is required");

  const params = [];
  const where = buildScopedLegalEntityClause({
    req,
    tenantId: tId,
    alias: "p",
    legalEntityId: filters.legalEntityId,
    params,
    buildScopeFilter,
    assertScopeAccess,
  });

  const datasetCode = filters.datasetCode ? normalizeDatasetCode(filters.datasetCode) : null;
  if (datasetCode) {
    where.push("p.dataset_code = ?");
    params.push(datasetCode);
  }

  const actionCode = filters.actionCode ? up(filters.actionCode) : null;
  if (actionCode) {
    where.push("p.action_code = ?");
    params.push(actionCode);
  }

  const status = filters.status ? normalizePolicyStatus(filters.status) : null;
  if (status) {
    where.push("p.status = ?");
    params.push(status);
  }

  if (filters.q) {
    const like = `%${String(filters.q).trim()}%`;
    where.push("(p.policy_code LIKE ? OR p.policy_name LIKE ?)");
    params.push(like, like);
  }

  const safeLimit = Math.min(500, Math.max(1, Number(filters.limit || 100)));
  const safeOffset = Math.max(0, Number(filters.offset || 0));
  const whereSql = where.join(" AND ");

  const countRes = await query(
    `SELECT COUNT(*) AS total
     FROM data_retention_policies p
     WHERE ${whereSql}`,
    params
  );

  const listRes = await query(
    `SELECT p.*, le.code AS legal_entity_code, le.name AS legal_entity_name,
            cu.email AS created_by_user_email, uu.email AS updated_by_user_email
     FROM data_retention_policies p
     LEFT JOIN legal_entities le
       ON le.tenant_id = p.tenant_id
      AND le.id = p.legal_entity_id
     LEFT JOIN users cu
       ON cu.tenant_id = p.tenant_id
      AND cu.id = p.created_by_user_id
     LEFT JOIN users uu
       ON uu.tenant_id = p.tenant_id
      AND uu.id = p.updated_by_user_id
     WHERE ${whereSql}
     ORDER BY p.updated_at DESC, p.id DESC
     LIMIT ${Math.trunc(safeLimit)} OFFSET ${Math.trunc(safeOffset)}`,
    params
  );

  return {
    rows: (listRes.rows || []).map(mapPolicyRow),
    total: Number(countRes.rows?.[0]?.total || 0),
    limit: Math.trunc(safeLimit),
    offset: Math.trunc(safeOffset),
  };
}

export async function createRetentionPolicy({
  req,
  tenantId,
  userId,
  input,
  assertScopeAccess,
}) {
  const tId = parsePositiveInt(tenantId);
  if (!tId) throw badRequest("tenantId is required");
  const actorId = parsePositiveInt(userId);
  if (!actorId) throw badRequest("userId is required");

  const payload = normalizeRetentionPolicyInput(input || {});
  if (payload.legalEntityId && typeof assertScopeAccess === "function") {
    assertScopeAccess(req, "legal_entity", payload.legalEntityId, "legalEntityId");
  }

  try {
    const ins = await query(
      `INSERT INTO data_retention_policies (
         tenant_id,
         legal_entity_id,
         policy_code,
         policy_name,
         dataset_code,
         action_code,
         retention_days,
         scope_type,
         status,
         config_json,
         created_by_user_id,
         updated_by_user_id
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        tId,
        payload.legalEntityId,
        payload.policyCode,
        payload.policyName,
        payload.datasetCode,
        payload.actionCode,
        payload.retentionDays,
        payload.scopeType,
        payload.status,
        safeJson(payload.configJson),
        actorId,
        actorId,
      ]
    );

    const policyId = parsePositiveInt(ins.rows?.insertId);
    const row = await findPolicyRow({ tenantId: tId, policyId });
    return { row, created: true };
  } catch (err) {
    if (!isDuplicateError(err)) throw err;
    throw conflict("A retention policy with this code already exists in this tenant");
  }
}

export async function updateRetentionPolicy({
  req,
  tenantId,
  userId,
  policyId,
  input,
  assertScopeAccess,
}) {
  const tId = parsePositiveInt(tenantId);
  const actorId = parsePositiveInt(userId);
  const pId = parsePositiveInt(policyId);
  if (!tId) throw badRequest("tenantId is required");
  if (!actorId) throw badRequest("userId is required");
  if (!pId) throw badRequest("policyId is required");

  const updates = {};
  if (input?.policyName !== undefined || input?.policy_name !== undefined) {
    const policyName = String((input.policyName ?? input.policy_name) || "").trim();
    if (!policyName) throw badRequest("policyName cannot be empty");
    if (policyName.length > 190) throw badRequest("policyName cannot exceed 190 characters");
    updates.policy_name = policyName;
  }
  if (input?.retentionDays !== undefined || input?.retention_days !== undefined) {
    updates.retention_days = normalizeRetentionDays(input.retentionDays ?? input.retention_days);
  }
  if (input?.status !== undefined) {
    updates.status = normalizePolicyStatus(input.status);
  }
  if (input?.configJson !== undefined || input?.config_json !== undefined) {
    updates.config_json = parseObjectMaybe(input.configJson ?? input.config_json, "configJson");
  }

  if (Object.keys(updates).length === 0) {
    throw badRequest("At least one updatable field is required");
  }

  await withTransaction(async (tx) => {
    const existing = await findPolicyRow({
      tenantId: tId,
      policyId: pId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!existing) throw notFound("Retention policy not found");

    const legalEntityId = parsePositiveInt(existing.legal_entity_id);
    if (legalEntityId && typeof assertScopeAccess === "function") {
      assertScopeAccess(req, "legal_entity", legalEntityId, "policyId");
    }

    const sqlParts = [];
    const params = [];

    if (updates.policy_name !== undefined) {
      sqlParts.push("policy_name = ?");
      params.push(updates.policy_name);
    }
    if (updates.retention_days !== undefined) {
      sqlParts.push("retention_days = ?");
      params.push(updates.retention_days);
    }
    if (updates.status !== undefined) {
      sqlParts.push("status = ?");
      params.push(updates.status);
    }
    if (Object.prototype.hasOwnProperty.call(updates, "config_json")) {
      sqlParts.push("config_json = ?");
      params.push(safeJson(updates.config_json));
    }

    sqlParts.push("updated_by_user_id = ?");
    params.push(actorId);

    await tx.query(
      `UPDATE data_retention_policies
       SET ${sqlParts.join(", ")},
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND id = ?`,
      [...params, tId, pId]
    );
  });

  const row = await findPolicyRow({ tenantId: tId, policyId: pId });
  return { row, updated: true };
}

export async function listRetentionRunRows({
  req,
  tenantId,
  filters = {},
  buildScopeFilter,
  assertScopeAccess,
}) {
  const tId = parsePositiveInt(tenantId);
  if (!tId) throw badRequest("tenantId is required");

  const params = [];
  const where = buildScopedLegalEntityClause({
    req,
    tenantId: tId,
    alias: "r",
    legalEntityId: filters.legalEntityId,
    params,
    buildScopeFilter,
    assertScopeAccess,
  });

  const policyId = parsePositiveInt(filters.policyId);
  if (policyId) {
    where.push("r.data_retention_policy_id = ?");
    params.push(policyId);
  }

  const status = filters.status ? normalizeRunStatus(filters.status) : null;
  if (status) {
    where.push("r.status = ?");
    params.push(status);
  }

  const triggerMode = filters.triggerMode
    ? normalizeRunTriggerMode(filters.triggerMode)
    : null;
  if (triggerMode) {
    where.push("r.trigger_mode = ?");
    params.push(triggerMode);
  }

  const datasetCode = filters.datasetCode ? normalizeDatasetCode(filters.datasetCode) : null;
  if (datasetCode) {
    where.push("p.dataset_code = ?");
    params.push(datasetCode);
  }

  if (filters.dateFrom) {
    where.push("DATE(r.started_at) >= ?");
    params.push(filters.dateFrom);
  }
  if (filters.dateTo) {
    where.push("DATE(r.started_at) <= ?");
    params.push(filters.dateTo);
  }

  const safeLimit = Math.min(500, Math.max(1, Number(filters.limit || 100)));
  const safeOffset = Math.max(0, Number(filters.offset || 0));
  const whereSql = where.join(" AND ");

  const countRes = await query(
    `SELECT COUNT(*) AS total
     FROM data_retention_runs r
     JOIN data_retention_policies p
       ON p.tenant_id = r.tenant_id
      AND p.id = r.data_retention_policy_id
     WHERE ${whereSql}`,
    params
  );

  const listRes = await query(
    `SELECT r.*, p.policy_code, p.policy_name, p.dataset_code, p.action_code,
            le.code AS legal_entity_code, le.name AS legal_entity_name,
            u.email AS acted_by_user_email
     FROM data_retention_runs r
     JOIN data_retention_policies p
       ON p.tenant_id = r.tenant_id
      AND p.id = r.data_retention_policy_id
     LEFT JOIN legal_entities le
       ON le.tenant_id = r.tenant_id
      AND le.id = r.legal_entity_id
     LEFT JOIN users u
       ON u.tenant_id = r.tenant_id
      AND u.id = r.acted_by_user_id
     WHERE ${whereSql}
     ORDER BY r.id DESC
     LIMIT ${Math.trunc(safeLimit)} OFFSET ${Math.trunc(safeOffset)}`,
    params
  );

  return {
    rows: (listRes.rows || []).map(mapRunRow),
    total: Number(countRes.rows?.[0]?.total || 0),
    limit: Math.trunc(safeLimit),
    offset: Math.trunc(safeOffset),
  };
}
export async function getRetentionRunDetail({
  req,
  tenantId,
  runId,
  assertScopeAccess,
}) {
  const tId = parsePositiveInt(tenantId);
  const rId = parsePositiveInt(runId);
  if (!tId) throw badRequest("tenantId is required");
  if (!rId) throw badRequest("runId is required");

  const row = await findRunRow({ tenantId: tId, runId: rId });
  if (!row) throw notFound("Retention run not found");

  const legalEntityId = parsePositiveInt(row.legal_entity_id);
  if (legalEntityId && typeof assertScopeAccess === "function") {
    assertScopeAccess(req, "legal_entity", legalEntityId, "runId");
  }

  return { row };
}

export async function executeDataRetentionPolicyRun({
  req,
  tenantId,
  userId,
  policyId,
  input = {},
  assertScopeAccess,
}) {
  const tId = parsePositiveInt(tenantId);
  const actorId = parsePositiveInt(userId) || null;
  const pId = parsePositiveInt(policyId ?? input.policyId ?? input.policy_id);
  if (!tId) throw badRequest("tenantId is required");
  if (!pId) throw badRequest("policyId is required");

  const runIdempotencyKeyRaw = String(
    (input.runIdempotencyKey ?? input.run_idempotency_key) || ""
  ).trim();
  const runIdempotencyKey = runIdempotencyKeyRaw ? truncate(runIdempotencyKeyRaw, 190) : null;
  const triggerMode = normalizeRunTriggerMode(input.triggerMode ?? input.trigger_mode, "MANUAL");

  if (runIdempotencyKey) {
    const existingRes = await query(
      `SELECT id
       FROM data_retention_runs
       WHERE tenant_id = ?
         AND run_idempotency_key = ?
       LIMIT 1`,
      [tId, runIdempotencyKey]
    );
    const existingRunId = parsePositiveInt(existingRes.rows?.[0]?.id);
    if (existingRunId) {
      const existing = await findRunRow({ tenantId: tId, runId: existingRunId });
      return {
        row: existing,
        idempotent: true,
      };
    }
  }

  const result = await withTransaction(async (tx) => {
    const policy = await findPolicyRow({
      tenantId: tId,
      policyId: pId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!policy) throw notFound("Retention policy not found");

    const legalEntityId = parsePositiveInt(policy.legal_entity_id) || null;
    if (legalEntityId && typeof assertScopeAccess === "function") {
      assertScopeAccess(req, "legal_entity", legalEntityId, "policyId");
    }

    const policyStatus = up(policy.status);
    if (policyStatus === "DISABLED") {
      throw conflict("Retention policy is DISABLED");
    }

    const datasetCode = normalizeDatasetCode(policy.dataset_code);
    const actionCode = normalizeActionCode(policy.action_code, datasetCode);
    assertDatasetPolicyCompatibility({ datasetCode, actionCode, legalEntityId });

    const cutoffDate = computeCutoffDate(policy.retention_days);

    const ins = await tx.query(
      `INSERT INTO data_retention_runs (
         tenant_id,
         data_retention_policy_id,
         legal_entity_id,
         trigger_mode,
         status,
         run_idempotency_key,
         retention_cutoff_date,
         payload_json,
         acted_by_user_id,
         started_at
       ) VALUES (?, ?, ?, ?, 'RUNNING', ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
      [
        tId,
        pId,
        legalEntityId,
        triggerMode,
        runIdempotencyKey,
        cutoffDate,
        safeJson({
          dataset_code: datasetCode,
          action_code: actionCode,
          cutoff_date: cutoffDate,
        }),
        actorId,
      ]
    );

    const runId = parsePositiveInt(ins.rows?.insertId);
    if (!runId) {
      throw new Error("Retention run could not be created");
    }

    try {
      const summary = await executePolicyDataset({
        tenantId: tId,
        legalEntityId,
        policy,
        cutoffDate,
        runQuery: tx.query,
        userId: actorId,
      });

      const runStatus = summary?.status && RUN_STATUSES.has(up(summary.status))
        ? up(summary.status)
        : "COMPLETED";

      await tx.query(
        `UPDATE data_retention_runs
         SET status = ?,
             scanned_rows = ?,
             affected_rows = ?,
             masked_rows = ?,
             purged_rows = ?,
             archived_rows = ?,
             error_rows = ?,
             payload_json = ?,
             error_text = NULL,
             finished_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ? AND id = ?`,
        [
          runStatus,
          Number(summary?.scannedRows || 0),
          Number(summary?.affectedRows || 0),
          Number(summary?.maskedRows || 0),
          Number(summary?.purgedRows || 0),
          Number(summary?.archivedRows || 0),
          Number(summary?.errorRows || 0),
          safeJson({
            ...summary?.payload,
            note: summary?.note || null,
            cutoff_date: cutoffDate,
          }),
          tId,
          runId,
        ]
      );

      await tx.query(
        `UPDATE data_retention_policies
         SET last_run_at = CURRENT_TIMESTAMP,
             last_run_status = ?,
             last_run_note = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ? AND id = ?`,
        [runStatus, truncate(summary?.note || `Retention run ${runStatus}`, 500), tId, pId]
      );
    } catch (err) {
      await tx.query(
        `UPDATE data_retention_runs
         SET status = 'FAILED',
             error_rows = error_rows + 1,
             error_text = ?,
             payload_json = ?,
             finished_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ? AND id = ?`,
        [
          truncate(err?.message || "Retention run failed", 500),
          safeJson({
            dataset_code: datasetCode,
            action_code: actionCode,
            error: {
              message: truncate(err?.message || "Retention run failed", 500),
              code: truncate(err?.errorCode || err?.code || "RETENTION_RUN_FAILED", 80),
            },
            cutoff_date: cutoffDate,
          }),
          tId,
          runId,
        ]
      );

      await tx.query(
        `UPDATE data_retention_policies
         SET last_run_at = CURRENT_TIMESTAMP,
             last_run_status = 'FAILED',
             last_run_note = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ? AND id = ?`,
        [truncate(err?.message || "Retention run failed", 500), tId, pId]
      );

      throw err;
    }

    const row = await findRunRow({ tenantId: tId, runId, runQuery: tx.query });
    return {
      row,
      idempotent: false,
    };
  });

  return result;
}

export default {
  resolveDataRetentionPolicyScope,
  resolveDataRetentionRunScope,
  listRetentionPolicyRows,
  createRetentionPolicy,
  updateRetentionPolicy,
  listRetentionRunRows,
  getRetentionRunDetail,
  executeDataRetentionPolicyRun,
};
