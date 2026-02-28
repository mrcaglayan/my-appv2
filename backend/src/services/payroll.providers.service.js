import crypto from "node:crypto";
import { query, withTransaction } from "../db.js";
import { assertCurrencyExists, assertLegalEntityBelongsToTenant } from "../tenantGuards.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  decodeCursorToken,
  encodeCursorToken,
  requireCursorDateTime,
  requireCursorId,
  toCursorDateTime,
} from "../utils/cursorPagination.js";
import {
  getPayrollProviderAdapter,
  listSupportedPayrollProviderAdapters,
} from "./payroll.providers.registry.js";
import { assertPayrollPeriodActionAllowed } from "./payroll.close.service.js";
import { getPayrollRunByIdForTenant, importPayrollRunCsv } from "./payroll.runs.service.js";
import {
  decryptJson,
  encryptJson,
  parseEnvelopeText,
  serializeEnvelope,
} from "../utils/cryptoEnvelope.js";
import { redactObject, redactRawPayloadText } from "../utils/redaction.js";
import { evaluateApprovalNeed, submitApprovalRequest } from "./approvalPolicies.service.js";

function up(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function toDateOnly(value) {
  if (!value) return null;
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

function toAmount(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Number(n.toFixed(6)) : 0;
}

function sha256(value) {
  return crypto.createHash("sha256").update(String(value ?? ""), "utf8").digest("hex");
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

function forbidden(message) {
  const err = new Error(message);
  err.status = 403;
  return err;
}

function noopScopeAccess() {
  return true;
}

function isDup(err) {
  return Number(err?.errno) === 1062 || up(err?.code) === "ER_DUP_ENTRY";
}

function dupKey(err) {
  const text = String(err?.sqlMessage || err?.message || "");
  return text.match(/for key ['`"]([^'"`]+)['`"]/i)?.[1] || "";
}

function csvCell(value) {
  const text = value === undefined || value === null ? "" : String(value);
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

const CSV_COLUMNS = [
  "employee_code",
  "employee_name",
  "cost_center_code",
  "base_salary",
  "overtime_pay",
  "bonus_pay",
  "allowances_total",
  "gross_pay",
  "employee_tax",
  "employee_social_security",
  "other_deductions",
  "employer_tax",
  "employer_social_security",
  "net_pay",
];

function csvAmount(value) {
  return toAmount(value).toFixed(6);
}

function buildPayrollCsv(rows = []) {
  const lines = [CSV_COLUMNS.join(",")];
  for (const row of rows) {
    lines.push(
      [
        row.employee_code || "",
        row.employee_name || "",
        row.cost_center_code || "",
        csvAmount(row.base_salary),
        csvAmount(row.overtime_pay),
        csvAmount(row.bonus_pay),
        csvAmount(row.allowances_total),
        csvAmount(row.gross_pay),
        csvAmount(row.employee_tax),
        csvAmount(row.employee_social_security),
        csvAmount(row.other_deductions),
        csvAmount(row.employer_tax),
        csvAmount(row.employer_social_security),
        csvAmount(row.net_pay),
      ]
        .map(csvCell)
        .join(",")
    );
  }
  return lines.join("\n");
}

function normalizePayloadText(raw) {
  return String(raw ?? "").replace(/\r\n/g, "\n");
}

function maskSecretsForApiRow(row) {
  if (!row) return null;
  const out = { ...row };
  const hasSecrets = Boolean(out.secrets_encrypted_json || out.secrets_json);
  delete out.secrets_encrypted_json;
  if (Object.prototype.hasOwnProperty.call(out, "secrets_json")) {
    out.secrets_json = out.secrets_json ? { _masked: true } : null;
  }
  out.has_secrets = hasSecrets;
  return out;
}

function getConnectionSecretsDecrypted(row) {
  const envelope = parseEnvelopeText(row?.secrets_encrypted_json);
  if (envelope) {
    return decryptJson(envelope);
  }
  const legacy = parseJsonMaybe(row?.secrets_json);
  if (legacy && typeof legacy === "object" && !Array.isArray(legacy)) {
    return legacy;
  }
  return {};
}

function mapConnection(row) {
  if (!row) return null;
  return maskSecretsForApiRow({
    ...row,
    settings_json: parseJsonMaybe(row.settings_json),
  });
}

function mapRef(row) {
  if (!row) return null;
  return { ...row, payload_json: parseJsonMaybe(row.payload_json) };
}

function mapJob(row, { includeRaw = false } = {}) {
  if (!row) return null;
  const out = {
    ...row,
    preview_summary_json: parseJsonMaybe(row.preview_summary_json),
    validation_errors_json: parseJsonMaybe(row.validation_errors_json) || [],
    match_errors_json: parseJsonMaybe(row.match_errors_json) || [],
    match_warnings_json: parseJsonMaybe(row.match_warnings_json) || [],
    normalized_payload_json: parseJsonMaybe(row.normalized_payload_json),
  };
  if (!includeRaw) delete out.raw_payload_text;
  return out;
}

function mapAudit(row) {
  if (!row) return null;
  return { ...row, payload_json: parseJsonMaybe(row.payload_json) };
}

function deriveImportApprovalThreshold(job) {
  const preview = parseJsonMaybe(job?.preview_summary_json) || {};
  const candidates = [
    preview.total_net_pay,
    preview.total_gross_pay,
    preview.total_amount,
    preview.total_payroll_amount,
  ];
  for (const candidate of candidates) {
    const amt = toAmount(candidate);
    if (amt > 0) return amt;
  }
  return null;
}

function assertLeScope(req, assertScopeAccess, legalEntityId, label = "legalEntityId") {
  if (assertScopeAccess && parsePositiveInt(legalEntityId)) {
    assertScopeAccess(req, "legal_entity", parsePositiveInt(legalEntityId), label);
  }
}

function scopeListCondition({ req, filters, params, buildScopeFilter, assertScopeAccess, columnName }) {
  const leId = parsePositiveInt(filters?.legalEntityId);
  if (leId) {
    assertLeScope(req, assertScopeAccess, leId, "legalEntityId");
    params.push(leId);
    return `${columnName} = ?`;
  }
  if (typeof buildScopeFilter === "function") {
    return buildScopeFilter(req, "legal_entity", columnName, params);
  }
  return "1 = 1";
}

async function writeImportAudit({
  tenantId,
  legalEntityId,
  importJobId,
  action,
  payload = null,
  note = null,
  userId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO payroll_provider_import_audit (
        tenant_id, legal_entity_id, payroll_provider_import_job_id,
        action, payload_json, note, acted_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [
      tenantId,
      legalEntityId,
      importJobId,
      up(action),
      payload === null ? null : safeJson(payload),
      note || null,
      parsePositiveInt(userId),
    ]
  );
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
      tenantId,
      parsePositiveInt(legalEntityId),
      up(moduleCode),
      up(objectType),
      parsePositiveInt(objectId),
      up(action),
      payload === null ? null : safeJson(payload),
      note || null,
      parsePositiveInt(userId),
    ]
  );
}

async function findConnection({ tenantId, connectionId, runQuery = query, forUpdate = false }) {
  const res = await runQuery(
    `SELECT * FROM payroll_provider_connections
     WHERE tenant_id = ? AND id = ?
     LIMIT 1${forUpdate ? " FOR UPDATE" : ""}`,
    [tenantId, connectionId]
  );
  return res.rows?.[0] || null;
}

async function findJob({ tenantId, importJobId, runQuery = query, forUpdate = false }) {
  const res = await runQuery(
    `SELECT * FROM payroll_provider_import_jobs
     WHERE tenant_id = ? AND id = ?
     LIMIT 1${forUpdate ? " FOR UPDATE" : ""}`,
    [tenantId, importJobId]
  );
  return res.rows?.[0] || null;
}

async function requireConnection(args) {
  const row = await findConnection(args);
  if (!row) throw notFound("Payroll provider connection not found");
  return row;
}

async function requireJob(args) {
  const row = await findJob(args);
  if (!row) throw notFound("Payroll provider import job not found");
  return row;
}

export function listSupportedPayrollProviders() {
  return listSupportedPayrollProviderAdapters();
}

export async function resolvePayrollProviderConnectionScope(connectionId, tenantId, runQuery = query) {
  const id = parsePositiveInt(connectionId);
  const t = parsePositiveInt(tenantId);
  if (!id || !t) return null;
  const row = await findConnection({ tenantId: t, connectionId: id, runQuery });
  if (!row) return null;
  return { scopeType: "LEGAL_ENTITY", scopeId: parsePositiveInt(row.legal_entity_id) };
}

export async function resolvePayrollProviderImportJobScope(importJobId, tenantId, runQuery = query) {
  const id = parsePositiveInt(importJobId);
  const t = parsePositiveInt(tenantId);
  if (!id || !t) return null;
  const row = await findJob({ tenantId: t, importJobId: id, runQuery });
  if (!row) return null;
  return { scopeType: "LEGAL_ENTITY", scopeId: parsePositiveInt(row.legal_entity_id) };
}

export async function listPayrollProviderConnections({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["c.tenant_id = ?"];
  conditions.push(
    scopeListCondition({
      req,
      filters,
      params,
      buildScopeFilter,
      assertScopeAccess,
      columnName: "c.legal_entity_id",
    })
  );
  if (filters.providerCode) {
    conditions.push("c.provider_code = ?");
    params.push(filters.providerCode);
  }
  if (filters.status) {
    conditions.push("c.status = ?");
    params.push(filters.status);
  }
  const whereSql = conditions.join(" AND ");
  const countRes = await query(
    `SELECT COUNT(*) AS total FROM payroll_provider_connections c WHERE ${whereSql}`,
    params
  );
  const total = Number(countRes.rows?.[0]?.total || 0);
  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset = Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const effectiveOffset = safeOffset;
  const listRes = await query(
    `SELECT c.*, le.code AS legal_entity_code, le.name AS legal_entity_name
     FROM payroll_provider_connections c
     JOIN legal_entities le ON le.tenant_id = c.tenant_id AND le.id = c.legal_entity_id
     WHERE ${whereSql}
     ORDER BY c.is_default DESC, c.updated_at DESC, c.id DESC
     LIMIT ${safeLimit} OFFSET ${effectiveOffset}`,
    params
  );
  return {
    rows: (listRes.rows || []).map(mapConnection),
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function createPayrollProviderConnection({ req, tenantId, userId, input, assertScopeAccess }) {
  await assertLegalEntityBelongsToTenant(tenantId, input.legalEntityId, "legalEntityId");
  assertLeScope(req, assertScopeAccess, input.legalEntityId, "legalEntityId");
  let createdId = null;
  try {
    await withTransaction(async (tx) => {
      const secretsEnvelope =
        input.secretsJson !== undefined && input.secretsJson !== null
          ? encryptJson(input.secretsJson)
          : null;

      if (input.isDefault) {
        await tx.query(
          `UPDATE payroll_provider_connections
           SET is_default = 0, updated_by_user_id = ?, updated_at = CURRENT_TIMESTAMP
           WHERE tenant_id = ? AND legal_entity_id = ?`,
          [userId, tenantId, input.legalEntityId]
        );
      }
      const ins = await tx.query(
        `INSERT INTO payroll_provider_connections (
            tenant_id, legal_entity_id, provider_code, provider_name, adapter_version,
            status, is_default, settings_json,
            secrets_json, secrets_encrypted_json, secrets_key_version, secrets_migrated_at,
            created_by_user_id, updated_by_user_id
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          tenantId,
          input.legalEntityId,
          input.providerCode,
          input.providerName || input.providerCode,
          input.adapterVersion || "v1",
          input.status || "ACTIVE",
          input.isDefault ? 1 : 0,
          safeJson(input.settingsJson || {}),
          null,
          secretsEnvelope ? serializeEnvelope(secretsEnvelope) : null,
          secretsEnvelope ? String(secretsEnvelope.kid || "") : null,
          secretsEnvelope ? new Date() : null,
          userId,
          userId,
        ]
      );
      createdId = parsePositiveInt(ins.rows?.insertId);
      if (!createdId) {
        throw new Error("Failed to create payroll provider connection");
      }

      if (input.secretsJson !== undefined) {
        await writeSensitiveDataAudit({
          tenantId,
          legalEntityId: input.legalEntityId,
          moduleCode: "PAYROLL",
          objectType: "PROVIDER_CONNECTION",
          objectId: createdId,
          action: "ENCRYPTED_WRITE",
          payload: {
            provider_code: input.providerCode,
            has_secrets: Boolean(secretsEnvelope),
          },
          note: "Provider connection created with encrypted secrets",
          userId,
          runQuery: tx.query,
        });
      }
    });
  } catch (err) {
    if (isDup(err)) throw conflict("Payroll provider connection could not be created (duplicate)");
    throw err;
  }
  return mapConnection(await requireConnection({ tenantId, connectionId: createdId }));
}

export async function updatePayrollProviderConnection({
  req,
  tenantId,
  userId,
  connectionId,
  input,
  assertScopeAccess,
}) {
  const current = await requireConnection({ tenantId, connectionId });
  assertLeScope(req, assertScopeAccess, current.legal_entity_id, "connectionId");
  try {
    await withTransaction(async (tx) => {
      const locked = await requireConnection({ tenantId, connectionId, runQuery: tx.query, forUpdate: true });
      let updateSecrets = false;
      let secretsEnvelope = null;
      if (input.secretsJson !== undefined) {
        updateSecrets = true;
        secretsEnvelope = input.secretsJson ? encryptJson(input.secretsJson) : null;
      }
      if (input.isDefault === true) {
        await tx.query(
          `UPDATE payroll_provider_connections
           SET is_default = 0, updated_by_user_id = ?, updated_at = CURRENT_TIMESTAMP
           WHERE tenant_id = ? AND legal_entity_id = ?`,
          [userId, tenantId, locked.legal_entity_id]
        );
      }
      await tx.query(
        `UPDATE payroll_provider_connections
         SET provider_name = COALESCE(?, provider_name),
             adapter_version = COALESCE(?, adapter_version),
             status = COALESCE(?, status),
             is_default = COALESCE(?, is_default),
             settings_json = COALESCE(?, settings_json),
             secrets_json = CASE WHEN ? THEN NULL ELSE secrets_json END,
             secrets_encrypted_json = CASE WHEN ? THEN ? ELSE secrets_encrypted_json END,
             secrets_key_version = CASE WHEN ? THEN ? ELSE secrets_key_version END,
             secrets_migrated_at = CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE secrets_migrated_at END,
             updated_by_user_id = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ? AND id = ?`,
        [
          input.providerName ?? null,
          input.adapterVersion ?? null,
          input.status ?? null,
          input.isDefault === undefined ? null : input.isDefault ? 1 : 0,
          input.settingsJson === undefined ? null : safeJson(input.settingsJson),
          updateSecrets ? 1 : 0,
          updateSecrets ? 1 : 0,
          updateSecrets && secretsEnvelope ? serializeEnvelope(secretsEnvelope) : null,
          updateSecrets ? 1 : 0,
          updateSecrets && secretsEnvelope ? String(secretsEnvelope.kid || "") : null,
          updateSecrets ? 1 : 0,
          userId,
          tenantId,
          connectionId,
        ]
      );

      if (updateSecrets) {
        await writeSensitiveDataAudit({
          tenantId,
          legalEntityId: locked.legal_entity_id,
          moduleCode: "PAYROLL",
          objectType: "PROVIDER_CONNECTION",
          objectId: connectionId,
          action: "ENCRYPTED_WRITE",
          payload: {
            has_secrets: Boolean(secretsEnvelope),
          },
          note: "Provider connection secrets rotated/updated",
          userId,
          runQuery: tx.query,
        });
      }
    });
  } catch (err) {
    if (isDup(err)) throw conflict("Payroll provider connection update conflicts with existing data");
    throw err;
  }
  return mapConnection(await requireConnection({ tenantId, connectionId }));
}

export async function listPayrollEmployeeProviderRefs({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["r.tenant_id = ?"];
  conditions.push(
    scopeListCondition({
      req,
      filters,
      params,
      buildScopeFilter,
      assertScopeAccess,
      columnName: "r.legal_entity_id",
    })
  );
  if (filters.providerCode) {
    conditions.push("r.provider_code = ?");
    params.push(filters.providerCode);
  }
  if (filters.q) {
    const like = `%${filters.q}%`;
    conditions.push(
      "(r.external_employee_id LIKE ? OR r.external_employee_code LIKE ? OR r.internal_employee_code LIKE ? OR r.internal_employee_name LIKE ?)"
    );
    params.push(like, like, like, like);
  }
  const whereSql = conditions.join(" AND ");
  const countRes = await query(
    `SELECT COUNT(*) AS total FROM payroll_employee_provider_refs r WHERE ${whereSql}`,
    params
  );
  const total = Number(countRes.rows?.[0]?.total || 0);
  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 200;
  const safeOffset = Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const listRes = await query(
    `SELECT r.*, le.code AS legal_entity_code, le.name AS legal_entity_name
     FROM payroll_employee_provider_refs r
     JOIN legal_entities le ON le.tenant_id = r.tenant_id AND le.id = r.legal_entity_id
     WHERE ${whereSql}
     ORDER BY r.provider_code ASC, r.external_employee_id ASC, r.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );
  return {
    rows: (listRes.rows || []).map(mapRef),
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function upsertPayrollEmployeeProviderRef({ req, tenantId, userId, input, assertScopeAccess }) {
  await assertLegalEntityBelongsToTenant(tenantId, input.legalEntityId, "legalEntityId");
  assertLeScope(req, assertScopeAccess, input.legalEntityId, "legalEntityId");
  await withTransaction(async (tx) => {
    if (input.isPrimary) {
      await tx.query(
        `UPDATE payroll_employee_provider_refs
         SET is_primary = 0, updated_by_user_id = ?, updated_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ? AND legal_entity_id = ? AND provider_code = ? AND external_employee_id = ?`,
        [userId, tenantId, input.legalEntityId, input.providerCode, input.externalEmployeeId]
      );
    }
    await tx.query(
      `INSERT INTO payroll_employee_provider_refs (
          tenant_id, legal_entity_id, provider_code,
          external_employee_id, external_employee_code,
          internal_employee_code, internal_employee_name,
          status, is_primary, payload_json,
          created_by_user_id, updated_by_user_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          external_employee_code = VALUES(external_employee_code),
          internal_employee_code = VALUES(internal_employee_code),
          internal_employee_name = VALUES(internal_employee_name),
          status = VALUES(status),
          is_primary = VALUES(is_primary),
          payload_json = VALUES(payload_json),
          updated_by_user_id = VALUES(updated_by_user_id),
          updated_at = CURRENT_TIMESTAMP`,
      [
        tenantId,
        input.legalEntityId,
        input.providerCode,
        input.externalEmployeeId,
        input.externalEmployeeCode || null,
        input.internalEmployeeCode,
        input.internalEmployeeName || null,
        input.status || "ACTIVE",
        input.isPrimary ? 1 : 0,
        input.payloadJson === undefined ? null : safeJson(input.payloadJson),
        userId,
        userId,
      ]
    );
  });
  const res = await query(
    `SELECT * FROM payroll_employee_provider_refs
     WHERE tenant_id = ? AND legal_entity_id = ? AND provider_code = ? AND external_employee_id = ?
     LIMIT 1`,
    [tenantId, input.legalEntityId, input.providerCode, input.externalEmployeeId]
  );
  return mapRef(res.rows?.[0] || null);
}

function chooseRef(candidates = []) {
  if (!candidates.length) return { row: null, ambiguous: false };
  if (candidates.length === 1) return { row: candidates[0], ambiguous: false };
  const primaries = candidates.filter((r) => Number(r.is_primary || 0) === 1);
  if (primaries.length === 1) return { row: primaries[0], ambiguous: false };
  return { row: null, ambiguous: true };
}

async function resolveEmployeeMappings({ tenantId, legalEntityId, providerCode, employees, runQuery = query }) {
  const rows = Array.isArray(employees) ? employees : [];
  const extIds = Array.from(new Set(rows.map((r) => String(r.external_employee_id || "").trim()).filter(Boolean)));
  const extCodes = Array.from(
    new Set(rows.map((r) => String(r.external_employee_code || "").trim()).filter(Boolean))
  );
  let refs = [];
  if (extIds.length || extCodes.length) {
    const params = [tenantId, legalEntityId, providerCode];
    const ors = [];
    if (extIds.length) {
      ors.push(`external_employee_id IN (${extIds.map(() => "?").join(", ")})`);
      params.push(...extIds);
    }
    if (extCodes.length) {
      ors.push(`external_employee_code IN (${extCodes.map(() => "?").join(", ")})`);
      params.push(...extCodes);
    }
    const res = await runQuery(
      `SELECT *
       FROM payroll_employee_provider_refs
       WHERE tenant_id = ? AND legal_entity_id = ? AND provider_code = ? AND status = 'ACTIVE'
         AND (${ors.join(" OR ")})`,
      params
    );
    refs = res.rows || [];
  }
  const byId = new Map();
  const byCode = new Map();
  for (const ref of refs) {
    const k1 = String(ref.external_employee_id || "").trim();
    const k2 = String(ref.external_employee_code || "").trim();
    if (k1) (byId.get(k1) || byId.set(k1, []).get(k1)).push(ref);
    if (k2) (byCode.get(k2) || byCode.set(k2, []).get(k2)).push(ref);
  }
  const mappedEmployees = [];
  const applyRows = [];
  const matchErrors = [];
  const matchWarnings = [];
  rows.forEach((r, i) => {
    const extId = String(r.external_employee_id || "").trim();
    const extCode = String(r.external_employee_code || "").trim() || null;
    const name = String(r.employee_name || "").trim() || null;
    let internalCode = null;
    let method = null;
    let refId = null;

    if (extId) {
      const pick = chooseRef(byId.get(extId) || []);
      if (pick.ambiguous) {
        matchErrors.push({ row_no: i + 1, external_employee_id: extId, external_employee_code: extCode, employee_name: name, reason: "Ambiguous mapping by external_employee_id" });
        return;
      }
      if (pick.row?.internal_employee_code) {
        internalCode = String(pick.row.internal_employee_code || "").trim();
        method = "REF_EXTERNAL_ID";
        refId = parsePositiveInt(pick.row.id);
      }
    }
    if (!internalCode && extCode) {
      const pick = chooseRef(byCode.get(extCode) || []);
      if (pick.ambiguous) {
        matchErrors.push({ row_no: i + 1, external_employee_id: extId || null, external_employee_code: extCode, employee_name: name, reason: "Ambiguous mapping by external_employee_code" });
        return;
      }
      if (pick.row?.internal_employee_code) {
        internalCode = String(pick.row.internal_employee_code || "").trim();
        method = "REF_EXTERNAL_CODE";
        refId = parsePositiveInt(pick.row.id);
        matchWarnings.push({ row_no: i + 1, warning: "Fallback mapping by external_employee_code", external_employee_id: extId || null, external_employee_code: extCode });
      }
    }
    if (!internalCode && extCode) {
      internalCode = extCode;
      method = "DIRECT_EMPLOYEE_CODE";
      matchWarnings.push({ row_no: i + 1, warning: "Fallback mapping by direct employee_code", external_employee_id: extId || null, external_employee_code: extCode });
    }
    if (!internalCode) {
      matchErrors.push({ row_no: i + 1, external_employee_id: extId || null, external_employee_code: extCode, employee_name: name, reason: "No employee mapping found" });
      return;
    }
    mappedEmployees.push({ ...r, internal_employee_code: internalCode, mapping_method: method, mapping_ref_id: refId });
    applyRows.push({
      line_no: applyRows.length + 1,
      employee_code: internalCode,
      employee_name: r.employee_name || internalCode,
      cost_center_code: r.cost_center_code || null,
      base_salary: toAmount(r.base_salary),
      overtime_pay: toAmount(r.overtime_pay),
      bonus_pay: toAmount(r.bonus_pay),
      allowances_total: toAmount(r.allowances_total),
      gross_pay: toAmount(r.gross_pay),
      employee_tax: toAmount(r.employee_tax),
      employee_social_security: toAmount(r.employee_social_security),
      other_deductions: toAmount(r.other_deductions),
      employer_tax: toAmount(r.employer_tax),
      employer_social_security: toAmount(r.employer_social_security),
      net_pay: toAmount(r.net_pay),
    });
  });
  return { mappedEmployees, applyRows, matchErrors, matchWarnings };
}

function buildPreviewSummary({ payload, applyRows, validationErrors, schemaWarnings, matchErrors, matchWarnings }) {
  const summary = payload?.summary || {};
  const employees = payload?.employees || [];
  return {
    payroll_period: payload?.run?.payroll_period || null,
    pay_date: payload?.run?.pay_date || null,
    currency_code: payload?.run?.currency_code || null,
    employee_count_in_payload: Number(summary.employee_count || employees.length || 0),
    employee_count_mapped: Number(applyRows?.length || 0),
    employee_count_unmatched: Number(matchErrors?.length || 0),
    total_gross_pay: toAmount(summary.total_gross_pay),
    total_net_pay: toAmount(summary.total_net_pay),
    total_employer_tax: toAmount(summary.total_employer_tax),
    total_employer_social_security: toAmount(summary.total_employer_social_security),
    validation_error_count: Number(validationErrors?.length || 0),
    schema_warning_count: Number(schemaWarnings?.length || 0),
    match_warning_count: Number(matchWarnings?.length || 0),
    apply_blocked:
      (validationErrors?.length || 0) > 0 || (matchErrors?.length || 0) > 0 || (applyRows?.length || 0) === 0,
  };
}

export async function previewPayrollProviderImport({ req, tenantId, userId, input, assertScopeAccess }) {
  await assertCurrencyExists(input.currencyCode, "currencyCode");
  const connection = await requireConnection({ tenantId, connectionId: input.payrollProviderConnectionId });
  assertLeScope(req, assertScopeAccess, connection.legal_entity_id, "payrollProviderConnectionId");
  if (up(connection.status) !== "ACTIVE") throw conflict("Payroll provider connection is not ACTIVE");
  await assertLegalEntityBelongsToTenant(tenantId, connection.legal_entity_id, "connection legal entity");

  const providerCode = up(connection.provider_code);
  const rawPayloadText = normalizePayloadText(input.rawPayloadText);
  const rawPayloadHash = sha256(rawPayloadText);

  if (input.importKey) {
    const byKey = await query(
      `SELECT id FROM payroll_provider_import_jobs
       WHERE tenant_id = ? AND legal_entity_id = ? AND provider_code = ? AND import_key = ?
       ORDER BY id DESC LIMIT 1`,
      [tenantId, connection.legal_entity_id, providerCode, input.importKey]
    );
    const existingId = parsePositiveInt(byKey.rows?.[0]?.id);
    if (existingId) {
      const detail = await getPayrollProviderImportJobDetail({ req, tenantId, importJobId: existingId, assertScopeAccess });
      return { ...detail, idempotent_preview: true };
    }
  }
  const byHash = await query(
    `SELECT id FROM payroll_provider_import_jobs
     WHERE tenant_id = ? AND legal_entity_id = ? AND provider_code = ? AND payroll_period = ? AND raw_payload_hash = ?
     ORDER BY id DESC LIMIT 1`,
    [tenantId, connection.legal_entity_id, providerCode, input.payrollPeriod, rawPayloadHash]
  );
  if (parsePositiveInt(byHash.rows?.[0]?.id)) {
    const detail = await getPayrollProviderImportJobDetail({ req, tenantId, importJobId: byHash.rows[0].id, assertScopeAccess });
    return { ...detail, idempotent_preview: true };
  }

  const adapter = getPayrollProviderAdapter(providerCode, {
    settings: parseJsonMaybe(connection.settings_json) || {},
    adapterVersion: connection.adapter_version || "v1",
  });

  let schemaWarnings = [];
  let validationErrors = [];
  let normalized = null;
  let applyRows = [];
  let mappedEmployees = [];
  let matchErrors = [];
  let matchWarnings = [];
  try {
    const parsed = adapter.parseRaw(rawPayloadText, { sourceFormat: input.sourceFormat });
    const schema = adapter.validateSchema(parsed, { sourceFormat: input.sourceFormat }) || {};
    schemaWarnings = (schema.warnings || []).map((v) => String(v));
    validationErrors = (schema.errors || []).map((v) => String(v));
    if (validationErrors.length === 0) {
      normalized = adapter.normalizePayrollResults(parsed, {
        payrollPeriod: input.payrollPeriod,
        payDate: input.payDate,
        currencyCode: input.currencyCode,
        sourceBatchRef: input.sourceBatchRef,
      });
      const mapping = await resolveEmployeeMappings({
        tenantId,
        legalEntityId: connection.legal_entity_id,
        providerCode,
        employees: normalized.employees || [],
      });
      applyRows = mapping.applyRows || [];
      mappedEmployees = mapping.mappedEmployees || [];
      matchErrors = mapping.matchErrors || [];
      matchWarnings = mapping.matchWarnings || [];
    }
  } catch (err) {
    validationErrors = [String(err?.message || "Provider import preview failed")];
  }

  const canonicalPayload = {
    schema_version: "prp09.v1",
    run: {
      payroll_period: input.payrollPeriod,
      period_start: input.periodStart || input.payrollPeriod,
      period_end: input.periodEnd || input.payrollPeriod,
      pay_date: input.payDate,
      currency_code: input.currencyCode,
      source_batch_ref: input.sourceBatchRef || null,
    },
    source: {
      payroll_provider_connection_id: parsePositiveInt(connection.id),
      provider_code: providerCode,
      adapter_version: connection.adapter_version || "v1",
      source_format: input.sourceFormat,
      source_filename: input.sourceFilename || null,
      import_key: input.importKey || null,
    },
    summary: normalized?.summary || {},
    employees: mappedEmployees,
    apply_rows: applyRows,
  };
  const allMatchWarnings = [
    ...schemaWarnings.map((warning) => ({ warning })),
    ...matchWarnings,
  ];
  const previewSummary = buildPreviewSummary({
    payload: canonicalPayload,
    applyRows,
    validationErrors,
    schemaWarnings,
    matchErrors,
    matchWarnings: allMatchWarnings,
  });
  const normalizedHash = validationErrors.length ? null : sha256(JSON.stringify(canonicalPayload));

  let importJobId = null;
  try {
    await withTransaction(async (tx) => {
      const ins = await tx.query(
        `INSERT INTO payroll_provider_import_jobs (
            tenant_id, legal_entity_id, payroll_provider_connection_id,
            provider_code, adapter_version,
            payroll_period, period_start, period_end, pay_date, currency_code,
            import_key, raw_payload_hash, normalized_payload_hash,
            source_format, source_filename, status,
            preview_summary_json, validation_errors_json, match_errors_json, match_warnings_json,
            raw_payload_text, normalized_payload_json, requested_by_user_id
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PREVIEWED', ?, ?, ?, ?, ?, ?, ?)`,
        [
          tenantId,
          connection.legal_entity_id,
          connection.id,
          providerCode,
          connection.adapter_version || "v1",
          input.payrollPeriod,
          input.periodStart || input.payrollPeriod,
          input.periodEnd || input.payrollPeriod,
          input.payDate,
          input.currencyCode,
          input.importKey || null,
          rawPayloadHash,
          normalizedHash,
          input.sourceFormat,
          input.sourceFilename || null,
          safeJson(previewSummary),
          safeJson(validationErrors),
          safeJson(matchErrors),
          safeJson(allMatchWarnings),
          rawPayloadText,
          safeJson(canonicalPayload),
          userId,
        ]
      );
      importJobId = parsePositiveInt(ins.rows?.insertId);
      await writeImportAudit({
        tenantId,
        legalEntityId: connection.legal_entity_id,
        importJobId,
        action: "PREVIEWED",
        payload: { preview_summary: previewSummary },
        note: "Previewed payroll provider import",
        userId,
        runQuery: tx.query,
      });
    });
  } catch (err) {
    if (isDup(err)) {
      const key = dupKey(err);
      if (key.includes("payload_hash") || key.includes("import_key")) {
        const detailRes = key.includes("import_key") && input.importKey
          ? await query(
              `SELECT id FROM payroll_provider_import_jobs
               WHERE tenant_id = ? AND legal_entity_id = ? AND provider_code = ? AND import_key = ?
               ORDER BY id DESC LIMIT 1`,
              [tenantId, connection.legal_entity_id, providerCode, input.importKey]
            )
          : await query(
              `SELECT id FROM payroll_provider_import_jobs
               WHERE tenant_id = ? AND legal_entity_id = ? AND provider_code = ? AND payroll_period = ? AND raw_payload_hash = ?
               ORDER BY id DESC LIMIT 1`,
              [tenantId, connection.legal_entity_id, providerCode, input.payrollPeriod, rawPayloadHash]
            );
        const existingId = parsePositiveInt(detailRes.rows?.[0]?.id);
        if (existingId) {
          const detail = await getPayrollProviderImportJobDetail({ req, tenantId, importJobId: existingId, assertScopeAccess });
          return { ...detail, idempotent_preview: true };
        }
      }
    }
    throw err;
  }

  const detail = await getPayrollProviderImportJobDetail({ req, tenantId, importJobId, assertScopeAccess });
  return { ...detail, preview_summary: previewSummary, idempotent_preview: false };
}

export async function listPayrollProviderImportJobs({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["j.tenant_id = ?"];
  conditions.push(
    scopeListCondition({
      req,
      filters,
      params,
      buildScopeFilter,
      assertScopeAccess,
      columnName: "j.legal_entity_id",
    })
  );
  if (filters.providerCode) {
    conditions.push("j.provider_code = ?");
    params.push(filters.providerCode);
  }
  if (filters.status) {
    conditions.push("j.status = ?");
    params.push(filters.status);
  }
  if (filters.payrollPeriod) {
    conditions.push("j.payroll_period = ?");
    params.push(filters.payrollPeriod);
  }

  const baseConditions = [...conditions];
  const baseParams = [...params];
  const cursorToken = filters.cursor || null;
  const cursor = decodeCursorToken(cursorToken);
  const pageConditions = [...baseConditions];
  const pageParams = [...baseParams];
  if (cursorToken) {
    const cursorRequestedAt = requireCursorDateTime(cursor, "requestedAt");
    const cursorId = requireCursorId(cursor, "id");
    pageConditions.push("(j.requested_at < ? OR (j.requested_at = ? AND j.id < ?))");
    pageParams.push(cursorRequestedAt, cursorRequestedAt, cursorId);
  }

  const whereSql = pageConditions.join(" AND ");
  const countWhereSql = baseConditions.join(" AND ");
  const countRes = await query(
    `SELECT COUNT(*) AS total FROM payroll_provider_import_jobs j WHERE ${countWhereSql}`,
    baseParams
  );
  const total = Number(countRes.rows?.[0]?.total || 0);
  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset = Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const effectiveOffset = cursorToken ? 0 : safeOffset;
  const listRes = await query(
    `SELECT
        j.id, j.tenant_id, j.legal_entity_id, j.payroll_provider_connection_id,
        j.provider_code, j.adapter_version, j.payroll_period, j.period_start, j.period_end,
        j.pay_date, j.currency_code, j.import_key, j.source_format, j.source_filename,
        j.status, j.preview_summary_json, j.validation_errors_json, j.match_errors_json,
        j.match_warnings_json, j.applied_payroll_run_id, j.requested_by_user_id, j.requested_at,
        j.applied_by_user_id, j.applied_at, j.failure_message, j.created_at, j.updated_at,
        le.code AS legal_entity_code, le.name AS legal_entity_name
     FROM payroll_provider_import_jobs j
     JOIN legal_entities le ON le.tenant_id = j.tenant_id AND le.id = j.legal_entity_id
     WHERE ${whereSql}
     ORDER BY j.requested_at DESC, j.id DESC
     LIMIT ${safeLimit} OFFSET ${effectiveOffset}`,
    pageParams
  );
  const rawRows = listRes.rows || [];
  const lastRow = rawRows.length > 0 ? rawRows[rawRows.length - 1] : null;
  const nextCursor =
    cursorToken || safeOffset === 0
      ? rawRows.length === safeLimit && lastRow
        ? encodeCursorToken({
            requestedAt: toCursorDateTime(lastRow.requested_at),
            id: parsePositiveInt(lastRow.id),
          })
        : null
      : null;
  return {
    rows: rawRows.map((row) => mapJob(row)),
    total,
    limit: filters.limit,
    offset: cursorToken ? 0 : filters.offset,
    pageMode: cursorToken ? "CURSOR" : "OFFSET",
    nextCursor,
  };
}

export async function getPayrollProviderImportJobDetail({ req, tenantId, importJobId, assertScopeAccess }) {
  const job = await requireJob({ tenantId, importJobId });
  assertLeScope(req, assertScopeAccess, job.legal_entity_id, "importJobId");
  const auditRes = await query(
    `SELECT *
     FROM payroll_provider_import_audit
     WHERE tenant_id = ? AND legal_entity_id = ? AND payroll_provider_import_job_id = ?
     ORDER BY id DESC`,
    [tenantId, job.legal_entity_id, importJobId]
  );

  let appliedRun = null;
  const appliedRunId = parsePositiveInt(job.applied_payroll_run_id);
  if (appliedRunId) {
    try {
      appliedRun = await getPayrollRunByIdForTenant({ req, tenantId, runId: appliedRunId, assertScopeAccess });
    } catch {
      appliedRun = null;
    }
  }

  return {
    job: mapJob(job, { includeRaw: true }),
    audit: (auditRes.rows || []).map(mapAudit),
    applied_payroll_run: appliedRun,
  };
}

export async function maskPayrollProviderImportRawPayload({
  req,
  tenantId,
  userId,
  importJobId,
  reason = "Manual mask",
  assertScopeAccess,
}) {
  let legalEntityId = null;

  await withTransaction(async (tx) => {
    const job = await requireJob({ tenantId, importJobId, runQuery: tx.query, forUpdate: true });
    legalEntityId = parsePositiveInt(job.legal_entity_id);
    assertLeScope(req, assertScopeAccess, legalEntityId, "importJobId");

    const currentStatus = up(job.raw_payload_retention_status || "ACTIVE");
    if (currentStatus === "PURGED") {
      throw conflict("Raw payload already purged");
    }

    const redacted = redactRawPayloadText(job.raw_payload_text || "");
    await tx.query(
      `UPDATE payroll_provider_import_jobs
       SET raw_payload_text = ?,
           raw_payload_retention_status = 'MASKED',
           raw_payload_masked_at = CURRENT_TIMESTAMP,
           raw_payload_redaction_note = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND id = ?`,
      [redacted, reason, tenantId, importJobId]
    );

    await writeSensitiveDataAudit({
      tenantId,
      legalEntityId,
      moduleCode: "PAYROLL",
      objectType: "IMPORT_JOB",
      objectId: importJobId,
      action: "MASKED_PAYLOAD",
      payload: { previous_status: currentStatus },
      note: reason,
      userId,
      runQuery: tx.query,
    });

    await writeImportAudit({
      tenantId,
      legalEntityId,
      importJobId,
      action: "STATUS",
      payload: { retention_action: "MASK", previous_status: currentStatus },
      note: reason,
      userId,
      runQuery: tx.query,
    });
  });

  return getPayrollProviderImportJobDetail({ req, tenantId, importJobId, assertScopeAccess });
}

export async function purgePayrollProviderImportRawPayload({
  req,
  tenantId,
  userId,
  importJobId,
  reason = "Manual purge",
  assertScopeAccess,
}) {
  let legalEntityId = null;

  await withTransaction(async (tx) => {
    const job = await requireJob({ tenantId, importJobId, runQuery: tx.query, forUpdate: true });
    legalEntityId = parsePositiveInt(job.legal_entity_id);
    assertLeScope(req, assertScopeAccess, legalEntityId, "importJobId");

    const currentStatus = up(job.raw_payload_retention_status || "ACTIVE");
    if (currentStatus === "PURGED") {
      // idempotent
      return;
    }

    await tx.query(
      `UPDATE payroll_provider_import_jobs
       SET raw_payload_text = NULL,
           raw_payload_retention_status = 'PURGED',
           raw_payload_purged_at = CURRENT_TIMESTAMP,
           raw_payload_redaction_note = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND id = ?`,
      [reason, tenantId, importJobId]
    );

    await writeSensitiveDataAudit({
      tenantId,
      legalEntityId,
      moduleCode: "PAYROLL",
      objectType: "IMPORT_JOB",
      objectId: importJobId,
      action: "PURGED_PAYLOAD",
      payload: { previous_status: currentStatus },
      note: reason,
      userId,
      runQuery: tx.query,
    });

    await writeImportAudit({
      tenantId,
      legalEntityId,
      importJobId,
      action: "STATUS",
      payload: { retention_action: "PURGE", previous_status: currentStatus },
      note: reason,
      userId,
      runQuery: tx.query,
    });
  });

  return getPayrollProviderImportJobDetail({ req, tenantId, importJobId, assertScopeAccess });
}

async function findExistingRunByChecksum({
  tenantId,
  legalEntityId,
  providerCode,
  payrollPeriod,
  fileChecksum,
  runQuery = query,
}) {
  const res = await runQuery(
    `SELECT id
     FROM payroll_runs
     WHERE tenant_id = ? AND legal_entity_id = ? AND provider_code = ? AND payroll_period = ? AND file_checksum = ?
     ORDER BY id DESC LIMIT 1`,
    [tenantId, legalEntityId, providerCode, payrollPeriod, fileChecksum]
  );
  return parsePositiveInt(res.rows?.[0]?.id);
}

async function setRunSourceTrace({
  tenantId,
  legalEntityId,
  runId,
  providerCode,
  importJobId,
  runQuery = query,
}) {
  await runQuery(
    `UPDATE payroll_runs
     SET source_type = 'PROVIDER_IMPORT',
         source_provider_code = ?,
         source_provider_import_job_id = ?
     WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
    [providerCode, importJobId, tenantId, legalEntityId, runId]
  );
}

function readNormalizedPayload(job) {
  const payload = parseJsonMaybe(job.normalized_payload_json) || {};
  return {
    payload,
    run: payload.run || {},
    source: payload.source || {},
    applyRows: Array.isArray(payload.apply_rows) ? payload.apply_rows : [],
  };
}

async function markJobFailed({ tenantId, importJobId, userId, errorMessage }) {
  try {
    await withTransaction(async (tx) => {
      const job = await findJob({ tenantId, importJobId, runQuery: tx.query, forUpdate: true });
      if (!job || up(job.status) === "APPLIED") return;
      await tx.query(
        `UPDATE payroll_provider_import_jobs
         SET status = 'FAILED', failure_message = ?, updated_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ? AND id = ?`,
        [String(errorMessage || "Import apply failed").slice(0, 500), tenantId, importJobId]
      );
      await writeImportAudit({
        tenantId,
        legalEntityId: job.legal_entity_id,
        importJobId,
        action: "FAILED",
        payload: { error: String(errorMessage || "Import apply failed") },
        note: "Payroll provider import apply failed",
        userId,
        runQuery: tx.query,
      });
    });
  } catch {
    // ignore secondary errors
  }
}

function defaultImportFilename(job) {
  const ext = up(job?.source_format) === "JSON" ? "json" : "csv";
  return `${up(job?.provider_code || "PAYROLL")}-${toDateOnly(job?.payroll_period) || "period"}.${ext}`;
}

export async function enqueuePayrollProviderImportApplyJob({
  req,
  tenantId,
  userId,
  importJobId,
  input,
  assertScopeAccess,
}) {
  const job = await requireJob({ tenantId, importJobId });
  assertLeScope(req, assertScopeAccess, job.legal_entity_id, "importJobId");

  if (up(job.status) === "APPLIED") {
    throw conflict("Payroll provider import job is already applied");
  }
  if (!["PREVIEWED", "FAILED", "APPLYING"].includes(up(job.status))) {
    throw conflict(`Payroll provider import job status ${job.status} cannot be queued for apply`);
  }

  const jobsModule = await import("./jobs.service.js");
  return jobsModule.enqueueJob({
    tenantId,
    userId,
    spec: {
      queue_name: "payroll.imports",
      module_code: "PAYROLL",
      job_type: "PAYROLL_IMPORT_APPLY",
      priority: 50,
      idempotency_key:
        input.applyIdempotencyKey || `PAYROLL_IMPORT_APPLY:${tenantId}:${importJobId}`,
      max_attempts: 5,
      payload: {
        import_job_id: importJobId,
        acting_user_id: userId || null,
        apply_note: input.note || "Queued payroll provider import apply",
        allow_same_user_apply: Boolean(input.allowSameUserApply),
        retry_base_seconds: 30,
        retry_max_seconds: 1800,
      },
    },
  });
}

export async function applyPayrollProviderImport({
  req,
  tenantId,
  userId,
  importJobId,
  input,
  assertScopeAccess,
  skipUnifiedApprovalGate = false,
  approvalRequestId = null,
}) {
  if (input.applyIdempotencyKey) {
    const idemRes = await query(
      `SELECT id, status
       FROM payroll_provider_import_jobs
       WHERE tenant_id = ? AND apply_idempotency_key = ?
       LIMIT 1`,
      [tenantId, input.applyIdempotencyKey]
    );
    const idem = idemRes.rows?.[0] || null;
    if (idem && parsePositiveInt(idem.id) !== importJobId) {
      throw conflict("applyIdempotencyKey is already used by another payroll provider import job");
    }
    if (idem && parsePositiveInt(idem.id) === importJobId && up(idem.status) === "APPLIED") {
      return getPayrollProviderImportJobDetail({ req, tenantId, importJobId, assertScopeAccess });
    }
  }

  if (!skipUnifiedApprovalGate) {
    const previewJob = await requireJob({ tenantId, importJobId });
    assertLeScope(req, assertScopeAccess, previewJob.legal_entity_id, "importJobId");
    if (!["APPLIED"].includes(up(previewJob.status))) {
      const gov = await evaluateApprovalNeed({
        moduleCode: "PAYROLL",
        tenantId,
        targetType: "PAYROLL_PROVIDER_IMPORT",
        actionType: "APPLY",
        legalEntityId: parsePositiveInt(previewJob.legal_entity_id),
        thresholdAmount: deriveImportApprovalThreshold(previewJob),
        currencyCode: up(previewJob.currency_code),
      });
      if (gov?.approval_required || gov?.approvalRequired) {
        const submitRes = await submitApprovalRequest({
          tenantId,
          userId,
          requestInput: {
            moduleCode: "PAYROLL",
            requestKey: `PRP09:IMPORT_APPLY:${tenantId}:${importJobId}`,
            targetType: "PAYROLL_PROVIDER_IMPORT",
            targetId: importJobId,
            actionType: "APPLY",
            legalEntityId: parsePositiveInt(previewJob.legal_entity_id),
            thresholdAmount: deriveImportApprovalThreshold(previewJob),
            currencyCode: up(previewJob.currency_code),
            actionPayload: {
              importJobId,
              note: input.note || null,
              allowSameUserApply: Boolean(input.allowSameUserApply),
            },
            targetSnapshot: {
              module_code: "PAYROLL",
              target_type: "PAYROLL_PROVIDER_IMPORT",
              target_id: importJobId,
              legal_entity_id: parsePositiveInt(previewJob.legal_entity_id),
              provider_code: up(previewJob.provider_code),
              payroll_period: toDateOnly(previewJob.payroll_period),
              status: up(previewJob.status),
              threshold_amount: deriveImportApprovalThreshold(previewJob),
              currency_code: up(previewJob.currency_code),
            },
          },
        });
        return {
          approval_required: true,
          approval_request: submitRes?.item || null,
          idempotent: Boolean(submitRes?.idempotent),
          job: mapJob(previewJob),
        };
      }
    }
  }

  let preJob = null;
  await withTransaction(async (tx) => {
    const job = await requireJob({ tenantId, importJobId, runQuery: tx.query, forUpdate: true });
    preJob = job;
    assertLeScope(req, assertScopeAccess, job.legal_entity_id, "importJobId");
    if (up(job.status) === "APPLIED") return;
    if (!["PREVIEWED", "FAILED"].includes(up(job.status))) {
      throw conflict(`Payroll provider import job status ${job.status} cannot be applied`);
    }
    if (
      !input.allowSameUserApply &&
      parsePositiveInt(job.requested_by_user_id) &&
      parsePositiveInt(job.requested_by_user_id) === parsePositiveInt(userId)
    ) {
      throw forbidden("Maker-checker violation: preview user cannot apply the same import job");
    }
    const preview = parseJsonMaybe(job.preview_summary_json) || {};
    const validationErrors = parseJsonMaybe(job.validation_errors_json) || [];
    const matchErrors = parseJsonMaybe(job.match_errors_json) || [];
    if ((validationErrors.length || 0) > 0 || (matchErrors.length || 0) > 0 || preview.apply_blocked) {
      throw conflict("Import apply blocked: preview contains validation/matching errors");
    }
    await assertPayrollPeriodActionAllowed({
      tenantId,
      legalEntityId: job.legal_entity_id,
      payrollPeriod: job.payroll_period,
      actionType: "RUN_IMPORT_APPLY",
      runQuery: tx.query,
    });
    await tx.query(
      `UPDATE payroll_provider_import_jobs
       SET status = 'APPLYING',
           apply_idempotency_key = COALESCE(?, apply_idempotency_key),
           failure_message = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND id = ?`,
      [input.applyIdempotencyKey || null, tenantId, importJobId]
    );
    await writeImportAudit({
      tenantId,
      legalEntityId: job.legal_entity_id,
      importJobId,
      action: "APPLY_STARTED",
      payload: {
        allow_same_user_apply: Boolean(input.allowSameUserApply),
        approval_request_id: parsePositiveInt(approvalRequestId) || null,
      },
      note: input.note || "Applying payroll provider import",
      userId,
      runQuery: tx.query,
    });
  });

  if (preJob && up(preJob.status) === "APPLIED") {
    return getPayrollProviderImportJobDetail({ req, tenantId, importJobId, assertScopeAccess });
  }

  try {
    const job = preJob || (await requireJob({ tenantId, importJobId }));
    const { run, source, applyRows } = readNormalizedPayload(job);
    if (!applyRows.length) {
      throw conflict("Import apply blocked: normalized payload has no mapped rows");
    }

    const providerCode = up(job.provider_code);
    const payrollPeriod = toDateOnly(job.payroll_period);
    const payDate = toDateOnly(job.pay_date) || payrollPeriod;
    const currencyCode = up(job.currency_code);
    const csvText = buildPayrollCsv(applyRows);
    const csvChecksum = sha256(normalizePayloadText(csvText));

    let importedRun = null;
    try {
      importedRun = await importPayrollRunCsv({
        req,
        payload: {
          tenantId,
          userId,
          legalEntityId: parsePositiveInt(job.legal_entity_id),
          targetRunId: null,
          providerCode,
          payrollPeriod,
          payDate,
          currencyCode,
          sourceBatchRef: run?.source_batch_ref || `PRP09-JOB-${importJobId}`,
          originalFilename: job.source_filename || source.source_filename || defaultImportFilename(job),
          csvText,
        },
        assertScopeAccess,
      });
    } catch (err) {
      if (err?.status !== 409) throw err;
      const existingRunId = await findExistingRunByChecksum({
        tenantId,
        legalEntityId: parsePositiveInt(job.legal_entity_id),
        providerCode,
        payrollPeriod,
        fileChecksum: csvChecksum,
      });
      if (!existingRunId) throw err;
      importedRun = await getPayrollRunByIdForTenant({ req, tenantId, runId: existingRunId, assertScopeAccess });
    }

    const runId = parsePositiveInt(importedRun?.id);
    if (!runId) throw new Error("Payroll provider import apply failed to resolve payroll run");

    await withTransaction(async (tx) => {
      const locked = await requireJob({ tenantId, importJobId, runQuery: tx.query, forUpdate: true });
      assertLeScope(req, assertScopeAccess, locked.legal_entity_id, "importJobId");
      if (up(locked.status) === "APPLIED") return;
      await setRunSourceTrace({
        tenantId,
        legalEntityId: parsePositiveInt(locked.legal_entity_id),
        runId,
        providerCode,
        importJobId,
        runQuery: tx.query,
      });
      await tx.query(
        `UPDATE payroll_provider_import_jobs
         SET status = 'APPLIED',
             applied_payroll_run_id = ?,
             applied_by_user_id = ?,
             applied_at = CURRENT_TIMESTAMP,
             failure_message = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ? AND id = ?`,
        [runId, userId, tenantId, importJobId]
      );
      await writeImportAudit({
        tenantId,
        legalEntityId: locked.legal_entity_id,
        importJobId,
        action: "APPLIED",
        payload: {
          payroll_run_id: runId,
          line_count: applyRows.length,
          approval_request_id: parsePositiveInt(approvalRequestId) || null,
        },
        note: input.note || "Applied payroll provider import",
        userId,
        runQuery: tx.query,
      });
    });
  } catch (err) {
    await markJobFailed({ tenantId, importJobId, userId, errorMessage: err?.message });
    throw err;
  }

  return getPayrollProviderImportJobDetail({ req, tenantId, importJobId, assertScopeAccess });
}

export async function executeApprovedPayrollProviderImportApply({
  tenantId,
  approvalRequestId,
  approvedByUserId,
  payload = {},
}) {
  const importJobId = parsePositiveInt(payload?.importJobId ?? payload?.import_job_id);
  if (!importJobId) {
    throw badRequest("Approved payroll provider import payload is missing importJobId");
  }
  return applyPayrollProviderImport({
    req: null,
    tenantId,
    userId: parsePositiveInt(approvedByUserId) || null,
    importJobId,
    input: {
      applyIdempotencyKey:
        String((payload?.applyIdempotencyKey ?? payload?.apply_idempotency_key) || "").trim() ||
        `H04:${approvalRequestId}|PAYROLL_PROVIDER_IMPORT_APPLY`,
      note: String(payload?.note || "").trim() || "Approved payroll provider import apply",
      allowSameUserApply: Boolean(payload?.allowSameUserApply ?? payload?.allow_same_user_apply),
    },
    assertScopeAccess: noopScopeAccess,
    skipUnifiedApprovalGate: true,
    approvalRequestId,
  });
}

export default {
  listSupportedPayrollProviders,
  getConnectionSecretsDecrypted,
  resolvePayrollProviderConnectionScope,
  resolvePayrollProviderImportJobScope,
  listPayrollProviderConnections,
  createPayrollProviderConnection,
  updatePayrollProviderConnection,
  listPayrollEmployeeProviderRefs,
  upsertPayrollEmployeeProviderRef,
  listPayrollProviderImportJobs,
  previewPayrollProviderImport,
  getPayrollProviderImportJobDetail,
  maskPayrollProviderImportRawPayload,
  purgePayrollProviderImportRawPayload,
  enqueuePayrollProviderImportApplyJob,
  applyPayrollProviderImport,
  executeApprovedPayrollProviderImportApply,
};
