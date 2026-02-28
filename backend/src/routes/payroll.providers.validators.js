import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeCode,
  normalizeCurrencyCode,
  normalizeText,
  optionalPositiveInt,
  parseBooleanFlag,
  parseDateOnly,
  parsePagination,
  requirePositiveInt,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const CONNECTION_STATUS_VALUES = ["ACTIVE", "INACTIVE"];
const IMPORT_JOB_STATUS_VALUES = ["PREVIEWED", "APPLYING", "APPLIED", "REJECTED", "FAILED"];
const SOURCE_FORMAT_VALUES = ["CSV", "JSON"];

function normalizeEnumOrNull(value, label, allowedValues) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const normalized = String(value).trim().toUpperCase();
  if (!allowedValues.includes(normalized)) {
    throw badRequest(`${label} must be one of ${allowedValues.join(", ")}`);
  }
  return normalized;
}

function parseConnectionIdParam(req) {
  const connectionId = parsePositiveInt(req.params?.connectionId ?? req.params?.id);
  if (!connectionId) {
    throw badRequest("connectionId must be a positive integer");
  }
  return connectionId;
}

function parseImportJobIdParam(req) {
  const importJobId = parsePositiveInt(req.params?.importJobId ?? req.params?.jobId ?? req.params?.id);
  if (!importJobId) {
    throw badRequest("importJobId must be a positive integer");
  }
  return importJobId;
}

function parseJsonObjectField(value, label, { required = false } = {}) {
  if (value === undefined || value === null || value === "") {
    if (required) {
      throw badRequest(`${label} is required`);
    }
    return null;
  }
  if (typeof value === "object" && !Array.isArray(value)) {
    return value;
  }
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error("invalid");
      }
      return parsed;
    } catch {
      throw badRequest(`${label} must be a JSON object`);
    }
  }
  throw badRequest(`${label} must be a JSON object`);
}

export function parsePayrollProviderConnectionListInput(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(
    req.query?.legalEntityId ?? req.query?.legal_entity_id,
    "legalEntityId"
  );
  const providerCode = (() => {
    const raw = req.query?.providerCode ?? req.query?.provider_code;
    if (raw === undefined || raw === null || raw === "") return null;
    return normalizeCode(raw, "providerCode", 60);
  })();
  const status = normalizeEnumOrNull(req.query?.status, "status", CONNECTION_STATUS_VALUES);
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 300 });
  return {
    tenantId,
    legalEntityId,
    providerCode,
    status,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parsePayrollProviderConnectionCreateInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    legalEntityId: requirePositiveInt(
      req.body?.legalEntityId ?? req.body?.legal_entity_id,
      "legalEntityId"
    ),
    providerCode: normalizeCode(
      req.body?.providerCode ?? req.body?.provider_code,
      "providerCode",
      60
    ),
    providerName:
      normalizeText(req.body?.providerName ?? req.body?.provider_name, "providerName", 120) || null,
    adapterVersion:
      normalizeText(
        req.body?.adapterVersion ?? req.body?.adapter_version,
        "adapterVersion",
        40
      ) || "v1",
    status:
      normalizeEnumOrNull(req.body?.status, "status", CONNECTION_STATUS_VALUES) || "ACTIVE",
    isDefault: parseBooleanFlag(req.body?.isDefault ?? req.body?.is_default, false),
    settingsJson:
      parseJsonObjectField(req.body?.settingsJson ?? req.body?.settings_json, "settingsJson") || {},
    secretsJson: parseJsonObjectField(req.body?.secretsJson ?? req.body?.secrets_json, "secretsJson"),
    note: normalizeText(req.body?.note, "note", 500),
  };
}

export function parsePayrollProviderConnectionUpdateInput(req) {
  const hasAnyField =
    req.body &&
    [
      "providerName",
      "provider_name",
      "adapterVersion",
      "adapter_version",
      "status",
      "isDefault",
      "is_default",
      "settingsJson",
      "settings_json",
      "secretsJson",
      "secrets_json",
      "note",
    ].some((key) => Object.prototype.hasOwnProperty.call(req.body, key));
  if (!hasAnyField) {
    throw badRequest("At least one updatable field is required");
  }
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    connectionId: parseConnectionIdParam(req),
    providerName: normalizeText(
      req.body?.providerName ?? req.body?.provider_name,
      "providerName",
      120
    ),
    adapterVersion: normalizeText(
      req.body?.adapterVersion ?? req.body?.adapter_version,
      "adapterVersion",
      40
    ),
    status: normalizeEnumOrNull(req.body?.status, "status", CONNECTION_STATUS_VALUES),
    isDefault:
      req.body?.isDefault === undefined && req.body?.is_default === undefined
        ? undefined
        : parseBooleanFlag(req.body?.isDefault ?? req.body?.is_default, false),
    settingsJson:
      req.body?.settingsJson === undefined && req.body?.settings_json === undefined
        ? undefined
        : parseJsonObjectField(req.body?.settingsJson ?? req.body?.settings_json, "settingsJson"),
    secretsJson:
      req.body?.secretsJson === undefined && req.body?.secrets_json === undefined
        ? undefined
        : parseJsonObjectField(req.body?.secretsJson ?? req.body?.secrets_json, "secretsJson"),
    note: normalizeText(req.body?.note, "note", 500),
  };
}

export function parsePayrollEmployeeProviderRefListInput(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = requirePositiveInt(
    req.query?.legalEntityId ?? req.query?.legal_entity_id,
    "legalEntityId"
  );
  const providerCode = (() => {
    const raw = req.query?.providerCode ?? req.query?.provider_code;
    if (raw === undefined || raw === null || raw === "") return null;
    return normalizeCode(raw, "providerCode", 60);
  })();
  const q = normalizeText(req.query?.q, "q", 120);
  const pagination = parsePagination(req.query, { limit: 200, offset: 0, maxLimit: 500 });
  return {
    tenantId,
    legalEntityId,
    providerCode,
    q,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parsePayrollEmployeeProviderRefUpsertInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    legalEntityId: requirePositiveInt(
      req.body?.legalEntityId ?? req.body?.legal_entity_id,
      "legalEntityId"
    ),
    providerCode: normalizeCode(
      req.body?.providerCode ?? req.body?.provider_code,
      "providerCode",
      60
    ),
    externalEmployeeId: normalizeText(
      req.body?.externalEmployeeId ?? req.body?.external_employee_id,
      "externalEmployeeId",
      120,
      { required: true }
    ),
    externalEmployeeCode: normalizeText(
      req.body?.externalEmployeeCode ?? req.body?.external_employee_code,
      "externalEmployeeCode",
      120
    ),
    internalEmployeeCode: normalizeText(
      req.body?.internalEmployeeCode ?? req.body?.internal_employee_code,
      "internalEmployeeCode",
      100,
      { required: true }
    ),
    internalEmployeeName: normalizeText(
      req.body?.internalEmployeeName ?? req.body?.internal_employee_name,
      "internalEmployeeName",
      255
    ),
    status:
      normalizeEnumOrNull(req.body?.status, "status", CONNECTION_STATUS_VALUES) || "ACTIVE",
    isPrimary: parseBooleanFlag(req.body?.isPrimary ?? req.body?.is_primary, true),
    payloadJson: parseJsonObjectField(req.body?.payloadJson ?? req.body?.payload_json, "payloadJson"),
  };
}

export function parsePayrollProviderImportJobListInput(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(
    req.query?.legalEntityId ?? req.query?.legal_entity_id,
    "legalEntityId"
  );
  const providerCode = (() => {
    const raw = req.query?.providerCode ?? req.query?.provider_code;
    if (raw === undefined || raw === null || raw === "") return null;
    return normalizeCode(raw, "providerCode", 60);
  })();
  const status = normalizeEnumOrNull(req.query?.status, "status", IMPORT_JOB_STATUS_VALUES);
  const payrollPeriod = (() => {
    const raw = req.query?.payrollPeriod ?? req.query?.payroll_period;
    if (raw === undefined || raw === null || raw === "") return null;
    return parseDateOnly(raw, "payrollPeriod");
  })();
  const cursor = normalizeText(req.query?.cursor, "cursor", 1200) || null;
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 300 });
  return {
    tenantId,
    legalEntityId,
    providerCode,
    status,
    payrollPeriod,
    cursor,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parsePayrollProviderImportPreviewInput(req) {
  const sourceFormat = normalizeEnumOrNull(
    req.body?.sourceFormat ?? req.body?.source_format,
    "sourceFormat",
    SOURCE_FORMAT_VALUES
  );
  if (!sourceFormat) {
    throw badRequest("sourceFormat is required");
  }
  const payrollPeriod = parseDateOnly(
    req.body?.payrollPeriod ?? req.body?.payroll_period,
    "payrollPeriod"
  );
  const periodStart =
    req.body?.periodStart ?? req.body?.period_start
      ? parseDateOnly(req.body?.periodStart ?? req.body?.period_start, "periodStart")
      : payrollPeriod;
  const periodEnd =
    req.body?.periodEnd ?? req.body?.period_end
      ? parseDateOnly(req.body?.periodEnd ?? req.body?.period_end, "periodEnd")
      : payrollPeriod;
  if (periodStart > periodEnd) {
    throw badRequest("periodEnd must be >= periodStart");
  }

  const rawPayloadText = String(req.body?.rawPayloadText ?? req.body?.raw_payload_text ?? "");
  if (!rawPayloadText.trim()) {
    throw badRequest("rawPayloadText is required");
  }

  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    payrollProviderConnectionId: requirePositiveInt(
      req.body?.payrollProviderConnectionId ?? req.body?.payroll_provider_connection_id,
      "payrollProviderConnectionId"
    ),
    payrollPeriod,
    periodStart,
    periodEnd,
    payDate: parseDateOnly(req.body?.payDate ?? req.body?.pay_date, "payDate"),
    currencyCode: normalizeCurrencyCode(
      req.body?.currencyCode ?? req.body?.currency_code,
      "currencyCode"
    ),
    sourceFormat,
    sourceFilename:
      normalizeText(
        req.body?.sourceFilename ?? req.body?.source_filename,
        "sourceFilename",
        255
      ) || null,
    sourceBatchRef: normalizeText(
      req.body?.sourceBatchRef ?? req.body?.source_batch_ref,
      "sourceBatchRef",
      120
    ),
    rawPayloadText,
    importKey: normalizeText(req.body?.importKey ?? req.body?.import_key, "importKey", 190),
  };
}

export function parsePayrollProviderImportJobReadInput(req) {
  return {
    tenantId: requireTenantId(req),
    importJobId: parseImportJobIdParam(req),
  };
}

export function parsePayrollProviderImportApplyInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    importJobId: parseImportJobIdParam(req),
    applyIdempotencyKey: normalizeText(
      req.body?.applyIdempotencyKey ?? req.body?.apply_idempotency_key ?? req.body?.idempotencyKey,
      "applyIdempotencyKey",
      190
    ),
    note: normalizeText(req.body?.note, "note", 500),
    allowSameUserApply: parseBooleanFlag(
      req.body?.allowSameUserApply ?? req.body?.allow_same_user_apply,
      false
    ),
  };
}

export function parseSensitiveRetentionActionInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    importJobId: parseImportJobIdParam(req),
    reason:
      normalizeText(req.body?.reason, "reason", 500) || "Manual retention action",
  };
}
