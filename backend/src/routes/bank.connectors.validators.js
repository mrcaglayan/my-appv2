import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeText,
  optionalPositiveInt,
  parseBooleanFlag,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const CONNECTOR_STATUSES = ["DRAFT", "ACTIVE", "PAUSED", "ERROR", "DISABLED"];
const CONNECTOR_TYPES = ["OPEN_BANKING", "HOST_TO_HOST", "SFTP", "API"];
const CONNECTOR_SYNC_MODES = ["MANUAL", "SCHEDULED"];
const LINK_STATUSES = ["ACTIVE", "INACTIVE"];

function normalizeEnumOrNull(value, label, allowedValues) {
  if (value === undefined || value === null || value === "") return null;
  const normalized = String(value).trim().toUpperCase();
  if (!allowedValues.includes(normalized)) {
    throw badRequest(`${label} must be one of ${allowedValues.join(", ")}`);
  }
  return normalized;
}

function normalizeDateOnlyOptional(value, label) {
  if (value === undefined || value === null || value === "") return null;
  const text = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    throw badRequest(`${label} must be YYYY-MM-DD`);
  }
  const parsed = new Date(`${text}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== text) {
    throw badRequest(`${label} must be a valid date`);
  }
  return text;
}

function normalizeDateTimeOptional(value, label) {
  if (value === undefined || value === null || value === "") return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw badRequest(`${label} must be a valid datetime`);
  }
  return parsed.toISOString().slice(0, 19).replace("T", " ");
}

export function parseBankConnectorIdParam(req) {
  const connectorId = parsePositiveInt(req.params?.connectorId ?? req.params?.id);
  if (!connectorId) throw badRequest("connectorId must be a positive integer");
  return connectorId;
}

export function parseBankConnectorListFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const status = normalizeEnumOrNull(req.query?.status, "status", CONNECTOR_STATUSES);
  const providerCode = normalizeText(req.query?.providerCode ?? req.query?.provider_code, "providerCode", 60);
  const q = normalizeText(req.query?.q, "q", 120);
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });

  return {
    tenantId,
    legalEntityId,
    status,
    providerCode: providerCode ? providerCode.toUpperCase() : null,
    q: q || null,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseBankConnectorCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const legalEntityId = optionalPositiveInt(
    req.body?.legalEntityId ?? req.body?.legal_entity_id,
    "legalEntityId"
  );
  if (!legalEntityId) throw badRequest("legalEntityId is required");

  const connectorCode = normalizeText(
    req.body?.connectorCode ?? req.body?.connector_code,
    "connectorCode",
    60,
    { required: true }
  );
  const connectorName = normalizeText(
    req.body?.connectorName ?? req.body?.connector_name,
    "connectorName",
    190,
    { required: true }
  );
  const providerCode = normalizeText(
    req.body?.providerCode ?? req.body?.provider_code,
    "providerCode",
    60,
    { required: true }
  );
  const connectorType = normalizeEnumOrNull(
    req.body?.connectorType ?? req.body?.connector_type,
    "connectorType",
    CONNECTOR_TYPES
  ) || "OPEN_BANKING";
  const syncMode = normalizeEnumOrNull(
    req.body?.syncMode ?? req.body?.sync_mode,
    "syncMode",
    CONNECTOR_SYNC_MODES
  ) || "MANUAL";
  const syncFrequencyMinutes =
    req.body?.syncFrequencyMinutes !== undefined || req.body?.sync_frequency_minutes !== undefined
      ? Number(req.body?.syncFrequencyMinutes ?? req.body?.sync_frequency_minutes)
      : null;
  if (
    syncFrequencyMinutes !== null &&
    (!Number.isInteger(syncFrequencyMinutes) || syncFrequencyMinutes <= 0 || syncFrequencyMinutes > 10080)
  ) {
    throw badRequest("syncFrequencyMinutes must be integer between 1 and 10080");
  }
  const config = req.body?.config ?? req.body?.config_json ?? {};
  if (config !== null && (typeof config !== "object" || Array.isArray(config))) {
    throw badRequest("config must be an object");
  }
  const credentials = req.body?.credentials ?? req.body?.credentials_json ?? {};
  if (credentials !== null && (typeof credentials !== "object" || Array.isArray(credentials))) {
    throw badRequest("credentials must be an object");
  }

  return {
    tenantId,
    userId,
    legalEntityId,
    connectorCode: connectorCode.toUpperCase(),
    connectorName,
    providerCode: providerCode.toUpperCase(),
    connectorType,
    status:
      normalizeEnumOrNull(req.body?.status, "status", CONNECTOR_STATUSES) || "DRAFT",
    adapterVersion: normalizeText(
      req.body?.adapterVersion ?? req.body?.adapter_version,
      "adapterVersion",
      40
    ) || "v1",
    config: config || {},
    credentials: credentials || {},
    syncMode,
    syncFrequencyMinutes,
    nextSyncAt: normalizeDateTimeOptional(
      req.body?.nextSyncAt ?? req.body?.next_sync_at,
      "nextSyncAt"
    ),
  };
}

export function parseBankConnectorUpdateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const connectorId = parseBankConnectorIdParam(req);
  const out = {
    tenantId,
    userId,
    connectorId,
  };

  if (req.body?.connectorName !== undefined || req.body?.connector_name !== undefined) {
    out.connectorName = normalizeText(
      req.body?.connectorName ?? req.body?.connector_name,
      "connectorName",
      190
    );
  }
  if (req.body?.status !== undefined) {
    out.status = normalizeEnumOrNull(req.body?.status, "status", CONNECTOR_STATUSES);
  }
  if (req.body?.connectorType !== undefined || req.body?.connector_type !== undefined) {
    out.connectorType = normalizeEnumOrNull(
      req.body?.connectorType ?? req.body?.connector_type,
      "connectorType",
      CONNECTOR_TYPES
    );
  }
  if (req.body?.config !== undefined || req.body?.config_json !== undefined) {
    out.config = req.body?.config ?? req.body?.config_json;
    if (out.config !== null && (typeof out.config !== "object" || Array.isArray(out.config))) {
      throw badRequest("config must be an object");
    }
  }
  if (req.body?.credentials !== undefined || req.body?.credentials_json !== undefined) {
    out.credentials = req.body?.credentials ?? req.body?.credentials_json;
    if (
      out.credentials !== null &&
      (typeof out.credentials !== "object" || Array.isArray(out.credentials))
    ) {
      throw badRequest("credentials must be an object");
    }
  }
  if (req.body?.syncMode !== undefined || req.body?.sync_mode !== undefined) {
    out.syncMode = normalizeEnumOrNull(
      req.body?.syncMode ?? req.body?.sync_mode,
      "syncMode",
      CONNECTOR_SYNC_MODES
    );
  }
  if (
    req.body?.syncFrequencyMinutes !== undefined ||
    req.body?.sync_frequency_minutes !== undefined
  ) {
    const n = Number(req.body?.syncFrequencyMinutes ?? req.body?.sync_frequency_minutes);
    if (req.body?.syncFrequencyMinutes === null || req.body?.sync_frequency_minutes === null) {
      out.syncFrequencyMinutes = null;
    } else if (!Number.isInteger(n) || n <= 0 || n > 10080) {
      throw badRequest("syncFrequencyMinutes must be integer between 1 and 10080");
    } else {
      out.syncFrequencyMinutes = n;
    }
  }
  if (req.body?.nextSyncAt !== undefined || req.body?.next_sync_at !== undefined) {
    out.nextSyncAt = normalizeDateTimeOptional(
      req.body?.nextSyncAt ?? req.body?.next_sync_at,
      "nextSyncAt"
    );
  }
  return out;
}

export function parseBankConnectorAccountLinkInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const connectorId = parseBankConnectorIdParam(req);
  const bankAccountId = optionalPositiveInt(
    req.body?.bankAccountId ?? req.body?.bank_account_id,
    "bankAccountId"
  );
  if (!bankAccountId) throw badRequest("bankAccountId is required");
  const externalAccountId = normalizeText(
    req.body?.externalAccountId ?? req.body?.external_account_id,
    "externalAccountId",
    190,
    { required: true }
  );
  const externalCurrencyCode = String(
    req.body?.externalCurrencyCode ?? req.body?.external_currency_code ?? ""
  )
    .trim()
    .toUpperCase();
  if (!externalCurrencyCode || externalCurrencyCode.length !== 3) {
    throw badRequest("externalCurrencyCode must be a 3-letter code");
  }
  return {
    tenantId,
    userId,
    connectorId,
    bankAccountId,
    externalAccountId,
    externalAccountName: normalizeText(
      req.body?.externalAccountName ?? req.body?.external_account_name,
      "externalAccountName",
      190
    ),
    externalCurrencyCode,
    status:
      normalizeEnumOrNull(req.body?.status, "status", LINK_STATUSES) || "ACTIVE",
  };
}

export function parseBankConnectorSyncTriggerInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const connectorId = parseBankConnectorIdParam(req);
  const fromDate = normalizeDateOnlyOptional(
    req.body?.fromDate ?? req.body?.from_date,
    "fromDate"
  );
  const toDate = normalizeDateOnlyOptional(req.body?.toDate ?? req.body?.to_date, "toDate");
  if (fromDate && toDate && fromDate > toDate) {
    throw badRequest("fromDate cannot be later than toDate");
  }
  return {
    tenantId,
    userId,
    connectorId,
    fromDate,
    toDate,
    requestId:
      normalizeText(req.body?.requestId ?? req.body?.request_id, "requestId", 190) || null,
    forceFull: parseBooleanFlag(req.body?.forceFull ?? req.body?.force_full, false),
  };
}

export function parseBankConnectorSyncRunListFilters(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const connectorId = parseBankConnectorIdParam(req);
  const pagination = parsePagination(req.query, { limit: 50, offset: 0, maxLimit: 200 });
  return {
    tenantId,
    userId,
    connectorId,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export default {
  parseBankConnectorIdParam,
  parseBankConnectorListFilters,
  parseBankConnectorCreateInput,
  parseBankConnectorUpdateInput,
  parseBankConnectorAccountLinkInput,
  parseBankConnectorSyncTriggerInput,
  parseBankConnectorSyncRunListFilters,
};
