import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeText,
  optionalPositiveInt,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const RECON_STATUS_VALUES = ["UNMATCHED", "PARTIAL", "MATCHED", "IGNORED"];

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

function parseCsvImportSource(value) {
  const normalized = String(value || "CSV").trim().toUpperCase();
  if (!["CSV", "API", "MANUAL"].includes(normalized)) {
    throw badRequest("importSource must be one of CSV, API, MANUAL");
  }
  return normalized;
}

export function parseBankStatementImportIdParam(req) {
  const importId = parsePositiveInt(req.params?.importId);
  if (!importId) {
    throw badRequest("importId must be a positive integer");
  }
  return importId;
}

export function parseBankStatementLineIdParam(req) {
  const lineId = parsePositiveInt(req.params?.lineId);
  if (!lineId) {
    throw badRequest("lineId must be a positive integer");
  }
  return lineId;
}

export function parseBankStatementImportCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const bankAccountId = optionalPositiveInt(req.body?.bankAccountId, "bankAccountId");
  if (!bankAccountId) {
    throw badRequest("bankAccountId is required");
  }

  const csvText = String(req.body?.csvText || "");
  if (!csvText.trim()) {
    throw badRequest("csvText is required");
  }

  return {
    tenantId,
    userId,
    bankAccountId,
    importSource: parseCsvImportSource(req.body?.importSource),
    originalFilename: normalizeText(req.body?.originalFilename, "originalFilename", 255) || "manual.csv",
    csvText,
  };
}

export function parseBankStatementImportReadFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const bankAccountId = optionalPositiveInt(req.query?.bankAccountId, "bankAccountId");
  const status = normalizeEnumOrNull(req.query?.status, "status", ["IMPORTED", "FAILED"]);
  const cursor = normalizeText(req.query?.cursor, "cursor", 1200) || null;
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 300 });

  return {
    tenantId,
    legalEntityId,
    bankAccountId,
    status,
    cursor,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseBankStatementLineReadFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const bankAccountId = optionalPositiveInt(req.query?.bankAccountId, "bankAccountId");
  const importId = optionalPositiveInt(req.query?.importId, "importId");
  const reconStatus = normalizeEnumOrNull(req.query?.reconStatus, "reconStatus", RECON_STATUS_VALUES);
  const cursor = normalizeText(req.query?.cursor, "cursor", 1200) || null;
  const pagination = parsePagination(req.query, { limit: 200, offset: 0, maxLimit: 500 });

  return {
    tenantId,
    legalEntityId,
    bankAccountId,
    importId,
    reconStatus,
    cursor,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}
