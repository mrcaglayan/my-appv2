import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeCode,
  normalizeCurrencyCode,
  normalizeText,
  optionalPositiveInt,
  parseBooleanFlag,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

function normalizeOptionalCompactUpperText(value, label, maxLength) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const normalized = String(value)
    .trim()
    .replace(/\s+/g, "")
    .toUpperCase();
  if (!normalized) {
    return null;
  }
  if (normalized.length > maxLength) {
    throw badRequest(`${label} cannot exceed ${maxLength} characters`);
  }
  return normalized;
}

function parseOptionalIsActiveFilter(rawValue) {
  if (rawValue === undefined || rawValue === null || rawValue === "") {
    return null;
  }
  return parseBooleanFlag(rawValue);
}

export function parseBankAccountIdParam(req, label = "bankAccountId") {
  const bankAccountId = parsePositiveInt(req.params?.bankAccountId);
  if (!bankAccountId) {
    throw badRequest(`${label} must be a positive integer`);
  }
  return bankAccountId;
}

export function parseBankAccountReadFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const isActive = parseOptionalIsActiveFilter(req.query?.isActive);
  const q = normalizeText(req.query?.q, "q", 120);
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 300 });

  return {
    tenantId,
    legalEntityId,
    isActive,
    q,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

function parseBankAccountBody(req, { requireIdFromParam = false } = {}) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const bankAccountId = requireIdFromParam ? parseBankAccountIdParam(req) : null;

  const legalEntityId = optionalPositiveInt(req.body?.legalEntityId, "legalEntityId");
  const glAccountId = optionalPositiveInt(req.body?.glAccountId, "glAccountId");
  if (!legalEntityId || !glAccountId) {
    throw badRequest("legalEntityId and glAccountId are required");
  }

  const code = normalizeCode(req.body?.code, "code", 60);
  const name = normalizeText(req.body?.name, "name", 255, { required: true });
  const currencyCode = normalizeCurrencyCode(req.body?.currencyCode, "currencyCode");
  const bankName = normalizeText(req.body?.bankName, "bankName", 255);
  const branchName = normalizeText(req.body?.branchName, "branchName", 255);
  const iban = normalizeOptionalCompactUpperText(req.body?.iban, "iban", 64);
  const accountNo = normalizeText(req.body?.accountNo, "accountNo", 80);
  const isActive = parseBooleanFlag(req.body?.isActive, true);

  return {
    tenantId,
    userId,
    bankAccountId,
    legalEntityId,
    code,
    name,
    currencyCode,
    glAccountId,
    bankName,
    branchName,
    iban,
    accountNo,
    isActive,
  };
}

export function parseBankAccountCreateInput(req) {
  return parseBankAccountBody(req, { requireIdFromParam: false });
}

export function parseBankAccountUpdateInput(req) {
  return parseBankAccountBody(req, { requireIdFromParam: true });
}

export function parseBankAccountStatusActionInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const bankAccountId = parseBankAccountIdParam(req);
  return {
    tenantId,
    userId,
    bankAccountId,
  };
}
