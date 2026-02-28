import { badRequest } from "./_utils.js";
import {
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

export const PAYROLL_COMPONENT_CODES = [
  "BASE_SALARY_EXPENSE",
  "OVERTIME_EXPENSE",
  "BONUS_EXPENSE",
  "ALLOWANCES_EXPENSE",
  "EMPLOYER_TAX_EXPENSE",
  "EMPLOYER_SOCIAL_SECURITY_EXPENSE",
  "PAYROLL_NET_PAYABLE",
  "EMPLOYEE_TAX_PAYABLE",
  "EMPLOYEE_SOCIAL_SECURITY_PAYABLE",
  "EMPLOYER_TAX_PAYABLE",
  "EMPLOYER_SOCIAL_SECURITY_PAYABLE",
  "OTHER_DEDUCTIONS_PAYABLE",
];

function normalizeUpperToken(value, label, maxLength, { required = false } = {}) {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!normalized) {
    if (required) {
      throw badRequest(`${label} is required`);
    }
    return null;
  }
  if (normalized.length > maxLength) {
    throw badRequest(`${label} cannot exceed ${maxLength} characters`);
  }
  return normalized;
}

function normalizeEntrySide(value, label = "entrySide") {
  const normalized = normalizeUpperToken(value, label, 6, { required: true });
  if (!["DEBIT", "CREDIT"].includes(normalized)) {
    throw badRequest(`${label} must be DEBIT or CREDIT`);
  }
  return normalized;
}

function normalizeComponentCode(value, label = "componentCode") {
  const normalized = normalizeUpperToken(value, label, 80, { required: true });
  if (!PAYROLL_COMPONENT_CODES.includes(normalized)) {
    throw badRequest(`${label} must be one of ${PAYROLL_COMPONENT_CODES.join(", ")}`);
  }
  return normalized;
}

function normalizeComponentCodeOptional(value, label = "componentCode") {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  return normalizeComponentCode(value, label);
}

function normalizeProviderCodeOptional(value, label = "providerCode") {
  return normalizeUpperToken(value, label, 60);
}

function normalizeEntityCodeOptional(value, label = "entityCode") {
  return normalizeUpperToken(value, label, 60);
}

export function parsePayrollMappingListFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(
    req.query?.legalEntityId ?? req.query?.legal_entity_id,
    "legalEntityId"
  );
  const entityCode = normalizeEntityCodeOptional(req.query?.entityCode ?? req.query?.entity_code);
  const providerCode = normalizeProviderCodeOptional(
    req.query?.providerCode ?? req.query?.provider_code
  );
  const currencyCode = (() => {
    const raw = req.query?.currencyCode ?? req.query?.currency_code;
    if (raw === undefined || raw === null || raw === "") {
      return null;
    }
    return normalizeCurrencyCode(raw, "currencyCode");
  })();
  const componentCode = normalizeComponentCodeOptional(
    req.query?.componentCode ?? req.query?.component_code
  );
  const asOfDate = (() => {
    const raw = req.query?.asOfDate ?? req.query?.as_of_date;
    if (raw === undefined || raw === null || raw === "") {
      return null;
    }
    return parseDateOnly(raw, "asOfDate");
  })();
  const activeOnly = parseBooleanFlag(req.query?.activeOnly ?? req.query?.active_only, true);
  const pagination = parsePagination(req.query, { limit: 200, offset: 0, maxLimit: 500 });

  return {
    tenantId,
    legalEntityId,
    entityCode,
    providerCode,
    currencyCode,
    componentCode,
    asOfDate,
    activeOnly,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parsePayrollMappingUpsertInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const legalEntityId = requirePositiveInt(
    req.body?.legalEntityId ?? req.body?.legal_entity_id,
    "legalEntityId"
  );

  const effectiveFrom = parseDateOnly(
    req.body?.effectiveFrom ?? req.body?.effective_from,
    "effectiveFrom"
  );
  const effectiveToRaw = req.body?.effectiveTo ?? req.body?.effective_to;
  const effectiveTo =
    effectiveToRaw === undefined || effectiveToRaw === null || effectiveToRaw === ""
      ? null
      : parseDateOnly(effectiveToRaw, "effectiveTo");

  return {
    tenantId,
    userId,
    legalEntityId,
    entityCodeInput: normalizeEntityCodeOptional(req.body?.entityCode ?? req.body?.entity_code),
    providerCode: normalizeProviderCodeOptional(req.body?.providerCode ?? req.body?.provider_code),
    currencyCode: normalizeCurrencyCode(
      req.body?.currencyCode ?? req.body?.currency_code,
      "currencyCode"
    ),
    componentCode: normalizeComponentCode(req.body?.componentCode ?? req.body?.component_code),
    entrySide: normalizeEntrySide(req.body?.entrySide ?? req.body?.entry_side),
    glAccountId: requirePositiveInt(req.body?.glAccountId ?? req.body?.gl_account_id, "glAccountId"),
    effectiveFrom,
    effectiveTo,
    closePreviousOpenMapping: parseBooleanFlag(
      req.body?.closePreviousOpenMapping ?? req.body?.close_previous_open_mapping,
      true
    ),
    notes: normalizeText(req.body?.notes, "notes", 500),
  };
}

export function parsePayrollMappingDeactivateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const mappingId = requirePositiveInt(
    req.body?.mappingId ?? req.body?.mapping_id ?? req.params?.mappingId,
    "mappingId"
  );
  const isActive = parseBooleanFlag(req.body?.isActive ?? req.body?.is_active, false);
  const note = normalizeText(req.body?.note, "note", 500);
  return {
    tenantId,
    userId,
    mappingId,
    isActive,
    note,
  };
}

