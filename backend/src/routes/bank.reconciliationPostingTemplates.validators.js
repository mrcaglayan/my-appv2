import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeText,
  optionalPositiveInt,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const TEMPLATE_STATUSES = ["ACTIVE", "PAUSED", "DISABLED"];
const TEMPLATE_SCOPE_TYPES = ["GLOBAL", "LEGAL_ENTITY", "BANK_ACCOUNT"];
const DIRECTION_POLICIES = ["OUTFLOW_ONLY", "INFLOW_ONLY", "BOTH"];
const DESCRIPTION_MODES = ["USE_STATEMENT_TEXT", "FIXED_TEXT", "PREFIXED"];
const TAX_MODES = ["NONE", "INCLUDED"];

function normalizeEnum(value, label, allowed, { required = false, fallback = null } = {}) {
  if (value === undefined || value === null || value === "") {
    if (required) throw badRequest(`${label} is required`);
    return fallback;
  }
  const normalized = String(value).trim().toUpperCase();
  if (!allowed.includes(normalized)) {
    throw badRequest(`${label} must be one of ${allowed.join(", ")}`);
  }
  return normalized;
}

function parseOptionalDateOnly(value, label) {
  if (value === undefined || value === null || value === "") return null;
  const text = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) throw badRequest(`${label} must be YYYY-MM-DD`);
  const parsed = new Date(`${text}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== text) {
    throw badRequest(`${label} must be a valid date`);
  }
  return text;
}

function parseOptionalDecimal(value, label) {
  if (value === undefined || value === null || value === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n)) throw badRequest(`${label} must be numeric`);
  return Number(n.toFixed(6));
}

function parseOptionalCurrencyCode(value, label) {
  if (value === undefined || value === null || value === "") return null;
  const code = String(value).trim().toUpperCase();
  if (code.length !== 3) throw badRequest(`${label} must be a 3-letter code`);
  return code;
}

function validateScope(scopeType, legalEntityId, bankAccountId) {
  if (scopeType === "BANK_ACCOUNT" && !bankAccountId) {
    throw badRequest("bankAccountId is required for BANK_ACCOUNT template scope");
  }
  if (scopeType === "LEGAL_ENTITY" && !legalEntityId) {
    throw badRequest("legalEntityId is required for LEGAL_ENTITY template scope");
  }
}

function validateAmountRange(minAmountAbs, maxAmountAbs) {
  if (minAmountAbs !== null && minAmountAbs < 0) throw badRequest("minAmountAbs cannot be negative");
  if (maxAmountAbs !== null && maxAmountAbs < 0) throw badRequest("maxAmountAbs cannot be negative");
  if (
    minAmountAbs !== null &&
    maxAmountAbs !== null &&
    Number(minAmountAbs) > Number(maxAmountAbs)
  ) {
    throw badRequest("minAmountAbs cannot be greater than maxAmountAbs");
  }
}

function validateDescriptionModeFields(descriptionMode, fixedDescription, descriptionPrefix) {
  if (descriptionMode === "FIXED_TEXT" && !fixedDescription) {
    throw badRequest("fixedDescription is required when descriptionMode=FIXED_TEXT");
  }
  if (descriptionMode === "PREFIXED" && !descriptionPrefix) {
    throw badRequest("descriptionPrefix is required when descriptionMode=PREFIXED");
  }
}

export function parsePostingTemplateIdParam(req) {
  const templateId = parsePositiveInt(req.params?.templateId ?? req.params?.id);
  if (!templateId) throw badRequest("templateId must be a positive integer");
  return templateId;
}

export function parsePostingTemplateListFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const bankAccountId = optionalPositiveInt(req.query?.bankAccountId, "bankAccountId");
  const status = normalizeEnum(req.query?.status, "status", TEMPLATE_STATUSES);
  const q = normalizeText(req.query?.q, "q", 120) || null;
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });
  return {
    tenantId,
    legalEntityId,
    bankAccountId,
    status,
    q,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parsePostingTemplateCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const scopeType = normalizeEnum(
    req.body?.scopeType ?? req.body?.scope_type,
    "scopeType",
    TEMPLATE_SCOPE_TYPES,
    { fallback: "LEGAL_ENTITY" }
  );
  const legalEntityId = optionalPositiveInt(
    req.body?.legalEntityId ?? req.body?.legal_entity_id,
    "legalEntityId"
  );
  const bankAccountId = optionalPositiveInt(
    req.body?.bankAccountId ?? req.body?.bank_account_id,
    "bankAccountId"
  );
  validateScope(scopeType, legalEntityId, bankAccountId);

  const descriptionMode = normalizeEnum(
    req.body?.descriptionMode ?? req.body?.description_mode,
    "descriptionMode",
    DESCRIPTION_MODES,
    { fallback: "USE_STATEMENT_TEXT" }
  );
  const fixedDescription = normalizeText(
    req.body?.fixedDescription ?? req.body?.fixed_description,
    "fixedDescription",
    255
  ) || null;
  const descriptionPrefix = normalizeText(
    req.body?.descriptionPrefix ?? req.body?.description_prefix,
    "descriptionPrefix",
    100
  ) || null;
  validateDescriptionModeFields(descriptionMode, fixedDescription, descriptionPrefix);

  const minAmountAbs = parseOptionalDecimal(
    req.body?.minAmountAbs ?? req.body?.min_amount_abs,
    "minAmountAbs"
  );
  const maxAmountAbs = parseOptionalDecimal(
    req.body?.maxAmountAbs ?? req.body?.max_amount_abs,
    "maxAmountAbs"
  );
  validateAmountRange(minAmountAbs, maxAmountAbs);

  const taxMode = normalizeEnum(req.body?.taxMode ?? req.body?.tax_mode, "taxMode", TAX_MODES, {
    fallback: "NONE",
  });

  return {
    tenantId,
    userId,
    templateCode: normalizeText(
      req.body?.templateCode ?? req.body?.template_code,
      "templateCode",
      60,
      { required: true }
    )
      .trim()
      .toUpperCase(),
    templateName: normalizeText(
      req.body?.templateName ?? req.body?.template_name,
      "templateName",
      190,
      { required: true }
    ),
    status: normalizeEnum(req.body?.status, "status", TEMPLATE_STATUSES, { fallback: "ACTIVE" }),
    scopeType,
    legalEntityId: legalEntityId || null,
    bankAccountId: bankAccountId || null,
    entryKind:
      normalizeText(req.body?.entryKind ?? req.body?.entry_kind, "entryKind", 40) ||
      "BANK_MISC",
    directionPolicy: normalizeEnum(
      req.body?.directionPolicy ?? req.body?.direction_policy,
      "directionPolicy",
      DIRECTION_POLICIES,
      { fallback: "BOTH" }
    ),
    counterAccountId:
      optionalPositiveInt(req.body?.counterAccountId ?? req.body?.counter_account_id, "counterAccountId") ||
      (() => {
        throw badRequest("counterAccountId is required");
      })(),
    taxAccountId: optionalPositiveInt(req.body?.taxAccountId ?? req.body?.tax_account_id, "taxAccountId"),
    taxMode,
    taxRate: parseOptionalDecimal(req.body?.taxRate ?? req.body?.tax_rate, "taxRate"),
    currencyCode: parseOptionalCurrencyCode(req.body?.currencyCode ?? req.body?.currency_code, "currencyCode"),
    minAmountAbs,
    maxAmountAbs,
    descriptionMode,
    fixedDescription,
    descriptionPrefix,
    journalSourceCode:
      normalizeText(req.body?.journalSourceCode ?? req.body?.journal_source_code, "journalSourceCode", 30) ||
      "BANK_AUTO_POST",
    journalDocType:
      normalizeText(req.body?.journalDocType ?? req.body?.journal_doc_type, "journalDocType", 30) ||
      "BANK_AUTO",
    effectiveFrom: parseOptionalDateOnly(req.body?.effectiveFrom ?? req.body?.effective_from, "effectiveFrom"),
    effectiveTo: parseOptionalDateOnly(req.body?.effectiveTo ?? req.body?.effective_to, "effectiveTo"),
  };
}

export function parsePostingTemplateUpdateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const templateId = parsePostingTemplateIdParam(req);

  const scopeType =
    req.body?.scopeType !== undefined || req.body?.scope_type !== undefined
      ? normalizeEnum(req.body?.scopeType ?? req.body?.scope_type, "scopeType", TEMPLATE_SCOPE_TYPES)
      : undefined;
  const legalEntityId =
    req.body?.legalEntityId !== undefined || req.body?.legal_entity_id !== undefined
      ? optionalPositiveInt(req.body?.legalEntityId ?? req.body?.legal_entity_id, "legalEntityId")
      : undefined;
  const bankAccountId =
    req.body?.bankAccountId !== undefined || req.body?.bank_account_id !== undefined
      ? optionalPositiveInt(req.body?.bankAccountId ?? req.body?.bank_account_id, "bankAccountId")
      : undefined;
  if (scopeType) {
    validateScope(scopeType, legalEntityId ?? null, bankAccountId ?? null);
  }

  const descriptionMode =
    req.body?.descriptionMode !== undefined || req.body?.description_mode !== undefined
      ? normalizeEnum(
          req.body?.descriptionMode ?? req.body?.description_mode,
          "descriptionMode",
          DESCRIPTION_MODES
        )
      : undefined;
  const fixedDescription =
    req.body?.fixedDescription !== undefined || req.body?.fixed_description !== undefined
      ? normalizeText(req.body?.fixedDescription ?? req.body?.fixed_description, "fixedDescription", 255) || null
      : undefined;
  const descriptionPrefix =
    req.body?.descriptionPrefix !== undefined || req.body?.description_prefix !== undefined
      ? normalizeText(req.body?.descriptionPrefix ?? req.body?.description_prefix, "descriptionPrefix", 100) || null
      : undefined;
  if (descriptionMode) {
    validateDescriptionModeFields(descriptionMode, fixedDescription ?? null, descriptionPrefix ?? null);
  }

  const minAmountAbs =
    req.body?.minAmountAbs !== undefined || req.body?.min_amount_abs !== undefined
      ? parseOptionalDecimal(req.body?.minAmountAbs ?? req.body?.min_amount_abs, "minAmountAbs")
      : undefined;
  const maxAmountAbs =
    req.body?.maxAmountAbs !== undefined || req.body?.max_amount_abs !== undefined
      ? parseOptionalDecimal(req.body?.maxAmountAbs ?? req.body?.max_amount_abs, "maxAmountAbs")
      : undefined;
  if (minAmountAbs !== undefined || maxAmountAbs !== undefined) {
    validateAmountRange(minAmountAbs ?? null, maxAmountAbs ?? null);
  }

  return {
    tenantId,
    userId,
    templateId,
    templateName:
      req.body?.templateName !== undefined || req.body?.template_name !== undefined
        ? normalizeText(req.body?.templateName ?? req.body?.template_name, "templateName", 190)
        : undefined,
    status:
      req.body?.status !== undefined
        ? normalizeEnum(req.body?.status, "status", TEMPLATE_STATUSES)
        : undefined,
    scopeType,
    legalEntityId,
    bankAccountId,
    entryKind:
      req.body?.entryKind !== undefined || req.body?.entry_kind !== undefined
        ? (normalizeText(req.body?.entryKind ?? req.body?.entry_kind, "entryKind", 40) || null)
        : undefined,
    directionPolicy:
      req.body?.directionPolicy !== undefined || req.body?.direction_policy !== undefined
        ? normalizeEnum(
            req.body?.directionPolicy ?? req.body?.direction_policy,
            "directionPolicy",
            DIRECTION_POLICIES
          )
        : undefined,
    counterAccountId:
      req.body?.counterAccountId !== undefined || req.body?.counter_account_id !== undefined
        ? optionalPositiveInt(req.body?.counterAccountId ?? req.body?.counter_account_id, "counterAccountId")
        : undefined,
    taxAccountId:
      req.body?.taxAccountId !== undefined || req.body?.tax_account_id !== undefined
        ? optionalPositiveInt(req.body?.taxAccountId ?? req.body?.tax_account_id, "taxAccountId")
        : undefined,
    taxMode:
      req.body?.taxMode !== undefined || req.body?.tax_mode !== undefined
        ? normalizeEnum(req.body?.taxMode ?? req.body?.tax_mode, "taxMode", TAX_MODES)
        : undefined,
    taxRate:
      req.body?.taxRate !== undefined || req.body?.tax_rate !== undefined
        ? parseOptionalDecimal(req.body?.taxRate ?? req.body?.tax_rate, "taxRate")
        : undefined,
    currencyCode:
      req.body?.currencyCode !== undefined || req.body?.currency_code !== undefined
        ? parseOptionalCurrencyCode(req.body?.currencyCode ?? req.body?.currency_code, "currencyCode")
        : undefined,
    minAmountAbs,
    maxAmountAbs,
    descriptionMode,
    fixedDescription,
    descriptionPrefix,
    journalSourceCode:
      req.body?.journalSourceCode !== undefined || req.body?.journal_source_code !== undefined
        ? (normalizeText(req.body?.journalSourceCode ?? req.body?.journal_source_code, "journalSourceCode", 30) || null)
        : undefined,
    journalDocType:
      req.body?.journalDocType !== undefined || req.body?.journal_doc_type !== undefined
        ? (normalizeText(req.body?.journalDocType ?? req.body?.journal_doc_type, "journalDocType", 30) || null)
        : undefined,
    effectiveFrom:
      req.body?.effectiveFrom !== undefined || req.body?.effective_from !== undefined
        ? parseOptionalDateOnly(req.body?.effectiveFrom ?? req.body?.effective_from, "effectiveFrom")
        : undefined,
    effectiveTo:
      req.body?.effectiveTo !== undefined || req.body?.effective_to !== undefined
        ? parseOptionalDateOnly(req.body?.effectiveTo ?? req.body?.effective_to, "effectiveTo")
        : undefined,
  };
}

export default {
  parsePostingTemplateIdParam,
  parsePostingTemplateListFilters,
  parsePostingTemplateCreateInput,
  parsePostingTemplateUpdateInput,
};
