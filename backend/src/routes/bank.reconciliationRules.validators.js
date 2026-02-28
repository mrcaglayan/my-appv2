import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeText,
  optionalPositiveInt,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const RULE_STATUSES = ["ACTIVE", "PAUSED", "DISABLED"];
const RULE_SCOPE_TYPES = ["GLOBAL", "LEGAL_ENTITY", "BANK_ACCOUNT"];
const RULE_MATCH_TYPES = [
  "PAYMENT_BY_BANK_REFERENCE",
  "PAYMENT_BY_TEXT_AND_AMOUNT",
  "JOURNAL_BY_TEXT_AND_AMOUNT",
  "JOURNAL_BY_REFERENCE_AND_AMOUNT",
];
const RULE_ACTION_TYPES = [
  "AUTO_MATCH_PAYMENT_BATCH",
  "AUTO_MATCH_PAYMENT_LINE_WITH_DIFFERENCE",
  "AUTO_MATCH_JOURNAL",
  "AUTO_POST_TEMPLATE",
  "PROCESS_PAYMENT_RETURN",
  "QUEUE_EXCEPTION",
  "SUGGEST_ONLY",
];

function normalizeEnum(value, label, allowedValues, { required = false, fallback = null } = {}) {
  if (value === undefined || value === null || value === "") {
    if (required) throw badRequest(`${label} is required`);
    if (fallback !== null) return fallback;
    return null;
  }
  const normalized = String(value).trim().toUpperCase();
  if (!allowedValues.includes(normalized)) {
    throw badRequest(`${label} must be one of ${allowedValues.join(", ")}`);
  }
  return normalized;
}

function parseOptionalDateOnly(value, label) {
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

function parsePositiveOrDefault(value, defaultValue, min = 1, max = 1000) {
  if (value === undefined || value === null || value === "") return defaultValue;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    throw badRequest(`Value must be integer between ${min} and ${max}`);
  }
  return parsed;
}

export function parseReconciliationRuleIdParam(req) {
  const ruleId = parsePositiveInt(req.params?.ruleId ?? req.params?.id);
  if (!ruleId) throw badRequest("ruleId must be a positive integer");
  return ruleId;
}

function validateRuleScope(scopeType, legalEntityId, bankAccountId) {
  if (scopeType === "BANK_ACCOUNT" && !bankAccountId) {
    throw badRequest("bankAccountId is required for BANK_ACCOUNT scoped rule");
  }
  if (scopeType === "LEGAL_ENTITY" && !legalEntityId) {
    throw badRequest("legalEntityId is required for LEGAL_ENTITY scoped rule");
  }
}

export function parseReconciliationRuleListFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const bankAccountId = optionalPositiveInt(req.query?.bankAccountId, "bankAccountId");
  const status = normalizeEnum(req.query?.status, "status", RULE_STATUSES);
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

export function parseReconciliationRuleCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const ruleCode = normalizeText(req.body?.ruleCode ?? req.body?.rule_code, "ruleCode", 60, {
    required: true,
  });
  const ruleName = normalizeText(req.body?.ruleName ?? req.body?.rule_name, "ruleName", 190, {
    required: true,
  });
  const scopeType = normalizeEnum(
    req.body?.scopeType ?? req.body?.scope_type,
    "scopeType",
    RULE_SCOPE_TYPES,
    { fallback: "GLOBAL" }
  );
  const legalEntityId = optionalPositiveInt(
    req.body?.legalEntityId ?? req.body?.legal_entity_id,
    "legalEntityId"
  );
  const bankAccountId = optionalPositiveInt(
    req.body?.bankAccountId ?? req.body?.bank_account_id,
    "bankAccountId"
  );
  validateRuleScope(scopeType, legalEntityId, bankAccountId);

  const matchType = normalizeEnum(
    req.body?.matchType ?? req.body?.match_type,
    "matchType",
    RULE_MATCH_TYPES,
    { required: true }
  );
  const actionType = normalizeEnum(
    req.body?.actionType ?? req.body?.action_type,
    "actionType",
    RULE_ACTION_TYPES,
    { required: true }
  );

  const priority = parsePositiveOrDefault(req.body?.priority, 100, 1, 1000000);
  const status = normalizeEnum(req.body?.status, "status", RULE_STATUSES, { fallback: "ACTIVE" });
  const stopOnMatch =
    req.body?.stopOnMatch === undefined && req.body?.stop_on_match === undefined
      ? true
      : Boolean(req.body?.stopOnMatch ?? req.body?.stop_on_match);

  const conditions = req.body?.conditions ?? req.body?.conditions_json ?? {};
  if (conditions === null || typeof conditions !== "object" || Array.isArray(conditions)) {
    throw badRequest("conditions must be an object");
  }
  const actionPayload = req.body?.actionPayload ?? req.body?.action_payload ?? {};
  if (actionPayload !== null && (typeof actionPayload !== "object" || Array.isArray(actionPayload))) {
    throw badRequest("actionPayload must be an object");
  }

  return {
    tenantId,
    userId,
    ruleCode: String(ruleCode).trim().toUpperCase(),
    ruleName,
    priority,
    status,
    scopeType,
    legalEntityId: legalEntityId || null,
    bankAccountId: bankAccountId || null,
    matchType,
    conditions,
    actionType,
    actionPayload: actionPayload || {},
    stopOnMatch,
    effectiveFrom: parseOptionalDateOnly(
      req.body?.effectiveFrom ?? req.body?.effective_from,
      "effectiveFrom"
    ),
    effectiveTo: parseOptionalDateOnly(req.body?.effectiveTo ?? req.body?.effective_to, "effectiveTo"),
  };
}

export function parseReconciliationRuleUpdateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const ruleId = parseReconciliationRuleIdParam(req);

  const scopeType =
    req.body?.scopeType !== undefined || req.body?.scope_type !== undefined
      ? normalizeEnum(
          req.body?.scopeType ?? req.body?.scope_type,
          "scopeType",
          RULE_SCOPE_TYPES
        )
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
    validateRuleScope(scopeType, legalEntityId ?? null, bankAccountId ?? null);
  }

  const input = {
    tenantId,
    userId,
    ruleId,
    ruleName: normalizeText(req.body?.ruleName ?? req.body?.rule_name, "ruleName", 190),
    priority:
      req.body?.priority !== undefined
        ? parsePositiveOrDefault(req.body?.priority, 100, 1, 1000000)
        : undefined,
    status:
      req.body?.status !== undefined ? normalizeEnum(req.body?.status, "status", RULE_STATUSES) : undefined,
    scopeType,
    legalEntityId,
    bankAccountId,
    matchType:
      req.body?.matchType !== undefined || req.body?.match_type !== undefined
        ? normalizeEnum(
            req.body?.matchType ?? req.body?.match_type,
            "matchType",
            RULE_MATCH_TYPES
          )
        : undefined,
    actionType:
      req.body?.actionType !== undefined || req.body?.action_type !== undefined
        ? normalizeEnum(
            req.body?.actionType ?? req.body?.action_type,
            "actionType",
            RULE_ACTION_TYPES
          )
        : undefined,
    conditions:
      req.body?.conditions !== undefined || req.body?.conditions_json !== undefined
        ? req.body?.conditions ?? req.body?.conditions_json
        : undefined,
    actionPayload:
      req.body?.actionPayload !== undefined || req.body?.action_payload !== undefined
        ? req.body?.actionPayload ?? req.body?.action_payload
        : undefined,
    stopOnMatch:
      req.body?.stopOnMatch !== undefined || req.body?.stop_on_match !== undefined
        ? Boolean(req.body?.stopOnMatch ?? req.body?.stop_on_match)
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

  if (input.conditions !== undefined) {
    if (input.conditions === null || typeof input.conditions !== "object" || Array.isArray(input.conditions)) {
      throw badRequest("conditions must be an object");
    }
  }
  if (input.actionPayload !== undefined) {
    if (
      input.actionPayload !== null &&
      (typeof input.actionPayload !== "object" || Array.isArray(input.actionPayload))
    ) {
      throw badRequest("actionPayload must be an object");
    }
  }
  return input;
}

function parseAutoBodyCommon(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const legalEntityId = optionalPositiveInt(
    req.body?.legalEntityId ?? req.body?.legal_entity_id,
    "legalEntityId"
  );
  const bankAccountId = optionalPositiveInt(
    req.body?.bankAccountId ?? req.body?.bank_account_id,
    "bankAccountId"
  );
  const dateFrom = parseOptionalDateOnly(req.body?.dateFrom ?? req.body?.date_from, "dateFrom");
  const dateTo = parseOptionalDateOnly(req.body?.dateTo ?? req.body?.date_to, "dateTo");
  if (dateFrom && dateTo && dateFrom > dateTo) {
    throw badRequest("dateFrom cannot be later than dateTo");
  }
  return {
    tenantId,
    userId,
    legalEntityId,
    bankAccountId,
    dateFrom,
    dateTo,
    limit: parsePositiveOrDefault(req.body?.limit, 100, 1, 500),
  };
}

export function parseReconciliationAutoPreviewInput(req) {
  return {
    ...parseAutoBodyCommon(req),
    dryRun: true,
  };
}

export function parseReconciliationAutoApplyInput(req) {
  return {
    ...parseAutoBodyCommon(req),
    runRequestId: normalizeText(
      req.body?.runRequestId ?? req.body?.run_request_id,
      "runRequestId",
      190
    ),
  };
}

export default {
  parseReconciliationRuleIdParam,
  parseReconciliationRuleListFilters,
  parseReconciliationRuleCreateInput,
  parseReconciliationRuleUpdateInput,
  parseReconciliationAutoPreviewInput,
  parseReconciliationAutoApplyInput,
};
