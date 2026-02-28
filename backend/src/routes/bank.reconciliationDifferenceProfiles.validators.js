import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeCode,
  normalizeCurrencyCode,
  normalizeText,
  optionalPositiveInt,
  parseAmount,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const PROFILE_STATUSES = ["ACTIVE", "PAUSED", "DISABLED"];
const PROFILE_SCOPE_TYPES = ["LEGAL_ENTITY", "BANK_ACCOUNT", "GLOBAL"];
const DIFF_TYPES = ["FEE", "FX"];
const DIRECTION_POLICIES = ["BOTH", "INCREASE_ONLY", "DECREASE_ONLY"];

function normalizeEnumOptional(value, label, allowed) {
  if (value === undefined || value === null || value === "") return null;
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

function parseOptionalAmount(value, label) {
  if (value === undefined || value === null || value === "") return null;
  return Number(parseAmount(value, label, { allowZero: true, required: false }));
}

export function parseDifferenceProfileIdParam(req) {
  const profileId = parsePositiveInt(req.params?.profileId ?? req.params?.id);
  if (!profileId) throw badRequest("profileId must be a positive integer");
  return profileId;
}

export function parseDifferenceProfileListFilters(req) {
  const tenantId = requireTenantId(req);
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });
  return {
    tenantId,
    legalEntityId: optionalPositiveInt(req.query?.legalEntityId, "legalEntityId"),
    bankAccountId: optionalPositiveInt(req.query?.bankAccountId, "bankAccountId"),
    status: normalizeEnumOptional(req.query?.status, "status", PROFILE_STATUSES),
    differenceType: normalizeEnumOptional(
      req.query?.differenceType ?? req.query?.difference_type,
      "differenceType",
      DIFF_TYPES
    ),
    q: normalizeText(req.query?.q, "q", 120),
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseDifferenceProfileCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const scopeType =
    normalizeEnumOptional(req.body?.scopeType ?? req.body?.scope_type, "scopeType", PROFILE_SCOPE_TYPES) ||
    "LEGAL_ENTITY";
  const legalEntityId = optionalPositiveInt(req.body?.legalEntityId ?? req.body?.legal_entity_id, "legalEntityId");
  const bankAccountId = optionalPositiveInt(req.body?.bankAccountId ?? req.body?.bank_account_id, "bankAccountId");

  return {
    tenantId,
    userId,
    profileCode: normalizeCode(req.body?.profileCode ?? req.body?.profile_code, "profileCode", 60),
    profileName: normalizeText(req.body?.profileName ?? req.body?.profile_name, "profileName", 190, {
      required: true,
    }),
    status: normalizeEnumOptional(req.body?.status, "status", PROFILE_STATUSES) || "ACTIVE",
    scopeType,
    legalEntityId,
    bankAccountId,
    differenceType:
      normalizeEnumOptional(
      req.body?.differenceType ?? req.body?.difference_type,
      "differenceType",
      DIFF_TYPES
      ) || (() => {
        throw badRequest("differenceType is required");
      })(),
    directionPolicy:
      normalizeEnumOptional(
        req.body?.directionPolicy ?? req.body?.direction_policy,
        "directionPolicy",
        DIRECTION_POLICIES
      ) || "BOTH",
    maxAbsDifference: parseOptionalAmount(
      req.body?.maxAbsDifference ?? req.body?.max_abs_difference,
      "maxAbsDifference"
    ),
    expenseAccountId: optionalPositiveInt(
      req.body?.expenseAccountId ?? req.body?.expense_account_id,
      "expenseAccountId"
    ),
    fxGainAccountId: optionalPositiveInt(
      req.body?.fxGainAccountId ?? req.body?.fx_gain_account_id,
      "fxGainAccountId"
    ),
    fxLossAccountId: optionalPositiveInt(
      req.body?.fxLossAccountId ?? req.body?.fx_loss_account_id,
      "fxLossAccountId"
    ),
    currencyCode:
      req.body?.currencyCode !== undefined || req.body?.currency_code !== undefined
        ? normalizeCurrencyCode(req.body?.currencyCode ?? req.body?.currency_code, "currencyCode")
        : null,
    descriptionPrefix: normalizeText(
      req.body?.descriptionPrefix ?? req.body?.description_prefix,
      "descriptionPrefix",
      100
    ),
    effectiveFrom: parseOptionalDateOnly(
      req.body?.effectiveFrom ?? req.body?.effective_from,
      "effectiveFrom"
    ),
    effectiveTo: parseOptionalDateOnly(req.body?.effectiveTo ?? req.body?.effective_to, "effectiveTo"),
  };
}

export function parseDifferenceProfileUpdateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const profileId = parseDifferenceProfileIdParam(req);
  return {
    tenantId,
    userId,
    profileId,
    profileName:
      req.body?.profileName !== undefined || req.body?.profile_name !== undefined
        ? normalizeText(req.body?.profileName ?? req.body?.profile_name, "profileName", 190)
        : undefined,
    status:
      req.body?.status !== undefined
        ? normalizeEnumOptional(req.body?.status, "status", PROFILE_STATUSES)
        : undefined,
    maxAbsDifference:
      req.body?.maxAbsDifference !== undefined || req.body?.max_abs_difference !== undefined
        ? parseOptionalAmount(
            req.body?.maxAbsDifference ?? req.body?.max_abs_difference,
            "maxAbsDifference"
          )
        : undefined,
    expenseAccountId:
      req.body?.expenseAccountId !== undefined || req.body?.expense_account_id !== undefined
        ? optionalPositiveInt(req.body?.expenseAccountId ?? req.body?.expense_account_id, "expenseAccountId")
        : undefined,
    fxGainAccountId:
      req.body?.fxGainAccountId !== undefined || req.body?.fx_gain_account_id !== undefined
        ? optionalPositiveInt(req.body?.fxGainAccountId ?? req.body?.fx_gain_account_id, "fxGainAccountId")
        : undefined,
    fxLossAccountId:
      req.body?.fxLossAccountId !== undefined || req.body?.fx_loss_account_id !== undefined
        ? optionalPositiveInt(req.body?.fxLossAccountId ?? req.body?.fx_loss_account_id, "fxLossAccountId")
        : undefined,
    currencyCode:
      req.body?.currencyCode !== undefined || req.body?.currency_code !== undefined
        ? (req.body?.currencyCode ?? req.body?.currency_code)
          ? normalizeCurrencyCode(req.body?.currencyCode ?? req.body?.currency_code, "currencyCode")
          : null
        : undefined,
    descriptionPrefix:
      req.body?.descriptionPrefix !== undefined || req.body?.description_prefix !== undefined
        ? normalizeText(req.body?.descriptionPrefix ?? req.body?.description_prefix, "descriptionPrefix", 100)
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
