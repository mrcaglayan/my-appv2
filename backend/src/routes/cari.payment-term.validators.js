import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  parseBooleanFlag,
  normalizeEnum,
  normalizeText,
  normalizeCode,
  optionalPositiveInt,
  parsePagination,
  requireTenantId,
} from "./cash.validators.common.js";

const PAYMENT_TERM_STATUSES = ["ACTIVE", "INACTIVE"];

function parseNonNegativeInteger(value, label, fallback = 0) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw badRequest(`${label} must be a non-negative integer`);
  }
  return parsed;
}

export function parsePaymentTermIdParam(req) {
  const paymentTermId = parsePositiveInt(req.params?.paymentTermId);
  if (!paymentTermId) {
    throw badRequest("paymentTermId must be a positive integer");
  }
  return paymentTermId;
}

export function parsePaymentTermReadFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const q = normalizeText(req.query?.q, "q", 120);

  const statusRaw = String(req.query?.status || "")
    .trim()
    .toUpperCase();
  const status = statusRaw
    ? normalizeEnum(statusRaw, "status", PAYMENT_TERM_STATUSES)
    : null;

  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });

  return {
    tenantId,
    legalEntityId,
    q,
    status,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parsePaymentTermCreateInput(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.body?.legalEntityId, "legalEntityId");
  if (!legalEntityId) {
    throw badRequest("legalEntityId is required");
  }

  const code = normalizeCode(req.body?.code, "code", 50);
  const name = normalizeText(req.body?.name, "name", 255, { required: true });
  const dueDays = parseNonNegativeInteger(req.body?.dueDays, "dueDays", 0);
  const graceDays = parseNonNegativeInteger(req.body?.graceDays, "graceDays", 0);
  const isEndOfMonth = parseBooleanFlag(req.body?.isEndOfMonth, false);
  const status = normalizeEnum(req.body?.status, "status", PAYMENT_TERM_STATUSES, "ACTIVE");

  return {
    tenantId,
    legalEntityId,
    code,
    name,
    dueDays,
    graceDays,
    isEndOfMonth,
    status,
  };
}
