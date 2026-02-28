import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeEnum,
  normalizeText,
  optionalPositiveInt,
  parsePagination,
  requireTenantId,
} from "./cash.validators.common.js";

const PAYMENT_TERM_STATUSES = ["ACTIVE", "INACTIVE"];

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
