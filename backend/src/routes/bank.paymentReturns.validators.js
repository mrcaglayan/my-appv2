import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeCurrencyCode,
  normalizeText,
  optionalPositiveInt,
  parseAmount,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const RETURN_EVENT_TYPES = ["PAYMENT_RETURNED", "PAYMENT_REJECTED", "PAYMENT_REVERSAL"];
const RETURN_EVENT_STATUSES = ["DETECTED", "CONFIRMED", "IGNORED"];

function normalizeEnumOptional(value, label, allowed) {
  if (value === undefined || value === null || value === "") return null;
  const normalized = String(value).trim().toUpperCase();
  if (!allowed.includes(normalized)) {
    throw badRequest(`${label} must be one of ${allowed.join(", ")}`);
  }
  return normalized;
}

function normalizeCodeOptional(value, label, maxLength) {
  if (value === undefined || value === null || value === "") return null;
  const normalized = String(value).trim().toUpperCase();
  if (!normalized) return null;
  if (normalized.length > maxLength) {
    throw badRequest(`${label} cannot exceed ${maxLength} characters`);
  }
  return normalized;
}

export function parsePaymentReturnEventIdParam(req) {
  const eventId = parsePositiveInt(req.params?.eventId ?? req.params?.id);
  if (!eventId) throw badRequest("eventId must be a positive integer");
  return eventId;
}

export function parsePaymentReturnEventsListFilters(req) {
  const tenantId = requireTenantId(req);
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });
  return {
    tenantId,
    legalEntityId: optionalPositiveInt(req.query?.legalEntityId, "legalEntityId"),
    bankAccountId: optionalPositiveInt(req.query?.bankAccountId, "bankAccountId"),
    paymentBatchId: optionalPositiveInt(
      req.query?.paymentBatchId ?? req.query?.payment_batch_id,
      "paymentBatchId"
    ),
    paymentBatchLineId: optionalPositiveInt(
      req.query?.paymentBatchLineId ?? req.query?.payment_batch_line_id,
      "paymentBatchLineId"
    ),
    bankStatementLineId: optionalPositiveInt(
      req.query?.bankStatementLineId ?? req.query?.bank_statement_line_id,
      "bankStatementLineId"
    ),
    eventType: normalizeEnumOptional(
      req.query?.eventType ?? req.query?.event_type,
      "eventType",
      RETURN_EVENT_TYPES
    ),
    eventStatus: normalizeEnumOptional(
      req.query?.eventStatus ?? req.query?.event_status,
      "eventStatus",
      RETURN_EVENT_STATUSES
    ),
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseManualPaymentReturnCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const paymentBatchLineId = optionalPositiveInt(
    req.body?.paymentBatchLineId ?? req.body?.payment_batch_line_id,
    "paymentBatchLineId"
  );
  if (!paymentBatchLineId) throw badRequest("paymentBatchLineId is required");

  return {
    tenantId,
    userId,
    eventRequestId: normalizeText(
      req.body?.eventRequestId ?? req.body?.event_request_id,
      "eventRequestId",
      190
    ),
    sourceType: "MANUAL",
    sourceRef: normalizeText(req.body?.sourceRef ?? req.body?.source_ref, "sourceRef", 190),
    paymentBatchLineId,
    bankStatementLineId: optionalPositiveInt(
      req.body?.bankStatementLineId ?? req.body?.bank_statement_line_id,
      "bankStatementLineId"
    ),
    eventType:
      normalizeEnumOptional(
        req.body?.eventType ?? req.body?.event_type,
        "eventType",
        RETURN_EVENT_TYPES
      ) || "PAYMENT_RETURNED",
    amount: Number(parseAmount(req.body?.amount, "amount", { required: true })),
    currencyCode: normalizeCurrencyCode(
      req.body?.currencyCode ?? req.body?.currency_code,
      "currencyCode"
    ),
    bankReference: normalizeText(
      req.body?.bankReference ?? req.body?.bank_reference,
      "bankReference",
      190
    ),
    reasonCode: normalizeCodeOptional(req.body?.reasonCode ?? req.body?.reason_code, "reasonCode", 50),
    reasonMessage: normalizeText(
      req.body?.reasonMessage ?? req.body?.reason_message,
      "reasonMessage",
      255
    ),
    payload: req.body?.payload ?? req.body?.payload_json ?? null,
  };
}

export function parsePaymentReturnIgnoreInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    eventId: parsePaymentReturnEventIdParam(req),
    reasonMessage:
      normalizeText(req.body?.reasonMessage ?? req.body?.reason_message, "reasonMessage", 255) ||
      "Ignored by reviewer",
  };
}
