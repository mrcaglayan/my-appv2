import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeText,
  parseBooleanFlag,
  parseDateOnly,
  parsePagination,
  requireTenantId,
  requireUserId,
  requirePositiveInt,
} from "./cash.validators.common.js";

function parseCloseIdParam(req) {
  const closeId = parsePositiveInt(req.params?.closeId ?? req.params?.id);
  if (!closeId) {
    throw badRequest("closeId must be a positive integer");
  }
  return closeId;
}

function validatePeriodRange(periodStart, periodEnd) {
  if (periodStart > periodEnd) {
    throw badRequest("periodEnd must be >= periodStart");
  }
}

export function parsePayrollPeriodCloseListInput(req) {
  const tenantId = requireTenantId(req);
  const legalEntityIdRaw = req.query?.legalEntityId ?? req.query?.legal_entity_id;
  const legalEntityId = legalEntityIdRaw ? requirePositiveInt(legalEntityIdRaw, "legalEntityId") : null;
  const status = req.query?.status ? String(req.query.status).trim().toUpperCase() : null;
  const periodStart = req.query?.periodStart ?? req.query?.period_start;
  const periodEnd = req.query?.periodEnd ?? req.query?.period_end;
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 300 });

  const parsedPeriodStart = periodStart ? parseDateOnly(periodStart, "periodStart") : null;
  const parsedPeriodEnd = periodEnd ? parseDateOnly(periodEnd, "periodEnd") : null;
  if (parsedPeriodStart && parsedPeriodEnd) validatePeriodRange(parsedPeriodStart, parsedPeriodEnd);

  return {
    tenantId,
    legalEntityId,
    status,
    periodStart: parsedPeriodStart,
    periodEnd: parsedPeriodEnd,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parsePayrollPeriodCloseReadInput(req) {
  return {
    tenantId: requireTenantId(req),
    closeId: parseCloseIdParam(req),
  };
}

export function parsePayrollPeriodClosePrepareInput(req) {
  const periodStart = parseDateOnly(req.body?.periodStart ?? req.body?.period_start, "periodStart");
  const periodEnd = parseDateOnly(req.body?.periodEnd ?? req.body?.period_end, "periodEnd");
  validatePeriodRange(periodStart, periodEnd);
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    legalEntityId: requirePositiveInt(
      req.body?.legalEntityId ?? req.body?.legal_entity_id,
      "legalEntityId"
    ),
    periodStart,
    periodEnd,
    lockRunChanges: parseBooleanFlag(
      req.body?.lockRunChanges ?? req.body?.lock_run_changes,
      true
    ),
    lockManualSettlements: parseBooleanFlag(
      req.body?.lockManualSettlements ?? req.body?.lock_manual_settlements,
      true
    ),
    lockPaymentPrep: parseBooleanFlag(
      req.body?.lockPaymentPrep ?? req.body?.lock_payment_prep,
      false
    ),
    note: normalizeText(req.body?.note, "note", 500),
  };
}

export function parsePayrollPeriodCloseRequestInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    closeId: parseCloseIdParam(req),
    note: normalizeText(req.body?.note, "note", 500),
    requestIdempotencyKey: normalizeText(
      req.body?.requestIdempotencyKey ?? req.body?.request_idempotency_key,
      "requestIdempotencyKey",
      190
    ),
  };
}

export function parsePayrollPeriodCloseApproveInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    closeId: parseCloseIdParam(req),
    note: normalizeText(req.body?.note, "note", 500),
    closeIdempotencyKey: normalizeText(
      req.body?.closeIdempotencyKey ?? req.body?.close_idempotency_key,
      "closeIdempotencyKey",
      190
    ),
  };
}

export function parsePayrollPeriodCloseReopenInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    closeId: parseCloseIdParam(req),
    reason: normalizeText(req.body?.reason, "reason", 500, { required: true }),
  };
}

