import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeEnum,
  normalizeText,
  optionalPositiveInt,
  parseAmount,
  parseBooleanFlag,
  parseDateOnly,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const SESSION_STATUSES = ["OPEN", "CLOSED", "CANCELLED"];
const CLOSE_REASONS = ["END_SHIFT", "FORCED_CLOSE", "COUNT_CORRECTION"];

export function parseCashSessionIdParam(req) {
  const sessionId = parsePositiveInt(req.params?.sessionId);
  if (!sessionId) {
    throw badRequest("sessionId must be a positive integer");
  }
  return sessionId;
}

export function parseCashSessionReadFilters(req) {
  const tenantId = requireTenantId(req);
  const registerId = optionalPositiveInt(req.query?.registerId, "registerId");
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const statusRaw = String(req.query?.status || "")
    .trim()
    .toUpperCase();
  const status = statusRaw ? normalizeEnum(statusRaw, "status", SESSION_STATUSES) : null;
  const openedFrom = req.query?.openedFrom
    ? parseDateOnly(req.query?.openedFrom, "openedFrom")
    : null;
  const openedTo = req.query?.openedTo ? parseDateOnly(req.query?.openedTo, "openedTo") : null;
  const pagination = parsePagination(req.query, { limit: 50, offset: 0, maxLimit: 200 });

  return {
    tenantId,
    registerId,
    legalEntityId,
    status,
    openedFrom,
    openedTo,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseCashSessionOpenInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const registerId = optionalPositiveInt(req.body?.registerId, "registerId");
  if (!registerId) {
    throw badRequest("registerId is required");
  }

  const openingAmount = parseAmount(req.body?.openingAmount, "openingAmount", {
    allowZero: true,
  });

  return {
    tenantId,
    userId,
    registerId,
    openingAmount: openingAmount ?? "0.000000",
  };
}

export function parseCashSessionCloseInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const sessionId = parseCashSessionIdParam(req);
  const countedClosingAmount = parseAmount(req.body?.countedClosingAmount, "countedClosingAmount", {
    allowZero: true,
    required: true,
  });
  const closedReason = normalizeEnum(
    req.body?.closedReason,
    "closedReason",
    CLOSE_REASONS,
    "END_SHIFT"
  );
  const closeNote = normalizeText(req.body?.closeNote, "closeNote", 500);
  const approveVariance = parseBooleanFlag(req.body?.approveVariance, false);

  return {
    tenantId,
    userId,
    sessionId,
    countedClosingAmount,
    closedReason,
    closeNote,
    approveVariance,
  };
}
