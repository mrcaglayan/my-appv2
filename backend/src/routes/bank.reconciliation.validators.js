import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeText,
  optionalPositiveInt,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const QUEUE_STATUS_VALUES = ["UNMATCHED", "PARTIAL", "MATCHED", "IGNORED"];
const MATCH_ENTITY_TYPES = ["JOURNAL", "PAYMENT_BATCH", "CASH_TXN", "MANUAL_ADJUSTMENT"];

function normalizeEnumOrNull(value, label, allowedValues) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const normalized = String(value).trim().toUpperCase();
  if (!allowedValues.includes(normalized)) {
    throw badRequest(`${label} must be one of ${allowedValues.join(", ")}`);
  }
  return normalized;
}

function parsePositiveAmount(value, label) {
  if (value === undefined || value === null || value === "") {
    throw badRequest(`${label} is required`);
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw badRequest(`${label} must be a positive number`);
  }
  return Number(parsed.toFixed(6));
}

export function parseReconciliationLineIdParam(req) {
  const lineId = parsePositiveInt(req.params?.lineId);
  if (!lineId) {
    throw badRequest("lineId must be a positive integer");
  }
  return lineId;
}

export function parseReconciliationQueueFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const bankAccountId = optionalPositiveInt(req.query?.bankAccountId, "bankAccountId");
  const reconStatus = normalizeEnumOrNull(
    req.query?.reconStatus ?? req.query?.recon_status,
    "reconStatus",
    QUEUE_STATUS_VALUES
  );
  const q = normalizeText(req.query?.q, "q", 120) || null;
  const cursor = normalizeText(req.query?.cursor, "cursor", 1200) || null;
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });

  return {
    tenantId,
    legalEntityId,
    bankAccountId,
    reconStatus,
    q,
    cursor,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseReconciliationAuditFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const statementLineId = optionalPositiveInt(
    req.query?.statementLineId ?? req.query?.statement_line_id,
    "statementLineId"
  );
  const action = normalizeText(req.query?.action, "action", 30);
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });

  return {
    tenantId,
    legalEntityId,
    statementLineId,
    action: action ? action.toUpperCase() : null,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseReconciliationMatchInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const lineId = parseReconciliationLineIdParam(req);

  const matchType = normalizeText(
    req.body?.matchType ?? req.body?.match_type,
    "matchType",
    30
  );
  const matchedEntityType = normalizeEnumOrNull(
    req.body?.matchedEntityType ?? req.body?.matched_entity_type,
    "matchedEntityType",
    MATCH_ENTITY_TYPES
  );
  if (!matchedEntityType) {
    throw badRequest("matchedEntityType is required");
  }

  const matchedEntityId = optionalPositiveInt(
    req.body?.matchedEntityId ?? req.body?.matched_entity_id,
    "matchedEntityId"
  );
  if (!matchedEntityId) {
    throw badRequest("matchedEntityId is required");
  }

  const matchedAmount = parsePositiveAmount(
    req.body?.matchedAmount ?? req.body?.matched_amount,
    "matchedAmount"
  );

  return {
    tenantId,
    userId,
    lineId,
    matchType: String(matchType || "MANUAL").trim().toUpperCase(),
    matchedEntityType,
    matchedEntityId,
    matchedAmount,
    notes: normalizeText(req.body?.notes, "notes", 500),
  };
}

export function parseReconciliationUnmatchInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const lineId = parseReconciliationLineIdParam(req);
  const matchId = optionalPositiveInt(req.body?.matchId ?? req.body?.match_id, "matchId");

  return {
    tenantId,
    userId,
    lineId,
    matchId,
    notes: normalizeText(req.body?.notes, "notes", 500),
  };
}

export function parseReconciliationIgnoreInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const lineId = parseReconciliationLineIdParam(req);

  return {
    tenantId,
    userId,
    lineId,
    reason: normalizeText(req.body?.reason, "reason", 500),
  };
}

export function parseReconciliationUnignoreInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const lineId = parseReconciliationLineIdParam(req);

  return {
    tenantId,
    userId,
    lineId,
    reason: normalizeText(req.body?.reason, "reason", 500),
  };
}
