import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeText,
  optionalPositiveInt,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const EXCEPTION_STATUSES = ["OPEN", "ASSIGNED", "RESOLVED", "IGNORED"];
const EXCEPTION_REASON_CODES = [
  "NO_RULE_MATCH",
  "AMBIGUOUS_TARGET",
  "POLICY_BLOCKED",
  "APPLY_ERROR",
  "RULE_QUEUE_EXCEPTION",
];

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

export function parseReconciliationExceptionIdParam(req) {
  const exceptionId = parsePositiveInt(req.params?.exceptionId ?? req.params?.id);
  if (!exceptionId) {
    throw badRequest("exceptionId must be a positive integer");
  }
  return exceptionId;
}

export function parseReconciliationExceptionListFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const bankAccountId = optionalPositiveInt(req.query?.bankAccountId, "bankAccountId");
  const statementLineId = optionalPositiveInt(
    req.query?.statementLineId ?? req.query?.statement_line_id,
    "statementLineId"
  );
  const status = normalizeEnumOrNull(req.query?.status, "status", EXCEPTION_STATUSES);
  const reasonCode = normalizeEnumOrNull(
    req.query?.reasonCode ?? req.query?.reason_code,
    "reasonCode",
    EXCEPTION_REASON_CODES
  );
  const q = normalizeText(req.query?.q, "q", 120) || null;
  const cursor = normalizeText(req.query?.cursor, "cursor", 1200) || null;
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });

  return {
    tenantId,
    legalEntityId,
    bankAccountId,
    statementLineId,
    status,
    reasonCode,
    q,
    cursor,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseReconciliationExceptionAssignInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const exceptionId = parseReconciliationExceptionIdParam(req);
  const assignedToUserId =
    req.body?.assignedToUserId === undefined && req.body?.assigned_to_user_id === undefined
      ? userId
      : optionalPositiveInt(
          req.body?.assignedToUserId ?? req.body?.assigned_to_user_id,
          "assignedToUserId"
        );

  return {
    tenantId,
    userId,
    exceptionId,
    assignedToUserId: assignedToUserId || null,
  };
}

export function parseReconciliationExceptionResolveInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const exceptionId = parseReconciliationExceptionIdParam(req);

  return {
    tenantId,
    userId,
    exceptionId,
    resolutionCode:
      normalizeText(req.body?.resolutionCode ?? req.body?.resolution_code, "resolutionCode", 50) ||
      "RESOLVED_MANUALLY",
    resolutionNote: normalizeText(
      req.body?.resolutionNote ?? req.body?.resolution_note,
      "resolutionNote",
      500
    ),
  };
}

export function parseReconciliationExceptionIgnoreInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const exceptionId = parseReconciliationExceptionIdParam(req);
  return {
    tenantId,
    userId,
    exceptionId,
    resolutionNote:
      normalizeText(req.body?.resolutionNote ?? req.body?.resolution_note, "resolutionNote", 500) ||
      "Ignored",
  };
}

export function parseReconciliationExceptionRetryInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const exceptionId = parseReconciliationExceptionIdParam(req);
  return {
    tenantId,
    userId,
    exceptionId,
    note: normalizeText(req.body?.note, "note", 500),
  };
}

export default {
  parseReconciliationExceptionIdParam,
  parseReconciliationExceptionListFilters,
  parseReconciliationExceptionAssignInput,
  parseReconciliationExceptionResolveInput,
  parseReconciliationExceptionIgnoreInput,
  parseReconciliationExceptionRetryInput,
};
