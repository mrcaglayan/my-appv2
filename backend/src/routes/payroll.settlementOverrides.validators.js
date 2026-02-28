import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeText,
  parseAmount,
  parseDateTime,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

function parseLiabilityIdParam(req) {
  const liabilityId = parsePositiveInt(req.params?.liabilityId ?? req.params?.id);
  if (!liabilityId) {
    throw badRequest("liabilityId must be a positive integer");
  }
  return liabilityId;
}

function parseOverrideRequestIdParam(req) {
  const requestId = parsePositiveInt(req.params?.requestId ?? req.params?.id);
  if (!requestId) {
    throw badRequest("requestId must be a positive integer");
  }
  return requestId;
}

export function parsePayrollManualSettlementRequestListInput(req) {
  return {
    tenantId: requireTenantId(req),
    liabilityId: parseLiabilityIdParam(req),
  };
}

export function parsePayrollManualSettlementRequestCreateInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    liabilityId: parseLiabilityIdParam(req),
    amount: Number(
      parseAmount(req.body?.amount, "amount", { required: true, allowZero: false })
    ),
    settledAt: parseDateTime(
      req.body?.settledAt ?? req.body?.settled_at,
      "settledAt"
    ),
    reason: normalizeText(req.body?.reason, "reason", 500, { required: true }),
    externalRef: normalizeText(
      req.body?.externalRef ?? req.body?.external_ref,
      "externalRef",
      190
    ),
    idempotencyKey: normalizeText(
      req.body?.idempotencyKey ?? req.body?.idempotency_key,
      "idempotencyKey",
      190
    ),
  };
}

export function parsePayrollManualSettlementRequestApproveInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    requestId: parseOverrideRequestIdParam(req),
    decisionNote: normalizeText(
      req.body?.decisionNote ?? req.body?.decision_note,
      "decisionNote",
      500
    ),
  };
}

export function parsePayrollManualSettlementRequestRejectInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    requestId: parseOverrideRequestIdParam(req),
    decisionNote:
      normalizeText(
        req.body?.decisionNote ?? req.body?.decision_note,
        "decisionNote",
        500
      ) || "Rejected",
  };
}
