import {
  normalizeCode,
  normalizeCurrencyCode,
  normalizeText,
  optionalPositiveInt,
  parseAmount,
  parseBooleanFlag,
  parsePagination,
  requirePositiveInt,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

function parseOptionalAmount(value, label) {
  if (value === undefined || value === null || value === "") return null;
  return Number(parseAmount(value, label, { allowZero: true }));
}

export function parseBankApprovalRequestsListInput(req) {
  const tenantId = requireTenantId(req);
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });
  return {
    tenantId,
    ...pagination,
    moduleCode: req.query?.moduleCode || req.query?.module_code
      ? normalizeCode(req.query?.moduleCode ?? req.query?.module_code, "moduleCode", 20)
      : null,
    requestStatus: req.query?.requestStatus
      ? normalizeCode(req.query.requestStatus, "requestStatus", 20)
      : null,
    targetType: req.query?.targetType ? normalizeCode(req.query.targetType, "targetType", 40) : null,
    actionType: req.query?.actionType ? normalizeCode(req.query.actionType, "actionType", 40) : null,
    mineOnly: parseBooleanFlag(req.query?.mineOnly ?? req.query?.mine_only, false),
  };
}

export function parseBankApprovalRequestIdParam(req) {
  return requirePositiveInt(req.params?.requestId ?? req.params?.id, "requestId");
}

export function parseBankApprovalRequestSubmitInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const body = req.body || {};
  return {
    tenantId,
    userId,
    moduleCode: normalizeCode(body.moduleCode ?? body.module_code ?? "BANK", "moduleCode", 20),
    requestKey: normalizeText(body.requestKey ?? body.request_key, "requestKey", 190),
    targetType: normalizeCode(body.targetType ?? body.target_type, "targetType", 40),
    targetId: optionalPositiveInt(body.targetId ?? body.target_id, "targetId"),
    actionType: normalizeCode(body.actionType ?? body.action_type, "actionType", 40),
    legalEntityId: optionalPositiveInt(body.legalEntityId ?? body.legal_entity_id, "legalEntityId"),
    bankAccountId: optionalPositiveInt(body.bankAccountId ?? body.bank_account_id, "bankAccountId"),
    thresholdAmount: parseOptionalAmount(body.thresholdAmount ?? body.threshold_amount, "thresholdAmount"),
    currencyCode:
      body.currencyCode === undefined && body.currency_code === undefined
        ? null
        : normalizeCurrencyCode(body.currencyCode ?? body.currency_code, "currencyCode"),
    actionPayload: body.actionPayload ?? body.action_payload ?? null,
    targetSnapshot: body.targetSnapshot ?? body.target_snapshot ?? null,
    policyCode: normalizeText(body.policyCode ?? body.policy_code, "policyCode", 60),
  };
}

export function parseBankApprovalRequestDecisionInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const requestId = parseBankApprovalRequestIdParam(req);
  const body = req.body || {};
  return {
    tenantId,
    userId,
    requestId,
    decisionComment: normalizeText(body.decisionComment ?? body.decision_comment, "decisionComment", 500),
  };
}

export default {
  parseBankApprovalRequestsListInput,
  parseBankApprovalRequestIdParam,
  parseBankApprovalRequestSubmitInput,
  parseBankApprovalRequestDecisionInput,
};
