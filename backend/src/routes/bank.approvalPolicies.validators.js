import {
  normalizeCode,
  normalizeCurrencyCode,
  normalizeEnum,
  normalizeText,
  optionalPositiveInt,
  parseAmount,
  parseBooleanFlag,
  parsePagination,
  requirePositiveInt,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";
import { badRequest } from "./_utils.js";

const POLICY_STATUS = ["ACTIVE", "PAUSED", "DISABLED"];
const SCOPE_TYPES = ["GLOBAL", "LEGAL_ENTITY", "BANK_ACCOUNT"];
const MODULE_CODES = ["BANK", "PAYROLL"];

function normalizeOptionalEnum(value, label, allowedValues) {
  if (value === undefined || value === null || value === "") return null;
  return normalizeEnum(value, label, allowedValues);
}

function parseOptionalAmount(value, label) {
  if (value === undefined || value === null || value === "") return null;
  return Number(parseAmount(value, label, { allowZero: true, required: false }));
}

function parseOptionalDate(value, label) {
  if (value === undefined || value === null || value === "") return null;
  const raw = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) throw badRequest(`${label} must be YYYY-MM-DD`);
  return raw;
}

export function parseBankApprovalPoliciesListInput(req) {
  const tenantId = requireTenantId(req);
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });
  return {
    tenantId,
    ...pagination,
    moduleCode: normalizeOptionalEnum(req.query?.moduleCode ?? req.query?.module_code, "moduleCode", MODULE_CODES),
    status: normalizeOptionalEnum(req.query?.status, "status", POLICY_STATUS),
    targetType: req.query?.targetType ? normalizeCode(req.query.targetType, "targetType", 40) : null,
    actionType: req.query?.actionType ? normalizeCode(req.query.actionType, "actionType", 40) : null,
    scopeType: normalizeOptionalEnum(req.query?.scopeType, "scopeType", SCOPE_TYPES),
    legalEntityId: optionalPositiveInt(req.query?.legalEntityId, "legalEntityId"),
    bankAccountId: optionalPositiveInt(req.query?.bankAccountId, "bankAccountId"),
    q: normalizeText(req.query?.q, "q", 120),
  };
}

export function parseBankApprovalPolicyIdParam(req) {
  return requirePositiveInt(req.params?.policyId ?? req.params?.id, "policyId");
}

export function parseBankApprovalPolicyCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const body = req.body || {};
  const minAmount = parseOptionalAmount(body.minAmount ?? body.min_amount, "minAmount");
  const maxAmount = parseOptionalAmount(body.maxAmount ?? body.max_amount, "maxAmount");
  if (minAmount !== null && maxAmount !== null && minAmount > maxAmount) {
    throw badRequest("minAmount cannot exceed maxAmount");
  }

  return {
    tenantId,
    userId,
    policyCode: normalizeCode(body.policyCode ?? body.policy_code, "policyCode", 60),
    policyName: normalizeText(body.policyName ?? body.policy_name, "policyName", 190, {
      required: true,
    }),
    moduleCode: normalizeEnum(body.moduleCode ?? body.module_code ?? "BANK", "moduleCode", MODULE_CODES),
    status: normalizeEnum(body.status || "ACTIVE", "status", POLICY_STATUS),
    targetType: normalizeCode(body.targetType ?? body.target_type, "targetType", 40),
    actionType: normalizeCode(body.actionType ?? body.action_type, "actionType", 40),
    scopeType: normalizeEnum(body.scopeType || body.scope_type || "GLOBAL", "scopeType", SCOPE_TYPES),
    legalEntityId: optionalPositiveInt(body.legalEntityId ?? body.legal_entity_id, "legalEntityId"),
    bankAccountId: optionalPositiveInt(body.bankAccountId ?? body.bank_account_id, "bankAccountId"),
    currencyCode:
      body.currencyCode === undefined && body.currency_code === undefined
        ? null
        : normalizeCurrencyCode(body.currencyCode ?? body.currency_code, "currencyCode"),
    minAmount,
    maxAmount,
    requiredApprovals:
      body.requiredApprovals === undefined && body.required_approvals === undefined
        ? 1
        : requirePositiveInt(body.requiredApprovals ?? body.required_approvals, "requiredApprovals"),
    makerCheckerRequired: parseBooleanFlag(
      body.makerCheckerRequired ?? body.maker_checker_required,
      true
    ),
    approverPermissionCode:
      normalizeText(
        body.approverPermissionCode ?? body.approver_permission_code,
        "approverPermissionCode",
        120
      ) || "bank.approvals.requests.approve",
    autoExecuteOnFinalApproval: parseBooleanFlag(
      body.autoExecuteOnFinalApproval ?? body.auto_execute_on_final_approval,
      true
    ),
    effectiveFrom: parseOptionalDate(body.effectiveFrom ?? body.effective_from, "effectiveFrom"),
    effectiveTo: parseOptionalDate(body.effectiveTo ?? body.effective_to, "effectiveTo"),
  };
}

export function parseBankApprovalPolicyUpdateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const policyId = parseBankApprovalPolicyIdParam(req);
  const body = req.body || {};

  const hasMin = Object.prototype.hasOwnProperty.call(body, "minAmount") || Object.prototype.hasOwnProperty.call(body, "min_amount");
  const hasMax = Object.prototype.hasOwnProperty.call(body, "maxAmount") || Object.prototype.hasOwnProperty.call(body, "max_amount");
  const minAmount = hasMin ? parseOptionalAmount(body.minAmount ?? body.min_amount, "minAmount") : undefined;
  const maxAmount = hasMax ? parseOptionalAmount(body.maxAmount ?? body.max_amount, "maxAmount") : undefined;
  if (minAmount !== undefined && maxAmount !== undefined && minAmount !== null && maxAmount !== null && minAmount > maxAmount) {
    throw badRequest("minAmount cannot exceed maxAmount");
  }

  return {
    tenantId,
    userId,
    policyId,
    policyName:
      body.policyName !== undefined || body.policy_name !== undefined
        ? normalizeText(body.policyName ?? body.policy_name, "policyName", 190, { required: true })
        : undefined,
    moduleCode:
      body.moduleCode !== undefined || body.module_code !== undefined
        ? normalizeEnum(body.moduleCode ?? body.module_code, "moduleCode", MODULE_CODES)
        : undefined,
    status:
      body.status !== undefined ? normalizeEnum(body.status, "status", POLICY_STATUS) : undefined,
    legalEntityId:
      body.legalEntityId !== undefined || body.legal_entity_id !== undefined
        ? optionalPositiveInt(body.legalEntityId ?? body.legal_entity_id, "legalEntityId")
        : undefined,
    bankAccountId:
      body.bankAccountId !== undefined || body.bank_account_id !== undefined
        ? optionalPositiveInt(body.bankAccountId ?? body.bank_account_id, "bankAccountId")
        : undefined,
    currencyCode:
      body.currencyCode !== undefined || body.currency_code !== undefined
        ? (body.currencyCode ?? body.currency_code) === null || (body.currencyCode ?? body.currency_code) === ""
          ? null
          : normalizeCurrencyCode(body.currencyCode ?? body.currency_code, "currencyCode")
        : undefined,
    minAmount,
    maxAmount,
    requiredApprovals:
      body.requiredApprovals !== undefined || body.required_approvals !== undefined
        ? requirePositiveInt(body.requiredApprovals ?? body.required_approvals, "requiredApprovals")
        : undefined,
    makerCheckerRequired:
      body.makerCheckerRequired !== undefined || body.maker_checker_required !== undefined
        ? parseBooleanFlag(body.makerCheckerRequired ?? body.maker_checker_required, true)
        : undefined,
    approverPermissionCode:
      body.approverPermissionCode !== undefined || body.approver_permission_code !== undefined
        ? normalizeText(
            body.approverPermissionCode ?? body.approver_permission_code,
            "approverPermissionCode",
            120
          )
        : undefined,
    autoExecuteOnFinalApproval:
      body.autoExecuteOnFinalApproval !== undefined || body.auto_execute_on_final_approval !== undefined
        ? parseBooleanFlag(body.autoExecuteOnFinalApproval ?? body.auto_execute_on_final_approval, true)
        : undefined,
    effectiveFrom:
      body.effectiveFrom !== undefined || body.effective_from !== undefined
        ? parseOptionalDate(body.effectiveFrom ?? body.effective_from, "effectiveFrom")
        : undefined,
    effectiveTo:
      body.effectiveTo !== undefined || body.effective_to !== undefined
        ? parseOptionalDate(body.effectiveTo ?? body.effective_to, "effectiveTo")
        : undefined,
  };
}

export default {
  parseBankApprovalPoliciesListInput,
  parseBankApprovalPolicyIdParam,
  parseBankApprovalPolicyCreateInput,
  parseBankApprovalPolicyUpdateInput,
};
