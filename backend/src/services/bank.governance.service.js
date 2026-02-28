import { query } from "../db.js";
import { parsePositiveInt } from "../routes/_utils.js";

function u(value) {
  return String(value || "").trim().toUpperCase();
}

function toAmount(value) {
  if (value === undefined || value === null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? Number(n.toFixed(6)) : null;
}

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function hydratePolicy(row) {
  if (!row) return null;
  return {
    ...row,
    module_code: u(row.module_code || "BANK"),
    min_amount: toAmount(row.min_amount),
    max_amount: toAmount(row.max_amount),
    maker_checker_required: parseDbBoolean(row.maker_checker_required),
    auto_execute_on_final_approval: parseDbBoolean(row.auto_execute_on_final_approval),
    required_approvals: Number(row.required_approvals || 1),
  };
}

function policyMatchesAmount(policy, amount) {
  const thresholdAmount = toAmount(amount);
  const minAmount = toAmount(policy?.min_amount);
  const maxAmount = toAmount(policy?.max_amount);
  if (minAmount === null && maxAmount === null) return true;
  if (thresholdAmount === null) return false;
  if (minAmount !== null && thresholdAmount < minAmount) return false;
  if (maxAmount !== null && thresholdAmount > maxAmount) return false;
  return true;
}

function policyMatchesCurrency(policy, currencyCode) {
  const policyCurrency = u(policy?.currency_code);
  if (!policyCurrency) return true;
  return policyCurrency === u(currencyCode);
}

function policyScopeRank(policy) {
  const scope = u(policy?.scope_type);
  if (scope === "BANK_ACCOUNT") return 3;
  if (scope === "LEGAL_ENTITY") return 2;
  return 1;
}

function policyMatchesScope(policy, { legalEntityId = null, bankAccountId = null } = {}) {
  const scopeType = u(policy?.scope_type);
  if (scopeType === "GLOBAL") return true;
  if (scopeType === "LEGAL_ENTITY") {
    return parsePositiveInt(policy?.legal_entity_id) === parsePositiveInt(legalEntityId);
  }
  if (scopeType === "BANK_ACCOUNT") {
    return parsePositiveInt(policy?.bank_account_id) === parsePositiveInt(bankAccountId);
  }
  return false;
}

function sortPoliciesForMatch(a, b) {
  const rankDiff = policyScopeRank(b) - policyScopeRank(a);
  if (rankDiff !== 0) return rankDiff;
  const aMin = a.min_amount === null || a.min_amount === undefined ? -1 : Number(a.min_amount);
  const bMin = b.min_amount === null || b.min_amount === undefined ? -1 : Number(b.min_amount);
  if (bMin !== aMin) return bMin - aMin;
  const aReq = Number(a.required_approvals || 1);
  const bReq = Number(b.required_approvals || 1);
  if (bReq !== aReq) return bReq - aReq;
  return Number(b.id || 0) - Number(a.id || 0);
}

export function snapshotBankApprovalPolicy(policy) {
  if (!policy) return null;
  return {
    id: parsePositiveInt(policy.id),
    module_code: u(policy.module_code || "BANK"),
    policy_code: policy.policy_code || null,
    policy_name: policy.policy_name || null,
    status: u(policy.status),
    target_type: u(policy.target_type),
    action_type: u(policy.action_type),
    scope_type: u(policy.scope_type),
    legal_entity_id: parsePositiveInt(policy.legal_entity_id) || null,
    bank_account_id: parsePositiveInt(policy.bank_account_id) || null,
    currency_code: u(policy.currency_code || "") || null,
    min_amount: toAmount(policy.min_amount),
    max_amount: toAmount(policy.max_amount),
    required_approvals: Number(policy.required_approvals || 1),
    maker_checker_required: Boolean(policy.maker_checker_required),
    approver_permission_code:
      String(policy.approver_permission_code || "bank.approvals.requests.approve").trim() ||
      "bank.approvals.requests.approve",
    auto_execute_on_final_approval: Boolean(policy.auto_execute_on_final_approval),
    effective_from: policy.effective_from || null,
    effective_to: policy.effective_to || null,
  };
}

export async function evaluateBankApprovalNeed({
  tenantId,
  moduleCode = "BANK",
  targetType,
  actionType,
  legalEntityId = null,
  bankAccountId = null,
  thresholdAmount = null,
  currencyCode = null,
  asOfDate = null,
  runQuery = query,
}) {
  const evalDate = String(asOfDate || new Date().toISOString().slice(0, 10));
  const res = await runQuery(
    `SELECT *
     FROM bank_approval_policies
     WHERE tenant_id = ?
       AND COALESCE(module_code, 'BANK') = ?
       AND status = 'ACTIVE'
       AND target_type = ?
       AND action_type = ?
       AND (effective_from IS NULL OR effective_from <= ?)
       AND (effective_to IS NULL OR effective_to >= ?)`,
    [tenantId, u(moduleCode || "BANK"), u(targetType), u(actionType), evalDate, evalDate]
  );

  const candidates = (res.rows || [])
    .map(hydratePolicy)
    .filter((policy) =>
      policyMatchesScope(policy, {
        legalEntityId: parsePositiveInt(legalEntityId),
        bankAccountId: parsePositiveInt(bankAccountId),
      })
    )
    .filter((policy) => policyMatchesCurrency(policy, currencyCode))
    .filter((policy) => policyMatchesAmount(policy, thresholdAmount))
    .sort(sortPoliciesForMatch);

  const matchedPolicy = candidates[0] || null;
  if (!matchedPolicy) {
    return {
      approvalRequired: false,
      approval_required: false,
      policy: null,
      policySnapshot: null,
    };
  }

  return {
    approvalRequired: true,
    approval_required: true,
    policy: matchedPolicy,
    policySnapshot: snapshotBankApprovalPolicy(matchedPolicy),
  };
}

export default {
  evaluateBankApprovalNeed,
  snapshotBankApprovalPolicy,
};
