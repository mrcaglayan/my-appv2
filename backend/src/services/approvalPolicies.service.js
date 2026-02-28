import { query } from "../db.js";
import { evaluateBankApprovalNeed, snapshotBankApprovalPolicy } from "./bank.governance.service.js";
import {
  approveBankApprovalRequest,
  executeBankApprovalRequest,
  getBankApprovalRequestById,
  listBankApprovalRequestRows,
  rejectBankApprovalRequest,
  resolveBankApprovalRequestScope,
  submitBankApprovalRequest,
  submitBankApprovalRequestFromRoute,
} from "./bank.approvals.service.js";
import {
  createBankApprovalPolicy,
  getBankApprovalPolicyById,
  listBankApprovalPolicies,
  resolveBankApprovalPolicyScope,
  updateBankApprovalPolicy,
} from "./bank.approvalPolicies.service.js";

function normalizeModuleCode(moduleCode, fallback = "BANK") {
  return String(moduleCode || fallback)
    .trim()
    .toUpperCase();
}

export function snapshotApprovalPolicy(policy) {
  return snapshotBankApprovalPolicy(policy);
}

export async function evaluateApprovalNeed({
  moduleCode = "BANK",
  tenantId,
  targetType,
  actionType,
  legalEntityId = null,
  bankAccountId = null,
  thresholdAmount = null,
  currencyCode = null,
  asOfDate = null,
  runQuery = query,
}) {
  return evaluateBankApprovalNeed({
    moduleCode: normalizeModuleCode(moduleCode),
    tenantId,
    targetType,
    actionType,
    legalEntityId,
    bankAccountId,
    thresholdAmount,
    currencyCode,
    asOfDate,
    runQuery,
  });
}

export async function submitApprovalRequest({
  tenantId,
  userId,
  requestInput,
  snapshotBuilder = null,
  policyOverride = null,
  runQuery = query,
}) {
  return submitBankApprovalRequest({
    tenantId,
    userId,
    requestInput: {
      ...requestInput,
      moduleCode: normalizeModuleCode(requestInput?.moduleCode),
    },
    snapshotBuilder,
    policyOverride,
    runQuery,
  });
}

export async function submitApprovalRequestFromRoute({
  req,
  tenantId,
  userId,
  input,
  assertScopeAccess,
}) {
  return submitBankApprovalRequestFromRoute({
    req,
    tenantId,
    userId,
    input: {
      ...input,
      moduleCode: normalizeModuleCode(input?.moduleCode),
    },
    assertScopeAccess,
  });
}

export {
  resolveBankApprovalPolicyScope as resolveApprovalPolicyScope,
  listBankApprovalPolicies as listApprovalPolicies,
  getBankApprovalPolicyById as getApprovalPolicyById,
  createBankApprovalPolicy as createApprovalPolicy,
  updateBankApprovalPolicy as updateApprovalPolicy,
  resolveBankApprovalRequestScope as resolveApprovalRequestScope,
  listBankApprovalRequestRows as listApprovalRequestRows,
  getBankApprovalRequestById as getApprovalRequestById,
  approveBankApprovalRequest as approveApprovalRequest,
  rejectBankApprovalRequest as rejectApprovalRequest,
  executeBankApprovalRequest as executeApprovalRequest,
};

export default {
  snapshotApprovalPolicy,
  evaluateApprovalNeed,
  submitApprovalRequest,
  submitApprovalRequestFromRoute,
  resolveApprovalPolicyScope: resolveBankApprovalPolicyScope,
  listApprovalPolicies: listBankApprovalPolicies,
  getApprovalPolicyById: getBankApprovalPolicyById,
  createApprovalPolicy: createBankApprovalPolicy,
  updateApprovalPolicy: updateBankApprovalPolicy,
  resolveApprovalRequestScope: resolveBankApprovalRequestScope,
  listApprovalRequestRows: listBankApprovalRequestRows,
  getApprovalRequestById: getBankApprovalRequestById,
  approveApprovalRequest: approveBankApprovalRequest,
  rejectApprovalRequest: rejectBankApprovalRequest,
  executeApprovalRequest: executeBankApprovalRequest,
};

