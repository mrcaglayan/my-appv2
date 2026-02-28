import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { resolveBankAccountScope } from "../services/bank.accounts.service.js";
import {
  approveApprovalRequest,
  createApprovalPolicy,
  getApprovalPolicyById,
  getApprovalRequestById,
  listApprovalPolicies,
  listApprovalRequestRows,
  rejectApprovalRequest,
  resolveApprovalPolicyScope,
  resolveApprovalRequestScope,
  submitApprovalRequestFromRoute,
  updateApprovalPolicy,
} from "../services/approvalPolicies.service.js";
import {
  parseBankApprovalPoliciesListInput,
  parseBankApprovalPolicyCreateInput,
  parseBankApprovalPolicyIdParam,
  parseBankApprovalPolicyUpdateInput,
} from "./bank.approvalPolicies.validators.js";
import {
  parseBankApprovalRequestDecisionInput,
  parseBankApprovalRequestIdParam,
  parseBankApprovalRequestsListInput,
  parseBankApprovalRequestSubmitInput,
} from "./bank.approvalRequests.validators.js";

const router = express.Router();

async function resolvePoliciesScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId ?? req.body?.legalEntityId ?? req.body?.legal_entity_id);
  if (legalEntityId) return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  const bankAccountId = parsePositiveInt(req.query?.bankAccountId ?? req.body?.bankAccountId ?? req.body?.bank_account_id);
  if (bankAccountId) return resolveBankAccountScope(bankAccountId, tenantId);
  return null;
}

async function resolveRequestsScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId ?? req.body?.legalEntityId ?? req.body?.legal_entity_id);
  if (legalEntityId) return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  const bankAccountId = parsePositiveInt(req.query?.bankAccountId ?? req.body?.bankAccountId ?? req.body?.bank_account_id);
  if (bankAccountId) return resolveBankAccountScope(bankAccountId, tenantId);
  return null;
}

router.get(
  "/policies",
  requirePermission("approvals.policies.read", { resolveScope: resolvePoliciesScope }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalPoliciesListInput(req);
    const result = await listApprovalPolicies({
      req,
      tenantId: input.tenantId,
      filters: input,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.get(
  "/policies/:policyId",
  requirePermission("approvals.policies.read", {
    resolveScope: (req, tenantId) => resolveApprovalPolicyScope(req.params?.policyId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const listInput = parseBankApprovalPoliciesListInput(req);
    const policyId = parseBankApprovalPolicyIdParam(req);
    const row = await getApprovalPolicyById({
      req,
      tenantId: listInput.tenantId,
      policyId,
      assertScopeAccess,
    });
    return res.json({ tenantId: listInput.tenantId, row });
  })
);

router.post(
  "/policies",
  requirePermission("approvals.policies.write", { resolveScope: resolvePoliciesScope }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalPolicyCreateInput(req);
    // For PAYROLL policies, default approver permission to generic H04 approval permission if caller did not set it.
    const rawApproverPermission = req.body?.approverPermissionCode ?? req.body?.approver_permission_code;
    if (
      String(input.moduleCode || "BANK").toUpperCase() === "PAYROLL" &&
      (rawApproverPermission === undefined || rawApproverPermission === null || rawApproverPermission === "")
    ) {
      input.approverPermissionCode = "approvals.requests.approve";
    }
    const row = await createApprovalPolicy({ req, input, assertScopeAccess });
    return res.status(201).json({ tenantId: input.tenantId, row });
  })
);

router.patch(
  "/policies/:policyId",
  requirePermission("approvals.policies.write", {
    resolveScope: (req, tenantId) => resolveApprovalPolicyScope(req.params?.policyId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalPolicyUpdateInput(req);
    const rawApproverPermission = req.body?.approverPermissionCode ?? req.body?.approver_permission_code;
    if (
      String(input.moduleCode || "").toUpperCase() === "PAYROLL" &&
      (rawApproverPermission === undefined || rawApproverPermission === null || rawApproverPermission === "")
    ) {
      input.approverPermissionCode = "approvals.requests.approve";
    }
    const row = await updateApprovalPolicy({ req, input, assertScopeAccess });
    return res.json({ tenantId: input.tenantId, row });
  })
);

router.get(
  "/requests",
  requirePermission("approvals.requests.read", { resolveScope: resolveRequestsScope }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalRequestsListInput(req);
    const result = await listApprovalRequestRows({
      req,
      tenantId: input.tenantId,
      filters: input,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.get(
  "/requests/:requestId",
  requirePermission("approvals.requests.read", {
    resolveScope: (req, tenantId) => resolveApprovalRequestScope(req.params?.requestId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const listInput = parseBankApprovalRequestsListInput(req);
    const requestId = parseBankApprovalRequestIdParam(req);
    const row = await getApprovalRequestById({
      req,
      tenantId: listInput.tenantId,
      requestId,
      assertScopeAccess,
    });
    return res.json({ tenantId: listInput.tenantId, row });
  })
);

router.post(
  "/requests",
  requirePermission("approvals.requests.submit", { resolveScope: resolveRequestsScope }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalRequestSubmitInput(req);
    if (
      String(input.moduleCode || "BANK").toUpperCase() === "PAYROLL" &&
      !req.body?.targetSnapshot &&
      !req.body?.target_snapshot
    ) {
      input.targetSnapshot = {
        module_code: "PAYROLL",
        target_type: input.targetType,
        target_id: input.targetId || null,
      };
    }
    const result = await submitApprovalRequestFromRoute({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      input,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: input.tenantId,
      approval_required: Boolean(result.approval_required),
      item: result.item || null,
      idempotent: Boolean(result.idempotent),
    });
  })
);

router.post(
  "/requests/:requestId/approve",
  requirePermission("approvals.requests.approve", {
    resolveScope: (req, tenantId) => resolveApprovalRequestScope(req.params?.requestId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalRequestDecisionInput(req);
    const result = await approveApprovalRequest({
      req,
      tenantId: input.tenantId,
      requestId: input.requestId,
      userId: input.userId,
      decisionComment: input.decisionComment,
      assertScopeAccess,
    });
    return res.json({
      tenantId: input.tenantId,
      item: result.item || null,
      execution_result: result.execution_result || null,
      idempotent: Boolean(result.idempotent),
    });
  })
);

router.post(
  "/requests/:requestId/reject",
  requirePermission("approvals.requests.reject", {
    resolveScope: (req, tenantId) => resolveApprovalRequestScope(req.params?.requestId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalRequestDecisionInput(req);
    const result = await rejectApprovalRequest({
      req,
      tenantId: input.tenantId,
      requestId: input.requestId,
      userId: input.userId,
      decisionComment: input.decisionComment,
      assertScopeAccess,
    });
    return res.json({
      tenantId: input.tenantId,
      item: result.item || null,
      idempotent: Boolean(result.idempotent),
    });
  })
);

export default router;

