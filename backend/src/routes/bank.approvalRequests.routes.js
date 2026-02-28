import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { resolveBankAccountScope } from "../services/bank.accounts.service.js";
import {
  approveBankApprovalRequest,
  getBankApprovalRequestById,
  listBankApprovalRequestRows,
  rejectBankApprovalRequest,
  resolveBankApprovalRequestScope,
  submitBankApprovalRequestFromRoute,
} from "../services/bank.approvals.service.js";
import {
  parseBankApprovalRequestDecisionInput,
  parseBankApprovalRequestIdParam,
  parseBankApprovalRequestsListInput,
  parseBankApprovalRequestSubmitInput,
} from "./bank.approvalRequests.validators.js";

const router = express.Router();

async function resolveApprovalRequestsListScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId ?? req.body?.legalEntityId ?? req.body?.legal_entity_id);
  if (legalEntityId) return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  const bankAccountId = parsePositiveInt(req.query?.bankAccountId ?? req.body?.bankAccountId ?? req.body?.bank_account_id);
  if (bankAccountId) return resolveBankAccountScope(bankAccountId, tenantId);
  return null;
}

router.get(
  "/approvals/requests",
  requirePermission("bank.approvals.requests.read", { resolveScope: resolveApprovalRequestsListScope }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalRequestsListInput(req);
    const result = await listBankApprovalRequestRows({
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
  "/approvals/requests/:requestId",
  requirePermission("bank.approvals.requests.read", {
    resolveScope: (req, tenantId) => resolveBankApprovalRequestScope(req.params?.requestId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const listInput = parseBankApprovalRequestsListInput(req);
    const requestId = parseBankApprovalRequestIdParam(req);
    const row = await getBankApprovalRequestById({
      req,
      tenantId: listInput.tenantId,
      requestId,
      assertScopeAccess,
    });
    return res.json({ tenantId: listInput.tenantId, row });
  })
);

router.post(
  "/approvals/requests",
  requirePermission("bank.approvals.requests.submit", { resolveScope: resolveApprovalRequestsListScope }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalRequestSubmitInput(req);
    const result = await submitBankApprovalRequestFromRoute({
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
  "/approvals/requests/:requestId/approve",
  requirePermission("bank.approvals.requests.approve", {
    resolveScope: (req, tenantId) => resolveBankApprovalRequestScope(req.params?.requestId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalRequestDecisionInput(req);
    const result = await approveBankApprovalRequest({
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
  "/approvals/requests/:requestId/reject",
  requirePermission("bank.approvals.requests.reject", {
    resolveScope: (req, tenantId) => resolveBankApprovalRequestScope(req.params?.requestId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalRequestDecisionInput(req);
    const result = await rejectBankApprovalRequest({
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
