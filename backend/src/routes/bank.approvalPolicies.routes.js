import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { resolveBankAccountScope } from "../services/bank.accounts.service.js";
import {
  createBankApprovalPolicy,
  getBankApprovalPolicyById,
  listBankApprovalPolicies,
  resolveBankApprovalPolicyScope,
  updateBankApprovalPolicy,
} from "../services/bank.approvalPolicies.service.js";
import {
  parseBankApprovalPoliciesListInput,
  parseBankApprovalPolicyCreateInput,
  parseBankApprovalPolicyIdParam,
  parseBankApprovalPolicyUpdateInput,
} from "./bank.approvalPolicies.validators.js";

const router = express.Router();

async function resolvePoliciesScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId ?? req.body?.legalEntityId ?? req.body?.legal_entity_id);
  if (legalEntityId) return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  const bankAccountId = parsePositiveInt(req.query?.bankAccountId ?? req.body?.bankAccountId ?? req.body?.bank_account_id);
  if (bankAccountId) return resolveBankAccountScope(bankAccountId, tenantId);
  return null;
}

router.get(
  "/approvals/policies",
  requirePermission("bank.approvals.policies.read", { resolveScope: resolvePoliciesScope }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalPoliciesListInput(req);
    const result = await listBankApprovalPolicies({
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
  "/approvals/policies/:policyId",
  requirePermission("bank.approvals.policies.read", {
    resolveScope: (req, tenantId) => resolveBankApprovalPolicyScope(req.params?.policyId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalPoliciesListInput(req);
    const policyId = parseBankApprovalPolicyIdParam(req);
    const row = await getBankApprovalPolicyById({
      req,
      tenantId: input.tenantId,
      policyId,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, row });
  })
);

router.post(
  "/approvals/policies",
  requirePermission("bank.approvals.policies.create", { resolveScope: resolvePoliciesScope }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalPolicyCreateInput(req);
    const row = await createBankApprovalPolicy({ req, input, assertScopeAccess });
    return res.status(201).json({ tenantId: input.tenantId, row });
  })
);

router.patch(
  "/approvals/policies/:policyId",
  requirePermission("bank.approvals.policies.update", {
    resolveScope: (req, tenantId) => resolveBankApprovalPolicyScope(req.params?.policyId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankApprovalPolicyUpdateInput(req);
    const row = await updateBankApprovalPolicy({ req, input, assertScopeAccess });
    return res.json({ tenantId: input.tenantId, row });
  })
);

export default router;
