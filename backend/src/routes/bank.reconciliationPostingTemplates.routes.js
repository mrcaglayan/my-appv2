import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { requireTenantId } from "./cash.validators.common.js";
import { resolveBankAccountScope } from "../services/bank.accounts.service.js";
import {
  createPostingTemplate,
  getPostingTemplateByIdForTenant,
  listPostingTemplateRows,
  resolvePostingTemplateScope,
  updatePostingTemplate,
} from "../services/bank.reconciliationPostingTemplates.service.js";
import {
  parsePostingTemplateCreateInput,
  parsePostingTemplateIdParam,
  parsePostingTemplateListFilters,
  parsePostingTemplateUpdateInput,
} from "./bank.reconciliationPostingTemplates.validators.js";

const router = express.Router();

async function resolveTemplateListScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId ?? req.body?.legalEntityId);
  if (legalEntityId) return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  const bankAccountId = parsePositiveInt(req.query?.bankAccountId ?? req.body?.bankAccountId);
  if (bankAccountId) return resolveBankAccountScope(bankAccountId, tenantId);
  return null;
}

router.get(
  "/posting-templates",
  requirePermission("bank.reconcile.templates.read", {
    resolveScope: resolveTemplateListScope,
  }),
  asyncHandler(async (req, res) => {
    const filters = parsePostingTemplateListFilters(req);
    const result = await listPostingTemplateRows({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: filters.tenantId, ...result });
  })
);

router.get(
  "/posting-templates/:templateId",
  requirePermission("bank.reconcile.templates.read", {
    resolveScope: async (req, tenantId) => resolvePostingTemplateScope(req.params?.templateId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const templateId = parsePostingTemplateIdParam(req);
    const row = await getPostingTemplateByIdForTenant({
      req,
      tenantId,
      templateId,
      assertScopeAccess,
    });
    return res.json({ tenantId, row });
  })
);

router.post(
  "/posting-templates",
  requirePermission("bank.reconcile.templates.write", {
    resolveScope: resolveTemplateListScope,
  }),
  asyncHandler(async (req, res) => {
    const input = parsePostingTemplateCreateInput(req);
    const result = await createPostingTemplate({
      req,
      input,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: input.tenantId,
      row: result?.row || result || null,
      approval_required: Boolean(result?.approval_required),
      approval_request: result?.approval_request || null,
      idempotent: Boolean(result?.idempotent),
    });
  })
);

router.patch(
  "/posting-templates/:templateId",
  requirePermission("bank.reconcile.templates.write", {
    resolveScope: async (req, tenantId) => {
      const scope = await resolvePostingTemplateScope(req.params?.templateId, tenantId);
      if (scope) return scope;
      const legalEntityId = parsePositiveInt(req.body?.legalEntityId ?? req.body?.legal_entity_id);
      if (legalEntityId) return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const input = parsePostingTemplateUpdateInput(req);
    const result = await updatePostingTemplate({
      req,
      input,
      assertScopeAccess,
    });
    return res.json({
      tenantId: input.tenantId,
      row: result?.row || result || null,
      approval_required: Boolean(result?.approval_required),
      approval_request: result?.approval_request || null,
      idempotent: Boolean(result?.idempotent),
    });
  })
);

export default router;
