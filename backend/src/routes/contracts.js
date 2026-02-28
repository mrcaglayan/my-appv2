import express from "express";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { requirePermission, assertScopeAccess, buildScopeFilter } from "../middleware/rbac.js";
import {
  parseContractAmendInput,
  parseContractAmendmentsListInput,
  parseContractLinkableDocumentsInput,
  parseContractLinePatchInput,
  parseContractLinkAdjustmentInput,
  parseContractCreateInput,
  parseContractIdParam,
  parseContractLifecycleInput,
  parseContractLinkDocumentInput,
  parseContractGenerateBillingInput,
  parseContractGenerateRevrecInput,
  parseContractLinkUnlinkInput,
  parseContractListFilters,
  parseContractUpdateInput,
} from "./contracts.validators.js";
import {
  activateContractById,
  amendContractById,
  adjustContractDocumentLink,
  cancelContractById,
  patchContractLineById,
  closeContractById,
  createContract,
  generateContractBilling,
  generateContractRevrec,
  getContractByIdForTenant,
  linkDocumentToContract,
  listContractAmendments,
  listContractDocumentLinks,
  listContractLinkableDocuments,
  listContracts,
  resolveContractScope,
  suspendContractById,
  unlinkContractDocumentLink,
  updateContractById,
} from "../services/contracts.service.js";

const router = express.Router();

function resolveLegalEntityScopeFromQuery(req) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
  if (!legalEntityId) {
    return null;
  }
  return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
}

function resolveLegalEntityScopeFromBody(req) {
  const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
  if (!legalEntityId) {
    return null;
  }
  return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
}

router.get(
  "/",
  requirePermission("contract.read", {
    resolveScope: async (req) => resolveLegalEntityScopeFromQuery(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseContractListFilters(req);
    const result = await listContracts({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });

    return res.json({
      tenantId: filters.tenantId,
      ...result,
    });
  })
);

router.get(
  "/:contractId",
  requirePermission("contract.read", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const filters = parseContractListFilters(req);
    const contractId = parseContractIdParam(req);
    const row = await getContractByIdForTenant({
      req,
      tenantId: filters.tenantId,
      contractId,
      assertScopeAccess,
    });

    return res.json({
      tenantId: filters.tenantId,
      row,
    });
  })
);

router.post(
  "/",
  requirePermission("contract.upsert", {
    resolveScope: async (req) => resolveLegalEntityScopeFromBody(req),
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractCreateInput(req);
    const row = await createContract({
      req,
      payload,
      assertScopeAccess,
    });

    return res.status(201).json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.put(
  "/:contractId",
  requirePermission("contract.upsert", {
    resolveScope: async (req, tenantId) => {
      const scope = await resolveContractScope(req.params?.contractId, tenantId);
      if (scope) {
        return scope;
      }
      return resolveLegalEntityScopeFromBody(req);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractUpdateInput(req);
    const row = await updateContractById({
      req,
      payload,
      assertScopeAccess,
    });

    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.post(
  "/:contractId/activate",
  requirePermission("contract.activate", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractLifecycleInput(req);
    const row = await activateContractById({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.post(
  "/:contractId/suspend",
  requirePermission("contract.suspend", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractLifecycleInput(req);
    const row = await suspendContractById({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.post(
  "/:contractId/close",
  requirePermission("contract.close", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractLifecycleInput(req);
    const row = await closeContractById({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.post(
  "/:contractId/cancel",
  requirePermission("contract.cancel", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractLifecycleInput(req);
    const row = await cancelContractById({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.post(
  "/:contractId/link-document",
  requirePermission("contract.link_document", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractLinkDocumentInput(req);
    const row = await linkDocumentToContract({
      req,
      payload,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.post(
  "/:contractId/generate-billing",
  requirePermission("contract.link_document", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractGenerateBillingInput(req);
    const result = await generateContractBilling({
      req,
      payload,
      assertScopeAccess,
    });
    return res.status(result.idempotentReplay ? 200 : 201).json({
      tenantId: payload.tenantId,
      ...result,
    });
  })
);

router.post(
  "/:contractId/generate-revrec",
  requirePermission("revenue.schedule.generate", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractGenerateRevrecInput(req);
    const result = await generateContractRevrec({
      req,
      payload,
      assertScopeAccess,
    });
    return res.status(result.idempotentReplay ? 200 : 201).json({
      tenantId: payload.tenantId,
      ...result,
    });
  })
);

router.post(
  "/:contractId/amend",
  requirePermission("contract.upsert", {
    resolveScope: async (req, tenantId) => {
      const scope = await resolveContractScope(req.params?.contractId, tenantId);
      if (scope) {
        return scope;
      }
      return resolveLegalEntityScopeFromBody(req);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractAmendInput(req);
    const row = await amendContractById({
      req,
      payload,
      assertScopeAccess,
    });

    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.patch(
  "/:contractId/lines/:lineId",
  requirePermission("contract.upsert", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractLinePatchInput(req);
    const result = await patchContractLineById({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      row: result.row,
      line: result.line,
    });
  })
);

router.post(
  "/:contractId/documents/:linkId/adjust",
  requirePermission("contract.link_document", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractLinkAdjustmentInput(req);
    const row = await adjustContractDocumentLink({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.post(
  "/:contractId/documents/:linkId/unlink",
  requirePermission("contract.link_document", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractLinkUnlinkInput(req);
    const row = await unlinkContractDocumentLink({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.get(
  "/:contractId/documents",
  requirePermission("contract.read", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const filters = parseContractListFilters(req);
    const contractId = parseContractIdParam(req);
    const rows = await listContractDocumentLinks({
      req,
      tenantId: filters.tenantId,
      contractId,
      assertScopeAccess,
    });
    return res.json({
      tenantId: filters.tenantId,
      contractId,
      rows,
    });
  })
);

router.get(
  "/:contractId/linkable-documents",
  requirePermission("contract.link_document", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractLinkableDocumentsInput(req);
    const rows = await listContractLinkableDocuments({
      req,
      tenantId: payload.tenantId,
      contractId: payload.contractId,
      q: payload.q,
      limit: payload.limit,
      offset: payload.offset,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      contractId: payload.contractId,
      rows,
      limit: payload.limit,
      offset: payload.offset,
    });
  })
);

router.get(
  "/:contractId/amendments",
  requirePermission("contract.read", {
    resolveScope: async (req, tenantId) => {
      return resolveContractScope(req.params?.contractId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseContractAmendmentsListInput(req);
    const rows = await listContractAmendments({
      req,
      tenantId: payload.tenantId,
      contractId: payload.contractId,
      assertScopeAccess,
    });

    return res.json({
      tenantId: payload.tenantId,
      contractId: payload.contractId,
      rows,
    });
  })
);

export default router;
