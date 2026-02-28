import express from "express";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { requireTenantId } from "./cash.validators.common.js";
import {
  assertScopeAccess,
  buildScopeFilter,
  requirePermission,
} from "../middleware/rbac.js";
import {
  parseDocumentCreateInput,
  parseDocumentPostInput,
  parseDocumentReverseInput,
  parseDocumentIdParam,
  parseDocumentReadFilters,
  parseDocumentUpdateInput,
  parseDraftCancelInput,
} from "./cari.document.validators.js";
import {
  cancelCariDraftDocumentById,
  createCariDraftDocument,
  getCariDocumentByIdForTenant,
  listCariDocuments,
  postCariDocumentById,
  reverseCariPostedDocumentById,
  resolveCariDocumentScope,
  updateCariDraftDocumentById,
} from "../services/cari.document.service.js";

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

async function runPermissionMiddleware(middleware, req, res) {
  await new Promise((resolve, reject) => {
    middleware(req, res, (err) => {
      if (err) {
        reject(err);
        return;
      }
      resolve();
    });
  });
}

const requireCariFxOverridePermission = requirePermission("cari.fx.override", {
  resolveScope: async (req, tenantId) => {
    return resolveCariDocumentScope(req.params?.documentId, tenantId);
  },
});

router.get(
  "/",
  requirePermission("cari.doc.read", {
    resolveScope: async (req) => resolveLegalEntityScopeFromQuery(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseDocumentReadFilters(req);
    const result = await listCariDocuments({
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
  "/:documentId",
  requirePermission("cari.doc.read", {
    resolveScope: async (req, tenantId) => {
      return resolveCariDocumentScope(req.params?.documentId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const documentId = parseDocumentIdParam(req);
    const row = await getCariDocumentByIdForTenant({
      req,
      tenantId,
      documentId,
      assertScopeAccess,
    });
    return res.json({
      tenantId,
      row,
    });
  })
);

router.post(
  "/",
  requirePermission("cari.doc.create", {
    resolveScope: async (req) => resolveLegalEntityScopeFromBody(req),
  }),
  asyncHandler(async (req, res) => {
    const payload = parseDocumentCreateInput(req);
    const row = await createCariDraftDocument({
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
  "/:documentId",
  requirePermission("cari.doc.update", {
    resolveScope: async (req, tenantId) => {
      const scope = await resolveCariDocumentScope(req.params?.documentId, tenantId);
      if (scope) {
        return scope;
      }
      return resolveLegalEntityScopeFromBody(req);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseDocumentUpdateInput(req);
    const row = await updateCariDraftDocumentById({
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
  "/:documentId/cancel",
  requirePermission("cari.doc.update", {
    resolveScope: async (req, tenantId) => {
      return resolveCariDocumentScope(req.params?.documentId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseDraftCancelInput(req);
    const row = await cancelCariDraftDocumentById({
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
  "/:documentId/post",
  requirePermission("cari.doc.post", {
    resolveScope: async (req, tenantId) => {
      return resolveCariDocumentScope(req.params?.documentId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseDocumentPostInput(req);
    if (payload.useFxOverride) {
      await runPermissionMiddleware(requireCariFxOverridePermission, req, res);
    }
    const result = await postCariDocumentById({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      row: result.row,
      journal: result.journal,
    });
  })
);

router.post(
  "/:documentId/reverse",
  requirePermission("cari.doc.reverse", {
    resolveScope: async (req, tenantId) => {
      return resolveCariDocumentScope(req.params?.documentId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseDocumentReverseInput(req);
    const result = await reverseCariPostedDocumentById({
      req,
      payload,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: payload.tenantId,
      row: result.row,
      original: result.original,
      journal: result.journal,
    });
  })
);

export default router;
