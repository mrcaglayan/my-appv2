import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, badRequest, parsePositiveInt, resolveTenantId } from "./_utils.js";
import { resolveOffsetPagination } from "../utils/pagination.js";
import {
  bulkActionExceptionWorkbench,
  claimExceptionWorkbench,
  getExceptionWorkbenchById,
  ignoreExceptionWorkbench,
  listExceptionWorkbenchRows,
  refreshExceptionWorkbench,
  reopenExceptionWorkbench,
  resolveExceptionWorkbench,
  resolveExceptionWorkbenchScope,
} from "../services/exceptions.workbench.service.js";

const router = express.Router();

function requireTenantIdFromReq(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) throw badRequest("tenantId is required");
  return tenantId;
}

function parsePositiveIntMaybe(value, label) {
  if (value === undefined || value === null || value === "") return null;
  const parsed = parsePositiveInt(value);
  if (!parsed) throw badRequest(`${label} must be a positive integer`);
  return parsed;
}

function parseBool(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  const raw = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(raw)) return true;
  if (["0", "false", "no", "n", "off"].includes(raw)) return false;
  throw badRequest("Invalid boolean flag");
}

function parseDateMaybe(value, label) {
  if (value === undefined || value === null || value === "") return null;
  const raw = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) throw badRequest(`${label} must be YYYY-MM-DD`);
  return raw;
}

function parseListFilters(req) {
  const tenantId = requireTenantIdFromReq(req);
  const pagination = resolveOffsetPagination(req.query, {
    defaultLimit: 100,
    defaultOffset: 0,
    maxLimit: 500,
    strict: true,
  });
  return {
    tenantId,
    limit: pagination.limit,
    offset: pagination.offset,
    refresh: parseBool(req.query?.refresh, true),
    dateFrom: parseDateMaybe(req.query?.dateFrom ?? req.query?.date_from, "dateFrom"),
    dateTo: parseDateMaybe(req.query?.dateTo ?? req.query?.date_to, "dateTo"),
    days: parsePositiveIntMaybe(req.query?.days, "days"),
    legalEntityId: parsePositiveIntMaybe(req.query?.legalEntityId ?? req.query?.legal_entity_id, "legalEntityId"),
    moduleCode: req.query?.moduleCode ?? req.query?.module_code ?? null,
    status: req.query?.status ?? null,
    severity: req.query?.severity ?? null,
    ownerUserId: parsePositiveIntMaybe(req.query?.ownerUserId ?? req.query?.owner_user_id, "ownerUserId"),
    exceptionType: req.query?.exceptionType ?? req.query?.exception_type ?? null,
    sourceType: req.query?.sourceType ?? req.query?.source_type ?? null,
    q: req.query?.q ?? null,
    sortBy: req.query?.sortBy ?? req.query?.sort_by ?? null,
  };
}

function parseRefreshInput(req) {
  const tenantId = requireTenantIdFromReq(req);
  return {
    tenantId,
    dateFrom: parseDateMaybe(req.body?.dateFrom ?? req.body?.date_from, "dateFrom"),
    dateTo: parseDateMaybe(req.body?.dateTo ?? req.body?.date_to, "dateTo"),
    days: parsePositiveIntMaybe(req.body?.days, "days"),
    legalEntityId: parsePositiveIntMaybe(req.body?.legalEntityId ?? req.body?.legal_entity_id, "legalEntityId"),
  };
}

function parseExceptionId(req) {
  const id = parsePositiveInt(req.params?.exceptionId ?? req.params?.id);
  if (!id) throw badRequest("exceptionId must be a positive integer");
  return id;
}

function parseClaimInput(req) {
  const tenantId = requireTenantIdFromReq(req);
  const userId = parsePositiveInt(req.user?.id) || parsePositiveInt(req.user?.userId) || null;
  if (!userId) throw badRequest("Authenticated user id is required");
  return {
    tenantId,
    exceptionId: parseExceptionId(req),
    userId,
    ownerUserId: parsePositiveIntMaybe(req.body?.ownerUserId ?? req.body?.owner_user_id, "ownerUserId"),
    note: req.body?.note ? String(req.body.note).slice(0, 500) : null,
  };
}

function parseStatusActionInput(req) {
  const tenantId = requireTenantIdFromReq(req);
  const userId = parsePositiveInt(req.user?.id) || parsePositiveInt(req.user?.userId) || null;
  if (!userId) throw badRequest("Authenticated user id is required");
  return {
    tenantId,
    exceptionId: parseExceptionId(req),
    userId,
    resolutionAction: req.body?.resolutionAction ?? req.body?.resolution_action ?? null,
    resolutionNote: req.body?.resolutionNote ?? req.body?.resolution_note ?? null,
  };
}

function parseExceptionIdsArray(value) {
  const items = Array.isArray(value) ? value : [];
  if (items.length === 0) {
    throw badRequest("exceptionIds must be a non-empty array");
  }

  const parsed = [];
  for (const item of items) {
    const id = parsePositiveInt(item);
    if (!id) {
      throw badRequest("exceptionIds must contain only positive integers");
    }
    if (!parsed.includes(id)) {
      parsed.push(id);
    }
  }

  if (parsed.length > 200) {
    throw badRequest("exceptionIds supports at most 200 ids per request");
  }

  return parsed;
}

function parseBulkActionInput(req) {
  const tenantId = requireTenantIdFromReq(req);
  const userId = parsePositiveInt(req.user?.id) || parsePositiveInt(req.user?.userId) || null;
  if (!userId) throw badRequest("Authenticated user id is required");

  const actionRaw = String(req.body?.action || "").trim().toUpperCase();
  if (!["CLAIM", "RESOLVE", "IGNORE", "REOPEN"].includes(actionRaw)) {
    throw badRequest("action must be one of: claim, resolve, ignore, reopen");
  }
  const noteRaw = req.body?.note ?? null;
  const resolutionActionRaw = req.body?.resolutionAction ?? req.body?.resolution_action ?? null;
  const resolutionNoteRaw = req.body?.resolutionNote ?? req.body?.resolution_note ?? null;
  const noteText = noteRaw === null ? "" : String(noteRaw).trim();
  const resolutionActionText = resolutionActionRaw === null ? "" : String(resolutionActionRaw).trim();
  const resolutionNoteText = resolutionNoteRaw === null ? "" : String(resolutionNoteRaw).trim();

  return {
    tenantId,
    userId,
    action: actionRaw,
    exceptionIds: parseExceptionIdsArray(req.body?.exceptionIds ?? req.body?.exception_ids),
    ownerUserId: parsePositiveIntMaybe(req.body?.ownerUserId ?? req.body?.owner_user_id, "ownerUserId"),
    note: noteText ? noteText.slice(0, 500) : null,
    resolutionAction: resolutionActionText ? resolutionActionText.slice(0, 80) : null,
    resolutionNote: resolutionNoteText ? resolutionNoteText.slice(0, 500) : null,
    continueOnError: parseBool(req.body?.continueOnError ?? req.body?.continue_on_error, true),
  };
}

function resolveListScope(req) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId ?? req.query?.legal_entity_id);
  if (!legalEntityId) return null;
  return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
}

router.get(
  "/workbench",
  requirePermission("ops.exceptions.read", { resolveScope: resolveListScope }),
  asyncHandler(async (req, res) => {
    const filters = parseListFilters(req);
    const result = await listExceptionWorkbenchRows({
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

router.post(
  "/workbench/refresh",
  requirePermission("ops.exceptions.read", {
    resolveScope: (req) => {
      const legalEntityId = parsePositiveInt(req.body?.legalEntityId ?? req.body?.legal_entity_id);
      if (!legalEntityId) return null;
      return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
    },
  }),
  asyncHandler(async (req, res) => {
    const input = parseRefreshInput(req);
    const result = await refreshExceptionWorkbench({
      req,
      tenantId: input.tenantId,
      filters: input,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({
      tenantId: input.tenantId,
      ...result,
    });
  })
);

router.post(
  "/workbench/bulk-action",
  requirePermission("ops.exceptions.manage"),
  asyncHandler(async (req, res) => {
    const input = parseBulkActionInput(req);
    const result = await bulkActionExceptionWorkbench({
      req,
      tenantId: input.tenantId,
      action: input.action,
      exceptionIds: input.exceptionIds,
      actorUserId: input.userId,
      ownerUserId: input.ownerUserId,
      note: input.note,
      resolutionAction: input.resolutionAction,
      resolutionNote: input.resolutionNote,
      continueOnError: input.continueOnError,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.get(
  "/workbench/:exceptionId",
  requirePermission("ops.exceptions.read", {
    resolveScope: (req, tenantId) => resolveExceptionWorkbenchScope(req.params?.exceptionId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantIdFromReq(req);
    const exceptionId = parseExceptionId(req);
    const result = await getExceptionWorkbenchById({
      req,
      tenantId,
      exceptionId,
      assertScopeAccess,
    });
    return res.json({ tenantId, ...result });
  })
);

router.post(
  "/workbench/:exceptionId/claim",
  requirePermission("ops.exceptions.manage", {
    resolveScope: (req, tenantId) => resolveExceptionWorkbenchScope(req.params?.exceptionId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseClaimInput(req);
    const row = await claimExceptionWorkbench({
      req,
      tenantId: input.tenantId,
      exceptionId: input.exceptionId,
      actorUserId: input.userId,
      ownerUserId: input.ownerUserId,
      note: input.note,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, row });
  })
);

router.post(
  "/workbench/:exceptionId/resolve",
  requirePermission("ops.exceptions.manage", {
    resolveScope: (req, tenantId) => resolveExceptionWorkbenchScope(req.params?.exceptionId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseStatusActionInput(req);
    const row = await resolveExceptionWorkbench({
      req,
      tenantId: input.tenantId,
      exceptionId: input.exceptionId,
      actorUserId: input.userId,
      resolutionAction: input.resolutionAction || "MANUAL_RESOLVE",
      resolutionNote: input.resolutionNote || null,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, row });
  })
);

router.post(
  "/workbench/:exceptionId/ignore",
  requirePermission("ops.exceptions.manage", {
    resolveScope: (req, tenantId) => resolveExceptionWorkbenchScope(req.params?.exceptionId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseStatusActionInput(req);
    const row = await ignoreExceptionWorkbench({
      req,
      tenantId: input.tenantId,
      exceptionId: input.exceptionId,
      actorUserId: input.userId,
      resolutionAction: input.resolutionAction || "MANUAL_IGNORE",
      resolutionNote: input.resolutionNote || null,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, row });
  })
);

router.post(
  "/workbench/:exceptionId/reopen",
  requirePermission("ops.exceptions.manage", {
    resolveScope: (req, tenantId) => resolveExceptionWorkbenchScope(req.params?.exceptionId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseStatusActionInput(req);
    const row = await reopenExceptionWorkbench({
      req,
      tenantId: input.tenantId,
      exceptionId: input.exceptionId,
      actorUserId: input.userId,
      resolutionNote: input.resolutionNote || null,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, row });
  })
);

export default router;
