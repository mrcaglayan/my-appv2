import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { resolveBankAccountScope } from "../services/bank.accounts.service.js";
import { requireTenantId } from "./cash.validators.common.js";
import {
  approvePaymentBatch,
  cancelPaymentBatch,
  createPaymentBatch,
  exportPaymentBatch,
  getPaymentBatchDetailByIdForTenant,
  listPaymentBatchRows,
  postPaymentBatch,
  resolvePaymentBatchScope,
} from "../services/payments.service.js";
import {
  parsePaymentBatchApproveInput,
  parsePaymentBatchCancelInput,
  parsePaymentBatchCreateInput,
  parsePaymentBatchExportInput,
  parsePaymentBatchIdParam,
  parsePaymentBatchListFilters,
  parsePaymentBatchPostInput,
} from "./payments.validators.js";

const router = express.Router();

async function resolvePaymentBatchListScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId ?? req.query?.legal_entity_id);
  if (legalEntityId) {
    return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  }

  const bankAccountId = parsePositiveInt(req.query?.bankAccountId ?? req.query?.bank_account_id);
  if (bankAccountId) {
    return resolveBankAccountScope(bankAccountId, tenantId);
  }

  return null;
}

router.get(
  "/batches",
  requirePermission("payments.batch.read", {
    resolveScope: resolvePaymentBatchListScope,
  }),
  asyncHandler(async (req, res) => {
    const filters = parsePaymentBatchListFilters(req);
    const result = await listPaymentBatchRows({
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
  "/batches/:batchId",
  requirePermission("payments.batch.read", {
    resolveScope: async (req, tenantId) => {
      return resolvePaymentBatchScope(req.params?.batchId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const batchId = parsePaymentBatchIdParam(req);
    const row = await getPaymentBatchDetailByIdForTenant({
      req,
      tenantId,
      batchId,
      assertScopeAccess,
    });

    return res.json({
      tenantId,
      row,
    });
  })
);

router.post(
  "/batches",
  requirePermission("payments.batch.create", {
    resolveScope: async (req, tenantId) => {
      return resolveBankAccountScope(req.body?.bankAccountId ?? req.body?.bank_account_id, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePaymentBatchCreateInput(req);
    const row = await createPaymentBatch({
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
  "/batches/:batchId/approve",
  requirePermission("payments.batch.approve", {
    resolveScope: async (req, tenantId) => {
      return resolvePaymentBatchScope(req.params?.batchId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePaymentBatchApproveInput(req);
    const row = await approvePaymentBatch({
      req,
      tenantId: payload.tenantId,
      batchId: payload.batchId,
      userId: payload.userId,
      approveInput: payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.post(
  "/batches/:batchId/export",
  requirePermission("payments.batch.export", {
    resolveScope: async (req, tenantId) => {
      return resolvePaymentBatchScope(req.params?.batchId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePaymentBatchExportInput(req);
    const result = await exportPaymentBatch({
      req,
      tenantId: payload.tenantId,
      batchId: payload.batchId,
      userId: payload.userId,
      exportInput: payload,
      assertScopeAccess,
    });

    return res.json({
      tenantId: payload.tenantId,
      row: result.row,
      export: result.export,
    });
  })
);

router.post(
  "/batches/:batchId/post",
  requirePermission("payments.batch.post", {
    resolveScope: async (req, tenantId) => {
      return resolvePaymentBatchScope(req.params?.batchId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePaymentBatchPostInput(req);
    const row = await postPaymentBatch({
      req,
      tenantId: payload.tenantId,
      batchId: payload.batchId,
      userId: payload.userId,
      postInput: payload,
      assertScopeAccess,
    });

    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

router.post(
  "/batches/:batchId/cancel",
  requirePermission("payments.batch.cancel", {
    resolveScope: async (req, tenantId) => {
      return resolvePaymentBatchScope(req.params?.batchId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parsePaymentBatchCancelInput(req);
    const row = await cancelPaymentBatch({
      req,
      tenantId: payload.tenantId,
      batchId: payload.batchId,
      userId: payload.userId,
      cancelInput: payload,
      assertScopeAccess,
    });

    return res.json({
      tenantId: payload.tenantId,
      row,
    });
  })
);

export default router;
