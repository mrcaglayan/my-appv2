import express from "express";
import { assertScopeAccess, requirePermission } from "../middleware/rbac.js";
import { asyncHandler } from "./_utils.js";
import { resolvePaymentBatchScope } from "../services/payments.service.js";
import {
  exportPaymentBatchFile,
  importPaymentBatchAck,
  listBatchAckImports,
  listBatchExports,
} from "../services/bank.paymentFiles.service.js";
import {
  parseBankPaymentBatchAckImportCreateInput,
  parseBankPaymentBatchAckImportListInput,
  parseBankPaymentBatchExportCreateInput,
  parseBankPaymentBatchExportsListInput,
} from "./bank.paymentFiles.validators.js";

const router = express.Router();

function resolveBatchScope(req, tenantId) {
  return resolvePaymentBatchScope(req.params?.id ?? req.params?.batchId, tenantId);
}

router.get(
  "/payment-batches/:id/exports",
  requirePermission("bank.payments.export.read", {
    resolveScope: resolveBatchScope,
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankPaymentBatchExportsListInput(req);
    const result = await listBatchExports({
      req,
      tenantId: input.tenantId,
      batchId: input.batchId,
      assertScopeAccess,
    });

    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.post(
  "/payment-batches/:id/export-file",
  requirePermission("bank.payments.export.create", {
    resolveScope: resolveBatchScope,
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankPaymentBatchExportCreateInput(req);
    const result = await exportPaymentBatchFile({
      req,
      tenantId: input.tenantId,
      batchId: input.batchId,
      userId: input.userId,
      input,
      assertScopeAccess,
    });

    return res.json({
      tenantId: input.tenantId,
      row: result.row,
      export: result.export || null,
      approval_required: Boolean(result.approval_required),
      approval_request: result.approval_request || null,
      idempotent: Boolean(result.idempotent),
    });
  })
);

router.get(
  "/payment-batches/:id/ack-imports",
  requirePermission("bank.payments.ack.read", {
    resolveScope: resolveBatchScope,
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankPaymentBatchAckImportListInput(req);
    const result = await listBatchAckImports({
      req,
      tenantId: input.tenantId,
      batchId: input.batchId,
      assertScopeAccess,
    });

    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.post(
  "/payment-batches/:id/import-ack",
  requirePermission("bank.payments.ack.import", {
    resolveScope: resolveBatchScope,
  }),
  asyncHandler(async (req, res) => {
    const input = parseBankPaymentBatchAckImportCreateInput(req);
    const result = await importPaymentBatchAck({
      req,
      tenantId: input.tenantId,
      batchId: input.batchId,
      userId: input.userId,
      input,
      assertScopeAccess,
    });

    return res.json({
      tenantId: input.tenantId,
      row: result.row,
      ack_import: result.ack_import,
      idempotent: Boolean(result.idempotent),
    });
  })
);

export default router;
