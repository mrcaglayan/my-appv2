import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { resolveBankAccountScope } from "../services/bank.accounts.service.js";
import {
  createManualPaymentReturnEvent,
  getPaymentReturnEventById,
  ignorePaymentReturnEvent,
  listPaymentReturnEventRows,
  resolvePaymentBatchLineReturnScope,
  resolvePaymentReturnEventScope,
} from "../services/bank.paymentReturns.service.js";
import {
  parseManualPaymentReturnCreateInput,
  parsePaymentReturnEventIdParam,
  parsePaymentReturnEventsListFilters,
  parsePaymentReturnIgnoreInput,
} from "./bank.paymentReturns.validators.js";

const router = express.Router();

async function resolvePaymentReturnListScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
  if (legalEntityId) return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  const bankAccountId = parsePositiveInt(req.query?.bankAccountId);
  if (bankAccountId) return resolveBankAccountScope(bankAccountId, tenantId);
  return null;
}

router.get(
  "/payment-returns",
  requirePermission("bank.payments.returns.read", { resolveScope: resolvePaymentReturnListScope }),
  asyncHandler(async (req, res) => {
    const filters = parsePaymentReturnEventsListFilters(req);
    const result = await listPaymentReturnEventRows({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: filters.tenantId, ...result });
  })
);

router.post(
  "/payment-returns",
  requirePermission("bank.payments.returns.write", {
    resolveScope: (req, tenantId) =>
      resolvePaymentBatchLineReturnScope(
        req.body?.paymentBatchLineId ?? req.body?.payment_batch_line_id,
        tenantId
      ),
  }),
  asyncHandler(async (req, res) => {
    const input = parseManualPaymentReturnCreateInput(req);
    const result = await createManualPaymentReturnEvent({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      input,
      assertScopeAccess,
    });
    return res.status(201).json({
      tenantId: input.tenantId,
      row: result?.row || null,
      approval_required: Boolean(result?.approval_required),
      approval_request: result?.approval_request || null,
      idempotent: Boolean(result?.idempotent),
    });
  })
);

router.post(
  "/payment-returns/:eventId/ignore",
  requirePermission("bank.payments.returns.write", {
    resolveScope: (req, tenantId) => resolvePaymentReturnEventScope(req.params?.eventId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parsePaymentReturnIgnoreInput(req);
    const row = await ignorePaymentReturnEvent({
      req,
      tenantId: input.tenantId,
      eventId: input.eventId,
      reasonMessage: input.reasonMessage,
      userId: input.userId,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, row });
  })
);

router.get(
  "/payment-returns/:eventId",
  requirePermission("bank.payments.returns.read", {
    resolveScope: (req, tenantId) => resolvePaymentReturnEventScope(req.params?.eventId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const tenantId = parsePaymentReturnEventsListFilters(req).tenantId;
    const eventId = parsePaymentReturnEventIdParam(req);
    const row = await getPaymentReturnEventById({ req, tenantId, eventId, assertScopeAccess });
    return res.json({ tenantId, row });
  })
);

export default router;
