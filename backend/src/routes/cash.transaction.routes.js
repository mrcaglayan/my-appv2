import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { resolveCashRegisterScope } from "../services/cash.register.service.js";
import {
  cancelCashTransitTransferById,
  createCashTransaction,
  getCashTransitTransferByIdForTenant,
  applyCariFromCashTransactionById,
  getCashTransactionByIdForTenant,
  initiateCashTransitTransfer,
  listCashTransitTransferRows,
  listCashTransactionRows,
  postCashTransactionById,
  receiveCashTransitTransferById,
  resolveCashTransitTransferScope,
  resolveCashTransactionScope,
  reverseCashTransactionById,
  cancelCashTransactionById,
} from "../services/cash.transaction.service.js";
import {
  parseCashTransactionCancelInput,
  parseCashTransactionApplyCariInput,
  parseCashTransitTransferCancelInput,
  parseCashTransitTransferReadFilters,
  parseCashTransitTransferIdParam,
  parseCashTransitTransferInitiateInput,
  parseCashTransitTransferReceiveInput,
  parseCashTransactionCreateInput,
  parseCashTransactionIdParam,
  parseCashTransactionPostInput,
  parseCashTransactionReadFilters,
  parseCashTransactionReverseInput,
} from "./cash.transaction.validators.js";
import { requireTenantId } from "./cash.validators.common.js";

const router = express.Router();

const requireCashOverridePostPermission = requirePermission("cash.override.post", {
  resolveScope: async (req, tenantId) => {
    return resolveCashTransactionScope(req.params?.transactionId, tenantId);
  },
});

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

function buildCashApplyCariResponse(tenantId, result) {
  const metrics = {
    ...(result.applyAuditPayload || {}),
    ...(result.metrics || {}),
  };
  const unappliedConsumed = Array.isArray(metrics.unappliedConsumed)
    ? metrics.unappliedConsumed
    : [];

  return {
    tenantId,
    cashTransaction: result.cashTransaction || null,
    row: result.row || null,
    allocations: Array.isArray(result.allocations) ? result.allocations : [],
    journal: result.journal || null,
    fx: {
      settlementFxRate:
        metrics.settlementFxRate === undefined || metrics.settlementFxRate === null
          ? null
          : Number(metrics.settlementFxRate),
      settlementFxSource: metrics.settlementFxSource || null,
      fxRateDate: metrics.fxRateDate || null,
      realizedGainLossBase:
        metrics.realizedFxNetBase === undefined || metrics.realizedFxNetBase === null
          ? null
          : Number(metrics.realizedFxNetBase),
    },
    unapplied: {
      createdUnappliedCashId: parsePositiveInt(metrics.createdUnappliedCashId) || null,
      consumed: unappliedConsumed.map((entry) => ({
        unappliedCashId: parsePositiveInt(entry?.unappliedCashId) || null,
        consumeTxn:
          entry?.consumeTxn === undefined || entry?.consumeTxn === null
            ? null
            : Number(entry.consumeTxn),
        consumeBase:
          entry?.consumeBase === undefined || entry?.consumeBase === null
            ? null
            : Number(entry.consumeBase),
      })),
      rows: Array.isArray(result.unappliedCash) ? result.unappliedCash : [],
    },
    unappliedCash: Array.isArray(result.unappliedCash) ? result.unappliedCash : [],
    metrics: result.metrics || null,
    idempotentReplay: Boolean(result.idempotentReplay),
    followUpRisks: Array.isArray(result.followUpRisks) ? result.followUpRisks : [],
  };
}

function buildCashTransitTransferResponse(tenantId, result) {
  return {
    tenantId,
    transfer: result.transfer || null,
    transferOutTransaction: result.transferOutTransaction || null,
    transferInTransaction: result.transferInTransaction || null,
    idempotentReplay: Boolean(result.idempotentReplay),
  };
}

router.get(
  "/",
  requirePermission("cash.txn.read", {
    resolveScope: async (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }

      const registerId = parsePositiveInt(req.query?.registerId);
      if (registerId) {
        return resolveCashRegisterScope(registerId, tenantId);
      }
      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const filters = parseCashTransactionReadFilters(req);
    const result = await listCashTransactionRows({
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
  "/transit",
  requirePermission("cash.txn.read", {
    resolveScope: async (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }

      const sourceRegisterId = parsePositiveInt(req.query?.sourceRegisterId);
      if (sourceRegisterId) {
        return resolveCashRegisterScope(sourceRegisterId, tenantId);
      }

      const targetRegisterId = parsePositiveInt(req.query?.targetRegisterId);
      if (targetRegisterId) {
        return resolveCashRegisterScope(targetRegisterId, tenantId);
      }

      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const filters = parseCashTransitTransferReadFilters(req);
    const result = await listCashTransitTransferRows({
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
  "/transit/:transitTransferId",
  requirePermission("cash.txn.read", {
    resolveScope: async (req, tenantId) => {
      return resolveCashTransitTransferScope(req.params?.transitTransferId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const transitTransferId = parseCashTransitTransferIdParam(req);
    const result = await getCashTransitTransferByIdForTenant({
      req,
      tenantId,
      transitTransferId,
      assertScopeAccess,
    });
    return res.json(buildCashTransitTransferResponse(tenantId, result));
  })
);

router.post(
  "/transit/initiate",
  requirePermission("cash.txn.create", {
    resolveScope: async (req, tenantId) => {
      return resolveCashRegisterScope(req.body?.registerId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseCashTransitTransferInitiateInput(req);
    const result = await initiateCashTransitTransfer({
      req,
      payload,
      assertScopeAccess,
    });
    return res
      .status(result.idempotentReplay ? 200 : 201)
      .json(buildCashTransitTransferResponse(payload.tenantId, result));
  })
);

router.post(
  "/transit/:transitTransferId/receive",
  requirePermission("cash.txn.create", {
    resolveScope: async (req, tenantId) => {
      return resolveCashTransitTransferScope(req.params?.transitTransferId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseCashTransitTransferReceiveInput(req);
    const result = await receiveCashTransitTransferById({
      req,
      payload,
      assertScopeAccess,
    });
    return res
      .status(result.idempotentReplay ? 200 : 201)
      .json(buildCashTransitTransferResponse(payload.tenantId, result));
  })
);

router.post(
  "/transit/:transitTransferId/cancel",
  requirePermission("cash.txn.cancel", {
    resolveScope: async (req, tenantId) => {
      return resolveCashTransitTransferScope(req.params?.transitTransferId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseCashTransitTransferCancelInput(req);
    const result = await cancelCashTransitTransferById({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json(buildCashTransitTransferResponse(payload.tenantId, result));
  })
);

router.get(
  "/:transactionId",
  requirePermission("cash.txn.read", {
    resolveScope: async (req, tenantId) => {
      return resolveCashTransactionScope(req.params?.transactionId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const transactionId = parseCashTransactionIdParam(req);
    const row = await getCashTransactionByIdForTenant({
      req,
      tenantId,
      transactionId,
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
  requirePermission("cash.txn.create", {
    resolveScope: async (req, tenantId) => {
      return resolveCashRegisterScope(req.body?.registerId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseCashTransactionCreateInput(req);
    const result = await createCashTransaction({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      row: result.row,
      idempotentReplay: result.idempotentReplay,
    });
  })
);

router.post(
  "/:transactionId/cancel",
  requirePermission("cash.txn.cancel", {
    resolveScope: async (req, tenantId) => {
      return resolveCashTransactionScope(req.params?.transactionId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseCashTransactionCancelInput(req);
    const row = await cancelCashTransactionById({
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
  "/:transactionId/post",
  requirePermission("cash.txn.post", {
    resolveScope: async (req, tenantId) => {
      return resolveCashTransactionScope(req.params?.transactionId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseCashTransactionPostInput(req);

    if (payload.overrideCashControl) {
      await runPermissionMiddleware(requireCashOverridePostPermission, req, res);
    }

    const result = await postCashTransactionById({
      req,
      payload,
      assertScopeAccess,
    });

    return res.json({
      tenantId: payload.tenantId,
      row: result.row,
      idempotentReplay: result.idempotentReplay,
    });
  })
);

router.post(
  "/:transactionId/reverse",
  requirePermission("cash.txn.reverse", {
    resolveScope: async (req, tenantId) => {
      return resolveCashTransactionScope(req.params?.transactionId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseCashTransactionReverseInput(req);
    const result = await reverseCashTransactionById({
      req,
      payload,
      assertScopeAccess,
    });
    return res.json({
      tenantId: payload.tenantId,
      original: result.original,
      reversal: result.reversal,
      idempotentReplay: result.idempotentReplay,
    });
  })
);

router.post(
  "/:transactionId/apply-cari",
  requirePermission("cari.settlement.apply", {
    resolveScope: async (req, tenantId) => {
      return resolveCashTransactionScope(req.params?.transactionId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseCashTransactionApplyCariInput(req);
    const result = await applyCariFromCashTransactionById({
      req,
      payload,
      assertScopeAccess,
    });
    return res
      .status(result.idempotentReplay ? 200 : 201)
      .json(buildCashApplyCariResponse(payload.tenantId, result));
  })
);

export default router;
