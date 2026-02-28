import express from "express";
import {
  assertScopeAccess,
  buildScopeFilter,
  requirePermission,
} from "../middleware/rbac.js";
import counterpartyRoutes from "./cari.counterparty.routes.js";
import paymentTermRoutes from "./cari.payment-term.routes.js";
import documentRoutes from "./cari.document.routes.js";
import {
  parseAgingReportFilters,
  parseCariAuditFilters,
  parseCounterpartyStatementFilters,
  parseGenericAgingReportFilters,
  parseOpenItemsReportFilters,
} from "./cari.report.validators.js";
import {
  parseBankApplyInput,
  parseBankAttachInput,
  parseSettlementApplyInput,
  parseSettlementReverseInput,
} from "./cari.settlement.validators.js";
import {
  attachCariBankReference,
  applyCariSettlement,
  resolveCariSettlementScope,
  reverseCariSettlementById,
} from "../services/cari.settlement.service.js";
import { resolveCashRegisterScope } from "../services/cash.register.service.js";
import {
  getCariAgingReport,
  getCariCounterpartyStatementReport,
  getCariOpenItemsReport,
} from "../services/cari.report.service.js";
import { getCariAuditTrail } from "../services/cari.audit.service.js";
import {
  asyncHandler,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";

const router = express.Router();

router.use("/counterparties", counterpartyRoutes);
router.use("/payment-terms", paymentTermRoutes);
router.use("/documents", documentRoutes);

function requireTenant(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }
  return tenantId;
}

function resolveCariScope(req) {
  const legalEntityId =
    parsePositiveInt(req.body?.legalEntityId) ||
    parsePositiveInt(req.query?.legalEntityId);
  if (legalEntityId) {
    return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  }
  return null;
}

function ok(res, payload) {
  return res.json({
    ok: true,
    scaffolded: true,
    ...payload,
  });
}

const requireCashTxnCreatePermissionForCariApply = requirePermission("cash.txn.create", {
  resolveScope: async (req, tenantId) => {
    const registerId =
      parsePositiveInt(req.body?.linkedCashTransaction?.registerId) ||
      parsePositiveInt(req.body?.cashTransaction?.registerId);
    if (registerId) {
      return resolveCashRegisterScope(registerId, tenantId);
    }
    return resolveCariScope(req);
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

function buildSettlementApplyResponse(tenantId, result) {
  const metrics = {
    ...(result.applyAuditPayload || {}),
    ...(result.metrics || {}),
  };
  const realizedGainLossBase =
    metrics.realizedFxNetBase === undefined || metrics.realizedFxNetBase === null
      ? null
      : Number(metrics.realizedFxNetBase);
  const unappliedConsumed = Array.isArray(metrics.unappliedConsumed)
    ? metrics.unappliedConsumed
    : [];

  return {
    tenantId,
    row: result.row,
    cashTransaction: result.cashTransaction || null,
    allocations: Array.isArray(result.allocations) ? result.allocations : [],
    journal: result.journal || null,
    fx: {
      settlementFxRate:
        metrics.settlementFxRate === undefined || metrics.settlementFxRate === null
          ? null
          : Number(metrics.settlementFxRate),
      settlementFxSource: metrics.settlementFxSource || null,
      settlementFxFallbackMode: metrics.settlementFxFallbackMode || null,
      settlementFxFallbackMaxDays:
        metrics.settlementFxFallbackMaxDays === undefined ||
        metrics.settlementFxFallbackMaxDays === null
          ? null
          : Number(metrics.settlementFxFallbackMaxDays),
      fxRateDate: metrics.fxRateDate || null,
      realizedGainLossBase,
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

router.get(
  "/cards",
  requirePermission("cari.card.read", {
    resolveScope: (req) => resolveCariScope(req),
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenant(req);
    return ok(res, {
      tenantId,
      rows: [],
    });
  })
);

router.post(
  "/cards",
  requirePermission("cari.card.upsert", {
    resolveScope: (req) => resolveCariScope(req),
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenant(req);
    return ok(res, {
      tenantId,
      message: "Cari card upsert endpoint is guard-ready for PR-03+",
    });
  })
);

router.post(
  "/settlements/apply",
  requirePermission("cari.settlement.apply", {
    resolveScope: async (req) => resolveCariScope(req),
  }),
  asyncHandler(async (req, res) => {
    const payload = parseSettlementApplyInput(req);
    if (payload.paymentChannel === "CASH" && !payload.cashTransactionId) {
      await runPermissionMiddleware(requireCashTxnCreatePermissionForCariApply, req, res);
    }
    const result = await applyCariSettlement({
      req,
      payload,
      assertScopeAccess,
    });

    return res
      .status(result.idempotentReplay ? 200 : 201)
      .json(buildSettlementApplyResponse(payload.tenantId, result));
  })
);

router.post(
  "/settlements/:settlementBatchId/reverse",
  requirePermission("cari.settlement.reverse", {
    resolveScope: async (req, tenantId) => {
      return resolveCariSettlementScope(req.params?.settlementBatchId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseSettlementReverseInput(req);
    const result = await reverseCariSettlementById({
      req,
      payload,
      assertScopeAccess,
    });

    return res.status(201).json({
      tenantId: payload.tenantId,
      row: result.row,
      original: result.original,
      journal: result.journal,
      followUpRisks: result.followUpRisks || [],
    });
  })
);

router.get(
  "/reports/aging",
  requirePermission("cari.report.read", {
    resolveScope: (req) => resolveCariScope(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseGenericAgingReportFilters(req);
    const result = await getCariAgingReport({
      req,
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
  "/reports/ar-aging",
  requirePermission("cari.report.read", {
    resolveScope: (req) => resolveCariScope(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseAgingReportFilters(req, { fixedDirection: "AR" });
    const result = await getCariAgingReport({
      req,
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
  "/reports/ap-aging",
  requirePermission("cari.report.read", {
    resolveScope: (req) => resolveCariScope(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseAgingReportFilters(req, { fixedDirection: "AP" });
    const result = await getCariAgingReport({
      req,
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
  "/reports/open-items",
  requirePermission("cari.report.read", {
    resolveScope: (req) => resolveCariScope(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseOpenItemsReportFilters(req);
    const result = await getCariOpenItemsReport({
      req,
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
  "/reports/statement",
  requirePermission("cari.report.read", {
    resolveScope: (req) => resolveCariScope(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseCounterpartyStatementFilters(req);
    const result = await getCariCounterpartyStatementReport({
      req,
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
  "/fx/override",
  requirePermission("cari.fx.override", {
    resolveScope: (req) => resolveCariScope(req),
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenant(req);
    return ok(res, {
      tenantId,
      message: "Cari FX override endpoint is guard-ready for PR-03+",
    });
  })
);

router.get(
  "/audit",
  requirePermission("cari.audit.read", {
    resolveScope: (req) => resolveCariScope(req),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseCariAuditFilters(req);
    const result = await getCariAuditTrail({
      req,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json(result);
  })
);

router.post(
  "/bank/attach",
  requirePermission("cari.bank.attach", {
    resolveScope: (req) => resolveCariScope(req),
  }),
  asyncHandler(async (req, res) => {
    const payload = parseBankAttachInput(req);
    const result = await attachCariBankReference({
      req,
      payload,
      assertScopeAccess,
    });

    return res.status(result.idempotentReplay ? 200 : 201).json({
      tenantId: payload.tenantId,
      targetType: result.targetType,
      settlement: result.settlement,
      unappliedCash: result.unappliedCash,
      idempotentReplay: result.idempotentReplay,
    });
  })
);

router.post(
  "/bank/apply",
  requirePermission("cari.bank.apply", {
    resolveScope: (req) => resolveCariScope(req),
  }),
  asyncHandler(async (req, res) => {
    const payload = parseBankApplyInput(req);
    const result = await applyCariSettlement({
      req,
      payload,
      assertScopeAccess,
    });

    return res
      .status(result.idempotentReplay ? 200 : 201)
      .json(buildSettlementApplyResponse(payload.tenantId, result));
  })
);

export default router;
