import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { requireTenantId } from "./cash.validators.common.js";
import { resolveBankAccountScope } from "../services/bank.accounts.service.js";
import {
  getBankStatementImportByIdForTenant,
  getBankStatementLineByIdForTenant,
  importBankStatementCsv,
  listBankStatementImportRows,
  listBankStatementLineRows,
  resolveBankStatementImportScope,
  resolveBankStatementLineScope,
} from "../services/bank.statements.service.js";
import {
  parseBankStatementImportCreateInput,
  parseBankStatementImportIdParam,
  parseBankStatementImportReadFilters,
  parseBankStatementLineIdParam,
  parseBankStatementLineReadFilters,
} from "./bank.statements.validators.js";

const router = express.Router();

async function resolveImportsListScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
  if (legalEntityId) {
    return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  }

  const bankAccountId = parsePositiveInt(req.query?.bankAccountId);
  if (bankAccountId) {
    return resolveBankAccountScope(bankAccountId, tenantId);
  }

  return null;
}

async function resolveLinesListScope(req, tenantId) {
  const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
  if (legalEntityId) {
    return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  }

  const importId = parsePositiveInt(req.query?.importId);
  if (importId) {
    return resolveBankStatementImportScope(importId, tenantId);
  }

  const bankAccountId = parsePositiveInt(req.query?.bankAccountId);
  if (bankAccountId) {
    return resolveBankAccountScope(bankAccountId, tenantId);
  }

  return null;
}

router.post(
  "/import",
  requirePermission("bank.statements.import", {
    resolveScope: async (req, tenantId) => {
      return resolveBankAccountScope(req.body?.bankAccountId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const payload = parseBankStatementImportCreateInput(req);
    const row = await importBankStatementCsv({
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

router.get(
  "/imports",
  requirePermission("bank.statements.read", {
    resolveScope: resolveImportsListScope,
  }),
  asyncHandler(async (req, res) => {
    const filters = parseBankStatementImportReadFilters(req);
    const result = await listBankStatementImportRows({
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
  "/imports/:importId",
  requirePermission("bank.statements.read", {
    resolveScope: async (req, tenantId) => {
      return resolveBankStatementImportScope(req.params?.importId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const importId = parseBankStatementImportIdParam(req);
    const row = await getBankStatementImportByIdForTenant({
      req,
      tenantId,
      importId,
      assertScopeAccess,
    });

    return res.json({
      tenantId,
      row,
    });
  })
);

router.get(
  "/lines",
  requirePermission("bank.statements.read", {
    resolveScope: resolveLinesListScope,
  }),
  asyncHandler(async (req, res) => {
    const filters = parseBankStatementLineReadFilters(req);
    const result = await listBankStatementLineRows({
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
  "/lines/:lineId",
  requirePermission("bank.statements.read", {
    resolveScope: async (req, tenantId) => {
      return resolveBankStatementLineScope(req.params?.lineId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantId(req);
    const lineId = parseBankStatementLineIdParam(req);
    const row = await getBankStatementLineByIdForTenant({
      req,
      tenantId,
      lineId,
      assertScopeAccess,
    });

    return res.json({
      tenantId,
      row,
    });
  })
);

export default router;
