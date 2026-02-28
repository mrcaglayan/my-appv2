import express from "express";
import { query, withTransaction } from "../db.js";
import {
  assertScopeAccess,
  buildScopeFilter,
  getScopeContext,
  invalidateRbacCache,
  requirePermission,
} from "../middleware/rbac.js";
import {
  asyncHandler,
  assertRequiredFields,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";
import {
  assertCurrencyExists,
  assertCountryExists,
  assertFiscalCalendarBelongsToTenant,
  assertGroupCompanyBelongsToTenant,
  assertLegalEntityBelongsToTenant,
} from "../tenantGuards.js";
import {
  listCountries,
  listCurrencies,
  listFiscalCalendarPeriods,
  listFiscalCalendars,
  listGroupCompanies,
  listLegalEntities,
  listOrgTree,
  listShareholderJournalConfigs,
  listShareholders,
  listOperatingUnits,
} from "../services/org.read.service.js";
import {
  assertShareholderParentAccount,
  buildShareholderCommitmentBatchPreviewTx,
  buildShareholderChildCode,
  createShareholderCommitmentDraftJournal,
  createBatchRowIssue,
  generateAutoJournalNo,
  isDescendantOfParentAccount,
  loadLegalEntityAccountHierarchy,
  normalizeAccountNormalSide,
  normalizeCurrencyCode,
  normalizeMoney,
  normalizeShareholderChildSequenceFromCode,
  parseBatchShareholderIds,
  resolveShareholderParentMappings,
  toCommitmentJournalFailureMessage,
  validateShareholderMappedLeafAccount,
} from "../services/org.shareholder.helpers.js";
import {
  autoProvisionShareholderSubAccounts,
  executeShareholderCommitmentJournalBatch,
  generateFiscalPeriods,
  previewShareholderCommitmentJournalBatch,
  upsertFiscalCalendar,
  upsertGroupCompany,
  upsertLegalEntity,
  upsertOperatingUnit,
  upsertShareholder,
  upsertShareholderJournalConfig,
} from "../services/org.write.service.js";
import { recalculateShareholderOwnershipPctTx } from "../services/shareholderOwnership.js";
import {
  parseFiscalCalendarPeriodFilters,
  parseLegalEntityReadFilters,
  parseOperatingUnitReadFilters,
  parseShareholderJournalConfigFilters,
  parseShareholderReadFilters,
  requireOrgTenantId,
} from "./org.read.validators.js";
import {
  parseFiscalCalendarUpsertInput,
  parseFiscalPeriodGenerateInput,
  parseGroupCompanyUpsertInput,
  parseShareholderAutoProvisionSubAccountsInput,
  parseShareholderCommitmentBatchExecuteInput,
  parseLegalEntityUpsertInput,
  parseOperatingUnitUpsertInput,
  parseShareholderCommitmentBatchPreviewInput,
  parseShareholderJournalConfigUpsertInput,
  parseShareholderUpsertInput,
} from "./org.write.validators.js";

const router = express.Router();
const SHAREHOLDER_CAPITAL_CREDIT_PARENT_PURPOSE =
  "SHAREHOLDER_CAPITAL_CREDIT_PARENT";
const SHAREHOLDER_COMMITMENT_DEBIT_PARENT_PURPOSE =
  "SHAREHOLDER_COMMITMENT_DEBIT_PARENT";
const DEFAULT_GL_ACCOUNTS = [
  {
    code: "1000",
    name: "Cash and Cash Equivalents",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  {
    code: "1100",
    name: "Accounts Receivable",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  {
    code: "2000",
    name: "Accounts Payable",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  },
  {
    code: "3000",
    name: "Retained Earnings",
    accountType: "EQUITY",
    normalSide: "CREDIT",
  },
  {
    code: "4000",
    name: "Revenue",
    accountType: "REVENUE",
    normalSide: "CREDIT",
  },
  {
    code: "5000",
    name: "Operating Expense",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
];

function toLocalYyyyMmDd(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(
    date.getDate()
  ).padStart(2, "0")}`;
}

function toIsoDate(value, fieldLabel = "date") {
  if (value === undefined || value === null || value === "") {
    throw badRequest(`${fieldLabel} is required`);
  }

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw badRequest(`${fieldLabel} must be a valid date`);
    }
    return toLocalYyyyMmDd(value);
  }

  const asString = String(value).trim();
  if (!asString) {
    throw badRequest(`${fieldLabel} must be a valid date`);
  }

  const yyyyMmDdMatch = asString.match(/^(\d{4}-\d{2}-\d{2})/);
  if (yyyyMmDdMatch?.[1]) {
    return yyyyMmDdMatch[1];
  }

  const parsed = new Date(asString);
  if (Number.isNaN(parsed.getTime())) {
    throw badRequest(`${fieldLabel} must be a valid date`);
  }
  return toLocalYyyyMmDd(parsed);
}

function normalizeCode(rawValue, fallback = "DEFAULT", maxLength = 50) {
  const normalized = String(rawValue || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9_-]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");

  const safe = normalized || fallback;
  return safe.slice(0, maxLength);
}

function normalizeName(rawValue, fallback = "Default Name", maxLength = 255) {
  const normalized = String(rawValue || "").trim();
  return (normalized || fallback).slice(0, maxLength);
}

function parseBooleanValue(rawValue, defaultValue = false) {
  if (rawValue === undefined || rawValue === null) {
    return defaultValue;
  }
  if (typeof rawValue === "boolean") {
    return rawValue;
  }
  if (typeof rawValue === "number") {
    return rawValue !== 0;
  }
  if (typeof rawValue === "string") {
    const normalized = rawValue.trim().toLowerCase();
    if (["true", "1", "yes", "on"].includes(normalized)) {
      return true;
    }
    if (["false", "0", "no", "off", ""].includes(normalized)) {
      return false;
    }
  }
  return Boolean(rawValue);
}

function parseOptionalNonNegativeNumber(rawValue, label, defaultValue = null) {
  if (rawValue === undefined || rawValue === null || rawValue === "") {
    return defaultValue;
  }

  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw badRequest(`${label} must be a non-negative number`);
  }
  return parsed;
}

async function resolveLegalEntityByCode(tx, tenantId, code) {
  const result = await tx.query(
    `SELECT id, code, name, functional_currency_code
     FROM legal_entities
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, code]
  );
  const row = result.rows[0];
  if (!row) {
    throw new Error("Unable to resolve legal entity after upsert");
  }

  const id = parsePositiveInt(row.id);
  if (!id) {
    throw new Error("Invalid legal entity id");
  }

  return {
    id,
    code: String(row.code || ""),
    name: String(row.name || ""),
    functionalCurrencyCode: String(row.functional_currency_code || "USD").toUpperCase(),
  };
}

async function resolveOrCreateDefaultFiscalCalendar(tx, tenantId) {
  const existing = await tx.query(
    `SELECT id, code, name, year_start_month, year_start_day
     FROM fiscal_calendars
     WHERE tenant_id = ?
     ORDER BY id
     LIMIT 1`,
    [tenantId]
  );
  if (existing.rows[0]) {
    const row = existing.rows[0];
    return {
      id: parsePositiveInt(row.id),
      code: String(row.code || ""),
      name: String(row.name || ""),
      yearStartMonth: Number(row.year_start_month || 1),
      yearStartDay: Number(row.year_start_day || 1),
      created: false,
    };
  }

  const code = "MAIN";
  const name = "Main Calendar";
  const yearStartMonth = 1;
  const yearStartDay = 1;
  await tx.query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
     )
     VALUES (?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name),
       year_start_month = VALUES(year_start_month),
       year_start_day = VALUES(year_start_day)`,
    [tenantId, code, name, yearStartMonth, yearStartDay]
  );

  const created = await tx.query(
    `SELECT id, code, name, year_start_month, year_start_day
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, code]
  );
  const row = created.rows[0];
  if (!row) {
    throw new Error("Unable to resolve fiscal calendar");
  }

  return {
    id: parsePositiveInt(row.id),
    code: String(row.code || ""),
    name: String(row.name || ""),
    yearStartMonth: Number(row.year_start_month || 1),
    yearStartDay: Number(row.year_start_day || 1),
    created: true,
  };
}

async function ensureFiscalPeriodsForYear(tx, calendar, fiscalYear) {
  let created = 0;
  for (let i = 0; i < 12; i += 1) {
    const periodNo = i + 1;
    const existing = await tx.query(
      `SELECT id
       FROM fiscal_periods
       WHERE calendar_id = ?
         AND fiscal_year = ?
         AND period_no = ?
         AND is_adjustment = FALSE
       LIMIT 1`,
      [calendar.id, fiscalYear, periodNo]
    );
    if (existing.rows[0]) {
      continue;
    }

    const monthOffset = calendar.yearStartMonth - 1 + i;
    const start = new Date(Date.UTC(fiscalYear, monthOffset, calendar.yearStartDay));
    const nextStart = new Date(
      Date.UTC(fiscalYear, monthOffset + 1, calendar.yearStartDay)
    );
    const end = new Date(nextStart.getTime() - 24 * 60 * 60 * 1000);
    const periodName = `P${String(periodNo).padStart(2, "0")}`;

    // eslint-disable-next-line no-await-in-loop
    await tx.query(
      `INSERT INTO fiscal_periods (
          calendar_id, fiscal_year, period_no, period_name, start_date, end_date, is_adjustment
       )
       VALUES (?, ?, ?, ?, ?, ?, FALSE)`,
      [calendar.id, fiscalYear, periodNo, periodName, toIsoDate(start), toIsoDate(end)]
    );
    created += 1;
  }
  return created;
}

async function resolveOrCreateDefaultCoa(tx, tenantId, legalEntity) {
  const existing = await tx.query(
    `SELECT id, code
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND scope = 'LEGAL_ENTITY'
     ORDER BY id
     LIMIT 1`,
    [tenantId, legalEntity.id]
  );
  if (existing.rows[0]) {
    return {
      id: parsePositiveInt(existing.rows[0].id),
      code: String(existing.rows[0].code || ""),
      created: false,
    };
  }

  const code = normalizeCode(`COA-${legalEntity.code}`, `COA-${legalEntity.id}`);
  const name = normalizeName(`${legalEntity.name} CoA`, "Default CoA");
  await tx.query(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
     )
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name),
       legal_entity_id = VALUES(legal_entity_id),
       scope = VALUES(scope)`,
    [tenantId, legalEntity.id, code, name]
  );

  const resolved = await tx.query(
    `SELECT id, code
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, code]
  );
  const row = resolved.rows[0];
  if (!row) {
    throw new Error("Unable to resolve chart of accounts");
  }

  return {
    id: parsePositiveInt(row.id),
    code: String(row.code || ""),
    created: true,
  };
}

async function ensureDefaultAccountsForCoa(tx, coaId) {
  const existing = await tx.query(
    `SELECT COUNT(*) AS count
     FROM accounts
     WHERE coa_id = ?`,
    [coaId]
  );
  const existingCount = Number(existing.rows[0]?.count || 0);
  if (existingCount > 0) {
    return 0;
  }

  let created = 0;
  for (const account of DEFAULT_GL_ACCOUNTS) {
    // eslint-disable-next-line no-await-in-loop
    await tx.query(
      `INSERT INTO accounts (
          coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id
       )
       VALUES (?, ?, ?, ?, ?, TRUE, NULL)
       ON DUPLICATE KEY UPDATE
         name = VALUES(name),
         account_type = VALUES(account_type),
         normal_side = VALUES(normal_side),
         allow_posting = VALUES(allow_posting)`,
      [
        coaId,
        String(account.code).trim(),
        String(account.name).trim(),
        String(account.accountType).toUpperCase(),
        String(account.normalSide).toUpperCase(),
      ]
    );
    created += 1;
  }
  return created;
}

async function resolveOrCreateDefaultBook(tx, tenantId, legalEntity, calendarId) {
  const existing = await tx.query(
    `SELECT id, code
     FROM books
     WHERE tenant_id = ?
       AND legal_entity_id = ?
     ORDER BY id
     LIMIT 1`,
    [tenantId, legalEntity.id]
  );
  if (existing.rows[0]) {
    return {
      id: parsePositiveInt(existing.rows[0].id),
      code: String(existing.rows[0].code || ""),
      created: false,
    };
  }

  const code = normalizeCode(`BOOK-${legalEntity.code}`, `BOOK-${legalEntity.id}`);
  const name = normalizeName(`${legalEntity.name} Book`, "Default Book");
  const baseCurrencyCode = normalizeCode(
    legalEntity.functionalCurrencyCode || "USD",
    "USD",
    3
  );
  await tx.query(
    `INSERT INTO books (
        tenant_id, legal_entity_id, calendar_id, code, name, book_type, base_currency_code
     )
     VALUES (?, ?, ?, ?, ?, 'LOCAL', ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name),
       calendar_id = VALUES(calendar_id),
       base_currency_code = VALUES(base_currency_code)`,
    [tenantId, legalEntity.id, calendarId, code, name, baseCurrencyCode]
  );

  const resolved = await tx.query(
    `SELECT id, code
     FROM books
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntity.id, code]
  );
  const row = resolved.rows[0];
  if (!row) {
    throw new Error("Unable to resolve book");
  }

  return {
    id: parsePositiveInt(row.id),
    code: String(row.code || ""),
    created: true,
  };
}

async function autoProvisionLegalEntityGl(tx, tenantId, legalEntity, fiscalYear) {
  const calendar = await resolveOrCreateDefaultFiscalCalendar(tx, tenantId);
  const fiscalPeriodsCreated = await ensureFiscalPeriodsForYear(tx, calendar, fiscalYear);
  const coa = await resolveOrCreateDefaultCoa(tx, tenantId, legalEntity);
  const accountsCreated = await ensureDefaultAccountsForCoa(tx, coa.id);
  const book = await resolveOrCreateDefaultBook(tx, tenantId, legalEntity, calendar.id);

  return {
    calendarId: calendar.id,
    calendarCode: calendar.code,
    coaId: coa.id,
    coaCode: coa.code,
    bookId: book.id,
    bookCode: book.code,
    created: {
      fiscalCalendars: calendar.created ? 1 : 0,
      fiscalPeriods: fiscalPeriodsCreated,
      chartsOfAccounts: coa.created ? 1 : 0,
      accounts: accountsCreated,
      books: book.created ? 1 : 0,
    },
  };
}

router.get(
  "/tree",
  requirePermission("org.tree.read"),
  asyncHandler(async (req, res) => {
    const tenantId = requireOrgTenantId(req);
    const tree = await listOrgTree({
      req,
      tenantId,
      buildScopeFilter,
    });

    return res.json({
      tenantId,
      groups: tree.groups,
      countries: tree.countries,
      legalEntities: tree.legalEntities,
      operatingUnits: tree.operatingUnits,
      rbacSource: req.rbac?.source || null,
      tenantWideScope: Boolean(getScopeContext(req)?.tenantWide),
    });
  })
);

router.get(
  "/group-companies",
  requirePermission("org.tree.read"),
  asyncHandler(async (req, res) => {
    const tenantId = requireOrgTenantId(req);
    const rows = await listGroupCompanies({
      req,
      tenantId,
      buildScopeFilter,
    });

    return res.json({
      tenantId,
      rows,
    });
  })
);

router.get(
  "/countries",
  requirePermission("org.tree.read"),
  asyncHandler(async (req, res) => {
    const tenantId = requireOrgTenantId(req);
    const rows = await listCountries({
      req,
      buildScopeFilter,
    });

    return res.json({
      tenantId,
      rows,
    });
  })
);

router.get(
  "/currencies",
  requirePermission("org.tree.read"),
  asyncHandler(async (req, res) => {
    const tenantId = requireOrgTenantId(req);
    const rows = await listCurrencies();

    return res.json({
      tenantId,
      rows,
    });
  })
);

router.get(
  "/legal-entities",
  requirePermission("org.tree.read"),
  asyncHandler(async (req, res) => {
    const tenantId = requireOrgTenantId(req);
    const filters = parseLegalEntityReadFilters(req.query);
    const rows = await listLegalEntities({
      req,
      tenantId,
      filters,
      buildScopeFilter,
    });

    return res.json({
      tenantId,
      rows,
    });
  })
);

router.get(
  "/operating-units",
  requirePermission("org.tree.read"),
  asyncHandler(async (req, res) => {
    const tenantId = requireOrgTenantId(req);
    const filters = parseOperatingUnitReadFilters(req.query);
    const rows = await listOperatingUnits({
      req,
      tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });

    return res.json({
      tenantId,
      rows,
    });
  })
);

router.get(
  "/fiscal-calendars",
  requirePermission("org.fiscal_calendar.read"),
  asyncHandler(async (req, res) => {
    const tenantId = requireOrgTenantId(req);
    const rows = await listFiscalCalendars({ tenantId });

    return res.json({
      tenantId,
      rows,
    });
  })
);

router.get(
  "/fiscal-calendars/:calendarId/periods",
  requirePermission("org.fiscal_period.read"),
  asyncHandler(async (req, res) => {
    const tenantId = requireOrgTenantId(req);
    const filters = parseFiscalCalendarPeriodFilters(req.params, req.query);
    const result = await listFiscalCalendarPeriods({
      tenantId,
      filters,
    });

    return res.json({
      tenantId,
      calendar: result.calendar,
      fiscalYear: result.fiscalYear,
      rows: result.rows,
    });
  })
);

router.post(
  "/group-companies",
  requirePermission("org.group_company.upsert"),
  asyncHandler(async (req, res) => {
    const { tenantId, code, name } = parseGroupCompanyUpsertInput(req);
    const result = await upsertGroupCompany({
      req,
      tenantId,
      code,
      name,
      assertScopeAccess,
      getScopeContext,
    });
    await invalidateRbacCache(tenantId);

    return res.status(201).json({
      ok: true,
      ...result,
    });
  })
);

router.post(
  "/legal-entities",
  requirePermission("org.legal_entity.upsert", {
    resolveScope: (req, tenantId) => {
      const groupCompanyId = parsePositiveInt(req.body?.groupCompanyId);
      if (groupCompanyId) {
        return { scopeType: "GROUP", scopeId: groupCompanyId };
      }
      return { scopeType: "TENANT", scopeId: tenantId };
    },
  }),
  asyncHandler(async (req, res) => {
    const input = parseLegalEntityUpsertInput(req);
    const operationResult = await upsertLegalEntity({
      req,
      ...input,
      parseBooleanValue,
      assertGroupCompanyBelongsToTenant,
      assertCountryExists,
      assertCurrencyExists,
      assertScopeAccess,
      resolveLegalEntityByCode,
      autoProvisionLegalEntityGl,
    });
    await invalidateRbacCache(input.tenantId);

    return res.status(201).json({
      ok: true,
      ...operationResult,
    });
  })
);

router.get(
  "/shareholder-journal-config",
  requirePermission("org.tree.read", {
    resolveScope: (req) => {
      const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireOrgTenantId(req);
    const filters = parseShareholderJournalConfigFilters(req.query);
    const rows = await listShareholderJournalConfigs({
      req,
      tenantId,
      filters,
      buildScopeFilter,
      assertLegalEntityBelongsToTenant,
      assertScopeAccess,
      capitalPurposeCode: SHAREHOLDER_CAPITAL_CREDIT_PARENT_PURPOSE,
      commitmentPurposeCode: SHAREHOLDER_COMMITMENT_DEBIT_PARENT_PURPOSE,
    });

    return res.json({
      tenantId,
      rows,
    });
  })
);

router.post(
  "/shareholder-journal-config",
  requirePermission("org.legal_entity.upsert", {
    resolveScope: (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return { scopeType: "TENANT", scopeId: tenantId };
    },
  }),
  asyncHandler(async (req, res) => {
    const input = parseShareholderJournalConfigUpsertInput(req);
    const result = await upsertShareholderJournalConfig({
      req,
      ...input,
      assertLegalEntityBelongsToTenant,
      assertScopeAccess,
      assertShareholderParentAccount,
      capitalPurposeCode: SHAREHOLDER_CAPITAL_CREDIT_PARENT_PURPOSE,
      commitmentPurposeCode: SHAREHOLDER_COMMITMENT_DEBIT_PARENT_PURPOSE,
    });

    return res.status(201).json({
      ok: true,
      row: result.row,
    });
  })
);

router.get(
  "/shareholders",
  requirePermission("org.tree.read", {
    resolveScope: (req) => {
      const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = requireOrgTenantId(req);
    const filters = parseShareholderReadFilters(req.query);
    const rows = await listShareholders({
      req,
      tenantId,
      filters,
      buildScopeFilter,
      assertLegalEntityBelongsToTenant,
      assertScopeAccess,
    });

    return res.json({
      tenantId,
      rows,
    });
  })
);

router.post(
  "/shareholders",
  requirePermission("org.legal_entity.upsert", {
    resolveScope: (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return { scopeType: "TENANT", scopeId: tenantId };
    },
  }),
  asyncHandler(async (req, res) => {
    const input = parseShareholderUpsertInput(req);
    const operation = await upsertShareholder({
      req,
      ...input,
      assertLegalEntityBelongsToTenant,
      assertScopeAccess,
      assertCurrencyExists,
      parseOptionalNonNegativeNumber,
      toIsoDate,
      parseBooleanValue,
      resolveShareholderParentMappings,
      assertShareholderParentAccount,
      loadLegalEntityAccountHierarchy,
      normalizeAccountNormalSide,
      isDescendantOfParentAccount,
      normalizeMoney,
      createShareholderCommitmentDraftJournal,
      toCommitmentJournalFailureMessage,
      recalculateShareholderOwnershipPctTx,
    });

    return res.status(201).json({
      ok: true,
      id: operation.shareholderId,
      committedCapitalDelta: operation.committedCapitalDelta,
      commitmentJournal: operation.commitmentJournal,
      autoCommitmentJournal: operation.autoCommitmentJournal,
    });
  })
);

router.post(
  "/shareholders/commitment-journal-batch/preview",
  requirePermission("org.legal_entity.upsert", {
    resolveScope: (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return { scopeType: "TENANT", scopeId: tenantId };
    },
  }),
  asyncHandler(async (req, res) => {
    const input = parseShareholderCommitmentBatchPreviewInput(req);
    const preview = await previewShareholderCommitmentJournalBatch({
      req,
      ...input,
      assertLegalEntityBelongsToTenant,
      assertScopeAccess,
      parseBatchShareholderIds,
      toIsoDate,
      buildShareholderCommitmentBatchPreviewTx,
    });

    return res.json({
      ok: true,
      ...preview,
    });
  })
);

router.post(
  "/shareholders/commitment-journal-batch",
  requirePermission("org.legal_entity.upsert", {
    resolveScope: (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return { scopeType: "TENANT", scopeId: tenantId };
    },
  }),
  asyncHandler(async (req, res) => {
    try {
      const input = parseShareholderCommitmentBatchExecuteInput(req);
      const operation = await executeShareholderCommitmentJournalBatch({
        req,
        ...input,
        assertLegalEntityBelongsToTenant,
        assertScopeAccess,
        parseBatchShareholderIds,
        toIsoDate,
        buildShareholderCommitmentBatchPreviewTx,
        normalizeMoney,
        generateAutoJournalNo,
        normalizeCurrencyCode,
      });

      return res.status(201).json({
        ok: true,
        ...operation,
      });
    } catch (err) {
      if (err?.code === "BATCH_VALIDATION_FAILED") {
        return res.status(400).json({
          message: err.message,
          ...(err.payload || {}),
        });
      }
      throw err;
    }
  })
);

router.post(
  "/shareholders/auto-provision-sub-accounts",
  requirePermission("gl.account.upsert", {
    resolveScope: (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return { scopeType: "TENANT", scopeId: tenantId };
    },
  }),
  asyncHandler(async (req, res) => {
    const input = parseShareholderAutoProvisionSubAccountsInput(req);
    const operation = await autoProvisionShareholderSubAccounts({
      req,
      ...input,
      assertLegalEntityBelongsToTenant,
      assertScopeAccess,
      resolveShareholderParentMappings,
      assertShareholderParentAccount,
      loadLegalEntityAccountHierarchy,
      validateShareholderMappedLeafAccount,
      normalizeShareholderChildSequenceFromCode,
      buildShareholderChildCode,
    });

    return res.status(201).json({
      ok: true,
      message: "Shareholder sub-accounts are ready",
      ...operation,
    });
  })
);

router.post(
  "/operating-units",
  requirePermission("org.operating_unit.upsert", {
    resolveScope: (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return { scopeType: "TENANT", scopeId: tenantId };
    },
  }),
  asyncHandler(async (req, res) => {
    const input = parseOperatingUnitUpsertInput(req);
    const result = await upsertOperatingUnit({
      req,
      ...input,
      assertLegalEntityBelongsToTenant,
      assertScopeAccess,
    });
    await invalidateRbacCache(input.tenantId);

    return res.status(201).json({ ok: true, id: result.id });
  })
);

router.post(
  "/fiscal-calendars",
  requirePermission("org.fiscal_calendar.upsert"),
  asyncHandler(async (req, res) => {
    const input = parseFiscalCalendarUpsertInput(req);
    const result = await upsertFiscalCalendar(input);

    return res.status(201).json({ ok: true, id: result.id });
  })
);

router.post(
  "/fiscal-periods/generate",
  requirePermission("org.fiscal_period.generate"),
  asyncHandler(async (req, res) => {
    const input = parseFiscalPeriodGenerateInput(req);
    const result = await generateFiscalPeriods({
      ...input,
      assertFiscalCalendarBelongsToTenant,
      toIsoDate,
    });

    return res.status(201).json({
      ok: true,
      calendarId: result.calendarId,
      fiscalYear: result.fiscalYear,
      periodsGenerated: result.periodsGenerated,
    });
  })
);

export default router;
