import express from "express";
import { query, withTransaction } from "../db.js";
import { invalidateRbacCache, requirePermission } from "../middleware/rbac.js";
import {
  asyncHandler,
  assertRequiredFields,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";

const router = express.Router();
const SHAREHOLDER_CAPITAL_CREDIT_PARENT_PURPOSE =
  "SHAREHOLDER_CAPITAL_CREDIT_PARENT";
const SHAREHOLDER_COMMITMENT_DEBIT_PARENT_PURPOSE =
  "SHAREHOLDER_COMMITMENT_DEBIT_PARENT";

const DEFAULT_ACCOUNTS = [
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

const READINESS_DEFINITIONS = [
  { key: "groupCompanies", label: "Group companies", minimum: 1 },
  { key: "legalEntities", label: "Legal entities", minimum: 1 },
  { key: "fiscalCalendars", label: "Fiscal calendars", minimum: 1 },
  { key: "fiscalPeriods", label: "Fiscal periods", minimum: 1 },
  { key: "books", label: "Books", minimum: 1 },
  { key: "openBookPeriods", label: "Open book periods", minimum: 1 },
  { key: "chartsOfAccounts", label: "Charts of accounts", minimum: 1 },
  { key: "accounts", label: "Accounts", minimum: 1 },
  {
    key: "shareholders",
    label: "Shareholders",
    minimum: 1,
  },
  {
    key: "shareholderCommitmentConfigs",
    label: "Shareholder parent account mappings",
    minimum: 1,
  },
];

const PAYMENT_TERM_STATUS_VALUES = new Set(["ACTIVE", "INACTIVE"]);
const DEFAULT_PAYMENT_TERM_TEMPLATES = [
  {
    code: "DUE_ON_RECEIPT",
    name: "Due on Receipt",
    dueDays: 0,
    graceDays: 0,
    isEndOfMonth: false,
    status: "ACTIVE",
  },
  {
    code: "NET_15",
    name: "Net 15",
    dueDays: 15,
    graceDays: 0,
    isEndOfMonth: false,
    status: "ACTIVE",
  },
  {
    code: "NET_30",
    name: "Net 30",
    dueDays: 30,
    graceDays: 0,
    isEndOfMonth: false,
    status: "ACTIVE",
  },
  {
    code: "NET_45",
    name: "Net 45",
    dueDays: 45,
    graceDays: 0,
    isEndOfMonth: false,
    status: "ACTIVE",
  },
  {
    code: "NET_60",
    name: "Net 60",
    dueDays: 60,
    graceDays: 0,
    isEndOfMonth: false,
    status: "ACTIVE",
  },
];

function toIsoDate(date) {
  return date.toISOString().slice(0, 10);
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

function parseNonNegativeInt(value, fieldName, defaultValue = 0) {
  if (value === undefined || value === null || value === "") {
    return defaultValue;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw badRequest(`${fieldName} must be a non-negative integer`);
  }
  return parsed;
}

function parseBooleanFlag(value, fallback = false, fieldName = "value") {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (value === 1 || value === "1") {
    return true;
  }
  if (value === 0 || value === "0") {
    return false;
  }

  const normalized = String(value).trim().toLowerCase();
  if (normalized === "true") {
    return true;
  }
  if (normalized === "false") {
    return false;
  }

  throw badRequest(`${fieldName} must be a boolean`);
}

function normalizePaymentTermTemplate(rawTerm, index) {
  const term = rawTerm || {};
  const code = normalizeCode(term.code, `TERM_${index + 1}`, 50);
  const name = normalizeName(term.name, `Payment Term ${index + 1}`, 255);
  const dueDays = parseNonNegativeInt(
    term.dueDays ?? term.due_days,
    `terms[${index}].dueDays`,
    0
  );
  const graceDays = parseNonNegativeInt(
    term.graceDays ?? term.grace_days,
    `terms[${index}].graceDays`,
    0
  );
  const isEndOfMonth = parseBooleanFlag(
    term.isEndOfMonth ?? term.is_end_of_month,
    false,
    `terms[${index}].isEndOfMonth`
  );
  const status = String(term.status || "ACTIVE")
    .trim()
    .toUpperCase();
  if (!PAYMENT_TERM_STATUS_VALUES.has(status)) {
    throw badRequest(`terms[${index}].status must be ACTIVE or INACTIVE`);
  }

  return {
    code,
    name,
    dueDays,
    graceDays,
    isEndOfMonth,
    status,
  };
}

function normalizePaymentTermTemplates(rawTerms) {
  if (rawTerms !== undefined && !Array.isArray(rawTerms)) {
    throw badRequest("terms must be an array when provided");
  }
  if (Array.isArray(rawTerms) && rawTerms.length === 0) {
    throw badRequest("terms must be a non-empty array when provided");
  }

  const useDefaults = !Array.isArray(rawTerms) || rawTerms.length === 0;
  const sourceTemplates = useDefaults ? DEFAULT_PAYMENT_TERM_TEMPLATES : rawTerms;
  const termTemplates = sourceTemplates.map((term, index) =>
    normalizePaymentTermTemplate(term, index)
  );

  const seenCodes = new Set();
  for (const term of termTemplates) {
    const key = String(term.code || "").toUpperCase();
    if (seenCodes.has(key)) {
      throw badRequest(`Duplicate payment term code: ${term.code}`);
    }
    seenCodes.add(key);
  }

  return {
    termTemplates,
    defaultsUsed: useDefaults,
  };
}

function parseRequestedLegalEntityIds(payload) {
  const body = payload || {};
  const ids = [];

  if (body.legalEntityId !== undefined) {
    const parsedLegalEntityId = parsePositiveInt(body.legalEntityId);
    if (!parsedLegalEntityId) {
      throw badRequest("legalEntityId must be a positive integer");
    }
    ids.push(parsedLegalEntityId);
  }

  if (body.legalEntityIds !== undefined) {
    if (!Array.isArray(body.legalEntityIds)) {
      throw badRequest("legalEntityIds must be an array when provided");
    }
    if (body.legalEntityIds.length === 0) {
      throw badRequest("legalEntityIds must be a non-empty array when provided");
    }

    body.legalEntityIds.forEach((value, index) => {
      const parsedId = parsePositiveInt(value);
      if (!parsedId) {
        throw badRequest(`legalEntityIds[${index}] must be a positive integer`);
      }
      ids.push(parsedId);
    });
  }

  return Array.from(new Set(ids));
}

async function resolveTargetLegalEntityIds(
  tenantId,
  requestedLegalEntityIds,
  runQuery = query
) {
  if (requestedLegalEntityIds.length === 0) {
    const allEntities = await runQuery(
      `SELECT id
       FROM legal_entities
       WHERE tenant_id = ?
       ORDER BY id`,
      [tenantId]
    );
    const allIds = allEntities.rows
      .map((row) => parsePositiveInt(row.id))
      .filter(Boolean);
    if (allIds.length === 0) {
      throw badRequest(
        "No legal entities found for tenant. Run onboarding readiness bootstrap first."
      );
    }
    return allIds;
  }

  const placeholders = requestedLegalEntityIds.map(() => "?").join(", ");
  const result = await runQuery(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
       AND id IN (${placeholders})`,
    [tenantId, ...requestedLegalEntityIds]
  );
  const allowedIds = new Set(
    result.rows.map((row) => parsePositiveInt(row.id)).filter(Boolean)
  );
  const missingIds = requestedLegalEntityIds.filter((id) => !allowedIds.has(id));
  if (missingIds.length > 0) {
    throw badRequest(
      `Legal entity ids not found for tenant: ${missingIds.join(", ")}`
    );
  }

  return requestedLegalEntityIds.filter((id, index) => {
    return requestedLegalEntityIds.indexOf(id) === index;
  });
}

async function bootstrapPaymentTermsForLegalEntities({
  tenantId,
  legalEntityIds,
  termTemplates,
  runQuery = query,
}) {
  const perLegalEntity = [];
  let createdCount = 0;
  let skippedCount = 0;

  for (const legalEntityId of legalEntityIds) {
    let entityCreatedCount = 0;
    let entitySkippedCount = 0;

    for (const term of termTemplates) {
      // eslint-disable-next-line no-await-in-loop
      const insertResult = await runQuery(
        `INSERT IGNORE INTO payment_terms (
            tenant_id,
            legal_entity_id,
            code,
            name,
            due_days,
            grace_days,
            is_end_of_month,
            status
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          tenantId,
          legalEntityId,
          term.code,
          term.name,
          term.dueDays,
          term.graceDays,
          term.isEndOfMonth ? 1 : 0,
          term.status,
        ]
      );
      const affectedRows = Number(insertResult.rows?.affectedRows || 0);
      if (affectedRows > 0) {
        entityCreatedCount += affectedRows;
      } else {
        entitySkippedCount += 1;
      }
    }

    createdCount += entityCreatedCount;
    skippedCount += entitySkippedCount;
    perLegalEntity.push({
      legalEntityId,
      createdCount: entityCreatedCount,
      skippedCount: entitySkippedCount,
    });
  }

  return {
    createdCount,
    skippedCount,
    perLegalEntity,
  };
}

async function scalarCount(sql, params = [], runQuery = query) {
  const result = await runQuery(sql, params);
  const count = Number(result.rows[0]?.count || 0);
  return Number.isFinite(count) ? count : 0;
}

async function buildTenantReadinessSnapshot(tenantId) {
  const [
    groupCompanies,
    legalEntities,
    fiscalCalendars,
    fiscalPeriods,
    books,
    openBookPeriods,
    chartsOfAccounts,
    accounts,
    shareholders,
    shareholderCommitmentConfigs,
  ] = await Promise.all([
    scalarCount(
      `SELECT COUNT(*) AS count
       FROM group_companies
       WHERE tenant_id = ?`,
      [tenantId]
    ),
    scalarCount(
      `SELECT COUNT(*) AS count
       FROM legal_entities
       WHERE tenant_id = ?`,
      [tenantId]
    ),
    scalarCount(
      `SELECT COUNT(*) AS count
       FROM fiscal_calendars
       WHERE tenant_id = ?`,
      [tenantId]
    ),
    scalarCount(
      `SELECT COUNT(*) AS count
       FROM fiscal_periods fp
       JOIN fiscal_calendars fc ON fc.id = fp.calendar_id
       WHERE fc.tenant_id = ?`,
      [tenantId]
    ),
    scalarCount(
      `SELECT COUNT(*) AS count
       FROM books
       WHERE tenant_id = ?`,
      [tenantId]
    ),
    scalarCount(
      `SELECT COUNT(*) AS count
       FROM books b
       JOIN fiscal_periods fp
         ON fp.calendar_id = b.calendar_id
        AND fp.is_adjustment = FALSE
       LEFT JOIN period_statuses ps
         ON ps.book_id = b.id
        AND ps.fiscal_period_id = fp.id
       WHERE b.tenant_id = ?
         AND COALESCE(ps.status, 'OPEN') = 'OPEN'`,
      [tenantId]
    ),
    scalarCount(
      `SELECT COUNT(*) AS count
       FROM charts_of_accounts
       WHERE tenant_id = ?`,
      [tenantId]
    ),
    scalarCount(
      `SELECT COUNT(*) AS count
       FROM accounts a
       JOIN charts_of_accounts c ON c.id = a.coa_id
       WHERE c.tenant_id = ?`,
      [tenantId]
    ),
    scalarCount(
      `SELECT COUNT(*) AS count
       FROM shareholders
       WHERE tenant_id = ?`,
      [tenantId]
    ),
    scalarCount(
      `SELECT COUNT(*) AS count
       FROM legal_entities le
       WHERE le.tenant_id = ?
         AND EXISTS (
           SELECT 1
           FROM journal_purpose_accounts cap
           WHERE cap.tenant_id = le.tenant_id
             AND cap.legal_entity_id = le.id
             AND cap.purpose_code = ?
         )
         AND EXISTS (
           SELECT 1
           FROM journal_purpose_accounts deb
           WHERE deb.tenant_id = le.tenant_id
             AND deb.legal_entity_id = le.id
             AND deb.purpose_code = ?
         )`,
      [
        tenantId,
        SHAREHOLDER_CAPITAL_CREDIT_PARENT_PURPOSE,
        SHAREHOLDER_COMMITMENT_DEBIT_PARENT_PURPOSE,
      ]
    ),
  ]);

  const counts = {
    groupCompanies,
    legalEntities,
    fiscalCalendars,
    fiscalPeriods,
    books,
    openBookPeriods,
    chartsOfAccounts,
    accounts,
    shareholders,
    shareholderCommitmentConfigs,
  };

  const checks = READINESS_DEFINITIONS.map((definition) => {
    const count = Number(counts[definition.key] || 0);
    return {
      ...definition,
      count,
      ready: count >= definition.minimum,
    };
  });

  const missing = checks.filter((check) => !check.ready);

  return {
    tenantId,
    ready: missing.length === 0,
    checks,
    counts,
    missingKeys: missing.map((check) => check.key),
    generatedAt: new Date().toISOString(),
  };
}

async function getCountryId(countryId, countryIso2, runQuery = query) {
  const normalizedCountryId = parsePositiveInt(countryId);
  if (normalizedCountryId) {
    return normalizedCountryId;
  }

  if (!countryIso2) {
    throw badRequest("countryId or countryIso2 is required for legal entity");
  }

  const result = await runQuery(
    `SELECT id
     FROM countries
     WHERE iso2 = ?
     LIMIT 1`,
    [String(countryIso2).trim().toUpperCase()]
  );
  const resolved = parsePositiveInt(result.rows[0]?.id);
  if (!resolved) {
    throw badRequest(`Country not found for iso2=${countryIso2}`);
  }
  return resolved;
}

async function getGroupCompanyId(tenantId, code, runQuery = query) {
  const result = await runQuery(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, String(code).trim()]
  );
  return parsePositiveInt(result.rows[0]?.id);
}

async function getFiscalCalendarId(tenantId, code, runQuery = query) {
  const result = await runQuery(
    `SELECT id
     FROM fiscal_calendars
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, String(code).trim()]
  );
  return parsePositiveInt(result.rows[0]?.id);
}

async function getLegalEntityId(tenantId, code, runQuery = query) {
  const result = await runQuery(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, String(code).trim()]
  );
  return parsePositiveInt(result.rows[0]?.id);
}

async function getCoaId(tenantId, code, runQuery = query) {
  const result = await runQuery(
    `SELECT id
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, String(code).trim()]
  );
  return parsePositiveInt(result.rows[0]?.id);
}

async function getPrimaryCountry(runQuery = query) {
  const preferredResult = await runQuery(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const preferred = preferredResult.rows[0];
  if (preferred) {
    return {
      id: parsePositiveInt(preferred.id),
      defaultCurrencyCode: String(preferred.default_currency_code || "USD").toUpperCase(),
    };
  }

  const fallbackResult = await runQuery(
    `SELECT id, default_currency_code
     FROM countries
     ORDER BY id
     LIMIT 1`
  );
  const fallback = fallbackResult.rows[0];
  const fallbackId = parsePositiveInt(fallback?.id);
  if (!fallbackId) {
    throw new Error("No countries available to build baseline legal entity");
  }

  return {
    id: fallbackId,
    defaultCurrencyCode: String(fallback.default_currency_code || "USD").toUpperCase(),
  };
}

async function ensureDefaultGroupCompany(tenantId, runQuery = query) {
  const existingResult = await runQuery(
    `SELECT id, code, name
     FROM group_companies
     WHERE tenant_id = ?
     ORDER BY id
     LIMIT 1`,
    [tenantId]
  );
  const existing = existingResult.rows[0];
  if (existing) {
    return {
      id: parsePositiveInt(existing.id),
      code: String(existing.code),
      name: String(existing.name),
      created: false,
    };
  }

  const code = "DEFAULT";
  const name = "Default Group";
  await runQuery(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name)`,
    [tenantId, code, name]
  );

  const id = await getGroupCompanyId(tenantId, code, runQuery);
  if (!id) {
    throw new Error("Unable to resolve default group company");
  }

  return { id, code, name, created: true };
}

async function ensureDefaultLegalEntity(tenantId, groupCompanyId, runQuery = query) {
  const existingResult = await runQuery(
    `SELECT id, code, name, functional_currency_code
     FROM legal_entities
     WHERE tenant_id = ?
     ORDER BY id
     LIMIT 1`,
    [tenantId]
  );
  const existing = existingResult.rows[0];
  if (existing) {
    return {
      id: parsePositiveInt(existing.id),
      code: String(existing.code),
      name: String(existing.name),
      functionalCurrencyCode: String(existing.functional_currency_code || "USD").toUpperCase(),
      created: false,
    };
  }

  const country = await getPrimaryCountry(runQuery);
  const code = "DEFAULT_LE";
  const name = "Default Legal Entity";
  await runQuery(
    `INSERT INTO legal_entities (
        tenant_id,
        group_company_id,
        code,
        name,
        tax_id,
        country_id,
        functional_currency_code,
        is_intercompany_enabled,
        intercompany_partner_required
     )
     VALUES (?, ?, ?, ?, NULL, ?, ?, TRUE, FALSE)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name),
       group_company_id = VALUES(group_company_id),
       country_id = VALUES(country_id),
       functional_currency_code = VALUES(functional_currency_code),
       is_intercompany_enabled = VALUES(is_intercompany_enabled),
       intercompany_partner_required = VALUES(intercompany_partner_required)`,
    [tenantId, groupCompanyId, code, name, country.id, country.defaultCurrencyCode]
  );

  const id = await getLegalEntityId(tenantId, code, runQuery);
  if (!id) {
    throw new Error("Unable to resolve default legal entity");
  }

  return {
    id,
    code,
    name,
    functionalCurrencyCode: country.defaultCurrencyCode,
    created: true,
  };
}

async function ensureDefaultFiscalCalendar(tenantId, runQuery = query) {
  const existingResult = await runQuery(
    `SELECT id, code, name, year_start_month, year_start_day
     FROM fiscal_calendars
     WHERE tenant_id = ?
     ORDER BY id
     LIMIT 1`,
    [tenantId]
  );
  const existing = existingResult.rows[0];
  if (existing) {
    return {
      id: parsePositiveInt(existing.id),
      code: String(existing.code),
      name: String(existing.name),
      yearStartMonth: Number(existing.year_start_month),
      yearStartDay: Number(existing.year_start_day),
      created: false,
    };
  }

  const code = "MAIN";
  const name = "Main Calendar";
  const yearStartMonth = 1;
  const yearStartDay = 1;
  await runQuery(
    `INSERT INTO fiscal_calendars (
        tenant_id,
        code,
        name,
        year_start_month,
        year_start_day
     )
     VALUES (?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name),
       year_start_month = VALUES(year_start_month),
       year_start_day = VALUES(year_start_day)`,
    [tenantId, code, name, yearStartMonth, yearStartDay]
  );

  const id = await getFiscalCalendarId(tenantId, code, runQuery);
  if (!id) {
    throw new Error("Unable to resolve default fiscal calendar");
  }

  return { id, code, name, yearStartMonth, yearStartDay, created: true };
}

async function ensureFiscalPeriods(
  calendarId,
  fiscalYear,
  yearStartMonth,
  yearStartDay,
  runQuery = query
) {
  let created = 0;

  for (let i = 0; i < 12; i += 1) {
    const periodNo = i + 1;
    const existingResult = await runQuery(
      `SELECT id
       FROM fiscal_periods
       WHERE calendar_id = ?
         AND fiscal_year = ?
         AND period_no = ?
         AND is_adjustment = FALSE
       LIMIT 1`,
      [calendarId, fiscalYear, periodNo]
    );
    if (existingResult.rows[0]) {
      continue;
    }

    const monthOffset = yearStartMonth - 1 + i;
    const start = new Date(Date.UTC(fiscalYear, monthOffset, yearStartDay));
    const nextStart = new Date(Date.UTC(fiscalYear, monthOffset + 1, yearStartDay));
    const end = new Date(nextStart.getTime() - 24 * 60 * 60 * 1000);
    const periodName = `P${String(periodNo).padStart(2, "0")}`;

    await runQuery(
      `INSERT INTO fiscal_periods (
          calendar_id, fiscal_year, period_no, period_name, start_date, end_date, is_adjustment
       )
       VALUES (?, ?, ?, ?, ?, ?, FALSE)`,
      [calendarId, fiscalYear, periodNo, periodName, toIsoDate(start), toIsoDate(end)]
    );
    created += 1;
  }

  return created;
}

async function getTenantLegalEntities(tenantId, runQuery = query) {
  const result = await runQuery(
    `SELECT id, code, name, functional_currency_code
     FROM legal_entities
     WHERE tenant_id = ?
     ORDER BY id`,
    [tenantId]
  );

  return result.rows.map((row) => ({
    id: parsePositiveInt(row.id),
    code: String(row.code),
    name: String(row.name),
    functionalCurrencyCode: String(row.functional_currency_code || "USD").toUpperCase(),
  }));
}

async function ensureCoaForLegalEntity(tenantId, legalEntity, runQuery = query) {
  const existingResult = await runQuery(
    `SELECT id, code
     FROM charts_of_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND scope = 'LEGAL_ENTITY'
     ORDER BY id
     LIMIT 1`,
    [tenantId, legalEntity.id]
  );
  const existing = existingResult.rows[0];
  if (existing) {
    return {
      id: parsePositiveInt(existing.id),
      code: String(existing.code),
      created: false,
    };
  }

  const code = normalizeCode(`COA-${legalEntity.code}`, `COA-${legalEntity.id}`);
  const name = normalizeName(`${legalEntity.name} CoA`, "Default CoA");
  await runQuery(
    `INSERT INTO charts_of_accounts (
        tenant_id, legal_entity_id, scope, code, name
     )
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name),
       legal_entity_id = VALUES(legal_entity_id)`,
    [tenantId, legalEntity.id, code, name]
  );

  const id = await getCoaId(tenantId, code, runQuery);
  if (!id) {
    throw new Error(`Unable to resolve CoA for legal entity ${legalEntity.id}`);
  }

  return { id, code, created: true };
}

async function ensureDefaultAccountsForCoa(coaId, runQuery = query) {
  const existingCount = await scalarCount(
    `SELECT COUNT(*) AS count
     FROM accounts
     WHERE coa_id = ?`,
    [coaId],
    runQuery
  );
  if (existingCount > 0) {
    return 0;
  }

  let created = 0;
  for (const account of DEFAULT_ACCOUNTS) {
    await runQuery(
      `INSERT INTO accounts (
          coa_id,
          code,
          name,
          account_type,
          normal_side,
          allow_posting,
          parent_account_id
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

async function ensureBookForLegalEntity(
  tenantId,
  legalEntity,
  calendarId,
  runQuery = query
) {
  const existingResult = await runQuery(
    `SELECT id, code
     FROM books
     WHERE tenant_id = ?
       AND legal_entity_id = ?
     ORDER BY id
     LIMIT 1`,
    [tenantId, legalEntity.id]
  );
  const existing = existingResult.rows[0];
  if (existing) {
    return {
      id: parsePositiveInt(existing.id),
      code: String(existing.code),
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

  await runQuery(
    `INSERT INTO books (
        tenant_id,
        legal_entity_id,
        calendar_id,
        code,
        name,
        book_type,
        base_currency_code
     )
     VALUES (?, ?, ?, ?, ?, 'LOCAL', ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name),
       calendar_id = VALUES(calendar_id),
       base_currency_code = VALUES(base_currency_code)`,
    [tenantId, legalEntity.id, calendarId, code, name, baseCurrencyCode]
  );

  const resolved = await runQuery(
    `SELECT id, code
     FROM books
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntity.id, code]
  );
  const id = parsePositiveInt(resolved.rows[0]?.id);
  if (!id) {
    throw new Error(`Unable to resolve book for legal entity ${legalEntity.id}`);
  }

  return { id, code, created: true };
}

router.get(
  "/readiness",
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const readiness = await buildTenantReadinessSnapshot(tenantId);
    return res.json(readiness);
  })
);

router.post(
  "/readiness/bootstrap-baseline",
  requirePermission("onboarding.company.setup"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const fiscalYear =
      parsePositiveInt(req.body?.fiscalYear) || new Date().getUTCFullYear();
    if (!fiscalYear) {
      throw badRequest("fiscalYear must be a positive integer");
    }

    const readinessBefore = await buildTenantReadinessSnapshot(tenantId);
    const bootstrapResult = await withTransaction(async (tx) => {
      const groupCompany = await ensureDefaultGroupCompany(tenantId, tx.query);
      const legalEntity = await ensureDefaultLegalEntity(
        tenantId,
        groupCompany.id,
        tx.query
      );
      const calendar = await ensureDefaultFiscalCalendar(tenantId, tx.query);
      const fiscalPeriodsCreated = await ensureFiscalPeriods(
        calendar.id,
        fiscalYear,
        calendar.yearStartMonth,
        calendar.yearStartDay,
        tx.query
      );

      const legalEntities = await getTenantLegalEntities(tenantId, tx.query);
      let coasCreated = 0;
      let accountsCreated = 0;
      let booksCreated = 0;

      for (const entity of legalEntities) {
        // eslint-disable-next-line no-await-in-loop
        const coa = await ensureCoaForLegalEntity(tenantId, entity, tx.query);
        if (coa.created) {
          coasCreated += 1;
        }

        // eslint-disable-next-line no-await-in-loop
        accountsCreated += await ensureDefaultAccountsForCoa(coa.id, tx.query);

        // eslint-disable-next-line no-await-in-loop
        const book = await ensureBookForLegalEntity(
          tenantId,
          entity,
          calendar.id,
          tx.query
        );
        if (book.created) {
          booksCreated += 1;
        }
      }

      return {
        groupCompany,
        legalEntity,
        calendar,
        fiscalPeriodsCreated,
        coasCreated,
        accountsCreated,
        booksCreated,
      };
    });
    await invalidateRbacCache(tenantId);

    const readinessAfter = await buildTenantReadinessSnapshot(tenantId);

    return res.status(201).json({
      ok: true,
      tenantId,
      fiscalYear,
      created: {
        groupCompanies: bootstrapResult.groupCompany.created ? 1 : 0,
        legalEntities: bootstrapResult.legalEntity.created ? 1 : 0,
        fiscalCalendars: bootstrapResult.calendar.created ? 1 : 0,
        fiscalPeriods: bootstrapResult.fiscalPeriodsCreated,
        chartsOfAccounts: bootstrapResult.coasCreated,
        accounts: bootstrapResult.accountsCreated,
        books: bootstrapResult.booksCreated,
      },
      readinessBefore: {
        ready: readinessBefore.ready,
        missingKeys: readinessBefore.missingKeys,
      },
      readinessAfter: {
        ready: readinessAfter.ready,
        missingKeys: readinessAfter.missingKeys,
      },
    });
  })
);

router.post(
  "/payment-terms/bootstrap",
  requirePermission("onboarding.company.setup"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const requestedLegalEntityIds = parseRequestedLegalEntityIds(req.body);
    const { termTemplates, defaultsUsed } = normalizePaymentTermTemplates(
      req.body?.terms
    );

    const bootstrapResult = await withTransaction(async (tx) => {
      const legalEntityIds = await resolveTargetLegalEntityIds(
        tenantId,
        requestedLegalEntityIds,
        tx.query
      );
      const insertResult = await bootstrapPaymentTermsForLegalEntities({
        tenantId,
        legalEntityIds,
        termTemplates,
        runQuery: tx.query,
      });

      return {
        legalEntityIds,
        ...insertResult,
      };
    });

    return res.status(201).json({
      ok: true,
      tenantId,
      defaultsUsed,
      legalEntityIds: bootstrapResult.legalEntityIds,
      termTemplates,
      createdCount: bootstrapResult.createdCount,
      skippedCount: bootstrapResult.skippedCount,
      perLegalEntity: bootstrapResult.perLegalEntity,
    });
  })
);

router.post(
  "/company-bootstrap",
  requirePermission("onboarding.company.setup"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    assertRequiredFields(req.body, [
      "groupCompany",
      "fiscalCalendar",
      "fiscalYear",
      "legalEntities",
    ]);

    const groupCompany = req.body.groupCompany || {};
    const fiscalCalendar = req.body.fiscalCalendar || {};
    const fiscalYear = parsePositiveInt(req.body.fiscalYear);
    const legalEntities = Array.isArray(req.body.legalEntities)
      ? req.body.legalEntities
      : [];

    if (!fiscalYear) {
      throw badRequest("fiscalYear must be a positive integer");
    }
    if (legalEntities.length === 0) {
      throw badRequest("legalEntities must be a non-empty array");
    }

    assertRequiredFields(groupCompany, ["code", "name"]);
    assertRequiredFields(fiscalCalendar, [
      "code",
      "name",
      "yearStartMonth",
      "yearStartDay",
    ]);

    const yearStartMonth = parsePositiveInt(fiscalCalendar.yearStartMonth);
    const yearStartDay = parsePositiveInt(fiscalCalendar.yearStartDay);
    if (!yearStartMonth || yearStartMonth > 12) {
      throw badRequest("fiscalCalendar.yearStartMonth must be between 1 and 12");
    }
    if (!yearStartDay || yearStartDay > 31) {
      throw badRequest("fiscalCalendar.yearStartDay must be between 1 and 31");
    }

    const bootstrapResult = await withTransaction(async (tx) => {
      await tx.query(
        `INSERT INTO group_companies (tenant_id, code, name)
         VALUES (?, ?, ?)
         ON DUPLICATE KEY UPDATE
           name = VALUES(name)`,
        [tenantId, String(groupCompany.code).trim(), String(groupCompany.name).trim()]
      );
      const groupCompanyId = await getGroupCompanyId(
        tenantId,
        groupCompany.code,
        tx.query
      );
      if (!groupCompanyId) {
        throw new Error("Unable to resolve group company id");
      }

      await tx.query(
        `INSERT INTO fiscal_calendars (
            tenant_id, code, name, year_start_month, year_start_day
         )
         VALUES (?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           name = VALUES(name),
           year_start_month = VALUES(year_start_month),
           year_start_day = VALUES(year_start_day)`,
        [
          tenantId,
          String(fiscalCalendar.code).trim(),
          String(fiscalCalendar.name).trim(),
          yearStartMonth,
          yearStartDay,
        ]
      );
      const calendarId = await getFiscalCalendarId(
        tenantId,
        fiscalCalendar.code,
        tx.query
      );
      if (!calendarId) {
        throw new Error("Unable to resolve fiscal calendar id");
      }

      for (let i = 0; i < 12; i += 1) {
        const monthOffset = yearStartMonth - 1 + i;
        const start = new Date(Date.UTC(fiscalYear, monthOffset, yearStartDay));
        const nextStart = new Date(Date.UTC(fiscalYear, monthOffset + 1, yearStartDay));
        const end = new Date(nextStart.getTime() - 24 * 60 * 60 * 1000);
        const periodNo = i + 1;
        const periodName = `P${String(periodNo).padStart(2, "0")}`;

        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO fiscal_periods (
              calendar_id, fiscal_year, period_no, period_name, start_date, end_date, is_adjustment
           )
           VALUES (?, ?, ?, ?, ?, ?, FALSE)
           ON DUPLICATE KEY UPDATE
             period_name = VALUES(period_name),
             start_date = VALUES(start_date),
             end_date = VALUES(end_date)`,
          [calendarId, fiscalYear, periodNo, periodName, toIsoDate(start), toIsoDate(end)]
        );
      }

      const entitySummaries = [];

      for (const entity of legalEntities) {
        assertRequiredFields(entity, ["code", "name", "functionalCurrencyCode"]);
        // eslint-disable-next-line no-await-in-loop
        const countryId = await getCountryId(entity.countryId, entity.countryIso2, tx.query);

        const intercompanyEnabled =
          entity.isIntercompanyEnabled === undefined
            ? true
            : Boolean(entity.isIntercompanyEnabled);
        const partnerRequired = Boolean(entity.intercompanyPartnerRequired);

        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO legal_entities (
              tenant_id, group_company_id, code, name, tax_id, country_id, functional_currency_code,
              is_intercompany_enabled, intercompany_partner_required
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             name = VALUES(name),
             tax_id = VALUES(tax_id),
             country_id = VALUES(country_id),
             functional_currency_code = VALUES(functional_currency_code),
             group_company_id = VALUES(group_company_id),
             is_intercompany_enabled = VALUES(is_intercompany_enabled),
             intercompany_partner_required = VALUES(intercompany_partner_required)`,
          [
            tenantId,
            groupCompanyId,
            String(entity.code).trim(),
            String(entity.name).trim(),
            entity.taxId ? String(entity.taxId).trim() : null,
            countryId,
            String(entity.functionalCurrencyCode).toUpperCase(),
            intercompanyEnabled,
            partnerRequired,
          ]
        );

        // eslint-disable-next-line no-await-in-loop
        const legalEntityId = await getLegalEntityId(tenantId, entity.code, tx.query);
        if (!legalEntityId) {
          throw new Error(`Unable to resolve legal entity id for ${entity.code}`);
        }

        const branches = Array.isArray(entity.branches) ? entity.branches : [];
        for (const branch of branches) {
          assertRequiredFields(branch, ["code", "name"]);
          // eslint-disable-next-line no-await-in-loop
          await tx.query(
            `INSERT INTO operating_units (
                tenant_id, legal_entity_id, code, name, unit_type, has_subledger
             )
             VALUES (?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
               name = VALUES(name),
               unit_type = VALUES(unit_type),
               has_subledger = VALUES(has_subledger)`,
            [
              tenantId,
              legalEntityId,
              String(branch.code).trim(),
              String(branch.name).trim(),
              String(branch.unitType || "BRANCH").toUpperCase(),
              Boolean(branch.hasSubledger),
            ]
          );
        }

        const coaCode = entity.coaCode
          ? String(entity.coaCode).trim()
          : `COA-${String(entity.code).trim().toUpperCase()}`;
        const coaName = entity.coaName
          ? String(entity.coaName).trim()
          : `${String(entity.name).trim()} CoA`;

        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO charts_of_accounts (
              tenant_id, legal_entity_id, scope, code, name
           )
           VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)
           ON DUPLICATE KEY UPDATE
             name = VALUES(name),
             legal_entity_id = VALUES(legal_entity_id)`,
          [tenantId, legalEntityId, coaCode, coaName]
        );
        // eslint-disable-next-line no-await-in-loop
        const coaId = await getCoaId(tenantId, coaCode, tx.query);
        if (!coaId) {
          throw new Error(`Unable to resolve CoA for ${coaCode}`);
        }

        const accounts =
          Array.isArray(entity.defaultAccounts) && entity.defaultAccounts.length
            ? entity.defaultAccounts
            : DEFAULT_ACCOUNTS;

        for (const account of accounts) {
          assertRequiredFields(account, ["code", "name", "accountType", "normalSide"]);
          // eslint-disable-next-line no-await-in-loop
          await tx.query(
            `INSERT INTO accounts (
                coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id
             )
             VALUES (?, ?, ?, ?, ?, ?, NULL)
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
              account.allowPosting === undefined ? true : Boolean(account.allowPosting),
            ]
          );
        }

        const bookCode = entity.bookCode
          ? String(entity.bookCode).trim()
          : `BOOK-${String(entity.code).trim().toUpperCase()}`;
        const bookName = entity.bookName
          ? String(entity.bookName).trim()
          : `${String(entity.name).trim()} Book`;

        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO books (
              tenant_id, legal_entity_id, calendar_id, code, name, book_type, base_currency_code
           )
           VALUES (?, ?, ?, ?, ?, 'LOCAL', ?)
           ON DUPLICATE KEY UPDATE
             name = VALUES(name),
             calendar_id = VALUES(calendar_id),
             base_currency_code = VALUES(base_currency_code)`,
          [
            tenantId,
            legalEntityId,
            calendarId,
            bookCode,
            bookName,
            String(entity.functionalCurrencyCode).toUpperCase(),
          ]
        );

        entitySummaries.push({
          code: String(entity.code).trim(),
          legalEntityId,
          coaCode,
          coaId,
          branchCount: branches.length,
        });
      }

      const legalEntityIds = entitySummaries.map((entity) => entity.legalEntityId);
      const paymentTermBootstrap = await bootstrapPaymentTermsForLegalEntities({
        tenantId,
        legalEntityIds,
        termTemplates: DEFAULT_PAYMENT_TERM_TEMPLATES,
        runQuery: tx.query,
      });

      return {
        groupCompanyId,
        calendarId,
        entitySummaries,
        paymentTermBootstrap,
      };
    });
    await invalidateRbacCache(tenantId);

    return res.status(201).json({
      ok: true,
      tenantId,
      groupCompanyId: bootstrapResult.groupCompanyId,
      calendarId: bootstrapResult.calendarId,
      fiscalYear,
      periodsGenerated: 12,
      legalEntities: bootstrapResult.entitySummaries,
      paymentTerms: {
        defaultsUsed: true,
        templateCount: DEFAULT_PAYMENT_TERM_TEMPLATES.length,
        createdCount: bootstrapResult.paymentTermBootstrap.createdCount,
        skippedCount: bootstrapResult.paymentTermBootstrap.skippedCount,
        perLegalEntity: bootstrapResult.paymentTermBootstrap.perLegalEntity,
      },
    });
  })
);

export default router;
