import { query } from "./db.js";

function parsePositiveInt(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
}

function badRequest(message) {
  const err = new Error(message);
  err.status = 400;
  return err;
}

async function requireRow(sql, params, errorMessage, options = {}) {
  const runQuery =
    typeof options === "function" ? options : options.runQuery || query;
  const result = await runQuery(sql, params);
  const row = result.rows[0] || null;
  if (!row) {
    throw badRequest(errorMessage);
  }
  return row;
}

export async function assertUserBelongsToTenant(tenantId, userId, label = "userId") {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedUserId = parsePositiveInt(userId);
  if (!parsedTenantId || !parsedUserId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT id, tenant_id, status
     FROM users
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [parsedUserId, parsedTenantId],
    `${label} not found for tenant`
  );
}

export async function assertRoleBelongsToTenant(tenantId, roleId, label = "roleId") {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedRoleId = parsePositiveInt(roleId);
  if (!parsedTenantId || !parsedRoleId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT id, tenant_id, code, is_system
     FROM roles
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [parsedRoleId, parsedTenantId],
    `${label} not found for tenant`
  );
}

export async function assertGroupCompanyBelongsToTenant(
  tenantId,
  groupCompanyId,
  label = "groupCompanyId"
) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedGroupCompanyId = parsePositiveInt(groupCompanyId);
  if (!parsedTenantId || !parsedGroupCompanyId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT id, tenant_id, code, name
     FROM group_companies
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [parsedGroupCompanyId, parsedTenantId],
    `${label} not found for tenant`
  );
}

export async function assertLegalEntityBelongsToTenant(
  tenantId,
  legalEntityId,
  label = "legalEntityId"
) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedLegalEntityId = parsePositiveInt(legalEntityId);
  if (!parsedTenantId || !parsedLegalEntityId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT
       id,
       tenant_id,
       group_company_id,
       code,
       name,
       country_id,
       functional_currency_code,
       status
     FROM legal_entities
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [parsedLegalEntityId, parsedTenantId],
    `${label} not found for tenant`
  );
}

export async function assertOperatingUnitBelongsToTenant(
  tenantId,
  operatingUnitId,
  label = "operatingUnitId"
) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedOperatingUnitId = parsePositiveInt(operatingUnitId);
  if (!parsedTenantId || !parsedOperatingUnitId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT id, tenant_id, legal_entity_id, status
     FROM operating_units
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [parsedOperatingUnitId, parsedTenantId],
    `${label} not found for tenant`
  );
}

export async function assertCountryExists(countryId, label = "countryId") {
  const parsedCountryId = parsePositiveInt(countryId);
  if (!parsedCountryId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT id, iso2, iso3, default_currency_code
     FROM countries
     WHERE id = ?
     LIMIT 1`,
    [parsedCountryId],
    `${label} not found`
  );
}

export async function assertCurrencyExists(currencyCode, label = "currencyCode") {
  const normalized = String(currencyCode || "")
    .trim()
    .toUpperCase();
  if (!normalized || normalized.length !== 3) {
    throw badRequest(`${label} must be a 3-letter currency code`);
  }

  return requireRow(
    `SELECT code, name, minor_units
     FROM currencies
     WHERE code = ?
     LIMIT 1`,
    [normalized],
    `${label} not found`
  );
}

export async function assertFiscalCalendarBelongsToTenant(
  tenantId,
  calendarId,
  label = "calendarId"
) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedCalendarId = parsePositiveInt(calendarId);
  if (!parsedTenantId || !parsedCalendarId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT id, tenant_id, code, year_start_month, year_start_day
     FROM fiscal_calendars
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [parsedCalendarId, parsedTenantId],
    `${label} not found for tenant`
  );
}

export async function assertFiscalPeriodBelongsToCalendar(
  calendarId,
  fiscalPeriodId,
  label = "fiscalPeriodId"
) {
  const parsedCalendarId = parsePositiveInt(calendarId);
  const parsedFiscalPeriodId = parsePositiveInt(fiscalPeriodId);
  if (!parsedCalendarId || !parsedFiscalPeriodId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT id, calendar_id, fiscal_year, period_no
     FROM fiscal_periods
     WHERE id = ?
       AND calendar_id = ?
     LIMIT 1`,
    [parsedFiscalPeriodId, parsedCalendarId],
    `${label} not found for calendar`
  );
}

export async function assertBookBelongsToTenant(tenantId, bookId, label = "bookId") {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedBookId = parsePositiveInt(bookId);
  if (!parsedTenantId || !parsedBookId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT
       id,
       tenant_id,
       legal_entity_id,
       calendar_id,
       base_currency_code
     FROM books
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [parsedBookId, parsedTenantId],
    `${label} not found for tenant`
  );
}

export async function assertCoaBelongsToTenant(tenantId, coaId, label = "coaId") {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedCoaId = parsePositiveInt(coaId);
  if (!parsedTenantId || !parsedCoaId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT id, tenant_id, legal_entity_id, scope, code
     FROM charts_of_accounts
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [parsedCoaId, parsedTenantId],
    `${label} not found for tenant`
  );
}

export async function assertAccountBelongsToTenant(
  tenantId,
  accountId,
  label = "accountId",
  options = {}
) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedAccountId = parsePositiveInt(accountId);
  if (!parsedTenantId || !parsedAccountId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT
       a.id,
       a.coa_id,
       c.tenant_id,
       c.legal_entity_id,
       c.scope
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE a.id = ?
       AND c.tenant_id = ?
     LIMIT 1`,
    [parsedAccountId, parsedTenantId],
    `${label} not found for tenant`,
    options
  );
}

export async function assertConsolidationGroupBelongsToTenant(
  tenantId,
  consolidationGroupId,
  label = "consolidationGroupId"
) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedConsolidationGroupId = parsePositiveInt(consolidationGroupId);
  if (!parsedTenantId || !parsedConsolidationGroupId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT
       id,
       tenant_id,
       group_company_id,
       calendar_id,
       presentation_currency_code,
       status
     FROM consolidation_groups
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [parsedConsolidationGroupId, parsedTenantId],
    `${label} not found for tenant`
  );
}

export async function assertConsolidationRunBelongsToTenant(
  tenantId,
  runId,
  label = "runId"
) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedRunId = parsePositiveInt(runId);
  if (!parsedTenantId || !parsedRunId) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return requireRow(
    `SELECT
       cr.id,
       cr.consolidation_group_id,
       cr.fiscal_period_id,
       cr.status,
       cg.tenant_id,
       cg.group_company_id,
       cg.calendar_id
     FROM consolidation_runs cr
     JOIN consolidation_groups cg ON cg.id = cr.consolidation_group_id
     WHERE cr.id = ?
       AND cg.tenant_id = ?
     LIMIT 1`,
    [parsedRunId, parsedTenantId],
    `${label} not found for tenant`
  );
}
