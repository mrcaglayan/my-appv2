import { badRequest, parsePositiveInt, resolveTenantId } from "./_utils.js";

export function requireOrgTenantId(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }
  return tenantId;
}

export function parseLegalEntityReadFilters(rawQuery = {}) {
  return {
    countryId: parsePositiveInt(rawQuery.countryId),
    groupCompanyId: parsePositiveInt(rawQuery.groupCompanyId),
    status: rawQuery.status ? String(rawQuery.status).toUpperCase() : null,
  };
}

export function parseOperatingUnitReadFilters(rawQuery = {}) {
  return {
    legalEntityId: parsePositiveInt(rawQuery.legalEntityId),
  };
}

export function parseFiscalCalendarPeriodFilters(rawParams = {}, rawQuery = {}) {
  const calendarId = parsePositiveInt(rawParams.calendarId);
  if (!calendarId) {
    throw badRequest("calendarId must be a positive integer");
  }

  return {
    calendarId,
    fiscalYear: parsePositiveInt(rawQuery.fiscalYear),
  };
}

export function parseShareholderJournalConfigFilters(rawQuery = {}) {
  return {
    legalEntityId: parsePositiveInt(rawQuery.legalEntityId),
  };
}

export function parseShareholderReadFilters(rawQuery = {}) {
  const status = rawQuery.status ? String(rawQuery.status).toUpperCase() : null;
  if (status && !["ACTIVE", "INACTIVE"].includes(status)) {
    throw badRequest("status must be ACTIVE or INACTIVE");
  }

  return {
    legalEntityId: parsePositiveInt(rawQuery.legalEntityId),
    status,
  };
}
