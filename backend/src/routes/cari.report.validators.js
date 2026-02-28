import { badRequest } from "./_utils.js";
import {
  normalizeEnum,
  optionalPositiveInt,
  parseBooleanFlag,
  parseDateOnly,
  parsePagination,
  requireTenantId,
} from "./cash.validators.common.js";

const DIRECTION_VALUES = ["AR", "AP"];
const ROLE_FILTER_VALUES = ["CUSTOMER", "VENDOR", "BOTH"];
const OPEN_STATUS_FILTER_VALUES = ["OPEN", "PARTIALLY_SETTLED", "SETTLED", "ALL"];

function parseAsOfDate(value) {
  return parseDateOnly(value, "asOfDate", new Date().toISOString().slice(0, 10));
}

function parseOptionalRoleFilter(value) {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!normalized) {
    return null;
  }
  return normalizeEnum(normalized, "role", ROLE_FILTER_VALUES);
}

function parseOptionalOpenStatusFilter(value, fallback = null) {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!normalized) {
    return fallback;
  }
  return normalizeEnum(normalized, "status", OPEN_STATUS_FILTER_VALUES);
}

function parseOptionalDirection(value) {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!normalized) {
    return null;
  }
  return normalizeEnum(normalized, "direction", DIRECTION_VALUES);
}

function parseOptionalTextFilter(value, label, maxLength) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const normalized = String(value).trim();
  if (!normalized) {
    return null;
  }
  if (normalized.length > maxLength) {
    throw badRequest(`${label} cannot exceed ${maxLength} characters`);
  }
  return normalized;
}

function parseOptionalDateTimeFilter(value, label) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw badRequest(`${label} must be a valid datetime`);
  }
  return parsed.toISOString().slice(0, 19).replace("T", " ");
}

function parsePositivePagination(query, defaults = {}) {
  const pagination = parsePagination(query, defaults);
  return {
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

function parseCommonReportFilters(req, options = {}) {
  const {
    defaultStatus = null,
    paginationDefaults = { limit: 200, offset: 0, maxLimit: 500 },
  } = options;

  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const counterpartyId = optionalPositiveInt(req.query?.counterpartyId, "counterpartyId");
  const asOfDate = parseAsOfDate(req.query?.asOfDate || req.query?.as_of_date);
  const role = parseOptionalRoleFilter(req.query?.role || req.query?.customerVendor);
  const status = parseOptionalOpenStatusFilter(req.query?.status, defaultStatus);
  const direction = parseOptionalDirection(req.query?.direction);
  const includeDetails = parseBooleanFlag(req.query?.includeDetails, true);
  const pagination = parsePositivePagination(req.query, paginationDefaults);

  return {
    tenantId,
    legalEntityId,
    counterpartyId,
    asOfDate,
    role,
    status,
    direction,
    includeDetails,
    ...pagination,
  };
}

export function parseAgingReportFilters(req, { fixedDirection = null } = {}) {
  const filters = parseCommonReportFilters(req, {
    defaultStatus: "OPEN",
    paginationDefaults: { limit: 500, offset: 0, maxLimit: 2_000 },
  });

  if (fixedDirection) {
    filters.direction = normalizeEnum(fixedDirection, "direction", DIRECTION_VALUES);
  }
  if (!filters.direction) {
    throw badRequest("direction is required for aging report");
  }

  return filters;
}

export function parseOpenItemsReportFilters(req) {
  return parseCommonReportFilters(req, {
    defaultStatus: "OPEN",
    paginationDefaults: { limit: 200, offset: 0, maxLimit: 1_000 },
  });
}

export function parseCounterpartyStatementFilters(req) {
  return parseCommonReportFilters(req, {
    defaultStatus: "ALL",
    paginationDefaults: { limit: 1_000, offset: 0, maxLimit: 5_000 },
  });
}

export function parseGenericAgingReportFilters(req) {
  return parseAgingReportFilters(req, {
    fixedDirection: parseOptionalDirection(req.query?.direction),
  });
}

export function parseCariAuditFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const actorUserId = optionalPositiveInt(req.query?.actorUserId, "actorUserId");
  const includePayload = parseBooleanFlag(req.query?.includePayload, true);
  const action = parseOptionalTextFilter(req.query?.action, "action", 120);
  const resourceType = parseOptionalTextFilter(req.query?.resourceType, "resourceType", 80);
  const resourceId = parseOptionalTextFilter(req.query?.resourceId, "resourceId", 80);
  const requestId = parseOptionalTextFilter(req.query?.requestId, "requestId", 80);
  const createdFrom = parseOptionalDateTimeFilter(
    req.query?.createdFrom || req.query?.created_from,
    "createdFrom"
  );
  const createdTo = parseOptionalDateTimeFilter(
    req.query?.createdTo || req.query?.created_to,
    "createdTo"
  );
  if (createdFrom && createdTo && createdFrom > createdTo) {
    throw badRequest("createdFrom must be <= createdTo");
  }

  const pagination = parsePositivePagination(req.query, {
    limit: 100,
    offset: 0,
    maxLimit: 500,
  });

  return {
    tenantId,
    legalEntityId,
    action,
    resourceType,
    resourceId,
    actorUserId,
    requestId,
    createdFrom,
    createdTo,
    includePayload,
    ...pagination,
  };
}
