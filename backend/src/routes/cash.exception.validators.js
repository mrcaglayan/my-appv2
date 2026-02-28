import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  optionalPositiveInt,
  parseDateOnly,
  parsePagination,
  requireTenantId,
} from "./cash.validators.common.js";

const EXCEPTION_TYPES = new Set([
  "HIGH_VARIANCE",
  "FORCED_CLOSE",
  "OVERRIDE_USAGE",
  "UNPOSTED",
  "GL_CASH_CONTROL_WARN",
  "GL_CASH_CONTROL_OVERRIDE",
  "GL_CASH_CONTROL",
]);

function parseMinAbsVariance(value) {
  if (value === undefined || value === null || value === "") {
    return 0;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw badRequest("minAbsVariance must be a non-negative number");
  }
  return Number(parsed.toFixed(6));
}

function parseExceptionTypes(rawValue) {
  if (rawValue === undefined || rawValue === null || rawValue === "") {
    return [];
  }

  const chunks = Array.isArray(rawValue)
    ? rawValue
    : String(rawValue)
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);

  const normalized = new Set();
  for (const chunk of chunks) {
    const value = String(chunk || "")
      .trim()
      .toUpperCase();
    if (!value) {
      continue;
    }
    if (!EXCEPTION_TYPES.has(value)) {
      throw badRequest(
        "type must be one of HIGH_VARIANCE, FORCED_CLOSE, OVERRIDE_USAGE, UNPOSTED, GL_CASH_CONTROL_WARN, GL_CASH_CONTROL_OVERRIDE, GL_CASH_CONTROL"
      );
    }
    if (value === "GL_CASH_CONTROL") {
      normalized.add("GL_CASH_CONTROL_WARN");
      normalized.add("GL_CASH_CONTROL_OVERRIDE");
      continue;
    }
    normalized.add(value);
  }
  return Array.from(normalized);
}

export function parseCashExceptionReadFilters(req) {
  const tenantId = requireTenantId(req);
  const registerId = optionalPositiveInt(req.query?.registerId, "registerId");
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const operatingUnitId = optionalPositiveInt(req.query?.operatingUnitId, "operatingUnitId");
  const fromDate = req.query?.fromDate ? parseDateOnly(req.query?.fromDate, "fromDate") : null;
  const toDate = req.query?.toDate ? parseDateOnly(req.query?.toDate, "toDate") : null;
  const minAbsVariance = parseMinAbsVariance(req.query?.minAbsVariance);
  const types = parseExceptionTypes(req.query?.type);
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });

  if (fromDate && toDate && fromDate > toDate) {
    throw badRequest("fromDate cannot be after toDate");
  }

  const includeRows = String(req.query?.includeRows || "true")
    .trim()
    .toLowerCase();

  return {
    tenantId,
    registerId,
    legalEntityId,
    operatingUnitId,
    fromDate,
    toDate,
    minAbsVariance,
    types,
    limit: pagination.limit,
    offset: pagination.offset,
    includeRows: includeRows !== "false",
  };
}

export function parseCashExceptionRouteScopeInput(req) {
  return {
    legalEntityId: parsePositiveInt(req.query?.legalEntityId),
    registerId: parsePositiveInt(req.query?.registerId),
  };
}
