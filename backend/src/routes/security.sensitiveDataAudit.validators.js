import { badRequest } from "./_utils.js";
import {
  normalizeText,
  optionalPositiveInt,
  parseDateOnly,
  parsePagination,
  requireTenantId,
} from "./cash.validators.common.js";

function normalizeToken(value, label, maxLength) {
  if (value === undefined || value === null || value === "") return null;
  const normalized = String(value).trim().toUpperCase();
  if (!normalized) return null;
  if (normalized.length > maxLength) {
    throw badRequest(`${label} cannot exceed ${maxLength} characters`);
  }
  return normalized;
}

export function parseSensitiveDataAuditListInput(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(
    req.query?.legalEntityId ?? req.query?.legal_entity_id,
    "legalEntityId"
  );
  const moduleCode = normalizeToken(
    req.query?.moduleCode ?? req.query?.module_code,
    "moduleCode",
    40
  );
  const objectType = normalizeToken(
    req.query?.objectType ?? req.query?.object_type,
    "objectType",
    60
  );
  const action = normalizeToken(req.query?.action, "action", 30);
  const dateFromRaw = req.query?.dateFrom ?? req.query?.date_from;
  const dateToRaw = req.query?.dateTo ?? req.query?.date_to;
  const dateFrom = dateFromRaw ? parseDateOnly(dateFromRaw, "dateFrom") : null;
  const dateTo = dateToRaw ? parseDateOnly(dateToRaw, "dateTo") : null;
  if (dateFrom && dateTo && dateFrom > dateTo) {
    throw badRequest("dateTo must be >= dateFrom");
  }
  const q = normalizeText(req.query?.q, "q", 120);
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 300 });

  return {
    tenantId,
    legalEntityId,
    moduleCode,
    objectType,
    action,
    dateFrom,
    dateTo,
    q,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

