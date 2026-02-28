import { badRequest } from "./_utils.js";
import {
  normalizeText,
  optionalPositiveInt,
  parsePagination,
  requirePositiveInt,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";
import { parsePayrollRunIdParam } from "./payroll.runs.validators.js";

const LIABILITY_SCOPE_VALUES = ["NET_PAY", "STATUTORY", "ALL"];
const LIABILITY_STATUS_VALUES = ["OPEN", "IN_BATCH", "PARTIALLY_PAID", "PAID", "CANCELLED"];
const LIABILITY_TYPE_VALUES = [
  "NET_PAY",
  "EMPLOYEE_TAX",
  "EMPLOYEE_SOCIAL_SECURITY",
  "EMPLOYER_TAX",
  "EMPLOYER_SOCIAL_SECURITY",
  "OTHER_DEDUCTIONS",
];

function normalizeEnumOrNull(value, label, allowedValues) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const normalized = String(value).trim().toUpperCase();
  if (!allowedValues.includes(normalized)) {
    throw badRequest(`${label} must be one of ${allowedValues.join(", ")}`);
  }
  return normalized;
}

function normalizeScope(value, label = "scope", fallback = null) {
  return normalizeEnumOrNull(value ?? fallback, label, LIABILITY_SCOPE_VALUES) || fallback;
}

export function parsePayrollLiabilityListFilters(req) {
  const tenantId = requireTenantId(req);
  const runId = optionalPositiveInt(req.query?.runId ?? req.query?.run_id, "runId");
  const legalEntityId = optionalPositiveInt(
    req.query?.legalEntityId ?? req.query?.legal_entity_id,
    "legalEntityId"
  );
  const status = normalizeEnumOrNull(req.query?.status, "status", LIABILITY_STATUS_VALUES);
  const liabilityType = normalizeEnumOrNull(
    req.query?.liabilityType ?? req.query?.liability_type,
    "liabilityType",
    LIABILITY_TYPE_VALUES
  );
  const scope = normalizeScope(req.query?.scope, "scope", null);
  const q = normalizeText(req.query?.q, "q", 120);
  const cursor = normalizeText(req.query?.cursor, "cursor", 1200) || null;
  const pagination = parsePagination(req.query, { limit: 200, offset: 0, maxLimit: 500 });

  return {
    tenantId,
    runId,
    legalEntityId,
    status,
    liabilityType,
    scope,
    q,
    cursor,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parsePayrollRunLiabilityBuildInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    runId: parsePayrollRunIdParam(req),
    note: normalizeText(req.body?.note, "note", 500),
  };
}

export function parsePayrollRunLiabilityReadInput(req) {
  return {
    tenantId: requireTenantId(req),
    runId: parsePayrollRunIdParam(req),
  };
}

export function parsePayrollRunPaymentBatchPreviewInput(req) {
  return {
    tenantId: requireTenantId(req),
    runId: parsePayrollRunIdParam(req),
    scope: normalizeScope(req.query?.scope, "scope", "NET_PAY") || "NET_PAY",
  };
}

export function parsePayrollRunCreatePaymentBatchInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    runId: parsePayrollRunIdParam(req),
    scope: normalizeScope(req.body?.scope, "scope", "NET_PAY") || "NET_PAY",
    bankAccountId: requirePositiveInt(
      req.body?.bankAccountId ?? req.body?.bank_account_id,
      "bankAccountId"
    ),
    idempotencyKey: normalizeText(
      req.body?.idempotencyKey ?? req.body?.idempotency_key,
      "idempotencyKey",
      120
    ),
    notes: normalizeText(req.body?.notes, "notes", 500),
  };
}
