import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeCurrencyCode,
  normalizeText,
  optionalPositiveInt,
  parseDateOnly,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const PAYROLL_RUN_STATUS_VALUES = ["DRAFT", "IMPORTED", "REVIEWED", "FINALIZED"];

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

function normalizeCodeToken(value, label, maxLength, { required = false } = {}) {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!normalized) {
    if (required) {
      throw badRequest(`${label} is required`);
    }
    return null;
  }
  if (normalized.length > maxLength) {
    throw badRequest(`${label} cannot exceed ${maxLength} characters`);
  }
  return normalized;
}

function normalizeEntityCode(value, label = "entityCode") {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!normalized) {
    return null;
  }
  if (normalized.length > 60) {
    throw badRequest(`${label} cannot exceed 60 characters`);
  }
  return normalized;
}

export function parsePayrollRunIdParam(req, label = "runId") {
  const runId = parsePositiveInt(req.params?.runId ?? req.params?.id);
  if (!runId) {
    throw badRequest(`${label} must be a positive integer`);
  }
  return runId;
}

export function parsePayrollRunImportInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const targetRunId = optionalPositiveInt(
    req.body?.targetRunId ?? req.body?.target_run_id,
    "targetRunId"
  );
  const legalEntityId = optionalPositiveInt(
    req.body?.legalEntityId ?? req.body?.legal_entity_id,
    "legalEntityId"
  );
  if (!legalEntityId && !targetRunId) {
    throw badRequest("legalEntityId is required unless targetRunId is provided");
  }
  const providerCode = normalizeCodeToken(
    req.body?.providerCode ?? req.body?.provider_code,
    "providerCode",
    60,
    { required: true }
  );
  const payrollPeriod = parseDateOnly(
    req.body?.payrollPeriod ?? req.body?.payroll_period,
    "payrollPeriod"
  );
  const payDate = parseDateOnly(req.body?.payDate ?? req.body?.pay_date, "payDate");
  const currencyCode = normalizeCurrencyCode(
    req.body?.currencyCode ?? req.body?.currency_code,
    "currencyCode"
  );

  const csvText = String(req.body?.csvText ?? req.body?.csv_text ?? "");
  if (!csvText.trim()) {
    throw badRequest("csvText is required");
  }

  return {
    tenantId,
    userId,
    legalEntityId,
    targetRunId,
    providerCode,
    payrollPeriod,
    payDate,
    currencyCode,
    sourceBatchRef: normalizeText(
      req.body?.sourceBatchRef ?? req.body?.source_batch_ref,
      "sourceBatchRef",
      120
    ),
    originalFilename:
      normalizeText(
        req.body?.originalFilename ?? req.body?.original_filename,
        "originalFilename",
        255
      ) || "payroll.csv",
    csvText,
  };
}

export function parsePayrollRunListFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(
    req.query?.legalEntityId ?? req.query?.legal_entity_id,
    "legalEntityId"
  );
  const entityCode = normalizeEntityCode(req.query?.entityCode ?? req.query?.entity_code);
  const providerCode = normalizeCodeToken(
    req.query?.providerCode ?? req.query?.provider_code,
    "providerCode",
    60
  );
  const payrollPeriod = (() => {
    const raw = req.query?.payrollPeriod ?? req.query?.payroll_period;
    if (raw === undefined || raw === null || raw === "") {
      return null;
    }
    return parseDateOnly(raw, "payrollPeriod");
  })();
  const status = normalizeEnumOrNull(req.query?.status, "status", PAYROLL_RUN_STATUS_VALUES);
  const q = normalizeText(req.query?.q, "q", 120);
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 300 });

  return {
    tenantId,
    legalEntityId,
    entityCode,
    providerCode,
    payrollPeriod,
    status,
    q,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parsePayrollRunLineListFilters(req) {
  const tenantId = requireTenantId(req);
  const runId = parsePayrollRunIdParam(req);
  const q = normalizeText(req.query?.q, "q", 120);
  const costCenterCode = normalizeEntityCode(
    req.query?.costCenterCode ?? req.query?.cost_center_code,
    "costCenterCode"
  );
  const pagination = parsePagination(req.query, { limit: 200, offset: 0, maxLimit: 500 });

  return {
    tenantId,
    runId,
    q,
    costCenterCode,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}
