import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeCurrencyCode,
  normalizeEnum,
  normalizeText,
  optionalPositiveInt,
  parseAmount,
  parseDateOnly,
  parsePagination,
  requirePositiveInt,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const PAYMENT_BATCH_STATUS_VALUES = [
  "DRAFT",
  "APPROVED",
  "EXPORTED",
  "POSTED",
  "FAILED",
  "CANCELLED",
];
const PAYMENT_SOURCE_TYPE_VALUES = ["PAYROLL", "AP", "TAX", "MANUAL"];
const EXPORT_FORMAT_VALUES = ["CSV"];

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

function normalizeUpperToken(value, label, maxLength, { required = false } = {}) {
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

export function parsePaymentBatchIdParam(req) {
  const batchId = parsePositiveInt(req.params?.batchId);
  if (!batchId) {
    throw badRequest("batchId must be a positive integer");
  }
  return batchId;
}

export function parsePaymentBatchListFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(
    req.query?.legalEntityId ?? req.query?.legal_entity_id,
    "legalEntityId"
  );
  const bankAccountId = optionalPositiveInt(
    req.query?.bankAccountId ?? req.query?.bank_account_id,
    "bankAccountId"
  );
  const status = normalizeEnumOrNull(req.query?.status, "status", PAYMENT_BATCH_STATUS_VALUES);
  const sourceType = normalizeEnumOrNull(
    req.query?.sourceType ?? req.query?.source_type,
    "sourceType",
    PAYMENT_SOURCE_TYPE_VALUES
  );
  const sourceId = optionalPositiveInt(req.query?.sourceId ?? req.query?.source_id, "sourceId");
  const q = normalizeText(req.query?.q, "q", 120);
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 300 });

  return {
    tenantId,
    legalEntityId,
    bankAccountId,
    status,
    sourceType,
    sourceId,
    q,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

function parseBatchLines(bodyLines) {
  if (!Array.isArray(bodyLines) || bodyLines.length === 0) {
    throw badRequest("lines must be a non-empty array");
  }
  if (bodyLines.length > 1000) {
    throw badRequest("lines cannot exceed 1000 rows");
  }

  return bodyLines.map((line, index) => {
    const label = `lines[${index}]`;
    const beneficiaryType = normalizeUpperToken(
      line?.beneficiaryType ?? line?.beneficiary_type,
      `${label}.beneficiaryType`,
      30,
      { required: true }
    );
    const payableEntityType = normalizeUpperToken(
      line?.payableEntityType ?? line?.payable_entity_type,
      `${label}.payableEntityType`,
      40,
      { required: true }
    );
    const beneficiaryName = normalizeText(
      line?.beneficiaryName ?? line?.beneficiary_name,
      `${label}.beneficiaryName`,
      255,
      { required: true }
    );

    return {
      beneficiaryType,
      beneficiaryId: optionalPositiveInt(
        line?.beneficiaryId ?? line?.beneficiary_id,
        `${label}.beneficiaryId`
      ),
      beneficiaryName,
      beneficiaryBankRef: normalizeText(
        line?.beneficiaryBankRef ?? line?.beneficiary_bank_ref,
        `${label}.beneficiaryBankRef`,
        255
      ),
      payableEntityType,
      payableEntityId: optionalPositiveInt(
        line?.payableEntityId ?? line?.payable_entity_id,
        `${label}.payableEntityId`
      ),
      payableGlAccountId: requirePositiveInt(
        line?.payableGlAccountId ?? line?.payable_gl_account_id,
        `${label}.payableGlAccountId`
      ),
      payableRef: normalizeText(line?.payableRef ?? line?.payable_ref, `${label}.payableRef`, 120),
      amount: parseAmount(line?.amount, `${label}.amount`, { required: true }),
      notes: normalizeText(line?.notes, `${label}.notes`, 500),
    };
  });
}

export function parsePaymentBatchCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);

  const sourceType = normalizeEnum(
    req.body?.sourceType ?? req.body?.source_type,
    "sourceType",
    PAYMENT_SOURCE_TYPE_VALUES
  );

  return {
    tenantId,
    userId,
    sourceType,
    sourceId: optionalPositiveInt(req.body?.sourceId ?? req.body?.source_id, "sourceId"),
    bankAccountId: requirePositiveInt(
      req.body?.bankAccountId ?? req.body?.bank_account_id,
      "bankAccountId"
    ),
    currencyCode: normalizeCurrencyCode(
      req.body?.currencyCode ?? req.body?.currency_code,
      "currencyCode"
    ),
    idempotencyKey: normalizeText(
      req.body?.idempotencyKey ?? req.body?.idempotency_key,
      "idempotencyKey",
      120
    ),
    notes: normalizeText(req.body?.notes, "notes", 500),
    lines: parseBatchLines(req.body?.lines),
  };
}

export function parsePaymentBatchApproveInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const batchId = parsePaymentBatchIdParam(req);
  return {
    tenantId,
    userId,
    batchId,
    note: normalizeText(req.body?.note, "note", 500),
  };
}

export function parsePaymentBatchExportInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const batchId = parsePaymentBatchIdParam(req);
  return {
    tenantId,
    userId,
    batchId,
    format: normalizeEnum(
      req.body?.format || "CSV",
      "format",
      EXPORT_FORMAT_VALUES
    ),
  };
}

export function parsePaymentBatchPostInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const batchId = parsePaymentBatchIdParam(req);
  const rawPostingDate = req.body?.postingDate ?? req.body?.posting_date;

  return {
    tenantId,
    userId,
    batchId,
    note: normalizeText(req.body?.note, "note", 500),
    externalPaymentRefPrefix: normalizeText(
      req.body?.externalPaymentRefPrefix ?? req.body?.external_payment_ref_prefix,
      "externalPaymentRefPrefix",
      60
    ),
    postingDate:
      rawPostingDate === undefined || rawPostingDate === null || rawPostingDate === ""
        ? null
        : parseDateOnly(rawPostingDate, "postingDate"),
  };
}

export function parsePaymentBatchCancelInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const batchId = parsePaymentBatchIdParam(req);
  return {
    tenantId,
    userId,
    batchId,
    reason: normalizeText(req.body?.reason, "reason", 500),
  };
}

