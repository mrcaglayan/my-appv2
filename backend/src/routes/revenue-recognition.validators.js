import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeCurrencyCode,
  normalizeEnum,
  normalizeText,
  optionalPositiveInt,
  parseAmount,
  parseBooleanFlag,
  parseDateOnly,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const ACCOUNT_FAMILY_VALUES = [
  "DEFREV",
  "ACCRUED_REVENUE",
  "ACCRUED_EXPENSE",
  "PREPAID_EXPENSE",
];
const ACCRUAL_ACCOUNT_FAMILY_VALUES = ["ACCRUED_REVENUE", "ACCRUED_EXPENSE"];

const MATURITY_BUCKET_VALUES = ["SHORT_TERM", "LONG_TERM"];
const RUN_STATUS_VALUES = ["DRAFT", "READY", "POSTED", "REVERSED"];
const SCHEDULE_STATUS_VALUES = ["DRAFT", "READY", "POSTED", "REVERSED"];

function parseRequiredPositiveIntField(value, label) {
  const parsed = optionalPositiveInt(value, label);
  if (!parsed) {
    throw badRequest(`${label} is required`);
  }
  return parsed;
}

function parseOptionalEnum(value, label, allowedValues) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  return normalizeEnum(value, label, allowedValues);
}

function parseOptionalDate(value, label) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  return parseDateOnly(value, label);
}

function parseOptionalFxRate(value, label = "fxRate") {
  if (value === undefined || value === null || value === "") {
    return null;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw badRequest(`${label} must be > 0`);
  }
  return parsed.toFixed(10);
}

function parseOptionalSourceUid(value, label) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  return normalizeText(value, label, 160, { required: true });
}

function parseRequiredDate(value, label) {
  return parseDateOnly(value, label);
}

export function parseRevenueScheduleListFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const fiscalPeriodId = optionalPositiveInt(req.query?.fiscalPeriodId, "fiscalPeriodId");
  const accountFamily = parseOptionalEnum(
    req.query?.accountFamily,
    "accountFamily",
    ACCOUNT_FAMILY_VALUES
  );
  const status = parseOptionalEnum(req.query?.status, "status", SCHEDULE_STATUS_VALUES);
  const q = normalizeText(req.query?.q, "q", 120);
  const pagination = parsePagination(req.query, {
    limit: 100,
    offset: 0,
    maxLimit: 300,
  });

  return {
    tenantId,
    legalEntityId,
    fiscalPeriodId,
    accountFamily,
    status,
    q,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseRevenueRunListFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const fiscalPeriodId = optionalPositiveInt(req.query?.fiscalPeriodId, "fiscalPeriodId");
  const accountFamily = parseOptionalEnum(
    req.query?.accountFamily,
    "accountFamily",
    ACCOUNT_FAMILY_VALUES
  );
  const status = parseOptionalEnum(req.query?.status, "status", RUN_STATUS_VALUES);
  const q = normalizeText(req.query?.q, "q", 120);
  const pagination = parsePagination(req.query, {
    limit: 100,
    offset: 0,
    maxLimit: 300,
  });

  return {
    tenantId,
    legalEntityId,
    fiscalPeriodId,
    accountFamily,
    status,
    q,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseRevenueScheduleGenerateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const legalEntityId = parseRequiredPositiveIntField(req.body?.legalEntityId, "legalEntityId");
  const fiscalPeriodId = parseRequiredPositiveIntField(
    req.body?.fiscalPeriodId,
    "fiscalPeriodId"
  );
  const accountFamily = normalizeEnum(
    req.body?.accountFamily,
    "accountFamily",
    ACCOUNT_FAMILY_VALUES
  );
  const maturityBucket = normalizeEnum(
    req.body?.maturityBucket,
    "maturityBucket",
    MATURITY_BUCKET_VALUES
  );
  const maturityDate = parseRequiredDate(req.body?.maturityDate, "maturityDate");
  const reclassRequired = parseBooleanFlag(req.body?.reclassRequired, false);
  const currencyCode = normalizeCurrencyCode(req.body?.currencyCode, "currencyCode");
  const fxRate = parseOptionalFxRate(req.body?.fxRate, "fxRate");
  const amountTxn = parseAmount(req.body?.amountTxn, "amountTxn", {
    required: true,
    allowZero: true,
  });
  const amountBase = parseAmount(req.body?.amountBase, "amountBase", {
    required: true,
    allowZero: true,
  });
  const sourceEventUid = parseOptionalSourceUid(req.body?.sourceEventUid, "sourceEventUid");

  return {
    tenantId,
    userId,
    legalEntityId,
    fiscalPeriodId,
    accountFamily,
    maturityBucket,
    maturityDate,
    reclassRequired,
    currencyCode,
    fxRate,
    amountTxn,
    amountBase,
    sourceEventUid,
  };
}

export function parseRevenueRunCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const legalEntityId = parseRequiredPositiveIntField(req.body?.legalEntityId, "legalEntityId");
  const fiscalPeriodId = parseRequiredPositiveIntField(
    req.body?.fiscalPeriodId,
    "fiscalPeriodId"
  );
  const scheduleId = optionalPositiveInt(req.body?.scheduleId, "scheduleId");
  const runNo = normalizeText(req.body?.runNo, "runNo", 80);
  const sourceRunUid = parseOptionalSourceUid(req.body?.sourceRunUid, "sourceRunUid");
  const accountFamily = normalizeEnum(
    req.body?.accountFamily,
    "accountFamily",
    ACCOUNT_FAMILY_VALUES
  );
  const maturityBucket = normalizeEnum(
    req.body?.maturityBucket,
    "maturityBucket",
    MATURITY_BUCKET_VALUES
  );
  const maturityDate = parseRequiredDate(req.body?.maturityDate, "maturityDate");
  const reclassRequired = parseBooleanFlag(req.body?.reclassRequired, false);
  const currencyCode = normalizeCurrencyCode(req.body?.currencyCode, "currencyCode");
  const fxRate = parseOptionalFxRate(req.body?.fxRate, "fxRate");
  const totalAmountTxn = parseAmount(req.body?.totalAmountTxn, "totalAmountTxn", {
    required: true,
    allowZero: true,
  });
  const totalAmountBase = parseAmount(req.body?.totalAmountBase, "totalAmountBase", {
    required: true,
    allowZero: true,
  });

  return {
    tenantId,
    userId,
    legalEntityId,
    fiscalPeriodId,
    scheduleId,
    runNo,
    sourceRunUid,
    accountFamily,
    maturityBucket,
    maturityDate,
    reclassRequired,
    currencyCode,
    fxRate,
    totalAmountTxn,
    totalAmountBase,
  };
}

export function parseRevenueRunActionInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const runId = parsePositiveInt(req.params?.runId);
  if (!runId) {
    throw badRequest("runId must be a positive integer");
  }

  return {
    tenantId,
    userId,
    runId,
    settlementPeriodId: optionalPositiveInt(req.body?.settlementPeriodId, "settlementPeriodId"),
    reversalPeriodId: optionalPositiveInt(req.body?.reversalPeriodId, "reversalPeriodId"),
    reason: normalizeText(req.body?.reason, "reason", 255),
  };
}

export function parseRevenueAccrualGenerateInput(req) {
  const payload = parseRevenueRunCreateInput(req);
  if (!ACCRUAL_ACCOUNT_FAMILY_VALUES.includes(payload.accountFamily)) {
    throw badRequest(
      `accountFamily must be one of ${ACCRUAL_ACCOUNT_FAMILY_VALUES.join(", ")} for accrual generation`
    );
  }
  return payload;
}

export function parseRevenueAccrualActionInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const accrualId = parsePositiveInt(req.params?.accrualId);
  if (!accrualId) {
    throw badRequest("accrualId must be a positive integer");
  }

  return {
    tenantId,
    userId,
    accrualId,
    runId: accrualId,
    settlementPeriodId: optionalPositiveInt(req.body?.settlementPeriodId, "settlementPeriodId"),
    reversalPeriodId: optionalPositiveInt(req.body?.reversalPeriodId, "reversalPeriodId"),
    reason: normalizeText(req.body?.reason, "reason", 255),
  };
}

export function parseRevenueReportFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const fiscalPeriodId = optionalPositiveInt(req.query?.fiscalPeriodId, "fiscalPeriodId");
  const accountFamily = parseOptionalEnum(
    req.query?.accountFamily,
    "accountFamily",
    ACCOUNT_FAMILY_VALUES
  );
  const asOfDate = parseOptionalDate(req.query?.asOfDate, "asOfDate");
  const pagination = parsePagination(req.query, {
    limit: 100,
    offset: 0,
    maxLimit: 300,
  });

  return {
    tenantId,
    legalEntityId,
    fiscalPeriodId,
    accountFamily,
    asOfDate,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}
