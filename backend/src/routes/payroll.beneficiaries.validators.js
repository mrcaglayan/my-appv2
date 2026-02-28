import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeCurrencyCode,
  normalizeText,
  parseBooleanFlag,
  parseDateOnly,
  requireTenantId,
  requireUserId,
  requirePositiveInt,
} from "./cash.validators.common.js";

const ACCOUNT_STATUS_VALUES = ["ACTIVE", "INACTIVE"];
const VERIFICATION_STATUS_VALUES = ["UNVERIFIED", "VERIFIED"];
const SOURCE_TYPE_VALUES = ["MANUAL", "IMPORT", "PROVIDER"];

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function normalizeEmployeeCode(value, label = "employeeCode") {
  const normalized = normalizeUpperText(value);
  if (!normalized) {
    throw badRequest(`${label} is required`);
  }
  if (normalized.length > 100) {
    throw badRequest(`${label} cannot exceed 100 characters`);
  }
  return normalized;
}

function normalizeOptionalEnum(value, label, allowedValues) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const normalized = normalizeUpperText(value);
  if (!allowedValues.includes(normalized)) {
    throw badRequest(`${label} must be one of ${allowedValues.join(", ")}`);
  }
  return normalized;
}

function parseOptionalDateOnly(value, label) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  return parseDateOnly(value, label);
}

function parseBeneficiaryAccountIdParam(req) {
  const accountId = parsePositiveInt(req.params?.accountId ?? req.params?.id);
  if (!accountId) {
    throw badRequest("accountId must be a positive integer");
  }
  return accountId;
}

function parseLiabilityIdParam(req) {
  const liabilityId = parsePositiveInt(req.params?.liabilityId ?? req.params?.id);
  if (!liabilityId) {
    throw badRequest("liabilityId must be a positive integer");
  }
  return liabilityId;
}

export function parsePayrollBeneficiaryAccountListInput(req) {
  const rawCurrencyCode = req.query?.currencyCode ?? req.query?.currency_code;
  return {
    tenantId: requireTenantId(req),
    legalEntityId: requirePositiveInt(
      req.query?.legalEntityId ?? req.query?.legal_entity_id,
      "legalEntityId"
    ),
    employeeCode: normalizeEmployeeCode(
      req.query?.employeeCode ?? req.query?.employee_code,
      "employeeCode"
    ),
    currencyCode: rawCurrencyCode ? normalizeCurrencyCode(rawCurrencyCode, "currencyCode") : null,
    status: normalizeOptionalEnum(req.query?.status, "status", ACCOUNT_STATUS_VALUES),
  };
}

export function parsePayrollBeneficiaryAccountCreateInput(req) {
  const iban = normalizeText(req.body?.iban, "iban", 64);
  const accountNumber = normalizeText(
    req.body?.accountNumber ?? req.body?.account_number,
    "accountNumber",
    64
  );
  if (!iban && !accountNumber) {
    throw badRequest("Either iban or accountNumber is required");
  }

  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    legalEntityId: requirePositiveInt(
      req.body?.legalEntityId ?? req.body?.legal_entity_id,
      "legalEntityId"
    ),
    employeeCode: normalizeEmployeeCode(
      req.body?.employeeCode ?? req.body?.employee_code,
      "employeeCode"
    ),
    employeeName: normalizeText(
      req.body?.employeeName ?? req.body?.employee_name,
      "employeeName",
      255
    ),
    accountHolderName: normalizeText(
      req.body?.accountHolderName ?? req.body?.account_holder_name,
      "accountHolderName",
      190,
      { required: true }
    ),
    bankName: normalizeText(req.body?.bankName ?? req.body?.bank_name, "bankName", 190, {
      required: true,
    }),
    bankBranchName: normalizeText(
      req.body?.bankBranchName ?? req.body?.bank_branch_name,
      "bankBranchName",
      190
    ),
    countryCode: normalizeText(req.body?.countryCode ?? req.body?.country_code, "countryCode", 2)
      ? normalizeUpperText(req.body?.countryCode ?? req.body?.country_code)
      : null,
    currencyCode: normalizeCurrencyCode(
      req.body?.currencyCode ?? req.body?.currency_code,
      "currencyCode"
    ),
    iban,
    accountNumber,
    routingNumber: normalizeText(
      req.body?.routingNumber ?? req.body?.routing_number,
      "routingNumber",
      64
    ),
    swiftBic: normalizeText(req.body?.swiftBic ?? req.body?.swift_bic, "swiftBic", 32)
      ? normalizeUpperText(req.body?.swiftBic ?? req.body?.swift_bic)
      : null,
    isPrimary: parseBooleanFlag(req.body?.isPrimary ?? req.body?.is_primary, false),
    effectiveFrom: parseOptionalDateOnly(
      req.body?.effectiveFrom ?? req.body?.effective_from,
      "effectiveFrom"
    ),
    effectiveTo: parseOptionalDateOnly(req.body?.effectiveTo ?? req.body?.effective_to, "effectiveTo"),
    verificationStatus:
      normalizeOptionalEnum(
        req.body?.verificationStatus ?? req.body?.verification_status,
        "verificationStatus",
        VERIFICATION_STATUS_VALUES
      ) || "UNVERIFIED",
    sourceType:
      normalizeOptionalEnum(req.body?.sourceType ?? req.body?.source_type, "sourceType", SOURCE_TYPE_VALUES) ||
      "MANUAL",
    externalRef: normalizeText(req.body?.externalRef ?? req.body?.external_ref, "externalRef", 190),
    reason: normalizeText(req.body?.reason, "reason", 255),
  };
}

export function parsePayrollBeneficiaryAccountUpdateInput(req) {
  const payload = {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    accountId: parseBeneficiaryAccountIdParam(req),
    employeeName: normalizeText(
      req.body?.employeeName ?? req.body?.employee_name,
      "employeeName",
      255
    ),
    accountHolderName: normalizeText(
      req.body?.accountHolderName ?? req.body?.account_holder_name,
      "accountHolderName",
      190
    ),
    bankName: normalizeText(req.body?.bankName ?? req.body?.bank_name, "bankName", 190),
    bankBranchName: normalizeText(
      req.body?.bankBranchName ?? req.body?.bank_branch_name,
      "bankBranchName",
      190
    ),
    countryCode:
      req.body?.countryCode !== undefined || req.body?.country_code !== undefined
        ? normalizeText(req.body?.countryCode ?? req.body?.country_code, "countryCode", 2)
          ? normalizeUpperText(req.body?.countryCode ?? req.body?.country_code)
          : null
        : undefined,
    currencyCode:
      req.body?.currencyCode !== undefined || req.body?.currency_code !== undefined
        ? normalizeCurrencyCode(req.body?.currencyCode ?? req.body?.currency_code, "currencyCode")
        : null,
    iban:
      req.body?.iban !== undefined ? normalizeText(req.body?.iban, "iban", 64) : undefined,
    accountNumber:
      req.body?.accountNumber !== undefined || req.body?.account_number !== undefined
        ? normalizeText(req.body?.accountNumber ?? req.body?.account_number, "accountNumber", 64)
        : undefined,
    routingNumber:
      req.body?.routingNumber !== undefined || req.body?.routing_number !== undefined
        ? normalizeText(req.body?.routingNumber ?? req.body?.routing_number, "routingNumber", 64)
        : undefined,
    swiftBic:
      req.body?.swiftBic !== undefined || req.body?.swift_bic !== undefined
        ? (normalizeText(req.body?.swiftBic ?? req.body?.swift_bic, "swiftBic", 32)
            ? normalizeUpperText(req.body?.swiftBic ?? req.body?.swift_bic)
            : null)
        : undefined,
    status: normalizeOptionalEnum(req.body?.status, "status", ACCOUNT_STATUS_VALUES),
    verificationStatus: normalizeOptionalEnum(
      req.body?.verificationStatus ?? req.body?.verification_status,
      "verificationStatus",
      VERIFICATION_STATUS_VALUES
    ),
    effectiveFrom:
      req.body?.effectiveFrom !== undefined || req.body?.effective_from !== undefined
        ? parseOptionalDateOnly(req.body?.effectiveFrom ?? req.body?.effective_from, "effectiveFrom")
        : undefined,
    effectiveTo:
      req.body?.effectiveTo !== undefined || req.body?.effective_to !== undefined
        ? parseOptionalDateOnly(req.body?.effectiveTo ?? req.body?.effective_to, "effectiveTo")
        : undefined,
    externalRef:
      req.body?.externalRef !== undefined || req.body?.external_ref !== undefined
        ? normalizeText(req.body?.externalRef ?? req.body?.external_ref, "externalRef", 190)
        : undefined,
    reason: normalizeText(req.body?.reason, "reason", 255) || "Updated payroll beneficiary bank account",
  };

  const updatableKeys = [
    "employeeName",
    "accountHolderName",
    "bankName",
    "bankBranchName",
    "countryCode",
    "currencyCode",
    "iban",
    "accountNumber",
    "routingNumber",
    "swiftBic",
    "status",
    "verificationStatus",
    "effectiveFrom",
    "effectiveTo",
    "externalRef",
  ];
  if (!updatableKeys.some((key) => payload[key] !== undefined && payload[key] !== null)) {
    throw badRequest("At least one updatable field is required");
  }

  return payload;
}

export function parsePayrollBeneficiarySetPrimaryInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    accountId: parseBeneficiaryAccountIdParam(req),
    reason:
      normalizeText(req.body?.reason, "reason", 255) || "Set primary payroll beneficiary bank account",
  };
}

export function parsePayrollLiabilityBeneficiarySnapshotReadInput(req) {
  return {
    tenantId: requireTenantId(req),
    liabilityId: parseLiabilityIdParam(req),
  };
}
