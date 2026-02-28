import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeCode,
  normalizeCurrencyCode,
  normalizeEnum,
  normalizeText,
  optionalPositiveInt,
  parseAmount,
  parseBooleanFlag,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const REGISTER_TYPES = ["VAULT", "DRAWER", "TILL"];
const SESSION_MODES = ["REQUIRED", "OPTIONAL", "NONE"];
const REGISTER_STATUSES = ["ACTIVE", "INACTIVE"];

export function parseCashRegisterIdParam(req) {
  const registerId = parsePositiveInt(req.params?.registerId);
  if (!registerId) {
    throw badRequest("registerId must be a positive integer");
  }
  return registerId;
}

export function parseCashRegisterReadFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const operatingUnitId = optionalPositiveInt(req.query?.operatingUnitId, "operatingUnitId");
  const statusRaw = String(req.query?.status || "")
    .trim()
    .toUpperCase();
  const status = statusRaw ? normalizeEnum(statusRaw, "status", REGISTER_STATUSES) : null;
  const q = normalizeText(req.query?.q, "q", 120);
  const pagination = parsePagination(req.query, { limit: 50, offset: 0, maxLimit: 200 });

  return {
    tenantId,
    legalEntityId,
    operatingUnitId,
    status,
    q,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseCashRegisterUpsertInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const id = optionalPositiveInt(req.body?.id, "id");
  const legalEntityId = optionalPositiveInt(req.body?.legalEntityId, "legalEntityId");
  const operatingUnitId = optionalPositiveInt(req.body?.operatingUnitId, "operatingUnitId");
  const accountId = optionalPositiveInt(req.body?.accountId, "accountId");
  const varianceGainAccountId = optionalPositiveInt(
    req.body?.varianceGainAccountId,
    "varianceGainAccountId"
  );
  const varianceLossAccountId = optionalPositiveInt(
    req.body?.varianceLossAccountId,
    "varianceLossAccountId"
  );

  if (!legalEntityId || !accountId) {
    throw badRequest("legalEntityId and accountId are required");
  }

  const code = normalizeCode(req.body?.code, "code", 60);
  const name = normalizeText(req.body?.name, "name", 255, { required: true });
  const registerType = normalizeEnum(
    req.body?.registerType,
    "registerType",
    REGISTER_TYPES,
    "DRAWER"
  );
  const sessionMode = normalizeEnum(
    req.body?.sessionMode,
    "sessionMode",
    SESSION_MODES,
    "REQUIRED"
  );
  const status = normalizeEnum(req.body?.status, "status", REGISTER_STATUSES, "ACTIVE");
  const currencyCode = normalizeCurrencyCode(req.body?.currencyCode);
  const allowNegative = parseBooleanFlag(req.body?.allowNegative, false);
  const maxTxnAmount = parseAmount(req.body?.maxTxnAmount, "maxTxnAmount");
  const requiresApprovalOverAmount = parseAmount(
    req.body?.requiresApprovalOverAmount,
    "requiresApprovalOverAmount",
    { allowZero: true }
  );

  if (
    maxTxnAmount !== null &&
    requiresApprovalOverAmount !== null &&
    Number(requiresApprovalOverAmount) > Number(maxTxnAmount)
  ) {
    throw badRequest("requiresApprovalOverAmount cannot exceed maxTxnAmount");
  }

  return {
    tenantId,
    userId,
    id,
    legalEntityId,
    operatingUnitId,
    accountId,
    code,
    name,
    registerType,
    sessionMode,
    currencyCode,
    status,
    allowNegative,
    varianceGainAccountId,
    varianceLossAccountId,
    maxTxnAmount,
    requiresApprovalOverAmount,
  };
}

export function parseCashRegisterStatusUpdateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const registerId = parseCashRegisterIdParam(req);
  const status = normalizeEnum(req.body?.status, "status", REGISTER_STATUSES);

  return {
    tenantId,
    userId,
    registerId,
    status,
  };
}
