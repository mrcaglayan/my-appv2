import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeCurrencyCode,
  normalizeEnum,
  normalizeText,
  optionalPositiveInt,
  parseBooleanFlag,
  parseAmount,
  parseDateOnly,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const DIRECTION_VALUES = ["AR", "AP"];
const DOCUMENT_TYPE_VALUES = [
  "INVOICE",
  "DEBIT_NOTE",
  "CREDIT_NOTE",
  "PAYMENT",
  "ADJUSTMENT",
];
const DOCUMENT_STATUS_VALUES = [
  "DRAFT",
  "POSTED",
  "REVERSED",
  "CANCELLED",
  "PARTIALLY_SETTLED",
  "SETTLED",
];
const DUE_DATE_REQUIRED_TYPES = new Set(["INVOICE", "DEBIT_NOTE"]);

function parseOptionalDate(value, label) {
  if (value === undefined) {
    return undefined;
  }
  if (value === null || value === "") {
    return null;
  }
  return parseDateOnly(value, label);
}

function parseOptionalFilterDate(value, label) {
  if (value === undefined || value === null) {
    return null;
  }
  const raw = String(value).trim();
  if (!raw) {
    return null;
  }
  return parseDateOnly(raw, label);
}

function pickPrimaryOrAlias(primaryValue, aliasValue) {
  if (primaryValue !== undefined && primaryValue !== null) {
    const normalizedPrimary = String(primaryValue).trim();
    if (normalizedPrimary) {
      return primaryValue;
    }
  }
  return aliasValue;
}

function parseOptionalPositiveIntField(value, label) {
  if (value === undefined) {
    return undefined;
  }
  if (value === null || value === "") {
    return null;
  }
  return optionalPositiveInt(value, label);
}

function parseRequiredPositiveIntField(value, label) {
  const parsed = optionalPositiveInt(value, label);
  if (!parsed) {
    throw badRequest(`${label} is required`);
  }
  return parsed;
}

function parseOptionalDecimal(value, label) {
  if (value === undefined) {
    return undefined;
  }
  if (value === null || value === "") {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw badRequest(`${label} must be a numeric value greater than 0`);
  }
  return parsed.toFixed(10);
}

function parseOptionalShortText(value, label, maxLength = 255) {
  if (value === undefined) {
    return undefined;
  }
  if (value === null) {
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

function parseRequiredAmount(value, label) {
  return parseAmount(value, label, {
    required: true,
    allowZero: false,
  });
}

function parseOptionalAmount(value, label) {
  if (value === undefined) {
    return undefined;
  }
  if (value === null || value === "") {
    return null;
  }
  return parseAmount(value, label, {
    required: true,
    allowZero: false,
  });
}

function assertFrozenTxnType({ direction, documentType }) {
  if (!DIRECTION_VALUES.includes(direction)) {
    throw badRequest(`direction must be one of ${DIRECTION_VALUES.join(", ")}`);
  }
  if (!DOCUMENT_TYPE_VALUES.includes(documentType)) {
    throw badRequest(
      `documentType must be one of ${DOCUMENT_TYPE_VALUES.join(", ")}`
    );
  }
}

function assertDueDateRule({ documentType, dueDate }) {
  if (DUE_DATE_REQUIRED_TYPES.has(documentType) && !dueDate) {
    throw badRequest(`dueDate is required for documentType=${documentType}`);
  }
}

export function parseDocumentIdParam(req) {
  const documentId = parsePositiveInt(req.params?.documentId);
  if (!documentId) {
    throw badRequest("documentId must be a positive integer");
  }
  return documentId;
}

export function parseDocumentReadFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const counterpartyId = optionalPositiveInt(req.query?.counterpartyId, "counterpartyId");
  const q = normalizeText(req.query?.q, "q", 120);
  const dateFrom = parseOptionalFilterDate(
    pickPrimaryOrAlias(req.query?.dateFrom, req.query?.documentDateFrom),
    "dateFrom"
  );
  const dateTo = parseOptionalFilterDate(
    pickPrimaryOrAlias(req.query?.dateTo, req.query?.documentDateTo),
    "dateTo"
  );
  if (dateFrom && dateTo && dateFrom > dateTo) {
    throw badRequest("dateFrom cannot be greater than dateTo");
  }

  const directionRaw = String(req.query?.direction || "")
    .trim()
    .toUpperCase();
  const direction = directionRaw
    ? normalizeEnum(directionRaw, "direction", DIRECTION_VALUES)
    : null;

  const documentTypeRaw = String(req.query?.documentType || "")
    .trim()
    .toUpperCase();
  const documentType = documentTypeRaw
    ? normalizeEnum(documentTypeRaw, "documentType", DOCUMENT_TYPE_VALUES)
    : null;

  const statusRaw = String(req.query?.status || "")
    .trim()
    .toUpperCase();
  const status = statusRaw
    ? normalizeEnum(statusRaw, "status", DOCUMENT_STATUS_VALUES)
    : null;

  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 300 });
  return {
    tenantId,
    legalEntityId,
    counterpartyId,
    dateFrom,
    dateTo,
    q,
    direction,
    documentType,
    status,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseDocumentCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const legalEntityId = parseRequiredPositiveIntField(
    req.body?.legalEntityId,
    "legalEntityId"
  );
  const counterpartyId = parseRequiredPositiveIntField(
    req.body?.counterpartyId,
    "counterpartyId"
  );
  const paymentTermIdInput = parseOptionalPositiveIntField(
    req.body?.paymentTermId,
    "paymentTermId"
  );
  const paymentTermId =
    paymentTermIdInput === undefined ? null : paymentTermIdInput;

  const direction = normalizeEnum(req.body?.direction, "direction", DIRECTION_VALUES);
  const documentType = normalizeEnum(
    req.body?.documentType,
    "documentType",
    DOCUMENT_TYPE_VALUES
  );
  assertFrozenTxnType({ direction, documentType });

  const documentDate = parseDateOnly(req.body?.documentDate, "documentDate");
  const dueDateInput = parseOptionalDate(req.body?.dueDate, "dueDate");
  const dueDate = dueDateInput === undefined ? null : dueDateInput;
  assertDueDateRule({ documentType, dueDate });

  const amountTxn = parseRequiredAmount(req.body?.amountTxn, "amountTxn");
  const amountBase = parseRequiredAmount(req.body?.amountBase, "amountBase");
  const currencyCode = normalizeCurrencyCode(req.body?.currencyCode, "currencyCode");
  const fxRateInput = parseOptionalDecimal(req.body?.fxRate, "fxRate");
  const fxRate = fxRateInput === undefined ? null : fxRateInput;

  return {
    tenantId,
    userId,
    legalEntityId,
    counterpartyId,
    paymentTermId,
    direction,
    documentType,
    documentDate,
    dueDate,
    amountTxn,
    amountBase,
    currencyCode,
    fxRate,
  };
}

export function parseDocumentUpdateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const documentId = parseDocumentIdParam(req);

  const legalEntityId = parseOptionalPositiveIntField(
    req.body?.legalEntityId,
    "legalEntityId"
  );
  const counterpartyId = parseOptionalPositiveIntField(
    req.body?.counterpartyId,
    "counterpartyId"
  );
  const paymentTermId = parseOptionalPositiveIntField(
    req.body?.paymentTermId,
    "paymentTermId"
  );

  const direction =
    req.body?.direction !== undefined
      ? normalizeEnum(req.body?.direction, "direction", DIRECTION_VALUES)
      : undefined;
  const documentType =
    req.body?.documentType !== undefined
      ? normalizeEnum(req.body?.documentType, "documentType", DOCUMENT_TYPE_VALUES)
      : undefined;

  if (direction !== undefined && documentType !== undefined) {
    assertFrozenTxnType({ direction, documentType });
  } else if (direction !== undefined || documentType !== undefined) {
    const missingField = direction === undefined ? "direction" : "documentType";
    throw badRequest(
      `${missingField} is required when updating documentType/direction pair`
    );
  }

  const documentDate =
    req.body?.documentDate !== undefined
      ? parseDateOnly(req.body?.documentDate, "documentDate")
      : undefined;
  const dueDate = parseOptionalDate(req.body?.dueDate, "dueDate");
  const amountTxn = parseOptionalAmount(req.body?.amountTxn, "amountTxn");
  const amountBase = parseOptionalAmount(req.body?.amountBase, "amountBase");
  const currencyCode =
    req.body?.currencyCode !== undefined
      ? normalizeCurrencyCode(req.body?.currencyCode, "currencyCode")
      : undefined;
  const fxRate = parseOptionalDecimal(req.body?.fxRate, "fxRate");

  const hasAnyMutationField =
    legalEntityId !== undefined ||
    counterpartyId !== undefined ||
    paymentTermId !== undefined ||
    direction !== undefined ||
    documentType !== undefined ||
    documentDate !== undefined ||
    dueDate !== undefined ||
    amountTxn !== undefined ||
    amountBase !== undefined ||
    currencyCode !== undefined ||
    fxRate !== undefined;

  if (!hasAnyMutationField) {
    throw badRequest("At least one updatable field is required");
  }

  return {
    tenantId,
    userId,
    documentId,
    legalEntityId,
    counterpartyId,
    paymentTermId,
    direction,
    documentType,
    documentDate,
    dueDate,
    amountTxn,
    amountBase,
    currencyCode,
    fxRate,
  };
}

export function parseDraftCancelInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const documentId = parseDocumentIdParam(req);
  return {
    tenantId,
    userId,
    documentId,
  };
}

export function parseDocumentPostInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const documentId = parseDocumentIdParam(req);
  const useFxOverride = parseBooleanFlag(
    req.body?.useFxOverride ?? req.body?.overrideFxRate,
    false
  );
  const fxOverrideReason = parseOptionalShortText(
    req.body?.fxOverrideReason ?? req.body?.overrideReason,
    "fxOverrideReason",
    500
  );
  if (!useFxOverride && fxOverrideReason) {
    throw badRequest("fxOverrideReason requires useFxOverride=true");
  }

  return {
    tenantId,
    userId,
    documentId,
    useFxOverride,
    fxOverrideReason: fxOverrideReason || null,
  };
}

export function parseDocumentReverseInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const documentId = parseDocumentIdParam(req);
  const reason =
    parseOptionalShortText(req.body?.reason, "reason", 255) || "Manual reversal";
  const reversalDate =
    req.body?.reversalDate === undefined
      ? null
      : parseDateOnly(req.body?.reversalDate, "reversalDate");

  return {
    tenantId,
    userId,
    documentId,
    reason,
    reversalDate,
  };
}
