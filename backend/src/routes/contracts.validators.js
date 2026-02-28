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

const CONTRACT_TYPE_VALUES = ["CUSTOMER", "VENDOR"];
const CONTRACT_STATUS_VALUES = ["DRAFT", "ACTIVE", "SUSPENDED", "CLOSED", "CANCELLED"];
const LINE_STATUS_VALUES = ["ACTIVE", "INACTIVE"];
const RECOGNITION_METHOD_VALUES = ["STRAIGHT_LINE", "MILESTONE", "MANUAL"];
const LINK_TYPE_VALUES = ["BILLING", "ADVANCE", "ADJUSTMENT"];
const BILLING_DOC_TYPE_VALUES = ["INVOICE", "ADVANCE", "ADJUSTMENT"];
const BILLING_STRATEGY_VALUES = ["FULL", "PARTIAL", "MILESTONE"];
const REVREC_GENERATION_MODE_VALUES = ["BY_CONTRACT_LINE", "BY_LINKED_DOCUMENT"];

function parseRequiredPositiveIntField(value, label) {
  const parsed = optionalPositiveInt(value, label);
  if (!parsed) {
    throw badRequest(`${label} is required`);
  }
  return parsed;
}

function parseOptionalDate(value, label) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  return parseDateOnly(value, label);
}

function parseOptionalText(value, label, maxLength = 500) {
  if (value === undefined || value === null) {
    return null;
  }
  const normalized = normalizeText(value, label, maxLength);
  return normalized || null;
}

function parseOptionalPositiveIntField(value, label) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  return optionalPositiveInt(value, label);
}

function parseOptionalFxRate(value, label = "linkFxRate") {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw badRequest(`${label} must be a numeric value greater than 0`);
  }
  return parsed.toFixed(10);
}

function parseOptionalPositiveAmount(value, label) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  return parseAmount(value, label, {
    required: true,
    allowZero: false,
    allowNegative: false,
  });
}

function parsePositiveIntArray(value, label) {
  if (value === undefined || value === null || value === "") {
    return [];
  }
  if (!Array.isArray(value)) {
    throw badRequest(`${label} must be an array`);
  }
  const parsed = value.map((entry, index) =>
    parseRequiredPositiveIntField(entry, `${label}[${index}]`)
  );
  const unique = new Set(parsed);
  if (unique.size !== parsed.length) {
    throw badRequest(`${label} contains duplicate values`);
  }
  return parsed;
}

function hasOwnField(obj, key) {
  return Object.prototype.hasOwnProperty.call(obj || {}, key);
}

function parseLineStatus(value, label) {
  if (value === undefined || value === null || value === "") {
    return "ACTIVE";
  }
  return normalizeEnum(value, label, LINE_STATUS_VALUES);
}

function parseRecognitionMethod(value, label) {
  if (value === undefined || value === null || value === "") {
    return "STRAIGHT_LINE";
  }
  return normalizeEnum(value, label, RECOGNITION_METHOD_VALUES);
}

function assertRecognitionDates({
  recognitionMethod,
  recognitionStartDate,
  recognitionEndDate,
  basePath,
}) {
  const hasStart = Boolean(recognitionStartDate);
  const hasEnd = Boolean(recognitionEndDate);

  if (recognitionMethod === "STRAIGHT_LINE") {
    if (!hasStart || !hasEnd) {
      throw badRequest(
        `${basePath}.recognitionStartDate and ${basePath}.recognitionEndDate are required for STRAIGHT_LINE`
      );
    }
  } else if (recognitionMethod === "MILESTONE") {
    if (!hasStart || !hasEnd) {
      throw badRequest(
        `${basePath}.recognitionStartDate and ${basePath}.recognitionEndDate are required for MILESTONE`
      );
    }
    if (recognitionStartDate !== recognitionEndDate) {
      throw badRequest(
        `${basePath}.recognitionStartDate and ${basePath}.recognitionEndDate must match for MILESTONE`
      );
    }
  } else if (recognitionMethod === "MANUAL") {
    if (hasStart || hasEnd) {
      throw badRequest(
        `${basePath}.recognitionStartDate and ${basePath}.recognitionEndDate must be omitted for MANUAL`
      );
    }
  }

  if (
    recognitionStartDate &&
    recognitionEndDate &&
    recognitionStartDate > recognitionEndDate
  ) {
    throw badRequest(
      `${basePath}.recognitionStartDate cannot be greater than ${basePath}.recognitionEndDate`
    );
  }
}

function parseContractLines(linesInput, label = "lines") {
  if (!Array.isArray(linesInput)) {
    throw badRequest(`${label} must be an array`);
  }

  return linesInput.map((line, index) => {
    const linePath = `${label}[${index}]`;
    const description = normalizeText(
      line?.description,
      `${linePath}.description`,
      255,
      { required: true }
    );
    const lineAmountTxn = parseAmount(line?.lineAmountTxn, `${linePath}.lineAmountTxn`, {
      required: true,
      allowZero: false,
      allowNegative: true,
    });
    const lineAmountBase = parseAmount(line?.lineAmountBase, `${linePath}.lineAmountBase`, {
      required: true,
      allowZero: false,
      allowNegative: true,
    });

    const recognitionMethod = parseRecognitionMethod(
      line?.recognitionMethod,
      `${linePath}.recognitionMethod`
    );
    const recognitionStartDate = parseOptionalDate(
      line?.recognitionStartDate,
      `${linePath}.recognitionStartDate`
    );
    const recognitionEndDate = parseOptionalDate(
      line?.recognitionEndDate,
      `${linePath}.recognitionEndDate`
    );

    assertRecognitionDates({
      recognitionMethod,
      recognitionStartDate,
      recognitionEndDate,
      basePath: linePath,
    });

    return {
      description,
      lineAmountTxn,
      lineAmountBase,
      recognitionMethod,
      recognitionStartDate,
      recognitionEndDate,
      deferredAccountId: parseOptionalPositiveIntField(
        line?.deferredAccountId,
        `${linePath}.deferredAccountId`
      ),
      revenueAccountId: parseOptionalPositiveIntField(
        line?.revenueAccountId,
        `${linePath}.revenueAccountId`
      ),
      status: parseLineStatus(line?.status, `${linePath}.status`),
    };
  });
}

function parseHeaderDates(startDateValue, endDateValue) {
  const startDate = parseDateOnly(startDateValue, "startDate");
  const endDate = parseOptionalDate(endDateValue, "endDate");
  if (endDate && startDate > endDate) {
    throw badRequest("startDate cannot be greater than endDate");
  }
  return { startDate, endDate };
}

function parseContractUpsertInput(req, { includeContractId }) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const contractId = includeContractId
    ? parseRequiredPositiveIntField(req.params?.contractId, "contractId")
    : null;

  const legalEntityId = parseRequiredPositiveIntField(
    req.body?.legalEntityId,
    "legalEntityId"
  );
  const counterpartyId = parseRequiredPositiveIntField(
    req.body?.counterpartyId,
    "counterpartyId"
  );
  const contractNo = normalizeText(req.body?.contractNo, "contractNo", 80, {
    required: true,
  });
  const contractType = normalizeEnum(
    req.body?.contractType,
    "contractType",
    CONTRACT_TYPE_VALUES
  );
  const currencyCode = normalizeCurrencyCode(req.body?.currencyCode, "currencyCode");
  const { startDate, endDate } = parseHeaderDates(req.body?.startDate, req.body?.endDate);
  const notes = parseOptionalText(req.body?.notes, "notes", 500);
  const lines = parseContractLines(req.body?.lines, "lines");

  return {
    tenantId,
    userId,
    contractId,
    legalEntityId,
    counterpartyId,
    contractNo,
    contractType,
    currencyCode,
    startDate,
    endDate,
    notes,
    lines,
  };
}

export function parseContractIdParam(req) {
  const contractId = parsePositiveInt(req.params?.contractId);
  if (!contractId) {
    throw badRequest("contractId must be a positive integer");
  }
  return contractId;
}

export function parseContractListFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const counterpartyId = optionalPositiveInt(req.query?.counterpartyId, "counterpartyId");
  const q = normalizeText(req.query?.q, "q", 120);

  const contractTypeRaw = String(req.query?.contractType || "")
    .trim()
    .toUpperCase();
  const contractType = contractTypeRaw
    ? normalizeEnum(contractTypeRaw, "contractType", CONTRACT_TYPE_VALUES)
    : null;

  const statusRaw = String(req.query?.status || "")
    .trim()
    .toUpperCase();
  const status = statusRaw
    ? normalizeEnum(statusRaw, "status", CONTRACT_STATUS_VALUES)
    : null;

  const pagination = parsePagination(req.query, {
    limit: 100,
    offset: 0,
    maxLimit: 300,
  });

  return {
    tenantId,
    legalEntityId,
    counterpartyId,
    q,
    contractType,
    status,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseContractCreateInput(req) {
  return parseContractUpsertInput(req, { includeContractId: false });
}

export function parseContractUpdateInput(req) {
  return parseContractUpsertInput(req, { includeContractId: true });
}

export function parseContractAmendInput(req) {
  const payload = parseContractUpsertInput(req, { includeContractId: true });
  const reason = normalizeText(req.body?.reason, "reason", 500, {
    required: true,
  });
  return {
    ...payload,
    reason,
  };
}

function parsePatchDateField(value, label) {
  if (value === null || value === "") {
    return null;
  }
  return parseDateOnly(value, label);
}

function parsePatchOptionalAccountField(value, label) {
  if (value === null || value === "") {
    return null;
  }
  return optionalPositiveInt(value, label);
}

export function parseContractLinePatchInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const contractId = parseContractIdParam(req);
  const lineId = parseRequiredPositiveIntField(req.params?.lineId, "lineId");
  const reason = normalizeText(req.body?.reason, "reason", 500, {
    required: true,
  });

  const patch = {};
  const body = req.body || {};

  if (hasOwnField(body, "description")) {
    patch.description = normalizeText(body.description, "description", 255, {
      required: true,
    });
  }
  if (hasOwnField(body, "lineAmountTxn")) {
    patch.lineAmountTxn = parseAmount(body.lineAmountTxn, "lineAmountTxn", {
      required: true,
      allowZero: false,
      allowNegative: true,
    });
  }
  if (hasOwnField(body, "lineAmountBase")) {
    patch.lineAmountBase = parseAmount(body.lineAmountBase, "lineAmountBase", {
      required: true,
      allowZero: false,
      allowNegative: true,
    });
  }
  if (hasOwnField(body, "recognitionMethod")) {
    patch.recognitionMethod = parseRecognitionMethod(body.recognitionMethod, "recognitionMethod");
  }
  if (hasOwnField(body, "recognitionStartDate")) {
    patch.recognitionStartDate = parsePatchDateField(
      body.recognitionStartDate,
      "recognitionStartDate"
    );
  }
  if (hasOwnField(body, "recognitionEndDate")) {
    patch.recognitionEndDate = parsePatchDateField(body.recognitionEndDate, "recognitionEndDate");
  }
  if (hasOwnField(body, "deferredAccountId")) {
    patch.deferredAccountId = parsePatchOptionalAccountField(
      body.deferredAccountId,
      "deferredAccountId"
    );
  }
  if (hasOwnField(body, "revenueAccountId")) {
    patch.revenueAccountId = parsePatchOptionalAccountField(
      body.revenueAccountId,
      "revenueAccountId"
    );
  }
  if (hasOwnField(body, "status")) {
    patch.status = parseLineStatus(body.status, "status");
  }

  if (Object.keys(patch).length === 0) {
    throw badRequest("At least one patch field must be provided");
  }

  return {
    tenantId,
    userId,
    contractId,
    lineId,
    reason,
    patch,
  };
}

export function parseContractAmendmentsListInput(req) {
  const tenantId = requireTenantId(req);
  const contractId = parseContractIdParam(req);
  return {
    tenantId,
    contractId,
  };
}

export function parseContractLinkableDocumentsInput(req) {
  const tenantId = requireTenantId(req);
  const contractId = parseContractIdParam(req);
  const q = normalizeText(req.query?.q, "q", 120);
  const pagination = parsePagination(req.query, {
    limit: 100,
    offset: 0,
    maxLimit: 300,
  });

  return {
    tenantId,
    contractId,
    q,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseContractLifecycleInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const contractId = parseContractIdParam(req);

  return {
    tenantId,
    userId,
    contractId,
  };
}

export function parseContractLinkDocumentInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const contractId = parseContractIdParam(req);
  const cariDocumentId = parseRequiredPositiveIntField(
    req.body?.cariDocumentId,
    "cariDocumentId"
  );
  const linkType = normalizeEnum(req.body?.linkType, "linkType", LINK_TYPE_VALUES);
  const linkedAmountTxn = parseAmount(req.body?.linkedAmountTxn, "linkedAmountTxn", {
    required: true,
    allowZero: false,
  });
  const linkedAmountBase = parseAmount(req.body?.linkedAmountBase, "linkedAmountBase", {
    required: true,
    allowZero: false,
  });
  const linkFxRate = parseOptionalFxRate(req.body?.linkFxRate, "linkFxRate");

  return {
    tenantId,
    userId,
    contractId,
    cariDocumentId,
    linkType,
    linkedAmountTxn,
    linkedAmountBase,
    linkFxRate,
  };
}

export function parseContractLinkAdjustmentInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const contractId = parseContractIdParam(req);
  const linkId = parseRequiredPositiveIntField(req.params?.linkId, "linkId");
  const nextLinkedAmountTxn = parseAmount(
    req.body?.nextLinkedAmountTxn,
    "nextLinkedAmountTxn",
    {
      required: true,
      allowZero: false,
    }
  );
  const nextLinkedAmountBase = parseAmount(
    req.body?.nextLinkedAmountBase,
    "nextLinkedAmountBase",
    {
      required: true,
      allowZero: false,
    }
  );
  const reason = normalizeText(req.body?.reason, "reason", 500, {
    required: true,
  });

  return {
    tenantId,
    userId,
    contractId,
    linkId,
    nextLinkedAmountTxn,
    nextLinkedAmountBase,
    reason,
  };
}

export function parseContractLinkUnlinkInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const contractId = parseContractIdParam(req);
  const linkId = parseRequiredPositiveIntField(req.params?.linkId, "linkId");
  const reason = normalizeText(req.body?.reason, "reason", 500, {
    required: true,
  });

  return {
    tenantId,
    userId,
    contractId,
    linkId,
    reason,
  };
}

export function parseContractGenerateBillingInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const contractId = parseContractIdParam(req);

  const docType = normalizeEnum(req.body?.docType, "docType", BILLING_DOC_TYPE_VALUES);
  const amountStrategy = normalizeEnum(
    req.body?.amountStrategy || "FULL",
    "amountStrategy",
    BILLING_STRATEGY_VALUES
  );
  const billingDate = parseDateOnly(req.body?.billingDate, "billingDate");
  const dueDate = parseOptionalDate(req.body?.dueDate, "dueDate");
  if (dueDate && dueDate < billingDate) {
    throw badRequest("dueDate cannot be earlier than billingDate");
  }

  const selectedLineIds = parsePositiveIntArray(req.body?.selectedLineIds, "selectedLineIds");
  const amountTxn = parseOptionalPositiveAmount(req.body?.amountTxn, "amountTxn");
  const amountBase = parseOptionalPositiveAmount(req.body?.amountBase, "amountBase");
  if ((amountTxn === null) !== (amountBase === null)) {
    throw badRequest("amountTxn and amountBase must be provided together");
  }
  if (amountStrategy === "FULL" && (amountTxn !== null || amountBase !== null)) {
    throw badRequest("amountTxn and amountBase must be omitted for FULL strategy");
  }
  if (amountStrategy !== "FULL" && (amountTxn === null || amountBase === null)) {
    throw badRequest("amountTxn and amountBase are required for PARTIAL/MILESTONE strategies");
  }

  const idempotencyKey = normalizeText(req.body?.idempotencyKey, "idempotencyKey", 100, {
    required: true,
  });
  const integrationEventUid = parseOptionalText(
    req.body?.integrationEventUid,
    "integrationEventUid",
    100
  );
  const referenceNo = parseOptionalText(req.body?.referenceNo, "referenceNo", 100);
  const note = parseOptionalText(req.body?.note, "note", 500);

  return {
    tenantId,
    userId,
    contractId,
    docType,
    amountStrategy,
    billingDate,
    dueDate,
    selectedLineIds,
    amountTxn,
    amountBase,
    idempotencyKey,
    integrationEventUid,
    referenceNo,
    note,
  };
}

export function parseContractGenerateRevrecInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const contractId = parseContractIdParam(req);
  const fiscalPeriodId = parseRequiredPositiveIntField(req.body?.fiscalPeriodId, "fiscalPeriodId");
  const generationMode = normalizeEnum(
    req.body?.generationMode || "BY_CONTRACT_LINE",
    "generationMode",
    REVREC_GENERATION_MODE_VALUES
  );
  const contractLineIds = parsePositiveIntArray(req.body?.contractLineIds, "contractLineIds");
  const sourceCariDocumentId = parseOptionalPositiveIntField(
    req.body?.sourceCariDocumentId,
    "sourceCariDocumentId"
  );
  const regenerateMissingOnly = parseBooleanFlag(req.body?.regenerateMissingOnly, true);

  if (generationMode === "BY_LINKED_DOCUMENT" && !sourceCariDocumentId) {
    throw badRequest("sourceCariDocumentId is required for BY_LINKED_DOCUMENT mode");
  }

  return {
    tenantId,
    userId,
    contractId,
    fiscalPeriodId,
    generationMode,
    contractLineIds,
    sourceCariDocumentId,
    regenerateMissingOnly,
  };
}
