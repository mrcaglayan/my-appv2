import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeCurrencyCode,
  normalizeEnum,
  normalizeText,
  optionalPositiveInt,
  parseAmount,
  parseBooleanFlag,
  parseDateOnly,
  parseDateTime,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const TXN_TYPES = [
  "RECEIPT",
  "PAYOUT",
  "DEPOSIT_TO_BANK",
  "WITHDRAWAL_FROM_BANK",
  "TRANSFER_OUT",
  "TRANSFER_IN",
  "VARIANCE",
  "OPENING_FLOAT",
  "CLOSING_ADJUSTMENT",
];

const TXN_STATUSES = ["DRAFT", "SUBMITTED", "APPROVED", "POSTED", "REVERSED", "CANCELLED"];
const TRANSIT_STATUSES = ["INITIATED", "IN_TRANSIT", "RECEIVED", "CANCELED", "REVERSED"];

const SOURCE_DOC_TYPES = [
  "AP_PAYMENT",
  "AR_RECEIPT",
  "EXPENSE_CLAIM",
  "PETTY_CASH_VOUCHER",
  "BANK_DEPOSIT_SLIP",
  "OTHER",
];

const COUNTERPARTY_TYPES = ["CUSTOMER", "VENDOR", "EMPLOYEE", "LEGAL_ENTITY", "OTHER"];
const SOURCE_MODULES = ["MANUAL", "CARI", "CONTRACTS", "REVREC", "CASH", "SYSTEM", "OTHER"];
const INTEGRATION_LINK_STATUSES = [
  "UNLINKED",
  "PENDING",
  "LINKED",
  "PARTIALLY_LINKED",
  "FAILED",
];

export function parseCashTransitTransferIdParam(req) {
  const transitTransferId = parsePositiveInt(req.params?.transitTransferId);
  if (!transitTransferId) {
    throw badRequest("transitTransferId must be a positive integer");
  }
  return transitTransferId;
}

export function parseCashTransactionIdParam(req) {
  const transactionId = parsePositiveInt(req.params?.transactionId);
  if (!transactionId) {
    throw badRequest("transactionId must be a positive integer");
  }
  return transactionId;
}

export function parseCashTransactionReadFilters(req) {
  const tenantId = requireTenantId(req);
  const registerId = optionalPositiveInt(req.query?.registerId, "registerId");
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const sessionId = optionalPositiveInt(req.query?.sessionId, "sessionId");
  const txnTypeRaw = String(req.query?.txnType || "")
    .trim()
    .toUpperCase();
  const txnType = txnTypeRaw ? normalizeEnum(txnTypeRaw, "txnType", TXN_TYPES) : null;
  const statusRaw = String(req.query?.status || "")
    .trim()
    .toUpperCase();
  const status = statusRaw ? normalizeEnum(statusRaw, "status", TXN_STATUSES) : null;
  const bookDateFrom = req.query?.bookDateFrom
    ? parseDateOnly(req.query?.bookDateFrom, "bookDateFrom")
    : null;
  const bookDateTo = req.query?.bookDateTo
    ? parseDateOnly(req.query?.bookDateTo, "bookDateTo")
    : null;
  const pagination = parsePagination(req.query, { limit: 50, offset: 0, maxLimit: 200 });

  return {
    tenantId,
    registerId,
    legalEntityId,
    sessionId,
    txnType,
    status,
    bookDateFrom,
    bookDateTo,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseCashTransitTransferReadFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const sourceRegisterId = optionalPositiveInt(req.query?.sourceRegisterId, "sourceRegisterId");
  const targetRegisterId = optionalPositiveInt(req.query?.targetRegisterId, "targetRegisterId");
  const statusRaw = String(req.query?.status || "")
    .trim()
    .toUpperCase();
  const status = statusRaw ? normalizeEnum(statusRaw, "status", TRANSIT_STATUSES) : null;
  const initiatedDateFrom = req.query?.initiatedDateFrom
    ? parseDateOnly(req.query?.initiatedDateFrom, "initiatedDateFrom")
    : null;
  const initiatedDateTo = req.query?.initiatedDateTo
    ? parseDateOnly(req.query?.initiatedDateTo, "initiatedDateTo")
    : null;
  const pagination = parsePagination(req.query, { limit: 50, offset: 0, maxLimit: 200 });

  return {
    tenantId,
    legalEntityId,
    sourceRegisterId,
    targetRegisterId,
    status,
    initiatedDateFrom,
    initiatedDateTo,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseCashTransactionCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const registerId = optionalPositiveInt(req.body?.registerId, "registerId");
  const cashSessionId = optionalPositiveInt(req.body?.cashSessionId, "cashSessionId");
  const counterAccountId = optionalPositiveInt(req.body?.counterAccountId, "counterAccountId");
  const counterCashRegisterId = optionalPositiveInt(
    req.body?.counterCashRegisterId,
    "counterCashRegisterId"
  );
  const counterpartyId = optionalPositiveInt(req.body?.counterpartyId, "counterpartyId");
  const linkedCariSettlementBatchId = optionalPositiveInt(
    req.body?.linkedCariSettlementBatchId,
    "linkedCariSettlementBatchId"
  );
  const linkedCariUnappliedCashId = optionalPositiveInt(
    req.body?.linkedCariUnappliedCashId,
    "linkedCariUnappliedCashId"
  );

  if (!registerId) {
    throw badRequest("registerId is required");
  }

  const txnType = normalizeEnum(req.body?.txnType, "txnType", TXN_TYPES);
  const txnDatetime = parseDateTime(req.body?.txnDatetime, "txnDatetime", new Date().toISOString());
  const bookDate = parseDateOnly(
    req.body?.bookDate,
    "bookDate",
    new Date().toISOString().slice(0, 10)
  );
  const amount = parseAmount(req.body?.amount, "amount", { required: true });
  const currencyCode = normalizeCurrencyCode(req.body?.currencyCode, "currencyCode");
  const description = normalizeText(req.body?.description, "description", 500);
  const referenceNo = normalizeText(req.body?.referenceNo, "referenceNo", 100);
  const sourceDocTypeRaw = String(req.body?.sourceDocType || "")
    .trim()
    .toUpperCase();
  const sourceDocType = sourceDocTypeRaw
    ? normalizeEnum(sourceDocTypeRaw, "sourceDocType", SOURCE_DOC_TYPES)
    : null;
  const sourceDocId = normalizeText(req.body?.sourceDocId, "sourceDocId", 80);
  const counterpartyTypeRaw = String(req.body?.counterpartyType || "")
    .trim()
    .toUpperCase();
  const counterpartyType = counterpartyTypeRaw
    ? normalizeEnum(counterpartyTypeRaw, "counterpartyType", COUNTERPARTY_TYPES)
    : null;
  const sourceModuleRaw = String(req.body?.sourceModule || "")
    .trim()
    .toUpperCase();
  const sourceModule = sourceModuleRaw
    ? normalizeEnum(sourceModuleRaw, "sourceModule", SOURCE_MODULES)
    : null;
  const sourceEntityType = normalizeText(req.body?.sourceEntityType, "sourceEntityType", 60);
  const sourceEntityId = normalizeText(req.body?.sourceEntityId, "sourceEntityId", 120);
  const integrationLinkStatusRaw = String(req.body?.integrationLinkStatus || "")
    .trim()
    .toUpperCase();
  const integrationLinkStatus = integrationLinkStatusRaw
    ? normalizeEnum(
        integrationLinkStatusRaw,
        "integrationLinkStatus",
        INTEGRATION_LINK_STATUSES
      )
    : null;
  const integrationEventUid = normalizeText(req.body?.integrationEventUid, "integrationEventUid", 100);
  const idempotencyKey = normalizeText(req.body?.idempotencyKey, "idempotencyKey", 100, {
    required: true,
  });

  if ((sourceEntityType || sourceEntityId || integrationEventUid) && !sourceModule) {
    throw badRequest(
      "sourceModule is required when sourceEntityType, sourceEntityId, or integrationEventUid is provided"
    );
  }
  if (sourceModule && sourceModule !== "MANUAL" && (!sourceEntityType || !sourceEntityId)) {
    throw badRequest("sourceEntityType and sourceEntityId are required when sourceModule is not MANUAL");
  }
  if (
    (linkedCariSettlementBatchId || linkedCariUnappliedCashId) &&
    integrationLinkStatus === "UNLINKED"
  ) {
    throw badRequest(
      "integrationLinkStatus cannot be UNLINKED when linkedCariSettlementBatchId or linkedCariUnappliedCashId is provided"
    );
  }

  return {
    tenantId,
    userId,
    registerId,
    cashSessionId,
    txnType,
    txnDatetime,
    bookDate,
    amount,
    currencyCode,
    description,
    referenceNo,
    sourceDocType,
    sourceDocId,
    counterpartyType,
    counterpartyId,
    counterAccountId,
    counterCashRegisterId,
    linkedCariSettlementBatchId,
    linkedCariUnappliedCashId,
    sourceModule,
    sourceEntityType,
    sourceEntityId,
    integrationLinkStatus,
    integrationEventUid,
    idempotencyKey,
  };
}

export function parseCashTransactionCancelInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const transactionId = parseCashTransactionIdParam(req);
  const cancelReason = normalizeText(req.body?.cancelReason, "cancelReason", 255, {
    required: true,
  });

  return {
    tenantId,
    userId,
    transactionId,
    cancelReason,
  };
}

export function parseCashTransactionPostInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const transactionId = parseCashTransactionIdParam(req);
  const overrideCashControl = parseBooleanFlag(req.body?.overrideCashControl, false);
  const overrideReason = normalizeText(req.body?.overrideReason, "overrideReason", 500);

  if (overrideCashControl && !overrideReason) {
    throw badRequest("overrideReason is required when overrideCashControl=true");
  }

  return {
    tenantId,
    userId,
    transactionId,
    overrideCashControl,
    overrideReason,
  };
}

export function parseCashTransactionReverseInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const transactionId = parseCashTransactionIdParam(req);
  const reverseReason = normalizeText(req.body?.reverseReason, "reverseReason", 255, {
    required: true,
  });

  return {
    tenantId,
    userId,
    transactionId,
    reverseReason,
  };
}

function parseOptionalPositiveDecimal(value, label) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw badRequest(`${label} must be a numeric value greater than 0`);
  }
  return Number(parsed.toFixed(10));
}

function parseCariApplications(value) {
  if (value === undefined || value === null) {
    return [];
  }
  if (!Array.isArray(value)) {
    throw badRequest("applications must be an array");
  }

  return value.map((entry, index) => {
    const openItemId = optionalPositiveInt(entry?.openItemId, `applications[${index}].openItemId`);
    const cariDocumentId = optionalPositiveInt(
      entry?.cariDocumentId,
      `applications[${index}].cariDocumentId`
    );
    if (!openItemId && !cariDocumentId) {
      throw badRequest(`applications[${index}] requires openItemId or cariDocumentId`);
    }
    if (openItemId && cariDocumentId) {
      throw badRequest(`applications[${index}] cannot include both openItemId and cariDocumentId`);
    }

    const amountTxn = parseAmount(
      entry?.amountTxn ?? entry?.amount,
      `applications[${index}].amountTxn`,
      {
        required: true,
        allowZero: false,
      }
    );
    return {
      openItemId: openItemId || null,
      cariDocumentId: cariDocumentId || null,
      amountTxn: Number(amountTxn),
    };
  });
}

export function parseCashTransactionApplyCariInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const transactionId = parseCashTransactionIdParam(req);
  const settlementDate = parseDateOnly(
    req.body?.settlementDate,
    "settlementDate",
    new Date().toISOString().slice(0, 10)
  );
  const idempotencyKey = normalizeText(req.body?.idempotencyKey, "idempotencyKey", 100, {
    required: true,
  });
  const integrationEventUid = normalizeText(req.body?.integrationEventUid, "integrationEventUid", 100);
  const autoAllocate = parseBooleanFlag(req.body?.autoAllocate, false);
  const useUnappliedCash = parseBooleanFlag(req.body?.useUnappliedCash, true);
  const note = normalizeText(req.body?.note, "note", 500);
  const fxRate = parseOptionalPositiveDecimal(req.body?.fxRate, "fxRate");
  const applications = parseCariApplications(req.body?.applications);

  if (autoAllocate && applications.length > 0) {
    throw badRequest("applications must be empty when autoAllocate=true");
  }

  return {
    tenantId,
    userId,
    transactionId,
    settlementDate,
    idempotencyKey,
    integrationEventUid,
    autoAllocate,
    useUnappliedCash,
    note,
    fxRate,
    applications,
  };
}

export function parseCashTransitTransferInitiateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const registerId = optionalPositiveInt(req.body?.registerId, "registerId");
  const targetRegisterId = optionalPositiveInt(
    req.body?.targetRegisterId ?? req.body?.counterCashRegisterId,
    "targetRegisterId"
  );
  const transitAccountId = optionalPositiveInt(
    req.body?.transitAccountId ?? req.body?.counterAccountId,
    "transitAccountId"
  );
  const cashSessionId = optionalPositiveInt(req.body?.cashSessionId, "cashSessionId");
  const txnDatetime = parseDateTime(req.body?.txnDatetime, "txnDatetime", new Date().toISOString());
  const bookDate = parseDateOnly(
    req.body?.bookDate,
    "bookDate",
    new Date().toISOString().slice(0, 10)
  );
  const amount = parseAmount(req.body?.amount, "amount", { required: true });
  const currencyCode = normalizeCurrencyCode(req.body?.currencyCode, "currencyCode");
  const description = normalizeText(req.body?.description, "description", 500);
  const referenceNo = normalizeText(req.body?.referenceNo, "referenceNo", 100);
  const note = normalizeText(req.body?.note, "note", 500);
  const integrationEventUid = normalizeText(req.body?.integrationEventUid, "integrationEventUid", 100);
  const idempotencyKey = normalizeText(req.body?.idempotencyKey, "idempotencyKey", 100, {
    required: true,
  });

  if (!registerId) {
    throw badRequest("registerId is required");
  }
  if (!targetRegisterId) {
    throw badRequest("targetRegisterId is required");
  }
  if (!transitAccountId) {
    throw badRequest("transitAccountId is required");
  }
  if (registerId === targetRegisterId) {
    throw badRequest("registerId and targetRegisterId must be different");
  }

  return {
    tenantId,
    userId,
    registerId,
    targetRegisterId,
    transitAccountId,
    cashSessionId,
    txnDatetime,
    bookDate,
    amount: Number(amount),
    currencyCode,
    description,
    referenceNo,
    note,
    integrationEventUid,
    idempotencyKey,
  };
}

export function parseCashTransitTransferReceiveInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const transitTransferId = parseCashTransitTransferIdParam(req);
  const cashSessionId = optionalPositiveInt(req.body?.cashSessionId, "cashSessionId");
  const txnDatetime = parseDateTime(req.body?.txnDatetime, "txnDatetime", new Date().toISOString());
  const bookDate = parseDateOnly(
    req.body?.bookDate,
    "bookDate",
    new Date().toISOString().slice(0, 10)
  );
  const description = normalizeText(req.body?.description, "description", 500);
  const referenceNo = normalizeText(req.body?.referenceNo, "referenceNo", 100);
  const integrationEventUid = normalizeText(req.body?.integrationEventUid, "integrationEventUid", 100);
  const idempotencyKey = normalizeText(req.body?.idempotencyKey, "idempotencyKey", 100, {
    required: true,
  });

  return {
    tenantId,
    userId,
    transitTransferId,
    cashSessionId,
    txnDatetime,
    bookDate,
    description,
    referenceNo,
    integrationEventUid,
    idempotencyKey,
  };
}

export function parseCashTransitTransferCancelInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const transitTransferId = parseCashTransitTransferIdParam(req);
  const cancelReason = normalizeText(req.body?.cancelReason, "cancelReason", 255, {
    required: true,
  });

  return {
    tenantId,
    userId,
    transitTransferId,
    cancelReason,
  };
}
