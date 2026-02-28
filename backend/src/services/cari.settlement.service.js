import { query, withTransaction } from "../db.js";
import {
  assertAccountBelongsToTenant,
  assertCurrencyExists,
  assertLegalEntityBelongsToTenant,
} from "../tenantGuards.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { assertRegisterOperationalConfig } from "./cash.register.service.js";
import {
  findCashRegisterById,
  findCashSessionById,
  findOpenCashSessionByRegisterId,
  findCashTransactionByIdempotency,
  findCashTransactionByIntegrationEventUid,
  generateCashTxnNoForLegalEntityYearTx,
  insertCashTransaction,
} from "./cash.queries.js";

const AMOUNT_SCALE = 6;
const AMOUNT_EPSILON = 0.000001;
const FX_RATE_TYPE_SPOT = "SPOT";
const FX_FALLBACK_MODE_EXACT_ONLY = "EXACT_ONLY";
const FX_FALLBACK_MODE_PRIOR_DATE = "PRIOR_DATE";
const SETTLEMENT_SEQUENCE_NAMESPACE = "SETTLEMENT";
const SETTLEMENT_STATUS_POSTED = "POSTED";
const SETTLEMENT_STATUS_REVERSED = "REVERSED";
const OPEN_ITEM_STATUS_OPEN = "OPEN";
const OPEN_ITEM_STATUS_PARTIALLY_SETTLED = "PARTIALLY_SETTLED";
const OPEN_ITEM_STATUS_SETTLED = "SETTLED";
const DOCUMENT_STATUS_POSTED = "POSTED";
const DOCUMENT_STATUS_PARTIALLY_SETTLED = "PARTIALLY_SETTLED";
const DOCUMENT_STATUS_SETTLED = "SETTLED";
const UNAPPLIED_STATUS_UNAPPLIED = "UNAPPLIED";
const UNAPPLIED_STATUS_PARTIALLY_APPLIED = "PARTIALLY_APPLIED";
const UNAPPLIED_STATUS_FULL = "FULLY_APPLIED";
const UNAPPLIED_STATUS_REVERSED = "REVERSED";
const INTEGRATION_LINK_STATUS_UNLINKED = "UNLINKED";
const INTEGRATION_LINK_STATUS_LINKED = "LINKED";
const INTEGRATION_LINK_STATUS_PENDING = "PENDING";
const PAYMENT_CHANNEL_MANUAL = "MANUAL";
const PAYMENT_CHANNEL_CASH = "CASH";
const DIRECTION_TO_CASH_TXN_TYPE = Object.freeze({
  AR: "RECEIPT",
  AP: "PAYOUT",
});
const DIRECTION_TO_CASH_COUNTERPARTY_TYPE = Object.freeze({
  AR: "CUSTOMER",
  AP: "VENDOR",
});
const BANK_ATTACH_TARGET_SETTLEMENT = "SETTLEMENT";
const BANK_ATTACH_TARGET_UNAPPLIED_CASH = "UNAPPLIED_CASH";
const RESOURCE_TYPE_SETTLEMENT_BATCH = "cari_settlement_batch";
const RESOURCE_TYPE_UNAPPLIED_CASH = "cari_unapplied_cash";
const RESOURCE_TYPE_CASH_TRANSACTION = "cash_transaction";
const CARI_SETTLEMENT_REFERENCE_PREFIX = "CARI_SETTLE:";
const CARI_SETTLEMENT_REVERSE_REFERENCE_PREFIX = "CARI_SETTLE_REV:";
const CARI_SETTLEMENT_INTENT_SOURCE_ENTITY_TYPE = "cari_settlement_apply";
const SETTLEMENT_POSTING_SOURCE_CONTEXT = Object.freeze({
  CASH_LINKED: "CASH_LINKED",
  MANUAL: "MANUAL",
  ON_ACCOUNT_APPLY: "ON_ACCOUNT_APPLY",
});
const FOLLOW_UP_RISKS = Object.freeze([
  "Posting depends on configured journal_purpose_accounts mappings. Engine tries context codes first (for CASH, MANUAL, ON_ACCOUNT) and falls back to base mappings (CARI_AR_CONTROL/CARI_AR_OFFSET/CARI_AP_CONTROL/CARI_AP_OFFSET). Missing setup blocks posting.",
  "FX lookup uses request fxRate first, then exact-date SPOT, then optional nearest-prior fallback when enabled by config.",
  "Settlement posting resolves source context (CASH_LINKED, MANUAL, ON_ACCOUNT_APPLY) and falls back to generic purpose mappings for compatibility.",
]);
const CARI_SETTLEMENT_PURPOSES = Object.freeze({
  AR: Object.freeze({
    control: "CARI_AR_CONTROL",
    offset: "CARI_AR_OFFSET",
  }),
  AP: Object.freeze({
    control: "CARI_AP_CONTROL",
    offset: "CARI_AP_OFFSET",
  }),
});
const DEFAULT_SETTLEMENT_FX_FALLBACK_MODE = (() => {
  const normalized = normalizeUpperText(process.env.CARI_SETTLEMENT_FX_FALLBACK_MODE);
  if (!normalized) {
    return FX_FALLBACK_MODE_EXACT_ONLY;
  }
  if (
    normalized !== FX_FALLBACK_MODE_EXACT_ONLY &&
    normalized !== FX_FALLBACK_MODE_PRIOR_DATE
  ) {
    throw new Error(
      "CARI_SETTLEMENT_FX_FALLBACK_MODE must be EXACT_ONLY or PRIOR_DATE when provided"
    );
  }
  return normalized;
})();
const DEFAULT_SETTLEMENT_FX_FALLBACK_MAX_DAYS = (() => {
  const raw = process.env.CARI_SETTLEMENT_FX_FALLBACK_MAX_DAYS;
  if (raw === undefined || raw === null || raw === "") {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error("CARI_SETTLEMENT_FX_FALLBACK_MAX_DAYS must be a non-negative integer");
  }
  return parsed;
})();

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function normalizePaymentChannel(value) {
  const normalized = normalizeUpperText(value || PAYMENT_CHANNEL_MANUAL);
  if (
    normalized !== PAYMENT_CHANNEL_MANUAL &&
    normalized !== PAYMENT_CHANNEL_CASH
  ) {
    throw badRequest("paymentChannel must be CASH or MANUAL");
  }
  return normalized;
}

function toDecimalNumber(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function roundAmount(value) {
  return Number(Number(value || 0).toFixed(AMOUNT_SCALE));
}

function amountsAreEqual(left, right, epsilon = AMOUNT_EPSILON) {
  return Math.abs(Number(left || 0) - Number(right || 0)) <= epsilon;
}

function toDateOnlyString(value, label = "date") {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw badRequest(`${label} must be a valid date`);
    }
    const year = value.getFullYear();
    const month = String(value.getMonth() + 1).padStart(2, "0");
    const day = String(value.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }

  const raw = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}(?:\b|T)/.test(raw)) {
    return raw.slice(0, 10);
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    throw badRequest(`${label} must be a valid date`);
  }
  const year = parsed.getFullYear();
  const month = String(parsed.getMonth() + 1).padStart(2, "0");
  const day = String(parsed.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function normalizeDateInput(value, label) {
  const normalized = toDateOnlyString(value, label);
  if (!normalized) {
    throw badRequest(`${label} must be YYYY-MM-DD`);
  }
  return normalized;
}

function normalizeAmount(value, label = "amount", { allowZero = false } = {}) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw badRequest(`${label} must be numeric`);
  }
  if (allowZero ? parsed < 0 : parsed <= 0) {
    throw badRequest(
      allowZero ? `${label} must be >= 0` : `${label} must be > 0`
    );
  }
  return roundAmount(parsed);
}

function normalizeSignedAmount(value, label = "amount", { allowZero = true } = {}) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw badRequest(`${label} must be numeric`);
  }
  if (!allowZero && Math.abs(parsed) <= AMOUNT_EPSILON) {
    throw badRequest(`${label} must not be zero`);
  }
  return roundAmount(parsed);
}

function normalizeOptionalPositiveDecimal(value, label) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw badRequest(`${label} must be a numeric value greater than 0`);
  }
  return Number(parsed.toFixed(10));
}

function normalizeOptionalPositiveInt(value, label) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = parsePositiveInt(value);
  if (!parsed) {
    throw badRequest(`${label} must be a positive integer`);
  }
  return parsed;
}

function normalizeSettlementFxFallbackMode(value) {
  const normalized = normalizeUpperText(
    value === undefined || value === null || value === ""
      ? DEFAULT_SETTLEMENT_FX_FALLBACK_MODE
      : value
  );
  if (
    normalized !== FX_FALLBACK_MODE_EXACT_ONLY &&
    normalized !== FX_FALLBACK_MODE_PRIOR_DATE
  ) {
    throw badRequest("fxFallbackMode must be EXACT_ONLY or PRIOR_DATE");
  }
  return normalized;
}

function normalizeSettlementFxFallbackMaxDays(value) {
  const source =
    value === undefined || value === null || value === ""
      ? DEFAULT_SETTLEMENT_FX_FALLBACK_MAX_DAYS
      : value;
  if (source === null || source === undefined || source === "") {
    return null;
  }
  const parsed = Number(source);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw badRequest("fxFallbackMaxDays must be a non-negative integer");
  }
  return parsed;
}

function normalizeSettlementPostingSourceContext(value) {
  const normalized = normalizeUpperText(
    value || SETTLEMENT_POSTING_SOURCE_CONTEXT.MANUAL
  );
  if (
    normalized !== SETTLEMENT_POSTING_SOURCE_CONTEXT.CASH_LINKED &&
    normalized !== SETTLEMENT_POSTING_SOURCE_CONTEXT.MANUAL &&
    normalized !== SETTLEMENT_POSTING_SOURCE_CONTEXT.ON_ACCOUNT_APPLY
  ) {
    throw badRequest(
      "Settlement posting source context must be CASH_LINKED, MANUAL, or ON_ACCOUNT_APPLY"
    );
  }
  return normalized;
}

function deriveSettlementPostingSourceContext({
  paymentChannel,
  cashTransactionId,
  sourceModule,
  unappliedConsumedCount,
}) {
  const normalizedPaymentChannel = normalizePaymentChannel(paymentChannel);
  const normalizedSourceModule = normalizeUpperText(sourceModule);
  if (
    parsePositiveInt(cashTransactionId) ||
    normalizedPaymentChannel === PAYMENT_CHANNEL_CASH ||
    normalizedSourceModule === "CASH"
  ) {
    return SETTLEMENT_POSTING_SOURCE_CONTEXT.CASH_LINKED;
  }
  if (Number(unappliedConsumedCount || 0) > 0) {
    return SETTLEMENT_POSTING_SOURCE_CONTEXT.ON_ACCOUNT_APPLY;
  }
  return SETTLEMENT_POSTING_SOURCE_CONTEXT.MANUAL;
}

function buildSettlementPostingPurposeCandidates({
  direction,
  sourceContext,
}) {
  const normalizedDirection = normalizeUpperText(direction);
  const normalizedSourceContext =
    normalizeSettlementPostingSourceContext(sourceContext);
  const basePurposes = CARI_SETTLEMENT_PURPOSES[normalizedDirection];
  if (!basePurposes) {
    throw badRequest("Settlement direction must be AR or AP");
  }

  const prefix = normalizedDirection === "AR" ? "CARI_AR" : "CARI_AP";
  if (normalizedSourceContext === SETTLEMENT_POSTING_SOURCE_CONTEXT.CASH_LINKED) {
    return {
      controlCandidates: [`${prefix}_CONTROL_CASH`, basePurposes.control],
      offsetCandidates: [`${prefix}_OFFSET_CASH`, basePurposes.offset],
      normalizedDirection,
      normalizedSourceContext,
    };
  }
  if (normalizedSourceContext === SETTLEMENT_POSTING_SOURCE_CONTEXT.ON_ACCOUNT_APPLY) {
    return {
      controlCandidates: [`${prefix}_CONTROL_ON_ACCOUNT`, basePurposes.control],
      offsetCandidates: [`${prefix}_OFFSET_ON_ACCOUNT`, basePurposes.offset],
      normalizedDirection,
      normalizedSourceContext,
    };
  }
  return {
    controlCandidates: [`${prefix}_CONTROL_MANUAL`, basePurposes.control],
    offsetCandidates: [`${prefix}_OFFSET_MANUAL`, basePurposes.offset],
    normalizedDirection,
    normalizedSourceContext,
  };
}

function resolvePurposeAccountByCandidates({
  byPurpose,
  candidates,
}) {
  for (const candidate of candidates) {
    const normalizedPurpose = normalizeUpperText(candidate);
    const found = byPurpose.get(normalizedPurpose);
    if (found?.id) {
      return {
        ...found,
        purposeCode: normalizedPurpose,
      };
    }
  }
  return null;
}

function resolveSettlementIntegrationMetadata({
  payload,
  idempotencyKey,
  cashTransactionId,
  paymentChannel,
  integrationEventUid,
}) {
  const resolvedCashTransactionId = parsePositiveInt(cashTransactionId);
  const resolvedPaymentChannel = normalizePaymentChannel(
    paymentChannel || payload?.paymentChannel || PAYMENT_CHANNEL_MANUAL
  );
  const sourceModule =
    toNullableString(payload?.sourceModule, 40) ||
    (resolvedPaymentChannel === PAYMENT_CHANNEL_CASH
      ? "CARI"
      : resolvedCashTransactionId
        ? "CASH"
        : "MANUAL");
  const sourceEntityType =
    toNullableString(payload?.sourceEntityType, 60) ||
    (sourceModule === "CASH" && resolvedCashTransactionId
      ? "cash_transaction"
      : sourceModule === "CARI"
        ? CARI_SETTLEMENT_INTENT_SOURCE_ENTITY_TYPE
        : null);
  const sourceEntityId =
    toNullableString(payload?.sourceEntityId, 120) ||
    (sourceModule === "CASH" && resolvedCashTransactionId
      ? String(resolvedCashTransactionId)
      : sourceModule === "CARI"
        ? toNullableString(idempotencyKey, 100)
        : null);
  const integrationLinkStatus =
    toNullableString(payload?.integrationLinkStatus, 30) ||
    (resolvedCashTransactionId || resolvedPaymentChannel === PAYMENT_CHANNEL_CASH
      ? INTEGRATION_LINK_STATUS_LINKED
      : INTEGRATION_LINK_STATUS_UNLINKED);
  const resolvedIntegrationEventUid =
    toNullableString(integrationEventUid, 100) ||
    toNullableString(payload?.integrationEventUid, 100) ||
    toNullableString(idempotencyKey, 100);

  return {
    sourceModule,
    sourceEntityType,
    sourceEntityId,
    integrationLinkStatus,
    integrationEventUid: resolvedIntegrationEventUid,
  };
}

function textsEqual(left, right) {
  return toNullableString(left, 100) === toNullableString(right, 100);
}

function resolveBankLinkFields({
  targetLabel,
  existingBankStatementLineId,
  existingBankTransactionRef,
  requestedBankStatementLineId,
  requestedBankTransactionRef,
}) {
  const existingStatementLineId = normalizeOptionalPositiveInt(
    existingBankStatementLineId,
    `${targetLabel}.bankStatementLineId`
  );
  const requestedStatementLineId = normalizeOptionalPositiveInt(
    requestedBankStatementLineId,
    "bankStatementLineId"
  );
  if (
    existingStatementLineId &&
    requestedStatementLineId &&
    existingStatementLineId !== requestedStatementLineId
  ) {
    throw badRequest(`${targetLabel} is already linked to a different bankStatementLineId`);
  }

  const existingTransactionRef = toNullableString(existingBankTransactionRef, 100);
  const requestedTransactionRef = toNullableString(requestedBankTransactionRef, 100);
  if (
    existingTransactionRef &&
    requestedTransactionRef &&
    !textsEqual(existingTransactionRef, requestedTransactionRef)
  ) {
    throw badRequest(`${targetLabel} is already linked to a different bankTransactionRef`);
  }

  const nextBankStatementLineId = requestedStatementLineId || existingStatementLineId || null;
  const nextBankTransactionRef = requestedTransactionRef || existingTransactionRef || null;
  if (!nextBankStatementLineId && !nextBankTransactionRef) {
    throw badRequest("bankStatementLineId or bankTransactionRef is required");
  }

  return {
    nextBankStatementLineId,
    nextBankTransactionRef,
  };
}

function toNullableString(value, maxLength = 255) {
  if (value === undefined || value === null) {
    return null;
  }
  const normalized = String(value).trim();
  if (!normalized) {
    return null;
  }
  return normalized.slice(0, maxLength);
}

function resolveClientIp(req) {
  const forwardedFor = String(req?.headers?.["x-forwarded-for"] || "").trim();
  if (forwardedFor) {
    const firstIp = forwardedFor
      .split(",")
      .map((segment) => segment.trim())
      .find(Boolean);
    if (firstIp) {
      return firstIp.slice(0, 64);
    }
  }
  return String(req?.ip || req?.socket?.remoteAddress || "unknown").slice(0, 64);
}

function safeStringify(value) {
  try {
    return JSON.stringify(value);
  } catch {
    return JSON.stringify({
      serializationError: "payload_json could not be serialized",
    });
  }
}

function isDuplicateKeyError(err, constraintName = null) {
  if (Number(err?.errno) !== 1062) {
    return false;
  }
  if (!constraintName) {
    return true;
  }
  return String(err?.message || "").includes(constraintName);
}

function buildCariJournalNo(prefix, settlementBatchId) {
  const normalizedPrefix =
    normalizeUpperText(prefix || "CARI-SETTLE").slice(0, 12) || "CARI-SETTLE";
  const parsedBatchId = parsePositiveInt(settlementBatchId);
  const stamp = Date.now().toString(36).toUpperCase();
  const base = parsedBatchId
    ? `${normalizedPrefix}-${parsedBatchId}-${stamp}`
    : `${normalizedPrefix}-${stamp}`;
  return base.slice(0, 40);
}

function buildSettlementNo({ fiscalYear, sequenceNo }) {
  const safeYear = Number(fiscalYear) || 0;
  const safeSequence = Number(sequenceNo) || 0;
  return `SETTLEMENT-${safeYear}-${String(safeSequence).padStart(6, "0")}`;
}

function buildUnappliedReceiptNo(settlementNo) {
  const base = `UNAP-${String(settlementNo || "").trim()}`;
  return base.slice(0, 80);
}

function ensureBalancedJournalLines(lines) {
  let debitTotal = 0;
  let creditTotal = 0;
  for (const line of lines || []) {
    debitTotal += Number(line.debitBase || 0);
    creditTotal += Number(line.creditBase || 0);
  }
  if (Math.abs(debitTotal - creditTotal) > AMOUNT_EPSILON) {
    throw badRequest("Cari settlement journal is not balanced");
  }
  return {
    totalDebit: roundAmount(debitTotal),
    totalCredit: roundAmount(creditTotal),
  };
}

function mapSettlementBatchRow(row) {
  if (!row) {
    return null;
  }
  return {
    id: parsePositiveInt(row.id),
    tenantId: parsePositiveInt(row.tenant_id),
    legalEntityId: parsePositiveInt(row.legal_entity_id),
    counterpartyId: parsePositiveInt(row.counterparty_id),
    cashTransactionId: parsePositiveInt(row.cash_transaction_id),
    sequenceNamespace: row.sequence_namespace,
    fiscalYear: Number(row.fiscal_year),
    sequenceNo: Number(row.sequence_no),
    settlementNo: row.settlement_no,
    settlementDate: toDateOnlyString(row.settlement_date, "settlementDate"),
    status: row.status,
    totalAllocatedTxn: toDecimalNumber(row.total_allocated_txn),
    totalAllocatedBase: toDecimalNumber(row.total_allocated_base),
    currencyCode: row.currency_code,
    postedJournalEntryId: parsePositiveInt(row.posted_journal_entry_id),
    reversalOfSettlementBatchId: parsePositiveInt(row.reversal_of_settlement_batch_id),
    bankStatementLineId: parsePositiveInt(row.bank_statement_line_id),
    bankTransactionRef: row.bank_transaction_ref || null,
    bankAttachIdempotencyKey: row.bank_attach_idempotency_key || null,
    bankApplyIdempotencyKey: row.bank_apply_idempotency_key || null,
    sourceModule: row.source_module || null,
    sourceEntityType: row.source_entity_type || null,
    sourceEntityId: row.source_entity_id || null,
    integrationLinkStatus: row.integration_link_status || null,
    integrationEventUid: row.integration_event_uid || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
    postedAt: row.posted_at || null,
    reversedAt: row.reversed_at || null,
  };
}

function mapAllocationRow(row) {
  return {
    id: parsePositiveInt(row.id),
    tenantId: parsePositiveInt(row.tenant_id),
    legalEntityId: parsePositiveInt(row.legal_entity_id),
    settlementBatchId: parsePositiveInt(row.settlement_batch_id),
    openItemId: parsePositiveInt(row.open_item_id),
    allocationDate: toDateOnlyString(row.allocation_date, "allocationDate"),
    allocationAmountTxn: toDecimalNumber(row.allocation_amount_txn),
    allocationAmountBase: toDecimalNumber(row.allocation_amount_base),
    applyIdempotencyKey: row.apply_idempotency_key || null,
    bankStatementLineId: parsePositiveInt(row.bank_statement_line_id),
    bankApplyIdempotencyKey: row.bank_apply_idempotency_key || null,
    note: row.note || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
  };
}

function mapUnappliedCashRow(row) {
  return {
    id: parsePositiveInt(row.id),
    tenantId: parsePositiveInt(row.tenant_id),
    legalEntityId: parsePositiveInt(row.legal_entity_id),
    counterpartyId: parsePositiveInt(row.counterparty_id),
    cashTransactionId: parsePositiveInt(row.cash_transaction_id),
    cashReceiptNo: row.cash_receipt_no,
    receiptDate: toDateOnlyString(row.receipt_date, "receiptDate"),
    status: row.status,
    amountTxn: toDecimalNumber(row.amount_txn),
    amountBase: toDecimalNumber(row.amount_base),
    residualAmountTxn: toDecimalNumber(row.residual_amount_txn),
    residualAmountBase: toDecimalNumber(row.residual_amount_base),
    currencyCode: row.currency_code,
    postedJournalEntryId: parsePositiveInt(row.posted_journal_entry_id),
    settlementBatchId: parsePositiveInt(row.settlement_batch_id),
    reversalOfUnappliedCashId: parsePositiveInt(row.reversal_of_unapplied_cash_id),
    bankStatementLineId: parsePositiveInt(row.bank_statement_line_id),
    bankTransactionRef: row.bank_transaction_ref || null,
    bankAttachIdempotencyKey: row.bank_attach_idempotency_key || null,
    bankApplyIdempotencyKey: row.bank_apply_idempotency_key || null,
    sourceModule: row.source_module || null,
    sourceEntityType: row.source_entity_type || null,
    sourceEntityId: row.source_entity_id || null,
    integrationLinkStatus: row.integration_link_status || null,
    integrationEventUid: row.integration_event_uid || null,
    note: row.note || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
  };
}

function mapCashTransactionLinkRow(row) {
  if (!row) {
    return null;
  }
  return {
    id: parsePositiveInt(row.id),
    tenantId: parsePositiveInt(row.tenant_id),
    legalEntityId: parsePositiveInt(row.register_legal_entity_id),
    cashRegisterId: parsePositiveInt(row.cash_register_id),
    cashSessionId: parsePositiveInt(row.cash_session_id),
    txnNo: row.txn_no || null,
    txnType: row.txn_type || null,
    status: row.status || null,
    amount: toDecimalNumber(row.amount),
    currencyCode: row.currency_code || null,
    bookDate: toDateOnlyString(row.book_date, "bookDate"),
    referenceNo: row.reference_no || null,
    description: row.description || null,
    counterpartyType: row.counterparty_type || null,
    counterpartyId: parsePositiveInt(row.counterparty_id),
    linkedCariSettlementBatchId: parsePositiveInt(row.linked_cari_settlement_batch_id),
    linkedCariUnappliedCashId: parsePositiveInt(row.linked_cari_unapplied_cash_id),
    sourceModule: row.source_module || null,
    sourceEntityType: row.source_entity_type || null,
    sourceEntityId: row.source_entity_id || null,
    integrationLinkStatus: row.integration_link_status || null,
    integrationEventUid: row.integration_event_uid || null,
    idempotencyKey: row.idempotency_key || null,
  };
}

async function insertAuditLog({
  req,
  runQuery = query,
  tenantId,
  userId,
  action,
  resourceType = RESOURCE_TYPE_SETTLEMENT_BATCH,
  legalEntityId,
  resourceId,
  payload,
}) {
  await runQuery(
    `INSERT INTO audit_logs (
        tenant_id,
        user_id,
        action,
        resource_type,
        resource_id,
        scope_type,
        scope_id,
        request_id,
        ip_address,
        user_agent,
        payload_json
     )
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      tenantId,
      userId || null,
      action,
      resourceType,
      resourceId ? String(resourceId) : null,
      legalEntityId ? "LEGAL_ENTITY" : null,
      legalEntityId || null,
      toNullableString(req?.requestId || req?.headers?.["x-request-id"], 80),
      resolveClientIp(req),
      toNullableString(req?.headers?.["user-agent"], 255),
      safeStringify(payload || null),
    ]
  );
}

async function fetchCounterpartyRow({
  tenantId,
  legalEntityId,
  counterpartyId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
        id,
        tenant_id,
        legal_entity_id,
        code,
        name,
        is_customer,
        is_vendor,
        ar_account_id,
        ap_account_id,
        status
     FROM counterparties
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, counterpartyId]
  );
  return result.rows?.[0] || null;
}

async function resolveBookAndOpenPeriodForDate({
  tenantId,
  legalEntityId,
  targetDate,
  preferredBookId = null,
  runQuery = query,
}) {
  const normalizedDate = normalizeDateInput(targetDate, "settlementDate");
  let book = null;

  if (preferredBookId) {
    const preferredBookResult = await runQuery(
      `SELECT id, calendar_id, base_currency_code, book_type
       FROM books
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?
       LIMIT 1`,
      [tenantId, legalEntityId, preferredBookId]
    );
    book = preferredBookResult.rows?.[0] || null;
  }

  if (!book) {
    const bookResult = await runQuery(
      `SELECT id, calendar_id, base_currency_code, book_type
       FROM books
       WHERE tenant_id = ?
         AND legal_entity_id = ?
       ORDER BY
         CASE WHEN book_type = 'LOCAL' THEN 0 ELSE 1 END,
         id ASC
       LIMIT 1`,
      [tenantId, legalEntityId]
    );
    book = bookResult.rows?.[0] || null;
  }
  if (!book) {
    throw badRequest("No book found for settlement legalEntityId");
  }

  const bookId = parsePositiveInt(book.id);
  const calendarId = parsePositiveInt(book.calendar_id);
  if (!bookId || !calendarId) {
    throw badRequest("Book configuration is invalid for settlement posting");
  }

  const periodResult = await runQuery(
    `SELECT id, fiscal_year, period_no, start_date, end_date
     FROM fiscal_periods
     WHERE calendar_id = ?
       AND ? BETWEEN start_date AND end_date
     ORDER BY is_adjustment ASC, id ASC
     LIMIT 1`,
    [calendarId, normalizedDate]
  );
  const period = periodResult.rows?.[0] || null;
  if (!period) {
    throw badRequest("No fiscal period found for settlement date");
  }

  const fiscalPeriodId = parsePositiveInt(period.id);
  if (!fiscalPeriodId) {
    throw badRequest("Fiscal period configuration is invalid for settlement posting");
  }

  const statusResult = await runQuery(
    `SELECT status
     FROM period_statuses
     WHERE book_id = ?
       AND fiscal_period_id = ?
     LIMIT 1`,
    [bookId, fiscalPeriodId]
  );
  const periodStatus = normalizeUpperText(statusResult.rows?.[0]?.status || "OPEN");
  if (periodStatus !== "OPEN") {
    throw badRequest(`Period is ${periodStatus}; cannot apply/reverse settlement`);
  }

  return {
    bookId,
    fiscalPeriodId,
    fiscalYear: Number(period.fiscal_year),
    baseCurrencyCode: normalizeUpperText(book.base_currency_code),
  };
}

async function reserveSettlementSequence({
  tenantId,
  legalEntityId,
  settlementDate,
  runQuery = query,
}) {
  const normalizedDate = normalizeDateInput(settlementDate, "settlementDate");
  const fiscalYear = Number(normalizedDate.slice(0, 4));
  const sequenceNamespace = SETTLEMENT_SEQUENCE_NAMESPACE;

  const result = await runQuery(
    `SELECT COALESCE(MAX(sequence_no), 0) AS current_max
     FROM cari_settlement_batches
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND sequence_namespace = ?
       AND fiscal_year = ?
     FOR UPDATE`,
    [tenantId, legalEntityId, sequenceNamespace, fiscalYear]
  );
  const nextSequenceNo = Number(result.rows?.[0]?.current_max || 0) + 1;
  return {
    sequenceNamespace,
    fiscalYear,
    sequenceNo: nextSequenceNo,
    settlementNo: buildSettlementNo({
      fiscalYear,
      sequenceNo: nextSequenceNo,
    }),
  };
}

async function resolveCounterpartyControlAccountOverride({
  tenantId,
  legalEntityId,
  direction,
  counterpartyRow,
  runQuery = query,
}) {
  if (!counterpartyRow || !parsePositiveInt(counterpartyRow.id)) {
    return null;
  }

  const normalizedDirection = normalizeUpperText(direction);
  const mapping =
    normalizedDirection === "AR"
      ? {
          accountId: parsePositiveInt(counterpartyRow.ar_account_id),
          roleEnabled: counterpartyRow.is_customer === true || Number(counterpartyRow.is_customer) === 1,
          fieldLabel: "arAccountId",
          expectedAccountType: "ASSET",
        }
      : normalizedDirection === "AP"
        ? {
            accountId: parsePositiveInt(counterpartyRow.ap_account_id),
            roleEnabled:
              counterpartyRow.is_vendor === true || Number(counterpartyRow.is_vendor) === 1,
            fieldLabel: "apAccountId",
            expectedAccountType: "LIABILITY",
          }
        : null;

  if (!mapping) {
    throw badRequest("Settlement direction must be AR or AP");
  }
  if (!mapping.accountId) {
    return null;
  }
  if (!mapping.roleEnabled) {
    throw badRequest(`${mapping.fieldLabel} requires compatible counterparty role`);
  }

  await assertAccountBelongsToTenant(tenantId, mapping.accountId, mapping.fieldLabel, {
    runQuery,
  });

  const accountResult = await runQuery(
    `SELECT
        a.id,
        a.code,
        a.account_type,
        a.is_active,
        a.allow_posting,
        c.scope AS coa_scope,
        c.legal_entity_id AS coa_legal_entity_id
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE a.id = ?
       AND c.tenant_id = ?
     LIMIT 1`,
    [mapping.accountId, tenantId]
  );
  const account = accountResult.rows?.[0] || null;
  if (!account) {
    throw badRequest(`${mapping.fieldLabel} not found for tenant`);
  }

  if (normalizeUpperText(account.coa_scope) !== "LEGAL_ENTITY") {
    throw badRequest(`${mapping.fieldLabel} must belong to a LEGAL_ENTITY chart`);
  }
  if (parsePositiveInt(account.coa_legal_entity_id) !== parsePositiveInt(legalEntityId)) {
    throw badRequest(`${mapping.fieldLabel} must belong to legalEntityId`);
  }
  if (normalizeUpperText(account.account_type) !== mapping.expectedAccountType) {
    throw badRequest(`${mapping.fieldLabel} must have accountType=${mapping.expectedAccountType}`);
  }
  if (!(account.is_active === true || Number(account.is_active) === 1)) {
    throw badRequest(`${mapping.fieldLabel} must reference an ACTIVE account`);
  }
  if (!(account.allow_posting === true || Number(account.allow_posting) === 1)) {
    throw badRequest(`${mapping.fieldLabel} must reference a postable account`);
  }

  return {
    id: parsePositiveInt(account.id),
    code: account.code || null,
  };
}

async function resolveSettlementPostingAccounts({
  tenantId,
  legalEntityId,
  direction,
  sourceContext = SETTLEMENT_POSTING_SOURCE_CONTEXT.MANUAL,
  counterpartyRow = null,
  runQuery = query,
}) {
  const purposeCandidates = buildSettlementPostingPurposeCandidates({
    direction,
    sourceContext,
  });
  const requestedPurposes = Array.from(
    new Set([
      ...purposeCandidates.controlCandidates.map((entry) => normalizeUpperText(entry)),
      ...purposeCandidates.offsetCandidates.map((entry) => normalizeUpperText(entry)),
    ])
  );
  const placeholders = requestedPurposes.map(() => "?").join(", ");
  const result = await runQuery(
    `SELECT
       jpa.purpose_code,
       a.id AS account_id,
       a.code AS account_code
     FROM journal_purpose_accounts jpa
     JOIN accounts a ON a.id = jpa.account_id
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE jpa.tenant_id = ?
       AND jpa.legal_entity_id = ?
       AND jpa.purpose_code IN (${placeholders})
       AND c.tenant_id = ?
       AND c.legal_entity_id = ?
       AND a.is_active = TRUE
       AND a.allow_posting = TRUE`,
    [tenantId, legalEntityId, ...requestedPurposes, tenantId, legalEntityId]
  );

  const byPurpose = new Map(
    (result.rows || []).map((row) => [
      normalizeUpperText(row.purpose_code),
      {
        id: parsePositiveInt(row.account_id),
        code: String(row.account_code || ""),
      },
    ])
  );

  const control = resolvePurposeAccountByCandidates({
    byPurpose,
    candidates: purposeCandidates.controlCandidates,
  });
  const offset = resolvePurposeAccountByCandidates({
    byPurpose,
    candidates: purposeCandidates.offsetCandidates,
  });
  if (!control?.id || !offset?.id) {
    const joinedControls = purposeCandidates.controlCandidates.join(" -> ");
    const joinedOffsets = purposeCandidates.offsetCandidates.join(" -> ");
    throw badRequest(
      `Setup required: configure journal_purpose_accounts for control [${joinedControls}] and offset [${joinedOffsets}]`
    );
  }

  const overrideControl = await resolveCounterpartyControlAccountOverride({
    tenantId,
    legalEntityId,
    direction,
    counterpartyRow,
    runQuery,
  });
  const effectiveControl = overrideControl?.id
    ? {
        id: overrideControl.id,
        code: overrideControl.code || null,
      }
    : control;

  if (effectiveControl.id === offset.id) {
    throw badRequest("Cari settlement control and offset accounts must be different");
  }

  return {
    sourceContext: purposeCandidates.normalizedSourceContext,
    direction: purposeCandidates.normalizedDirection,
    controlAccountId: effectiveControl.id,
    offsetAccountId: offset.id,
    controlAccountCode: effectiveControl.code || null,
    offsetAccountCode: offset.code || null,
    controlPurposeCode: control.purposeCode || null,
    offsetPurposeCode: offset.purposeCode || null,
  };
}

async function resolveSettlementFxRate({
  tenantId,
  settlementDate,
  settlementCurrencyCode,
  functionalCurrencyCode,
  providedFxRate,
  fxFallbackMode,
  fxFallbackMaxDays,
  runQuery = query,
}) {
  const normalizedDate = normalizeDateInput(settlementDate, "settlementDate");
  const settlementCurrency = normalizeUpperText(settlementCurrencyCode);
  const functionalCurrency = normalizeUpperText(functionalCurrencyCode);
  const normalizedProvidedRate = normalizeOptionalPositiveDecimal(
    providedFxRate,
    "fxRate"
  );
  const normalizedFallbackMode = normalizeSettlementFxFallbackMode(fxFallbackMode);
  const normalizedFallbackMaxDays = normalizeSettlementFxFallbackMaxDays(
    fxFallbackMaxDays
  );

  if (!settlementCurrency || !functionalCurrency) {
    throw badRequest("Settlement and functional currency codes are required");
  }

  if (settlementCurrency === functionalCurrency) {
    const effectiveRate = normalizedProvidedRate || 1;
    if (!amountsAreEqual(effectiveRate, 1)) {
      throw badRequest(
        "fxRate must be 1 when settlement currency equals legal entity functional currency"
      );
    }
    return {
      settlementFxRate: 1,
      source: "PARITY",
      rateDate: normalizedDate,
      fallbackMode: normalizedFallbackMode,
      fallbackMaxDays: normalizedFallbackMaxDays,
      riskNotes: FOLLOW_UP_RISKS,
    };
  }

  const fxResult = await runQuery(
    `SELECT rate, rate_date
     FROM fx_rates
     WHERE tenant_id = ?
       AND rate_date = ?
       AND from_currency_code = ?
       AND to_currency_code = ?
       AND rate_type = ?
     ORDER BY id DESC
     LIMIT 1`,
    [
      tenantId,
      normalizedDate,
      settlementCurrency,
      functionalCurrency,
      FX_RATE_TYPE_SPOT,
    ]
  );
  const fxRow = fxResult.rows?.[0] || null;
  const tableRate = normalizeOptionalPositiveDecimal(fxRow?.rate, "fxRates.rate");
  if (normalizedProvidedRate) {
    return {
      settlementFxRate: normalizedProvidedRate,
      source: "REQUEST",
      rateDate: normalizedDate,
      fallbackMode: normalizedFallbackMode,
      fallbackMaxDays: normalizedFallbackMaxDays,
      riskNotes: FOLLOW_UP_RISKS,
    };
  }
  if (tableRate) {
    return {
      settlementFxRate: tableRate,
      source: "FX_TABLE_EXACT_SPOT",
      rateDate: toDateOnlyString(fxRow?.rate_date || normalizedDate, "fxRateDate"),
      fallbackMode: normalizedFallbackMode,
      fallbackMaxDays: normalizedFallbackMaxDays,
      riskNotes: FOLLOW_UP_RISKS,
    };
  }

  if (normalizedFallbackMode === FX_FALLBACK_MODE_PRIOR_DATE) {
    const fallbackClauses = [];
    const fallbackParams = [
      tenantId,
      settlementCurrency,
      functionalCurrency,
      FX_RATE_TYPE_SPOT,
      normalizedDate,
    ];
    if (normalizedFallbackMaxDays !== null) {
      fallbackClauses.push("AND DATEDIFF(?, rate_date) <= ?");
      fallbackParams.push(normalizedDate, normalizedFallbackMaxDays);
    }
    const fallbackResult = await runQuery(
      `SELECT rate, rate_date
       FROM fx_rates
       WHERE tenant_id = ?
         AND from_currency_code = ?
         AND to_currency_code = ?
         AND rate_type = ?
         AND rate_date < ?
         ${fallbackClauses.join(" ")}
       ORDER BY rate_date DESC, id DESC
       LIMIT 1`,
      fallbackParams
    );
    const fallbackRow = fallbackResult.rows?.[0] || null;
    const fallbackRate = normalizeOptionalPositiveDecimal(
      fallbackRow?.rate,
      "fxRates.rate"
    );
    if (fallbackRate) {
      return {
        settlementFxRate: fallbackRate,
        source: "FX_TABLE_PRIOR_SPOT",
        rateDate: toDateOnlyString(fallbackRow?.rate_date, "fxRateDate"),
        fallbackMode: normalizedFallbackMode,
        fallbackMaxDays: normalizedFallbackMaxDays,
        riskNotes: FOLLOW_UP_RISKS,
      };
    }
    throw badRequest(
      normalizedFallbackMaxDays === null
        ? "fxRate is required because no exact-date SPOT rate exists and no prior SPOT rate was found for settlement currency pair"
        : "fxRate is required because no exact-date SPOT rate exists and no prior SPOT rate was found within fxFallbackMaxDays for settlement currency pair"
    );
  }

  throw badRequest(
    "fxRate is required because no exact-date SPOT FX rate exists for settlementDate and currency pair"
  );
}

async function insertPostedJournalWithLinesTx(tx, payload) {
  const totals = ensureBalancedJournalLines(payload.lines);
  const insertResult = await tx.query(
    `INSERT INTO journal_entries (
        tenant_id,
        legal_entity_id,
        book_id,
        fiscal_period_id,
        journal_no,
        source_type,
        status,
        entry_date,
        document_date,
        currency_code,
        description,
        reference_no,
        total_debit_base,
        total_credit_base,
        created_by_user_id,
        posted_by_user_id,
        posted_at
     )
     VALUES (?, ?, ?, ?, ?, 'SYSTEM', 'POSTED', ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
    [
      payload.tenantId,
      payload.legalEntityId,
      payload.bookId,
      payload.fiscalPeriodId,
      payload.journalNo,
      payload.entryDate,
      payload.documentDate,
      payload.currencyCode,
      payload.description,
      payload.referenceNo,
      totals.totalDebit,
      totals.totalCredit,
      payload.userId,
      payload.userId,
    ]
  );
  const journalEntryId = parsePositiveInt(insertResult.rows?.insertId);
  if (!journalEntryId) {
    throw badRequest("Failed to create settlement journal entry");
  }

  for (let i = 0; i < payload.lines.length; i += 1) {
    const line = payload.lines[i];
    // eslint-disable-next-line no-await-in-loop
    await tx.query(
      `INSERT INTO journal_lines (
          journal_entry_id,
          line_no,
          account_id,
          operating_unit_id,
          counterparty_legal_entity_id,
          description,
          subledger_reference_no,
          currency_code,
          amount_txn,
          debit_base,
          credit_base,
          tax_code
       )
       VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, NULL)`,
      [
        journalEntryId,
        i + 1,
        parsePositiveInt(line.accountId),
        line.description || null,
        line.subledgerReferenceNo || null,
        line.currencyCode,
        normalizeSignedAmount(line.amountTxn, `line[${i}].amountTxn`),
        normalizeAmount(line.debitBase, `line[${i}].debitBase`, { allowZero: true }),
        normalizeAmount(line.creditBase, `line[${i}].creditBase`, { allowZero: true }),
      ]
    );
  }

  return {
    journalEntryId,
    lineCount: payload.lines.length,
    totalDebit: totals.totalDebit,
    totalCredit: totals.totalCredit,
  };
}

async function fetchPostedJournalWithLines({
  tenantId,
  journalEntryId,
  runQuery = query,
}) {
  const journalResult = await runQuery(
    `SELECT
       id,
       tenant_id,
       legal_entity_id,
       book_id,
       fiscal_period_id,
       journal_no,
       source_type,
       status,
       entry_date,
       document_date,
       currency_code,
       description,
       reference_no,
       posted_at,
       reversal_journal_entry_id
     FROM journal_entries
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, journalEntryId]
  );
  const journalRow = journalResult.rows?.[0] || null;
  if (!journalRow) {
    return null;
  }

  const lineResult = await runQuery(
    `SELECT
       id,
       line_no,
       account_id,
       description,
       subledger_reference_no,
       currency_code,
       amount_txn,
       debit_base,
       credit_base
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no ASC`,
    [journalEntryId]
  );

  return {
    journal: journalRow,
    lines: lineResult.rows || [],
  };
}

async function fetchSettlementBatchRow({
  tenantId,
  settlementBatchId,
  runQuery = query,
  forUpdate = false,
}) {
  const lockClause = forUpdate ? "FOR UPDATE" : "";
  const result = await runQuery(
    `SELECT
       id,
       tenant_id,
       legal_entity_id,
       counterparty_id,
       cash_transaction_id,
       sequence_namespace,
       fiscal_year,
       sequence_no,
       settlement_no,
       settlement_date,
       status,
       total_allocated_txn,
       total_allocated_base,
       currency_code,
       posted_journal_entry_id,
       reversal_of_settlement_batch_id,
       bank_statement_line_id,
       bank_transaction_ref,
       bank_attach_idempotency_key,
       bank_apply_idempotency_key,
       source_module,
       source_entity_type,
       source_entity_id,
       integration_link_status,
       integration_event_uid,
       created_at,
       updated_at,
       posted_at,
       reversed_at
     FROM cari_settlement_batches
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1
     ${lockClause}`,
    [tenantId, settlementBatchId]
  );
  return result.rows?.[0] || null;
}

async function fetchSettlementAllocationsByBatchId({
  tenantId,
  settlementBatchId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
       id,
       tenant_id,
       legal_entity_id,
       settlement_batch_id,
       open_item_id,
       allocation_date,
       allocation_amount_txn,
       allocation_amount_base,
       apply_idempotency_key,
       bank_statement_line_id,
       bank_apply_idempotency_key,
       note,
       created_at,
       updated_at
     FROM cari_settlement_allocations
     WHERE tenant_id = ?
       AND settlement_batch_id = ?
     ORDER BY id ASC`,
    [tenantId, settlementBatchId]
  );
  return result.rows || [];
}
async function findSettlementBatchIdByApplyIdempotency({
  tenantId,
  legalEntityId,
  applyIdempotencyKey,
  runQuery = query,
}) {
  const normalizedKey = toNullableString(applyIdempotencyKey, 100);
  if (!normalizedKey) {
    return null;
  }
  const result = await runQuery(
    `SELECT settlement_batch_id
     FROM cari_settlement_allocations
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND apply_idempotency_key = ?
     LIMIT 1`,
    [tenantId, legalEntityId, normalizedKey]
  );
  return parsePositiveInt(result.rows?.[0]?.settlement_batch_id);
}

async function findSettlementBatchIdByBankApplyIdempotency({
  tenantId,
  legalEntityId,
  bankApplyIdempotencyKey,
  runQuery = query,
}) {
  const normalizedKey = toNullableString(bankApplyIdempotencyKey, 100);
  if (!normalizedKey) {
    return null;
  }
  const result = await runQuery(
    `SELECT id
     FROM cari_settlement_batches
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND bank_apply_idempotency_key = ?
     LIMIT 1`,
    [tenantId, legalEntityId, normalizedKey]
  );
  return parsePositiveInt(result.rows?.[0]?.id);
}

async function findSettlementBatchIdByIntegrationEventUid({
  tenantId,
  legalEntityId,
  integrationEventUid,
  runQuery = query,
}) {
  const normalizedUid = toNullableString(integrationEventUid, 100);
  if (!normalizedUid) {
    return null;
  }
  const result = await runQuery(
    `SELECT id
     FROM cari_settlement_batches
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND integration_event_uid = ?
     LIMIT 1`,
    [tenantId, legalEntityId, normalizedUid]
  );
  return parsePositiveInt(result.rows?.[0]?.id);
}

async function findSettlementBatchIdByCashTransactionId({
  tenantId,
  legalEntityId,
  cashTransactionId,
  runQuery = query,
}) {
  const parsedCashTxnId = parsePositiveInt(cashTransactionId);
  if (!parsedCashTxnId) {
    return null;
  }
  const result = await runQuery(
    `SELECT id
     FROM cari_settlement_batches
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND cash_transaction_id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, parsedCashTxnId]
  );
  return parsePositiveInt(result.rows?.[0]?.id);
}

async function fetchCashTransactionForSettlementLink({
  tenantId,
  cashTransactionId,
  runQuery = query,
  forUpdate = false,
}) {
  const parsedCashTxnId = parsePositiveInt(cashTransactionId);
  if (!parsedCashTxnId) {
    return null;
  }
  const lockClause = forUpdate ? "FOR UPDATE" : "";
  const result = await runQuery(
    `SELECT
       ct.id,
       ct.tenant_id,
       ct.cash_register_id,
       ct.cash_session_id,
       ct.txn_no,
       ct.txn_type,
       ct.status,
       ct.amount,
       ct.book_date,
       ct.currency_code,
       ct.reference_no,
       ct.description,
       ct.counterparty_type,
       ct.counterparty_id,
       ct.linked_cari_settlement_batch_id,
       ct.linked_cari_unapplied_cash_id,
       ct.source_module,
       ct.source_entity_type,
       ct.source_entity_id,
       ct.integration_link_status,
       ct.integration_event_uid,
       ct.idempotency_key,
       cr.legal_entity_id AS register_legal_entity_id
     FROM cash_transactions ct
     JOIN cash_registers cr ON cr.id = ct.cash_register_id
     WHERE ct.tenant_id = ?
       AND ct.id = ?
     LIMIT 1
     ${lockClause}`,
    [tenantId, parsedCashTxnId]
  );
  return result.rows?.[0] || null;
}

async function resolveCashSessionForSettlementPayment({
  tenantId,
  register,
  requestedSessionId,
  runQuery = query,
}) {
  const sessionMode = normalizeUpperText(register?.session_mode);
  const registerId = parsePositiveInt(register?.id);

  if (requestedSessionId) {
    const session = await findCashSessionById({
      tenantId,
      sessionId: requestedSessionId,
      runQuery,
      forUpdate: true,
    });
    if (!session) {
      throw badRequest("linkedCashTransaction.cashSessionId not found for tenant");
    }
    if (parsePositiveInt(session.cash_register_id) !== registerId) {
      throw badRequest("linkedCashTransaction.cashSessionId must belong to linkedCashTransaction.registerId");
    }
    if (normalizeUpperText(session.status) !== "OPEN") {
      throw badRequest("linkedCashTransaction.cashSessionId must be OPEN");
    }
    return session;
  }

  if (sessionMode === "NONE") {
    return null;
  }

  const openSession = await findOpenCashSessionByRegisterId({
    tenantId,
    registerId,
    runQuery,
    forUpdate: true,
  });

  if (sessionMode === "REQUIRED" && !openSession) {
    throw badRequest(
      "An OPEN cash session is required for linkedCashTransaction.registerId"
    );
  }

  return openSession || null;
}

function buildDefaultLinkedCashIdempotencyKey({
  legalEntityId,
  settlementIdempotencyKey,
}) {
  return toNullableString(
    `CARI-CASH-${Number(legalEntityId)}-${String(settlementIdempotencyKey || "").trim()}`,
    100
  );
}

function buildDefaultLinkedCashIntegrationEventUid({
  legalEntityId,
  settlementIntegrationEventUid,
  settlementIdempotencyKey,
}) {
  return toNullableString(
    settlementIntegrationEventUid ||
      `CARI-CASH-EVT-${Number(legalEntityId)}-${String(settlementIdempotencyKey || "").trim()}`,
    100
  );
}

async function createOrReplaySettlementCashTransaction({
  req,
  tenantId,
  userId,
  legalEntityId,
  direction,
  counterpartyId,
  counterpartyRow,
  currencyCode,
  amountTxn,
  settlementDate,
  settlementIdempotencyKey,
  settlementIntegrationEventUid,
  linkedCashTransaction,
  assertScopeAccess,
  runQuery = query,
}) {
  if (!linkedCashTransaction || typeof linkedCashTransaction !== "object") {
    throw badRequest("linkedCashTransaction is required when paymentChannel=CASH");
  }
  if (!Number.isFinite(Number(amountTxn)) || Number(amountTxn) <= AMOUNT_EPSILON) {
    throw badRequest("incomingAmountTxn must be > 0 when paymentChannel=CASH");
  }

  const normalizedDirection = normalizeUpperText(direction);
  const txnType = DIRECTION_TO_CASH_TXN_TYPE[normalizedDirection];
  const counterpartyType = DIRECTION_TO_CASH_COUNTERPARTY_TYPE[normalizedDirection];
  if (!txnType || !counterpartyType) {
    throw badRequest("direction must be AR or AP for paymentChannel=CASH");
  }

  const roleAllowed =
    counterpartyType === "CUSTOMER"
      ? counterpartyRow?.is_customer === true || Number(counterpartyRow?.is_customer) === 1
      : counterpartyRow?.is_vendor === true || Number(counterpartyRow?.is_vendor) === 1;
  if (!roleAllowed) {
    throw badRequest(
      `counterpartyId is not marked as ${counterpartyType === "CUSTOMER" ? "customer" : "vendor"}`
    );
  }

  const registerId = normalizeOptionalPositiveInt(
    linkedCashTransaction.registerId,
    "linkedCashTransaction.registerId"
  );
  const cashSessionId = normalizeOptionalPositiveInt(
    linkedCashTransaction.cashSessionId,
    "linkedCashTransaction.cashSessionId"
  );
  const counterAccountId = normalizeOptionalPositiveInt(
    linkedCashTransaction.counterAccountId,
    "linkedCashTransaction.counterAccountId"
  );
  const txnDatetime =
    toNullableString(linkedCashTransaction.txnDatetime, 40) ||
    new Date().toISOString().slice(0, 19).replace("T", " ");
  const bookDate =
    normalizeDateInput(
      linkedCashTransaction.bookDate || settlementDate,
      "linkedCashTransaction.bookDate"
    ) || settlementDate;
  const referenceNo = toNullableString(linkedCashTransaction.referenceNo, 100);
  const description =
    toNullableString(linkedCashTransaction.description, 500) ||
    `Linked cash transaction for settlement intent ${String(
      settlementIdempotencyKey || ""
    ).slice(0, 60)}`;
  const cashIdempotencyKey =
    toNullableString(linkedCashTransaction.idempotencyKey, 100) ||
    buildDefaultLinkedCashIdempotencyKey({
      legalEntityId,
      settlementIdempotencyKey,
    });
  const cashIntegrationEventUid =
    toNullableString(linkedCashTransaction.integrationEventUid, 100) ||
    buildDefaultLinkedCashIntegrationEventUid({
      legalEntityId,
      settlementIntegrationEventUid,
      settlementIdempotencyKey,
    });

  if (!registerId) {
    throw badRequest("linkedCashTransaction.registerId is required");
  }
  if (!counterAccountId) {
    throw badRequest("linkedCashTransaction.counterAccountId is required");
  }
  if (!cashIdempotencyKey) {
    throw badRequest("linkedCashTransaction.idempotencyKey could not be resolved");
  }

  const register = await findCashRegisterById({
    tenantId,
    registerId,
    runQuery,
  });
  if (!register) {
    throw badRequest("linkedCashTransaction.registerId not found for tenant");
  }
  assertRegisterOperationalConfig(register, {
    requireActive: true,
    requireCashControlledAccount: true,
    label: "linkedCashTransaction.registerId",
  });
  assertScopeAccess(req, "legal_entity", register.legal_entity_id, "linkedCashTransaction.registerId");
  if (register.operating_unit_id) {
    assertScopeAccess(
      req,
      "operating_unit",
      register.operating_unit_id,
      "linkedCashTransaction.registerId"
    );
  }
  if (parsePositiveInt(register.legal_entity_id) !== legalEntityId) {
    throw badRequest("linkedCashTransaction.registerId must belong to legalEntityId");
  }
  if (normalizeUpperText(register.currency_code) !== normalizeUpperText(currencyCode)) {
    throw badRequest("linkedCashTransaction.registerId currency must match settlement currencyCode");
  }
  if (Number(register.max_txn_amount || 0) > 0 && Number(amountTxn) > Number(register.max_txn_amount)) {
    throw badRequest("incomingAmountTxn exceeds linkedCashTransaction.registerId max_txn_amount");
  }

  await assertAccountBelongsToTenant(
    tenantId,
    counterAccountId,
    "linkedCashTransaction.counterAccountId",
    { runQuery }
  );

  const resolvedSession = await resolveCashSessionForSettlementPayment({
    tenantId,
    register,
    requestedSessionId: cashSessionId,
    runQuery,
  });

  const replayByEvent = cashIntegrationEventUid
    ? await findCashTransactionByIntegrationEventUid({
        tenantId,
        integrationEventUid: cashIntegrationEventUid,
        runQuery,
      })
    : null;
  const replayByIdempotency = await findCashTransactionByIdempotency({
    tenantId,
    registerId,
    idempotencyKey: cashIdempotencyKey,
    runQuery,
  });

  const replayRow = replayByEvent || replayByIdempotency;
  if (replayByEvent && replayByIdempotency && parsePositiveInt(replayByEvent.id) !== parsePositiveInt(replayByIdempotency.id)) {
    throw badRequest(
      "linkedCashTransaction.idempotencyKey and linkedCashTransaction.integrationEventUid map to different cash transactions"
    );
  }
  if (replayRow) {
    const replayLegalEntityId =
      parsePositiveInt(replayRow.register_legal_entity_id) ||
      parsePositiveInt(replayRow.legal_entity_id);
    if (parsePositiveInt(replayRow.cash_register_id) !== registerId) {
      throw badRequest(
        "linkedCashTransaction.idempotencyKey/integrationEventUid is already used by a different registerId"
      );
    }
    if (replayLegalEntityId !== legalEntityId) {
      throw badRequest(
        "linkedCashTransaction.idempotencyKey/integrationEventUid is already used by a different legalEntityId"
      );
    }
    if (normalizeUpperText(replayRow.txn_type) !== txnType) {
      throw badRequest(
        "linkedCashTransaction.idempotencyKey/integrationEventUid is already used by a different txnType"
      );
    }
    if (normalizeUpperText(replayRow.counterparty_type) !== counterpartyType) {
      throw badRequest(
        "linkedCashTransaction.idempotencyKey/integrationEventUid is already used by a different counterpartyType"
      );
    }
    if (parsePositiveInt(replayRow.counterparty_id) !== counterpartyId) {
      throw badRequest(
        "linkedCashTransaction.idempotencyKey/integrationEventUid is already used by a different counterpartyId"
      );
    }
    if (normalizeUpperText(replayRow.currency_code) !== normalizeUpperText(currencyCode)) {
      throw badRequest(
        "linkedCashTransaction.idempotencyKey/integrationEventUid is already used by a different currencyCode"
      );
    }
    if (!amountsAreEqual(replayRow.amount, amountTxn)) {
      throw badRequest(
        "linkedCashTransaction.idempotencyKey/integrationEventUid is already used by a different amount"
      );
    }
    return replayRow;
  }

  const txnNo = await generateCashTxnNoForLegalEntityYearTx({
    tenantId,
    legalEntityId,
    legalEntityCode: register.legal_entity_code,
    bookDate,
    runQuery,
  });
  try {
    const transactionId = await insertCashTransaction({
      payload: {
        tenantId,
        registerId,
        cashSessionId: parsePositiveInt(resolvedSession?.id) || null,
        txnNo,
        txnType,
        status: "DRAFT",
        txnDatetime,
        bookDate,
        amount: roundAmount(Number(amountTxn)).toFixed(6),
        currencyCode,
        description,
        referenceNo,
        sourceDocType: null,
        sourceDocId: null,
        sourceModule: "CARI",
        sourceEntityType: CARI_SETTLEMENT_INTENT_SOURCE_ENTITY_TYPE,
        sourceEntityId: toNullableString(settlementIdempotencyKey, 120),
        integrationLinkStatus: INTEGRATION_LINK_STATUS_PENDING,
        counterpartyType,
        counterpartyId,
        counterAccountId,
        counterCashRegisterId: null,
        linkedCariSettlementBatchId: null,
        linkedCariUnappliedCashId: null,
        reversalOfTransactionId: null,
        overrideCashControl: false,
        overrideReason: null,
        idempotencyKey: cashIdempotencyKey,
        integrationEventUid: cashIntegrationEventUid,
        userId,
        postedByUserId: null,
        postedAt: null,
      },
      runQuery,
    });
    const insertedRow = await fetchCashTransactionForSettlementLink({
      tenantId,
      cashTransactionId: transactionId,
      runQuery,
    });
    if (!insertedRow) {
      throw new Error("Linked cash transaction create failed");
    }
    return insertedRow;
  } catch (err) {
    if (!isDuplicateKeyError(err)) {
      throw err;
    }
    const replayByIdempotency = await findCashTransactionByIdempotency({
      tenantId,
      registerId,
      idempotencyKey: cashIdempotencyKey,
      runQuery,
    });
    if (replayByIdempotency) {
      return replayByIdempotency;
    }
    if (cashIntegrationEventUid) {
      const replayByEvent = await findCashTransactionByIntegrationEventUid({
        tenantId,
        integrationEventUid: cashIntegrationEventUid,
        runQuery,
      });
      if (replayByEvent) {
        return replayByEvent;
      }
    }
    if (isDuplicateKeyError(err, "uk_cash_txn_tenant_integration_event_uid")) {
      throw badRequest("linkedCashTransaction.integrationEventUid conflicts with an existing cash transaction");
    }
    throw badRequest("Duplicate linkedCashTransaction.idempotencyKey");
  }
}

async function enrichSettlementResultWithCashTransaction({
  tenantId,
  result,
  runQuery = query,
}) {
  if (!result) {
    return result;
  }
  const cashTransactionId =
    parsePositiveInt(result?.cashTransaction?.id) ||
    parsePositiveInt(result?.row?.cashTransactionId) ||
    parsePositiveInt(result?.row?.cash_transaction_id);
  if (!cashTransactionId) {
    return {
      ...result,
      cashTransaction: null,
    };
  }
  const cashRow = await fetchCashTransactionForSettlementLink({
    tenantId,
    cashTransactionId,
    runQuery,
  });
  return {
    ...result,
    cashTransaction: mapCashTransactionLinkRow(cashRow),
  };
}

async function fetchSettlementBatchRowByBankAttachIdempotency({
  tenantId,
  legalEntityId,
  bankAttachIdempotencyKey,
  runQuery = query,
  forUpdate = false,
}) {
  const normalizedKey = toNullableString(bankAttachIdempotencyKey, 100);
  if (!normalizedKey) {
    return null;
  }
  const lockClause = forUpdate ? "FOR UPDATE" : "";
  const result = await runQuery(
    `SELECT
       id,
       tenant_id,
       legal_entity_id,
       counterparty_id,
       cash_transaction_id,
       sequence_namespace,
       fiscal_year,
       sequence_no,
       settlement_no,
       settlement_date,
       status,
       total_allocated_txn,
       total_allocated_base,
       currency_code,
       posted_journal_entry_id,
       reversal_of_settlement_batch_id,
       bank_statement_line_id,
       bank_transaction_ref,
       bank_attach_idempotency_key,
       bank_apply_idempotency_key,
       source_module,
       source_entity_type,
       source_entity_id,
       integration_link_status,
       integration_event_uid,
       created_at,
       updated_at,
       posted_at,
       reversed_at
     FROM cari_settlement_batches
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND bank_attach_idempotency_key = ?
     LIMIT 1
     ${lockClause}`,
    [tenantId, legalEntityId, normalizedKey]
  );
  return result.rows?.[0] || null;
}

async function fetchUnappliedCashRowById({
  tenantId,
  legalEntityId,
  unappliedCashId,
  runQuery = query,
  forUpdate = false,
}) {
  const lockClause = forUpdate ? "FOR UPDATE" : "";
  const result = await runQuery(
    `SELECT
       id,
       tenant_id,
       legal_entity_id,
       counterparty_id,
       cash_transaction_id,
       cash_receipt_no,
       receipt_date,
       status,
       amount_txn,
       amount_base,
       residual_amount_txn,
       residual_amount_base,
       currency_code,
       posted_journal_entry_id,
       settlement_batch_id,
       reversal_of_unapplied_cash_id,
       bank_statement_line_id,
       bank_transaction_ref,
       bank_attach_idempotency_key,
       bank_apply_idempotency_key,
       source_module,
       source_entity_type,
       source_entity_id,
       integration_link_status,
       integration_event_uid,
       note,
       created_at,
       updated_at
     FROM cari_unapplied_cash
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1
     ${lockClause}`,
    [tenantId, legalEntityId, unappliedCashId]
  );
  return result.rows?.[0] || null;
}

async function fetchUnappliedCashRowByBankAttachIdempotency({
  tenantId,
  legalEntityId,
  bankAttachIdempotencyKey,
  runQuery = query,
  forUpdate = false,
}) {
  const normalizedKey = toNullableString(bankAttachIdempotencyKey, 100);
  if (!normalizedKey) {
    return null;
  }
  const lockClause = forUpdate ? "FOR UPDATE" : "";
  const result = await runQuery(
    `SELECT
       id,
       tenant_id,
       legal_entity_id,
       counterparty_id,
       cash_transaction_id,
       cash_receipt_no,
       receipt_date,
       status,
       amount_txn,
       amount_base,
       residual_amount_txn,
       residual_amount_base,
       currency_code,
       posted_journal_entry_id,
       settlement_batch_id,
       reversal_of_unapplied_cash_id,
       bank_statement_line_id,
       bank_transaction_ref,
       bank_attach_idempotency_key,
       bank_apply_idempotency_key,
       source_module,
       source_entity_type,
       source_entity_id,
       integration_link_status,
       integration_event_uid,
       note,
       created_at,
       updated_at
     FROM cari_unapplied_cash
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND bank_attach_idempotency_key = ?
     LIMIT 1
     ${lockClause}`,
    [tenantId, legalEntityId, normalizedKey]
  );
  return result.rows?.[0] || null;
}

async function findReversalSettlementBatchId({
  tenantId,
  originalSettlementBatchId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT id
     FROM cari_settlement_batches
     WHERE tenant_id = ?
       AND reversal_of_settlement_batch_id = ?
     LIMIT 1`,
    [tenantId, originalSettlementBatchId]
  );
  return parsePositiveInt(result.rows?.[0]?.id);
}

async function fetchOpenItemsForApply({
  tenantId,
  legalEntityId,
  counterpartyId,
  currencyCode,
  openItemIds = null,
  runQuery = query,
}) {
  const statuses = [OPEN_ITEM_STATUS_OPEN, OPEN_ITEM_STATUS_PARTIALLY_SETTLED];
  const params = [
    tenantId,
    legalEntityId,
    counterpartyId,
    normalizeUpperText(currencyCode),
    ...statuses,
  ];
  let whereExtra = "";
  if (Array.isArray(openItemIds) && openItemIds.length > 0) {
    whereExtra = ` AND oi.id IN (${openItemIds.map(() => "?").join(", ")})`;
    params.push(...openItemIds);
  }

  const result = await runQuery(
    `SELECT
       oi.id,
       oi.tenant_id,
       oi.legal_entity_id,
       oi.counterparty_id,
       oi.document_id,
       oi.status,
       oi.document_date,
       oi.due_date,
       oi.original_amount_txn,
       oi.original_amount_base,
       oi.residual_amount_txn,
       oi.residual_amount_base,
       oi.settled_amount_txn,
       oi.settled_amount_base,
       oi.currency_code,
       d.direction,
       d.document_type,
       d.status AS document_status
     FROM cari_open_items oi
     JOIN cari_documents d
       ON d.tenant_id = oi.tenant_id
      AND d.legal_entity_id = oi.legal_entity_id
      AND d.id = oi.document_id
     WHERE oi.tenant_id = ?
       AND oi.legal_entity_id = ?
       AND oi.counterparty_id = ?
       AND oi.currency_code = ?
       AND oi.status IN (?, ?)
       AND oi.residual_amount_txn > 0
       ${whereExtra}
     ORDER BY oi.id ASC
     FOR UPDATE`,
    params
  );
  return result.rows || [];
}

async function fetchOpenItemsByIdsForUpdate({
  tenantId,
  legalEntityId,
  openItemIds,
  runQuery = query,
}) {
  if (!Array.isArray(openItemIds) || openItemIds.length === 0) {
    return [];
  }
  const result = await runQuery(
    `SELECT
       oi.id,
       oi.tenant_id,
       oi.legal_entity_id,
       oi.counterparty_id,
       oi.document_id,
       oi.status,
       oi.document_date,
       oi.due_date,
       oi.original_amount_txn,
       oi.original_amount_base,
       oi.residual_amount_txn,
       oi.residual_amount_base,
       oi.settled_amount_txn,
       oi.settled_amount_base,
       oi.currency_code,
       d.direction,
       d.document_type,
       d.status AS document_status
     FROM cari_open_items oi
     JOIN cari_documents d
       ON d.tenant_id = oi.tenant_id
      AND d.legal_entity_id = oi.legal_entity_id
      AND d.id = oi.document_id
     WHERE oi.tenant_id = ?
       AND oi.legal_entity_id = ?
       AND oi.id IN (${openItemIds.map(() => "?").join(", ")})
     ORDER BY oi.id ASC
     FOR UPDATE`,
    [tenantId, legalEntityId, ...openItemIds]
  );
  return result.rows || [];
}

async function fetchUnappliedRowsForApply({
  tenantId,
  legalEntityId,
  counterpartyId,
  currencyCode,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
       id,
       tenant_id,
       legal_entity_id,
       counterparty_id,
       cash_transaction_id,
       cash_receipt_no,
       receipt_date,
       status,
       amount_txn,
       amount_base,
       residual_amount_txn,
       residual_amount_base,
       currency_code,
       note
     FROM cari_unapplied_cash
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND counterparty_id = ?
       AND currency_code = ?
       AND status IN (?, ?)
       AND residual_amount_txn > 0
     ORDER BY id ASC
     FOR UPDATE`,
    [
      tenantId,
      legalEntityId,
      counterpartyId,
      normalizeUpperText(currencyCode),
      UNAPPLIED_STATUS_UNAPPLIED,
      UNAPPLIED_STATUS_PARTIALLY_APPLIED,
    ]
  );
  return result.rows || [];
}

function normalizeOpenItemStatus({
  originalAmountTxn,
  residualAmountTxn,
  settledAmountTxn,
}) {
  const original = roundAmount(originalAmountTxn);
  const residual = roundAmount(residualAmountTxn);
  const settled = roundAmount(settledAmountTxn);

  if (amountsAreEqual(residual, 0)) {
    return OPEN_ITEM_STATUS_SETTLED;
  }
  if (amountsAreEqual(settled, 0) || amountsAreEqual(residual, original)) {
    return OPEN_ITEM_STATUS_OPEN;
  }
  return OPEN_ITEM_STATUS_PARTIALLY_SETTLED;
}

function normalizeUnappliedStatus({
  residualAmountTxn,
  amountTxn,
}) {
  const residual = roundAmount(residualAmountTxn);
  const amount = roundAmount(amountTxn);
  if (amountsAreEqual(residual, 0)) {
    return UNAPPLIED_STATUS_FULL;
  }
  if (amountsAreEqual(residual, amount)) {
    return UNAPPLIED_STATUS_UNAPPLIED;
  }
  return UNAPPLIED_STATUS_PARTIALLY_APPLIED;
}

function buildManualAllocationPlan(openItems, requestedAllocations) {
  if (!Array.isArray(requestedAllocations) || requestedAllocations.length === 0) {
    return [];
  }
  const byOpenItem = new Map();
  for (const allocation of requestedAllocations) {
    const openItemId = parsePositiveInt(allocation?.openItemId);
    const amountTxn = normalizeAmount(allocation?.amountTxn, "allocations[].amountTxn");
    if (!openItemId) {
      throw badRequest("allocations[].openItemId must be a positive integer");
    }
    const current = byOpenItem.get(openItemId) || 0;
    byOpenItem.set(openItemId, roundAmount(current + amountTxn));
  }

  const openItemById = new Map(openItems.map((row) => [parsePositiveInt(row.id), row]));
  const plan = [];
  for (const [openItemId, allocationTxn] of byOpenItem.entries()) {
    const row = openItemById.get(openItemId);
    if (!row) {
      throw badRequest(`openItemId=${openItemId} is not available for settlement`);
    }
    const residualTxn = normalizeAmount(row.residual_amount_txn, "openItem.residualAmountTxn");
    if (allocationTxn > residualTxn + AMOUNT_EPSILON) {
      throw badRequest(`allocation exceeds residual for openItemId=${openItemId}`);
    }
    plan.push({
      openItemId,
      allocationTxn,
      row,
    });
  }

  return plan.sort((left, right) => left.openItemId - right.openItemId);
}

function buildAutoAllocationPlan(openItems, availableFundsTxn) {
  let remainingFunds = roundAmount(availableFundsTxn);
  if (remainingFunds <= AMOUNT_EPSILON) {
    return [];
  }

  const ordered = [...openItems].sort((left, right) => {
    const leftDue = toDateOnlyString(left.due_date, "dueDate") || "9999-12-31";
    const rightDue = toDateOnlyString(right.due_date, "dueDate") || "9999-12-31";
    if (leftDue !== rightDue) {
      return leftDue < rightDue ? -1 : 1;
    }
    const leftDocDate = toDateOnlyString(left.document_date, "documentDate") || "9999-12-31";
    const rightDocDate =
      toDateOnlyString(right.document_date, "documentDate") || "9999-12-31";
    if (leftDocDate !== rightDocDate) {
      return leftDocDate < rightDocDate ? -1 : 1;
    }
    return Number(left.id) - Number(right.id);
  });

  const plan = [];
  for (const row of ordered) {
    if (remainingFunds <= AMOUNT_EPSILON) {
      break;
    }
    const residualTxn = normalizeAmount(row.residual_amount_txn, "openItem.residualAmountTxn");
    if (residualTxn <= AMOUNT_EPSILON) {
      continue;
    }
    const allocationTxn = roundAmount(Math.min(remainingFunds, residualTxn));
    if (allocationTxn <= AMOUNT_EPSILON) {
      continue;
    }
    plan.push({
      openItemId: parsePositiveInt(row.id),
      allocationTxn,
      row,
    });
    remainingFunds = roundAmount(remainingFunds - allocationTxn);
  }

  return plan;
}

function calculateHistoricalBaseAllocation(row, allocationTxn) {
  const residualTxn = normalizeAmount(row.residual_amount_txn, "openItem.residualAmountTxn");
  const residualBase = normalizeAmount(row.residual_amount_base, "openItem.residualAmountBase");
  const normalizedAllocation = normalizeAmount(allocationTxn, "allocationTxn");
  if (normalizedAllocation > residualTxn + AMOUNT_EPSILON) {
    throw badRequest(`allocation exceeds residual for openItemId=${row.id}`);
  }

  if (amountsAreEqual(normalizedAllocation, residualTxn)) {
    return residualBase;
  }

  if (residualTxn <= AMOUNT_EPSILON) {
    return 0;
  }

  const proportional = roundAmount((normalizedAllocation * residualBase) / residualTxn);
  return proportional > residualBase ? residualBase : proportional;
}

async function refreshDocumentBalancesTx({
  tx,
  tenantId,
  legalEntityId,
  documentIds,
}) {
  if (!Array.isArray(documentIds) || documentIds.length === 0) {
    return;
  }
  const uniqueDocumentIds = Array.from(
    new Set(documentIds.map((value) => parsePositiveInt(value)).filter(Boolean))
  ).sort((left, right) => left - right);

  for (const documentId of uniqueDocumentIds) {
    // eslint-disable-next-line no-await-in-loop
    const documentResult = await tx.query(
      `SELECT
         id,
         status,
         amount_txn,
         amount_base
       FROM cari_documents
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?
       LIMIT 1
       FOR UPDATE`,
      [tenantId, legalEntityId, documentId]
    );
    const documentRow = documentResult.rows?.[0] || null;
    if (!documentRow) {
      continue;
    }
    const existingStatus = normalizeUpperText(documentRow.status);
    if (["REVERSED", "CANCELLED", "DRAFT"].includes(existingStatus)) {
      continue;
    }

    // eslint-disable-next-line no-await-in-loop
    const aggregateResult = await tx.query(
      `SELECT
         COALESCE(SUM(residual_amount_txn), 0) AS residual_txn,
         COALESCE(SUM(residual_amount_base), 0) AS residual_base
       FROM cari_open_items
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND document_id = ?`,
      [tenantId, legalEntityId, documentId]
    );
    const residualTxn = roundAmount(aggregateResult.rows?.[0]?.residual_txn || 0);
    const residualBase = roundAmount(aggregateResult.rows?.[0]?.residual_base || 0);
    const amountTxn = normalizeAmount(documentRow.amount_txn, "document.amountTxn");

    let nextStatus = DOCUMENT_STATUS_PARTIALLY_SETTLED;
    if (amountsAreEqual(residualTxn, 0)) {
      nextStatus = DOCUMENT_STATUS_SETTLED;
    } else if (amountsAreEqual(residualTxn, amountTxn)) {
      nextStatus = DOCUMENT_STATUS_POSTED;
    }

    // eslint-disable-next-line no-await-in-loop
    await tx.query(
      `UPDATE cari_documents
       SET open_amount_txn = ?,
           open_amount_base = ?,
           status = ?
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?`,
      [residualTxn, residualBase, nextStatus, tenantId, legalEntityId, documentId]
    );
  }
}

async function fetchApplyAuditPayloadForSettlement({
  tenantId,
  settlementBatchId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT payload_json
     FROM audit_logs
     WHERE tenant_id = ?
       AND action = 'cari.settlement.apply'
       AND resource_type = 'cari_settlement_batch'
       AND resource_id = ?
     ORDER BY id DESC
     LIMIT 1`,
    [tenantId, String(settlementBatchId)]
  );
  const row = result.rows?.[0] || null;
  if (!row) {
    return null;
  }
  if (row.payload_json && typeof row.payload_json === "object") {
    return row.payload_json;
  }
  if (typeof row.payload_json === "string") {
    try {
      return JSON.parse(row.payload_json);
    } catch {
      return null;
    }
  }
  return null;
}

function buildSettlementPostingLines({
  direction,
  totalAmountTxn,
  totalAmountBase,
  controlAccountId,
  offsetAccountId,
  lineDescription,
  subledgerReferenceNo,
  currencyCode,
}) {
  const normalizedDirection = normalizeUpperText(direction);
  const normalizedCurrency = normalizeUpperText(currencyCode);
  const amountTxn = normalizeAmount(totalAmountTxn, "totalAllocatedTxn");
  const amountBase = normalizeAmount(totalAmountBase, "totalPostingAmountBase");

  if (normalizedDirection === "AR") {
    return [
      {
        accountId: parsePositiveInt(offsetAccountId),
        debitBase: amountBase,
        creditBase: 0,
        amountTxn,
        description: toNullableString(lineDescription, 255),
        subledgerReferenceNo: toNullableString(subledgerReferenceNo, 100),
        currencyCode: normalizedCurrency,
      },
      {
        accountId: parsePositiveInt(controlAccountId),
        debitBase: 0,
        creditBase: amountBase,
        amountTxn: roundAmount(amountTxn * -1),
        description: toNullableString(lineDescription, 255),
        subledgerReferenceNo: toNullableString(subledgerReferenceNo, 100),
        currencyCode: normalizedCurrency,
      },
    ];
  }

  if (normalizedDirection === "AP") {
    return [
      {
        accountId: parsePositiveInt(controlAccountId),
        debitBase: amountBase,
        creditBase: 0,
        amountTxn,
        description: toNullableString(lineDescription, 255),
        subledgerReferenceNo: toNullableString(subledgerReferenceNo, 100),
        currencyCode: normalizedCurrency,
      },
      {
        accountId: parsePositiveInt(offsetAccountId),
        debitBase: 0,
        creditBase: amountBase,
        amountTxn: roundAmount(amountTxn * -1),
        description: toNullableString(lineDescription, 255),
        subledgerReferenceNo: toNullableString(subledgerReferenceNo, 100),
        currencyCode: normalizedCurrency,
      },
    ];
  }

  throw badRequest("Settlement direction must be AR or AP");
}

async function loadSettlementResult({
  tenantId,
  settlementBatchId,
  includeApplyAudit = false,
  runQuery = query,
}) {
  const batchRow = await fetchSettlementBatchRow({
    tenantId,
    settlementBatchId,
    runQuery,
  });
  if (!batchRow) {
    throw badRequest("Settlement batch not found");
  }
  const allocations = await fetchSettlementAllocationsByBatchId({
    tenantId,
    settlementBatchId,
    runQuery,
  });
  const unappliedRowsResult = await runQuery(
    `SELECT
       id,
       tenant_id,
       legal_entity_id,
       counterparty_id,
       cash_receipt_no,
       receipt_date,
       status,
       amount_txn,
       amount_base,
       residual_amount_txn,
       residual_amount_base,
       currency_code,
       posted_journal_entry_id,
       settlement_batch_id,
       reversal_of_unapplied_cash_id,
       bank_statement_line_id,
       bank_transaction_ref,
       bank_attach_idempotency_key,
       bank_apply_idempotency_key,
       source_module,
       source_entity_type,
       source_entity_id,
       integration_link_status,
       integration_event_uid,
       note,
       created_at,
       updated_at
     FROM cari_unapplied_cash
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND settlement_batch_id = ?
     ORDER BY id ASC`,
    [tenantId, parsePositiveInt(batchRow.legal_entity_id), settlementBatchId]
  );
  const postedJournalEntryId = parsePositiveInt(batchRow.posted_journal_entry_id);
  const journalWithLines = postedJournalEntryId
    ? await fetchPostedJournalWithLines({
        tenantId,
        journalEntryId: postedJournalEntryId,
        runQuery,
      })
    : null;
  const applyAuditPayload = includeApplyAudit
    ? await fetchApplyAuditPayloadForSettlement({
        tenantId,
        settlementBatchId,
        runQuery,
      })
    : null;

  return {
    row: mapSettlementBatchRow(batchRow),
    allocations: allocations.map(mapAllocationRow),
    unappliedCash: (unappliedRowsResult.rows || []).map(mapUnappliedCashRow),
    journal: postedJournalEntryId
      ? {
          journalEntryId: postedJournalEntryId,
          lineCount: journalWithLines?.lines?.length || 0,
          lines: (journalWithLines?.lines || []).map((line) => ({
            id: parsePositiveInt(line.id),
            lineNo: Number(line.line_no),
            accountId: parsePositiveInt(line.account_id),
            amountTxn: toDecimalNumber(line.amount_txn),
            debitBase: toDecimalNumber(line.debit_base),
            creditBase: toDecimalNumber(line.credit_base),
            currencyCode: line.currency_code,
            description: line.description || null,
            subledgerReferenceNo: line.subledger_reference_no || null,
          })),
        }
      : null,
    applyAuditPayload,
  };
}

export async function resolveCariSettlementScope(settlementBatchId, tenantId) {
  const parsedSettlementBatchId = parsePositiveInt(settlementBatchId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedSettlementBatchId || !parsedTenantId) {
    return null;
  }

  const row = await fetchSettlementBatchRow({
    tenantId: parsedTenantId,
    settlementBatchId: parsedSettlementBatchId,
  });
  if (!row) {
    return null;
  }

  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: parsePositiveInt(row.legal_entity_id),
  };
}

export const CARI_SETTLEMENT_FOLLOW_UP_RISKS = FOLLOW_UP_RISKS;
export async function applyCariSettlement({
  req,
  payload,
  assertScopeAccess,
}) {
  const tenantId = payload.tenantId;
  const legalEntityId = payload.legalEntityId;
  const counterpartyId = payload.counterpartyId;
  const idempotencyKey = toNullableString(payload.idempotencyKey, 100);
  const requestedCashTransactionId = normalizeOptionalPositiveInt(
    payload.cashTransactionId,
    "cashTransactionId"
  );
  let effectiveCashTransactionId = requestedCashTransactionId;
  const paymentChannel = normalizePaymentChannel(payload.paymentChannel);
  const linkedCashTransaction = payload.linkedCashTransaction || null;
  const settlementDate = normalizeDateInput(payload.settlementDate, "settlementDate");
  const incomingAmountTxn = normalizeAmount(payload.incomingAmountTxn || 0, "incomingAmountTxn", {
    allowZero: true,
  });
  const useUnappliedCash = payload.useUnappliedCash !== false;
  const autoAllocate = Boolean(payload.autoAllocate);
  const bankApplyIdempotencyKey = toNullableString(payload.bankApplyIdempotencyKey, 100);
  const bankStatementLineId = normalizeOptionalPositiveInt(
    payload.bankStatementLineId,
    "bankStatementLineId"
  );
  const bankTransactionRef = toNullableString(payload.bankTransactionRef, 100);
  const initialIntegrationMetadata = resolveSettlementIntegrationMetadata({
    payload,
    idempotencyKey,
    cashTransactionId: requestedCashTransactionId,
    paymentChannel,
  });
  const integrationEventUid = initialIntegrationMetadata.integrationEventUid;

  if (!idempotencyKey) {
    throw badRequest("idempotencyKey is required");
  }
  if (bankApplyIdempotencyKey && !bankStatementLineId && !bankTransactionRef) {
    throw badRequest(
      "bankStatementLineId or bankTransactionRef is required when bankApplyIdempotencyKey is set"
    );
  }
  if (paymentChannel !== PAYMENT_CHANNEL_CASH && linkedCashTransaction) {
    throw badRequest("linkedCashTransaction is only supported when paymentChannel=CASH");
  }
  if (paymentChannel === PAYMENT_CHANNEL_CASH && requestedCashTransactionId && linkedCashTransaction) {
    throw badRequest(
      "linkedCashTransaction cannot be provided together with cashTransactionId when paymentChannel=CASH"
    );
  }
  if (paymentChannel === PAYMENT_CHANNEL_CASH && !requestedCashTransactionId && !linkedCashTransaction) {
    throw badRequest(
      "linkedCashTransaction is required when paymentChannel=CASH and cashTransactionId is not provided"
    );
  }
  if (paymentChannel === PAYMENT_CHANNEL_CASH && !requestedCashTransactionId && incomingAmountTxn <= AMOUNT_EPSILON) {
    throw badRequest(
      "incomingAmountTxn must be > 0 when paymentChannel=CASH and cashTransactionId is not provided"
    );
  }

  assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
  const legalEntity = await assertLegalEntityBelongsToTenant(
    tenantId,
    legalEntityId,
    "legalEntityId"
  );
  const settlementCurrencyCode = normalizeUpperText(payload.currencyCode);
  await assertCurrencyExists(settlementCurrencyCode, "currencyCode");

  const existingBatchIdByApply = await findSettlementBatchIdByApplyIdempotency({
    tenantId,
    legalEntityId,
    applyIdempotencyKey: idempotencyKey,
  });
  const existingBatchIdByBankApply = await findSettlementBatchIdByBankApplyIdempotency({
    tenantId,
    legalEntityId,
    bankApplyIdempotencyKey,
  });
  const existingBatchIdByEventUid = await findSettlementBatchIdByIntegrationEventUid({
    tenantId,
    legalEntityId,
    integrationEventUid,
  });
  const existingBatchIdByCashTxn = await findSettlementBatchIdByCashTransactionId({
    tenantId,
    legalEntityId,
    cashTransactionId: effectiveCashTransactionId,
  });
  const existingBatchIdCandidates = [
    existingBatchIdByApply,
    existingBatchIdByBankApply,
    existingBatchIdByEventUid,
    existingBatchIdByCashTxn,
  ].filter(Boolean);
  if (new Set(existingBatchIdCandidates).size > 1) {
    throw badRequest(
      "idempotencyKey, bankApplyIdempotencyKey, integrationEventUid, and cashTransactionId map to different settlements"
    );
  }
  const existingBatchId = existingBatchIdCandidates[0] || null;
  if (existingBatchId) {
    const replay = await loadSettlementResult({
      tenantId,
      settlementBatchId: existingBatchId,
      includeApplyAudit: true,
    });
    const replayWithCash = await enrichSettlementResultWithCashTransaction({
      tenantId,
      result: replay,
    });
    return {
      ...replayWithCash,
      idempotentReplay: true,
      followUpRisks: FOLLOW_UP_RISKS,
    };
  }

  const counterparty = await fetchCounterpartyRow({
    tenantId,
    legalEntityId,
    counterpartyId,
  });
  if (!counterparty) {
    throw badRequest("counterpartyId must belong to legalEntityId");
  }

  try {
    const created = await withTransaction(async (tx) => {
      const replayBatchIdByApply = await findSettlementBatchIdByApplyIdempotency({
        tenantId,
        legalEntityId,
        applyIdempotencyKey: idempotencyKey,
        runQuery: tx.query,
      });
      const replayBatchIdByBankApply = await findSettlementBatchIdByBankApplyIdempotency({
        tenantId,
        legalEntityId,
        bankApplyIdempotencyKey,
        runQuery: tx.query,
      });
      const replayBatchIdByEventUid = await findSettlementBatchIdByIntegrationEventUid({
        tenantId,
        legalEntityId,
        integrationEventUid,
        runQuery: tx.query,
      });
      const replayBatchIdByCashTxn = await findSettlementBatchIdByCashTransactionId({
        tenantId,
        legalEntityId,
        cashTransactionId: effectiveCashTransactionId,
        runQuery: tx.query,
      });
      const replayBatchCandidates = [
        replayBatchIdByApply,
        replayBatchIdByBankApply,
        replayBatchIdByEventUid,
        replayBatchIdByCashTxn,
      ].filter(Boolean);
      if (new Set(replayBatchCandidates).size > 1) {
        throw badRequest(
          "idempotencyKey, bankApplyIdempotencyKey, integrationEventUid, and cashTransactionId map to different settlements"
        );
      }
      const replayBatchId = replayBatchCandidates[0] || null;
      if (replayBatchId) {
        const replay = await loadSettlementResult({
          tenantId,
          settlementBatchId: replayBatchId,
          includeApplyAudit: true,
          runQuery: tx.query,
        });
        const replayWithCash = await enrichSettlementResultWithCashTransaction({
          tenantId,
          result: replay,
          runQuery: tx.query,
        });
        return {
          ...replayWithCash,
          idempotentReplay: true,
          followUpRisks: FOLLOW_UP_RISKS,
        };
      }

      const requestedOpenItemIds = Array.isArray(payload.allocations)
        ? payload.allocations
            .map((entry) => parsePositiveInt(entry?.openItemId))
            .filter(Boolean)
        : [];
      const lockedOpenItems = await fetchOpenItemsForApply({
        tenantId,
        legalEntityId,
        counterpartyId,
        currencyCode: settlementCurrencyCode,
        openItemIds: requestedOpenItemIds.length > 0 ? requestedOpenItemIds : null,
        runQuery: tx.query,
      });
      if (requestedOpenItemIds.length > 0 && lockedOpenItems.length !== requestedOpenItemIds.length) {
        throw badRequest("Some allocations target open items that are unavailable");
      }
      if (lockedOpenItems.length === 0) {
        throw badRequest("No open items are available for settlement");
      }

      const directions = new Set(
        lockedOpenItems
          .map((row) => normalizeUpperText(row.direction))
          .filter((value) => value === "AR" || value === "AP")
      );
      if (directions.size !== 1) {
        throw badRequest("Settlement apply supports one direction (AR or AP) per request");
      }
      const direction = Array.from(directions)[0];
      if (paymentChannel === PAYMENT_CHANNEL_CASH && !effectiveCashTransactionId) {
        const linkedCashTxn = await createOrReplaySettlementCashTransaction({
          req,
          tenantId,
          userId: payload.userId,
          legalEntityId,
          direction,
          counterpartyId,
          counterpartyRow: counterparty,
          currencyCode: settlementCurrencyCode,
          amountTxn: incomingAmountTxn,
          settlementDate,
          settlementIdempotencyKey: idempotencyKey,
          settlementIntegrationEventUid: integrationEventUid,
          linkedCashTransaction,
          assertScopeAccess,
          runQuery: tx.query,
        });
        effectiveCashTransactionId = parsePositiveInt(linkedCashTxn?.id);
        if (!effectiveCashTransactionId) {
          throw new Error("Failed to resolve linked cash transaction");
        }
      }

      if (effectiveCashTransactionId) {
        const replayBatchIdByCashTxn = await findSettlementBatchIdByCashTransactionId({
          tenantId,
          legalEntityId,
          cashTransactionId: effectiveCashTransactionId,
          runQuery: tx.query,
        });
        if (replayBatchIdByCashTxn) {
          const replay = await loadSettlementResult({
            tenantId,
            settlementBatchId: replayBatchIdByCashTxn,
            includeApplyAudit: true,
            runQuery: tx.query,
          });
          const replayWithCash = await enrichSettlementResultWithCashTransaction({
            tenantId,
            result: replay,
            runQuery: tx.query,
          });
          return {
            ...replayWithCash,
            idempotentReplay: true,
            followUpRisks: FOLLOW_UP_RISKS,
          };
        }
      }

      const integrationMetadata = resolveSettlementIntegrationMetadata({
        payload,
        idempotencyKey,
        cashTransactionId: effectiveCashTransactionId,
        paymentChannel,
        integrationEventUid,
      });

      if (effectiveCashTransactionId) {
        const linkedCashTransaction = await fetchCashTransactionForSettlementLink({
          tenantId,
          cashTransactionId: effectiveCashTransactionId,
          runQuery: tx.query,
          forUpdate: true,
        });
        if (!linkedCashTransaction) {
          throw badRequest("cashTransactionId not found for tenant");
        }
        if (parsePositiveInt(linkedCashTransaction.register_legal_entity_id) !== legalEntityId) {
          throw badRequest("cashTransactionId must belong to legalEntityId");
        }
        const expectedCashTxnType = DIRECTION_TO_CASH_TXN_TYPE[direction];
        if (normalizeUpperText(linkedCashTransaction.txn_type) !== expectedCashTxnType) {
          throw badRequest(
            `cashTransactionId must be txnType=${expectedCashTxnType} for settlement direction=${direction}`
          );
        }
        if (
          parsePositiveInt(linkedCashTransaction.counterparty_id) &&
          parsePositiveInt(linkedCashTransaction.counterparty_id) !== counterpartyId
        ) {
          throw badRequest("cashTransactionId counterparty does not match counterpartyId");
        }
        if (normalizeUpperText(linkedCashTransaction.currency_code) !== settlementCurrencyCode) {
          throw badRequest("cashTransactionId currency must match settlement currencyCode");
        }
        const linkedBatchOnCash = parsePositiveInt(
          linkedCashTransaction.linked_cari_settlement_batch_id
        );
        if (linkedBatchOnCash) {
          throw badRequest("cashTransactionId is already linked to a settlement batch");
        }
      }

      const fxPolicy = await resolveSettlementFxRate({
        tenantId,
        settlementDate,
        settlementCurrencyCode,
        functionalCurrencyCode: legalEntity.functional_currency_code,
        providedFxRate: payload.fxRate,
        fxFallbackMode: payload.fxFallbackMode,
        fxFallbackMaxDays: payload.fxFallbackMaxDays,
        runQuery: tx.query,
      });

      const unappliedRows = useUnappliedCash
        ? await fetchUnappliedRowsForApply({
            tenantId,
            legalEntityId,
            counterpartyId,
            currencyCode: settlementCurrencyCode,
            runQuery: tx.query,
          })
        : [];
      const unappliedAvailableTxn = roundAmount(
        unappliedRows.reduce(
          (total, row) => total + Number(row.residual_amount_txn || 0),
          0
        )
      );
      const totalAvailableFundsTxn = roundAmount(incomingAmountTxn + unappliedAvailableTxn);
      if (totalAvailableFundsTxn <= AMOUNT_EPSILON) {
        throw badRequest("No available funds from incomingAmountTxn or unapplied cash");
      }

      const requestedAllocations = Array.isArray(payload.allocations)
        ? payload.allocations
        : [];
      const manualPlan =
        requestedAllocations.length > 0
          ? buildManualAllocationPlan(lockedOpenItems, requestedAllocations)
          : [];
      let allocationPlan = manualPlan;
      if (autoAllocate || manualPlan.length === 0) {
        allocationPlan = buildAutoAllocationPlan(lockedOpenItems, totalAvailableFundsTxn);
      }
      if (allocationPlan.length === 0) {
        throw badRequest("No allocations can be produced for this settlement request");
      }

      const enrichedAllocations = allocationPlan.map((entry) => {
        const historicalBase = calculateHistoricalBaseAllocation(
          entry.row,
          entry.allocationTxn
        );
        const settlementBase = roundAmount(
          Number(entry.allocationTxn) * Number(fxPolicy.settlementFxRate)
        );
        return {
          ...entry,
          allocationBaseHistorical: historicalBase,
          allocationBaseSettlement: settlementBase,
        };
      });
      const totalAllocatedTxn = roundAmount(
        enrichedAllocations.reduce((sum, entry) => sum + Number(entry.allocationTxn), 0)
      );
      const totalAllocatedBaseHistorical = roundAmount(
        enrichedAllocations.reduce(
          (sum, entry) => sum + Number(entry.allocationBaseHistorical),
          0
        )
      );
      const totalAllocatedBaseSettlement = roundAmount(
        enrichedAllocations.reduce(
          (sum, entry) => sum + Number(entry.allocationBaseSettlement),
          0
        )
      );
      if (totalAllocatedTxn > totalAvailableFundsTxn + AMOUNT_EPSILON) {
        throw badRequest("Total allocations exceed incoming + unapplied available funds");
      }

      const unappliedConsumePlan = [];
      let remainingNeedTxn = totalAllocatedTxn;
      const unappliedConsumptionOrder = [...unappliedRows].sort((left, right) => {
        const leftReceipt = toDateOnlyString(left.receipt_date, "receiptDate") || "9999-12-31";
        const rightReceipt =
          toDateOnlyString(right.receipt_date, "receiptDate") || "9999-12-31";
        if (leftReceipt !== rightReceipt) {
          return leftReceipt < rightReceipt ? -1 : 1;
        }
        return Number(left.id) - Number(right.id);
      });

      for (const row of unappliedConsumptionOrder) {
        if (remainingNeedTxn <= AMOUNT_EPSILON) {
          break;
        }
        const rowResidualTxn = normalizeAmount(
          row.residual_amount_txn,
          "unapplied.residualAmountTxn"
        );
        if (rowResidualTxn <= AMOUNT_EPSILON) {
          continue;
        }
        const consumeTxn = roundAmount(Math.min(rowResidualTxn, remainingNeedTxn));
        if (consumeTxn <= AMOUNT_EPSILON) {
          continue;
        }
        const rowResidualBase = normalizeAmount(
          row.residual_amount_base,
          "unapplied.residualAmountBase"
        );
        const consumeBase = amountsAreEqual(consumeTxn, rowResidualTxn)
          ? rowResidualBase
          : roundAmount((consumeTxn * rowResidualBase) / rowResidualTxn);
        unappliedConsumePlan.push({
          row,
          consumeTxn,
          consumeBase: consumeBase > rowResidualBase ? rowResidualBase : consumeBase,
        });
        remainingNeedTxn = roundAmount(remainingNeedTxn - consumeTxn);
      }

      if (remainingNeedTxn > incomingAmountTxn + AMOUNT_EPSILON) {
        throw badRequest(
          "incomingAmountTxn is insufficient after unapplied consumption for requested allocations"
        );
      }
      const incomingUsedTxn = roundAmount(Math.max(0, remainingNeedTxn));
      const incomingResidualTxn = roundAmount(Math.max(0, incomingAmountTxn - incomingUsedTxn));
      const incomingResidualBase = roundAmount(
        incomingResidualTxn * Number(fxPolicy.settlementFxRate)
      );
      const realizedFxNetBase = roundAmount(
        totalAllocatedBaseSettlement - totalAllocatedBaseHistorical
      );

      const sequence = await reserveSettlementSequence({
        tenantId,
        legalEntityId,
        settlementDate,
        runQuery: tx.query,
      });
      const postingSourceContext = deriveSettlementPostingSourceContext({
        paymentChannel,
        cashTransactionId: effectiveCashTransactionId,
        sourceModule: integrationMetadata.sourceModule,
        unappliedConsumedCount: unappliedConsumePlan.length,
      });
      const postingAccounts = await resolveSettlementPostingAccounts({
        tenantId,
        legalEntityId,
        direction,
        sourceContext: postingSourceContext,
        counterpartyRow: counterparty,
        runQuery: tx.query,
      });
      const journalContext = await resolveBookAndOpenPeriodForDate({
        tenantId,
        legalEntityId,
        targetDate: settlementDate,
        runQuery: tx.query,
      });

      const postingLines = buildSettlementPostingLines({
        direction,
        totalAmountTxn: totalAllocatedTxn,
        totalAmountBase: totalAllocatedBaseSettlement,
        controlAccountId: postingAccounts.controlAccountId,
        offsetAccountId: postingAccounts.offsetAccountId,
        lineDescription: `Cari settlement ${sequence.settlementNo}`.slice(0, 255),
        subledgerReferenceNo: `${CARI_SETTLEMENT_REFERENCE_PREFIX}${sequence.settlementNo}`,
        currencyCode: settlementCurrencyCode,
      });
      const journalResult = await insertPostedJournalWithLinesTx(tx, {
        tenantId,
        legalEntityId,
        bookId: journalContext.bookId,
        fiscalPeriodId: journalContext.fiscalPeriodId,
        userId: payload.userId,
        journalNo: buildCariJournalNo("CARI-SETTLE", sequence.sequenceNo),
        entryDate: settlementDate,
        documentDate: settlementDate,
        currencyCode: settlementCurrencyCode,
        description: `Cari settlement apply ${sequence.settlementNo}`.slice(0, 500),
        referenceNo: toNullableString(sequence.settlementNo, 100),
        lines: postingLines,
      });

      const settlementInsert = await tx.query(
        `INSERT INTO cari_settlement_batches (
            tenant_id,
            legal_entity_id,
            counterparty_id,
            cash_transaction_id,
            sequence_namespace,
            fiscal_year,
            sequence_no,
            settlement_no,
            settlement_date,
            status,
            total_allocated_txn,
            total_allocated_base,
            currency_code,
            posted_journal_entry_id,
            reversal_of_settlement_batch_id,
            bank_statement_line_id,
            bank_transaction_ref,
            bank_attach_idempotency_key,
            bank_apply_idempotency_key,
            source_module,
            source_entity_type,
            source_entity_id,
            integration_link_status,
            integration_event_uid,
            posted_at
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, NULL, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
        [
          tenantId,
          legalEntityId,
          counterpartyId,
          effectiveCashTransactionId,
          sequence.sequenceNamespace,
          sequence.fiscalYear,
          sequence.sequenceNo,
          sequence.settlementNo,
          settlementDate,
          SETTLEMENT_STATUS_POSTED,
          totalAllocatedTxn,
          totalAllocatedBaseHistorical,
          settlementCurrencyCode,
          journalResult.journalEntryId,
          bankStatementLineId,
          bankTransactionRef,
          bankApplyIdempotencyKey,
          integrationMetadata.sourceModule,
          integrationMetadata.sourceEntityType,
          integrationMetadata.sourceEntityId,
          integrationMetadata.integrationLinkStatus,
          integrationMetadata.integrationEventUid,
        ]
      );
      const settlementBatchId = parsePositiveInt(settlementInsert.rows?.insertId);
      if (!settlementBatchId) {
        throw new Error("Failed to create settlement batch");
      }

      const allocationInsertOrder = [...enrichedAllocations].sort((left, right) => {
        const leftDue = toDateOnlyString(left.row.due_date, "dueDate") || "9999-12-31";
        const rightDue = toDateOnlyString(right.row.due_date, "dueDate") || "9999-12-31";
        if (leftDue !== rightDue) {
          return leftDue < rightDue ? -1 : 1;
        }
        const leftDocDate = toDateOnlyString(left.row.document_date, "documentDate") || "9999-12-31";
        const rightDocDate =
          toDateOnlyString(right.row.document_date, "documentDate") || "9999-12-31";
        if (leftDocDate !== rightDocDate) {
          return leftDocDate < rightDocDate ? -1 : 1;
        }
        return Number(left.openItemId) - Number(right.openItemId);
      });

      for (let index = 0; index < allocationInsertOrder.length; index += 1) {
        const entry = allocationInsertOrder[index];
        const applyIdempotencyKey = index === 0 ? idempotencyKey : null;
        const allocationBankApplyIdempotencyKey =
          index === 0 ? bankApplyIdempotencyKey : null;
        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO cari_settlement_allocations (
              tenant_id,
              legal_entity_id,
              settlement_batch_id,
              open_item_id,
              allocation_date,
              allocation_amount_txn,
              allocation_amount_base,
              apply_idempotency_key,
              bank_statement_line_id,
              bank_apply_idempotency_key,
              note
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            tenantId,
            legalEntityId,
            settlementBatchId,
            entry.openItemId,
            settlementDate,
            entry.allocationTxn,
            entry.allocationBaseHistorical,
            applyIdempotencyKey,
            bankStatementLineId,
            allocationBankApplyIdempotencyKey,
            toNullableString(payload.note, 500),
          ]
        );
      }

      const touchedDocumentIds = [];
      for (const entry of enrichedAllocations) {
        const row = entry.row;
        const currentResidualTxn = normalizeAmount(
          row.residual_amount_txn,
          "openItem.residualAmountTxn"
        );
        const currentResidualBase = normalizeAmount(
          row.residual_amount_base,
          "openItem.residualAmountBase"
        );
        const originalAmountTxn = normalizeAmount(
          row.original_amount_txn,
          "openItem.originalAmountTxn"
        );
        const originalAmountBase = normalizeAmount(
          row.original_amount_base,
          "openItem.originalAmountBase"
        );
        let nextResidualTxn = roundAmount(currentResidualTxn - entry.allocationTxn);
        let nextResidualBase = roundAmount(
          currentResidualBase - entry.allocationBaseHistorical
        );
        if (nextResidualTxn < 0 && Math.abs(nextResidualTxn) <= AMOUNT_EPSILON) {
          nextResidualTxn = 0;
        }
        if (nextResidualBase < 0 && Math.abs(nextResidualBase) <= AMOUNT_EPSILON) {
          nextResidualBase = 0;
        }
        if (nextResidualTxn < -AMOUNT_EPSILON || nextResidualBase < -AMOUNT_EPSILON) {
          throw badRequest(`allocation exceeds residual for openItemId=${entry.openItemId}`);
        }
        const nextSettledTxn = roundAmount(originalAmountTxn - nextResidualTxn);
        const nextSettledBase = roundAmount(originalAmountBase - nextResidualBase);
        const nextStatus = normalizeOpenItemStatus({
          originalAmountTxn,
          residualAmountTxn: nextResidualTxn,
          settledAmountTxn: nextSettledTxn,
        });

        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `UPDATE cari_open_items
           SET status = ?,
               residual_amount_txn = ?,
               residual_amount_base = ?,
               settled_amount_txn = ?,
               settled_amount_base = ?
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?`,
          [
            nextStatus,
            nextResidualTxn,
            nextResidualBase,
            nextSettledTxn,
            nextSettledBase,
            tenantId,
            legalEntityId,
            entry.openItemId,
          ]
        );
        touchedDocumentIds.push(parsePositiveInt(row.document_id));
      }

      await refreshDocumentBalancesTx({
        tx,
        tenantId,
        legalEntityId,
        documentIds: touchedDocumentIds,
      });

      for (const consumeEntry of unappliedConsumePlan) {
        const row = consumeEntry.row;
        const rowResidualTxn = normalizeAmount(
          row.residual_amount_txn,
          "unapplied.residualAmountTxn"
        );
        const rowResidualBase = normalizeAmount(
          row.residual_amount_base,
          "unapplied.residualAmountBase"
        );
        const amountTxn = normalizeAmount(row.amount_txn, "unapplied.amountTxn");
        let nextResidualTxn = roundAmount(rowResidualTxn - consumeEntry.consumeTxn);
        let nextResidualBase = roundAmount(rowResidualBase - consumeEntry.consumeBase);
        if (nextResidualTxn < 0 && Math.abs(nextResidualTxn) <= AMOUNT_EPSILON) {
          nextResidualTxn = 0;
        }
        if (nextResidualBase < 0 && Math.abs(nextResidualBase) <= AMOUNT_EPSILON) {
          nextResidualBase = 0;
        }
        if (nextResidualTxn < -AMOUNT_EPSILON || nextResidualBase < -AMOUNT_EPSILON) {
          throw badRequest(`Unapplied cash over-consume detected for id=${row.id}`);
        }
        const nextStatus = normalizeUnappliedStatus({
          residualAmountTxn: nextResidualTxn,
          amountTxn,
        });
        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `UPDATE cari_unapplied_cash
           SET status = ?,
               residual_amount_txn = ?,
               residual_amount_base = ?,
               note = ?
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?`,
          [
            nextStatus,
            nextResidualTxn,
            nextResidualBase,
            toNullableString(
              `${row.note || ""}${row.note ? " | " : ""}Applied by settlement ${
                sequence.settlementNo
              }`,
              500
            ),
            tenantId,
            legalEntityId,
            parsePositiveInt(row.id),
          ]
        );
      }

      let createdUnappliedCashId = null;
      if (incomingResidualTxn > AMOUNT_EPSILON) {
        const unappliedInsert = await tx.query(
          `INSERT INTO cari_unapplied_cash (
              tenant_id,
              legal_entity_id,
              counterparty_id,
              cash_transaction_id,
              cash_receipt_no,
              receipt_date,
              status,
              amount_txn,
              amount_base,
              residual_amount_txn,
              residual_amount_base,
              currency_code,
              posted_journal_entry_id,
              settlement_batch_id,
              reversal_of_unapplied_cash_id,
              bank_statement_line_id,
              bank_transaction_ref,
              bank_attach_idempotency_key,
              bank_apply_idempotency_key,
              source_module,
              source_entity_type,
              source_entity_id,
              integration_link_status,
              integration_event_uid,
              note
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)`,
          [
            tenantId,
            legalEntityId,
            counterpartyId,
            effectiveCashTransactionId,
            buildUnappliedReceiptNo(sequence.settlementNo),
            settlementDate,
            UNAPPLIED_STATUS_UNAPPLIED,
            incomingResidualTxn,
            incomingResidualBase,
            incomingResidualTxn,
            incomingResidualBase,
            settlementCurrencyCode,
            settlementBatchId,
            bankStatementLineId,
            bankTransactionRef,
            bankApplyIdempotencyKey,
            integrationMetadata.sourceModule,
            integrationMetadata.sourceEntityType,
            integrationMetadata.sourceEntityId,
            integrationMetadata.integrationLinkStatus,
            integrationMetadata.integrationEventUid,
            toNullableString(
              `Residual unapplied from settlement ${sequence.settlementNo}`,
              500
            ),
          ]
        );
        createdUnappliedCashId = parsePositiveInt(unappliedInsert.rows?.insertId);
      }

      if (effectiveCashTransactionId) {
        const cashLinkUpdate = await tx.query(
          `UPDATE cash_transactions
           SET linked_cari_settlement_batch_id = ?,
               linked_cari_unapplied_cash_id = COALESCE(?, linked_cari_unapplied_cash_id),
               integration_link_status = CASE
                 WHEN integration_link_status IN ('UNLINKED', 'PENDING') THEN 'LINKED'
                 ELSE integration_link_status
               END,
               source_module = COALESCE(source_module, 'CARI'),
               source_entity_type = COALESCE(source_entity_type, 'cari_settlement_batch'),
               source_entity_id = COALESCE(source_entity_id, ?),
               integration_event_uid = COALESCE(integration_event_uid, ?)
           WHERE tenant_id = ?
             AND id = ?
             AND (linked_cari_settlement_batch_id IS NULL OR linked_cari_settlement_batch_id = ?)
             AND (
               ? IS NULL
               OR linked_cari_unapplied_cash_id IS NULL
               OR linked_cari_unapplied_cash_id = ?
             )`,
          [
            settlementBatchId,
            createdUnappliedCashId,
            String(settlementBatchId),
            integrationMetadata.integrationEventUid,
            tenantId,
            effectiveCashTransactionId,
            settlementBatchId,
            createdUnappliedCashId,
            createdUnappliedCashId,
          ]
        );
        if (Number(cashLinkUpdate.rows?.affectedRows || 0) === 0) {
          throw badRequest("cashTransactionId already has conflicting Cari integration links");
        }
      }

      await insertAuditLog({
        req,
        runQuery: tx.query,
        tenantId,
        userId: payload.userId,
        action: "cari.settlement.apply",
        legalEntityId,
        resourceId: settlementBatchId,
        payload: {
          settlementBatchId,
          settlementNo: sequence.settlementNo,
          idempotencyKey,
          integrationEventUid,
          sourceModule: integrationMetadata.sourceModule,
          sourceEntityType: integrationMetadata.sourceEntityType,
          sourceEntityId: integrationMetadata.sourceEntityId,
          integrationLinkStatus: integrationMetadata.integrationLinkStatus,
          cashTransactionId: effectiveCashTransactionId,
          paymentChannel,
          linkedCashTransactionRequested: Boolean(paymentChannel === PAYMENT_CHANNEL_CASH),
          bankApplyIdempotencyKey,
          bankStatementLineId,
          bankTransactionRef,
          counterpartyId,
          direction,
          settlementDate,
          incomingAmountTxn,
          totalAllocatedTxn,
          totalAllocatedBaseHistorical,
          totalAllocatedBaseSettlement,
          realizedFxNetBase,
          settlementFxRate: fxPolicy.settlementFxRate,
          settlementFxSource: fxPolicy.source,
          settlementFxFallbackMode: fxPolicy.fallbackMode,
          settlementFxFallbackMaxDays: fxPolicy.fallbackMaxDays,
          allocations: enrichedAllocations.map((entry) => ({
            openItemId: entry.openItemId,
            documentId: parsePositiveInt(entry.row.document_id),
            allocationTxn: entry.allocationTxn,
            allocationBaseHistorical: entry.allocationBaseHistorical,
            allocationBaseSettlement: entry.allocationBaseSettlement,
          })),
          unappliedConsumed: unappliedConsumePlan.map((entry) => ({
            unappliedCashId: parsePositiveInt(entry.row.id),
            consumeTxn: entry.consumeTxn,
            consumeBase: entry.consumeBase,
          })),
          postingSourceContext: postingAccounts.sourceContext,
          postingControlPurposeCode: postingAccounts.controlPurposeCode,
          postingOffsetPurposeCode: postingAccounts.offsetPurposeCode,
          createdUnappliedCashId,
          followUpRisks: FOLLOW_UP_RISKS,
        },
      });
      if (bankApplyIdempotencyKey) {
        await insertAuditLog({
          req,
          runQuery: tx.query,
          tenantId,
          userId: payload.userId,
          action: "cari.bank.apply",
          legalEntityId,
          resourceId: settlementBatchId,
          payload: {
            settlementBatchId,
            settlementNo: sequence.settlementNo,
            idempotencyKey,
            integrationEventUid,
            cashTransactionId: effectiveCashTransactionId,
            bankApplyIdempotencyKey,
            bankStatementLineId,
            bankTransactionRef,
            followUpRisks: FOLLOW_UP_RISKS,
          },
        });
      }

      const result = await loadSettlementResult({
        tenantId,
        settlementBatchId,
        includeApplyAudit: true,
        runQuery: tx.query,
      });
      const resultWithCash = await enrichSettlementResultWithCashTransaction({
        tenantId,
        result,
        runQuery: tx.query,
      });

      return {
        ...resultWithCash,
        idempotentReplay: false,
        followUpRisks: FOLLOW_UP_RISKS,
        metrics: {
          createdCashTransactionId: effectiveCashTransactionId,
          totalAllocatedTxn,
          totalAllocatedBaseHistorical,
          totalAllocatedBaseSettlement,
          realizedFxNetBase,
          settlementFxRate: fxPolicy.settlementFxRate,
          settlementFxSource: fxPolicy.source,
          fxRateDate: fxPolicy.rateDate,
          journalPurposeAccounts: {
            sourceContext: postingAccounts.sourceContext,
            controlAccountId: postingAccounts.controlAccountId,
            offsetAccountId: postingAccounts.offsetAccountId,
            controlAccountCode: postingAccounts.controlAccountCode,
            offsetAccountCode: postingAccounts.offsetAccountCode,
            controlPurposeCode: postingAccounts.controlPurposeCode,
            offsetPurposeCode: postingAccounts.offsetPurposeCode,
          },
          settlementFxFallbackMode: fxPolicy.fallbackMode,
          settlementFxFallbackMaxDays: fxPolicy.fallbackMaxDays,
        },
      };
    });

    return created;
  } catch (err) {
    const duplicateApplyIdempotency = isDuplicateKeyError(err, "uk_cari_alloc_apply_idempo");
    const duplicateBankApplyIdempotency = isDuplicateKeyError(err, "uk_cari_alloc_bank_apply_idempo");
    const duplicateSettlementBankApply = isDuplicateKeyError(
      err,
      "uk_cari_settle_batches_bank_apply_idempo"
    );
    const duplicateUnappliedBankApply = isDuplicateKeyError(err, "uk_cari_unap_bank_apply_idempo");
    const duplicateSettlementEventUid = isDuplicateKeyError(
      err,
      "uk_cari_settle_batches_tenant_event_uid"
    );
    const duplicateUnappliedEventUid = isDuplicateKeyError(err, "uk_cari_unap_tenant_event_uid");
    const duplicateSettlementCashTxn = isDuplicateKeyError(
      err,
      "uk_cari_settle_batches_tenant_cash_txn"
    );
    const duplicateUnappliedCashTxn = isDuplicateKeyError(err, "uk_cari_unap_tenant_cash_txn");
    const duplicateCashEventUid = isDuplicateKeyError(
      err,
      "uk_cash_txn_tenant_integration_event_uid"
    );

    if (
      duplicateApplyIdempotency ||
      duplicateBankApplyIdempotency ||
      duplicateSettlementBankApply ||
      duplicateUnappliedBankApply ||
      duplicateSettlementEventUid ||
      duplicateUnappliedEventUid ||
      duplicateSettlementCashTxn ||
      duplicateUnappliedCashTxn ||
      duplicateCashEventUid
    ) {
      const replayBatchIdByApply = await findSettlementBatchIdByApplyIdempotency({
        tenantId,
        legalEntityId,
        applyIdempotencyKey: idempotencyKey,
      });
      const replayBatchIdByBankApply = await findSettlementBatchIdByBankApplyIdempotency({
        tenantId,
        legalEntityId,
        bankApplyIdempotencyKey,
      });
      const replayBatchIdByEventUid = await findSettlementBatchIdByIntegrationEventUid({
        tenantId,
        legalEntityId,
        integrationEventUid,
      });
      const replayBatchIdByCashTxn = await findSettlementBatchIdByCashTransactionId({
        tenantId,
        legalEntityId,
        cashTransactionId: effectiveCashTransactionId,
      });
      const replayBatchCandidates = [
        replayBatchIdByApply,
        replayBatchIdByBankApply,
        replayBatchIdByEventUid,
        replayBatchIdByCashTxn,
      ].filter(Boolean);
      if (new Set(replayBatchCandidates).size > 1) {
        throw badRequest(
          "idempotencyKey, bankApplyIdempotencyKey, integrationEventUid, and cashTransactionId map to different settlements"
        );
      }
      const replayBatchId = replayBatchCandidates[0] || null;
      if (replayBatchId) {
        const replay = await loadSettlementResult({
          tenantId,
          settlementBatchId: replayBatchId,
          includeApplyAudit: true,
        });
        const replayWithCash = await enrichSettlementResultWithCashTransaction({
          tenantId,
          result: replay,
        });
        return {
          ...replayWithCash,
          idempotentReplay: true,
          followUpRisks: FOLLOW_UP_RISKS,
        };
      }
      if (effectiveCashTransactionId && (duplicateSettlementCashTxn || duplicateUnappliedCashTxn)) {
        throw badRequest("cashTransactionId is already linked to another Cari settlement/unapplied row");
      }
      if (integrationEventUid && (duplicateSettlementEventUid || duplicateUnappliedEventUid)) {
        throw badRequest("Duplicate integrationEventUid");
      }
      if (integrationEventUid && duplicateCashEventUid) {
        throw badRequest("integrationEventUid conflicts with an existing cash transaction");
      }
      if (bankApplyIdempotencyKey) {
        throw badRequest("Duplicate settlement bank-apply idempotency key");
      }
      throw badRequest("Duplicate settlement apply idempotency key");
    }
    throw err;
  }
}

export async function attachCariBankReference({
  req,
  payload,
  assertScopeAccess,
}) {
  const tenantId = payload.tenantId;
  const userId = payload.userId;
  const legalEntityId = payload.legalEntityId;
  const targetType = normalizeUpperText(payload.targetType);
  const settlementBatchId = parsePositiveInt(payload.settlementBatchId);
  const unappliedCashId = parsePositiveInt(payload.unappliedCashId);
  const idempotencyKey = toNullableString(payload.idempotencyKey, 100);
  const bankStatementLineId = normalizeOptionalPositiveInt(
    payload.bankStatementLineId,
    "bankStatementLineId"
  );
  const bankTransactionRef = toNullableString(payload.bankTransactionRef, 100);
  const note = toNullableString(payload.note, 500);

  if (!idempotencyKey) {
    throw badRequest("idempotencyKey is required");
  }
  if (!bankStatementLineId && !bankTransactionRef) {
    throw badRequest("bankStatementLineId or bankTransactionRef is required");
  }
  if (
    targetType !== BANK_ATTACH_TARGET_SETTLEMENT &&
    targetType !== BANK_ATTACH_TARGET_UNAPPLIED_CASH
  ) {
    throw badRequest("targetType must be SETTLEMENT or UNAPPLIED_CASH");
  }

  assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
  await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");

  try {
    const result = await withTransaction(async (tx) => {
      if (targetType === BANK_ATTACH_TARGET_SETTLEMENT) {
        if (!settlementBatchId) {
          throw badRequest("settlementBatchId is required when targetType=SETTLEMENT");
        }

        const replayByKey = await fetchSettlementBatchRowByBankAttachIdempotency({
          tenantId,
          legalEntityId,
          bankAttachIdempotencyKey: idempotencyKey,
          runQuery: tx.query,
          forUpdate: true,
        });
        if (replayByKey) {
          if (parsePositiveInt(replayByKey.id) !== settlementBatchId) {
            throw badRequest(
              "idempotencyKey is already used for a different settlementBatchId"
            );
          }
          const replayLink = resolveBankLinkFields({
            targetLabel: `settlementBatchId=${settlementBatchId}`,
            existingBankStatementLineId: replayByKey.bank_statement_line_id,
            existingBankTransactionRef: replayByKey.bank_transaction_ref,
            requestedBankStatementLineId: bankStatementLineId,
            requestedBankTransactionRef: bankTransactionRef,
          });
          const replayChanged =
            parsePositiveInt(replayByKey.bank_statement_line_id) !==
              replayLink.nextBankStatementLineId ||
            !textsEqual(replayByKey.bank_transaction_ref, replayLink.nextBankTransactionRef);
          if (replayChanged) {
            throw badRequest(
              "idempotencyKey replay payload does not match existing settlement bank link"
            );
          }
          return {
            targetType,
            settlement: mapSettlementBatchRow(replayByKey),
            unappliedCash: null,
            idempotentReplay: true,
          };
        }

        const targetRow = await fetchSettlementBatchRow({
          tenantId,
          settlementBatchId,
          runQuery: tx.query,
          forUpdate: true,
        });
        if (!targetRow) {
          throw badRequest("Settlement batch not found");
        }
        if (parsePositiveInt(targetRow.legal_entity_id) !== legalEntityId) {
          throw badRequest("settlementBatchId must belong to legalEntityId");
        }

        const existingAttachIdempotencyKey = toNullableString(
          targetRow.bank_attach_idempotency_key,
          100
        );
        if (existingAttachIdempotencyKey && existingAttachIdempotencyKey !== idempotencyKey) {
          throw badRequest("Settlement batch already has a bank attach idempotency key");
        }

        const resolvedLink = resolveBankLinkFields({
          targetLabel: `settlementBatchId=${settlementBatchId}`,
          existingBankStatementLineId: targetRow.bank_statement_line_id,
          existingBankTransactionRef: targetRow.bank_transaction_ref,
          requestedBankStatementLineId: bankStatementLineId,
          requestedBankTransactionRef: bankTransactionRef,
        });
        const alreadyAttachedWithSameKey =
          existingAttachIdempotencyKey === idempotencyKey &&
          parsePositiveInt(targetRow.bank_statement_line_id) ===
            resolvedLink.nextBankStatementLineId &&
          textsEqual(targetRow.bank_transaction_ref, resolvedLink.nextBankTransactionRef);
        if (alreadyAttachedWithSameKey) {
          return {
            targetType,
            settlement: mapSettlementBatchRow(targetRow),
            unappliedCash: null,
            idempotentReplay: true,
          };
        }

        await tx.query(
          `UPDATE cari_settlement_batches
           SET bank_statement_line_id = ?,
               bank_transaction_ref = ?,
               bank_attach_idempotency_key = ?
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?`,
          [
            resolvedLink.nextBankStatementLineId,
            resolvedLink.nextBankTransactionRef,
            idempotencyKey,
            tenantId,
            legalEntityId,
            settlementBatchId,
          ]
        );

        const updatedRow = await fetchSettlementBatchRow({
          tenantId,
          settlementBatchId,
          runQuery: tx.query,
          forUpdate: true,
        });
        if (!updatedRow) {
          throw badRequest("Settlement batch not found after bank attach update");
        }

        await insertAuditLog({
          req,
          runQuery: tx.query,
          tenantId,
          userId,
          action: "cari.bank.attach",
          resourceType: RESOURCE_TYPE_SETTLEMENT_BATCH,
          legalEntityId,
          resourceId: settlementBatchId,
          payload: {
            targetType: BANK_ATTACH_TARGET_SETTLEMENT,
            settlementBatchId,
            bankStatementLineId: resolvedLink.nextBankStatementLineId,
            bankTransactionRef: resolvedLink.nextBankTransactionRef,
            idempotencyKey,
            note,
          },
        });

        return {
          targetType,
          settlement: mapSettlementBatchRow(updatedRow),
          unappliedCash: null,
          idempotentReplay: false,
        };
      }

      if (!unappliedCashId) {
        throw badRequest("unappliedCashId is required when targetType=UNAPPLIED_CASH");
      }
      const replayByKey = await fetchUnappliedCashRowByBankAttachIdempotency({
        tenantId,
        legalEntityId,
        bankAttachIdempotencyKey: idempotencyKey,
        runQuery: tx.query,
        forUpdate: true,
      });
      if (replayByKey) {
        if (parsePositiveInt(replayByKey.id) !== unappliedCashId) {
          throw badRequest("idempotencyKey is already used for a different unappliedCashId");
        }
        const replayLink = resolveBankLinkFields({
          targetLabel: `unappliedCashId=${unappliedCashId}`,
          existingBankStatementLineId: replayByKey.bank_statement_line_id,
          existingBankTransactionRef: replayByKey.bank_transaction_ref,
          requestedBankStatementLineId: bankStatementLineId,
          requestedBankTransactionRef: bankTransactionRef,
        });
        const replayChanged =
          parsePositiveInt(replayByKey.bank_statement_line_id) !==
            replayLink.nextBankStatementLineId ||
          !textsEqual(replayByKey.bank_transaction_ref, replayLink.nextBankTransactionRef);
        if (replayChanged) {
          throw badRequest(
            "idempotencyKey replay payload does not match existing unapplied cash bank link"
          );
        }
        return {
          targetType,
          settlement: null,
          unappliedCash: mapUnappliedCashRow(replayByKey),
          idempotentReplay: true,
        };
      }

      const targetRow = await fetchUnappliedCashRowById({
        tenantId,
        legalEntityId,
        unappliedCashId,
        runQuery: tx.query,
        forUpdate: true,
      });
      if (!targetRow) {
        throw badRequest("Unapplied cash row not found");
      }
      if (parsePositiveInt(targetRow.legal_entity_id) !== legalEntityId) {
        throw badRequest("unappliedCashId must belong to legalEntityId");
      }

      const existingAttachIdempotencyKey = toNullableString(
        targetRow.bank_attach_idempotency_key,
        100
      );
      if (existingAttachIdempotencyKey && existingAttachIdempotencyKey !== idempotencyKey) {
        throw badRequest("Unapplied cash row already has a bank attach idempotency key");
      }

      const resolvedLink = resolveBankLinkFields({
        targetLabel: `unappliedCashId=${unappliedCashId}`,
        existingBankStatementLineId: targetRow.bank_statement_line_id,
        existingBankTransactionRef: targetRow.bank_transaction_ref,
        requestedBankStatementLineId: bankStatementLineId,
        requestedBankTransactionRef: bankTransactionRef,
      });
      const alreadyAttachedWithSameKey =
        existingAttachIdempotencyKey === idempotencyKey &&
        parsePositiveInt(targetRow.bank_statement_line_id) ===
          resolvedLink.nextBankStatementLineId &&
        textsEqual(targetRow.bank_transaction_ref, resolvedLink.nextBankTransactionRef);
      if (alreadyAttachedWithSameKey) {
        return {
          targetType,
          settlement: null,
          unappliedCash: mapUnappliedCashRow(targetRow),
          idempotentReplay: true,
        };
      }

      await tx.query(
        `UPDATE cari_unapplied_cash
         SET bank_statement_line_id = ?,
             bank_transaction_ref = ?,
             bank_attach_idempotency_key = ?
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND id = ?`,
        [
          resolvedLink.nextBankStatementLineId,
          resolvedLink.nextBankTransactionRef,
          idempotencyKey,
          tenantId,
          legalEntityId,
          unappliedCashId,
        ]
      );

      const updatedRow = await fetchUnappliedCashRowById({
        tenantId,
        legalEntityId,
        unappliedCashId,
        runQuery: tx.query,
        forUpdate: true,
      });
      if (!updatedRow) {
        throw badRequest("Unapplied cash row not found after bank attach update");
      }

      await insertAuditLog({
        req,
        runQuery: tx.query,
        tenantId,
        userId,
        action: "cari.bank.attach",
        resourceType: RESOURCE_TYPE_UNAPPLIED_CASH,
        legalEntityId,
        resourceId: unappliedCashId,
        payload: {
          targetType: BANK_ATTACH_TARGET_UNAPPLIED_CASH,
          unappliedCashId,
          bankStatementLineId: resolvedLink.nextBankStatementLineId,
          bankTransactionRef: resolvedLink.nextBankTransactionRef,
          idempotencyKey,
          note,
        },
      });

      return {
        targetType,
        settlement: null,
        unappliedCash: mapUnappliedCashRow(updatedRow),
        idempotentReplay: false,
      };
    });

    return result;
  } catch (err) {
    if (
      isDuplicateKeyError(err, "uk_cari_settle_batches_bank_attach_idempo") ||
      isDuplicateKeyError(err, "uk_cari_unap_bank_attach_idempo")
    ) {
      if (targetType === BANK_ATTACH_TARGET_SETTLEMENT) {
        const replay = await fetchSettlementBatchRowByBankAttachIdempotency({
          tenantId,
          legalEntityId,
          bankAttachIdempotencyKey: idempotencyKey,
        });
        if (replay) {
          if (parsePositiveInt(replay.id) !== settlementBatchId) {
            throw badRequest(
              "idempotencyKey is already used for a different settlementBatchId"
            );
          }
          return {
            targetType,
            settlement: mapSettlementBatchRow(replay),
            unappliedCash: null,
            idempotentReplay: true,
          };
        }
      } else {
        const replay = await fetchUnappliedCashRowByBankAttachIdempotency({
          tenantId,
          legalEntityId,
          bankAttachIdempotencyKey: idempotencyKey,
        });
        if (replay) {
          if (parsePositiveInt(replay.id) !== unappliedCashId) {
            throw badRequest("idempotencyKey is already used for a different unappliedCashId");
          }
          return {
            targetType,
            settlement: null,
            unappliedCash: mapUnappliedCashRow(replay),
            idempotentReplay: true,
          };
        }
      }
      throw badRequest("Duplicate bank attach idempotency key");
    }
    throw err;
  }
}

export async function reverseCariSettlementById({
  req,
  payload,
  assertScopeAccess,
}) {
  const tenantId = payload.tenantId;
  const settlementBatchId = payload.settlementBatchId;
  const reason = toNullableString(payload.reason, 255) || "Manual settlement reversal";
  const reversalDate = payload.reversalDate
    ? normalizeDateInput(payload.reversalDate, "reversalDate")
    : toDateOnlyString(new Date(), "reversalDate");

  const existing = await fetchSettlementBatchRow({
    tenantId,
    settlementBatchId,
  });
  if (!existing) {
    throw badRequest("Settlement batch not found");
  }
  const legalEntityId = parsePositiveInt(existing.legal_entity_id);
  assertScopeAccess(req, "legal_entity", legalEntityId, "settlementBatchId");
  if (normalizeUpperText(existing.status) !== SETTLEMENT_STATUS_POSTED) {
    throw badRequest("Only POSTED settlements can be reversed");
  }

  try {
    const reversed = await withTransaction(async (tx) => {
      const original = await fetchSettlementBatchRow({
        tenantId,
        settlementBatchId,
        runQuery: tx.query,
        forUpdate: true,
      });
      if (!original) {
        throw badRequest("Settlement batch not found");
      }
      if (normalizeUpperText(original.status) !== SETTLEMENT_STATUS_POSTED) {
        throw badRequest("Only POSTED settlements can be reversed");
      }
      const lockedLegalEntityId = parsePositiveInt(original.legal_entity_id);
      const originalJournalEntryId = parsePositiveInt(original.posted_journal_entry_id);
      const linkedCashTransactionId = parsePositiveInt(original.cash_transaction_id);
      if (linkedCashTransactionId) {
        const linkedCashTxn = await fetchCashTransactionForSettlementLink({
          tenantId,
          cashTransactionId: linkedCashTransactionId,
          runQuery: tx.query,
          forUpdate: true,
        });
        if (linkedCashTxn && normalizeUpperText(linkedCashTxn.status) === "POSTED") {
          throw badRequest(
            `Settlement cannot be reversed while linked cash transaction ${linkedCashTransactionId} is POSTED. Reverse cash transaction first.`
          );
        }
      }
      if (!originalJournalEntryId) {
        throw badRequest("Settlement posted journal linkage is missing");
      }

      const existingReversalBatchId = await findReversalSettlementBatchId({
        tenantId,
        originalSettlementBatchId: settlementBatchId,
        runQuery: tx.query,
      });
      if (existingReversalBatchId) {
        throw badRequest("Settlement is already reversed");
      }

      const allocations = await fetchSettlementAllocationsByBatchId({
        tenantId,
        settlementBatchId,
        runQuery: tx.query,
      });
      if (!allocations.length) {
        throw badRequest("Settlement has no allocations to reverse");
      }
      const openItemIds = allocations
        .map((row) => parsePositiveInt(row.open_item_id))
        .filter(Boolean)
        .sort((left, right) => left - right);
      const lockedOpenItems = await fetchOpenItemsByIdsForUpdate({
        tenantId,
        legalEntityId: lockedLegalEntityId,
        openItemIds,
        runQuery: tx.query,
      });
      const openItemById = new Map(
        lockedOpenItems.map((row) => [parsePositiveInt(row.id), row])
      );

      const touchedDocumentIds = [];
      for (const allocation of allocations) {
        const openItemId = parsePositiveInt(allocation.open_item_id);
        const lockedOpenItem = openItemById.get(openItemId);
        if (!lockedOpenItem) {
          throw badRequest(`openItemId=${openItemId} no longer exists for reversal`);
        }

        const allocationTxn = normalizeAmount(
          allocation.allocation_amount_txn,
          "allocationAmountTxn"
        );
        const allocationBase = normalizeAmount(
          allocation.allocation_amount_base,
          "allocationAmountBase"
        );
        const originalAmountTxn = normalizeAmount(
          lockedOpenItem.original_amount_txn,
          "openItem.originalAmountTxn"
        );
        const originalAmountBase = normalizeAmount(
          lockedOpenItem.original_amount_base,
          "openItem.originalAmountBase"
        );
        const currentResidualTxn = normalizeAmount(
          lockedOpenItem.residual_amount_txn,
          "openItem.residualAmountTxn",
          { allowZero: true }
        );
        const currentResidualBase = normalizeAmount(
          lockedOpenItem.residual_amount_base,
          "openItem.residualAmountBase",
          { allowZero: true }
        );
        let nextResidualTxn = roundAmount(currentResidualTxn + allocationTxn);
        let nextResidualBase = roundAmount(currentResidualBase + allocationBase);
        if (nextResidualTxn > originalAmountTxn && nextResidualTxn - originalAmountTxn <= AMOUNT_EPSILON) {
          nextResidualTxn = originalAmountTxn;
        }
        if (nextResidualBase > originalAmountBase && nextResidualBase - originalAmountBase <= AMOUNT_EPSILON) {
          nextResidualBase = originalAmountBase;
        }
        if (
          nextResidualTxn > originalAmountTxn + AMOUNT_EPSILON ||
          nextResidualBase > originalAmountBase + AMOUNT_EPSILON
        ) {
          throw badRequest(
            `Cannot reverse settlement because open item ${openItemId} has progressed beyond reversible state`
          );
        }
        const nextSettledTxn = roundAmount(originalAmountTxn - nextResidualTxn);
        const nextSettledBase = roundAmount(originalAmountBase - nextResidualBase);
        const nextStatus = normalizeOpenItemStatus({
          originalAmountTxn,
          residualAmountTxn: nextResidualTxn,
          settledAmountTxn: nextSettledTxn,
        });

        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `UPDATE cari_open_items
           SET status = ?,
               residual_amount_txn = ?,
               residual_amount_base = ?,
               settled_amount_txn = ?,
               settled_amount_base = ?
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?`,
          [
            nextStatus,
            nextResidualTxn,
            nextResidualBase,
            nextSettledTxn,
            nextSettledBase,
            tenantId,
            lockedLegalEntityId,
            openItemId,
          ]
        );
        touchedDocumentIds.push(parsePositiveInt(lockedOpenItem.document_id));
      }

      await refreshDocumentBalancesTx({
        tx,
        tenantId,
        legalEntityId: lockedLegalEntityId,
        documentIds: touchedDocumentIds,
      });

      const applyAuditPayload = await fetchApplyAuditPayloadForSettlement({
        tenantId,
        settlementBatchId,
        runQuery: tx.query,
      });
      const unappliedConsumed = Array.isArray(applyAuditPayload?.unappliedConsumed)
        ? applyAuditPayload.unappliedConsumed
        : [];
      const createdUnappliedCashId = parsePositiveInt(
        applyAuditPayload?.createdUnappliedCashId
      );

      for (const consumed of unappliedConsumed.sort(
        (left, right) =>
          parsePositiveInt(left?.unappliedCashId) - parsePositiveInt(right?.unappliedCashId)
      )) {
        const unappliedCashId = parsePositiveInt(consumed?.unappliedCashId);
        if (!unappliedCashId) {
          continue;
        }
        const consumeTxn = normalizeAmount(consumed?.consumeTxn || 0, "consumeTxn", {
          allowZero: true,
        });
        const consumeBase = normalizeAmount(consumed?.consumeBase || 0, "consumeBase", {
          allowZero: true,
        });
        if (consumeTxn <= AMOUNT_EPSILON && consumeBase <= AMOUNT_EPSILON) {
          continue;
        }

        // eslint-disable-next-line no-await-in-loop
        const rowResult = await tx.query(
          `SELECT
             id,
             amount_txn,
             amount_base,
             residual_amount_txn,
             residual_amount_base,
             status,
             note
           FROM cari_unapplied_cash
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?
           LIMIT 1
           FOR UPDATE`,
          [tenantId, lockedLegalEntityId, unappliedCashId]
        );
        const row = rowResult.rows?.[0] || null;
        if (!row) {
          throw badRequest(
            `Cannot reverse settlement because unapplied cash ${unappliedCashId} no longer exists`
          );
        }

        const amountTxn = normalizeAmount(row.amount_txn, "unapplied.amountTxn");
        const amountBase = normalizeAmount(row.amount_base, "unapplied.amountBase");
        const residualTxn = normalizeAmount(
          row.residual_amount_txn,
          "unapplied.residualAmountTxn"
        );
        const residualBase = normalizeAmount(
          row.residual_amount_base,
          "unapplied.residualAmountBase"
        );
        let nextResidualTxn = roundAmount(residualTxn + consumeTxn);
        let nextResidualBase = roundAmount(residualBase + consumeBase);
        if (nextResidualTxn > amountTxn && nextResidualTxn - amountTxn <= AMOUNT_EPSILON) {
          nextResidualTxn = amountTxn;
        }
        if (nextResidualBase > amountBase && nextResidualBase - amountBase <= AMOUNT_EPSILON) {
          nextResidualBase = amountBase;
        }
        if (
          nextResidualTxn > amountTxn + AMOUNT_EPSILON ||
          nextResidualBase > amountBase + AMOUNT_EPSILON
        ) {
          throw badRequest(
            `Cannot reverse settlement because unapplied cash ${unappliedCashId} was consumed by later operations`
          );
        }
        const nextStatus = normalizeUnappliedStatus({
          residualAmountTxn: nextResidualTxn,
          amountTxn,
        });

        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `UPDATE cari_unapplied_cash
           SET status = ?,
               residual_amount_txn = ?,
               residual_amount_base = ?,
               note = ?
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?`,
          [
            nextStatus,
            nextResidualTxn,
            nextResidualBase,
            toNullableString(
              `${row.note || ""}${row.note ? " | " : ""}Restored by reversal of settlement ${
                original.settlement_no
              }`,
              500
            ),
            tenantId,
            lockedLegalEntityId,
            unappliedCashId,
          ]
        );
      }

      if (createdUnappliedCashId) {
        const createdUnappliedRowResult = await tx.query(
          `SELECT
             id,
             amount_txn,
             amount_base,
             residual_amount_txn,
             residual_amount_base,
             status,
             note
           FROM cari_unapplied_cash
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?
           LIMIT 1
           FOR UPDATE`,
          [tenantId, lockedLegalEntityId, createdUnappliedCashId]
        );
        const createdRow = createdUnappliedRowResult.rows?.[0] || null;
        if (createdRow) {
          const amountTxn = normalizeAmount(createdRow.amount_txn, "unapplied.amountTxn");
          const amountBase = normalizeAmount(createdRow.amount_base, "unapplied.amountBase");
          const residualTxn = normalizeAmount(
            createdRow.residual_amount_txn,
            "unapplied.residualAmountTxn"
          );
          const residualBase = normalizeAmount(
            createdRow.residual_amount_base,
            "unapplied.residualAmountBase"
          );
          if (
            !amountsAreEqual(residualTxn, amountTxn) ||
            !amountsAreEqual(residualBase, amountBase)
          ) {
            throw badRequest(
              `Cannot reverse settlement because created unapplied cash ${createdUnappliedCashId} is already consumed`
            );
          }
          await tx.query(
            `UPDATE cari_unapplied_cash
             SET status = ?,
                 residual_amount_txn = 0.000000,
                 residual_amount_base = 0.000000,
                 note = ?
             WHERE tenant_id = ?
               AND legal_entity_id = ?
               AND id = ?`,
            [
              UNAPPLIED_STATUS_REVERSED,
              toNullableString(
                `${createdRow.note || ""}${createdRow.note ? " | " : ""}Reversed by settlement ${
                  original.settlement_no
                }`,
                500
              ),
              tenantId,
              lockedLegalEntityId,
              createdUnappliedCashId,
            ]
          );
        }
      }

      const originalJournalWithLines = await fetchPostedJournalWithLines({
        tenantId,
        journalEntryId: originalJournalEntryId,
        runQuery: tx.query,
      });
      const originalJournal = originalJournalWithLines?.journal || null;
      const originalJournalLines = originalJournalWithLines?.lines || [];
      if (!originalJournal) {
        throw badRequest("Original settlement posted journal not found");
      }
      if (normalizeUpperText(originalJournal.status) !== "POSTED") {
        throw badRequest("Only POSTED journals can be reversed");
      }
      if (parsePositiveInt(originalJournal.reversal_journal_entry_id)) {
        throw badRequest("Settlement journal is already reversed");
      }
      if (!originalJournalLines.length) {
        throw badRequest("Original settlement journal has no lines to reverse");
      }

      const reversalPeriodContext = await resolveBookAndOpenPeriodForDate({
        tenantId,
        legalEntityId: lockedLegalEntityId,
        targetDate: reversalDate,
        preferredBookId: parsePositiveInt(originalJournal.book_id),
        runQuery: tx.query,
      });

      const reversalSubledgerReferenceNo = `${CARI_SETTLEMENT_REVERSE_REFERENCE_PREFIX}${settlementBatchId}`;
      const reversalLines = originalJournalLines.map((line) => ({
        accountId: parsePositiveInt(line.account_id),
        debitBase: Number(line.credit_base || 0),
        creditBase: Number(line.debit_base || 0),
        amountTxn: roundAmount(Number(line.amount_txn || 0) * -1),
        description: line.description
          ? String(line.description).slice(0, 255)
          : `Reversal of ${original.settlement_no || `SETTLEMENT-${settlementBatchId}`}`,
        subledgerReferenceNo: reversalSubledgerReferenceNo,
        currencyCode: normalizeUpperText(line.currency_code || original.currency_code),
      }));
      ensureBalancedJournalLines(reversalLines);

      const reversalJournalResult = await insertPostedJournalWithLinesTx(tx, {
        tenantId,
        legalEntityId: lockedLegalEntityId,
        bookId: reversalPeriodContext.bookId,
        fiscalPeriodId: reversalPeriodContext.fiscalPeriodId,
        userId: payload.userId,
        journalNo: buildCariJournalNo("CARI-SET-REV", settlementBatchId),
        entryDate: reversalDate,
        documentDate: reversalDate,
        currencyCode: normalizeUpperText(original.currency_code),
        description: `Reversal of ${original.settlement_no || `SETTLEMENT-${settlementBatchId}`}`.slice(
          0,
          500
        ),
        referenceNo: toNullableString(`REV:${original.settlement_no || settlementBatchId}`, 100),
        lines: reversalLines,
      });

      const reverseJournalUpdateResult = await tx.query(
        `UPDATE journal_entries
         SET status = 'REVERSED',
             reversed_by_user_id = ?,
             reversed_at = CURRENT_TIMESTAMP,
             reversal_journal_entry_id = ?,
             reverse_reason = ?
         WHERE tenant_id = ?
           AND id = ?
           AND status = 'POSTED'
           AND reversal_journal_entry_id IS NULL`,
        [
          payload.userId,
          reversalJournalResult.journalEntryId,
          reason,
          tenantId,
          originalJournalEntryId,
        ]
      );
      if (Number(reverseJournalUpdateResult.rows?.affectedRows || 0) === 0) {
        throw badRequest("Settlement journal is already reversed");
      }

      const reversalSequence = await reserveSettlementSequence({
        tenantId,
        legalEntityId: lockedLegalEntityId,
        settlementDate: reversalDate,
        runQuery: tx.query,
      });
      const reversalInsert = await tx.query(
        `INSERT INTO cari_settlement_batches (
            tenant_id,
            legal_entity_id,
            counterparty_id,
            sequence_namespace,
            fiscal_year,
            sequence_no,
            settlement_no,
            settlement_date,
            status,
            total_allocated_txn,
            total_allocated_base,
            currency_code,
            posted_journal_entry_id,
            reversal_of_settlement_batch_id,
            bank_statement_line_id,
            bank_transaction_ref,
            bank_attach_idempotency_key,
            bank_apply_idempotency_key,
            posted_at,
            reversed_at
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
        [
          tenantId,
          lockedLegalEntityId,
          parsePositiveInt(original.counterparty_id),
          reversalSequence.sequenceNamespace,
          reversalSequence.fiscalYear,
          reversalSequence.sequenceNo,
          reversalSequence.settlementNo,
          reversalDate,
          SETTLEMENT_STATUS_REVERSED,
          normalizeAmount(original.total_allocated_txn, "totalAllocatedTxn"),
          normalizeAmount(original.total_allocated_base, "totalAllocatedBase"),
          normalizeUpperText(original.currency_code),
          reversalJournalResult.journalEntryId,
          settlementBatchId,
        ]
      );
      const reversalSettlementBatchId = parsePositiveInt(reversalInsert.rows?.insertId);
      if (!reversalSettlementBatchId) {
        throw new Error("Reversal settlement batch create failed");
      }

      await tx.query(
        `UPDATE cari_settlement_batches
         SET status = ?,
             reversed_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ?
           AND id = ?`,
        [SETTLEMENT_STATUS_REVERSED, tenantId, settlementBatchId]
      );

      await insertAuditLog({
        req,
        runQuery: tx.query,
        tenantId,
        userId: payload.userId,
        action: "cari.settlement.reverse",
        legalEntityId: lockedLegalEntityId,
        resourceId: settlementBatchId,
        payload: {
          reason,
          originalSettlementBatchId: settlementBatchId,
          reversalSettlementBatchId,
          originalPostedJournalEntryId: originalJournalEntryId,
          reversalPostedJournalEntryId: reversalJournalResult.journalEntryId,
          followUpRisks: FOLLOW_UP_RISKS,
        },
      });

      const originalResult = await loadSettlementResult({
        tenantId,
        settlementBatchId,
        runQuery: tx.query,
      });
      const reversalResult = await loadSettlementResult({
        tenantId,
        settlementBatchId: reversalSettlementBatchId,
        runQuery: tx.query,
      });

      return {
        row: reversalResult.row,
        original: originalResult.row,
        journal: reversalResult.journal,
        idempotentReplay: false,
        followUpRisks: FOLLOW_UP_RISKS,
      };
    });

    return reversed;
  } catch (err) {
    if (isDuplicateKeyError(err, "uk_cari_settle_batches_single_reversal")) {
      throw badRequest("Settlement is already reversed");
    }
    throw err;
  }
}
