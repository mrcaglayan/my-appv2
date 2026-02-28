import { query, withTransaction } from "../db.js";
import {
  assertAccountBelongsToTenant,
  assertCurrencyExists,
  assertLegalEntityBelongsToTenant,
} from "../tenantGuards.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  buildOffsetPaginationResult,
  resolveOffsetPagination,
} from "../utils/pagination.js";

const DRAFT_STATUS = "DRAFT";
const CANCELLED_STATUS = "CANCELLED";
const POSTED_STATUS = "POSTED";
const REVERSED_STATUS = "REVERSED";
const OPEN_ITEM_STATUS_OPEN = "OPEN";
const OPEN_ITEM_STATUS_CANCELLED = "CANCELLED";
const DRAFT_SEQUENCE_NAMESPACE = "DRAFT";
const FX_RATE_TYPE_SPOT = "SPOT";
const AMOUNT_PRECISION_SCALE = 6;
const AMOUNT_BALANCE_EPSILON = 0.000001;
const CARI_SUBLEDGER_REFERENCE_PREFIX = "CARI_DOC:";
const CARI_SUBLEDGER_REVERSE_REFERENCE_PREFIX = "CARI_DOC_REV:";
const CARI_POSTING_PURPOSES = Object.freeze({
  AR: Object.freeze({
    control: "CARI_AR_CONTROL",
    offset: "CARI_AR_OFFSET",
  }),
  AP: Object.freeze({
    control: "CARI_AP_CONTROL",
    offset: "CARI_AP_OFFSET",
  }),
});
const POSITIVE_SIGN_DOCUMENT_TYPES = new Set(["INVOICE", "DEBIT_NOTE"]);
const DUE_DATE_REQUIRED_TYPES = new Set(["INVOICE", "DEBIT_NOTE"]);
const FROZEN_TRANSACTION_KEYS = new Set([
  "AR:INVOICE",
  "AR:DEBIT_NOTE",
  "AR:CREDIT_NOTE",
  "AR:PAYMENT",
  "AR:ADJUSTMENT",
  "AP:INVOICE",
  "AP:DEBIT_NOTE",
  "AP:CREDIT_NOTE",
  "AP:PAYMENT",
  "AP:ADJUSTMENT",
]);

function toDecimalNumber(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
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

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
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
  return Number(parsed.toFixed(AMOUNT_PRECISION_SCALE));
}

function normalizeSignedAmount(value, label = "amount", { allowZero = true } = {}) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw badRequest(`${label} must be numeric`);
  }
  if (!allowZero && Math.abs(parsed) <= AMOUNT_BALANCE_EPSILON) {
    throw badRequest(`${label} must not be zero`);
  }
  return Number(parsed.toFixed(AMOUNT_PRECISION_SCALE));
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

function amountsAreEqual(left, right, epsilon = 0.0000001) {
  return Math.abs(Number(left || 0) - Number(right || 0)) <= epsilon;
}

function ensureBalancedJournalLines(lines) {
  let debitTotal = 0;
  let creditTotal = 0;
  for (const line of lines || []) {
    debitTotal += Number(line.debitBase || 0);
    creditTotal += Number(line.creditBase || 0);
  }
  if (Math.abs(debitTotal - creditTotal) > AMOUNT_BALANCE_EPSILON) {
    throw badRequest("Cari posting journal is not balanced");
  }
  return {
    totalDebit: Number(debitTotal.toFixed(AMOUNT_PRECISION_SCALE)),
    totalCredit: Number(creditTotal.toFixed(AMOUNT_PRECISION_SCALE)),
  };
}

function buildCariJournalNo(prefix, documentId) {
  const normalizedPrefix = normalizeUpperText(prefix || "CARI").slice(0, 12) || "CARI";
  const parsedDocumentId = parsePositiveInt(documentId);
  const stamp = Date.now().toString(36).toUpperCase();
  const base = parsedDocumentId
    ? `${normalizedPrefix}-${parsedDocumentId}-${stamp}`
    : `${normalizedPrefix}-${stamp}`;
  return base.slice(0, 40);
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

function normalizeDateInput(value, label) {
  const normalized = toDateOnlyString(value, label);
  if (!normalized) {
    throw badRequest(`${label} must be YYYY-MM-DD`);
  }
  return normalized;
}

function addDays(dateString, daysToAdd) {
  const normalizedDate = normalizeDateInput(dateString, "documentDate");
  const utcDate = new Date(`${normalizedDate}T00:00:00.000Z`);
  if (Number.isNaN(utcDate.getTime())) {
    throw badRequest("documentDate must be a valid date");
  }

  const parsedDays = Number(daysToAdd || 0);
  if (!Number.isFinite(parsedDays)) {
    throw badRequest("payment term due/grace days must be numeric");
  }
  utcDate.setUTCDate(utcDate.getUTCDate() + parsedDays);
  return utcDate.toISOString().slice(0, 10);
}

function mapDocumentRow(row) {
  const documentDate = toDateOnlyString(row.document_date, "documentDate");
  const dueDate = toDateOnlyString(row.due_date, "dueDate");
  const dueDateSnapshot = toDateOnlyString(row.due_date_snapshot, "dueDateSnapshot");
  return {
    id: parsePositiveInt(row.id),
    tenantId: parsePositiveInt(row.tenant_id),
    legalEntityId: parsePositiveInt(row.legal_entity_id),
    counterpartyId: parsePositiveInt(row.counterparty_id),
    paymentTermId: parsePositiveInt(row.payment_term_id),
    paymentTermCode: row.payment_term_code || null,
    paymentTermName: row.payment_term_name || null,
    direction: row.direction,
    documentType: row.document_type,
    sequenceNamespace: row.sequence_namespace,
    fiscalYear: Number(row.fiscal_year),
    sequenceNo: Number(row.sequence_no),
    documentNo: row.document_no,
    status: row.status,
    documentDate,
    dueDate,
    amountTxn: toDecimalNumber(row.amount_txn),
    amountBase: toDecimalNumber(row.amount_base),
    openAmountTxn: toDecimalNumber(row.open_amount_txn),
    openAmountBase: toDecimalNumber(row.open_amount_base),
    currencyCode: row.currency_code,
    fxRate: toDecimalNumber(row.fx_rate),
    counterpartyCodeSnapshot: row.counterparty_code_snapshot || null,
    counterpartyNameSnapshot: row.counterparty_name_snapshot || null,
    paymentTermSnapshot: row.payment_term_snapshot || null,
    dueDateSnapshot,
    currencyCodeSnapshot: row.currency_code_snapshot || null,
    fxRateSnapshot: toDecimalNumber(row.fx_rate_snapshot),
    postedJournalEntryId: parsePositiveInt(row.posted_journal_entry_id),
    reversalOfDocumentId: parsePositiveInt(row.reversal_of_document_id),
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
    postedAt: row.posted_at || null,
    reversedAt: row.reversed_at || null,
    draftSequenceAssigned: row.sequence_namespace === DRAFT_SEQUENCE_NAMESPACE,
  };
}

function assertFrozenTransactionType(direction, documentType) {
  const key = `${direction}:${documentType}`;
  if (!FROZEN_TRANSACTION_KEYS.has(key)) {
    throw badRequest("Only frozen v1 transaction types are allowed");
  }
}

function assertDueDateByDocumentType({ documentType, dueDate }) {
  if (DUE_DATE_REQUIRED_TYPES.has(documentType) && !dueDate) {
    throw badRequest(`dueDate is required for documentType=${documentType}`);
  }
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

async function fetchPaymentTermRow({
  tenantId,
  legalEntityId,
  paymentTermId,
  runQuery = query,
}) {
  if (!paymentTermId) {
    return null;
  }
  const result = await runQuery(
    `SELECT
        id,
        tenant_id,
        legal_entity_id,
        code,
        name,
        due_days,
        grace_days,
        is_end_of_month,
        status
     FROM payment_terms
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, paymentTermId]
  );
  return result.rows?.[0] || null;
}

async function fetchDocumentRow({
  tenantId,
  documentId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
        d.*,
        pt.code AS payment_term_code,
        pt.name AS payment_term_name
     FROM cari_documents d
     LEFT JOIN payment_terms pt
       ON pt.tenant_id = d.tenant_id
      AND pt.legal_entity_id = d.legal_entity_id
      AND pt.id = d.payment_term_id
     WHERE d.tenant_id = ?
       AND d.id = ?
     LIMIT 1`,
    [tenantId, documentId]
  );
  return result.rows?.[0] || null;
}

async function reserveDraftSequence({
  tenantId,
  legalEntityId,
  direction,
  documentDate,
  runQuery,
}) {
  const fiscalYear = Number(String(documentDate).slice(0, 4));
  if (!Number.isInteger(fiscalYear) || fiscalYear < 1900) {
    throw badRequest("documentDate must include a valid fiscal year");
  }

  const maxResult = await runQuery(
    `SELECT COALESCE(MAX(sequence_no), 0) AS current_max
     FROM cari_documents
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND direction = ?
       AND sequence_namespace = ?
       AND fiscal_year = ?
     FOR UPDATE`,
    [tenantId, legalEntityId, direction, DRAFT_SEQUENCE_NAMESPACE, fiscalYear]
  );
  const currentMax = Number(maxResult.rows?.[0]?.current_max || 0);
  const nextSequenceNo = currentMax + 1;
  const documentNo = `DRAFT-${direction}-${fiscalYear}-${String(nextSequenceNo).padStart(
    6,
    "0"
  )}`;

  return {
    sequenceNamespace: DRAFT_SEQUENCE_NAMESPACE,
    fiscalYear,
    sequenceNo: nextSequenceNo,
    documentNo: documentNo.slice(0, 80),
  };
}

function resolveDueDate({
  documentDate,
  dueDate,
  documentType,
  paymentTermRow,
}) {
  if (dueDate) {
    return dueDate;
  }

  if (!DUE_DATE_REQUIRED_TYPES.has(documentType)) {
    return null;
  }

  if (!paymentTermRow) {
    throw badRequest(`dueDate is required for documentType=${documentType}`);
  }

  const dueDays = Number(paymentTermRow.due_days || 0);
  const graceDays = Number(paymentTermRow.grace_days || 0);
  const totalDays = dueDays + graceDays;
  return addDays(documentDate, totalDays);
}

function assertDateOrder(documentDate, dueDate) {
  const normalizedDocumentDate = toDateOnlyString(documentDate, "documentDate");
  const normalizedDueDate = toDateOnlyString(dueDate, "dueDate");
  if (!normalizedDueDate) {
    return;
  }
  if (!normalizedDocumentDate) {
    throw badRequest("documentDate is required");
  }
  if (normalizedDueDate < normalizedDocumentDate) {
    throw badRequest("dueDate cannot be before documentDate");
  }
}

function buildPaymentTermSnapshot(paymentTermRow) {
  if (!paymentTermRow) {
    return null;
  }
  return safeStringify({
    code: String(paymentTermRow.code || ""),
    name: String(paymentTermRow.name || ""),
    dueDays: Number(paymentTermRow.due_days || 0),
    graceDays: Number(paymentTermRow.grace_days || 0),
    isEndOfMonth: Boolean(paymentTermRow.is_end_of_month),
    status: String(paymentTermRow.status || ""),
  });
}

function buildPostedDocumentNo({
  direction,
  documentType,
  fiscalYear,
  sequenceNo,
}) {
  const prefix = `${normalizeUpperText(direction)}-${normalizeUpperText(documentType)}`;
  const suffix = String(Number(sequenceNo || 0)).padStart(6, "0");
  return `${prefix}-${fiscalYear}-${suffix}`.slice(0, 80);
}

async function fetchDocumentRowForUpdate({
  tenantId,
  documentId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT *
     FROM cari_documents
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1
     FOR UPDATE`,
    [tenantId, documentId]
  );
  return result.rows?.[0] || null;
}

async function reservePostedSequence({
  tenantId,
  legalEntityId,
  direction,
  documentType,
  documentDate,
  runQuery,
}) {
  const fiscalYear = Number(String(documentDate).slice(0, 4));
  if (!Number.isInteger(fiscalYear) || fiscalYear < 1900) {
    throw badRequest("documentDate must include a valid fiscal year");
  }

  const sequenceNamespace = normalizeUpperText(documentType);
  const directionCode = normalizeUpperText(direction);
  const maxResult = await runQuery(
    `SELECT COALESCE(MAX(sequence_no), 0) AS current_max
     FROM cari_documents
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND direction = ?
       AND sequence_namespace = ?
       AND fiscal_year = ?
     FOR UPDATE`,
    [tenantId, legalEntityId, directionCode, sequenceNamespace, fiscalYear]
  );
  const currentMax = Number(maxResult.rows?.[0]?.current_max || 0);
  const nextSequenceNo = currentMax + 1;

  return {
    sequenceNamespace,
    fiscalYear,
    sequenceNo: nextSequenceNo,
    documentNo: buildPostedDocumentNo({
      direction: directionCode,
      documentType: sequenceNamespace,
      fiscalYear,
      sequenceNo: nextSequenceNo,
    }),
  };
}

async function resolveBookAndOpenPeriodForDate({
  tenantId,
  legalEntityId,
  targetDate,
  preferredBookId = null,
  runQuery = query,
}) {
  const normalizedDate = normalizeDateInput(targetDate, "documentDate");

  let book = null;
  if (preferredBookId) {
    const preferredBookResult = await runQuery(
      `SELECT id, calendar_id, base_currency_code
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
    throw badRequest("No book found for document legalEntityId");
  }

  const bookId = parsePositiveInt(book.id);
  const calendarId = parsePositiveInt(book.calendar_id);
  if (!bookId || !calendarId) {
    throw badRequest("Book configuration is invalid for document posting");
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
    throw badRequest("No fiscal period found for document date");
  }

  const fiscalPeriodId = parsePositiveInt(period.id);
  if (!fiscalPeriodId) {
    throw badRequest("Fiscal period configuration is invalid for document posting");
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
    throw badRequest(`Period is ${periodStatus}; cannot post/reverse document`);
  }

  return {
    bookId,
    fiscalPeriodId,
    fiscalYear: Number(period.fiscal_year),
    baseCurrencyCode: normalizeUpperText(book.base_currency_code),
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
    throw badRequest("direction must be AR or AP");
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

async function resolveCariPostingAccounts({
  tenantId,
  legalEntityId,
  direction,
  counterpartyRow = null,
  runQuery = query,
}) {
  const purposeDefinition = CARI_POSTING_PURPOSES[normalizeUpperText(direction)];
  if (!purposeDefinition) {
    throw badRequest("direction must be AR or AP");
  }

  const requestedPurposes = [purposeDefinition.control, purposeDefinition.offset];
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

  const control = byPurpose.get(purposeDefinition.control);
  const offset = byPurpose.get(purposeDefinition.offset);
  if (!control?.id || !offset?.id) {
    throw badRequest(
      `Setup required: configure journal_purpose_accounts for ${purposeDefinition.control} and ${purposeDefinition.offset}`
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
    throw badRequest("Cari control and offset accounts must be different");
  }

  return {
    controlAccountId: effectiveControl.id,
    offsetAccountId: offset.id,
    controlAccountCode: effectiveControl.code || null,
    offsetAccountCode: offset.code || null,
  };
}

function buildCariPostingLines({
  direction,
  documentType,
  amountTxn,
  amountBase,
  controlAccountId,
  offsetAccountId,
  lineDescription,
  subledgerReferenceNo,
  currencyCode,
}) {
  const normalizedDirection = normalizeUpperText(direction);
  const normalizedType = normalizeUpperText(documentType);
  const normalizedCurrency = normalizeUpperText(currencyCode);
  const postingAmountTxn = normalizeAmount(amountTxn, "amountTxn");
  const postingAmountBase = normalizeAmount(amountBase, "amountBase");

  const isPositiveSign = POSITIVE_SIGN_DOCUMENT_TYPES.has(normalizedType);
  let debitAccountId = null;
  let creditAccountId = null;

  if (normalizedDirection === "AR") {
    debitAccountId = isPositiveSign ? controlAccountId : offsetAccountId;
    creditAccountId = isPositiveSign ? offsetAccountId : controlAccountId;
  } else if (normalizedDirection === "AP") {
    debitAccountId = isPositiveSign ? offsetAccountId : controlAccountId;
    creditAccountId = isPositiveSign ? controlAccountId : offsetAccountId;
  } else {
    throw badRequest("direction must be AR or AP");
  }

  const lines = [
    {
      accountId: parsePositiveInt(debitAccountId),
      debitBase: postingAmountBase,
      creditBase: 0,
      amountTxn: postingAmountTxn,
      description: toNullableString(lineDescription, 255),
      subledgerReferenceNo: toNullableString(subledgerReferenceNo, 100),
      currencyCode: normalizedCurrency,
    },
    {
      accountId: parsePositiveInt(creditAccountId),
      debitBase: 0,
      creditBase: postingAmountBase,
      amountTxn: Number((postingAmountTxn * -1).toFixed(AMOUNT_PRECISION_SCALE)),
      description: toNullableString(lineDescription, 255),
      subledgerReferenceNo: toNullableString(subledgerReferenceNo, 100),
      currencyCode: normalizedCurrency,
    },
  ];

  for (const [index, line] of lines.entries()) {
    if (!line.accountId) {
      throw badRequest(`Posting line ${index + 1} account is invalid`);
    }
  }
  ensureBalancedJournalLines(lines);
  return lines;
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
    throw badRequest("Failed to create posted journal entry");
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
       operating_unit_id,
       counterparty_legal_entity_id,
       description,
       subledger_reference_no,
       currency_code,
       amount_txn,
       debit_base,
       credit_base,
       tax_code
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

async function resolveFxPostingPolicy({
  tenantId,
  documentDate,
  documentCurrencyCode,
  functionalCurrencyCode,
  draftFxRate,
  useFxOverride,
  fxOverrideReason,
  runQuery = query,
}) {
  const documentCurrency = normalizeUpperText(documentCurrencyCode);
  const functionalCurrency = normalizeUpperText(functionalCurrencyCode);
  const normalizedDocumentDate = normalizeDateInput(documentDate, "documentDate");
  const providedFxRate = normalizeOptionalPositiveDecimal(draftFxRate, "fxRate");

  if (!documentCurrency || !functionalCurrency) {
    throw badRequest("Document and functional currency codes are required for posting");
  }

  if (documentCurrency === functionalCurrency) {
    const effectiveFxRate = providedFxRate || 1;
    if (!amountsAreEqual(effectiveFxRate, 1)) {
      throw badRequest(
        "fxRate must be 1 when document currency equals legal entity functional currency"
      );
    }
    return {
      effectiveFxRate: 1,
      fxRateLocked: false,
      referenceFxRate: 1,
      overrideUsed: false,
      fxRateSource: "PARITY",
      fxRateDate: normalizedDocumentDate,
    };
  }

  const fxResult = await runQuery(
    `SELECT rate, is_locked, rate_date
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
      normalizedDocumentDate,
      documentCurrency,
      functionalCurrency,
      FX_RATE_TYPE_SPOT,
    ]
  );
  const fxRow = fxResult.rows?.[0] || null;
  const referenceFxRate = normalizeOptionalPositiveDecimal(fxRow?.rate, "fxRates.rate");
  const fxRateLocked = Boolean(fxRow?.is_locked);

  let effectiveFxRate = providedFxRate || referenceFxRate;
  if (!effectiveFxRate) {
    throw badRequest(
      "fxRate is required because no SPOT FX rate exists for documentDate and currency pair"
    );
  }

  let overrideUsed = false;
  if (fxRateLocked && referenceFxRate && !amountsAreEqual(effectiveFxRate, referenceFxRate)) {
    if (!useFxOverride) {
      throw badRequest(
        "FX date is locked; useFxOverride=true with cari.fx.override permission is required"
      );
    }
    if (!toNullableString(fxOverrideReason, 500)) {
      throw badRequest("fxOverrideReason is required when overriding locked FX rate");
    }
    overrideUsed = true;
  }

  if (!providedFxRate && referenceFxRate) {
    effectiveFxRate = referenceFxRate;
  }

  return {
    effectiveFxRate,
    fxRateLocked,
    referenceFxRate: referenceFxRate || null,
    overrideUsed,
    fxRateSource: referenceFxRate ? "FX_TABLE" : "DOCUMENT",
    fxRateDate: toDateOnlyString(fxRow?.rate_date || normalizedDocumentDate, "fxRateDate"),
  };
}

async function findReversalDocumentByOriginalId({
  tenantId,
  originalDocumentId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT id
     FROM cari_documents
     WHERE tenant_id = ?
       AND reversal_of_document_id = ?
     LIMIT 1`,
    [tenantId, originalDocumentId]
  );
  return parsePositiveInt(result.rows?.[0]?.id);
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

async function insertAuditLog({
  req,
  runQuery = query,
  tenantId,
  userId,
  action,
  legalEntityId,
  documentId,
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
      "cari_document",
      documentId ? String(documentId) : null,
      legalEntityId ? "LEGAL_ENTITY" : null,
      legalEntityId || null,
      toNullableString(req?.requestId || req?.headers?.["x-request-id"], 80),
      resolveClientIp(req),
      toNullableString(req?.headers?.["user-agent"], 255),
      safeStringify(payload || null),
    ]
  );
}

export async function resolveCariDocumentScope(documentId, tenantId) {
  const parsedDocumentId = parsePositiveInt(documentId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedDocumentId || !parsedTenantId) {
    return null;
  }

  const result = await query(
    `SELECT legal_entity_id
     FROM cari_documents
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [parsedTenantId, parsedDocumentId]
  );
  const row = result.rows?.[0] || null;
  if (!row) {
    return null;
  }

  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: parsePositiveInt(row.legal_entity_id),
  };
}

export async function listCariDocuments({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["d.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "d.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("d.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.counterpartyId) {
    conditions.push("d.counterparty_id = ?");
    params.push(filters.counterpartyId);
  }
  if (filters.direction) {
    conditions.push("d.direction = ?");
    params.push(filters.direction);
  }
  if (filters.documentType) {
    conditions.push("d.document_type = ?");
    params.push(filters.documentType);
  }
  if (filters.status) {
    conditions.push("d.status = ?");
    params.push(filters.status);
  }
  if (filters.dateFrom) {
    conditions.push("d.document_date >= ?");
    params.push(filters.dateFrom);
  }
  if (filters.dateTo) {
    conditions.push("d.document_date <= ?");
    params.push(filters.dateTo);
  }
  if (filters.q) {
    conditions.push(
      "(d.document_no LIKE ? OR d.counterparty_code_snapshot LIKE ? OR d.counterparty_name_snapshot LIKE ?)"
    );
    params.push(`%${filters.q}%`, `%${filters.q}%`, `%${filters.q}%`);
  }

  const whereSql = conditions.join(" AND ");
  const totalResult = await query(
    `SELECT COUNT(*) AS row_count
     FROM cari_documents d
     WHERE ${whereSql}`,
    params
  );
  const total = Number(totalResult.rows?.[0]?.row_count || 0);

  const pagination = resolveOffsetPagination(filters, {
    defaultLimit: 100,
    defaultOffset: 0,
    maxLimit: 300,
  });

  const rowsResult = await query(
    `SELECT
        d.*,
        pt.code AS payment_term_code,
        pt.name AS payment_term_name
     FROM cari_documents d
     LEFT JOIN payment_terms pt
       ON pt.tenant_id = d.tenant_id
      AND pt.legal_entity_id = d.legal_entity_id
      AND pt.id = d.payment_term_id
     WHERE ${whereSql}
     ORDER BY d.id DESC
     LIMIT ${pagination.limit} OFFSET ${pagination.offset}`,
    params
  );

  return buildOffsetPaginationResult({
    rows: (rowsResult.rows || []).map(mapDocumentRow),
    total,
    limit: pagination.limit,
    offset: pagination.offset,
  });
}

export async function getCariDocumentByIdForTenant({
  req,
  tenantId,
  documentId,
  assertScopeAccess,
}) {
  const row = await fetchDocumentRow({ tenantId, documentId });
  if (!row) {
    throw badRequest("Document not found");
  }
  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "documentId");
  return mapDocumentRow(row);
}

export async function createCariDraftDocument({
  req,
  payload,
  assertScopeAccess,
}) {
  const tenantId = payload.tenantId;
  const legalEntityId = payload.legalEntityId;
  const counterpartyId = payload.counterpartyId;

  assertFrozenTransactionType(payload.direction, payload.documentType);
  await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
  assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
  await assertCurrencyExists(payload.currencyCode, "currencyCode");

  const created = await withTransaction(async (tx) => {
    const counterparty = await fetchCounterpartyRow({
      tenantId,
      legalEntityId,
      counterpartyId,
      runQuery: tx.query,
    });
    if (!counterparty) {
      throw badRequest("counterpartyId must belong to legalEntityId");
    }

    const paymentTerm = await fetchPaymentTermRow({
      tenantId,
      legalEntityId,
      paymentTermId: payload.paymentTermId,
      runQuery: tx.query,
    });
    if (payload.paymentTermId && !paymentTerm) {
      throw badRequest("paymentTermId must belong to legalEntityId");
    }

    const resolvedDueDate = resolveDueDate({
      documentDate: payload.documentDate,
      dueDate: payload.dueDate,
      documentType: payload.documentType,
      paymentTermRow: paymentTerm,
    });
    assertDateOrder(payload.documentDate, resolvedDueDate);
    assertDueDateByDocumentType({
      documentType: payload.documentType,
      dueDate: resolvedDueDate,
    });

    const draftNumbering = await reserveDraftSequence({
      tenantId,
      legalEntityId,
      direction: payload.direction,
      documentDate: payload.documentDate,
      runQuery: tx.query,
    });

    const insertResult = await tx.query(
      `INSERT INTO cari_documents (
          tenant_id,
          legal_entity_id,
          counterparty_id,
          payment_term_id,
          direction,
          document_type,
          sequence_namespace,
          fiscal_year,
          sequence_no,
          document_no,
          status,
          document_date,
          due_date,
          amount_txn,
          amount_base,
          open_amount_txn,
          open_amount_base,
          currency_code,
          fx_rate,
          counterparty_code_snapshot,
          counterparty_name_snapshot,
          payment_term_snapshot,
          due_date_snapshot,
          currency_code_snapshot,
          fx_rate_snapshot
       )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        tenantId,
        legalEntityId,
        counterpartyId,
        payload.paymentTermId,
        payload.direction,
        payload.documentType,
        draftNumbering.sequenceNamespace,
        draftNumbering.fiscalYear,
        draftNumbering.sequenceNo,
        draftNumbering.documentNo,
        DRAFT_STATUS,
        payload.documentDate,
        resolvedDueDate,
        payload.amountTxn,
        payload.amountBase,
        payload.amountTxn,
        payload.amountBase,
        payload.currencyCode,
        payload.fxRate,
        counterparty.code,
        counterparty.name,
        paymentTerm?.code || null,
        resolvedDueDate,
        payload.currencyCode,
        payload.fxRate,
      ]
    );
    const documentId = parsePositiveInt(insertResult.rows?.insertId);
    if (!documentId) {
      throw new Error("Document create failed");
    }

    const row = await fetchDocumentRow({
      tenantId,
      documentId,
      runQuery: tx.query,
    });
    if (!row) {
      throw new Error("Document create readback failed");
    }

    await insertAuditLog({
      req,
      runQuery: tx.query,
      tenantId,
      userId: payload.userId,
      action: "cari.document.draft.create",
      legalEntityId,
      documentId,
      payload: {
        direction: row.direction,
        documentType: row.document_type,
        status: row.status,
      },
    });

    return mapDocumentRow(row);
  });

  return created;
}

export async function updateCariDraftDocumentById({
  req,
  payload,
  assertScopeAccess,
}) {
  const tenantId = payload.tenantId;
  const documentId = payload.documentId;
  const existing = await fetchDocumentRow({
    tenantId,
    documentId,
  });
  if (!existing) {
    throw badRequest("Document not found");
  }

  const existingLegalEntityId = parsePositiveInt(existing.legal_entity_id);
  assertScopeAccess(req, "legal_entity", existingLegalEntityId, "documentId");
  if (existing.status !== DRAFT_STATUS) {
    throw badRequest("Only DRAFT documents can be updated");
  }

  if (
    payload.legalEntityId !== undefined &&
    payload.legalEntityId !== null &&
    payload.legalEntityId !== existingLegalEntityId
  ) {
    throw badRequest("legalEntityId cannot be changed for existing documents");
  }
  const legalEntityId = existingLegalEntityId;

  const nextDirection = payload.direction || existing.direction;
  const nextDocumentType = payload.documentType || existing.document_type;
  const nextDocumentDate =
    payload.documentDate || toDateOnlyString(existing.document_date, "documentDate");
  const nextCounterpartyId =
    payload.counterpartyId === undefined
      ? parsePositiveInt(existing.counterparty_id)
      : payload.counterpartyId;
  const nextPaymentTermId =
    payload.paymentTermId === undefined
      ? parsePositiveInt(existing.payment_term_id)
      : payload.paymentTermId;
  const nextAmountTxn =
    payload.amountTxn === undefined ? existing.amount_txn : payload.amountTxn;
  const nextAmountBase =
    payload.amountBase === undefined ? existing.amount_base : payload.amountBase;
  const nextCurrencyCode =
    payload.currencyCode === undefined ? existing.currency_code : payload.currencyCode;
  const nextFxRate = payload.fxRate === undefined ? existing.fx_rate : payload.fxRate;

  assertFrozenTransactionType(nextDirection, nextDocumentType);
  await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
  await assertCurrencyExists(nextCurrencyCode, "currencyCode");

  const updated = await withTransaction(async (tx) => {
    const counterparty = await fetchCounterpartyRow({
      tenantId,
      legalEntityId,
      counterpartyId: nextCounterpartyId,
      runQuery: tx.query,
    });
    if (!counterparty) {
      throw badRequest("counterpartyId must belong to legalEntityId");
    }

    const paymentTerm = await fetchPaymentTermRow({
      tenantId,
      legalEntityId,
      paymentTermId: nextPaymentTermId,
      runQuery: tx.query,
    });
    if (nextPaymentTermId && !paymentTerm) {
      throw badRequest("paymentTermId must belong to legalEntityId");
    }

    const requestedDueDate =
      payload.dueDate === undefined
        ? toDateOnlyString(existing.due_date, "dueDate")
        : payload.dueDate;
    const resolvedDueDate = resolveDueDate({
      documentDate: nextDocumentDate,
      dueDate: requestedDueDate,
      documentType: nextDocumentType,
      paymentTermRow: paymentTerm,
    });
    assertDateOrder(nextDocumentDate, resolvedDueDate);
    assertDueDateByDocumentType({
      documentType: nextDocumentType,
      dueDate: resolvedDueDate,
    });

    let sequenceNamespace = existing.sequence_namespace;
    let fiscalYear = Number(existing.fiscal_year);
    let sequenceNo = Number(existing.sequence_no);
    let documentNo = existing.document_no;

    const nextFiscalYear = Number(String(nextDocumentDate).slice(0, 4));
    const shouldReassignDraftNumber =
      existing.sequence_namespace === DRAFT_SEQUENCE_NAMESPACE &&
      (nextDirection !== existing.direction || nextFiscalYear !== Number(existing.fiscal_year));

    if (shouldReassignDraftNumber) {
      const draftNumbering = await reserveDraftSequence({
        tenantId,
        legalEntityId,
        direction: nextDirection,
        documentDate: nextDocumentDate,
        runQuery: tx.query,
      });
      sequenceNamespace = draftNumbering.sequenceNamespace;
      fiscalYear = draftNumbering.fiscalYear;
      sequenceNo = draftNumbering.sequenceNo;
      documentNo = draftNumbering.documentNo;
    }

    await tx.query(
      `UPDATE cari_documents
       SET counterparty_id = ?,
           payment_term_id = ?,
           direction = ?,
           document_type = ?,
           sequence_namespace = ?,
           fiscal_year = ?,
           sequence_no = ?,
           document_no = ?,
           document_date = ?,
           due_date = ?,
           amount_txn = ?,
           amount_base = ?,
           open_amount_txn = ?,
           open_amount_base = ?,
           currency_code = ?,
           fx_rate = ?,
           counterparty_code_snapshot = ?,
           counterparty_name_snapshot = ?,
           payment_term_snapshot = ?,
           due_date_snapshot = ?,
           currency_code_snapshot = ?,
           fx_rate_snapshot = ?
       WHERE tenant_id = ?
         AND id = ?`,
      [
        nextCounterpartyId,
        nextPaymentTermId,
        nextDirection,
        nextDocumentType,
        sequenceNamespace,
        fiscalYear,
        sequenceNo,
        documentNo,
        nextDocumentDate,
        resolvedDueDate,
        nextAmountTxn,
        nextAmountBase,
        nextAmountTxn,
        nextAmountBase,
        nextCurrencyCode,
        nextFxRate,
        counterparty.code,
        counterparty.name,
        paymentTerm?.code || null,
        resolvedDueDate,
        nextCurrencyCode,
        nextFxRate,
        tenantId,
        documentId,
      ]
    );

    const row = await fetchDocumentRow({
      tenantId,
      documentId,
      runQuery: tx.query,
    });
    if (!row) {
      throw new Error("Document update readback failed");
    }

    await insertAuditLog({
      req,
      runQuery: tx.query,
      tenantId,
      userId: payload.userId,
      action: "cari.document.draft.update",
      legalEntityId,
      documentId,
      payload: {
        before: {
          direction: existing.direction,
          documentType: existing.document_type,
          amountTxn: toDecimalNumber(existing.amount_txn),
          amountBase: toDecimalNumber(existing.amount_base),
          documentDate: existing.document_date,
          dueDate: existing.due_date,
        },
        after: {
          direction: row.direction,
          documentType: row.document_type,
          amountTxn: toDecimalNumber(row.amount_txn),
          amountBase: toDecimalNumber(row.amount_base),
          documentDate: row.document_date,
          dueDate: row.due_date,
        },
      },
    });

    return mapDocumentRow(row);
  });

  return updated;
}

export async function cancelCariDraftDocumentById({
  req,
  payload,
  assertScopeAccess,
}) {
  const tenantId = payload.tenantId;
  const documentId = payload.documentId;
  const existing = await fetchDocumentRow({
    tenantId,
    documentId,
  });
  if (!existing) {
    throw badRequest("Document not found");
  }

  const legalEntityId = parsePositiveInt(existing.legal_entity_id);
  assertScopeAccess(req, "legal_entity", legalEntityId, "documentId");
  if (existing.status !== DRAFT_STATUS) {
    throw badRequest("Only DRAFT documents can be cancelled");
  }

  const updated = await withTransaction(async (tx) => {
    await tx.query(
      `UPDATE cari_documents
       SET status = ?,
           open_amount_txn = 0.000000,
           open_amount_base = 0.000000
       WHERE tenant_id = ?
         AND id = ?`,
      [CANCELLED_STATUS, tenantId, documentId]
    );

    const row = await fetchDocumentRow({
      tenantId,
      documentId,
      runQuery: tx.query,
    });
    if (!row) {
      throw new Error("Document cancel readback failed");
    }

    await insertAuditLog({
      req,
      runQuery: tx.query,
      tenantId,
      userId: payload.userId,
      action: "cari.document.draft.cancel",
      legalEntityId,
      documentId,
      payload: {
        beforeStatus: existing.status,
        afterStatus: row.status,
      },
    });

    return mapDocumentRow(row);
  });

  return updated;
}

export async function postCariDocumentById({
  req,
  payload,
  assertScopeAccess,
}) {
  const tenantId = payload.tenantId;
  const documentId = payload.documentId;

  const existing = await fetchDocumentRow({
    tenantId,
    documentId,
  });
  if (!existing) {
    throw badRequest("Document not found");
  }

  const legalEntityId = parsePositiveInt(existing.legal_entity_id);
  assertScopeAccess(req, "legal_entity", legalEntityId, "documentId");
  if (normalizeUpperText(existing.status) !== DRAFT_STATUS) {
    throw badRequest("Only DRAFT documents can be posted");
  }

  const posted = await withTransaction(async (tx) => {
    const lockedDocument = await fetchDocumentRowForUpdate({
      tenantId,
      documentId,
      runQuery: tx.query,
    });
    if (!lockedDocument) {
      throw badRequest("Document not found");
    }

    const lockedLegalEntityId = parsePositiveInt(lockedDocument.legal_entity_id);
    if (normalizeUpperText(lockedDocument.status) !== DRAFT_STATUS) {
      throw badRequest("Only DRAFT documents can be posted");
    }

    await assertLegalEntityBelongsToTenant(
      tenantId,
      lockedLegalEntityId,
      "legalEntityId"
    );

    const documentDate = normalizeDateInput(
      lockedDocument.document_date,
      "documentDate"
    );
    const direction = normalizeUpperText(lockedDocument.direction);
    const documentType = normalizeUpperText(lockedDocument.document_type);
    const currencyCode = normalizeUpperText(lockedDocument.currency_code);
    const counterpartyId = parsePositiveInt(lockedDocument.counterparty_id);
    const paymentTermId = parsePositiveInt(lockedDocument.payment_term_id);

    const counterparty = await fetchCounterpartyRow({
      tenantId,
      legalEntityId: lockedLegalEntityId,
      counterpartyId,
      runQuery: tx.query,
    });
    if (!counterparty) {
      throw badRequest("counterpartyId must belong to legalEntityId");
    }

    const paymentTerm = await fetchPaymentTermRow({
      tenantId,
      legalEntityId: lockedLegalEntityId,
      paymentTermId,
      runQuery: tx.query,
    });
    if (paymentTermId && !paymentTerm) {
      throw badRequest("paymentTermId must belong to legalEntityId");
    }

    const resolvedDueDate = resolveDueDate({
      documentDate,
      dueDate: toDateOnlyString(lockedDocument.due_date, "dueDate"),
      documentType,
      paymentTermRow: paymentTerm,
    });
    assertDateOrder(documentDate, resolvedDueDate);
    assertDueDateByDocumentType({
      documentType,
      dueDate: resolvedDueDate,
    });

    const postedNumbering = await reservePostedSequence({
      tenantId,
      legalEntityId: lockedLegalEntityId,
      direction,
      documentType,
      documentDate,
      runQuery: tx.query,
    });

    const legalEntity = await assertLegalEntityBelongsToTenant(
      tenantId,
      lockedLegalEntityId,
      "legalEntityId"
    );
    const fxPolicy = await resolveFxPostingPolicy({
      tenantId,
      documentDate,
      documentCurrencyCode: currencyCode,
      functionalCurrencyCode: legalEntity.functional_currency_code,
      draftFxRate: lockedDocument.fx_rate,
      useFxOverride: Boolean(payload.useFxOverride),
      fxOverrideReason: payload.fxOverrideReason,
      runQuery: tx.query,
    });

    const postingAccounts = await resolveCariPostingAccounts({
      tenantId,
      legalEntityId: lockedLegalEntityId,
      direction,
      counterpartyRow: counterparty,
      runQuery: tx.query,
    });

    const amountTxn = normalizeAmount(lockedDocument.amount_txn, "amountTxn");
    const amountBase = normalizeAmount(lockedDocument.amount_base, "amountBase");
    const subledgerReferenceNo = `${CARI_SUBLEDGER_REFERENCE_PREFIX}${documentId}`;

    const postingLines = buildCariPostingLines({
      direction,
      documentType,
      amountTxn,
      amountBase,
      controlAccountId: postingAccounts.controlAccountId,
      offsetAccountId: postingAccounts.offsetAccountId,
      lineDescription: `Cari ${direction} ${documentType} ${postedNumbering.documentNo}`,
      subledgerReferenceNo,
      currencyCode,
    });

    const journalContext = await resolveBookAndOpenPeriodForDate({
      tenantId,
      legalEntityId: lockedLegalEntityId,
      targetDate: documentDate,
      runQuery: tx.query,
    });

    const journalResult = await insertPostedJournalWithLinesTx(tx, {
      tenantId,
      legalEntityId: lockedLegalEntityId,
      bookId: journalContext.bookId,
      fiscalPeriodId: journalContext.fiscalPeriodId,
      userId: payload.userId,
      journalNo: buildCariJournalNo("CARI", documentId),
      entryDate: documentDate,
      documentDate,
      currencyCode,
      description: `Cari ${direction} ${documentType} ${postedNumbering.documentNo}`.slice(
        0,
        500
      ),
      referenceNo: toNullableString(postedNumbering.documentNo, 100),
      lines: postingLines,
    });

    const paymentTermSnapshot = buildPaymentTermSnapshot(paymentTerm);
    await tx.query(
      `UPDATE cari_documents
       SET payment_term_id = ?,
           sequence_namespace = ?,
           fiscal_year = ?,
           sequence_no = ?,
           document_no = ?,
           status = ?,
           due_date = ?,
           amount_txn = ?,
           amount_base = ?,
           open_amount_txn = ?,
           open_amount_base = ?,
           fx_rate = ?,
           counterparty_code_snapshot = ?,
           counterparty_name_snapshot = ?,
           payment_term_snapshot = ?,
           due_date_snapshot = ?,
           currency_code_snapshot = ?,
           fx_rate_snapshot = ?,
           posted_journal_entry_id = ?,
           posted_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [
        paymentTermId || null,
        postedNumbering.sequenceNamespace,
        postedNumbering.fiscalYear,
        postedNumbering.sequenceNo,
        postedNumbering.documentNo,
        POSTED_STATUS,
        resolvedDueDate,
        amountTxn,
        amountBase,
        amountTxn,
        amountBase,
        fxPolicy.effectiveFxRate,
        counterparty.code,
        counterparty.name,
        paymentTermSnapshot,
        resolvedDueDate,
        currencyCode,
        fxPolicy.effectiveFxRate,
        journalResult.journalEntryId,
        tenantId,
        documentId,
      ]
    );

    const openItemDueDate = resolvedDueDate || documentDate;
    await tx.query(
      `INSERT INTO cari_open_items (
          tenant_id,
          legal_entity_id,
          counterparty_id,
          document_id,
          item_no,
          status,
          document_date,
          due_date,
          original_amount_txn,
          original_amount_base,
          residual_amount_txn,
          residual_amount_base,
          settled_amount_txn,
          settled_amount_base,
          currency_code
       )
       VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 0.000000, 0.000000, ?)`,
      [
        tenantId,
        lockedLegalEntityId,
        counterpartyId,
        documentId,
        OPEN_ITEM_STATUS_OPEN,
        documentDate,
        openItemDueDate,
        amountTxn,
        amountBase,
        amountTxn,
        amountBase,
        currencyCode,
      ]
    );

    const row = await fetchDocumentRow({
      tenantId,
      documentId,
      runQuery: tx.query,
    });
    if (!row) {
      throw new Error("Document post readback failed");
    }

    await insertAuditLog({
      req,
      runQuery: tx.query,
      tenantId,
      userId: payload.userId,
      action: "cari.document.post",
      legalEntityId: lockedLegalEntityId,
      documentId,
      payload: {
        status: row.status,
        sequenceNamespace: row.sequence_namespace,
        fiscalYear: Number(row.fiscal_year),
        sequenceNo: Number(row.sequence_no),
        documentNo: row.document_no,
        postedJournalEntryId: journalResult.journalEntryId,
        subledgerReferenceNo,
        fxRate: fxPolicy.effectiveFxRate,
      },
    });

    if (fxPolicy.overrideUsed) {
      await insertAuditLog({
        req,
        runQuery: tx.query,
        tenantId,
        userId: payload.userId,
        action: "cari.document.post.fx_override",
        legalEntityId: lockedLegalEntityId,
        documentId,
        payload: {
          reason: payload.fxOverrideReason || null,
          documentDate,
          documentCurrencyCode: currencyCode,
          functionalCurrencyCode: normalizeUpperText(
            legalEntity.functional_currency_code
          ),
          referenceFxRate: fxPolicy.referenceFxRate,
          overriddenFxRate: fxPolicy.effectiveFxRate,
          fxRateDate: fxPolicy.fxRateDate,
        },
      });
    }

    return {
      row: mapDocumentRow(row),
      journal: {
        journalEntryId: journalResult.journalEntryId,
        bookId: journalContext.bookId,
        fiscalPeriodId: journalContext.fiscalPeriodId,
        lineCount: journalResult.lineCount,
        totalDebit: journalResult.totalDebit,
        totalCredit: journalResult.totalCredit,
        subledgerReferenceNo,
      },
    };
  });

  return posted;
}

export async function reverseCariPostedDocumentById({
  req,
  payload,
  assertScopeAccess,
}) {
  const tenantId = payload.tenantId;
  const documentId = payload.documentId;

  const existing = await fetchDocumentRow({
    tenantId,
    documentId,
  });
  if (!existing) {
    throw badRequest("Document not found");
  }

  const legalEntityId = parsePositiveInt(existing.legal_entity_id);
  assertScopeAccess(req, "legal_entity", legalEntityId, "documentId");
  if (normalizeUpperText(existing.status) !== POSTED_STATUS) {
    throw badRequest("Only POSTED documents can be reversed");
  }

  try {
    const reversed = await withTransaction(async (tx) => {
      const original = await fetchDocumentRowForUpdate({
        tenantId,
        documentId,
        runQuery: tx.query,
      });
      if (!original) {
        throw badRequest("Document not found");
      }
      if (normalizeUpperText(original.status) !== POSTED_STATUS) {
        throw badRequest("Only POSTED documents can be reversed");
      }

      const lockedLegalEntityId = parsePositiveInt(original.legal_entity_id);
      const existingReversalId = await findReversalDocumentByOriginalId({
        tenantId,
        originalDocumentId: documentId,
        runQuery: tx.query,
      });
      if (existingReversalId) {
        throw badRequest("Document is already reversed");
      }

      const originalPostedJournalEntryId = parsePositiveInt(
        original.posted_journal_entry_id
      );
      if (!originalPostedJournalEntryId) {
        throw badRequest("Posted journal entry linkage is missing on document");
      }

      const originalJournalWithLines = await fetchPostedJournalWithLines({
        tenantId,
        journalEntryId: originalPostedJournalEntryId,
        runQuery: tx.query,
      });
      const originalJournal = originalJournalWithLines?.journal || null;
      const originalJournalLines = originalJournalWithLines?.lines || [];
      if (!originalJournal) {
        throw badRequest("Original posted journal not found for document reversal");
      }
      if (normalizeUpperText(originalJournal.status) !== POSTED_STATUS) {
        throw badRequest("Only POSTED journals can be reversed");
      }
      if (parsePositiveInt(originalJournal.reversal_journal_entry_id)) {
        throw badRequest("Journal is already reversed");
      }
      if (originalJournalLines.length === 0) {
        throw badRequest("Original journal has no lines to reverse");
      }

      const reversalDate =
        payload.reversalDate || toDateOnlyString(new Date(), "reversalDate");
      const reversalPeriodContext = await resolveBookAndOpenPeriodForDate({
        tenantId,
        legalEntityId: lockedLegalEntityId,
        targetDate: reversalDate,
        preferredBookId: parsePositiveInt(originalJournal.book_id),
        runQuery: tx.query,
      });

      const reversalSubledgerReferenceNo = `${CARI_SUBLEDGER_REVERSE_REFERENCE_PREFIX}${documentId}`;
      const reversalLines = originalJournalLines.map((line) => ({
        accountId: parsePositiveInt(line.account_id),
        debitBase: Number(line.credit_base || 0),
        creditBase: Number(line.debit_base || 0),
        amountTxn: Number((Number(line.amount_txn || 0) * -1).toFixed(AMOUNT_PRECISION_SCALE)),
        description: line.description
          ? String(line.description).slice(0, 255)
          : `Reversal of ${original.document_no || `DOC-${documentId}`}`,
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
        journalNo: buildCariJournalNo("CARI-REV", documentId),
        entryDate: reversalDate,
        documentDate: reversalDate,
        currencyCode: normalizeUpperText(original.currency_code),
        description: `Reversal of ${original.document_no || `DOC-${documentId}`}`.slice(
          0,
          500
        ),
        referenceNo: toNullableString(`REV:${original.document_no || documentId}`, 100),
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
          payload.reason || "Manual reversal",
          tenantId,
          originalPostedJournalEntryId,
        ]
      );
      if (Number(reverseJournalUpdateResult.rows?.affectedRows || 0) === 0) {
        throw badRequest("Journal is already reversed");
      }

      const reversalNumbering = await reservePostedSequence({
        tenantId,
        legalEntityId: lockedLegalEntityId,
        direction: original.direction,
        documentType: original.document_type,
        documentDate: reversalDate,
        runQuery: tx.query,
      });

      const reversalDocumentInsertResult = await tx.query(
        `INSERT INTO cari_documents (
            tenant_id,
            legal_entity_id,
            counterparty_id,
            payment_term_id,
            direction,
            document_type,
            sequence_namespace,
            fiscal_year,
            sequence_no,
            document_no,
            status,
            document_date,
            due_date,
            amount_txn,
            amount_base,
            open_amount_txn,
            open_amount_base,
            currency_code,
            fx_rate,
            counterparty_code_snapshot,
            counterparty_name_snapshot,
            payment_term_snapshot,
            due_date_snapshot,
            currency_code_snapshot,
            fx_rate_snapshot,
            posted_journal_entry_id,
            reversal_of_document_id,
            posted_at,
            reversed_at
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0.000000, 0.000000, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
        [
          tenantId,
          lockedLegalEntityId,
          parsePositiveInt(original.counterparty_id),
          parsePositiveInt(original.payment_term_id),
          normalizeUpperText(original.direction),
          normalizeUpperText(original.document_type),
          reversalNumbering.sequenceNamespace,
          reversalNumbering.fiscalYear,
          reversalNumbering.sequenceNo,
          reversalNumbering.documentNo,
          REVERSED_STATUS,
          reversalDate,
          reversalDate,
          normalizeAmount(original.amount_txn, "amountTxn"),
          normalizeAmount(original.amount_base, "amountBase"),
          normalizeUpperText(original.currency_code),
          normalizeOptionalPositiveDecimal(original.fx_rate, "fxRate"),
          original.counterparty_code_snapshot,
          original.counterparty_name_snapshot,
          original.payment_term_snapshot,
          reversalDate,
          original.currency_code_snapshot || original.currency_code,
          normalizeOptionalPositiveDecimal(original.fx_rate_snapshot, "fxRateSnapshot"),
          reversalJournalResult.journalEntryId,
          documentId,
        ]
      );
      const reversalDocumentId = parsePositiveInt(
        reversalDocumentInsertResult.rows?.insertId
      );
      if (!reversalDocumentId) {
        throw new Error("Reversal document create failed");
      }

      await tx.query(
        `UPDATE cari_documents
         SET status = ?,
             open_amount_txn = 0.000000,
             open_amount_base = 0.000000,
             reversed_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ?
           AND id = ?`,
        [REVERSED_STATUS, tenantId, documentId]
      );

      await tx.query(
        `UPDATE cari_open_items
         SET status = ?,
             residual_amount_txn = 0.000000,
             residual_amount_base = 0.000000,
             settled_amount_txn = 0.000000,
             settled_amount_base = 0.000000
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND document_id = ?`,
        [OPEN_ITEM_STATUS_CANCELLED, tenantId, lockedLegalEntityId, documentId]
      );

      const reversalRow = await fetchDocumentRow({
        tenantId,
        documentId: reversalDocumentId,
        runQuery: tx.query,
      });
      const originalRow = await fetchDocumentRow({
        tenantId,
        documentId,
        runQuery: tx.query,
      });
      if (!reversalRow || !originalRow) {
        throw new Error("Reversal readback failed");
      }

      await insertAuditLog({
        req,
        runQuery: tx.query,
        tenantId,
        userId: payload.userId,
        action: "cari.document.reverse",
        legalEntityId: lockedLegalEntityId,
        documentId,
        payload: {
          reason: payload.reason || null,
          originalDocumentId: documentId,
          reversalDocumentId,
          originalPostedJournalEntryId,
          reversalPostedJournalEntryId: reversalJournalResult.journalEntryId,
        },
      });

      return {
        row: mapDocumentRow(reversalRow),
        original: mapDocumentRow(originalRow),
        journal: {
          originalJournalEntryId: originalPostedJournalEntryId,
          reversalJournalEntryId: reversalJournalResult.journalEntryId,
          lineCount: reversalJournalResult.lineCount,
          totalDebit: reversalJournalResult.totalDebit,
          totalCredit: reversalJournalResult.totalCredit,
          subledgerReferenceNo: reversalSubledgerReferenceNo,
        },
      };
    });

    return reversed;
  } catch (err) {
    if (isDuplicateKeyError(err, "uk_cari_docs_single_reversal")) {
      throw badRequest("Document is already reversed");
    }
    throw err;
  }
}
