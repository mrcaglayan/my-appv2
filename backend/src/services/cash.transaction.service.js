import { query, withTransaction } from "../db.js";
import { assertAccountBelongsToTenant } from "../tenantGuards.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  buildOffsetPaginationResult,
  resolveOffsetPagination,
} from "../utils/pagination.js";
import {
  cancelCashTransaction,
  countCashTransitTransfers,
  countCashTransactions,
  findCashRegisterById,
  findCashTransitTransferById,
  findCashTransitTransferByIdempotency,
  findCashTransitTransferByInTransactionId,
  findCashTransitTransferByIntegrationEventUid,
  findCashTransitTransferByOutTransactionId,
  findCashTransitTransferScopeById,
  findCashTransactionById,
  findCashTransactionByIntegrationEventUid,
  findCashTransactionByIdempotency,
  findCashTransactionByReversalOf,
  findCashTransactionScopeById,
  findOpenCashSessionByRegisterId,
  findCashSessionById,
  generateCashTxnNoForLegalEntityYearTx,
  insertCashTransitTransfer,
  insertCashTransaction,
  listCashTransitTransfers,
  listCashTransactions,
  markCashTransitTransferCanceled,
  markCashTransitTransferInTransit,
  markCashTransitTransferReceived,
  markCashTransitTransferReversed,
  markCashTransactionAsReversed,
  postCashTransaction,
} from "./cash.queries.js";
import { assertRegisterOperationalConfig } from "./cash.register.service.js";
import { createAndPostCashJournalTx } from "./cash.service.js";
import {
  CARI_SETTLEMENT_FOLLOW_UP_RISKS,
  applyCariSettlement,
} from "./cari.settlement.service.js";

const TRANSFER_TXN_TYPES = new Set(["TRANSFER_OUT", "TRANSFER_IN"]);
const BANK_TXN_TYPES = new Set(["DEPOSIT_TO_BANK", "WITHDRAWAL_FROM_BANK"]);
const NON_BANK_COUNTER_ACCOUNT_REQUIRED_TXN_TYPES = new Set([
  "RECEIPT",
  "PAYOUT",
  "OPENING_FLOAT",
  "CLOSING_ADJUSTMENT",
  "VARIANCE",
]);
const MANUAL_PROHIBITED_TXN_TYPES = new Set(["VARIANCE"]);
const CANCELLABLE_TXN_STATUSES = new Set(["DRAFT", "SUBMITTED"]);
const POSTABLE_TXN_STATUSES = new Set(["DRAFT", "SUBMITTED", "APPROVED"]);
const CARI_LINKED_TXN_TYPES = new Set(["RECEIPT", "PAYOUT"]);
const CARI_COUNTERPARTY_TYPES = new Set(["CUSTOMER", "VENDOR"]);
const TRANSIT_STATUS_INITIATED = "INITIATED";
const TRANSIT_STATUS_IN_TRANSIT = "IN_TRANSIT";
const TRANSIT_STATUS_RECEIVED = "RECEIVED";
const TRANSIT_STATUS_CANCELED = "CANCELED";
const TRANSIT_STATUS_REVERSED = "REVERSED";

function nowMysqlDateTime() {
  return new Date().toISOString().slice(0, 19).replace("T", " ");
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function asUpper(value) {
  return String(value || "").trim().toUpperCase();
}

function isActive(value) {
  return asUpper(value) === "ACTIVE";
}

function assertStatusAllowed(actual, allowedSet, message) {
  if (!allowedSet.has(asUpper(actual))) {
    throw badRequest(message);
  }
}

function normalizeMoney(value) {
  return Number(value || 0).toFixed(6);
}

function normalizeCurrency(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

async function resolveSessionForCreate({
  tenantId,
  register,
  requestedSessionId,
  runQuery,
}) {
  const sessionMode = asUpper(register.session_mode);

  if (requestedSessionId) {
    const session = await findCashSessionById({
      tenantId,
      sessionId: requestedSessionId,
      runQuery,
    });
    if (!session) {
      throw badRequest("cashSessionId not found for tenant");
    }
    if (parsePositiveInt(session.cash_register_id) !== parsePositiveInt(register.id)) {
      throw badRequest("cashSessionId must belong to registerId");
    }
    if (asUpper(session.status) !== "OPEN") {
      throw badRequest("cashSessionId must be OPEN");
    }
    return session;
  }

  if (sessionMode === "NONE") {
    return null;
  }

  const openSession = await findOpenCashSessionByRegisterId({
    tenantId,
    registerId: register.id,
    runQuery,
  });

  if (sessionMode === "REQUIRED" && !openSession) {
    throw badRequest("An OPEN cash session is required for this register");
  }

  return openSession || null;
}

function validateTxnTypeSpecificRules(payload) {
  if (MANUAL_PROHIBITED_TXN_TYPES.has(payload.txnType)) {
    throw badRequest(`${payload.txnType} can only be system-generated`);
  }

  if (TRANSFER_TXN_TYPES.has(payload.txnType) && !payload.counterCashRegisterId) {
    throw badRequest(`${payload.txnType} requires counterCashRegisterId`);
  }

  if (
    (BANK_TXN_TYPES.has(payload.txnType) ||
      NON_BANK_COUNTER_ACCOUNT_REQUIRED_TXN_TYPES.has(payload.txnType)) &&
    !payload.counterAccountId
  ) {
    throw badRequest(`${payload.txnType} requires counterAccountId`);
  }
}

function resolveCashIntegrationDefaults(payload) {
  const hasLinkedCariRefs = Boolean(
    payload.linkedCariSettlementBatchId || payload.linkedCariUnappliedCashId
  );
  const sourceModule = payload.sourceModule || (hasLinkedCariRefs ? "CARI" : "MANUAL");
  const sourceEntityType =
    payload.sourceEntityType ||
    (payload.linkedCariSettlementBatchId
      ? "cari_settlement_batch"
      : payload.linkedCariUnappliedCashId
        ? "cari_unapplied_cash"
        : null);
  const sourceEntityId =
    payload.sourceEntityId ||
    (payload.linkedCariSettlementBatchId
      ? String(payload.linkedCariSettlementBatchId)
      : payload.linkedCariUnappliedCashId
        ? String(payload.linkedCariUnappliedCashId)
        : null);
  const integrationLinkStatus =
    payload.integrationLinkStatus || (hasLinkedCariRefs ? "LINKED" : "UNLINKED");
  return {
    sourceModule,
    sourceEntityType,
    sourceEntityId,
    integrationLinkStatus,
    hasLinkedCariRefs,
  };
}

async function fetchCariCounterpartyForRegister({
  tenantId,
  legalEntityId,
  counterpartyId,
  runQuery,
}) {
  const result = await runQuery(
    `SELECT id, is_customer, is_vendor
     FROM counterparties
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, counterpartyId]
  );
  return result.rows?.[0] || null;
}

async function fetchCariSettlementBatchForLink({
  tenantId,
  settlementBatchId,
  runQuery,
  forUpdate = false,
}) {
  const lockClause = forUpdate ? "FOR UPDATE" : "";
  const result = await runQuery(
    `SELECT id, legal_entity_id, counterparty_id, cash_transaction_id
     FROM cari_settlement_batches
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1
     ${lockClause}`,
    [tenantId, settlementBatchId]
  );
  return result.rows?.[0] || null;
}

async function fetchCariUnappliedCashForLink({
  tenantId,
  unappliedCashId,
  runQuery,
  forUpdate = false,
}) {
  const lockClause = forUpdate ? "FOR UPDATE" : "";
  const result = await runQuery(
    `SELECT id, legal_entity_id, counterparty_id, cash_transaction_id
     FROM cari_unapplied_cash
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1
     ${lockClause}`,
    [tenantId, unappliedCashId]
  );
  return result.rows?.[0] || null;
}

function normalizePositiveAmount(value, label) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw badRequest(`${label} must be a numeric value greater than 0`);
  }
  return Number(parsed.toFixed(6));
}

function toDateOnly(value, label) {
  const raw = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw badRequest(`${label} must be YYYY-MM-DD`);
  }
  return raw;
}

function parseDuplicateKeyError(err, constraintName = null) {
  if (Number(err?.errno) !== 1062) {
    return false;
  }
  if (!constraintName) {
    return true;
  }
  return String(err?.message || "").includes(constraintName);
}

function buildDerivedIdempotencyKey(prefix, rawValue) {
  const suffix = String(rawValue || "").trim();
  if (!suffix) {
    throw badRequest("idempotency key is required");
  }
  return `${String(prefix || "").trim()}:${suffix}`.slice(0, 100);
}

function assertTransitScopeAccess(req, transferRow, assertScopeAccess, fieldLabel) {
  assertScopeAccess(req, "legal_entity", transferRow.legal_entity_id, fieldLabel);
  const sourceOuId = parsePositiveInt(transferRow.source_operating_unit_id);
  if (sourceOuId) {
    assertScopeAccess(req, "operating_unit", sourceOuId, fieldLabel);
  }
  const targetOuId = parsePositiveInt(transferRow.target_operating_unit_id);
  if (targetOuId && targetOuId !== sourceOuId) {
    assertScopeAccess(req, "operating_unit", targetOuId, fieldLabel);
  }
}

function mapCariUnappliedCashRow(row) {
  if (!row) {
    return null;
  }
  return {
    id: parsePositiveInt(row.id),
    tenantId: parsePositiveInt(row.tenant_id),
    legalEntityId: parsePositiveInt(row.legal_entity_id),
    counterpartyId: parsePositiveInt(row.counterparty_id),
    cashTransactionId: parsePositiveInt(row.cash_transaction_id),
    cashReceiptNo: row.cash_receipt_no || null,
    receiptDate: row.receipt_date || null,
    status: row.status || null,
    amountTxn: row.amount_txn === null || row.amount_txn === undefined ? null : Number(row.amount_txn),
    amountBase: row.amount_base === null || row.amount_base === undefined ? null : Number(row.amount_base),
    residualAmountTxn:
      row.residual_amount_txn === null || row.residual_amount_txn === undefined
        ? null
        : Number(row.residual_amount_txn),
    residualAmountBase:
      row.residual_amount_base === null || row.residual_amount_base === undefined
        ? null
        : Number(row.residual_amount_base),
    currencyCode: row.currency_code || null,
    postedJournalEntryId: parsePositiveInt(row.posted_journal_entry_id),
    settlementBatchId: parsePositiveInt(row.settlement_batch_id),
    reversalOfUnappliedCashId: parsePositiveInt(row.reversal_of_unapplied_cash_id),
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

async function fetchOpenItemsByDocumentForCashApply({
  tenantId,
  legalEntityId,
  counterpartyId,
  currencyCode,
  documentId,
  runQuery,
}) {
  const result = await runQuery(
    `SELECT
       id,
       residual_amount_txn,
       due_date,
       document_date
     FROM cari_open_items
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND counterparty_id = ?
       AND document_id = ?
       AND currency_code = ?
       AND status IN ('OPEN', 'PARTIALLY_SETTLED')
       AND residual_amount_txn > 0
     ORDER BY due_date ASC, document_date ASC, id ASC`,
    [tenantId, legalEntityId, counterpartyId, documentId, currencyCode]
  );
  return result.rows || [];
}

async function resolveCashApplyAllocations({
  tenantId,
  legalEntityId,
  counterpartyId,
  currencyCode,
  applications,
  runQuery,
}) {
  if (!Array.isArray(applications) || applications.length === 0) {
    return [];
  }

  const allocationByOpenItem = new Map();
  for (const entry of applications) {
    const amountTxn = normalizePositiveAmount(entry?.amountTxn, "applications[].amountTxn");
    const explicitOpenItemId = parsePositiveInt(entry?.openItemId);
    if (explicitOpenItemId) {
      const current = Number(allocationByOpenItem.get(explicitOpenItemId) || 0);
      allocationByOpenItem.set(explicitOpenItemId, Number((current + amountTxn).toFixed(6)));
      continue;
    }

    const documentId = parsePositiveInt(entry?.cariDocumentId);
    if (!documentId) {
      throw badRequest("applications[] requires openItemId or cariDocumentId");
    }

    const documentOpenItems = await fetchOpenItemsByDocumentForCashApply({
      tenantId,
      legalEntityId,
      counterpartyId,
      currencyCode,
      documentId,
      runQuery,
    });
    if (!documentOpenItems.length) {
      throw badRequest(`No open items available for cariDocumentId=${documentId}`);
    }

    let remaining = amountTxn;
    for (const row of documentOpenItems) {
      if (remaining <= 0.000001) {
        break;
      }
      const openItemId = parsePositiveInt(row.id);
      const residual = normalizePositiveAmount(row.residual_amount_txn, "openItem.residualAmountTxn");
      const consume = remaining > residual ? residual : remaining;
      if (consume <= 0.000001) {
        continue;
      }
      const current = Number(allocationByOpenItem.get(openItemId) || 0);
      allocationByOpenItem.set(openItemId, Number((current + consume).toFixed(6)));
      remaining = Number((remaining - consume).toFixed(6));
    }

    if (remaining > 0.000001) {
      throw badRequest(
        `applications amount exceeds available residual for cariDocumentId=${documentId}`
      );
    }
  }

  return Array.from(allocationByOpenItem.entries()).map(([openItemId, amountTxn]) => ({
    openItemId,
    amountTxn,
  }));
}

async function fetchCariUnappliedCashById({ tenantId, unappliedCashId, runQuery }) {
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
       AND id = ?
     LIMIT 1`,
    [tenantId, unappliedCashId]
  );
  return result.rows?.[0] || null;
}

async function fetchCariUnappliedCashByCashTxnId({
  tenantId,
  legalEntityId,
  cashTransactionId,
  runQuery,
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
       posted_journal_entry_id,
       settlement_batch_id,
       reversal_of_unapplied_cash_id,
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
       AND cash_transaction_id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, cashTransactionId]
  );
  return result.rows?.[0] || null;
}

async function fetchCariUnappliedCashByIntegrationEventUid({
  tenantId,
  legalEntityId,
  integrationEventUid,
  runQuery,
}) {
  if (!integrationEventUid) {
    return null;
  }
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
       AND integration_event_uid = ?
     LIMIT 1`,
    [tenantId, legalEntityId, integrationEventUid]
  );
  return result.rows?.[0] || null;
}

export async function resolveCashTransactionScope(transactionId, tenantId) {
  const parsedTransactionId = parsePositiveInt(transactionId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedTransactionId || !parsedTenantId) {
    return null;
  }

  const row = await findCashTransactionScopeById({
    tenantId: parsedTenantId,
    transactionId: parsedTransactionId,
  });
  if (!row) {
    return null;
  }

  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: Number(row.legal_entity_id),
  };
}

export async function resolveCashTransitTransferScope(transitTransferId, tenantId) {
  const parsedTransitTransferId = parsePositiveInt(transitTransferId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedTransitTransferId || !parsedTenantId) {
    return null;
  }

  const row = await findCashTransitTransferScopeById({
    tenantId: parsedTenantId,
    transitTransferId: parsedTransitTransferId,
  });
  if (!row) {
    return null;
  }

  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: Number(row.legal_entity_id),
  };
}

export async function getCashTransitTransferByIdForTenant({
  req,
  tenantId,
  transitTransferId,
  assertScopeAccess,
}) {
  const row = await findCashTransitTransferById({
    tenantId,
    transitTransferId,
  });
  if (!row) {
    throw badRequest("Cash transit transfer not found");
  }

  assertTransitScopeAccess(req, row, assertScopeAccess, "transitTransferId");

  const transferOutTransactionId = parsePositiveInt(row.transfer_out_cash_transaction_id);
  const transferInTransactionId = parsePositiveInt(row.transfer_in_cash_transaction_id);
  const transferOutTransaction = transferOutTransactionId
    ? await findCashTransactionById({
        tenantId,
        transactionId: transferOutTransactionId,
      })
    : null;
  const transferInTransaction = transferInTransactionId
    ? await findCashTransactionById({
        tenantId,
        transactionId: transferInTransactionId,
      })
    : null;

  return {
    transfer: row,
    transferOutTransaction: transferOutTransaction || null,
    transferInTransaction: transferInTransaction || null,
  };
}

export async function listCashTransitTransferRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["ctt.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "ctt.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("ctt.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.sourceRegisterId) {
    const sourceRegister = await findCashRegisterById({
      tenantId,
      registerId: filters.sourceRegisterId,
    });
    if (!sourceRegister) {
      throw badRequest("sourceRegisterId not found for tenant");
    }
    assertScopeAccess(req, "legal_entity", sourceRegister.legal_entity_id, "sourceRegisterId");
    if (sourceRegister.operating_unit_id) {
      assertScopeAccess(req, "operating_unit", sourceRegister.operating_unit_id, "sourceRegisterId");
    }
    conditions.push("ctt.source_cash_register_id = ?");
    params.push(filters.sourceRegisterId);
  }

  if (filters.targetRegisterId) {
    const targetRegister = await findCashRegisterById({
      tenantId,
      registerId: filters.targetRegisterId,
    });
    if (!targetRegister) {
      throw badRequest("targetRegisterId not found for tenant");
    }
    assertScopeAccess(req, "legal_entity", targetRegister.legal_entity_id, "targetRegisterId");
    if (targetRegister.operating_unit_id) {
      assertScopeAccess(req, "operating_unit", targetRegister.operating_unit_id, "targetRegisterId");
    }
    conditions.push("ctt.target_cash_register_id = ?");
    params.push(filters.targetRegisterId);
  }

  if (filters.status) {
    conditions.push("ctt.status = ?");
    params.push(filters.status);
  }
  if (filters.initiatedDateFrom) {
    conditions.push("DATE(ctt.initiated_at) >= ?");
    params.push(filters.initiatedDateFrom);
  }
  if (filters.initiatedDateTo) {
    conditions.push("DATE(ctt.initiated_at) <= ?");
    params.push(filters.initiatedDateTo);
  }

  const whereSql = conditions.join(" AND ");
  const pagination = resolveOffsetPagination(filters, {
    defaultLimit: 50,
    defaultOffset: 0,
    maxLimit: 200,
  });
  const total = await countCashTransitTransfers({ whereSql, params });
  const rows = await listCashTransitTransfers({
    whereSql,
    params,
    limit: pagination.limit,
    offset: pagination.offset,
  });

  return buildOffsetPaginationResult({
    rows,
    total,
    limit: pagination.limit,
    offset: pagination.offset,
  });
}

export async function listCashTransactionRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["ct.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "cr.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("cr.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.registerId) {
    const register = await findCashRegisterById({
      tenantId,
      registerId: filters.registerId,
    });
    if (!register) {
      throw badRequest("registerId not found for tenant");
    }
    assertScopeAccess(req, "legal_entity", register.legal_entity_id, "registerId");
    if (register.operating_unit_id) {
      assertScopeAccess(req, "operating_unit", register.operating_unit_id, "registerId");
    }

    conditions.push("ct.cash_register_id = ?");
    params.push(filters.registerId);
  }

  if (filters.sessionId) {
    conditions.push("ct.cash_session_id = ?");
    params.push(filters.sessionId);
  }
  if (filters.txnType) {
    conditions.push("ct.txn_type = ?");
    params.push(filters.txnType);
  }
  if (filters.status) {
    conditions.push("ct.status = ?");
    params.push(filters.status);
  }
  if (filters.bookDateFrom) {
    conditions.push("ct.book_date >= ?");
    params.push(filters.bookDateFrom);
  }
  if (filters.bookDateTo) {
    conditions.push("ct.book_date <= ?");
    params.push(filters.bookDateTo);
  }

  const whereSql = conditions.join(" AND ");
  const pagination = resolveOffsetPagination(filters, {
    defaultLimit: 50,
    defaultOffset: 0,
    maxLimit: 200,
  });
  const total = await countCashTransactions({ whereSql, params });
  const rows = await listCashTransactions({
    whereSql,
    params,
    limit: pagination.limit,
    offset: pagination.offset,
  });

  return buildOffsetPaginationResult({
    rows,
    total,
    limit: pagination.limit,
    offset: pagination.offset,
  });
}

export async function getCashTransactionByIdForTenant({
  req,
  tenantId,
  transactionId,
  assertScopeAccess,
}) {
  const row = await findCashTransactionById({
    tenantId,
    transactionId,
  });
  if (!row) {
    throw badRequest("Cash transaction not found");
  }

  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "transactionId");
  if (row.operating_unit_id) {
    assertScopeAccess(req, "operating_unit", row.operating_unit_id, "transactionId");
  }

  return row;
}

async function loadTransitTransferBundle({
  tenantId,
  transitTransferId,
  runQuery = query,
  forUpdate = false,
}) {
  const transfer = await findCashTransitTransferById({
    tenantId,
    transitTransferId,
    runQuery,
    forUpdate,
  });
  if (!transfer) {
    return null;
  }
  const transferOutTransactionId = parsePositiveInt(transfer.transfer_out_cash_transaction_id);
  const transferInTransactionId = parsePositiveInt(transfer.transfer_in_cash_transaction_id);
  const transferOutTransaction = transferOutTransactionId
    ? await findCashTransactionById({
        tenantId,
        transactionId: transferOutTransactionId,
        runQuery,
        forUpdate,
      })
    : null;
  const transferInTransaction = transferInTransactionId
    ? await findCashTransactionById({
        tenantId,
        transactionId: transferInTransactionId,
        runQuery,
        forUpdate,
      })
    : null;
  return {
    transfer,
    transferOutTransaction: transferOutTransaction || null,
    transferInTransaction: transferInTransaction || null,
  };
}

async function createTransferCashTransactionTx({
  tx,
  tenantId,
  userId,
  register,
  requestedSessionId,
  txnType,
  amount,
  currencyCode,
  txnDatetime,
  bookDate,
  description,
  referenceNo,
  counterCashRegisterId,
  counterAccountId,
  sourceModule,
  sourceEntityType,
  sourceEntityId,
  integrationLinkStatus,
  idempotencyKey,
  integrationEventUid,
}) {
  validateTxnTypeSpecificRules({
    txnType,
    counterCashRegisterId,
    counterAccountId,
  });
  const linkedSession = await resolveSessionForCreate({
    tenantId,
    register,
    requestedSessionId,
    runQuery: tx.query,
  });
  const txnNo = await generateCashTxnNoForLegalEntityYearTx({
    tenantId,
    legalEntityId: register.legal_entity_id,
    legalEntityCode: register.legal_entity_code,
    bookDate,
    runQuery: tx.query,
  });

  const transactionId = await insertCashTransaction({
    payload: {
      tenantId,
      registerId: parsePositiveInt(register.id),
      cashSessionId: linkedSession?.id || null,
      txnNo,
      txnType,
      status: "DRAFT",
      txnDatetime,
      bookDate,
      amount: normalizeMoney(amount),
      currencyCode,
      description,
      referenceNo,
      sourceDocType: null,
      sourceDocId: null,
      sourceModule,
      sourceEntityType,
      sourceEntityId,
      integrationLinkStatus,
      counterpartyType: null,
      counterpartyId: null,
      counterAccountId,
      counterCashRegisterId,
      linkedCariSettlementBatchId: null,
      linkedCariUnappliedCashId: null,
      reversalOfTransactionId: null,
      overrideCashControl: false,
      overrideReason: null,
      idempotencyKey,
      integrationEventUid,
      userId,
      postedByUserId: null,
      postedAt: null,
    },
    runQuery: tx.query,
  });

  return findCashTransactionById({
    tenantId,
    transactionId,
    runQuery: tx.query,
  });
}

export async function initiateCashTransitTransfer({
  req,
  payload,
  assertScopeAccess,
}) {
  const integrationEventUid =
    payload.integrationEventUid ||
    buildDerivedIdempotencyKey(`TRANSIT_EVENT_${payload.registerId}`, payload.idempotencyKey);

  const replayByEvent = await findCashTransitTransferByIntegrationEventUid({
    tenantId: payload.tenantId,
    integrationEventUid,
  });
  if (replayByEvent) {
    const replayBundle = await loadTransitTransferBundle({
      tenantId: payload.tenantId,
      transitTransferId: parsePositiveInt(replayByEvent.id),
      runQuery: query,
    });
    if (replayBundle) {
      assertTransitScopeAccess(req, replayBundle.transfer, assertScopeAccess, "registerId");
      return {
        ...replayBundle,
        idempotentReplay: true,
      };
    }
  }

  try {
    return await withTransaction(async (tx) => {
      const replayByIdempotency = await findCashTransitTransferByIdempotency({
        tenantId: payload.tenantId,
        sourceRegisterId: payload.registerId,
        idempotencyKey: payload.idempotencyKey,
        runQuery: tx.query,
        forUpdate: true,
      });
      if (replayByIdempotency) {
        const replayBundle = await loadTransitTransferBundle({
          tenantId: payload.tenantId,
          transitTransferId: parsePositiveInt(replayByIdempotency.id),
          runQuery: tx.query,
          forUpdate: true,
        });
        if (!replayBundle) {
          throw badRequest("Failed to load existing cash transit transfer replay");
        }
        assertTransitScopeAccess(req, replayBundle.transfer, assertScopeAccess, "registerId");
        return {
          ...replayBundle,
          idempotentReplay: true,
        };
      }

      const sourceRegister = await findCashRegisterById({
        tenantId: payload.tenantId,
        registerId: payload.registerId,
        runQuery: tx.query,
      });
      if (!sourceRegister) {
        throw badRequest("registerId not found for tenant");
      }
      const targetRegister = await findCashRegisterById({
        tenantId: payload.tenantId,
        registerId: payload.targetRegisterId,
        runQuery: tx.query,
      });
      if (!targetRegister) {
        throw badRequest("targetRegisterId not found for tenant");
      }

      assertRegisterOperationalConfig(sourceRegister, {
        requireActive: true,
        requireCashControlledAccount: true,
      });
      assertRegisterOperationalConfig(targetRegister, {
        requireActive: true,
        requireCashControlledAccount: true,
      });

      assertScopeAccess(req, "legal_entity", sourceRegister.legal_entity_id, "registerId");
      if (sourceRegister.operating_unit_id) {
        assertScopeAccess(req, "operating_unit", sourceRegister.operating_unit_id, "registerId");
      }
      assertScopeAccess(req, "legal_entity", targetRegister.legal_entity_id, "targetRegisterId");
      if (
        targetRegister.operating_unit_id &&
        parsePositiveInt(targetRegister.operating_unit_id) !==
          parsePositiveInt(sourceRegister.operating_unit_id)
      ) {
        assertScopeAccess(req, "operating_unit", targetRegister.operating_unit_id, "targetRegisterId");
      }

      if (parsePositiveInt(sourceRegister.id) === parsePositiveInt(targetRegister.id)) {
        throw badRequest("registerId and targetRegisterId must be different");
      }
      if (
        parsePositiveInt(sourceRegister.legal_entity_id) !==
        parsePositiveInt(targetRegister.legal_entity_id)
      ) {
        throw badRequest("Cross-legal-entity cash transit transfer is not supported");
      }

      const sourceOuId = parsePositiveInt(sourceRegister.operating_unit_id);
      const targetOuId = parsePositiveInt(targetRegister.operating_unit_id);
      if (!sourceOuId || !targetOuId || sourceOuId === targetOuId) {
        throw badRequest(
          "Cash transit workflow requires source and target registers from different operating units"
        );
      }

      const sourceCurrency = normalizeCurrency(sourceRegister.currency_code);
      const targetCurrency = normalizeCurrency(targetRegister.currency_code);
      const requestedCurrency = normalizeCurrency(payload.currencyCode || sourceCurrency);
      if (!requestedCurrency || requestedCurrency.length !== 3) {
        throw badRequest("currencyCode must be a 3-letter code");
      }
      if (sourceCurrency !== targetCurrency || requestedCurrency !== sourceCurrency) {
        throw badRequest("Transit transfer currency must match both source and target register currency");
      }

      if (Number(sourceRegister.max_txn_amount || 0) > 0) {
        if (Number(payload.amount) > Number(sourceRegister.max_txn_amount)) {
          throw badRequest("amount exceeds register max_txn_amount");
        }
      }

      await assertAccountBelongsToTenant(
        payload.tenantId,
        payload.transitAccountId,
        "transitAccountId"
      );

      const transferOutTxnIdempotencyKey = buildDerivedIdempotencyKey(
        "TRANSIT_OUT",
        payload.idempotencyKey
      );
      const transferOutEventUid = buildDerivedIdempotencyKey("TRANSIT_OUT_EVENT", integrationEventUid);

      const transferOutDescription =
        payload.description ||
        `Transit transfer out to ${targetRegister.code || targetRegister.id}`;
      const transferOutReferenceNo =
        payload.referenceNo ||
        `TRANSIT-OUT-${sourceRegister.code || sourceRegister.id}-${targetRegister.code || targetRegister.id}`;

      const transferOutTransaction = await createTransferCashTransactionTx({
        tx,
        tenantId: payload.tenantId,
        userId: payload.userId,
        register: sourceRegister,
        requestedSessionId: payload.cashSessionId,
        txnType: "TRANSFER_OUT",
        amount: payload.amount,
        currencyCode: requestedCurrency,
        txnDatetime: payload.txnDatetime,
        bookDate: payload.bookDate,
        description: transferOutDescription,
        referenceNo: transferOutReferenceNo.slice(0, 100),
        counterCashRegisterId: parsePositiveInt(targetRegister.id),
        counterAccountId: payload.transitAccountId,
        sourceModule: "CASH",
        sourceEntityType: "cash_transit_transfer",
        sourceEntityId: "PENDING",
        integrationLinkStatus: "PENDING",
        idempotencyKey: transferOutTxnIdempotencyKey,
        integrationEventUid: transferOutEventUid,
      });
      if (!transferOutTransaction) {
        throw badRequest("Failed to create transfer-out transaction");
      }

      const transferOutTransactionId = parsePositiveInt(transferOutTransaction.id);
      const transitTransferId = await insertCashTransitTransfer({
        payload: {
          tenantId: payload.tenantId,
          legalEntityId: parsePositiveInt(sourceRegister.legal_entity_id),
          sourceCashRegisterId: parsePositiveInt(sourceRegister.id),
          targetCashRegisterId: parsePositiveInt(targetRegister.id),
          sourceOperatingUnitId: sourceOuId,
          targetOperatingUnitId: targetOuId,
          transferOutCashTransactionId: transferOutTransactionId,
          transferInCashTransactionId: null,
          status: TRANSIT_STATUS_INITIATED,
          amount: normalizeMoney(payload.amount),
          currencyCode: requestedCurrency,
          transitAccountId: payload.transitAccountId,
          initiatedByUserId: payload.userId,
          idempotencyKey: payload.idempotencyKey,
          integrationEventUid,
          sourceModule: "CASH",
          sourceEntityType: "cash_transaction",
          sourceEntityId: String(transferOutTransactionId),
          note: payload.note || null,
        },
        runQuery: tx.query,
      });
      if (!transitTransferId) {
        throw badRequest("Failed to create cash transit transfer");
      }

      await tx.query(
        `UPDATE cash_transactions
         SET source_module = 'CASH',
             source_entity_type = 'cash_transit_transfer',
             source_entity_id = ?,
             integration_link_status = 'LINKED'
         WHERE tenant_id = ?
           AND id = ?`,
        [String(transitTransferId), payload.tenantId, transferOutTransactionId]
      );

      const bundle = await loadTransitTransferBundle({
        tenantId: payload.tenantId,
        transitTransferId,
        runQuery: tx.query,
        forUpdate: true,
      });
      if (!bundle) {
        throw badRequest("Failed to load created cash transit transfer");
      }
      return {
        ...bundle,
        idempotentReplay: false,
      };
    });
  } catch (err) {
    const duplicateTransitIdempotency =
      parseDuplicateKeyError(err, "uk_cash_transit_tenant_source_register_idem") ||
      parseDuplicateKeyError(err, "uk_cash_transit_tenant_event_uid");
    const duplicateTransferOutTxn =
      parseDuplicateKeyError(err, "uk_cash_txn_tenant_register_idempotency") ||
      parseDuplicateKeyError(err, "uk_cash_txn_tenant_integration_event_uid");
    if (duplicateTransitIdempotency || duplicateTransferOutTxn) {
      const replay =
        (await findCashTransitTransferByIdempotency({
          tenantId: payload.tenantId,
          sourceRegisterId: payload.registerId,
          idempotencyKey: payload.idempotencyKey,
          runQuery: query,
        })) ||
        (await findCashTransitTransferByIntegrationEventUid({
          tenantId: payload.tenantId,
          integrationEventUid,
          runQuery: query,
        }));
      if (replay) {
        const bundle = await loadTransitTransferBundle({
          tenantId: payload.tenantId,
          transitTransferId: parsePositiveInt(replay.id),
          runQuery: query,
        });
        if (bundle) {
          assertTransitScopeAccess(req, bundle.transfer, assertScopeAccess, "registerId");
          return {
            ...bundle,
            idempotentReplay: true,
          };
        }
      }
    }
    throw err;
  }
}

export async function receiveCashTransitTransferById({
  req,
  payload,
  assertScopeAccess,
}) {
  try {
    return await withTransaction(async (tx) => {
      const transitTransfer = await findCashTransitTransferById({
        tenantId: payload.tenantId,
        transitTransferId: payload.transitTransferId,
        runQuery: tx.query,
        forUpdate: true,
      });
      if (!transitTransfer) {
        throw badRequest("Cash transit transfer not found");
      }

      assertTransitScopeAccess(req, transitTransfer, assertScopeAccess, "transitTransferId");

      const currentStatus = asUpper(transitTransfer.status);
      const existingTransferInTxnId = parsePositiveInt(transitTransfer.transfer_in_cash_transaction_id);
      if (currentStatus === TRANSIT_STATUS_RECEIVED && existingTransferInTxnId) {
        const replayBundle = await loadTransitTransferBundle({
          tenantId: payload.tenantId,
          transitTransferId: payload.transitTransferId,
          runQuery: tx.query,
          forUpdate: true,
        });
        return {
          ...replayBundle,
          idempotentReplay: true,
        };
      }

      if (currentStatus !== TRANSIT_STATUS_IN_TRANSIT) {
        throw badRequest("Cash transit transfer must be IN_TRANSIT before receive");
      }

      const transferOutTransactionId = parsePositiveInt(transitTransfer.transfer_out_cash_transaction_id);
      if (!transferOutTransactionId) {
        throw badRequest("Cash transit transfer is missing transfer-out transaction link");
      }
      const transferOutTransaction = await findCashTransactionById({
        tenantId: payload.tenantId,
        transactionId: transferOutTransactionId,
        runQuery: tx.query,
        forUpdate: true,
      });
      if (!transferOutTransaction) {
        throw badRequest("Linked transfer-out transaction not found");
      }
      if (asUpper(transferOutTransaction.status) !== "POSTED") {
        throw badRequest("Transfer-out transaction must be POSTED before receive");
      }

      const targetRegister = await findCashRegisterById({
        tenantId: payload.tenantId,
        registerId: parsePositiveInt(transitTransfer.target_cash_register_id),
        runQuery: tx.query,
      });
      if (!targetRegister) {
        throw badRequest("Target register not found for transit transfer");
      }
      assertRegisterOperationalConfig(targetRegister, {
        requireActive: true,
        requireCashControlledAccount: true,
      });

      const receiveTxnIdempotencyKey = buildDerivedIdempotencyKey(
        `TRANSIT_IN_${payload.transitTransferId}`,
        payload.idempotencyKey
      );
      const receiveTxnEventUid = buildDerivedIdempotencyKey(
        "TRANSIT_IN_EVENT",
        payload.integrationEventUid || `${payload.transitTransferId}:${payload.idempotencyKey}`
      );

      const referenceNo =
        payload.referenceNo ||
        transferOutTransaction.reference_no ||
        `TRANSIT-IN-${payload.transitTransferId}`;
      const description =
        payload.description ||
        `Transit receive from ${
          transferOutTransaction.cash_register_code || transferOutTransaction.cash_register_id
        }`;

      const transferInDraft = await createTransferCashTransactionTx({
        tx,
        tenantId: payload.tenantId,
        userId: payload.userId,
        register: targetRegister,
        requestedSessionId: payload.cashSessionId,
        txnType: "TRANSFER_IN",
        amount: transitTransfer.amount,
        currencyCode: transitTransfer.currency_code,
        txnDatetime: payload.txnDatetime,
        bookDate: payload.bookDate,
        description: description.slice(0, 500),
        referenceNo: referenceNo.slice(0, 100),
        counterCashRegisterId: parsePositiveInt(transitTransfer.source_cash_register_id),
        counterAccountId: parsePositiveInt(transitTransfer.transit_account_id),
        sourceModule: "CASH",
        sourceEntityType: "cash_transit_transfer",
        sourceEntityId: String(payload.transitTransferId),
        integrationLinkStatus: "LINKED",
        idempotencyKey: receiveTxnIdempotencyKey,
        integrationEventUid: receiveTxnEventUid,
      });
      if (!transferInDraft) {
        throw badRequest("Failed to create transfer-in transaction");
      }

      const transferInTransactionId = parsePositiveInt(transferInDraft.id);
      const transferInPosting = await createAndPostCashJournalTx(tx, {
        tenantId: payload.tenantId,
        userId: payload.userId,
        legalEntityId: parsePositiveInt(targetRegister.legal_entity_id),
        cashTxn: transferInDraft,
        req,
      });
      await postCashTransaction({
        tenantId: payload.tenantId,
        transactionId: transferInTransactionId,
        userId: payload.userId,
        postedJournalEntryId: transferInPosting.journalEntryId,
        overrideCashControl: false,
        overrideReason: null,
        runQuery: tx.query,
      });

      const markedReceived = await markCashTransitTransferReceived({
        tenantId: payload.tenantId,
        transitTransferId: payload.transitTransferId,
        transferInCashTransactionId: transferInTransactionId,
        receivedByUserId: payload.userId,
        runQuery: tx.query,
      });
      if (!markedReceived) {
        throw badRequest("Cash transit transfer is already received or not in transit");
      }

      const bundle = await loadTransitTransferBundle({
        tenantId: payload.tenantId,
        transitTransferId: payload.transitTransferId,
        runQuery: tx.query,
        forUpdate: true,
      });
      return {
        ...bundle,
        idempotentReplay: false,
      };
    });
  } catch (err) {
    const duplicateReceive =
      parseDuplicateKeyError(err, "uk_cash_transit_tenant_in_txn") ||
      parseDuplicateKeyError(err, "uk_cash_txn_tenant_register_idempotency") ||
      parseDuplicateKeyError(err, "uk_cash_txn_tenant_integration_event_uid");
    if (duplicateReceive) {
      const replayBundle = await loadTransitTransferBundle({
        tenantId: payload.tenantId,
        transitTransferId: payload.transitTransferId,
        runQuery: query,
      });
      if (replayBundle && parsePositiveInt(replayBundle.transfer?.transfer_in_cash_transaction_id)) {
        assertTransitScopeAccess(req, replayBundle.transfer, assertScopeAccess, "transitTransferId");
        return {
          ...replayBundle,
          idempotentReplay: true,
        };
      }
    }
    throw err;
  }
}

export async function cancelCashTransitTransferById({
  req,
  payload,
  assertScopeAccess,
}) {
  return withTransaction(async (tx) => {
    const transitTransfer = await findCashTransitTransferById({
      tenantId: payload.tenantId,
      transitTransferId: payload.transitTransferId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!transitTransfer) {
      throw badRequest("Cash transit transfer not found");
    }

    assertTransitScopeAccess(req, transitTransfer, assertScopeAccess, "transitTransferId");

    const currentStatus = asUpper(transitTransfer.status);
    if (currentStatus === TRANSIT_STATUS_CANCELED) {
      const replayBundle = await loadTransitTransferBundle({
        tenantId: payload.tenantId,
        transitTransferId: payload.transitTransferId,
        runQuery: tx.query,
        forUpdate: true,
      });
      return {
        ...replayBundle,
        idempotentReplay: true,
      };
    }
    if (currentStatus !== TRANSIT_STATUS_INITIATED) {
      throw badRequest("Only INITIATED cash transit transfer can be cancelled");
    }

    const transferOutTransactionId = parsePositiveInt(transitTransfer.transfer_out_cash_transaction_id);
    if (!transferOutTransactionId) {
      throw badRequest("Cash transit transfer is missing transfer-out transaction link");
    }
    const transferOutTransaction = await findCashTransactionById({
      tenantId: payload.tenantId,
      transactionId: transferOutTransactionId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!transferOutTransaction) {
      throw badRequest("Linked transfer-out transaction not found");
    }

    assertStatusAllowed(
      transferOutTransaction.status,
      CANCELLABLE_TXN_STATUSES,
      "Transit transfer-out transaction can only be cancelled from DRAFT or SUBMITTED status"
    );

    await cancelCashTransaction({
      tenantId: payload.tenantId,
      transactionId: transferOutTransactionId,
      userId: payload.userId,
      cancelReason: payload.cancelReason,
      runQuery: tx.query,
    });

    const markedCanceled = await markCashTransitTransferCanceled({
      tenantId: payload.tenantId,
      transitTransferId: payload.transitTransferId,
      canceledByUserId: payload.userId,
      cancelReason: payload.cancelReason,
      runQuery: tx.query,
    });
    if (!markedCanceled) {
      throw badRequest("Cash transit transfer cancellation failed");
    }

    const bundle = await loadTransitTransferBundle({
      tenantId: payload.tenantId,
      transitTransferId: payload.transitTransferId,
      runQuery: tx.query,
      forUpdate: true,
    });
    return {
      ...bundle,
      idempotentReplay: false,
    };
  });
}

export async function createCashTransaction({
  req,
  payload,
  assertScopeAccess,
}) {
  validateTxnTypeSpecificRules(payload);
  const integrationDefaults = resolveCashIntegrationDefaults(payload);
  if (
    (integrationDefaults.hasLinkedCariRefs || integrationDefaults.sourceModule === "CARI") &&
    !CARI_LINKED_TXN_TYPES.has(payload.txnType)
  ) {
    throw badRequest("Cari integration links are only supported for RECEIPT and PAYOUT cash transactions");
  }

  const register = await findCashRegisterById({
    tenantId: payload.tenantId,
    registerId: payload.registerId,
  });
  if (!register) {
    throw badRequest("registerId not found for tenant");
  }
  assertRegisterOperationalConfig(register, {
    requireActive: true,
    requireCashControlledAccount: true,
  });

  assertScopeAccess(req, "legal_entity", register.legal_entity_id, "registerId");
  if (register.operating_unit_id) {
    assertScopeAccess(req, "operating_unit", register.operating_unit_id, "registerId");
  }

  if (Number(register.max_txn_amount || 0) > 0) {
    if (Number(payload.amount) > Number(register.max_txn_amount)) {
      throw badRequest("amount exceeds register max_txn_amount");
    }
  }

  if (normalizeCurrency(payload.currencyCode) !== normalizeCurrency(register.currency_code)) {
    throw badRequest("Transaction currency must match register currency");
  }

  if (payload.counterAccountId) {
    await assertAccountBelongsToTenant(payload.tenantId, payload.counterAccountId, "counterAccountId");
  }

  if (payload.counterCashRegisterId) {
    const counterRegister = await findCashRegisterById({
      tenantId: payload.tenantId,
      registerId: payload.counterCashRegisterId,
    });
    if (!counterRegister) {
      throw badRequest("counterCashRegisterId not found for tenant");
    }
  }

  if (payload.integrationEventUid) {
    const replayByEvent = await findCashTransactionByIntegrationEventUid({
      tenantId: payload.tenantId,
      integrationEventUid: payload.integrationEventUid,
    });
    if (replayByEvent) {
      return {
        row: replayByEvent,
        idempotentReplay: true,
      };
    }
  }

  try {
    return await withTransaction(async (tx) => {
      if (payload.integrationEventUid) {
        const existingByEvent = await findCashTransactionByIntegrationEventUid({
          tenantId: payload.tenantId,
          integrationEventUid: payload.integrationEventUid,
          runQuery: tx.query,
        });
        if (existingByEvent) {
          return {
            row: existingByEvent,
            idempotentReplay: true,
          };
        }
      }

      const existing = await findCashTransactionByIdempotency({
        tenantId: payload.tenantId,
        registerId: payload.registerId,
        idempotencyKey: payload.idempotencyKey,
        runQuery: tx.query,
      });
      if (existing) {
        return {
          row: existing,
          idempotentReplay: true,
        };
      }

      if (
        integrationDefaults.sourceModule === "CARI" &&
        payload.counterpartyId &&
        !CARI_COUNTERPARTY_TYPES.has(asUpper(payload.counterpartyType))
      ) {
        throw badRequest("counterpartyType must be CUSTOMER or VENDOR when sourceModule=CARI");
      }
      if (payload.counterpartyId && CARI_COUNTERPARTY_TYPES.has(asUpper(payload.counterpartyType))) {
        const counterpartyRow = await fetchCariCounterpartyForRegister({
          tenantId: payload.tenantId,
          legalEntityId: parsePositiveInt(register.legal_entity_id),
          counterpartyId: payload.counterpartyId,
          runQuery: tx.query,
        });
        if (!counterpartyRow) {
          throw badRequest("counterpartyId must belong to register legalEntityId");
        }
        if (
          asUpper(payload.counterpartyType) === "CUSTOMER" &&
          !(counterpartyRow.is_customer === true || Number(counterpartyRow.is_customer) === 1)
        ) {
          throw badRequest("counterpartyId is not marked as customer");
        }
        if (
          asUpper(payload.counterpartyType) === "VENDOR" &&
          !(counterpartyRow.is_vendor === true || Number(counterpartyRow.is_vendor) === 1)
        ) {
          throw badRequest("counterpartyId is not marked as vendor");
        }
      }

      let linkedSettlement = null;
      if (payload.linkedCariSettlementBatchId) {
        linkedSettlement = await fetchCariSettlementBatchForLink({
          tenantId: payload.tenantId,
          settlementBatchId: payload.linkedCariSettlementBatchId,
          runQuery: tx.query,
          forUpdate: true,
        });
        if (!linkedSettlement) {
          throw badRequest("linkedCariSettlementBatchId not found for tenant");
        }
        if (
          parsePositiveInt(linkedSettlement.legal_entity_id) !==
          parsePositiveInt(register.legal_entity_id)
        ) {
          throw badRequest("linkedCariSettlementBatchId must belong to register legalEntityId");
        }
        const existingLinkedCashId = parsePositiveInt(linkedSettlement.cash_transaction_id);
        if (existingLinkedCashId) {
          throw badRequest("linkedCariSettlementBatchId is already linked to a cash transaction");
        }
        if (
          payload.counterpartyId &&
          parsePositiveInt(linkedSettlement.counterparty_id) &&
          parsePositiveInt(linkedSettlement.counterparty_id) !== payload.counterpartyId
        ) {
          throw badRequest("linkedCariSettlementBatchId counterparty does not match counterpartyId");
        }
      }

      let linkedUnapplied = null;
      if (payload.linkedCariUnappliedCashId) {
        linkedUnapplied = await fetchCariUnappliedCashForLink({
          tenantId: payload.tenantId,
          unappliedCashId: payload.linkedCariUnappliedCashId,
          runQuery: tx.query,
          forUpdate: true,
        });
        if (!linkedUnapplied) {
          throw badRequest("linkedCariUnappliedCashId not found for tenant");
        }
        if (
          parsePositiveInt(linkedUnapplied.legal_entity_id) !==
          parsePositiveInt(register.legal_entity_id)
        ) {
          throw badRequest("linkedCariUnappliedCashId must belong to register legalEntityId");
        }
        const existingLinkedCashId = parsePositiveInt(linkedUnapplied.cash_transaction_id);
        if (existingLinkedCashId) {
          throw badRequest("linkedCariUnappliedCashId is already linked to a cash transaction");
        }
        if (
          payload.counterpartyId &&
          parsePositiveInt(linkedUnapplied.counterparty_id) &&
          parsePositiveInt(linkedUnapplied.counterparty_id) !== payload.counterpartyId
        ) {
          throw badRequest("linkedCariUnappliedCashId counterparty does not match counterpartyId");
        }
      }

      if (
        linkedSettlement &&
        linkedUnapplied &&
        parsePositiveInt(linkedSettlement.counterparty_id) &&
        parsePositiveInt(linkedUnapplied.counterparty_id) &&
        parsePositiveInt(linkedSettlement.counterparty_id) !==
          parsePositiveInt(linkedUnapplied.counterparty_id)
      ) {
        throw badRequest(
          "linkedCariSettlementBatchId and linkedCariUnappliedCashId must target the same counterparty"
        );
      }

      const linkedSession = await resolveSessionForCreate({
        tenantId: payload.tenantId,
        register,
        requestedSessionId: payload.cashSessionId,
        runQuery: tx.query,
      });

      const txnNo = await generateCashTxnNoForLegalEntityYearTx({
        tenantId: payload.tenantId,
        legalEntityId: register.legal_entity_id,
        legalEntityCode: register.legal_entity_code,
        bookDate: payload.bookDate,
        runQuery: tx.query,
      });

      const transactionId = await insertCashTransaction({
        payload: {
          tenantId: payload.tenantId,
          registerId: payload.registerId,
          cashSessionId: linkedSession?.id || null,
          txnNo,
          txnType: payload.txnType,
          status: "DRAFT",
          txnDatetime: payload.txnDatetime,
          bookDate: payload.bookDate,
          amount: normalizeMoney(payload.amount),
          currencyCode: payload.currencyCode,
          description: payload.description,
          referenceNo: payload.referenceNo,
          sourceDocType: payload.sourceDocType,
          sourceDocId: payload.sourceDocId,
          sourceModule: integrationDefaults.sourceModule,
          sourceEntityType: integrationDefaults.sourceEntityType,
          sourceEntityId: integrationDefaults.sourceEntityId,
          integrationLinkStatus: integrationDefaults.integrationLinkStatus,
          counterpartyType: payload.counterpartyType,
          counterpartyId: payload.counterpartyId,
          counterAccountId: payload.counterAccountId,
          counterCashRegisterId: payload.counterCashRegisterId,
          linkedCariSettlementBatchId: payload.linkedCariSettlementBatchId,
          linkedCariUnappliedCashId: payload.linkedCariUnappliedCashId,
          reversalOfTransactionId: null,
          overrideCashControl: false,
          overrideReason: null,
          idempotencyKey: payload.idempotencyKey,
          integrationEventUid: payload.integrationEventUid || null,
          userId: payload.userId,
          postedByUserId: null,
          postedAt: null,
        },
        runQuery: tx.query,
      });

      if (linkedSettlement) {
        const settlementLinkUpdate = await tx.query(
          `UPDATE cari_settlement_batches
           SET cash_transaction_id = ?,
               source_module = COALESCE(source_module, 'CASH'),
               source_entity_type = COALESCE(source_entity_type, 'cash_transaction'),
               source_entity_id = COALESCE(source_entity_id, ?),
               integration_link_status = CASE
                 WHEN integration_link_status = 'UNLINKED' THEN 'LINKED'
                 ELSE integration_link_status
               END,
               integration_event_uid = COALESCE(integration_event_uid, ?)
           WHERE tenant_id = ?
             AND id = ?
             AND (cash_transaction_id IS NULL OR cash_transaction_id = ?)`,
          [
            transactionId,
            String(transactionId),
            payload.integrationEventUid || null,
            payload.tenantId,
            payload.linkedCariSettlementBatchId,
            transactionId,
          ]
        );
        if (Number(settlementLinkUpdate.rows?.affectedRows || 0) === 0) {
          throw badRequest("linkedCariSettlementBatchId is already linked to another cash transaction");
        }
      }

      if (linkedUnapplied) {
        const unappliedLinkUpdate = await tx.query(
          `UPDATE cari_unapplied_cash
           SET cash_transaction_id = ?,
               source_module = COALESCE(source_module, 'CASH'),
               source_entity_type = COALESCE(source_entity_type, 'cash_transaction'),
               source_entity_id = COALESCE(source_entity_id, ?),
               integration_link_status = CASE
                 WHEN integration_link_status = 'UNLINKED' THEN 'LINKED'
                 ELSE integration_link_status
               END,
               integration_event_uid = COALESCE(integration_event_uid, ?)
           WHERE tenant_id = ?
             AND id = ?
             AND (cash_transaction_id IS NULL OR cash_transaction_id = ?)`,
          [
            transactionId,
            String(transactionId),
            payload.integrationEventUid || null,
            payload.tenantId,
            payload.linkedCariUnappliedCashId,
            transactionId,
          ]
        );
        if (Number(unappliedLinkUpdate.rows?.affectedRows || 0) === 0) {
          throw badRequest("linkedCariUnappliedCashId is already linked to another cash transaction");
        }
      }

      const row = await findCashTransactionById({
        tenantId: payload.tenantId,
        transactionId,
        runQuery: tx.query,
      });
      return {
        row,
        idempotentReplay: false,
      };
    });
  } catch (err) {
    const isDuplicateKey =
      Number(err?.errno) === 1062 || String(err?.code || "").toUpperCase() === "ER_DUP_ENTRY";
    if (isDuplicateKey) {
      const duplicateMessage = String(err?.message || "");
      if (
        duplicateMessage.includes("uk_cari_settle_batches_tenant_cash_txn") ||
        duplicateMessage.includes("uk_cari_unap_tenant_cash_txn")
      ) {
        throw badRequest("Linked cari record is already connected to another cash transaction");
      }

      const replayRow = await findCashTransactionByIdempotency({
        tenantId: payload.tenantId,
        registerId: payload.registerId,
        idempotencyKey: payload.idempotencyKey,
      });
      if (replayRow) {
        return {
          row: replayRow,
          idempotentReplay: true,
        };
      }
      if (payload.integrationEventUid) {
        const replayByEvent = await findCashTransactionByIntegrationEventUid({
          tenantId: payload.tenantId,
          integrationEventUid: payload.integrationEventUid,
        });
        if (replayByEvent) {
          return {
            row: replayByEvent,
            idempotentReplay: true,
          };
        }
      }
      if (duplicateMessage.includes("uk_cash_txn_tenant_integration_event_uid")) {
        throw badRequest("Duplicate integrationEventUid");
      }
      throw badRequest("Duplicate transaction idempotency key");
    }
    throw err;
  }
}

async function createOrReplayCariUnappliedForCashTransaction({
  req,
  payload,
  cashTxn,
  assertScopeAccess,
}) {
  const tenantId = payload.tenantId;
  const legalEntityId = parsePositiveInt(cashTxn.legal_entity_id);
  const cashTransactionId = parsePositiveInt(cashTxn.id);
  const counterpartyId = parsePositiveInt(cashTxn.counterparty_id);
  const settlementDate = toDateOnly(payload.settlementDate || cashTxn.book_date, "settlementDate");
  const integrationEventUid = payload.integrationEventUid || payload.idempotencyKey;
  const amountTxn = normalizePositiveAmount(cashTxn.amount, "cashTransaction.amount");
  const amountBase = amountTxn;

  const existingLinkedUnappliedId = parsePositiveInt(cashTxn.linked_cari_unapplied_cash_id);
  if (existingLinkedUnappliedId) {
    const row = await fetchCariUnappliedCashById({
      tenantId,
      unappliedCashId: existingLinkedUnappliedId,
      runQuery: query,
    });
    if (row) {
      return {
        cashTransaction: cashTxn,
        row: null,
        allocations: [],
        unappliedCash: [mapCariUnappliedCashRow(row)],
        journal: null,
        metrics: {
          createdUnappliedCashId: parsePositiveInt(row.id),
        },
        idempotentReplay: true,
        followUpRisks: CARI_SETTLEMENT_FOLLOW_UP_RISKS,
      };
    }
  }

  try {
    return await withTransaction(async (tx) => {
      const lockedCashTxn = await findCashTransactionById({
        tenantId,
        transactionId: cashTransactionId,
        runQuery: tx.query,
        forUpdate: true,
      });
      if (!lockedCashTxn) {
        throw badRequest("Cash transaction not found");
      }
      assertScopeAccess(req, "legal_entity", lockedCashTxn.legal_entity_id, "transactionId");
      if (lockedCashTxn.operating_unit_id) {
        assertScopeAccess(req, "operating_unit", lockedCashTxn.operating_unit_id, "transactionId");
      }

      const replayByCashTxn = await fetchCariUnappliedCashByCashTxnId({
        tenantId,
        legalEntityId,
        cashTransactionId,
        runQuery: tx.query,
      });
      if (replayByCashTxn) {
        await tx.query(
          `UPDATE cash_transactions
           SET linked_cari_unapplied_cash_id = ?,
               integration_link_status = CASE
                 WHEN integration_link_status = 'UNLINKED' THEN 'LINKED'
                 ELSE integration_link_status
               END
           WHERE tenant_id = ?
             AND id = ?`,
          [parsePositiveInt(replayByCashTxn.id), tenantId, cashTransactionId]
        );

        const refreshedCashTxn = await findCashTransactionById({
          tenantId,
          transactionId: cashTransactionId,
          runQuery: tx.query,
        });
        return {
          cashTransaction: refreshedCashTxn || lockedCashTxn,
          row: null,
          allocations: [],
          unappliedCash: [mapCariUnappliedCashRow(replayByCashTxn)],
          journal: null,
          metrics: {
            createdUnappliedCashId: parsePositiveInt(replayByCashTxn.id),
          },
          idempotentReplay: true,
          followUpRisks: CARI_SETTLEMENT_FOLLOW_UP_RISKS,
        };
      }

      const replayByEvent = await fetchCariUnappliedCashByIntegrationEventUid({
        tenantId,
        legalEntityId,
        integrationEventUid,
        runQuery: tx.query,
      });
      if (replayByEvent) {
        if (parsePositiveInt(replayByEvent.cash_transaction_id) !== cashTransactionId) {
          throw badRequest("integrationEventUid is already used by another cash transaction");
        }
        await tx.query(
          `UPDATE cash_transactions
           SET linked_cari_unapplied_cash_id = ?,
               integration_link_status = CASE
                 WHEN integration_link_status = 'UNLINKED' THEN 'LINKED'
                 ELSE integration_link_status
               END
           WHERE tenant_id = ?
             AND id = ?`,
          [parsePositiveInt(replayByEvent.id), tenantId, cashTransactionId]
        );
        const refreshedCashTxn = await findCashTransactionById({
          tenantId,
          transactionId: cashTransactionId,
          runQuery: tx.query,
        });
        return {
          cashTransaction: refreshedCashTxn || lockedCashTxn,
          row: null,
          allocations: [],
          unappliedCash: [mapCariUnappliedCashRow(replayByEvent)],
          journal: null,
          metrics: {
            createdUnappliedCashId: parsePositiveInt(replayByEvent.id),
          },
          idempotentReplay: true,
          followUpRisks: CARI_SETTLEMENT_FOLLOW_UP_RISKS,
        };
      }

      const receiptNo = `UNAP-CASH-${cashTransactionId}`.slice(0, 80);
      const insertResult = await tx.query(
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
         VALUES (?, ?, ?, ?, ?, ?, 'UNAPPLIED', ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'CASH', 'cash_transaction', ?, 'LINKED', ?, ?)`,
        [
          tenantId,
          legalEntityId,
          counterpartyId,
          cashTransactionId,
          receiptNo,
          settlementDate,
          amountTxn,
          amountBase,
          amountTxn,
          amountBase,
          String(cashTxn.currency_code || "").trim().toUpperCase(),
          String(cashTransactionId),
          integrationEventUid,
          payload.note || `Unapplied cash generated from cash transaction ${cashTxn.txn_no || cashTxn.id}`,
        ]
      );
      const unappliedCashId = parsePositiveInt(insertResult.rows?.insertId);
      if (!unappliedCashId) {
        throw badRequest("Failed to create unapplied cash row");
      }

      await tx.query(
        `UPDATE cash_transactions
         SET linked_cari_unapplied_cash_id = ?,
             integration_link_status = CASE
               WHEN integration_link_status = 'UNLINKED' THEN 'LINKED'
               ELSE integration_link_status
             END,
             source_module = COALESCE(source_module, 'CASH'),
             source_entity_type = COALESCE(source_entity_type, 'cash_transaction'),
             source_entity_id = COALESCE(source_entity_id, ?),
             integration_event_uid = COALESCE(integration_event_uid, ?)
         WHERE tenant_id = ?
           AND id = ?
           AND (linked_cari_settlement_batch_id IS NULL)
           AND (linked_cari_unapplied_cash_id IS NULL OR linked_cari_unapplied_cash_id = ?)`,
        [
          unappliedCashId,
          String(cashTransactionId),
          integrationEventUid,
          tenantId,
          cashTransactionId,
          unappliedCashId,
        ]
      );

      const unappliedRow = await fetchCariUnappliedCashById({
        tenantId,
        unappliedCashId,
        runQuery: tx.query,
      });
      const refreshedCashTxn = await findCashTransactionById({
        tenantId,
        transactionId: cashTransactionId,
        runQuery: tx.query,
      });
      return {
        cashTransaction: refreshedCashTxn || lockedCashTxn,
        row: null,
        allocations: [],
        unappliedCash: unappliedRow ? [mapCariUnappliedCashRow(unappliedRow)] : [],
        journal: null,
        metrics: {
          createdUnappliedCashId: unappliedCashId,
        },
        idempotentReplay: false,
        followUpRisks: CARI_SETTLEMENT_FOLLOW_UP_RISKS,
      };
    });
  } catch (err) {
    if (
      parseDuplicateKeyError(err, "uk_cari_unap_tenant_cash_txn") ||
      parseDuplicateKeyError(err, "uk_cari_unap_tenant_event_uid")
    ) {
      const replay = await fetchCariUnappliedCashByCashTxnId({
        tenantId,
        legalEntityId,
        cashTransactionId,
        runQuery: query,
      });
      if (replay) {
        const refreshedCashTxn = await findCashTransactionById({
          tenantId,
          transactionId: cashTransactionId,
        });
        return {
          cashTransaction: refreshedCashTxn || cashTxn,
          row: null,
          allocations: [],
          unappliedCash: [mapCariUnappliedCashRow(replay)],
          journal: null,
          metrics: {
            createdUnappliedCashId: parsePositiveInt(replay.id),
          },
          idempotentReplay: true,
          followUpRisks: CARI_SETTLEMENT_FOLLOW_UP_RISKS,
        };
      }
    }
    throw err;
  }
}

export async function applyCariFromCashTransactionById({
  req,
  payload,
  assertScopeAccess,
}) {
  const cashTxn = await findCashTransactionById({
    tenantId: payload.tenantId,
    transactionId: payload.transactionId,
  });
  if (!cashTxn) {
    throw badRequest("Cash transaction not found");
  }

  assertScopeAccess(req, "legal_entity", cashTxn.legal_entity_id, "transactionId");
  if (cashTxn.operating_unit_id) {
    assertScopeAccess(req, "operating_unit", cashTxn.operating_unit_id, "transactionId");
  }

  if (asUpper(cashTxn.status) !== "POSTED") {
    throw badRequest("Cash transaction must be POSTED before applying Cari settlement");
  }

  const txnType = asUpper(cashTxn.txn_type);
  if (!CARI_LINKED_TXN_TYPES.has(txnType)) {
    throw badRequest("Only RECEIPT and PAYOUT cash transactions can be applied to Cari");
  }

  const direction = txnType === "RECEIPT" ? "AR" : "AP";
  const requiredCounterpartyType = direction === "AR" ? "CUSTOMER" : "VENDOR";
  const counterpartyType = asUpper(cashTxn.counterparty_type);
  const counterpartyId = parsePositiveInt(cashTxn.counterparty_id);
  if (!counterpartyId || counterpartyType !== requiredCounterpartyType) {
    throw badRequest(
      `Cash transaction must include counterpartyType=${requiredCounterpartyType} and counterpartyId`
    );
  }

  const legalEntityId = parsePositiveInt(cashTxn.legal_entity_id);
  const settlementDate = toDateOnly(payload.settlementDate || cashTxn.book_date, "settlementDate");
  const integrationEventUid =
    payload.integrationEventUid || `CASH-APPLY-${payload.transactionId}-${payload.idempotencyKey}`;
  const autoAllocate = payload.autoAllocate === true;
  const linkedSettlementBatchId = parsePositiveInt(cashTxn.linked_cari_settlement_batch_id);
  const linkedUnappliedCashId = parsePositiveInt(cashTxn.linked_cari_unapplied_cash_id);
  const settlementCurrencyCode = String(cashTxn.currency_code || "").trim().toUpperCase();
  const incomingAmountTxn = linkedUnappliedCashId ? 0 : Number(cashTxn.amount || 0);
  const useUnappliedCash = linkedUnappliedCashId ? true : payload.useUnappliedCash;

  const runSettlementApply = async (allocations, shouldAutoAllocate) => {
    return applyCariSettlement({
      req,
      payload: {
        tenantId: payload.tenantId,
        userId: payload.userId,
        legalEntityId,
        counterpartyId,
        direction,
        settlementDate,
        cashTransactionId: parsePositiveInt(cashTxn.id),
        currencyCode: settlementCurrencyCode,
        idempotencyKey: payload.idempotencyKey,
        incomingAmountTxn,
        useUnappliedCash,
        note: payload.note,
        autoAllocate: shouldAutoAllocate,
        fxRate: payload.fxRate,
        allocations,
        sourceModule: "CASH",
        sourceEntityType: "cash_transaction",
        sourceEntityId: String(cashTxn.id),
        integrationLinkStatus: "LINKED",
        integrationEventUid,
        bankApplyIdempotencyKey: null,
        bankStatementLineId: null,
        bankTransactionRef: null,
      },
      assertScopeAccess,
    });
  };

  if (linkedSettlementBatchId) {
    const replayResult = await runSettlementApply([], false);
    const refreshedCashTxnForReplay = await findCashTransactionById({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
    });
    return {
      ...replayResult,
      cashTransaction: refreshedCashTxnForReplay || cashTxn,
    };
  }

  const applications = await resolveCashApplyAllocations({
    tenantId: payload.tenantId,
    legalEntityId,
    counterpartyId,
    currencyCode: settlementCurrencyCode,
    applications: payload.applications,
    runQuery: query,
  });

  if (!autoAllocate && applications.length === 0) {
    return createOrReplayCariUnappliedForCashTransaction({
      req,
      payload: {
        ...payload,
        settlementDate,
        integrationEventUid,
      },
      cashTxn,
      assertScopeAccess,
    });
  }

  const settlementResult = await runSettlementApply(applications, autoAllocate);

  const refreshedCashTxn = await findCashTransactionById({
    tenantId: payload.tenantId,
    transactionId: payload.transactionId,
  });
  return {
    ...settlementResult,
    cashTransaction: refreshedCashTxn || cashTxn,
  };
}

export async function cancelCashTransactionById({
  req,
  payload,
  assertScopeAccess,
}) {
  return withTransaction(async (tx) => {
    const row = await findCashTransactionById({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!row) {
      throw badRequest("Cash transaction not found");
    }

    assertScopeAccess(req, "legal_entity", row.legal_entity_id, "transactionId");
    if (row.operating_unit_id) {
      assertScopeAccess(req, "operating_unit", row.operating_unit_id, "transactionId");
    }

    assertStatusAllowed(
      row.status,
      CANCELLABLE_TXN_STATUSES,
      "Only DRAFT or SUBMITTED transactions can be cancelled"
    );

    const linkedTransitAsOut = await findCashTransitTransferByOutTransactionId({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
      runQuery: tx.query,
      forUpdate: true,
    });
    const linkedTransitAsIn = linkedTransitAsOut
      ? null
      : await findCashTransitTransferByInTransactionId({
          tenantId: payload.tenantId,
          transactionId: payload.transactionId,
          runQuery: tx.query,
          forUpdate: true,
        });

    if (linkedTransitAsIn) {
      throw badRequest(
        "Transit receive transaction cannot be cancelled; reverse the transaction instead"
      );
    }
    if (linkedTransitAsOut && asUpper(linkedTransitAsOut.status) !== TRANSIT_STATUS_INITIATED) {
      throw badRequest(
        "Transit transfer-out can only be cancelled while transfer status is INITIATED"
      );
    }

    await cancelCashTransaction({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
      userId: payload.userId,
      cancelReason: payload.cancelReason,
      runQuery: tx.query,
    });

    if (linkedTransitAsOut) {
      const markedCanceled = await markCashTransitTransferCanceled({
        tenantId: payload.tenantId,
        transitTransferId: parsePositiveInt(linkedTransitAsOut.id),
        canceledByUserId: payload.userId,
        cancelReason: payload.cancelReason,
        runQuery: tx.query,
      });
      if (!markedCanceled && asUpper(linkedTransitAsOut.status) !== TRANSIT_STATUS_CANCELED) {
        throw badRequest("Transit transfer status update failed during cancellation");
      }
    }

    return findCashTransactionById({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
      runQuery: tx.query,
    });
  });
}

export async function postCashTransactionById({
  req,
  payload,
  assertScopeAccess,
}) {
  const posted = await withTransaction(async (tx) => {
    const row = await findCashTransactionById({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!row) {
      throw badRequest("Cash transaction not found");
    }

    assertScopeAccess(req, "legal_entity", row.legal_entity_id, "transactionId");
    if (row.operating_unit_id) {
      assertScopeAccess(req, "operating_unit", row.operating_unit_id, "transactionId");
    }

    if (asUpper(row.status) === "POSTED") {
      return {
        row,
        idempotentReplay: true,
      };
    }

    assertStatusAllowed(
      row.status,
      POSTABLE_TXN_STATUSES,
      "Only DRAFT, SUBMITTED, or APPROVED transactions can be posted"
    );

    if (!isActive(row.register_status)) {
      throw badRequest("Cash register is not ACTIVE");
    }

    const sessionMode = asUpper(row.register_session_mode);
    if (sessionMode === "REQUIRED") {
      if (!row.cash_session_id) {
        throw badRequest("Posting requires an OPEN cash session");
      }
      if (asUpper(row.cash_session_status) !== "OPEN") {
        throw badRequest("Posting requires cash_session_id to be OPEN");
      }
    }

    const posting = await createAndPostCashJournalTx(tx, {
      tenantId: payload.tenantId,
      userId: payload.userId,
      legalEntityId: parsePositiveInt(row.legal_entity_id),
      cashTxn: row,
      req,
    });

    await postCashTransaction({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
      userId: payload.userId,
      postedJournalEntryId: posting.journalEntryId,
      overrideCashControl: payload.overrideCashControl,
      overrideReason: payload.overrideCashControl ? payload.overrideReason : null,
      runQuery: tx.query,
    });

    if (asUpper(row.txn_type) === "TRANSFER_OUT") {
      const linkedTransit = await findCashTransitTransferByOutTransactionId({
        tenantId: payload.tenantId,
        transactionId: payload.transactionId,
        runQuery: tx.query,
        forUpdate: true,
      });
      if (linkedTransit) {
        const markedInTransit = await markCashTransitTransferInTransit({
          tenantId: payload.tenantId,
          transitTransferId: parsePositiveInt(linkedTransit.id),
          runQuery: tx.query,
        });
        const currentTransitStatus = asUpper(linkedTransit.status);
        if (
          !markedInTransit &&
          currentTransitStatus !== TRANSIT_STATUS_IN_TRANSIT &&
          currentTransitStatus !== TRANSIT_STATUS_RECEIVED
        ) {
          throw badRequest("Transit transfer status update failed during transfer-out posting");
        }
      }
    }

    const saved = await findCashTransactionById({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
      runQuery: tx.query,
    });

    return {
      row: saved,
      idempotentReplay: false,
    };
  });

  return posted;
}

export async function reverseCashTransactionById({
  req,
  payload,
  assertScopeAccess,
}) {
  const reversed = await withTransaction(async (tx) => {
    const original = await findCashTransactionById({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!original) {
      throw badRequest("Cash transaction not found");
    }

    assertScopeAccess(req, "legal_entity", original.legal_entity_id, "transactionId");
    if (original.operating_unit_id) {
      assertScopeAccess(req, "operating_unit", original.operating_unit_id, "transactionId");
    }

    const linkedTransitAsOut = await findCashTransitTransferByOutTransactionId({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
      runQuery: tx.query,
      forUpdate: true,
    });
    const linkedTransitAsIn = linkedTransitAsOut
      ? null
      : await findCashTransitTransferByInTransactionId({
          tenantId: payload.tenantId,
          transactionId: payload.transactionId,
          runQuery: tx.query,
          forUpdate: true,
        });
    if (linkedTransitAsOut && asUpper(linkedTransitAsOut.status) === TRANSIT_STATUS_RECEIVED) {
      throw badRequest(
        "Cannot reverse transfer-out after transit is RECEIVED; reverse transfer-in first"
      );
    }

    const existingReversal = await findCashTransactionByReversalOf({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
      runQuery: tx.query,
    });

    if (asUpper(original.status) === "REVERSED" && existingReversal) {
      return {
        original,
        reversal: existingReversal,
        idempotentReplay: true,
      };
    }

    if (parsePositiveInt(original.reversal_of_transaction_id)) {
      throw badRequest("Reversal transactions cannot be reversed");
    }

    if (asUpper(original.status) !== "POSTED") {
      throw badRequest("Only POSTED transactions can be reversed");
    }

    let reversal = existingReversal;
    if (!reversal) {
      const reversalBookDate = todayIsoDate();
      const originalTxnType = asUpper(original.txn_type);
      const isTransitLinkedTransfer =
        (originalTxnType === "TRANSFER_OUT" || originalTxnType === "TRANSFER_IN") &&
        asUpper(original.source_entity_type) === "CASH_TRANSIT_TRANSFER";
      const reversalTxnNo = await generateCashTxnNoForLegalEntityYearTx({
        tenantId: payload.tenantId,
        legalEntityId: original.legal_entity_id,
        legalEntityCode: original.legal_entity_code,
        bookDate: reversalBookDate,
        runQuery: tx.query,
      });

      const reversalId = await insertCashTransaction({
        payload: {
          tenantId: payload.tenantId,
          registerId: original.cash_register_id,
          cashSessionId: original.cash_session_id,
          txnNo: reversalTxnNo,
          txnType: original.txn_type,
          status: "DRAFT",
          txnDatetime: nowMysqlDateTime(),
          bookDate: reversalBookDate,
          amount: normalizeMoney(original.amount),
          currencyCode: original.currency_code,
          description: `Reversal of ${original.txn_no}: ${payload.reverseReason}`,
          referenceNo: original.reference_no,
          sourceDocType: original.source_doc_type,
          sourceDocId: original.source_doc_id,
          sourceModule: "CASH",
          sourceEntityType: isTransitLinkedTransfer
            ? original.source_entity_type
            : "cash_transaction_reversal",
          sourceEntityId: isTransitLinkedTransfer
            ? original.source_entity_id
            : String(original.id),
          integrationLinkStatus: isTransitLinkedTransfer
            ? original.integration_link_status || "LINKED"
            : "UNLINKED",
          counterpartyType: original.counterparty_type,
          counterpartyId: original.counterparty_id,
          counterAccountId: original.counter_account_id,
          counterCashRegisterId: original.counter_cash_register_id,
          linkedCariSettlementBatchId: null,
          linkedCariUnappliedCashId: null,
          reversalOfTransactionId: original.id,
          overrideCashControl: false,
          overrideReason: null,
          idempotencyKey: `REV-${original.id}`,
          integrationEventUid: `REV-${original.id}`,
          userId: payload.userId,
          postedByUserId: null,
          postedAt: null,
        },
        runQuery: tx.query,
      });

      reversal = await findCashTransactionById({
        tenantId: payload.tenantId,
        transactionId: reversalId,
        runQuery: tx.query,
      });
    }

    if (!reversal) {
      throw badRequest("Failed to create reversal cash transaction");
    }
    const reversalPostedJournalEntryId = parsePositiveInt(reversal.posted_journal_entry_id);
    if (asUpper(reversal.status) !== "POSTED" || !reversalPostedJournalEntryId) {
      const reversalPosting = await createAndPostCashJournalTx(tx, {
        tenantId: payload.tenantId,
        userId: payload.userId,
        legalEntityId: parsePositiveInt(reversal.legal_entity_id),
        cashTxn: reversal,
        req,
      });

      await postCashTransaction({
        tenantId: payload.tenantId,
        transactionId: parsePositiveInt(reversal.id),
        userId: payload.userId,
        postedJournalEntryId: reversalPosting.journalEntryId,
        overrideCashControl: false,
        overrideReason: null,
        runQuery: tx.query,
      });

      reversal = await findCashTransactionById({
        tenantId: payload.tenantId,
        transactionId: parsePositiveInt(reversal.id),
        runQuery: tx.query,
      });
    }

    await markCashTransactionAsReversed({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
      userId: payload.userId,
      runQuery: tx.query,
    });

    const linkedTransit = linkedTransitAsOut || linkedTransitAsIn;
    if (linkedTransit) {
      const markedReversed = await markCashTransitTransferReversed({
        tenantId: payload.tenantId,
        transitTransferId: parsePositiveInt(linkedTransit.id),
        reversedByUserId: payload.userId,
        reverseReason: payload.reverseReason,
        runQuery: tx.query,
      });
      if (!markedReversed && asUpper(linkedTransit.status) !== TRANSIT_STATUS_REVERSED) {
        throw badRequest("Transit transfer status update failed during reversal");
      }
    }

    const refreshedOriginal = await findCashTransactionById({
      tenantId: payload.tenantId,
      transactionId: payload.transactionId,
      runQuery: tx.query,
    });

    return {
      original: refreshedOriginal,
      reversal,
      idempotentReplay: false,
    };
  });

  return reversed;
}
