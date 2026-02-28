import { query } from "../db.js";

const CASH_REGISTER_BASE_SELECT = `
  SELECT
    cr.id,
    cr.tenant_id,
    cr.legal_entity_id,
    cr.operating_unit_id,
    cr.account_id,
    cr.code,
    cr.name,
    cr.register_type,
    cr.session_mode,
    cr.currency_code,
    cr.status,
    cr.allow_negative,
    cr.variance_gain_account_id,
    cr.variance_loss_account_id,
    cr.max_txn_amount,
    cr.requires_approval_over_amount,
    cr.created_by_user_id,
    cr.created_at,
    cr.updated_at,
    le.code AS legal_entity_code,
    le.name AS legal_entity_name,
    ou.code AS operating_unit_code,
    ou.name AS operating_unit_name,
    a.code AS account_code,
    a.name AS account_name,
    a.allow_posting AS account_allow_posting,
    a.parent_account_id AS account_parent_account_id,
    a.is_active AS account_is_active,
    a.is_cash_controlled AS account_is_cash_controlled,
    c.scope AS account_scope,
    c.legal_entity_id AS account_legal_entity_id,
    vg.code AS variance_gain_account_code,
    vg.name AS variance_gain_account_name,
    vl.code AS variance_loss_account_code,
    vl.name AS variance_loss_account_name
  FROM cash_registers cr
  JOIN legal_entities le ON le.id = cr.legal_entity_id
  LEFT JOIN operating_units ou ON ou.id = cr.operating_unit_id
  JOIN accounts a ON a.id = cr.account_id
  JOIN charts_of_accounts c ON c.id = a.coa_id
  LEFT JOIN accounts vg ON vg.id = cr.variance_gain_account_id
  LEFT JOIN accounts vl ON vl.id = cr.variance_loss_account_id
`;

const CASH_SESSION_BASE_SELECT = `
  SELECT
    cs.id,
    cs.tenant_id,
    cs.cash_register_id,
    cs.status,
    cs.opening_amount,
    cs.expected_closing_amount,
    cs.counted_closing_amount,
    cs.variance_amount,
    cs.opened_at,
    cs.opened_by_user_id,
    cs.closed_at,
    cs.closed_by_user_id,
    cs.closed_reason,
    cs.close_note,
    cs.approved_by_user_id,
    cs.approved_at,
    cs.created_at,
    cs.updated_at,
    cr.legal_entity_id,
    cr.operating_unit_id,
    cr.account_id AS register_account_id,
    cr.variance_gain_account_id,
    cr.variance_loss_account_id,
    cr.requires_approval_over_amount,
    cr.code AS cash_register_code,
    cr.name AS cash_register_name,
    cr.session_mode AS register_session_mode,
    cr.currency_code AS register_currency_code,
    cr.status AS register_status,
    le.code AS legal_entity_code,
    le.name AS legal_entity_name,
    openu.email AS opened_by_email,
    closeu.email AS closed_by_email,
    approveu.email AS approved_by_email
  FROM cash_sessions cs
  JOIN cash_registers cr ON cr.id = cs.cash_register_id
  JOIN legal_entities le ON le.id = cr.legal_entity_id
  LEFT JOIN users openu ON openu.id = cs.opened_by_user_id
  LEFT JOIN users closeu ON closeu.id = cs.closed_by_user_id
  LEFT JOIN users approveu ON approveu.id = cs.approved_by_user_id
`;

const CASH_TXN_BASE_SELECT = `
  SELECT
    ct.id,
    ct.tenant_id,
    ct.cash_register_id,
    ct.cash_session_id,
    ct.txn_no,
    ct.txn_type,
    ct.status,
    ct.txn_datetime,
    ct.book_date,
    ct.amount,
    ct.currency_code,
    ct.description,
    ct.reference_no,
    ct.source_doc_type,
    ct.source_doc_id,
    ct.source_module,
    ct.source_entity_type,
    ct.source_entity_id,
    ct.integration_link_status,
    ct.counterparty_type,
    ct.counterparty_id,
    ct.counter_account_id,
    ct.counter_cash_register_id,
    ct.linked_cari_settlement_batch_id,
    ct.linked_cari_unapplied_cash_id,
    ct.posted_journal_entry_id,
    ct.reversal_of_transaction_id,
    ct.cancel_reason,
    ct.override_cash_control,
    ct.override_reason,
    ct.idempotency_key,
    ct.integration_event_uid,
    ct.created_by_user_id,
    ct.submitted_by_user_id,
    ct.approved_by_user_id,
    ct.posted_by_user_id,
    ct.reversed_by_user_id,
    ct.cancelled_by_user_id,
    ct.submitted_at,
    ct.approved_at,
    ct.posted_at,
    ct.reversed_at,
    ct.cancelled_at,
    ct.created_at,
    ct.updated_at,
    cr.legal_entity_id,
    cr.operating_unit_id,
    cr.account_id AS register_account_id,
    cr.variance_gain_account_id AS register_variance_gain_account_id,
    cr.variance_loss_account_id AS register_variance_loss_account_id,
    le.code AS legal_entity_code,
    le.name AS legal_entity_name,
    cr.code AS cash_register_code,
    cr.name AS cash_register_name,
    cr.session_mode AS register_session_mode,
    cr.currency_code AS register_currency_code,
    cr.status AS register_status,
    s.status AS cash_session_status,
    ca.code AS counter_account_code,
    ca.name AS counter_account_name,
    ccr.id AS counter_cash_register_id_resolved,
    ccr.legal_entity_id AS counter_cash_register_legal_entity_id,
    ccr.operating_unit_id AS counter_cash_register_operating_unit_id,
    ccr.account_id AS counter_cash_register_account_id,
    ccr.currency_code AS counter_cash_register_currency_code,
    ccr.code AS counter_cash_register_code,
    ccr.name AS counter_cash_register_name,
    ctt.id AS cash_transit_transfer_id,
    ctt.status AS cash_transit_status,
    ctt.source_cash_register_id AS cash_transit_source_register_id,
    ctt.target_cash_register_id AS cash_transit_target_register_id,
    ctt.transfer_out_cash_transaction_id AS cash_transit_transfer_out_transaction_id,
    ctt.transfer_in_cash_transaction_id AS cash_transit_transfer_in_transaction_id,
    ctt.transit_account_id AS cash_transit_account_id,
    ctt.initiated_at AS cash_transit_initiated_at,
    ctt.in_transit_at AS cash_transit_in_transit_at,
    ctt.received_at AS cash_transit_received_at,
    ctt.canceled_at AS cash_transit_canceled_at,
    ctt.reversed_at AS cash_transit_reversed_at,
    ctt.cancel_reason AS cash_transit_cancel_reason,
    ctt.reverse_reason AS cash_transit_reverse_reason,
    ctt.note AS cash_transit_note
  FROM cash_transactions ct
  JOIN cash_registers cr ON cr.id = ct.cash_register_id
  JOIN legal_entities le ON le.id = cr.legal_entity_id
  LEFT JOIN cash_sessions s ON s.id = ct.cash_session_id
  LEFT JOIN accounts ca ON ca.id = ct.counter_account_id
  LEFT JOIN cash_registers ccr ON ccr.id = ct.counter_cash_register_id
  LEFT JOIN cash_transit_transfers ctt
    ON ctt.tenant_id = ct.tenant_id
   AND (
     ctt.transfer_out_cash_transaction_id = ct.id
     OR ctt.transfer_in_cash_transaction_id = ct.id
   )
`;

const CASH_TRANSIT_BASE_SELECT = `
  SELECT
    ctt.id,
    ctt.tenant_id,
    ctt.legal_entity_id,
    ctt.source_cash_register_id,
    ctt.target_cash_register_id,
    ctt.source_operating_unit_id,
    ctt.target_operating_unit_id,
    ctt.transfer_out_cash_transaction_id,
    ctt.transfer_in_cash_transaction_id,
    ctt.status,
    ctt.amount,
    ctt.currency_code,
    ctt.transit_account_id,
    ctt.initiated_by_user_id,
    ctt.received_by_user_id,
    ctt.canceled_by_user_id,
    ctt.reversed_by_user_id,
    ctt.initiated_at,
    ctt.in_transit_at,
    ctt.received_at,
    ctt.canceled_at,
    ctt.reversed_at,
    ctt.cancel_reason,
    ctt.reverse_reason,
    ctt.idempotency_key,
    ctt.integration_event_uid,
    ctt.source_module,
    ctt.source_entity_type,
    ctt.source_entity_id,
    ctt.note,
    ctt.created_at,
    ctt.updated_at,
    le.code AS legal_entity_code,
    le.name AS legal_entity_name,
    sr.code AS source_cash_register_code,
    sr.name AS source_cash_register_name,
    sou.code AS source_operating_unit_code,
    sou.name AS source_operating_unit_name,
    tr.code AS target_cash_register_code,
    tr.name AS target_cash_register_name,
    tou.code AS target_operating_unit_code,
    tou.name AS target_operating_unit_name,
    ta.code AS transit_account_code,
    ta.name AS transit_account_name,
    out_txn.txn_no AS transfer_out_txn_no,
    out_txn.book_date AS transfer_out_book_date,
    out_txn.posted_at AS transfer_out_posted_at,
    out_txn.status AS transfer_out_txn_status,
    out_txn.posted_journal_entry_id AS transfer_out_posted_journal_entry_id,
    in_txn.txn_no AS transfer_in_txn_no,
    in_txn.book_date AS transfer_in_book_date,
    in_txn.posted_at AS transfer_in_posted_at,
    in_txn.status AS transfer_in_txn_status,
    in_txn.posted_journal_entry_id AS transfer_in_posted_journal_entry_id
  FROM cash_transit_transfers ctt
  JOIN legal_entities le ON le.id = ctt.legal_entity_id
  JOIN cash_registers sr
    ON sr.id = ctt.source_cash_register_id
   AND sr.tenant_id = ctt.tenant_id
  LEFT JOIN operating_units sou ON sou.id = ctt.source_operating_unit_id
  JOIN cash_registers tr
    ON tr.id = ctt.target_cash_register_id
   AND tr.tenant_id = ctt.tenant_id
  LEFT JOIN operating_units tou ON tou.id = ctt.target_operating_unit_id
  LEFT JOIN accounts ta ON ta.id = ctt.transit_account_id
  LEFT JOIN cash_transactions out_txn
    ON out_txn.id = ctt.transfer_out_cash_transaction_id
   AND out_txn.tenant_id = ctt.tenant_id
  LEFT JOIN cash_transactions in_txn
    ON in_txn.id = ctt.transfer_in_cash_transaction_id
   AND in_txn.tenant_id = ctt.tenant_id
`;

function asRow(result) {
  return result.rows?.[0] || null;
}

function asCount(result, field = "total") {
  return Number(result.rows?.[0]?.[field] || 0);
}

function normalizeTxnNoLegalEntitySegment(legalEntityCode, legalEntityId) {
  const sanitized = String(legalEntityCode || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
  if (sanitized) {
    return sanitized.slice(0, 16);
  }
  return `LE${Number(legalEntityId)}`;
}

function parseTxnNoSequence(txnNo, prefix) {
  const normalized = String(txnNo || "");
  if (!normalized.startsWith(prefix)) {
    return 0;
  }
  const suffix = normalized.slice(prefix.length);
  if (!/^\d{6}$/.test(suffix)) {
    return 0;
  }
  return Number(suffix);
}

export async function findCashRegisterById({ tenantId, registerId, runQuery = query }) {
  const result = await runQuery(
    `${CASH_REGISTER_BASE_SELECT}
     WHERE cr.tenant_id = ?
       AND cr.id = ?
     LIMIT 1`,
    [tenantId, registerId]
  );
  return asRow(result);
}

export async function findCashRegisterByCode({ tenantId, code, runQuery = query }) {
  const result = await runQuery(
    `${CASH_REGISTER_BASE_SELECT}
     WHERE cr.tenant_id = ?
       AND cr.code = ?
     LIMIT 1`,
    [tenantId, code]
  );
  return asRow(result);
}

export async function countCashRegisters({ whereSql, params, runQuery = query }) {
  const result = await runQuery(
    `SELECT COUNT(*) AS total
     FROM cash_registers cr
     WHERE ${whereSql}`,
    params
  );
  return asCount(result);
}

export async function listCashRegisters({ whereSql, params, limit, offset, runQuery = query }) {
  const result = await runQuery(
    `${CASH_REGISTER_BASE_SELECT}
     WHERE ${whereSql}
     ORDER BY cr.id DESC
     LIMIT ${limit}
     OFFSET ${offset}`,
    params
  );
  return result.rows || [];
}

export async function insertCashRegister({ payload, runQuery = query }) {
  const result = await runQuery(
    `INSERT INTO cash_registers (
       tenant_id,
       legal_entity_id,
       operating_unit_id,
       account_id,
       code,
       name,
       register_type,
       session_mode,
       currency_code,
       status,
       allow_negative,
       variance_gain_account_id,
       variance_loss_account_id,
       max_txn_amount,
       requires_approval_over_amount,
       created_by_user_id
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      payload.tenantId,
      payload.legalEntityId,
      payload.operatingUnitId,
      payload.accountId,
      payload.code,
      payload.name,
      payload.registerType,
      payload.sessionMode,
      payload.currencyCode,
      payload.status,
      payload.allowNegative,
      payload.varianceGainAccountId,
      payload.varianceLossAccountId,
      payload.maxTxnAmount,
      payload.requiresApprovalOverAmount,
      payload.userId,
    ]
  );
  return Number(result.rows?.insertId || 0);
}

export async function updateCashRegister({ id, payload, runQuery = query }) {
  await runQuery(
    `UPDATE cash_registers
     SET
       legal_entity_id = ?,
       operating_unit_id = ?,
       account_id = ?,
       code = ?,
       name = ?,
       register_type = ?,
       session_mode = ?,
       currency_code = ?,
       status = ?,
       allow_negative = ?,
       variance_gain_account_id = ?,
       variance_loss_account_id = ?,
       max_txn_amount = ?,
       requires_approval_over_amount = ?
     WHERE tenant_id = ?
       AND id = ?`,
    [
      payload.legalEntityId,
      payload.operatingUnitId,
      payload.accountId,
      payload.code,
      payload.name,
      payload.registerType,
      payload.sessionMode,
      payload.currencyCode,
      payload.status,
      payload.allowNegative,
      payload.varianceGainAccountId,
      payload.varianceLossAccountId,
      payload.maxTxnAmount,
      payload.requiresApprovalOverAmount,
      payload.tenantId,
      id,
    ]
  );
}

export async function updateCashRegisterStatus({
  tenantId,
  registerId,
  status,
  runQuery = query,
}) {
  await runQuery(
    `UPDATE cash_registers
     SET status = ?
     WHERE tenant_id = ?
       AND id = ?`,
    [status, tenantId, registerId]
  );
}

export async function markAccountAsCashControlled({ accountId, runQuery = query }) {
  await runQuery(
    `UPDATE accounts
     SET is_cash_controlled = TRUE
     WHERE id = ?`,
    [accountId]
  );
}

export async function findAccountForCashRules({ tenantId, accountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
       a.id,
       a.code,
       a.name,
       a.allow_posting,
       a.parent_account_id,
       a.is_active,
       a.is_cash_controlled,
       c.scope,
       c.legal_entity_id
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE a.id = ?
       AND c.tenant_id = ?
     LIMIT 1`,
    [accountId, tenantId]
  );
  return asRow(result);
}

export async function countChildAccounts({ accountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT COUNT(*) AS total
     FROM accounts
     WHERE parent_account_id = ?`,
    [accountId]
  );
  return asCount(result);
}

export async function findCashRegisterScopeById({ tenantId, registerId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, legal_entity_id
     FROM cash_registers
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, registerId]
  );
  return asRow(result);
}

export async function generateCashTxnNoForLegalEntityYearTx({
  tenantId,
  legalEntityId,
  legalEntityCode,
  bookDate,
  runQuery = query,
}) {
  const year = Number(String(bookDate || "").slice(0, 4));
  if (!Number.isInteger(year) || year < 1900 || year > 9999) {
    throw new Error("Invalid bookDate year for cash txn_no sequence");
  }

  const tenant = Number(tenantId);
  const entity = Number(legalEntityId);
  if (!tenant || !entity) {
    throw new Error("tenantId and legalEntityId are required for cash txn_no sequence");
  }

  await runQuery(
    `SELECT id
     FROM cash_registers
     WHERE tenant_id = ?
       AND legal_entity_id = ?
     ORDER BY id
     LIMIT 1
     FOR UPDATE`,
    [tenant, entity]
  );

  const entitySegment = normalizeTxnNoLegalEntitySegment(legalEntityCode, entity);
  const prefix = `CASH-${entitySegment}-${year}-`;

  const result = await runQuery(
    `SELECT ct.txn_no
     FROM cash_transactions ct
     JOIN cash_registers cr ON cr.id = ct.cash_register_id
     WHERE ct.tenant_id = ?
       AND cr.legal_entity_id = ?
       AND ct.txn_no LIKE ?
     ORDER BY ct.id DESC
     LIMIT 1
     FOR UPDATE`,
    [tenant, entity, `${prefix}%`]
  );

  const lastTxnNo = String(result.rows?.[0]?.txn_no || "");
  const lastSeq = parseTxnNoSequence(lastTxnNo, prefix);
  const nextSeq = lastSeq + 1;

  return `${prefix}${String(nextSeq).padStart(6, "0")}`;
}

export async function findCashSessionById({
  tenantId,
  sessionId,
  runQuery = query,
  forUpdate = false,
}) {
  const result = await runQuery(
    `${CASH_SESSION_BASE_SELECT}
     WHERE cs.tenant_id = ?
       AND cs.id = ?
     LIMIT 1
     ${forUpdate ? "FOR UPDATE" : ""}`,
    [tenantId, sessionId]
  );
  return asRow(result);
}

export async function findCashSessionScopeById({ tenantId, sessionId, runQuery = query }) {
  const result = await runQuery(
    `SELECT cs.id, cr.legal_entity_id
     FROM cash_sessions cs
     JOIN cash_registers cr ON cr.id = cs.cash_register_id
     WHERE cs.tenant_id = ?
       AND cs.id = ?
     LIMIT 1`,
    [tenantId, sessionId]
  );
  return asRow(result);
}

export async function findOpenCashSessionByRegisterId({
  tenantId,
  registerId,
  runQuery = query,
  forUpdate = false,
}) {
  const result = await runQuery(
    `${CASH_SESSION_BASE_SELECT}
     WHERE cs.tenant_id = ?
       AND cs.cash_register_id = ?
       AND cs.status = 'OPEN'
     LIMIT 1
     ${forUpdate ? "FOR UPDATE" : ""}`,
    [tenantId, registerId]
  );
  return asRow(result);
}

export async function countCashSessions({ whereSql, params, runQuery = query }) {
  const result = await runQuery(
    `SELECT COUNT(*) AS total
     FROM cash_sessions cs
     JOIN cash_registers cr ON cr.id = cs.cash_register_id
     WHERE ${whereSql}`,
    params
  );
  return asCount(result);
}

export async function listCashSessions({ whereSql, params, limit, offset, runQuery = query }) {
  const result = await runQuery(
    `${CASH_SESSION_BASE_SELECT}
     WHERE ${whereSql}
     ORDER BY cs.id DESC
     LIMIT ${limit}
     OFFSET ${offset}`,
    params
  );
  return result.rows || [];
}

export async function insertCashSession({ payload, runQuery = query }) {
  const result = await runQuery(
    `INSERT INTO cash_sessions (
       tenant_id,
       cash_register_id,
       status,
       opening_amount,
       opened_by_user_id
     ) VALUES (?, ?, 'OPEN', ?, ?)`,
    [payload.tenantId, payload.registerId, payload.openingAmount, payload.userId]
  );
  return Number(result.rows?.insertId || 0);
}

export async function closeCashSession({
  sessionId,
  payload,
  runQuery = query,
}) {
  await runQuery(
    `UPDATE cash_sessions
     SET
       status = 'CLOSED',
       expected_closing_amount = ?,
       counted_closing_amount = ?,
       variance_amount = ?,
       closed_at = UTC_TIMESTAMP(),
       closed_by_user_id = ?,
       approved_by_user_id = ?,
       approved_at = ?,
       closed_reason = ?,
       close_note = ?
     WHERE id = ?
       AND tenant_id = ?
       AND status = 'OPEN'`,
    [
      payload.expectedClosingAmount,
      payload.countedClosingAmount,
      payload.varianceAmount,
      payload.userId,
      payload.approvedByUserId || null,
      payload.approvedAt || null,
      payload.closedReason,
      payload.closeNote,
      sessionId,
      payload.tenantId,
    ]
  );
}

export async function sumPostedSessionMovement({
  tenantId,
  registerId,
  sessionId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
       COALESCE(
         SUM(
           CASE
             WHEN txn_type IN ('RECEIPT', 'WITHDRAWAL_FROM_BANK', 'TRANSFER_IN', 'OPENING_FLOAT')
               THEN amount
             WHEN txn_type IN ('PAYOUT', 'DEPOSIT_TO_BANK', 'TRANSFER_OUT')
               THEN -amount
             ELSE 0
           END
         ),
         0
       ) AS net_amount
     FROM cash_transactions
     WHERE tenant_id = ?
       AND cash_register_id = ?
       AND cash_session_id = ?
       AND status = 'POSTED'`,
    [tenantId, registerId, sessionId]
  );
  return Number(result.rows?.[0]?.net_amount || 0);
}

export async function countOpenUnpostedSessionTransactions({
  tenantId,
  sessionId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT COUNT(*) AS total
     FROM cash_transactions
     WHERE tenant_id = ?
       AND cash_session_id = ?
       AND status IN ('DRAFT', 'SUBMITTED', 'APPROVED')`,
    [tenantId, sessionId]
  );
  return asCount(result);
}

export async function findCashTransactionById({
  tenantId,
  transactionId,
  runQuery = query,
  forUpdate = false,
}) {
  const result = await runQuery(
    `${CASH_TXN_BASE_SELECT}
     WHERE ct.tenant_id = ?
       AND ct.id = ?
     LIMIT 1
     ${forUpdate ? "FOR UPDATE" : ""}`,
    [tenantId, transactionId]
  );
  return asRow(result);
}

export async function findCashTransactionScopeById({ tenantId, transactionId, runQuery = query }) {
  const result = await runQuery(
    `SELECT ct.id, cr.legal_entity_id
     FROM cash_transactions ct
     JOIN cash_registers cr ON cr.id = ct.cash_register_id
     WHERE ct.tenant_id = ?
       AND ct.id = ?
     LIMIT 1`,
    [tenantId, transactionId]
  );
  return asRow(result);
}

export async function findCashTransactionByIdempotency({
  tenantId,
  registerId,
  idempotencyKey,
  runQuery = query,
}) {
  const result = await runQuery(
    `${CASH_TXN_BASE_SELECT}
     WHERE ct.tenant_id = ?
       AND ct.cash_register_id = ?
       AND ct.idempotency_key = ?
     LIMIT 1`,
    [tenantId, registerId, idempotencyKey]
  );
  return asRow(result);
}

export async function findCashTransactionByIntegrationEventUid({
  tenantId,
  integrationEventUid,
  runQuery = query,
}) {
  const result = await runQuery(
    `${CASH_TXN_BASE_SELECT}
     WHERE ct.tenant_id = ?
       AND ct.integration_event_uid = ?
     LIMIT 1`,
    [tenantId, integrationEventUid]
  );
  return asRow(result);
}

export async function findCashTransactionByReversalOf({
  tenantId,
  transactionId,
  runQuery = query,
}) {
  const result = await runQuery(
    `${CASH_TXN_BASE_SELECT}
     WHERE ct.tenant_id = ?
       AND ct.reversal_of_transaction_id = ?
     LIMIT 1`,
    [tenantId, transactionId]
  );
  return asRow(result);
}

export async function findCashTransitTransferScopeById({
  tenantId,
  transitTransferId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT id, legal_entity_id
     FROM cash_transit_transfers
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, transitTransferId]
  );
  return asRow(result);
}

export async function findCashTransitTransferById({
  tenantId,
  transitTransferId,
  runQuery = query,
  forUpdate = false,
}) {
  const result = await runQuery(
    `${CASH_TRANSIT_BASE_SELECT}
     WHERE ctt.tenant_id = ?
       AND ctt.id = ?
     LIMIT 1
     ${forUpdate ? "FOR UPDATE" : ""}`,
    [tenantId, transitTransferId]
  );
  return asRow(result);
}

export async function findCashTransitTransferByIdempotency({
  tenantId,
  sourceRegisterId,
  idempotencyKey,
  runQuery = query,
  forUpdate = false,
}) {
  const result = await runQuery(
    `${CASH_TRANSIT_BASE_SELECT}
     WHERE ctt.tenant_id = ?
       AND ctt.source_cash_register_id = ?
       AND ctt.idempotency_key = ?
     LIMIT 1
     ${forUpdate ? "FOR UPDATE" : ""}`,
    [tenantId, sourceRegisterId, idempotencyKey]
  );
  return asRow(result);
}

export async function findCashTransitTransferByIntegrationEventUid({
  tenantId,
  integrationEventUid,
  runQuery = query,
  forUpdate = false,
}) {
  const result = await runQuery(
    `${CASH_TRANSIT_BASE_SELECT}
     WHERE ctt.tenant_id = ?
       AND ctt.integration_event_uid = ?
     LIMIT 1
     ${forUpdate ? "FOR UPDATE" : ""}`,
    [tenantId, integrationEventUid]
  );
  return asRow(result);
}

export async function findCashTransitTransferByOutTransactionId({
  tenantId,
  transactionId,
  runQuery = query,
  forUpdate = false,
}) {
  const result = await runQuery(
    `${CASH_TRANSIT_BASE_SELECT}
     WHERE ctt.tenant_id = ?
       AND ctt.transfer_out_cash_transaction_id = ?
     LIMIT 1
     ${forUpdate ? "FOR UPDATE" : ""}`,
    [tenantId, transactionId]
  );
  return asRow(result);
}

export async function findCashTransitTransferByInTransactionId({
  tenantId,
  transactionId,
  runQuery = query,
  forUpdate = false,
}) {
  const result = await runQuery(
    `${CASH_TRANSIT_BASE_SELECT}
     WHERE ctt.tenant_id = ?
       AND ctt.transfer_in_cash_transaction_id = ?
     LIMIT 1
     ${forUpdate ? "FOR UPDATE" : ""}`,
    [tenantId, transactionId]
  );
  return asRow(result);
}

export async function insertCashTransitTransfer({ payload, runQuery = query }) {
  const result = await runQuery(
    `INSERT INTO cash_transit_transfers (
       tenant_id,
       legal_entity_id,
       source_cash_register_id,
       target_cash_register_id,
       source_operating_unit_id,
       target_operating_unit_id,
       transfer_out_cash_transaction_id,
       transfer_in_cash_transaction_id,
       status,
       amount,
       currency_code,
       transit_account_id,
       initiated_by_user_id,
       idempotency_key,
       integration_event_uid,
       source_module,
       source_entity_type,
       source_entity_id,
       note
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      payload.tenantId,
      payload.legalEntityId,
      payload.sourceCashRegisterId,
      payload.targetCashRegisterId,
      payload.sourceOperatingUnitId,
      payload.targetOperatingUnitId,
      payload.transferOutCashTransactionId,
      payload.transferInCashTransactionId,
      payload.status,
      payload.amount,
      payload.currencyCode,
      payload.transitAccountId,
      payload.initiatedByUserId,
      payload.idempotencyKey,
      payload.integrationEventUid,
      payload.sourceModule,
      payload.sourceEntityType,
      payload.sourceEntityId,
      payload.note,
    ]
  );
  return Number(result.rows?.insertId || 0);
}

export async function markCashTransitTransferInTransit({
  tenantId,
  transitTransferId,
  runQuery = query,
}) {
  const result = await runQuery(
    `UPDATE cash_transit_transfers
     SET
       status = 'IN_TRANSIT',
       in_transit_at = COALESCE(in_transit_at, UTC_TIMESTAMP())
     WHERE tenant_id = ?
       AND id = ?
       AND status = 'INITIATED'`,
    [tenantId, transitTransferId]
  );
  return Number(result.rows?.affectedRows || 0);
}

export async function markCashTransitTransferReceived({
  tenantId,
  transitTransferId,
  transferInCashTransactionId,
  receivedByUserId,
  runQuery = query,
}) {
  const result = await runQuery(
    `UPDATE cash_transit_transfers
     SET
       transfer_in_cash_transaction_id = ?,
       status = 'RECEIVED',
       received_by_user_id = ?,
       received_at = COALESCE(received_at, UTC_TIMESTAMP())
     WHERE tenant_id = ?
       AND id = ?
       AND status = 'IN_TRANSIT'
       AND transfer_in_cash_transaction_id IS NULL`,
    [transferInCashTransactionId, receivedByUserId, tenantId, transitTransferId]
  );
  return Number(result.rows?.affectedRows || 0);
}

export async function markCashTransitTransferCanceled({
  tenantId,
  transitTransferId,
  canceledByUserId,
  cancelReason,
  runQuery = query,
}) {
  const result = await runQuery(
    `UPDATE cash_transit_transfers
     SET
       status = 'CANCELED',
       canceled_by_user_id = ?,
       canceled_at = UTC_TIMESTAMP(),
       cancel_reason = ?
     WHERE tenant_id = ?
       AND id = ?
       AND status = 'INITIATED'`,
    [canceledByUserId, cancelReason, tenantId, transitTransferId]
  );
  return Number(result.rows?.affectedRows || 0);
}

export async function markCashTransitTransferReversed({
  tenantId,
  transitTransferId,
  reversedByUserId,
  reverseReason,
  runQuery = query,
}) {
  const result = await runQuery(
    `UPDATE cash_transit_transfers
     SET
       status = 'REVERSED',
       reversed_by_user_id = ?,
       reversed_at = UTC_TIMESTAMP(),
       reverse_reason = ?
     WHERE tenant_id = ?
       AND id = ?
       AND status IN ('IN_TRANSIT', 'RECEIVED')`,
    [reversedByUserId, reverseReason, tenantId, transitTransferId]
  );
  return Number(result.rows?.affectedRows || 0);
}

export async function countCashTransactions({ whereSql, params, runQuery = query }) {
  const result = await runQuery(
    `SELECT COUNT(*) AS total
     FROM cash_transactions ct
     JOIN cash_registers cr ON cr.id = ct.cash_register_id
     WHERE ${whereSql}`,
    params
  );
  return asCount(result);
}

export async function listCashTransactions({
  whereSql,
  params,
  limit,
  offset,
  runQuery = query,
}) {
  const result = await runQuery(
    `${CASH_TXN_BASE_SELECT}
     WHERE ${whereSql}
     ORDER BY ct.id DESC
     LIMIT ${limit}
     OFFSET ${offset}`,
    params
  );
  return result.rows || [];
}

export async function countCashTransitTransfers({ whereSql, params, runQuery = query }) {
  const result = await runQuery(
    `SELECT COUNT(*) AS total
     FROM cash_transit_transfers ctt
     WHERE ${whereSql}`,
    params
  );
  return asCount(result);
}

export async function listCashTransitTransfers({
  whereSql,
  params,
  limit,
  offset,
  runQuery = query,
}) {
  const result = await runQuery(
    `${CASH_TRANSIT_BASE_SELECT}
     WHERE ${whereSql}
     ORDER BY ctt.id DESC
     LIMIT ${limit}
     OFFSET ${offset}`,
    params
  );
  return result.rows || [];
}

export async function insertCashTransaction({ payload, runQuery = query }) {
  const result = await runQuery(
    `INSERT INTO cash_transactions (
       tenant_id,
       cash_register_id,
       cash_session_id,
       txn_no,
       txn_type,
       status,
       txn_datetime,
       book_date,
       amount,
       currency_code,
       description,
        reference_no,
        source_doc_type,
        source_doc_id,
        source_module,
        source_entity_type,
        source_entity_id,
        integration_link_status,
        counterparty_type,
        counterparty_id,
        counter_account_id,
        counter_cash_register_id,
        linked_cari_settlement_batch_id,
        linked_cari_unapplied_cash_id,
        reversal_of_transaction_id,
        override_cash_control,
        override_reason,
        idempotency_key,
        integration_event_uid,
        created_by_user_id,
        posted_by_user_id,
        posted_at
      ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
      )`,
    [
      payload.tenantId,
      payload.registerId,
      payload.cashSessionId,
      payload.txnNo,
      payload.txnType,
      payload.status,
      payload.txnDatetime,
      payload.bookDate,
      payload.amount,
      payload.currencyCode,
      payload.description,
      payload.referenceNo,
      payload.sourceDocType,
      payload.sourceDocId,
      payload.sourceModule,
      payload.sourceEntityType,
      payload.sourceEntityId,
      payload.integrationLinkStatus,
      payload.counterpartyType,
      payload.counterpartyId,
      payload.counterAccountId,
      payload.counterCashRegisterId,
      payload.linkedCariSettlementBatchId,
      payload.linkedCariUnappliedCashId,
      payload.reversalOfTransactionId,
      payload.overrideCashControl,
      payload.overrideReason,
      payload.idempotencyKey,
      payload.integrationEventUid,
      payload.userId,
      payload.postedByUserId,
      payload.postedAt,
    ]
  );
  return Number(result.rows?.insertId || 0);
}

export async function cancelCashTransaction({
  tenantId,
  transactionId,
  userId,
  cancelReason,
  runQuery = query,
}) {
  await runQuery(
    `UPDATE cash_transactions
     SET
       status = 'CANCELLED',
       cancel_reason = ?,
       cancelled_by_user_id = ?,
       cancelled_at = UTC_TIMESTAMP()
     WHERE tenant_id = ?
       AND id = ?`,
    [cancelReason, userId, tenantId, transactionId]
  );
}

export async function postCashTransaction({
  tenantId,
  transactionId,
  userId,
  postedJournalEntryId,
  overrideCashControl,
  overrideReason,
  runQuery = query,
}) {
  await runQuery(
    `UPDATE cash_transactions
     SET
       status = 'POSTED',
       posted_by_user_id = ?,
       posted_at = UTC_TIMESTAMP(),
       posted_journal_entry_id = ?,
       override_cash_control = ?,
       override_reason = ?
     WHERE tenant_id = ?
       AND id = ?`,
    [
      userId,
      postedJournalEntryId,
      overrideCashControl,
      overrideReason,
      tenantId,
      transactionId,
    ]
  );
}

export async function markCashTransactionAsReversed({
  tenantId,
  transactionId,
  userId,
  runQuery = query,
}) {
  await runQuery(
    `UPDATE cash_transactions
     SET
       status = 'REVERSED',
       reversed_by_user_id = ?,
       reversed_at = UTC_TIMESTAMP()
     WHERE tenant_id = ?
       AND id = ?`,
    [userId, tenantId, transactionId]
  );
}
