import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { matchReconciliationLine } from "./bank.reconciliation.service.js";
import { evaluateBankApprovalNeed } from "./bank.governance.service.js";
import { submitBankApprovalRequest } from "./bank.approvals.service.js";

const AMOUNT_EPSILON = 0.005;

function u(value) {
  return String(value || "").trim().toUpperCase();
}

function toAmount(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Number(n.toFixed(6));
}

function absAmount(value) {
  return Math.abs(toAmount(value));
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

async function getPaymentBatchLineForReturn({ tenantId, paymentBatchLineId, runQuery = query }) {
  const res = await runQuery(
    `SELECT
        l.id,
        l.tenant_id,
        l.legal_entity_id,
        l.batch_id,
        l.line_no,
        l.amount,
        b.currency_code AS currency_code,
        l.status,
        l.bank_execution_status,
        l.executed_amount,
        l.exported_amount,
        l.return_status,
        l.returned_amount,
        l.return_reason_code,
        l.bank_reference,
        b.status AS batch_status,
        b.batch_no,
        b.bank_account_id
     FROM payment_batch_lines l
     JOIN payment_batches b
       ON b.tenant_id = l.tenant_id
      AND b.legal_entity_id = l.legal_entity_id
      AND b.id = l.batch_id
     WHERE l.tenant_id = ?
       AND l.id = ?
     LIMIT 1`,
    [tenantId, paymentBatchLineId]
  );
  return res.rows?.[0] || null;
}

async function getStatementLineForReturn({ tenantId, statementLineId, runQuery = query }) {
  if (!parsePositiveInt(statementLineId)) return null;
  const res = await runQuery(
    `SELECT id, tenant_id, legal_entity_id, bank_account_id, txn_date, description, reference_no, amount, currency_code, recon_status
     FROM bank_statement_lines
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, statementLineId]
  );
  return res.rows?.[0] || null;
}

async function getAckImportLineForReturn({ tenantId, ackImportLineId, runQuery = query }) {
  if (!parsePositiveInt(ackImportLineId)) return null;
  const res = await runQuery(
    `SELECT *
     FROM payment_batch_ack_import_lines
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, ackImportLineId]
  );
  return res.rows?.[0] || null;
}

async function updatePaymentLineReturnEffectsTx({
  tenantId,
  legalEntityId,
  paymentBatchLineId,
  event,
  runQuery,
}) {
  const line = await getPaymentBatchLineForReturn({
    tenantId,
    paymentBatchLineId,
    runQuery,
  });
  if (!line) throw badRequest("Payment batch line not found");
  if (parsePositiveInt(line.legal_entity_id) !== parsePositiveInt(legalEntityId)) {
    throw badRequest("Payment batch line legal entity mismatch");
  }

  const lineAmountAbs = absAmount(line.amount);
  const currentReturned = absAmount(line.returned_amount);
  const eventAmount = absAmount(event.amount);

  let nextReturned = currentReturned;
  let nextReturnStatus = line.return_status || null;
  let nextBankExecStatus = line.bank_execution_status || null;
  let nextLineStatus = line.status || "PENDING";

  if (u(event.event_type) === "PAYMENT_REJECTED") {
    nextReturnStatus = "REJECTED_POST_ACK";
    nextBankExecStatus = "REJECTED";
    if (u(line.status) === "PENDING") {
      nextLineStatus = "FAILED";
    }
  } else {
    if (currentReturned + eventAmount > lineAmountAbs + AMOUNT_EPSILON) {
      throw badRequest("Return event amount exceeds payment line amount");
    }
    nextReturned = Number(Math.min(lineAmountAbs, currentReturned + eventAmount).toFixed(6));
    nextReturnStatus =
      Math.abs(nextReturned - lineAmountAbs) <= AMOUNT_EPSILON ? "RETURNED" : "PARTIALLY_RETURNED";
    nextBankExecStatus = nextReturnStatus;
    // Keep B04 enum status unchanged; bank-side return evidence is tracked via return_status/bank_execution_status.
    nextLineStatus = line.status;
  }

  await runQuery(
    `UPDATE payment_batch_lines
     SET returned_amount = ?,
         return_status = ?,
         return_reason_code = COALESCE(?, return_reason_code),
         bank_execution_status = COALESCE(?, bank_execution_status),
         status = ?,
         last_returned_at = CURRENT_TIMESTAMP
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?`,
    [
      nextReturned,
      nextReturnStatus,
      event.reason_code || null,
      nextBankExecStatus || null,
      nextLineStatus,
      tenantId,
      legalEntityId,
      paymentBatchLineId,
    ]
  );

  await runQuery(
    `INSERT INTO payment_batch_audit (
        tenant_id, legal_entity_id, batch_id, action, payload_json, acted_by_user_id
      ) VALUES (?, ?, ?, 'STATUS', ?, ?)`,
    [
      tenantId,
      legalEntityId,
      line.batch_id,
      safeJson({
        event: "BANK_RETURN_EVENT_APPLIED_B08B",
        payment_batch_line_id: paymentBatchLineId,
        event_type: event.event_type,
        event_status: event.event_status,
        amount: toAmount(event.amount),
        currency_code: event.currency_code,
        return_status_after: nextReturnStatus,
        returned_amount_after: nextReturned,
        bank_execution_status_after: nextBankExecStatus,
      }),
      event.created_by_user_id || null,
    ]
  );

  return {
    ...line,
    returned_amount: nextReturned,
    return_status: nextReturnStatus,
    bank_execution_status: nextBankExecStatus,
    status: nextLineStatus,
  };
}

async function getExistingEventByRequestId({ tenantId, legalEntityId, eventRequestId, runQuery = query }) {
  if (!eventRequestId) return null;
  const res = await runQuery(
    `SELECT *
     FROM bank_payment_return_events
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND event_request_id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, eventRequestId]
  );
  return res.rows?.[0] || null;
}

async function createPaymentReturnEventTx({
  tenantId,
  input,
  userId,
  applyEffects = true,
  runQuery = query,
}) {
  const paymentLine = await getPaymentBatchLineForReturn({
    tenantId,
    paymentBatchLineId: input.paymentBatchLineId,
    runQuery,
  });
  if (!paymentLine) throw badRequest("paymentBatchLineId not found");
  const legalEntityId = parsePositiveInt(paymentLine.legal_entity_id);

  const existing = await getExistingEventByRequestId({
    tenantId,
    legalEntityId,
    eventRequestId: input.eventRequestId || null,
    runQuery,
  });
  if (existing) {
    return { row: existing, idempotent: true, legalEntityId, paymentLine };
  }

  if (u(input.currencyCode) !== u(paymentLine.currency_code)) {
    throw badRequest("currencyCode does not match payment batch line currency");
  }

  const statementLine = parsePositiveInt(input.bankStatementLineId)
    ? await getStatementLineForReturn({
        tenantId,
        statementLineId: input.bankStatementLineId,
        runQuery,
      })
    : null;
  if (input.bankStatementLineId && !statementLine) {
    throw badRequest("bankStatementLineId not found");
  }
  if (statementLine && parsePositiveInt(statementLine.legal_entity_id) !== legalEntityId) {
    throw badRequest("bankStatementLineId legal entity mismatch");
  }

  const ackImportLine = parsePositiveInt(input.paymentBatchAckImportLineId)
    ? await getAckImportLineForReturn({
        tenantId,
        ackImportLineId: input.paymentBatchAckImportLineId,
        runQuery,
      })
    : null;
  if (input.paymentBatchAckImportLineId && !ackImportLine) {
    throw badRequest("paymentBatchAckImportLineId not found");
  }

  const ins = await runQuery(
    `INSERT INTO bank_payment_return_events (
        tenant_id, legal_entity_id,
        event_request_id, source_type, source_ref,
        payment_batch_id, payment_batch_line_id,
        bank_statement_line_id, payment_batch_ack_import_id, payment_batch_ack_import_line_id,
        event_type, event_status, amount, currency_code,
        bank_reference, reason_code, reason_message, payload_json,
        created_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      tenantId,
      legalEntityId,
      input.eventRequestId || null,
      u(input.sourceType || "MANUAL"),
      input.sourceRef || null,
      paymentLine.batch_id,
      paymentLine.id,
      parsePositiveInt(input.bankStatementLineId) || null,
      parsePositiveInt(input.paymentBatchAckImportId) || null,
      parsePositiveInt(input.paymentBatchAckImportLineId) || null,
      u(input.eventType || "PAYMENT_RETURNED"),
      u(input.eventStatus || "CONFIRMED"),
      absAmount(input.amount),
      u(input.currencyCode),
      input.bankReference || null,
      input.reasonCode || null,
      input.reasonMessage || null,
      safeJson(input.payload || null),
      userId || null,
    ]
  );

  const eventId = parsePositiveInt(ins.rows?.insertId);
  const rowRes = await runQuery(
    `SELECT * FROM bank_payment_return_events WHERE tenant_id = ? AND legal_entity_id = ? AND id = ? LIMIT 1`,
    [tenantId, legalEntityId, eventId]
  );
  const eventRow = rowRes.rows?.[0] || null;
  if (!eventRow) throw new Error("Failed to load created payment return event");

  let paymentLineAfter = paymentLine;
  if (applyEffects && u(eventRow.event_status) !== "IGNORED") {
    paymentLineAfter = await updatePaymentLineReturnEffectsTx({
      tenantId,
      legalEntityId,
      paymentBatchLineId: paymentLine.id,
      event: eventRow,
      runQuery,
    });
  }

  return {
    row: eventRow,
    idempotent: false,
    legalEntityId,
    paymentLine: paymentLineAfter,
  };
}

export async function resolvePaymentReturnEventScope(eventId, tenantId) {
  const parsedEventId = parsePositiveInt(eventId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedEventId || !parsedTenantId) return null;
  const res = await query(
    `SELECT legal_entity_id
     FROM bank_payment_return_events
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [parsedTenantId, parsedEventId]
  );
  const row = res.rows?.[0] || null;
  if (!row) return null;
  return { scopeType: "LEGAL_ENTITY", scopeId: parsePositiveInt(row.legal_entity_id) };
}

export async function resolvePaymentBatchLineReturnScope(paymentBatchLineId, tenantId) {
  const line = await getPaymentBatchLineForReturn({
    tenantId: parsePositiveInt(tenantId),
    paymentBatchLineId: parsePositiveInt(paymentBatchLineId),
  });
  if (!line) return null;
  return { scopeType: "LEGAL_ENTITY", scopeId: parsePositiveInt(line.legal_entity_id) };
}

export async function listPaymentReturnEventRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const where = ["e.tenant_id = ?"];
  where.push(buildScopeFilter(req, "legal_entity", "e.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    where.push("e.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }
  if (filters.bankAccountId) {
    where.push("pb.bank_account_id = ?");
    params.push(filters.bankAccountId);
  }
  if (filters.paymentBatchId) {
    where.push("e.payment_batch_id = ?");
    params.push(filters.paymentBatchId);
  }
  if (filters.paymentBatchLineId) {
    where.push("e.payment_batch_line_id = ?");
    params.push(filters.paymentBatchLineId);
  }
  if (filters.bankStatementLineId) {
    where.push("e.bank_statement_line_id = ?");
    params.push(filters.bankStatementLineId);
  }
  if (filters.eventType) {
    where.push("e.event_type = ?");
    params.push(filters.eventType);
  }
  if (filters.eventStatus) {
    where.push("e.event_status = ?");
    params.push(filters.eventStatus);
  }

  const whereSql = where.join(" AND ");
  const countRes = await query(
    `SELECT COUNT(*) AS total
     FROM bank_payment_return_events e
     JOIN payment_batches pb
       ON pb.tenant_id = e.tenant_id
      AND pb.legal_entity_id = e.legal_entity_id
      AND pb.id = e.payment_batch_id
     WHERE ${whereSql}`,
    params
  );
  const total = Number(countRes.rows?.[0]?.total || 0);

  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset = Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const res = await query(
    `SELECT
        e.*,
        pb.batch_no,
        pb.bank_account_id,
        pbl.line_no AS payment_batch_line_no,
        pbl.amount AS payment_line_amount,
        pbl.bank_execution_status,
        pbl.return_status,
        pbl.returned_amount,
        sl.txn_date AS statement_txn_date,
        sl.amount AS statement_amount,
        sl.description AS statement_description
     FROM bank_payment_return_events e
     JOIN payment_batches pb
       ON pb.tenant_id = e.tenant_id
      AND pb.legal_entity_id = e.legal_entity_id
      AND pb.id = e.payment_batch_id
     JOIN payment_batch_lines pbl
       ON pbl.tenant_id = e.tenant_id
      AND pbl.legal_entity_id = e.legal_entity_id
      AND pbl.id = e.payment_batch_line_id
     LEFT JOIN bank_statement_lines sl
       ON sl.tenant_id = e.tenant_id
      AND sl.legal_entity_id = e.legal_entity_id
      AND sl.id = e.bank_statement_line_id
     WHERE ${whereSql}
     ORDER BY e.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: res.rows || [],
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getPaymentReturnEventById({ req, tenantId, eventId, assertScopeAccess }) {
  const res = await query(
    `SELECT
        e.*,
        pb.batch_no,
        pb.bank_account_id,
        pbl.line_no AS payment_batch_line_no,
        pbl.amount AS payment_line_amount,
        pbl.bank_execution_status,
        pbl.return_status,
        pbl.returned_amount,
        sl.txn_date AS statement_txn_date,
        sl.amount AS statement_amount,
        sl.description AS statement_description
     FROM bank_payment_return_events e
     JOIN payment_batches pb
       ON pb.tenant_id = e.tenant_id
      AND pb.legal_entity_id = e.legal_entity_id
      AND pb.id = e.payment_batch_id
     JOIN payment_batch_lines pbl
       ON pbl.tenant_id = e.tenant_id
      AND pbl.legal_entity_id = e.legal_entity_id
      AND pbl.id = e.payment_batch_line_id
     LEFT JOIN bank_statement_lines sl
       ON sl.tenant_id = e.tenant_id
      AND sl.legal_entity_id = e.legal_entity_id
      AND sl.id = e.bank_statement_line_id
     WHERE e.tenant_id = ?
       AND e.id = ?
     LIMIT 1`,
    [tenantId, eventId]
  );
  const row = res.rows?.[0] || null;
  if (!row) throw badRequest("Payment return event not found");
  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "eventId");
  return row;
}

export async function createManualPaymentReturnEvent({ req, tenantId, userId, input, assertScopeAccess }) {
  if (!input?._b09SkipApprovalGate) {
    const paymentLine = await getPaymentBatchLineForReturn({
      tenantId,
      paymentBatchLineId: input.paymentBatchLineId,
    });
    if (!paymentLine) throw badRequest("paymentBatchLineId not found");
    assertScopeAccess(req, "legal_entity", paymentLine.legal_entity_id, "paymentBatchLineId");

    const gov = await evaluateBankApprovalNeed({
      tenantId,
      targetType: "MANUAL_RETURN",
      actionType: "CREATE",
      legalEntityId: paymentLine.legal_entity_id,
      bankAccountId: paymentLine.bank_account_id,
      thresholdAmount: absAmount(input.amount),
      currencyCode: input.currencyCode,
    });
    if (gov?.approvalRequired || gov?.approval_required) {
      const submitRes = await submitBankApprovalRequest({
        tenantId,
        userId,
        requestInput: {
          requestKey:
            input.eventRequestId && String(input.eventRequestId).trim()
              ? `B09:MANUAL_RETURN:${String(input.eventRequestId).trim()}`
              : `B09:MANUAL_RETURN:${tenantId}:${input.paymentBatchLineId}:${absAmount(input.amount)}:${u(
                  input.currencyCode
                )}:r${absAmount(paymentLine.returned_amount)}`,
          targetType: "MANUAL_RETURN",
          targetId: null,
          actionType: "CREATE",
          legalEntityId: paymentLine.legal_entity_id,
          bankAccountId: paymentLine.bank_account_id,
          thresholdAmount: absAmount(input.amount),
          currencyCode: input.currencyCode,
          actionPayload: {
            ...input,
            sourceType: "MANUAL",
          },
        },
        snapshotBuilder: async () => ({
          source_type: "MANUAL",
          payment_batch_line_id: parsePositiveInt(input.paymentBatchLineId),
          bank_statement_line_id: parsePositiveInt(input.bankStatementLineId) || null,
          amount: absAmount(input.amount),
          currency_code: u(input.currencyCode),
          reason_code: input.reasonCode || null,
        }),
        policyOverride: gov,
      });

      return {
        approval_required: true,
        approval_request: submitRes.item || null,
        idempotent: Boolean(submitRes?.idempotent),
      };
    }
  }

  return withTransaction(async (tx) => {
    const result = await createPaymentReturnEventTx({
      tenantId,
      input,
      userId,
      runQuery: tx.query,
      applyEffects: true,
    });
    assertScopeAccess(req, "legal_entity", result.legalEntityId, "paymentBatchLineId");
    return result;
  });
}

export async function executeApprovedManualReturn({
  tenantId,
  approvalRequestId,
  approvedByUserId,
  payload = {},
}) {
  const result = await withTransaction(async (tx) => {
    return createPaymentReturnEventTx({
      tenantId,
      input: {
        ...payload,
        sourceType: "MANUAL",
        sourceRef: `B09_APPROVAL:${approvalRequestId}`,
        _b09SkipApprovalGate: true,
      },
      userId: approvedByUserId,
      applyEffects: true,
      runQuery: tx.query,
    });
  });

  return {
    payment_return_event_id: parsePositiveInt(result?.row?.id) || null,
    approval_request_id: approvalRequestId || null,
    created: true,
    idempotent: Boolean(result?.idempotent),
  };
}

export async function ignorePaymentReturnEvent({ req, tenantId, eventId, reasonMessage, userId, assertScopeAccess }) {
  const res = await query(
    `SELECT * FROM bank_payment_return_events
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, eventId]
  );
  const row = res.rows?.[0] || null;
  if (!row) throw badRequest("Payment return event not found");
  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "eventId");

  await query(
    `UPDATE bank_payment_return_events
     SET event_status = 'IGNORED',
         reason_message = ?,
         updated_at = CURRENT_TIMESTAMP
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?`,
    [reasonMessage || "Ignored by reviewer", tenantId, row.legal_entity_id, eventId]
  );

  const reread = await query(
    `SELECT *
     FROM bank_payment_return_events
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, row.legal_entity_id, eventId]
  );
  return reread.rows?.[0] || null;
}

export async function ingestReturnEventsFromAckImportTx({
  tenantId,
  legalEntityId,
  ackImportId,
  userId = null,
  runQuery = query,
}) {
  const rowsRes = await runQuery(
    `SELECT
        l.id,
        l.payment_batch_line_id,
        l.ack_status,
        l.ack_code,
        l.ack_message,
        l.ack_amount,
        l.currency_code,
        l.bank_reference,
        i.batch_id,
        i.id AS ack_import_id
     FROM payment_batch_ack_import_lines l
     JOIN payment_batch_ack_imports i
       ON i.tenant_id = l.tenant_id
      AND i.legal_entity_id = l.legal_entity_id
      AND i.id = l.ack_import_id
     WHERE l.tenant_id = ?
       AND l.legal_entity_id = ?
       AND l.ack_import_id = ?
       AND l.payment_batch_line_id IS NOT NULL
       AND l.ack_status = 'REJECTED'`,
    [tenantId, legalEntityId, ackImportId]
  );
  const rows = rowsRes.rows || [];
  const created = [];
  const idempotent = [];

  for (const row of rows) {
    const requestId = `B08B-ACKREJ:${ackImportId}:${row.id}`;
    // eslint-disable-next-line no-await-in-loop
    const result = await createPaymentReturnEventTx({
      tenantId,
      input: {
        eventRequestId: requestId,
        sourceType: "ACK",
        sourceRef: `ACK_IMPORT_LINE:${row.id}`,
        paymentBatchLineId: row.payment_batch_line_id,
        paymentBatchAckImportId: row.ack_import_id,
        paymentBatchAckImportLineId: row.id,
        eventType: "PAYMENT_REJECTED",
        eventStatus: "CONFIRMED",
        amount: row.ack_amount === null || row.ack_amount === undefined ? 0 : absAmount(row.ack_amount),
        currencyCode: row.currency_code || (await getPaymentBatchLineForReturn({
          tenantId,
          paymentBatchLineId: row.payment_batch_line_id,
          runQuery,
        }))?.currency_code,
        bankReference: row.bank_reference || null,
        reasonCode: row.ack_code || "ACK_REJECTED",
        reasonMessage: row.ack_message || "Rejected by bank acknowledgement",
        payload: {
          ack_import_line_id: row.id,
          ack_import_id: row.ack_import_id,
          ack_status: row.ack_status,
        },
      },
      userId,
      applyEffects: true,
      runQuery,
    });
    if (result.idempotent) idempotent.push(result.row);
    else created.push(result.row);
  }

  return {
    createdCount: created.length,
    idempotentCount: idempotent.length,
    created,
    idempotent,
  };
}

function tokenize(...values) {
  const set = new Set();
  for (const value of values) {
    const text = u(value);
    if (!text) continue;
    for (const token of text.split(/[\s,.;:/\\|()[\]-]+/g)) {
      if (token && token.length >= 3) set.add(token);
    }
  }
  return Array.from(set).slice(0, 20);
}

async function findPaymentBatchLineCandidatesForStatementReturn({
  tenantId,
  line,
  runQuery = query,
}) {
  const ref = u(line.reference_no);
  const desc = u(line.description);
  const tokens = tokenize(line.reference_no, line.description);
  if (!ref && !desc && !tokens.length) return null;

  const res = await runQuery(
    `SELECT
        pbl.id,
        pbl.tenant_id,
        pbl.legal_entity_id,
        pbl.batch_id,
        pbl.line_no,
        pbl.amount,
        pb.currency_code AS currency_code,
        pbl.bank_reference,
        pbl.external_payment_ref,
        pbl.beneficiary_bank_ref,
        pbl.payable_ref,
        pbl.beneficiary_name,
        pb.batch_no,
        pb.status AS batch_status,
        pb.bank_account_id
     FROM payment_batch_lines pbl
     JOIN payment_batches pb
       ON pb.tenant_id = pbl.tenant_id
      AND pb.legal_entity_id = pbl.legal_entity_id
      AND pb.id = pbl.batch_id
     WHERE pbl.tenant_id = ?
       AND pbl.legal_entity_id = ?
       AND pb.bank_account_id = ?
       AND pb.status = 'POSTED'
       AND pb.currency_code = ?
     ORDER BY pbl.id DESC
     LIMIT 300`,
    [tenantId, line.legal_entity_id, line.bank_account_id, line.currency_code]
  );

  const lineAmtAbs = absAmount(line.amount);
  const candidates = [];
  for (const row of res.rows || []) {
    const baseAmt = absAmount(row.amount);
    if (lineAmtAbs - baseAmt > AMOUNT_EPSILON) {
      continue;
    }
    const blob = u(
      `${row.batch_no || ""} ${row.bank_reference || ""} ${row.external_payment_ref || ""} ${row.beneficiary_bank_ref || ""} ${row.payable_ref || ""} ${row.beneficiary_name || ""}`
    );
    let score = 0;
    if (ref && blob.includes(ref)) score += 50;
    if (tokens.length) {
      const tokenHits = tokens.filter((t) => blob.includes(t)).length;
      score += tokenHits * 10;
    }
    if (Math.abs(baseAmt - lineAmtAbs) <= 0.01) score += 15;
    if (score <= 0) continue;
    candidates.push({ ...row, _score: score });
  }
  candidates.sort((a, b) => b._score - a._score || b.id - a.id);
  return candidates;
}

export async function findPaymentLineCandidatesForReturnAutomation({
  tenantId,
  line,
}) {
  const rows = await findPaymentBatchLineCandidatesForStatementReturn({ tenantId, line });
  return (rows || []).map((row) => ({
    entityType: "PAYMENT_BATCH_LINE",
    entityId: parsePositiveInt(row.id),
    paymentBatchId: parsePositiveInt(row.batch_id),
    amount: absAmount(line?.amount),
    displayRef: row.bank_reference || row.external_payment_ref || `${row.batch_no || "PB"}#${row.line_no}`,
    displayText: row.beneficiary_name || row.payable_ref || null,
    date: null,
    confidence: Math.min(99, Number(row._score || 0)),
  }));
}

export async function processPaymentReturnFromStatementLine({
  req,
  tenantId,
  lineId,
  userId,
  input = {},
  ruleId = null,
  confidence = null,
  assertScopeAccess,
}) {
  const statementLine = await getStatementLineForReturn({
    tenantId,
    statementLineId: lineId,
  });
  if (!statementLine) throw badRequest("Statement line not found");
  assertScopeAccess(req, "legal_entity", statementLine.legal_entity_id, "lineId");

  const candidate =
    parsePositiveInt(input.paymentBatchLineId ?? input.payment_batch_line_id)
      ? await getPaymentBatchLineForReturn({
          tenantId,
          paymentBatchLineId: parsePositiveInt(input.paymentBatchLineId ?? input.payment_batch_line_id),
        })
      : (() => null)();
  const candidateRows =
    candidate
      ? [candidate]
      : await findPaymentBatchLineCandidatesForStatementReturn({
          tenantId,
          line: statementLine,
        });
  if (!candidateRows || candidateRows.length !== 1) {
    throw badRequest("Could not identify a unique payment batch line for return processing");
  }
  const chosen = candidateRows[0];
  const paymentLine =
    candidate && parsePositiveInt(candidate.id)
      ? candidate
      : await getPaymentBatchLineForReturn({ tenantId, paymentBatchLineId: chosen.id });
  const resolvedCandidate = paymentLine || chosen;
  if (parsePositiveInt(resolvedCandidate.legal_entity_id) !== parsePositiveInt(statementLine.legal_entity_id)) {
    throw badRequest("Return candidate legal entity mismatch");
  }
  if (parsePositiveInt(resolvedCandidate.bank_account_id) !== parsePositiveInt(statementLine.bank_account_id)) {
    throw badRequest("Return candidate bank account mismatch");
  }

  const requestId = `B08B-STMTRET:${lineId}:${resolvedCandidate.id}`;

  const txResult = await withTransaction(async (tx) => {
    const created = await createPaymentReturnEventTx({
      tenantId,
      input: {
        eventRequestId: requestId,
        sourceType: "STATEMENT",
        sourceRef: `BSL:${lineId}`,
        paymentBatchLineId: resolvedCandidate.id,
        bankStatementLineId: lineId,
        eventType: u(input.eventType || "PAYMENT_RETURNED"),
        eventStatus: "CONFIRMED",
        amount: absAmount(statementLine.amount),
        currencyCode: statementLine.currency_code,
        bankReference: statementLine.reference_no || resolvedCandidate.bank_reference || null,
        reasonCode: input.reasonCode || input.reason_code || null,
        reasonMessage: input.reasonMessage || input.reason_message || null,
        payload: {
          rule_id: parsePositiveInt(ruleId) || null,
          source: "B07_PROCESS_PAYMENT_RETURN",
        },
      },
      userId,
      applyEffects: true,
      runQuery: tx.query,
    });

    const match = await matchReconciliationLine({
      req,
      tenantId,
      lineId,
      matchInput: {
        matchType: "AUTO_RULE",
        matchedEntityType: "PAYMENT_BATCH",
        matchedEntityId: resolvedCandidate.batch_id,
        matchedAmount: absAmount(statementLine.amount),
        notes: `B08-B return processing via payment line #${resolvedCandidate.id}`,
        reconciliationMethod: "RULE_RETURN",
        reconciliationRuleId: parsePositiveInt(ruleId) || null,
        reconciliationConfidence:
          confidence === null || confidence === undefined ? null : Number(Number(confidence).toFixed(2)),
      },
      userId,
      assertScopeAccess: assertScopeAccess || (() => {}),
    });

    return {
      returnEvent: created.row,
      returnEventIdempotent: Boolean(created.idempotent),
      paymentLine: created.paymentLine,
      reconciliation: match,
      paymentBatchId: resolvedCandidate.batch_id,
      paymentBatchLineId: resolvedCandidate.id,
    };
  });

  return txResult;
}

export default {
  resolvePaymentReturnEventScope,
  resolvePaymentBatchLineReturnScope,
  listPaymentReturnEventRows,
  getPaymentReturnEventById,
  createManualPaymentReturnEvent,
  executeApprovedManualReturn,
  ignorePaymentReturnEvent,
  ingestReturnEventsFromAckImportTx,
  findPaymentLineCandidatesForReturnAutomation,
  processPaymentReturnFromStatementLine,
};
