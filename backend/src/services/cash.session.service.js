import { withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  closeCashSession,
  countOpenUnpostedSessionTransactions,
  countCashSessions,
  findCashRegisterById,
  findCashTransactionById,
  findCashSessionById,
  findCashSessionScopeById,
  findOpenCashSessionByRegisterId,
  generateCashTxnNoForLegalEntityYearTx,
  insertCashTransaction,
  insertCashSession,
  listCashSessions,
  postCashTransaction,
  sumPostedSessionMovement,
} from "./cash.queries.js";
import { assertRegisterOperationalConfig } from "./cash.register.service.js";
import { createAndPostCashJournalTx } from "./cash.service.js";

const VARIANCE_EPSILON = 0.0001;

function normalizeMoney(value) {
  const parsed = Number(value || 0);
  return parsed.toFixed(6);
}

function nowMysqlDateTime() {
  return new Date().toISOString().slice(0, 19).replace("T", " ");
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

export async function resolveCashSessionScope(sessionId, tenantId) {
  const parsedSessionId = parsePositiveInt(sessionId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedSessionId || !parsedTenantId) {
    return null;
  }

  const row = await findCashSessionScopeById({
    tenantId: parsedTenantId,
    sessionId: parsedSessionId,
  });
  if (!row) {
    return null;
  }

  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: Number(row.legal_entity_id),
  };
}

export async function listCashSessionRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["cs.tenant_id = ?"];
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

    conditions.push("cs.cash_register_id = ?");
    params.push(filters.registerId);
  }

  if (filters.status) {
    conditions.push("cs.status = ?");
    params.push(filters.status);
  }
  if (filters.openedFrom) {
    conditions.push("DATE(cs.opened_at) >= ?");
    params.push(filters.openedFrom);
  }
  if (filters.openedTo) {
    conditions.push("DATE(cs.opened_at) <= ?");
    params.push(filters.openedTo);
  }

  const whereSql = conditions.join(" AND ");
  const total = await countCashSessions({ whereSql, params });
  const rows = await listCashSessions({
    whereSql,
    params,
    limit: filters.limit,
    offset: filters.offset,
  });

  return {
    rows,
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getCashSessionByIdForTenant({
  req,
  tenantId,
  sessionId,
  assertScopeAccess,
}) {
  const row = await findCashSessionById({
    tenantId,
    sessionId,
  });
  if (!row) {
    throw badRequest("Cash session not found");
  }

  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "sessionId");
  if (row.operating_unit_id) {
    assertScopeAccess(req, "operating_unit", row.operating_unit_id, "sessionId");
  }

  return row;
}

export async function openCashSession({
  req,
  payload,
  assertScopeAccess,
}) {
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
  if (String(register.session_mode || "").toUpperCase() === "NONE") {
    throw badRequest("Cash register session_mode is NONE");
  }

  assertScopeAccess(req, "legal_entity", register.legal_entity_id, "registerId");
  if (register.operating_unit_id) {
    assertScopeAccess(req, "operating_unit", register.operating_unit_id, "registerId");
  }

  const opened = await withTransaction(async (tx) => {
    await tx.query(
      `SELECT id
       FROM cash_registers
       WHERE tenant_id = ?
         AND id = ?
       LIMIT 1
       FOR UPDATE`,
      [payload.tenantId, payload.registerId]
    );

    const openSession = await findOpenCashSessionByRegisterId({
      tenantId: payload.tenantId,
      registerId: payload.registerId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (openSession) {
      throw badRequest("An OPEN session already exists for this register");
    }

    const sessionId = await insertCashSession({
      payload,
      runQuery: tx.query,
    });

    return findCashSessionById({
      tenantId: payload.tenantId,
      sessionId,
      runQuery: tx.query,
    });
  });

  return opened;
}

export async function closeCashSessionById({
  req,
  payload,
  assertScopeAccess,
}) {
  const closed = await withTransaction(async (tx) => {
    const session = await findCashSessionById({
      tenantId: payload.tenantId,
      sessionId: payload.sessionId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!session) {
      throw badRequest("Cash session not found");
    }

    if (String(session.status || "").toUpperCase() !== "OPEN") {
      throw badRequest("Only OPEN sessions can be closed");
    }

    assertScopeAccess(req, "legal_entity", session.legal_entity_id, "sessionId");
    if (session.operating_unit_id) {
      assertScopeAccess(req, "operating_unit", session.operating_unit_id, "sessionId");
    }

    const netMovement = await sumPostedSessionMovement({
      tenantId: payload.tenantId,
      registerId: session.cash_register_id,
      sessionId: payload.sessionId,
      runQuery: tx.query,
    });

    const openUnpostedTxnCount = await countOpenUnpostedSessionTransactions({
      tenantId: payload.tenantId,
      sessionId: payload.sessionId,
      runQuery: tx.query,
    });
    if (openUnpostedTxnCount > 0) {
      throw badRequest(
        "Cannot close session while DRAFT/SUBMITTED/APPROVED transactions exist"
      );
    }

    const expectedClosingAmount = Number(session.opening_amount || 0) + Number(netMovement || 0);
    const countedClosingAmount = Number(payload.countedClosingAmount);
    const varianceAmount = countedClosingAmount - expectedClosingAmount;
    const absoluteVarianceAmount = Math.abs(varianceAmount);
    const varianceApprovalThreshold = Number(session.requires_approval_over_amount || 0);
    const varianceApprovalRequired =
      varianceApprovalThreshold > 0 &&
      absoluteVarianceAmount - varianceApprovalThreshold > VARIANCE_EPSILON;

    if (payload.closedReason === "FORCED_CLOSE" && !payload.closeNote) {
      throw badRequest("closeNote is required when closedReason is FORCED_CLOSE");
    }
    if (varianceApprovalRequired && !payload.closeNote) {
      throw badRequest("closeNote is required when variance exceeds approval threshold");
    }
    if (varianceApprovalRequired && !payload.approveVariance) {
      throw badRequest(
        "Variance exceeds configured threshold; supervisor/finance approval is required"
      );
    }

    let varianceTransaction = null;
    if (absoluteVarianceAmount > VARIANCE_EPSILON) {
      const isOverVariance = varianceAmount > 0;
      const varianceCounterAccountId = parsePositiveInt(
        isOverVariance ? session.variance_gain_account_id : session.variance_loss_account_id
      );
      if (!varianceCounterAccountId) {
        throw badRequest(
          isOverVariance
            ? "varianceGainAccountId must be configured on register for over variance"
            : "varianceLossAccountId must be configured on register for short variance"
        );
      }

      const bookDate = todayIsoDate();
      const varianceTxnNo = await generateCashTxnNoForLegalEntityYearTx({
        tenantId: payload.tenantId,
        legalEntityId: session.legal_entity_id,
        legalEntityCode: session.legal_entity_code,
        bookDate,
        runQuery: tx.query,
      });

      const varianceTxnId = await insertCashTransaction({
        payload: {
          tenantId: payload.tenantId,
          registerId: session.cash_register_id,
          cashSessionId: payload.sessionId,
          txnNo: varianceTxnNo,
          txnType: "VARIANCE",
          status: "DRAFT",
          txnDatetime: nowMysqlDateTime(),
          bookDate,
          amount: normalizeMoney(absoluteVarianceAmount),
          currencyCode: session.register_currency_code,
          description: isOverVariance
            ? `Session close over variance (session ${session.id})`
            : `Session close short variance (session ${session.id})`,
          referenceNo: `SESSION:${session.id}:VARIANCE`,
          sourceDocType: "OTHER",
          sourceDocId: `CASH_SESSION:${session.id}`,
          sourceModule: "SYSTEM",
          sourceEntityType: "cash_session",
          sourceEntityId: String(session.id),
          integrationLinkStatus: "UNLINKED",
          counterpartyType: null,
          counterpartyId: null,
          counterAccountId: varianceCounterAccountId,
          counterCashRegisterId: null,
          linkedCariSettlementBatchId: null,
          linkedCariUnappliedCashId: null,
          reversalOfTransactionId: null,
          overrideCashControl: false,
          overrideReason: null,
          idempotencyKey: `SYS-VARIANCE-SESSION-${session.id}`,
          integrationEventUid: `SYS-VARIANCE-SESSION-${session.id}`,
          userId: payload.userId,
          postedByUserId: null,
          postedAt: null,
        },
        runQuery: tx.query,
      });

      const varianceDraftTransaction = await findCashTransactionById({
        tenantId: payload.tenantId,
        transactionId: varianceTxnId,
        runQuery: tx.query,
      });
      if (!varianceDraftTransaction) {
        throw badRequest("Failed to create session variance transaction");
      }

      const variancePosting = await createAndPostCashJournalTx(tx, {
        tenantId: payload.tenantId,
        userId: payload.userId,
        legalEntityId: parsePositiveInt(session.legal_entity_id),
        cashTxn: varianceDraftTransaction,
        req,
      });

      await postCashTransaction({
        tenantId: payload.tenantId,
        transactionId: varianceTxnId,
        userId: payload.userId,
        postedJournalEntryId: variancePosting.journalEntryId,
        overrideCashControl: false,
        overrideReason: null,
        runQuery: tx.query,
      });

      varianceTransaction = await findCashTransactionById({
        tenantId: payload.tenantId,
        transactionId: varianceTxnId,
        runQuery: tx.query,
      });
    }

    await closeCashSession({
      sessionId: payload.sessionId,
      payload: {
        tenantId: payload.tenantId,
        userId: payload.userId,
        expectedClosingAmount: normalizeMoney(expectedClosingAmount),
        countedClosingAmount: normalizeMoney(countedClosingAmount),
        varianceAmount: normalizeMoney(varianceAmount),
        approvedByUserId: varianceApprovalRequired ? payload.userId : null,
        approvedAt: varianceApprovalRequired ? nowMysqlDateTime() : null,
        closedReason: payload.closedReason,
        closeNote: payload.closeNote,
      },
      runQuery: tx.query,
    });

    const closedSession = await findCashSessionById({
      tenantId: payload.tenantId,
      sessionId: payload.sessionId,
      runQuery: tx.query,
    });
    return {
      ...closedSession,
      varianceAutoPosted: Boolean(varianceTransaction),
      varianceTransactionId: parsePositiveInt(varianceTransaction?.id),
      varianceApprovalRequired,
      varianceApprovalThreshold:
        varianceApprovalThreshold > 0 ? Number(varianceApprovalThreshold.toFixed(6)) : null,
    };
  });

  return closed;
}
