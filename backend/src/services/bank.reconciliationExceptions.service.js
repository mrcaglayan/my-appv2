import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  decodeCursorToken,
  encodeCursorToken,
  requireCursorDateTime,
  requireCursorId,
  toCursorDateTime,
} from "../utils/cursorPagination.js";
import { evaluateBankApprovalNeed } from "./bank.governance.service.js";
import { submitBankApprovalRequest } from "./bank.approvals.service.js";

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function parseJson(value, fallback = null) {
  if (value === undefined || value === null) return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function hydrateExceptionRow(row) {
  if (!row) return null;
  return {
    ...row,
    suggested_payload_json: parseJson(row.suggested_payload_json, null),
  };
}

function toPositiveIntOrNull(value) {
  return parsePositiveInt(value) || null;
}

function exceptionStatusSortRank(status) {
  const normalized = String(status || "")
    .trim()
    .toUpperCase();
  if (normalized === "OPEN") return 0;
  if (normalized === "ASSIGNED") return 1;
  if (normalized === "IGNORED") return 2;
  return 3;
}

async function getExceptionRowById({ tenantId, exceptionId, runQuery = query }) {
  const result = await runQuery(
    `SELECT *
     FROM bank_reconciliation_exceptions
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, exceptionId]
  );
  return hydrateExceptionRow(result.rows?.[0] || null);
}

async function insertExceptionEvent({
  tenantId,
  legalEntityId,
  exceptionId,
  eventType,
  payload = null,
  actedByUserId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO bank_reconciliation_exception_events (
        tenant_id,
        legal_entity_id,
        bank_reconciliation_exception_id,
        event_type,
        payload_json,
        acted_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, exceptionId, eventType, safeJson(payload), actedByUserId]
  );
}

async function findOpenOrAssignedExceptionForLine({
  tenantId,
  legalEntityId,
  statementLineId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT *
     FROM bank_reconciliation_exceptions
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND statement_line_id = ?
       AND status IN ('OPEN','ASSIGNED')
     ORDER BY
       CASE status WHEN 'ASSIGNED' THEN 0 ELSE 1 END ASC,
       updated_at DESC,
       id DESC
     LIMIT 1`,
    [tenantId, legalEntityId, statementLineId]
  );
  return hydrateExceptionRow(result.rows?.[0] || null);
}

async function getStatementLineScopeRow({ tenantId, statementLineId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, legal_entity_id, bank_account_id, recon_status
     FROM bank_statement_lines
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, statementLineId]
  );
  return result.rows?.[0] || null;
}

export async function resolveBankReconciliationExceptionScope(exceptionId, tenantId) {
  const parsedExceptionId = parsePositiveInt(exceptionId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedExceptionId || !parsedTenantId) {
    return null;
  }
  const row = await getExceptionRowById({
    tenantId: parsedTenantId,
    exceptionId: parsedExceptionId,
  });
  if (!row) {
    return null;
  }
  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: toPositiveIntOrNull(row.legal_entity_id),
  };
}

export async function upsertReconciliationException({
  tenantId,
  legalEntityId,
  statementLineId,
  bankAccountId,
  reasonCode,
  reasonMessage = null,
  matchedRuleId = null,
  suggestedActionType = null,
  suggestedPayload = null,
  severity = "MEDIUM",
  userId = null,
  runQuery = query,
}) {
  const existing = await findOpenOrAssignedExceptionForLine({
    tenantId,
    legalEntityId,
    statementLineId,
    runQuery,
  });

  if (existing) {
    await runQuery(
      `UPDATE bank_reconciliation_exceptions
       SET reason_code = ?,
           reason_message = ?,
           matched_rule_id = ?,
           suggested_action_type = ?,
           suggested_payload_json = ?,
           severity = ?,
           last_seen_at = CURRENT_TIMESTAMP,
           occurrence_count = occurrence_count + 1
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?`,
      [
        reasonCode,
        reasonMessage || null,
        matchedRuleId || null,
        suggestedActionType || null,
        safeJson(suggestedPayload),
        severity,
        tenantId,
        legalEntityId,
        existing.id,
      ]
    );

    await insertExceptionEvent({
      tenantId,
      legalEntityId,
      exceptionId: existing.id,
      eventType: "UPDATED",
      payload: {
        reasonCode,
        reasonMessage: reasonMessage || null,
        matchedRuleId: matchedRuleId || null,
        suggestedActionType: suggestedActionType || null,
      },
      actedByUserId: userId,
      runQuery,
    });

    return getExceptionRowById({ tenantId, exceptionId: existing.id, runQuery });
  }

  const insertResult = await runQuery(
    `INSERT INTO bank_reconciliation_exceptions (
        tenant_id,
        legal_entity_id,
        statement_line_id,
        bank_account_id,
        status,
        severity,
        reason_code,
        reason_message,
        matched_rule_id,
        suggested_action_type,
        suggested_payload_json
      ) VALUES (?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?)`,
    [
      tenantId,
      legalEntityId,
      statementLineId,
      bankAccountId,
      severity,
      reasonCode,
      reasonMessage || null,
      matchedRuleId || null,
      suggestedActionType || null,
      safeJson(suggestedPayload),
    ]
  );

  const exceptionId = toPositiveIntOrNull(insertResult.rows?.insertId);
  if (!exceptionId) {
    throw new Error("Failed to create reconciliation exception");
  }

  await insertExceptionEvent({
    tenantId,
    legalEntityId,
    exceptionId,
    eventType: "CREATED",
    payload: {
      reasonCode,
      reasonMessage: reasonMessage || null,
      matchedRuleId: matchedRuleId || null,
      suggestedActionType: suggestedActionType || null,
    },
    actedByUserId: userId,
    runQuery,
  });

  return getExceptionRowById({ tenantId, exceptionId, runQuery });
}

export async function autoResolveOpenReconciliationExceptionsForLine({
  tenantId,
  legalEntityId,
  statementLineId,
  userId = null,
  resolutionCode = "RECONCILED",
  resolutionNote = "Automatically resolved after reconciliation match",
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT id, status
     FROM bank_reconciliation_exceptions
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND statement_line_id = ?
       AND status IN ('OPEN','ASSIGNED')
     ORDER BY id ASC`,
    [tenantId, legalEntityId, statementLineId]
  );
  const rows = result.rows || [];
  if (rows.length === 0) {
    return { resolvedCount: 0 };
  }

  for (const row of rows) {
    await runQuery(
      `UPDATE bank_reconciliation_exceptions
       SET status = 'RESOLVED',
           resolved_by_user_id = ?,
           resolved_at = CURRENT_TIMESTAMP,
           resolution_code = ?,
           resolution_note = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?`,
      [userId, resolutionCode, resolutionNote, tenantId, legalEntityId, row.id]
    );

    await insertExceptionEvent({
      tenantId,
      legalEntityId,
      exceptionId: row.id,
      eventType: "RESOLVED",
      payload: {
        autoResolved: true,
        resolutionCode,
        resolutionNote,
      },
      actedByUserId: userId,
      runQuery,
    });
  }

  return { resolvedCount: rows.length };
}

export async function listReconciliationExceptionRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["e.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "e.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("e.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.bankAccountId) {
    conditions.push("e.bank_account_id = ?");
    params.push(filters.bankAccountId);
  }

  if (filters.statementLineId) {
    conditions.push("e.statement_line_id = ?");
    params.push(filters.statementLineId);
  }

  if (filters.status) {
    conditions.push("e.status = ?");
    params.push(filters.status);
  }

  if (filters.reasonCode) {
    conditions.push("e.reason_code = ?");
    params.push(filters.reasonCode);
  }

  if (filters.q) {
    const like = `%${filters.q}%`;
    conditions.push(
      "(e.reason_message LIKE ? OR l.description LIKE ? OR l.reference_no LIKE ? OR ba.code LIKE ? OR ba.name LIKE ?)"
    );
    params.push(like, like, like, like, like);
  }

  const statusSortExpr =
    "CASE e.status WHEN 'OPEN' THEN 0 WHEN 'ASSIGNED' THEN 1 WHEN 'IGNORED' THEN 2 ELSE 3 END";
  const baseConditions = [...conditions];
  const baseParams = [...params];
  const cursorToken = filters.cursor || null;
  const cursor = decodeCursorToken(cursorToken);
  const pageConditions = [...baseConditions];
  const pageParams = [...baseParams];
  if (cursorToken) {
    const cursorStatusRankRaw = Number(cursor?.statusRank);
    const cursorStatusRank =
      Number.isInteger(cursorStatusRankRaw) && cursorStatusRankRaw >= 0 && cursorStatusRankRaw <= 3
        ? cursorStatusRankRaw
        : null;
    if (cursorStatusRank === null) {
      throw badRequest("cursor is invalid");
    }
    const cursorUpdatedAt = requireCursorDateTime(cursor, "updatedAt");
    const cursorId = requireCursorId(cursor, "id");
    pageConditions.push(
      `(
        ${statusSortExpr} > ?
        OR (${statusSortExpr} = ? AND e.updated_at < ?)
        OR (${statusSortExpr} = ? AND e.updated_at = ? AND e.id < ?)
      )`
    );
    pageParams.push(
      cursorStatusRank,
      cursorStatusRank,
      cursorUpdatedAt,
      cursorStatusRank,
      cursorUpdatedAt,
      cursorId
    );
  }
  const whereSql = pageConditions.join(" AND ");
  const countWhereSql = baseConditions.join(" AND ");
  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM bank_reconciliation_exceptions e
     JOIN bank_statement_lines l
       ON l.tenant_id = e.tenant_id
      AND l.legal_entity_id = e.legal_entity_id
      AND l.id = e.statement_line_id
     JOIN bank_accounts ba
       ON ba.tenant_id = e.tenant_id
      AND ba.legal_entity_id = e.legal_entity_id
      AND ba.id = e.bank_account_id
     WHERE ${countWhereSql}`,
    baseParams
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset = Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const effectiveOffset = cursorToken ? 0 : safeOffset;

  const listResult = await query(
    `SELECT
        e.*,
        l.txn_date,
        l.value_date,
        l.description AS statement_description,
        l.reference_no AS statement_reference_no,
        l.amount AS statement_amount,
        l.currency_code AS statement_currency_code,
        l.recon_status AS statement_recon_status,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name
     FROM bank_reconciliation_exceptions e
     JOIN bank_statement_lines l
       ON l.tenant_id = e.tenant_id
      AND l.legal_entity_id = e.legal_entity_id
      AND l.id = e.statement_line_id
     JOIN bank_accounts ba
       ON ba.tenant_id = e.tenant_id
      AND ba.legal_entity_id = e.legal_entity_id
      AND ba.id = e.bank_account_id
     WHERE ${whereSql}
     ORDER BY
       ${statusSortExpr} ASC,
       e.updated_at DESC,
       e.id DESC
     LIMIT ${safeLimit} OFFSET ${effectiveOffset}`,
    pageParams
  );

  const rawRows = listResult.rows || [];
  const lastRow = rawRows.length > 0 ? rawRows[rawRows.length - 1] : null;
  const nextCursor =
    cursorToken || safeOffset === 0
      ? rawRows.length === safeLimit && lastRow
        ? encodeCursorToken({
            statusRank: exceptionStatusSortRank(lastRow.status),
            updatedAt: toCursorDateTime(lastRow.updated_at),
            id: parsePositiveInt(lastRow.id),
          })
        : null
      : null;

  return {
    rows: rawRows.map(hydrateExceptionRow),
    total,
    limit: filters.limit,
    offset: cursorToken ? 0 : filters.offset,
    pageMode: cursorToken ? "CURSOR" : "OFFSET",
    nextCursor,
  };
}

export async function getReconciliationExceptionById({
  req,
  tenantId,
  exceptionId,
  assertScopeAccess,
}) {
  const row = await getExceptionRowById({ tenantId, exceptionId });
  if (!row) {
    throw badRequest("Reconciliation exception not found");
  }
  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "exceptionId");
  return row;
}

export async function assignReconciliationException({
  req,
  tenantId,
  exceptionId,
  assignedToUserId,
  userId,
  assertScopeAccess,
}) {
  const current = await getReconciliationExceptionById({
    req,
    tenantId,
    exceptionId,
    assertScopeAccess,
  });
  if (!["OPEN", "ASSIGNED"].includes(String(current.status || "").toUpperCase())) {
    throw badRequest("Only OPEN/ASSIGNED exceptions can be assigned");
  }

  return withTransaction(async (tx) => {
    const nextStatus = assignedToUserId ? "ASSIGNED" : "OPEN";
    await tx.query(
      `UPDATE bank_reconciliation_exceptions
       SET status = ?,
           assigned_to_user_id = ?,
           assigned_at = CASE WHEN ? IS NULL THEN NULL ELSE CURRENT_TIMESTAMP END
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?`,
      [nextStatus, assignedToUserId || null, assignedToUserId || null, tenantId, current.legal_entity_id, exceptionId]
    );

    await insertExceptionEvent({
      tenantId,
      legalEntityId: current.legal_entity_id,
      exceptionId,
      eventType: "ASSIGNED",
      payload: {
        status: nextStatus,
        assignedToUserId: assignedToUserId || null,
      },
      actedByUserId: userId,
      runQuery: tx.query,
    });

    return getExceptionRowById({ tenantId, exceptionId, runQuery: tx.query });
  });
}

export async function resolveReconciliationException({
  req,
  tenantId,
  exceptionId,
  resolutionCode,
  resolutionNote,
  userId,
  assertScopeAccess,
  skipApprovalGate = false,
  approvalRequestId = null,
}) {
  const current = await getReconciliationExceptionById({
    req,
    tenantId,
    exceptionId,
    assertScopeAccess,
  });
  if (String(current.status || "").toUpperCase() === "RESOLVED") {
    return current;
  }
  if (String(current.status || "").toUpperCase() === "IGNORED") {
    throw badRequest("Ignored exception cannot be resolved directly; retry first");
  }

  if (!skipApprovalGate) {
    const gov = await evaluateBankApprovalNeed({
      tenantId,
      targetType: "RECON_EXCEPTION_OVERRIDE",
      actionType: "RESOLVE",
      legalEntityId: current.legal_entity_id,
      bankAccountId: current.bank_account_id,
    });
    if (gov?.approvalRequired || gov?.approval_required) {
      const submitRes = await submitBankApprovalRequest({
        tenantId,
        userId,
        requestInput: {
          requestKey: `B09:RECON_EXC:${tenantId}:${exceptionId}:RESOLVE:${String(current.updated_at || "")}`,
          targetType: "RECON_EXCEPTION_OVERRIDE",
          targetId: exceptionId,
          actionType: "RESOLVE",
          legalEntityId: current.legal_entity_id,
          bankAccountId: current.bank_account_id,
          actionPayload: {
            exceptionId,
            overrideAction: "RESOLVE",
            resolutionCode,
            resolutionNote: resolutionNote || null,
          },
        },
        snapshotBuilder: async () => ({
          exception_id: current.id,
          status: current.status,
          severity: current.severity,
          reason_code: current.reason_code,
          bank_account_id: current.bank_account_id,
          statement_line_id: current.statement_line_id,
          requested_action: "RESOLVE",
          resolution_code: resolutionCode,
          resolution_note: resolutionNote || null,
        }),
        policyOverride: gov,
      });
      const approvalReqId = parsePositiveInt(submitRes?.item?.id) || null;
      if (approvalReqId) {
        await query(
          `UPDATE bank_reconciliation_exceptions
           SET override_approval_request_id = ?
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?`,
          [approvalReqId, tenantId, current.legal_entity_id, exceptionId]
        );
      }
      return {
        approval_required: true,
        approval_request: submitRes.item || null,
        idempotent: Boolean(submitRes?.idempotent),
      };
    }
  }

  return withTransaction(async (tx) => {
    await tx.query(
      `UPDATE bank_reconciliation_exceptions
       SET status = 'RESOLVED',
           resolved_by_user_id = ?,
           resolved_at = CURRENT_TIMESTAMP,
           resolution_code = ?,
           resolution_note = ?,
           override_approval_request_id = COALESCE(?, override_approval_request_id)
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?`,
      [
        userId,
        resolutionCode,
        resolutionNote || null,
        approvalRequestId || null,
        tenantId,
        current.legal_entity_id,
        exceptionId,
      ]
    );

    await insertExceptionEvent({
      tenantId,
      legalEntityId: current.legal_entity_id,
      exceptionId,
      eventType: "RESOLVED",
      payload: {
        resolutionCode,
        resolutionNote: resolutionNote || null,
      },
      actedByUserId: userId,
      runQuery: tx.query,
    });

    return getExceptionRowById({ tenantId, exceptionId, runQuery: tx.query });
  });
}

export async function ignoreReconciliationException({
  req,
  tenantId,
  exceptionId,
  resolutionNote,
  userId,
  assertScopeAccess,
  skipApprovalGate = false,
  approvalRequestId = null,
}) {
  const current = await getReconciliationExceptionById({
    req,
    tenantId,
    exceptionId,
    assertScopeAccess,
  });
  if (String(current.status || "").toUpperCase() === "IGNORED") {
    return current;
  }
  if (String(current.status || "").toUpperCase() === "RESOLVED") {
    throw badRequest("Resolved exception cannot be ignored directly; retry first");
  }

  if (!skipApprovalGate) {
    const gov = await evaluateBankApprovalNeed({
      tenantId,
      targetType: "RECON_EXCEPTION_OVERRIDE",
      actionType: "IGNORE",
      legalEntityId: current.legal_entity_id,
      bankAccountId: current.bank_account_id,
    });
    if (gov?.approvalRequired || gov?.approval_required) {
      const submitRes = await submitBankApprovalRequest({
        tenantId,
        userId,
        requestInput: {
          requestKey: `B09:RECON_EXC:${tenantId}:${exceptionId}:IGNORE:${String(current.updated_at || "")}`,
          targetType: "RECON_EXCEPTION_OVERRIDE",
          targetId: exceptionId,
          actionType: "IGNORE",
          legalEntityId: current.legal_entity_id,
          bankAccountId: current.bank_account_id,
          actionPayload: {
            exceptionId,
            overrideAction: "IGNORE",
            resolutionNote: resolutionNote || "Ignored",
          },
        },
        snapshotBuilder: async () => ({
          exception_id: current.id,
          status: current.status,
          severity: current.severity,
          reason_code: current.reason_code,
          bank_account_id: current.bank_account_id,
          statement_line_id: current.statement_line_id,
          requested_action: "IGNORE",
          resolution_note: resolutionNote || "Ignored",
        }),
        policyOverride: gov,
      });
      const approvalReqId = parsePositiveInt(submitRes?.item?.id) || null;
      if (approvalReqId) {
        await query(
          `UPDATE bank_reconciliation_exceptions
           SET override_approval_request_id = ?
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?`,
          [approvalReqId, tenantId, current.legal_entity_id, exceptionId]
        );
      }
      return {
        approval_required: true,
        approval_request: submitRes.item || null,
        idempotent: Boolean(submitRes?.idempotent),
      };
    }
  }

  return withTransaction(async (tx) => {
    await tx.query(
      `UPDATE bank_reconciliation_exceptions
       SET status = 'IGNORED',
           resolved_by_user_id = ?,
           resolved_at = CURRENT_TIMESTAMP,
           resolution_code = 'IGNORED',
           resolution_note = ?,
           override_approval_request_id = COALESCE(?, override_approval_request_id)
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?`,
      [
        userId,
        resolutionNote || "Ignored",
        approvalRequestId || null,
        tenantId,
        current.legal_entity_id,
        exceptionId,
      ]
    );

    await insertExceptionEvent({
      tenantId,
      legalEntityId: current.legal_entity_id,
      exceptionId,
      eventType: "IGNORED",
      payload: {
        resolutionNote: resolutionNote || "Ignored",
      },
      actedByUserId: userId,
      runQuery: tx.query,
    });

    return getExceptionRowById({ tenantId, exceptionId, runQuery: tx.query });
  });
}

export async function executeApprovedExceptionOverride({
  tenantId,
  approvalRequestId,
  approvedByUserId,
  payload = {},
}) {
  const action = u(payload.overrideAction || payload.actionType || "");
  const exceptionId = parsePositiveInt(payload.exceptionId || payload.exception_id);
  if (!exceptionId) throw badRequest("exceptionId is required in approval payload");
  if (action === "RESOLVE") {
    const row = await resolveReconciliationException({
      req: null,
      tenantId,
      exceptionId,
      resolutionCode: payload.resolutionCode || payload.resolution_code || "OVERRIDE_APPROVED",
      resolutionNote: payload.resolutionNote || payload.resolution_note || null,
      userId: approvedByUserId,
      assertScopeAccess: () => {},
      skipApprovalGate: true,
      approvalRequestId,
    });
    return { exception_id: exceptionId, action: "RESOLVE", status: row?.status || "RESOLVED" };
  }
  if (action === "IGNORE") {
    const row = await ignoreReconciliationException({
      req: null,
      tenantId,
      exceptionId,
      resolutionNote: payload.resolutionNote || payload.resolution_note || "Ignored",
      userId: approvedByUserId,
      assertScopeAccess: () => {},
      skipApprovalGate: true,
      approvalRequestId,
    });
    return { exception_id: exceptionId, action: "IGNORE", status: row?.status || "IGNORED" };
  }
  throw badRequest(`Unsupported overrideAction: ${action || "<empty>"}`);
}

export async function retryReconciliationException({
  req,
  tenantId,
  exceptionId,
  note = null,
  userId,
  assertScopeAccess,
}) {
  const current = await getReconciliationExceptionById({
    req,
    tenantId,
    exceptionId,
    assertScopeAccess,
  });

  return withTransaction(async (tx) => {
    await tx.query(
      `UPDATE bank_reconciliation_exceptions
       SET status = 'OPEN',
           assigned_to_user_id = NULL,
           assigned_at = NULL,
           resolved_by_user_id = NULL,
           resolved_at = NULL,
           resolution_code = NULL,
           resolution_note = NULL,
           last_seen_at = CURRENT_TIMESTAMP,
           occurrence_count = occurrence_count + 1
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?`,
      [tenantId, current.legal_entity_id, exceptionId]
    );

    await insertExceptionEvent({
      tenantId,
      legalEntityId: current.legal_entity_id,
      exceptionId,
      eventType: "RETRIED",
      payload: {
        note: note || null,
      },
      actedByUserId: userId,
      runQuery: tx.query,
    });

    return {
      exception: await getExceptionRowById({ tenantId, exceptionId, runQuery: tx.query }),
      statementLine: await getStatementLineScopeRow({
        tenantId,
        statementLineId: current.statement_line_id,
        runQuery: tx.query,
      }),
    };
  });
}

export default {
  resolveBankReconciliationExceptionScope,
  upsertReconciliationException,
  autoResolveOpenReconciliationExceptionsForLine,
  listReconciliationExceptionRows,
  getReconciliationExceptionById,
  assignReconciliationException,
  resolveReconciliationException,
  ignoreReconciliationException,
  retryReconciliationException,
  executeApprovedExceptionOverride,
};
