import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { assertPayrollPeriodActionAllowed } from "./payroll.close.service.js";
import { evaluateApprovalNeed, submitApprovalRequest } from "./approvalPolicies.service.js";

const EPSILON = 0.000001;

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function toAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Number(parsed.toFixed(6));
}

function amountString(value) {
  return toAmount(value).toFixed(6);
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function toDateTimeString(value) {
  if (!value) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(trimmed)) {
      return trimmed;
    }
    const parsed = new Date(trimmed);
    if (Number.isNaN(parsed.getTime())) return null;
    return parsed.toISOString().slice(0, 19).replace("T", " ");
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return value.toISOString().slice(0, 19).replace("T", " ");
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 19).replace("T", " ");
}

function makeNotFound(message) {
  const err = new Error(message);
  err.status = 404;
  return err;
}

function makeConflict(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}

function noopScopeAccess() {
  return true;
}

async function writeLiabilityAudit({
  tenantId,
  legalEntityId,
  runId,
  liabilityId = null,
  action,
  payload = null,
  userId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO payroll_liability_audit (
        tenant_id, legal_entity_id, run_id, payroll_liability_id, action, payload_json, acted_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, runId, liabilityId, action, safeJson(payload), userId]
  );
}

async function getLiabilityScopeRow({ tenantId, liabilityId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, tenant_id, legal_entity_id
     FROM payroll_run_liabilities
     WHERE tenant_id = ? AND id = ?
     LIMIT 1`,
    [tenantId, liabilityId]
  );
  return result.rows?.[0] || null;
}

async function getOverrideRequestScopeRow({ tenantId, requestId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, tenant_id, legal_entity_id
     FROM payroll_liability_override_requests
     WHERE tenant_id = ? AND id = ?
     LIMIT 1`,
    [tenantId, requestId]
  );
  return result.rows?.[0] || null;
}

async function getLiabilityWithLatestActiveLink({
  tenantId,
  liabilityId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
        l.id,
        l.tenant_id,
        l.legal_entity_id,
        l.run_id,
        l.liability_type,
        l.liability_group,
        l.employee_code,
        l.employee_name,
        l.beneficiary_name,
        l.amount,
        l.currency_code,
        l.status,
        l.reserved_payment_batch_id,
        l.settled_amount,
        l.outstanding_amount,
        l.paid_payment_batch_id,
        l.paid_payment_batch_line_id,
        l.paid_bank_statement_line_id,
        pl.id AS link_id,
        pl.payment_batch_id,
        pl.payment_batch_line_id,
        pl.allocated_amount,
        pl.settled_amount AS link_settled_amount,
        pl.status AS link_status,
        pl.settled_at AS link_settled_at
     FROM payroll_run_liabilities l
     LEFT JOIN payroll_liability_payment_links pl
       ON pl.tenant_id = l.tenant_id
      AND pl.legal_entity_id = l.legal_entity_id
      AND pl.run_id = l.run_id
      AND pl.payroll_liability_id = l.id
      AND pl.id = (
        SELECT pl2.id
        FROM payroll_liability_payment_links pl2
        WHERE pl2.tenant_id = l.tenant_id
          AND pl2.legal_entity_id = l.legal_entity_id
          AND pl2.run_id = l.run_id
          AND pl2.payroll_liability_id = l.id
          AND pl2.status IN ('LINKED','PARTIALLY_PAID','PAID')
        ORDER BY pl2.id DESC
        LIMIT 1
      )
     WHERE l.tenant_id = ? AND l.id = ?
     LIMIT 1`,
    [tenantId, liabilityId]
  );
  return result.rows?.[0] || null;
}

async function getOverrideRequestById({ tenantId, requestId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        r.*,
        l.run_id,
        l.legal_entity_id,
        l.liability_type,
        l.liability_group,
        l.employee_code,
        l.employee_name,
        l.beneficiary_name
     FROM payroll_liability_override_requests r
     JOIN payroll_run_liabilities l
       ON l.tenant_id = r.tenant_id
      AND l.legal_entity_id = r.legal_entity_id
      AND l.id = r.payroll_liability_id
     WHERE r.tenant_id = ? AND r.id = ?
     LIMIT 1`,
    [tenantId, requestId]
  );
  return result.rows?.[0] || null;
}

async function getPayrollRunPeriodRow({ tenantId, legalEntityId, runId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, tenant_id, legal_entity_id, payroll_period
     FROM payroll_runs
     WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, runId]
  );
  return result.rows?.[0] || null;
}

async function listOverrideRequestsForLiability({
  tenantId,
  legalEntityId,
  liabilityId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
        id, tenant_id, legal_entity_id, run_id, payroll_liability_id, payroll_liability_payment_link_id,
        request_type, requested_amount, currency_code, settled_at, reason, external_ref,
        status, idempotency_key,
        requested_by_user_id, requested_at,
        approved_by_user_id, approved_at,
        rejected_by_user_id, rejected_at,
        decision_note, applied_settlement_id,
        created_at, updated_at
     FROM payroll_liability_override_requests
     WHERE tenant_id = ? AND legal_entity_id = ? AND payroll_liability_id = ?
     ORDER BY id DESC`,
    [tenantId, legalEntityId, liabilityId]
  );
  return result.rows || [];
}

function derivePartialSettlementState({
  liabilityAmount,
  currentLiabilitySettled,
  linkAllocatedAmount,
  currentLinkSettled,
  deltaAmount,
}) {
  const liabilityAmt = toAmount(liabilityAmount);
  const liabSettled = toAmount(currentLiabilitySettled);
  const linkAllocated = toAmount(linkAllocatedAmount || liabilityAmount);
  const linkSettled = toAmount(currentLinkSettled);
  const delta = toAmount(deltaAmount);

  const newLinkSettled = toAmount(linkSettled + delta);
  const newLiabilitySettled = toAmount(liabSettled + delta);

  if (delta <= 0) {
    throw badRequest("Settlement delta must be > 0");
  }
  if (newLinkSettled > linkAllocated + EPSILON) {
    throw makeConflict(
      `Manual settlement would over-settle payment link (allocated ${linkAllocated}, target ${newLinkSettled})`
    );
  }
  if (newLiabilitySettled > liabilityAmt + EPSILON) {
    throw makeConflict(
      `Manual settlement would over-settle payroll liability (amount ${liabilityAmt}, target ${newLiabilitySettled})`
    );
  }

  const linkOutstanding = toAmount(Math.max(0, linkAllocated - newLinkSettled));
  const liabilityOutstanding = toAmount(Math.max(0, liabilityAmt - newLiabilitySettled));

  let linkStatus = "PARTIALLY_PAID";
  if (newLinkSettled <= EPSILON) {
    linkStatus = "LINKED";
  } else if (linkOutstanding <= EPSILON) {
    linkStatus = "PAID";
  }

  let liabilityStatus = "PARTIALLY_PAID";
  if (newLiabilitySettled <= EPSILON) {
    liabilityStatus = "IN_BATCH";
  } else if (liabilityOutstanding <= EPSILON) {
    liabilityStatus = "PAID";
  }

  return {
    deltaAmount: delta,
    linkAllocated,
    newLinkSettled,
    linkOutstanding,
    linkStatus,
    newLiabilitySettled,
    liabilityOutstanding,
    liabilityStatus,
  };
}

function validateManualOverrideEligibility(liabilityRow) {
  if (!liabilityRow) {
    throw makeNotFound("Payroll liability not found");
  }
  if (!parsePositiveInt(liabilityRow.link_id)) {
    throw badRequest(
      "Manual settlement override requires liability to be linked to a payment batch"
    );
  }

  const liabilityStatus = normalizeUpperText(liabilityRow.status);
  if (!["IN_BATCH", "PARTIALLY_PAID"].includes(liabilityStatus)) {
    throw badRequest(
      "Manual settlement override is allowed only for IN_BATCH or PARTIALLY_PAID liabilities"
    );
  }

  const linkStatus = normalizeUpperText(liabilityRow.link_status);
  if (!["LINKED", "PARTIALLY_PAID", "PAID"].includes(linkStatus)) {
    throw badRequest("Manual settlement override requires an active payroll payment link");
  }
}

function computeRemainingAmounts(liabilityRow) {
  const liabilityAmount = toAmount(liabilityRow.amount);
  const liabilitySettled = toAmount(liabilityRow.settled_amount);
  const liabilityOutstanding = toAmount(
    liabilityRow.outstanding_amount ?? Math.max(0, liabilityAmount - liabilitySettled)
  );

  const linkAllocated = toAmount(liabilityRow.allocated_amount || liabilityAmount);
  const linkSettled = toAmount(liabilityRow.link_settled_amount);
  const linkOutstanding = toAmount(Math.max(0, linkAllocated - linkSettled));

  return {
    liabilityAmount,
    liabilitySettled,
    liabilityOutstanding,
    linkAllocated,
    linkSettled,
    linkOutstanding,
    effectiveRemaining: toAmount(Math.min(liabilityOutstanding, linkOutstanding)),
  };
}

async function findOverrideRequestByIdempotency({
  tenantId,
  legalEntityId,
  idempotencyKey,
  runQuery = query,
}) {
  if (!idempotencyKey) return null;
  const result = await runQuery(
    `SELECT id
     FROM payroll_liability_override_requests
     WHERE tenant_id = ? AND legal_entity_id = ? AND idempotency_key = ?
     LIMIT 1`,
    [tenantId, legalEntityId, idempotencyKey]
  );
  return result.rows?.[0] || null;
}

async function getLiabilityForUpdateTx({ tenantId, liabilityId, runQuery }) {
  const result = await runQuery(
    `SELECT *
     FROM payroll_run_liabilities
     WHERE tenant_id = ? AND id = ?
     LIMIT 1
     FOR UPDATE`,
    [tenantId, liabilityId]
  );
  return result.rows?.[0] || null;
}

async function getPaymentLinkForUpdateTx({
  tenantId,
  legalEntityId,
  runId,
  liabilityId,
  linkId,
  runQuery,
}) {
  const result = await runQuery(
    `SELECT *
     FROM payroll_liability_payment_links
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND run_id = ?
       AND payroll_liability_id = ?
       AND id = ?
     LIMIT 1
     FOR UPDATE`,
    [tenantId, legalEntityId, runId, liabilityId, linkId]
  );
  return result.rows?.[0] || null;
}

async function getOverrideRequestForUpdateTx({ tenantId, requestId, runQuery }) {
  const result = await runQuery(
    `SELECT *
     FROM payroll_liability_override_requests
     WHERE tenant_id = ? AND id = ?
     LIMIT 1
     FOR UPDATE`,
    [tenantId, requestId]
  );
  return result.rows?.[0] || null;
}

async function getSettlementById({ tenantId, legalEntityId, settlementId, runQuery = query }) {
  if (!settlementId) return null;
  const result = await runQuery(
    `SELECT *
     FROM payroll_liability_settlements
     WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, settlementId]
  );
  return result.rows?.[0] || null;
}

async function getSettlementByKey({ tenantId, legalEntityId, settlementKey, runQuery = query }) {
  const result = await runQuery(
    `SELECT *
     FROM payroll_liability_settlements
     WHERE tenant_id = ? AND legal_entity_id = ? AND settlement_key = ?
     LIMIT 1`,
    [tenantId, legalEntityId, settlementKey]
  );
  return result.rows?.[0] || null;
}

export async function resolvePayrollLiabilityScope(liabilityId, tenantId) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedLiabilityId = parsePositiveInt(liabilityId);
  if (!parsedTenantId || !parsedLiabilityId) return null;
  const row = await getLiabilityScopeRow({
    tenantId: parsedTenantId,
    liabilityId: parsedLiabilityId,
  });
  if (!row) return null;
  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: parsePositiveInt(row.legal_entity_id),
  };
}

export async function resolvePayrollSettlementOverrideRequestScope(requestId, tenantId) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedRequestId = parsePositiveInt(requestId);
  if (!parsedTenantId || !parsedRequestId) return null;
  const row = await getOverrideRequestScopeRow({
    tenantId: parsedTenantId,
    requestId: parsedRequestId,
  });
  if (!row) return null;
  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: parsePositiveInt(row.legal_entity_id),
  };
}

export async function listPayrollManualSettlementRequests({
  req,
  tenantId,
  liabilityId,
  assertScopeAccess,
}) {
  const liability = await getLiabilityWithLatestActiveLink({ tenantId, liabilityId });
  if (!liability) throw makeNotFound("Payroll liability not found");

  const legalEntityId = parsePositiveInt(liability.legal_entity_id);
  assertScopeAccess(req, "legal_entity", legalEntityId, "liabilityId");

  const items = await listOverrideRequestsForLiability({
    tenantId,
    legalEntityId,
    liabilityId,
  });

  return {
    liability: {
      id: parsePositiveInt(liability.id),
      run_id: parsePositiveInt(liability.run_id),
      legal_entity_id: legalEntityId,
      liability_type: liability.liability_type,
      liability_group: liability.liability_group,
      employee_code: liability.employee_code || null,
      employee_name: liability.employee_name || null,
      beneficiary_name: liability.beneficiary_name || null,
      amount: toAmount(liability.amount),
      currency_code: normalizeUpperText(liability.currency_code),
      status: normalizeUpperText(liability.status),
      settled_amount: toAmount(liability.settled_amount),
      outstanding_amount: toAmount(
        liability.outstanding_amount ?? toAmount(liability.amount) - toAmount(liability.settled_amount)
      ),
      payment_link_id: parsePositiveInt(liability.link_id),
      payment_batch_id: parsePositiveInt(liability.payment_batch_id),
      payment_batch_line_id: parsePositiveInt(liability.payment_batch_line_id),
      allocated_amount: toAmount(liability.allocated_amount),
      link_status: liability.link_status || null,
      link_settled_amount: toAmount(liability.link_settled_amount),
    },
    items,
  };
}

export async function createPayrollManualSettlementRequest({
  req,
  tenantId,
  liabilityId,
  userId,
  input,
  assertScopeAccess,
}) {
  const liability = await getLiabilityWithLatestActiveLink({ tenantId, liabilityId });
  if (!liability) throw makeNotFound("Payroll liability not found");

  const legalEntityId = parsePositiveInt(liability.legal_entity_id);
  assertScopeAccess(req, "legal_entity", legalEntityId, "liabilityId");
  validateManualOverrideEligibility(liability);

  const runPeriodRow = await getPayrollRunPeriodRow({
    tenantId,
    legalEntityId,
    runId: parsePositiveInt(liability.run_id),
  });
  await assertPayrollPeriodActionAllowed({
    tenantId,
    legalEntityId,
    payrollPeriod: runPeriodRow?.payroll_period,
    actionType: "MANUAL_SETTLEMENT_REQUEST",
  });

  const remaining = computeRemainingAmounts(liability);
  const requestedAmount = toAmount(input.amount);
  if (requestedAmount > remaining.effectiveRemaining + EPSILON) {
    throw makeConflict(
      `Requested amount exceeds remaining settleable amount (${remaining.effectiveRemaining})`
    );
  }

  if (input.idempotencyKey) {
    const existing = await findOverrideRequestByIdempotency({
      tenantId,
      legalEntityId,
      idempotencyKey: input.idempotencyKey,
    });
    if (existing?.id) {
      const existingRow = await getOverrideRequestById({
        tenantId,
        requestId: parsePositiveInt(existing.id),
      });
      return { request: existingRow, idempotent: true };
    }
  }

  const ins = await query(
    `INSERT INTO payroll_liability_override_requests (
        tenant_id, legal_entity_id, run_id,
        payroll_liability_id, payroll_liability_payment_link_id,
        requested_amount, currency_code, settled_at, reason, external_ref,
        status, idempotency_key, requested_by_user_id
     )
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'REQUESTED', ?, ?)`,
    [
      tenantId,
      legalEntityId,
      parsePositiveInt(liability.run_id),
      liabilityId,
      parsePositiveInt(liability.link_id),
      amountString(requestedAmount),
      normalizeUpperText(liability.currency_code),
      input.settledAt,
      input.reason,
      input.externalRef || null,
      input.idempotencyKey || null,
      userId,
    ]
  );
  const requestId = parsePositiveInt(ins.rows?.insertId);
  if (!requestId) throw new Error("Failed to create manual settlement override request");

  await writeLiabilityAudit({
    tenantId,
    legalEntityId,
    runId: parsePositiveInt(liability.run_id),
    liabilityId,
    action: "MANUAL_SETTLEMENT_REQUESTED",
    payload: {
      requestId,
      requestedAmount,
      settledAt: input.settledAt,
      reason: input.reason,
      externalRef: input.externalRef || null,
    },
    userId,
  });

  const request = await getOverrideRequestById({ tenantId, requestId });
  return { request, idempotent: false };
}

export async function approveApplyPayrollManualSettlementRequest({
  req,
  tenantId,
  requestId,
  userId,
  decisionNote = null,
  assertScopeAccess,
  skipUnifiedApprovalGate = false,
  approvalRequestId = null,
}) {
  if (!skipUnifiedApprovalGate) {
    const previewRequestRow = await getOverrideRequestById({ tenantId, requestId });
    if (!previewRequestRow) throw makeNotFound("Manual settlement override request not found");

    const legalEntityId = parsePositiveInt(previewRequestRow.legal_entity_id);
    assertScopeAccess(req, "legal_entity", legalEntityId, "requestId");

    const requestStatus = normalizeUpperText(previewRequestRow.status);
    if (requestStatus === "REQUESTED") {
      const gov = await evaluateApprovalNeed({
        moduleCode: "PAYROLL",
        tenantId,
        targetType: "PAYROLL_MANUAL_SETTLEMENT_OVERRIDE",
        actionType: "APPLY",
        legalEntityId,
        thresholdAmount: toAmount(previewRequestRow.requested_amount),
        currencyCode: normalizeUpperText(previewRequestRow.currency_code),
      });

      if (gov?.approval_required || gov?.approvalRequired) {
        const submitRes = await submitApprovalRequest({
          tenantId,
          userId,
          requestInput: {
            moduleCode: "PAYROLL",
            requestKey: `PRP06:OVERRIDE_APPLY:${tenantId}:${requestId}`,
            targetType: "PAYROLL_MANUAL_SETTLEMENT_OVERRIDE",
            targetId: requestId,
            actionType: "APPLY",
            legalEntityId,
            thresholdAmount: toAmount(previewRequestRow.requested_amount),
            currencyCode: normalizeUpperText(previewRequestRow.currency_code),
            actionPayload: {
              requestId,
              decisionNote: decisionNote || null,
            },
            targetSnapshot: {
              module_code: "PAYROLL",
              target_type: "PAYROLL_MANUAL_SETTLEMENT_OVERRIDE",
              target_id: requestId,
              legal_entity_id: legalEntityId,
              run_id: parsePositiveInt(previewRequestRow.run_id) || null,
              payroll_liability_id: parsePositiveInt(previewRequestRow.payroll_liability_id) || null,
              requested_amount: toAmount(previewRequestRow.requested_amount),
              currency_code: normalizeUpperText(previewRequestRow.currency_code),
              status: requestStatus,
            },
          },
        });

        return {
          request: previewRequestRow,
          approval_required: true,
          approval_request: submitRes?.item || null,
          idempotent: Boolean(submitRes?.idempotent),
        };
      }
    }
  }

  return withTransaction(async (tx) => {
    const requestRow = await getOverrideRequestForUpdateTx({
      tenantId,
      requestId,
      runQuery: tx.query,
    });
    if (!requestRow) throw makeNotFound("Manual settlement override request not found");

    const legalEntityId = parsePositiveInt(requestRow.legal_entity_id);
    assertScopeAccess(req, "legal_entity", legalEntityId, "requestId");

    const requestStatus = normalizeUpperText(requestRow.status);
    if (requestStatus === "APPLIED") {
      const request = await getOverrideRequestById({ tenantId, requestId, runQuery: tx.query });
      const settlement = await getSettlementById({
        tenantId,
        legalEntityId,
        settlementId: parsePositiveInt(requestRow.applied_settlement_id),
        runQuery: tx.query,
      });
      return { request, settlement, idempotent: true };
    }
    if (requestStatus !== "REQUESTED") {
      throw badRequest(`Request status ${requestStatus} cannot be approved/applied`);
    }
    if (
      parsePositiveInt(requestRow.requested_by_user_id) &&
      parsePositiveInt(requestRow.requested_by_user_id) === parsePositiveInt(userId)
    ) {
      const err = new Error("Maker-checker violation: requester cannot approve/apply the same request");
      err.status = 403;
      throw err;
    }

    const liability = await getLiabilityForUpdateTx({
      tenantId,
      liabilityId: parsePositiveInt(requestRow.payroll_liability_id),
      runQuery: tx.query,
    });
    if (!liability) throw makeNotFound("Payroll liability not found");
    if (parsePositiveInt(liability.legal_entity_id) !== legalEntityId) {
      throw makeConflict("Override request liability entity mismatch");
    }

    const runPeriodRow = await getPayrollRunPeriodRow({
      tenantId,
      legalEntityId,
      runId: parsePositiveInt(liability.run_id),
      runQuery: tx.query,
    });
    await assertPayrollPeriodActionAllowed({
      tenantId,
      legalEntityId,
      payrollPeriod: runPeriodRow?.payroll_period,
      actionType: "MANUAL_SETTLEMENT_APPROVE",
      runQuery: tx.query,
    });

    const link = await getPaymentLinkForUpdateTx({
      tenantId,
      legalEntityId,
      runId: parsePositiveInt(liability.run_id),
      liabilityId: parsePositiveInt(liability.id),
      linkId: parsePositiveInt(requestRow.payroll_liability_payment_link_id),
      runQuery: tx.query,
    });
    if (!link) throw makeNotFound("Payroll liability payment link not found");

    validateManualOverrideEligibility({
      ...liability,
      link_id: link.id,
      link_status: link.status,
    });

    const state = derivePartialSettlementState({
      liabilityAmount: liability.amount,
      currentLiabilitySettled: liability.settled_amount,
      linkAllocatedAmount: link.allocated_amount,
      currentLinkSettled: link.settled_amount,
      deltaAmount: requestRow.requested_amount,
    });

    const settlementKey = `PRMANSET|T:${tenantId}|LE:${legalEntityId}|REQ:${requestId}`;
    const settledAt =
      toDateTimeString(requestRow.settled_at) ||
      new Date().toISOString().slice(0, 19).replace("T", " ");

    await tx.query(
      `INSERT INTO payroll_liability_settlements (
          tenant_id, legal_entity_id, settlement_key, run_id,
          payroll_liability_id, payroll_liability_payment_link_id,
          payment_batch_id, payment_batch_line_id, bank_statement_line_id,
          settlement_source, settled_amount, currency_code, settled_at,
          payload_json, created_by_user_id
       )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 'MANUAL_OVERRIDE', ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         settled_amount = GREATEST(settled_amount, VALUES(settled_amount)),
         settled_at = VALUES(settled_at),
         payload_json = VALUES(payload_json)`,
      [
        tenantId,
        legalEntityId,
        settlementKey,
        parsePositiveInt(liability.run_id),
        parsePositiveInt(liability.id),
        parsePositiveInt(link.id),
        parsePositiveInt(link.payment_batch_id),
        parsePositiveInt(link.payment_batch_line_id),
        amountString(state.newLinkSettled),
        normalizeUpperText(requestRow.currency_code || liability.currency_code),
        settledAt,
        safeJson({
          reason: requestRow.reason || null,
          externalRef: requestRow.external_ref || null,
          requestId,
          approvalRequestId: parsePositiveInt(approvalRequestId) || null,
          deltaAmount: state.deltaAmount,
          decisionNote: decisionNote || null,
        }),
        userId,
      ]
    );

    const settlement = await getSettlementByKey({
      tenantId,
      legalEntityId,
      settlementKey,
      runQuery: tx.query,
    });

    await tx.query(
      `UPDATE payroll_liability_payment_links
       SET status = ?,
           settled_amount = ?,
           settled_at = ?,
           last_sync_at = CURRENT_TIMESTAMP,
           sync_note = ?
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
      [
        state.linkStatus,
        amountString(state.newLinkSettled),
        settledAt,
        "manual_override",
        tenantId,
        legalEntityId,
        parsePositiveInt(link.id),
      ]
    );

    await tx.query(
      `UPDATE payroll_run_liabilities
       SET status = ?,
           settled_amount = ?,
           outstanding_amount = ?,
           paid_at = CASE WHEN ? = 'PAID' THEN ? ELSE paid_at END,
           paid_payment_batch_id = CASE WHEN ? = 'PAID' THEN ? ELSE paid_payment_batch_id END,
           paid_payment_batch_line_id = CASE WHEN ? = 'PAID' THEN ? ELSE paid_payment_batch_line_id END,
           paid_bank_statement_line_id = CASE WHEN ? = 'PAID' THEN NULL ELSE paid_bank_statement_line_id END,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
      [
        state.liabilityStatus,
        amountString(state.newLiabilitySettled),
        amountString(state.liabilityOutstanding),
        state.liabilityStatus,
        settledAt,
        state.liabilityStatus,
        parsePositiveInt(link.payment_batch_id),
        state.liabilityStatus,
        parsePositiveInt(link.payment_batch_line_id),
        state.liabilityStatus,
        tenantId,
        legalEntityId,
        parsePositiveInt(liability.id),
      ]
    );

    await tx.query(
      `UPDATE payroll_liability_override_requests
       SET status = 'APPLIED',
           approved_by_user_id = ?,
           approved_at = CURRENT_TIMESTAMP,
           decision_note = ?,
           applied_settlement_id = COALESCE(applied_settlement_id, ?)
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
      [
        userId,
        decisionNote || null,
        parsePositiveInt(settlement?.id),
        tenantId,
        legalEntityId,
        requestId,
      ]
    );

    await writeLiabilityAudit({
      tenantId,
      legalEntityId,
      runId: parsePositiveInt(liability.run_id),
      liabilityId: parsePositiveInt(liability.id),
      action: "MANUAL_SETTLEMENT_APPLIED",
      payload: {
        requestId,
        settlementId: parsePositiveInt(settlement?.id) || null,
        approvalRequestId: parsePositiveInt(approvalRequestId) || null,
        deltaAmount: state.deltaAmount,
        totalSettled: state.newLiabilitySettled,
        outstandingAmount: state.liabilityOutstanding,
        liabilityStatus: state.liabilityStatus,
        linkStatus: state.linkStatus,
      },
      userId,
      runQuery: tx.query,
    });

    const request = await getOverrideRequestById({ tenantId, requestId, runQuery: tx.query });
    return {
      request,
      settlement,
      idempotent: false,
    };
  });
}

export async function executeApprovedPayrollManualSettlementOverride({
  tenantId,
  approvalRequestId,
  approvedByUserId,
  payload = {},
}) {
  const requestId = parsePositiveInt(payload?.requestId ?? payload?.request_id);
  if (!requestId) {
    throw badRequest("Approved payroll manual settlement override payload is missing requestId");
  }
  return approveApplyPayrollManualSettlementRequest({
    req: null,
    tenantId,
    requestId,
    userId: parsePositiveInt(approvedByUserId) || null,
    decisionNote:
      String((payload?.decisionNote ?? payload?.decision_note) || "").trim() ||
      "Approved via unified approval engine",
    assertScopeAccess: noopScopeAccess,
    skipUnifiedApprovalGate: true,
    approvalRequestId,
  });
}

export async function rejectPayrollManualSettlementRequest({
  req,
  tenantId,
  requestId,
  userId,
  decisionNote = "Rejected",
  assertScopeAccess,
}) {
  return withTransaction(async (tx) => {
    const requestRow = await getOverrideRequestForUpdateTx({
      tenantId,
      requestId,
      runQuery: tx.query,
    });
    if (!requestRow) throw makeNotFound("Manual settlement override request not found");

    const legalEntityId = parsePositiveInt(requestRow.legal_entity_id);
    assertScopeAccess(req, "legal_entity", legalEntityId, "requestId");

    const requestStatus = normalizeUpperText(requestRow.status);
    if (requestStatus === "REJECTED") {
      const request = await getOverrideRequestById({ tenantId, requestId, runQuery: tx.query });
      return { request, idempotent: true };
    }
    if (requestStatus === "APPLIED") {
      throw makeConflict("Applied manual settlement override request cannot be rejected");
    }
    if (
      parsePositiveInt(requestRow.requested_by_user_id) &&
      parsePositiveInt(requestRow.requested_by_user_id) === parsePositiveInt(userId)
    ) {
      const err = new Error("Maker-checker violation: requester cannot reject the same request");
      err.status = 403;
      throw err;
    }

    const runPeriodRow = await getPayrollRunPeriodRow({
      tenantId,
      legalEntityId,
      runId: parsePositiveInt(requestRow.run_id),
      runQuery: tx.query,
    });
    await assertPayrollPeriodActionAllowed({
      tenantId,
      legalEntityId,
      payrollPeriod: runPeriodRow?.payroll_period,
      actionType: "MANUAL_SETTLEMENT_REJECT",
      runQuery: tx.query,
    });

    await tx.query(
      `UPDATE payroll_liability_override_requests
       SET status = 'REJECTED',
           rejected_by_user_id = ?,
           rejected_at = CURRENT_TIMESTAMP,
           decision_note = ?
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
      [userId, decisionNote || "Rejected", tenantId, legalEntityId, requestId]
    );

    const liabilityScope = await getLiabilityScopeRow({
      tenantId,
      liabilityId: parsePositiveInt(requestRow.payroll_liability_id),
      runQuery: tx.query,
    });

    if (liabilityScope) {
      const liability = await tx.query(
        `SELECT run_id
         FROM payroll_run_liabilities
         WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?
         LIMIT 1`,
        [tenantId, legalEntityId, parsePositiveInt(requestRow.payroll_liability_id)]
      );
      const runId = parsePositiveInt(liability.rows?.[0]?.run_id);
      if (runId) {
        await writeLiabilityAudit({
          tenantId,
          legalEntityId,
          runId,
          liabilityId: parsePositiveInt(requestRow.payroll_liability_id),
          action: "MANUAL_SETTLEMENT_REJECTED",
          payload: {
            requestId,
            decisionNote: decisionNote || "Rejected",
          },
          userId,
          runQuery: tx.query,
        });
      }
    }

    const request = await getOverrideRequestById({ tenantId, requestId, runQuery: tx.query });
    return { request, idempotent: false };
  });
}
