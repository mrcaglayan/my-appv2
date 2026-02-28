import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";

const VALID_SCOPES = new Set(["ALL", "NET_PAY", "STATUTORY"]);
const EPSILON = 0.000001;

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function normalizeScope(scope) {
  const normalized = normalizeUpperText(scope || "ALL");
  if (!VALID_SCOPES.has(normalized)) {
    throw badRequest("scope must be ALL, NET_PAY, or STATUTORY");
  }
  return normalized;
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

function toDateOnly(value) {
  if (!value) return null;
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return value.toISOString().slice(0, 10);
  }
  const text = String(value);
  if (/^\d{4}-\d{2}-\d{2}/.test(text)) {
    return text.slice(0, 10);
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
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
    if (Number.isNaN(parsed.getTime())) {
      return trimmed;
    }
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

function nowDateTimeString() {
  return new Date().toISOString().slice(0, 19).replace("T", " ");
}

function scopeSqlClause(scope, alias = "l") {
  const normalized = normalizeScope(scope);
  if (normalized === "NET_PAY") {
    return ` AND ${alias}.liability_group = 'EMPLOYEE_NET'`;
  }
  if (normalized === "STATUTORY") {
    return ` AND ${alias}.liability_group = 'STATUTORY'`;
  }
  return "";
}

function makeNotFound(message) {
  const err = new Error(message);
  err.status = 404;
  return err;
}

async function getRun(tenantId, runId, runQuery = query) {
  const result = await runQuery(
    `SELECT
        r.id, r.tenant_id, r.legal_entity_id, r.run_no, r.status, r.entity_code,
        r.provider_code, r.payroll_period, r.pay_date, r.currency_code,
        r.payment_sync_last_preview_at, r.payment_sync_last_applied_at,
        le.code AS legal_entity_code, le.name AS legal_entity_name
     FROM payroll_runs r
     JOIN legal_entities le ON le.id = r.legal_entity_id AND le.tenant_id = r.tenant_id
     WHERE r.tenant_id = ? AND r.id = ?
     LIMIT 1`,
    [tenantId, runId]
  );
  const row = result.rows?.[0] || null;
  if (!row) return null;
  return {
    ...row,
    payroll_period: toDateOnly(row.payroll_period),
    pay_date: toDateOnly(row.pay_date),
    payment_sync_last_preview_at: row.payment_sync_last_preview_at
      ? String(row.payment_sync_last_preview_at)
      : null,
    payment_sync_last_applied_at: row.payment_sync_last_applied_at
      ? String(row.payment_sync_last_applied_at)
      : null,
  };
}

async function getRunForUpdate(tenantId, runId, runQuery) {
  const result = await runQuery(
    `SELECT * FROM payroll_runs WHERE tenant_id = ? AND id = ? LIMIT 1 FOR UPDATE`,
    [tenantId, runId]
  );
  return result.rows?.[0] || null;
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

async function updateRunPaymentSyncPreviewTimestamp(tenantId, runId, runQuery = query) {
  await runQuery(
    `UPDATE payroll_runs
     SET payment_sync_last_preview_at = CURRENT_TIMESTAMP
     WHERE tenant_id = ? AND id = ?`,
    [tenantId, runId]
  );
}

async function updateRunPaymentSyncAppliedTimestamp(tenantId, runId, runQuery = query) {
  await runQuery(
    `UPDATE payroll_runs
     SET payment_sync_last_applied_at = CURRENT_TIMESTAMP
     WHERE tenant_id = ? AND id = ?`,
    [tenantId, runId]
  );
}

async function listSyncCandidates({
  tenantId,
  legalEntityId,
  runId,
  scope = "ALL",
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
        l.id AS payroll_liability_id,
        l.run_id,
        l.liability_type,
        l.liability_group,
        l.employee_code,
        l.employee_name,
        l.beneficiary_name,
        l.amount AS liability_amount,
        l.currency_code,
        l.status AS liability_status,
        l.settled_amount AS liability_settled_amount,
        l.outstanding_amount AS liability_outstanding_amount,
        l.reserved_payment_batch_id,
        l.paid_at,
        l.paid_payment_batch_id,
        l.paid_payment_batch_line_id,
        l.paid_bank_statement_line_id,

        pl.id AS link_id,
        pl.payment_batch_id,
        pl.payment_batch_line_id,
        pl.allocated_amount,
        pl.status AS link_status,
        pl.settled_amount AS link_settled_amount,
        pl.settled_at AS link_settled_at,
        pl.released_at AS link_released_at,
        pl.last_sync_at AS link_last_sync_at,
        pl.sync_note AS link_sync_note,

        pb.status AS payment_batch_status,
        pb.total_amount AS payment_batch_total_amount,
        pb.posted_at AS payment_batch_posted_at,
        pb.cancelled_at AS payment_batch_cancelled_at,

        pbl.status AS payment_batch_line_status,
        pbl.amount AS payment_batch_line_amount,
        pbl.external_payment_ref,
        pbl.updated_at AS payment_batch_line_updated_at,

        COALESCE(bme.active_match_count, 0) AS batch_bank_match_count,
        COALESCE(bme.active_matched_total, 0) AS batch_bank_matched_total,
        bme.latest_matched_at AS batch_bank_latest_matched_at,
        bme.sample_statement_line_id AS bank_statement_line_id
     FROM payroll_run_liabilities l
     JOIN payroll_liability_payment_links pl
       ON pl.tenant_id = l.tenant_id
      AND pl.legal_entity_id = l.legal_entity_id
      AND pl.run_id = l.run_id
      AND pl.payroll_liability_id = l.id
     JOIN payment_batches pb
       ON pb.tenant_id = pl.tenant_id
      AND pb.legal_entity_id = pl.legal_entity_id
      AND pb.id = pl.payment_batch_id
     LEFT JOIN payment_batch_lines pbl
       ON pbl.tenant_id = pl.tenant_id
      AND pbl.legal_entity_id = pl.legal_entity_id
      AND pbl.id = pl.payment_batch_line_id
     LEFT JOIN (
       SELECT
         m.tenant_id,
         m.legal_entity_id,
         m.matched_entity_id AS payment_batch_id,
         COUNT(*) AS active_match_count,
         COALESCE(SUM(m.matched_amount), 0) AS active_matched_total,
         MAX(m.matched_at) AS latest_matched_at,
         MIN(m.statement_line_id) AS sample_statement_line_id
       FROM bank_reconciliation_matches m
       WHERE m.status = 'ACTIVE'
         AND m.matched_entity_type = 'PAYMENT_BATCH'
       GROUP BY m.tenant_id, m.legal_entity_id, m.matched_entity_id
     ) bme
       ON bme.tenant_id = pl.tenant_id
      AND bme.legal_entity_id = pl.legal_entity_id
      AND bme.payment_batch_id = pl.payment_batch_id
     WHERE l.tenant_id = ?
       AND l.legal_entity_id = ?
       AND l.run_id = ?
       AND l.status IN ('IN_BATCH', 'PARTIALLY_PAID', 'PAID')
       AND pl.status IN ('LINKED', 'PARTIALLY_PAID', 'PAID')
       ${scopeSqlClause(scope, "l")}
       AND (
         (l.status IN ('IN_BATCH','PARTIALLY_PAID')
            AND (l.reserved_payment_batch_id IS NULL OR pl.payment_batch_id = l.reserved_payment_batch_id))
         OR
         (l.status = 'PAID' AND (l.paid_payment_batch_id IS NULL OR pl.payment_batch_id = l.paid_payment_batch_id))
       )
     ORDER BY l.id ASC, pl.id ASC`,
    [tenantId, legalEntityId, runId]
  );
  return result.rows || [];
}

function hasFullBatchBankEvidence(row) {
  const matchCount = Number(row?.batch_bank_match_count || 0);
  if (!Number.isFinite(matchCount) || matchCount <= 0) {
    return false;
  }
  const matchedTotal = toAmount(row?.batch_bank_matched_total);
  const batchTotal = toAmount(row?.payment_batch_total_amount);
  if (batchTotal <= EPSILON) {
    return matchedTotal > EPSILON;
  }
  return matchedTotal + EPSILON >= batchTotal;
}

function computeBatchEvidenceTargetSettled(row) {
  const allocated = toAmount(row.allocated_amount ?? row.liability_amount);
  const matchedTotal = toAmount(row.batch_bank_matched_total);
  const batchTotal = toAmount(row.payment_batch_total_amount);

  if (matchedTotal <= EPSILON) {
    return 0;
  }
  if (batchTotal <= EPSILON) {
    return allocated;
  }

  const ratio = Math.max(0, Math.min(1, matchedTotal / batchTotal));
  return toAmount(Math.min(allocated, allocated * ratio));
}

function classifySyncCandidate(row, { allowB04OnlySettlement = false } = {}) {
  const liabilityStatus = normalizeUpperText(row.liability_status);
  const batchStatus = normalizeUpperText(row.payment_batch_status);
  const lineStatus = normalizeUpperText(row.payment_batch_line_status);
  const allocatedAmount = toAmount(row.allocated_amount ?? row.liability_amount);
  const liabilityAmount = toAmount(row.liability_amount);
  const currentLinkSettled = toAmount(row.link_settled_amount);
  const currentLiabilitySettled = toAmount(
    row.liability_settled_amount ?? row.link_settled_amount
  );
  const currentLiabilityOutstanding = toAmount(
    row.liability_outstanding_amount ?? Math.max(0, liabilityAmount - currentLiabilitySettled)
  );

  const batchCancelled = ["CANCELLED", "FAILED"].includes(batchStatus);
  const lineCancelled = ["CANCELLED", "FAILED"].includes(lineStatus);
  const linePaid = ["PAID", "SETTLED", "EXECUTED"].includes(lineStatus);
  const batchBankEvidence = hasFullBatchBankEvidence(row);
  const batchBankMatchedTotal = toAmount(row.batch_bank_matched_total);

  if (["IN_BATCH", "PARTIALLY_PAID"].includes(liabilityStatus) && (batchCancelled || lineCancelled)) {
    if (currentLinkSettled > EPSILON || currentLiabilitySettled > EPSILON) {
      return {
        action: "EXCEPTION",
        amount: 0,
        deltaAmount: 0,
        targetSettledAmount: currentLinkSettled,
        currentSettledAmount: currentLinkSettled,
        currentOutstandingAmount: currentLiabilityOutstanding,
        reason: "payment_cancelled_after_partial_settlement",
      };
    }
    return {
      action: "RELEASE_TO_OPEN",
      amount: allocatedAmount,
      deltaAmount: 0,
      targetSettledAmount: 0,
      currentSettledAmount: currentLinkSettled,
      currentOutstandingAmount: currentLiabilityOutstanding,
      reason: batchCancelled ? "payment_batch_cancelled_or_failed" : "payment_batch_line_cancelled_or_failed",
    };
  }

  if (["IN_BATCH", "PARTIALLY_PAID"].includes(liabilityStatus)) {
    let targetSettledAmount = currentLinkSettled;
    let settlementSource = null;
    let bankStatementLineId = null;
    let settledAt = null;
    let reason = null;

    if (batchBankMatchedTotal > EPSILON) {
      const batchEvidenceTarget = computeBatchEvidenceTargetSettled(row);
      if (batchEvidenceTarget > targetSettledAmount + EPSILON) {
        targetSettledAmount = batchEvidenceTarget;
        settlementSource = "B03_RECON";
        bankStatementLineId = parsePositiveInt(row.bank_statement_line_id) || null;
        settledAt =
          toDateTimeString(row.batch_bank_latest_matched_at) ||
          toDateTimeString(row.payment_batch_line_updated_at) ||
          toDateTimeString(row.payment_batch_posted_at) ||
          nowDateTimeString();
        reason = batchBankEvidence
          ? "bank_reconciliation_match_for_payment_batch"
          : "partial_bank_reconciliation_for_payment_batch";
      }
    }

    if (allowB04OnlySettlement && linePaid && allocatedAmount > targetSettledAmount + EPSILON) {
      targetSettledAmount = allocatedAmount;
      settlementSource = "B04_ONLY";
      bankStatementLineId = null;
      settledAt =
        toDateTimeString(row.payment_batch_line_updated_at) ||
        toDateTimeString(row.payment_batch_posted_at) ||
        nowDateTimeString();
      reason = "payment_batch_line_paid_status_without_bank_reconciliation";
    }

    if (targetSettledAmount > currentLinkSettled + EPSILON) {
      const deltaAmount = toAmount(targetSettledAmount - currentLinkSettled);
      const action = targetSettledAmount + EPSILON >= allocatedAmount ? "MARK_PAID" : "MARK_PARTIAL";
      return {
        action,
        amount: deltaAmount,
        deltaAmount,
        targetSettledAmount,
        currentSettledAmount: currentLinkSettled,
        currentOutstandingAmount: currentLiabilityOutstanding,
        settlementSource,
        bankStatementLineId,
        settledAt,
        reason,
      };
    }
  }

  if (liabilityStatus === "PAID") {
    return {
      action: "NOOP",
      amount: 0,
      deltaAmount: 0,
      targetSettledAmount: currentLinkSettled,
      currentSettledAmount: currentLinkSettled,
      currentOutstandingAmount: currentLiabilityOutstanding,
      reason: "already_paid",
    };
  }

  if (
    ["IN_BATCH", "PARTIALLY_PAID"].includes(liabilityStatus) &&
    Number(row?.batch_bank_match_count || 0) > 0
  ) {
    return {
      action: "NOOP",
      amount: 0,
      deltaAmount: 0,
      targetSettledAmount: currentLinkSettled,
      currentSettledAmount: currentLinkSettled,
      currentOutstandingAmount: currentLiabilityOutstanding,
      reason: "bank_reconciliation_partial_or_insufficient_for_full_batch",
    };
  }

  return {
    action: "NOOP",
    amount: 0,
    deltaAmount: 0,
    targetSettledAmount: currentLinkSettled,
    currentSettledAmount: currentLinkSettled,
    currentOutstandingAmount: currentLiabilityOutstanding,
    reason: "no_new_settlement_or_release_evidence",
  };
}

function buildSettlementKey(candidate, verdict) {
  return [
    "PRPAYSYNC",
    `T:${candidate.tenant_id || "?"}`,
    `LE:${candidate.legal_entity_id || "?"}`,
    `RUN:${candidate.run_id}`,
    `L:${candidate.payroll_liability_id}`,
    `LINK:${candidate.link_id}`,
    `B:${candidate.payment_batch_id}`,
    `BL:${candidate.payment_batch_line_id || 0}`,
    `SRC:${verdict.settlementSource || "NA"}`,
  ].join("|");
}

function summarizePreviewItems(items) {
  const summary = {
    total_candidates: items.length,
    mark_partial_count: 0,
    mark_partial_amount: 0,
    mark_paid_count: 0,
    mark_paid_amount: 0,
    release_count: 0,
    release_amount: 0,
    exception_count: 0,
    exception_amount: 0,
    noop_count: 0,
    noop_amount: 0,
  };

  for (const item of items) {
    const amount = toAmount(item?.verdict?.amount);
    if (item?.verdict?.action === "MARK_PARTIAL") {
      summary.mark_partial_count += 1;
      summary.mark_partial_amount = toAmount(summary.mark_partial_amount + amount);
    } else if (item?.verdict?.action === "MARK_PAID") {
      summary.mark_paid_count += 1;
      summary.mark_paid_amount = toAmount(summary.mark_paid_amount + amount);
    } else if (item?.verdict?.action === "RELEASE_TO_OPEN") {
      summary.release_count += 1;
      summary.release_amount = toAmount(summary.release_amount + amount);
    } else if (item?.verdict?.action === "EXCEPTION") {
      summary.exception_count += 1;
      summary.exception_amount = toAmount(summary.exception_amount + amount);
    } else {
      summary.noop_count += 1;
      summary.noop_amount = toAmount(summary.noop_amount + amount);
    }
  }

  return summary;
}

async function buildPaymentSyncPreviewInternal({
  tenantId,
  runId,
  scope = "ALL",
  allowB04OnlySettlement = false,
  runQuery = query,
  updatePreviewTimestamp = false,
}) {
  const normalizedScope = normalizeScope(scope);
  const run = await getRun(tenantId, runId, runQuery);
  if (!run) {
    throw makeNotFound("Payroll run not found");
  }

  const rows = await listSyncCandidates({
    tenantId,
    legalEntityId: parsePositiveInt(run.legal_entity_id),
    runId,
    scope: normalizedScope,
    runQuery,
  });

  const items = rows.map((row) => {
    const verdict = classifySyncCandidate(row, { allowB04OnlySettlement });
    return {
      ...row,
      verdict,
    };
  });

  if (updatePreviewTimestamp) {
    await updateRunPaymentSyncPreviewTimestamp(tenantId, runId, runQuery);
  }

  return {
    run: {
      id: parsePositiveInt(run.id),
      run_no: run.run_no || null,
      status: normalizeUpperText(run.status),
      payroll_period: run.payroll_period || null,
      pay_date: run.pay_date || null,
      currency_code: normalizeUpperText(run.currency_code),
      legal_entity_id: parsePositiveInt(run.legal_entity_id),
      legal_entity_code: run.legal_entity_code || run.entity_code || null,
      legal_entity_name: run.legal_entity_name || null,
      payment_sync_last_preview_at: run.payment_sync_last_preview_at || null,
      payment_sync_last_applied_at: run.payment_sync_last_applied_at || null,
    },
    scope: normalizedScope,
    allow_b04_only_settlement: Boolean(allowB04OnlySettlement),
    summary: summarizePreviewItems(items),
    items,
  };
}

function ensureSyncEligibleRun(run) {
  const status = normalizeUpperText(run?.status);
  if (!run) {
    throw makeNotFound("Payroll run not found");
  }
  if (!["FINALIZED"].includes(status)) {
    throw badRequest("Payroll payment sync requires a FINALIZED payroll run");
  }
}

export async function getPayrollRunPaymentSyncPreview({
  req,
  tenantId,
  runId,
  scope = "ALL",
  allowB04OnlySettlement = false,
  assertScopeAccess,
}) {
  const run = await getRun(tenantId, runId);
  if (!run) {
    throw makeNotFound("Payroll run not found");
  }
  assertScopeAccess(req, "legal_entity", parsePositiveInt(run.legal_entity_id), "runId");
  ensureSyncEligibleRun(run);

  return buildPaymentSyncPreviewInternal({
    tenantId,
    runId,
    scope,
    allowB04OnlySettlement,
    updatePreviewTimestamp: true,
  });
}

export async function applyPayrollRunPaymentSync({
  req,
  tenantId,
  runId,
  userId,
  scope = "ALL",
  allowB04OnlySettlement = false,
  note = null,
  assertScopeAccess,
}) {
  const normalizedScope = normalizeScope(scope);

  const result = await withTransaction(async (tx) => {
    const lockedRun = await getRunForUpdate(tenantId, runId, tx.query);
    if (!lockedRun) {
      throw makeNotFound("Payroll run not found");
    }
    assertScopeAccess(req, "legal_entity", parsePositiveInt(lockedRun.legal_entity_id), "runId");
    ensureSyncEligibleRun(lockedRun);

    const preview = await buildPaymentSyncPreviewInternal({
      tenantId,
      runId,
      scope: normalizedScope,
      allowB04OnlySettlement,
      runQuery: tx.query,
      updatePreviewTimestamp: false,
    });

    let markPartialAppliedCount = 0;
    let markPartialAppliedAmount = 0;
    let markPaidAppliedCount = 0;
    let markPaidAppliedAmount = 0;
    let releasedAppliedCount = 0;
    let releasedAppliedAmount = 0;
    let exceptionCount = 0;

    for (const item of preview.items || []) {
      const verdict = item?.verdict || { action: "NOOP" };
      const liabilityId = parsePositiveInt(item.payroll_liability_id);
      const linkId = parsePositiveInt(item.link_id);
      const batchId = parsePositiveInt(item.payment_batch_id);
      const batchLineId = parsePositiveInt(item.payment_batch_line_id);

      if (!liabilityId || !linkId || !batchId) {
        continue;
      }

      if (verdict.action === "MARK_PARTIAL" || verdict.action === "MARK_PAID") {
        const settlementKey = buildSettlementKey(
          {
            ...item,
            tenant_id: tenantId,
            legal_entity_id: parsePositiveInt(lockedRun.legal_entity_id),
          },
          verdict
        );
        const settledAt = verdict.settledAt || nowDateTimeString();
        const targetSettledAmount = toAmount(
          verdict.targetSettledAmount ??
            verdict.target_settled_amount ??
            item.link_settled_amount ??
            0
        );
        const deltaAmount = toAmount(
          verdict.deltaAmount ?? verdict.delta_amount ?? verdict.amount ?? 0
        );
        const allocatedAmount = toAmount(item.allocated_amount ?? item.liability_amount);
        const liabilityAmount = toAmount(item.liability_amount);
        const liabilitySettledBefore = toAmount(
          item.liability_settled_amount ?? item.link_settled_amount ?? 0
        );
        const liabilitySettledAfter = toAmount(
          Math.min(liabilityAmount, liabilitySettledBefore + deltaAmount)
        );
        const liabilityOutstandingAfter = toAmount(
          Math.max(0, liabilityAmount - liabilitySettledAfter)
        );
        const linkStatus =
          targetSettledAmount + EPSILON >= allocatedAmount ? "PAID" : "PARTIALLY_PAID";
        const liabilityStatus =
          liabilityOutstandingAfter <= EPSILON ? "PAID" : "PARTIALLY_PAID";
        const bankStatementLineId = parsePositiveInt(verdict.bankStatementLineId) || null;

        await tx.query(
          `INSERT INTO payroll_liability_settlements (
              tenant_id, legal_entity_id, settlement_key, run_id,
              payroll_liability_id, payroll_liability_payment_link_id,
              payment_batch_id, payment_batch_line_id, bank_statement_line_id,
              settlement_source, settled_amount, currency_code, settled_at,
              payload_json, created_by_user_id
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             settled_amount = GREATEST(settled_amount, VALUES(settled_amount)),
             bank_statement_line_id = COALESCE(VALUES(bank_statement_line_id), bank_statement_line_id),
             settled_at = VALUES(settled_at),
             payload_json = VALUES(payload_json)`,
          [
            tenantId,
            parsePositiveInt(lockedRun.legal_entity_id),
            settlementKey,
            runId,
            liabilityId,
            linkId,
            batchId,
            batchLineId,
            bankStatementLineId,
            verdict.settlementSource || "B03_RECON",
            amountString(targetSettledAmount),
            normalizeUpperText(item.currency_code || lockedRun.currency_code),
            settledAt,
            safeJson({
              reason: verdict.reason || null,
              action: verdict.action,
              delta_amount: deltaAmount,
              target_settled_amount: targetSettledAmount,
              batch_bank_match_count: Number(item.batch_bank_match_count || 0),
              batch_bank_matched_total: toAmount(item.batch_bank_matched_total),
              payment_batch_total_amount: toAmount(item.payment_batch_total_amount),
            }),
            userId,
          ]
        );

        await tx.query(
          `UPDATE payroll_liability_payment_links
           SET status = ?,
               settled_amount = ?,
               settled_at = ?,
               last_sync_at = CURRENT_TIMESTAMP,
               sync_note = ?
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?`,
          [
            linkStatus,
            amountString(targetSettledAmount),
            settledAt,
            `P04:${verdict.reason || (verdict.action === "MARK_PAID" ? "mark_paid" : "mark_partial")}`,
            tenantId,
            parsePositiveInt(lockedRun.legal_entity_id),
            linkId,
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
               paid_bank_statement_line_id = CASE WHEN ? = 'PAID' THEN ? ELSE paid_bank_statement_line_id END,
               updated_at = CURRENT_TIMESTAMP
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?
             AND status IN ('IN_BATCH', 'PARTIALLY_PAID', 'PAID')`,
          [
            liabilityStatus,
            amountString(liabilitySettledAfter),
            amountString(liabilityOutstandingAfter),
            liabilityStatus,
            settledAt,
            liabilityStatus,
            batchId,
            liabilityStatus,
            batchLineId,
            liabilityStatus,
            bankStatementLineId,
            tenantId,
            parsePositiveInt(lockedRun.legal_entity_id),
            liabilityId,
          ]
        );

        await writeLiabilityAudit({
          tenantId,
          legalEntityId: parsePositiveInt(lockedRun.legal_entity_id),
          runId,
          liabilityId,
          action: liabilityStatus === "PAID" ? "SETTLED" : "PARTIALLY_SETTLED",
          payload: {
            settlementSource: verdict.settlementSource || "B03_RECON",
            paymentBatchId: batchId,
            paymentBatchLineId: batchLineId,
            bankStatementLineId,
            deltaAmount,
            targetSettledAmount,
            liabilitySettledAfter,
            liabilityOutstandingAfter,
            settledAt,
            linkStatus,
            liabilityStatus,
            reason: verdict.reason || null,
          },
          userId,
          runQuery: tx.query,
        });

        if (liabilityStatus === "PAID") {
          markPaidAppliedCount += 1;
          markPaidAppliedAmount = toAmount(markPaidAppliedAmount + deltaAmount);
        } else {
          markPartialAppliedCount += 1;
          markPartialAppliedAmount = toAmount(markPartialAppliedAmount + deltaAmount);
        }
        continue;
      }

      if (verdict.action === "EXCEPTION") {
        exceptionCount += 1;
        continue;
      }

      if (verdict.action === "RELEASE_TO_OPEN") {
        await tx.query(
          `UPDATE payroll_liability_payment_links
           SET status = 'RELEASED',
               settled_amount = 0,
               settled_at = NULL,
               released_at = CURRENT_TIMESTAMP,
               last_sync_at = CURRENT_TIMESTAMP,
               sync_note = ?
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?`,
          [
            `P04:${verdict.reason || "release_to_open"}`,
            tenantId,
            parsePositiveInt(lockedRun.legal_entity_id),
            linkId,
          ]
        );

        await tx.query(
          `UPDATE payroll_run_liabilities
           SET status = 'OPEN',
               settled_amount = 0,
               outstanding_amount = amount,
               reserved_payment_batch_id = NULL,
               paid_at = NULL,
               paid_payment_batch_id = NULL,
               paid_payment_batch_line_id = NULL,
               paid_bank_statement_line_id = NULL,
               updated_at = CURRENT_TIMESTAMP
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?
             AND status IN ('IN_BATCH','PARTIALLY_PAID')`,
          [tenantId, parsePositiveInt(lockedRun.legal_entity_id), liabilityId]
        );

        await writeLiabilityAudit({
          tenantId,
          legalEntityId: parsePositiveInt(lockedRun.legal_entity_id),
          runId,
          liabilityId,
          action: "RELEASED",
          payload: {
            paymentBatchId: batchId,
            paymentBatchLineId: batchLineId,
            amount: toAmount(verdict.amount),
            reason: verdict.reason || null,
          },
          userId,
          runQuery: tx.query,
        });

        releasedAppliedCount += 1;
        releasedAppliedAmount = toAmount(releasedAppliedAmount + toAmount(verdict.amount));
      }
    }

    await updateRunPaymentSyncAppliedTimestamp(tenantId, runId, tx.query);

    await writeLiabilityAudit({
      tenantId,
      legalEntityId: parsePositiveInt(lockedRun.legal_entity_id),
      runId,
      action: "PAYMENT_SYNC_APPLY",
      payload: {
        scope: normalizedScope,
        allowB04OnlySettlement: Boolean(allowB04OnlySettlement),
        note: note || null,
        previewSummary: preview.summary,
        applied: {
          markPartialCount: markPartialAppliedCount,
          markPartialAmount: markPartialAppliedAmount,
          markPaidCount: markPaidAppliedCount,
          markPaidAmount: markPaidAppliedAmount,
          releasedCount: releasedAppliedCount,
          releasedAmount: releasedAppliedAmount,
          exceptionCount,
        },
      },
      userId,
      runQuery: tx.query,
    });

    return {
      run: {
        id: parsePositiveInt(lockedRun.id),
        run_no: lockedRun.run_no || null,
        status: normalizeUpperText(lockedRun.status),
        legal_entity_id: parsePositiveInt(lockedRun.legal_entity_id),
      },
      scope: normalizedScope,
      allow_b04_only_settlement: Boolean(allowB04OnlySettlement),
      preview_summary: preview.summary,
      applied: {
        mark_partial_count: markPartialAppliedCount,
        mark_partial_amount: markPartialAppliedAmount,
        mark_paid_count: markPaidAppliedCount,
        mark_paid_amount: markPaidAppliedAmount,
        release_count: releasedAppliedCount,
        release_amount: releasedAppliedAmount,
        exception_count: exceptionCount,
      },
    };
  });

  return result;
}
