import crypto from "node:crypto";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { query, withTransaction } from "../db.js";
import {
  exportPaymentBatch,
  getPaymentBatchDetailByIdForTenant,
} from "./payments.service.js";
import { getBankPaymentFileFormat } from "./bankPaymentFileFormats/index.js";
import { ingestReturnEventsFromAckImportTx } from "./bank.paymentReturns.service.js";
import { evaluateBankApprovalNeed } from "./bank.governance.service.js";
import { submitBankApprovalRequest } from "./bank.approvals.service.js";

function up(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function sha256(value) {
  return crypto.createHash("sha256").update(String(value ?? ""), "utf8").digest("hex");
}

function toAmount(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Number(n.toFixed(6)) : 0;
}

function parseOptionalJson(value) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function notFound(message) {
  const err = new Error(message);
  err.status = 404;
  return err;
}

function conflict(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}

function isDuplicateEntryError(err) {
  return Number(err?.errno) === 1062 || up(err?.code) === "ER_DUP_ENTRY";
}

function toLineRef(batchId, lineId) {
  return `PB${batchId}-L${lineId}`;
}

function resolveExportBeneficiaryFields(batch, line) {
  const isPayrollLine = up(line?.payable_entity_type) === "PAYROLL_LIABILITY";
  const beneficiaryName =
    (isPayrollLine ? line?.snap_account_holder_name : null) || line?.beneficiary_name || null;
  const beneficiaryAccount =
    (isPayrollLine
      ? line?.snap_iban || line?.snap_account_number || line?.beneficiary_bank_ref
      : line?.beneficiary_bank_ref) || null;
  return {
    beneficiaryName,
    beneficiaryAccount,
    referenceText: line?.external_payment_ref || line?.payable_ref || null,
    currencyCode: up(batch?.currency_code || line?.snap_currency_code || ""),
  };
}

function assertBatchScope(req, assertScopeAccess, legalEntityId, label = "batchId") {
  if (typeof assertScopeAccess === "function" && parsePositiveInt(legalEntityId)) {
    assertScopeAccess(req, "legal_entity", parsePositiveInt(legalEntityId), label);
  }
}

async function findBatchHeader({ tenantId, batchId, runQuery = query, forUpdate = false }) {
  const result = await runQuery(
    `SELECT
        pb.id,
        pb.tenant_id,
        pb.legal_entity_id,
        pb.batch_no,
        pb.status,
        pb.total_amount,
        pb.bank_account_id,
        pb.currency_code,
        pb.bank_export_status,
        pb.bank_ack_status,
        pb.governance_approval_status,
        pb.governance_approval_request_id,
        pb.updated_at
     FROM payment_batches pb
     WHERE pb.tenant_id = ?
       AND pb.id = ?
     LIMIT 1
     ${forUpdate ? "FOR UPDATE" : ""}`,
    [tenantId, batchId]
  );
  return result.rows?.[0] || null;
}

async function findExportByRequestId({
  tenantId,
  batchId,
  exportRequestId,
  runQuery = query,
}) {
  if (!exportRequestId) return null;
  const result = await runQuery(
    `SELECT *
     FROM payment_batch_exports
     WHERE tenant_id = ?
       AND batch_id = ?
       AND export_request_id = ?
     LIMIT 1`,
    [tenantId, batchId, exportRequestId]
  );
  const row = result.rows?.[0] || null;
  return row ? { ...row, raw_meta_json: parseOptionalJson(row.raw_meta_json) } : null;
}

async function getLatestExportForBatch({ tenantId, batchId, runQuery = query }) {
  const result = await runQuery(
    `SELECT *
     FROM payment_batch_exports
     WHERE tenant_id = ?
       AND batch_id = ?
     ORDER BY id DESC
     LIMIT 1`,
    [tenantId, batchId]
  );
  const row = result.rows?.[0] || null;
  return row ? { ...row, raw_meta_json: parseOptionalJson(row.raw_meta_json) } : null;
}

async function getExportById({ tenantId, batchId, exportId, runQuery = query }) {
  const result = await runQuery(
    `SELECT *
     FROM payment_batch_exports
     WHERE tenant_id = ?
       AND batch_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, batchId, exportId]
  );
  const row = result.rows?.[0] || null;
  return row ? { ...row, raw_meta_json: parseOptionalJson(row.raw_meta_json) } : null;
}

async function listExportSnapshotsByExportId({
  tenantId,
  legalEntityId,
  exportId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
        x.id,
        x.tenant_id,
        x.legal_entity_id,
        x.payment_batch_export_id,
        x.payment_batch_line_id,
        x.line_ref,
        x.amount,
        x.currency_code,
        x.beneficiary_name,
        x.beneficiary_account,
        x.reference_text
     FROM payment_batch_export_lines x
     WHERE x.tenant_id = ?
       AND x.legal_entity_id = ?
       AND x.payment_batch_export_id = ?
     ORDER BY x.id ASC`,
    [tenantId, legalEntityId, exportId]
  );
  return result.rows || [];
}

function buildAckImportRowHash(row) {
  return sha256(
    JSON.stringify({
      line_ref: row.line_ref || null,
      batch_no: row.batch_no || null,
      line_no: Number.isInteger(row.line_no) ? row.line_no : null,
      ack_status: up(row.ack_status),
      ack_amount: row.ack_amount === null || row.ack_amount === undefined ? null : toAmount(row.ack_amount),
      bank_reference: row.bank_reference || null,
      ack_code: row.ack_code || null,
      ack_message: row.ack_message || null,
      executed_at: row.executed_at || null,
    })
  );
}

function mapAckToBankExecutionStatus(ackStatus, ackAmount, lineAmount) {
  const status = up(ackStatus);
  if (status === "REJECTED") return "REJECTED";
  if (status === "ACCEPTED") return "EXECUTED";
  if (status === "PARTIAL") return "PARTIALLY_PAID";
  if (status === "PAID") {
    return toAmount(ackAmount) >= toAmount(lineAmount) ? "PAID" : "PARTIALLY_PAID";
  }
  return "NONE";
}

function normalizeAckAmountForUpdate(ackStatus, ackAmount, lineAmount) {
  const status = up(ackStatus);
  if (status === "ACCEPTED" || status === "REJECTED") {
    return 0;
  }
  if (ackAmount === null || ackAmount === undefined) {
    if (status === "PAID") return toAmount(lineAmount);
    return 0;
  }
  return toAmount(ackAmount);
}

function computeBatchAckStatusFromAppliedRows(appliedRows = []) {
  const statuses = appliedRows.map((row) => up(row?.ack_status)).filter(Boolean);
  if (!statuses.length) return "NOT_ACKED";
  if (statuses.every((s) => s === "REJECTED")) return "REJECTED";
  if (statuses.every((s) => s === "ACCEPTED")) return "ACCEPTED";
  if (statuses.every((s) => s === "PAID")) return "PAID";
  return "PARTIAL";
}

function mapDbRowWithJson(row) {
  if (!row) return null;
  return { ...row, payload_json: parseOptionalJson(row.payload_json), raw_meta_json: parseOptionalJson(row.raw_meta_json) };
}

export async function listBatchExports({
  req,
  tenantId,
  batchId,
  assertScopeAccess,
}) {
  const batch = await findBatchHeader({ tenantId, batchId });
  if (!batch) throw notFound("Payment batch not found");
  assertBatchScope(req, assertScopeAccess, batch.legal_entity_id, "batchId");

  const result = await query(
    `SELECT
        e.*,
        (SELECT COUNT(*) FROM payment_batch_export_lines x
          WHERE x.tenant_id = e.tenant_id
            AND x.legal_entity_id = e.legal_entity_id
            AND x.payment_batch_export_id = e.id) AS snapshot_line_count
     FROM payment_batch_exports e
     WHERE e.tenant_id = ?
       AND e.batch_id = ?
     ORDER BY e.id DESC`,
    [tenantId, batchId]
  );

  return { items: (result.rows || []).map(mapDbRowWithJson) };
}

async function maybeRequestPaymentBatchExportApproval({
  req,
  tenantId,
  batchId,
  userId,
  batch,
  input,
  assertScopeAccess,
}) {
  if (input?._b09SkipApprovalGate) {
    return null;
  }
  if (!batch) return null;

  assertBatchScope(req, assertScopeAccess, batch.legal_entity_id, "batchId");

  const gov = await evaluateBankApprovalNeed({
    tenantId,
    targetType: "PAYMENT_BATCH",
    actionType: "SUBMIT_EXPORT",
    legalEntityId: batch.legal_entity_id,
    bankAccountId: batch.bank_account_id,
    thresholdAmount: batch.total_amount,
    currencyCode: batch.currency_code,
  });
  if (!gov?.approvalRequired && !gov?.approval_required) {
    return null;
  }

  const requestKey =
    (input?.b09ApprovalRequestKey && String(input.b09ApprovalRequestKey).trim()) ||
    (input?.exportRequestId && `B09:PBEXPORT:${tenantId}:${batchId}:${String(input.exportRequestId).trim()}`) ||
    `B09:PBEXPORT:${tenantId}:${batchId}:v${String(batch.updated_at || "")}:${up(
      input?.fileFormatCode || "GENERIC_CSV_V1"
    )}:${Boolean(input?.markSent)}`;

  const submitRes = await submitBankApprovalRequest({
    tenantId,
    userId,
    requestInput: {
      requestKey,
      targetType: "PAYMENT_BATCH",
      targetId: batchId,
      actionType: "SUBMIT_EXPORT",
      legalEntityId: batch.legal_entity_id,
      bankAccountId: batch.bank_account_id,
      thresholdAmount: batch.total_amount,
      currencyCode: batch.currency_code,
      actionPayload: {
        batchId,
        fileFormatCode: up(input?.fileFormatCode || "GENERIC_CSV_V1"),
        markSent: Boolean(input?.markSent),
        exportRequestId: input?.exportRequestId || null,
      },
    },
    snapshotBuilder: async () => ({
      batch_id: batch.id,
      batch_no: batch.batch_no,
      status: batch.status,
      legal_entity_id: parsePositiveInt(batch.legal_entity_id) || null,
      bank_account_id: parsePositiveInt(batch.bank_account_id) || null,
      total_amount: toAmount(batch.total_amount),
      currency_code: up(batch.currency_code || ""),
      bank_export_status: batch.bank_export_status || null,
      bank_ack_status: batch.bank_ack_status || null,
    }),
    policyOverride: gov,
  });

  const approvalRequest = submitRes?.item || null;
  if (!approvalRequest) {
    return null;
  }

  await query(
    `UPDATE payment_batches
     SET governance_approval_status = 'PENDING',
         governance_approval_request_id = ?,
         updated_at = CURRENT_TIMESTAMP
     WHERE tenant_id = ?
       AND id = ?`,
    [approvalRequest.id, tenantId, batchId]
  );

  const row = await getPaymentBatchDetailByIdForTenant({
    req,
    tenantId,
    batchId,
    assertScopeAccess,
  });

  return {
    approval_required: true,
    approvalRequired: true,
    approval_request: approvalRequest,
    row,
    idempotent: Boolean(submitRes?.idempotent),
  };
}

export async function exportPaymentBatchFile({
  req,
  tenantId,
  batchId,
  userId,
  input,
  assertScopeAccess,
}) {
  const formatCode = up(input?.fileFormatCode || "GENERIC_CSV_V1");
  getBankPaymentFileFormat(formatCode);

  const headerForGate = await findBatchHeader({ tenantId, batchId });
  if (!headerForGate) throw notFound("Payment batch not found");
  const approvalPending = await maybeRequestPaymentBatchExportApproval({
    req,
    tenantId,
    batchId,
    userId,
    batch: headerForGate,
    input: { ...input, fileFormatCode: formatCode },
    assertScopeAccess,
  });
  if (approvalPending) {
    return approvalPending;
  }

  const existing = await findExportByRequestId({
    tenantId,
    batchId,
    exportRequestId: input?.exportRequestId || null,
  });
  if (existing) {
    const row = await getPaymentBatchDetailByIdForTenant({ req, tenantId, batchId, assertScopeAccess });
    return { row, export: existing, idempotent: true };
  }

  const result = await exportPaymentBatch({
    req,
    tenantId,
    batchId,
    userId,
    exportInput: { format: "CSV" },
    assertScopeAccess,
  });

  const exportId = parsePositiveInt(result?.export?.id);
  if (!exportId) {
    throw new Error("B04 export did not return export record id");
  }

  await withTransaction(async (tx) => {
    const batch = await findBatchHeader({ tenantId, batchId, runQuery: tx.query, forUpdate: true });
    if (!batch) throw notFound("Payment batch not found");
    assertBatchScope(req, assertScopeAccess, batch.legal_entity_id, "batchId");

    const exportRow = await getExportById({ tenantId, batchId, exportId, runQuery: tx.query });
    if (!exportRow) throw notFound("Export record not found");

    const meta = typeof exportRow.raw_meta_json === "object" && exportRow.raw_meta_json ? exportRow.raw_meta_json : {};
    await tx.query(
      `UPDATE payment_batch_exports
       SET export_request_id = COALESCE(?, export_request_id),
           bank_file_format_code = ?,
           export_status = CASE WHEN ? THEN 'SENT' ELSE export_status END,
           raw_meta_json = ?
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?`,
      [
        input?.exportRequestId || null,
        formatCode,
        Boolean(input?.markSent),
        safeJson({
          ...meta,
          b06_bank_file_format_code: formatCode,
          b06_mark_sent: Boolean(input?.markSent),
        }),
        tenantId,
        batch.legal_entity_id,
        exportId,
      ]
    );

    for (const line of result?.row?.lines || []) {
      const lineId = parsePositiveInt(line?.id);
      if (!lineId) continue;
      const { beneficiaryName, beneficiaryAccount, referenceText, currencyCode } =
        resolveExportBeneficiaryFields(result.row, line);

      // eslint-disable-next-line no-await-in-loop
      await tx.query(
        `INSERT INTO payment_batch_export_lines (
            tenant_id,
            legal_entity_id,
            payment_batch_export_id,
            payment_batch_line_id,
            line_ref,
            beneficiary_name,
            beneficiary_account,
            amount,
            currency_code,
            reference_text
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            beneficiary_name = VALUES(beneficiary_name),
            beneficiary_account = VALUES(beneficiary_account),
            amount = VALUES(amount),
            currency_code = VALUES(currency_code),
            reference_text = VALUES(reference_text)`,
        [
          tenantId,
          batch.legal_entity_id,
          exportId,
          lineId,
          toLineRef(batchId, lineId),
          beneficiaryName,
          beneficiaryAccount,
          toAmount(line.amount),
          currencyCode || up(result?.row?.currency_code || ""),
          referenceText,
        ]
      );
    }

    await tx.query(
      `UPDATE payment_batches
       SET bank_file_format_code = ?,
           bank_export_status = CASE WHEN ? THEN 'SENT' ELSE 'EXPORTED' END
       WHERE tenant_id = ? AND id = ?`,
      [formatCode, Boolean(input?.markSent), tenantId, batchId]
    );

    await tx.query(
      `UPDATE payment_batch_lines
       SET exported_amount = CASE WHEN amount > exported_amount THEN amount ELSE exported_amount END,
           bank_execution_status = CASE
             WHEN bank_execution_status IN ('PAID','PARTIALLY_PAID','REJECTED') THEN bank_execution_status
             ELSE 'EXPORTED'
           END,
           exported_at = COALESCE(exported_at, CURRENT_TIMESTAMP)
       WHERE tenant_id = ?
         AND batch_id = ?`,
      [tenantId, batchId]
    );

    await tx.query(
      `INSERT INTO payment_batch_audit (
          tenant_id, legal_entity_id, batch_id, action, payload_json, acted_by_user_id
        )
        VALUES (?, ?, ?, 'STATUS', ?, ?)`,
      [
        tenantId,
        batch.legal_entity_id,
        batchId,
        safeJson({
          event: "BANK_FILE_EXPORTED_B06",
          export_id: exportId,
          file_format_code: formatCode,
          export_request_id: input?.exportRequestId || null,
          mark_sent: Boolean(input?.markSent),
        }),
        userId,
      ]
    );
  });

  const row = await getPaymentBatchDetailByIdForTenant({ req, tenantId, batchId, assertScopeAccess });
  const exportRow = await getExportById({ tenantId, batchId, exportId });
  return {
    row,
    export: {
      ...exportRow,
      csv: result?.export?.csv || null,
    },
    idempotent: false,
  };
}

export async function executeApprovedPaymentBatchExportFile({
  tenantId,
  batchId,
  approvalRequestId,
  approvedByUserId,
  payload = {},
}) {
  const result = await exportPaymentBatchFile({
    req: null,
    tenantId,
    batchId,
    userId: approvedByUserId,
    input: {
      fileFormatCode: payload.fileFormatCode || payload.file_format_code || "GENERIC_CSV_V1",
      markSent: Boolean(payload.markSent ?? payload.mark_sent),
      exportRequestId: payload.exportRequestId || payload.export_request_id || null,
      _b09SkipApprovalGate: true,
    },
    assertScopeAccess: () => {},
  });

  await query(
    `UPDATE payment_batches
     SET governance_approval_status = 'APPROVED',
         governance_approval_request_id = ?,
         governance_approved_at = CURRENT_TIMESTAMP,
         governance_approved_by_user_id = ?
     WHERE tenant_id = ?
       AND id = ?`,
    [approvalRequestId || null, approvedByUserId || null, tenantId, batchId]
  );

  return {
    batch_id: batchId,
    approval_request_id: approvalRequestId || null,
    export_id: parsePositiveInt(result?.export?.id) || null,
    exported: true,
    idempotent: Boolean(result?.idempotent),
  };
}

export async function listBatchAckImports({
  req,
  tenantId,
  batchId,
  assertScopeAccess,
}) {
  const batch = await findBatchHeader({ tenantId, batchId });
  if (!batch) throw notFound("Payment batch not found");
  assertBatchScope(req, assertScopeAccess, batch.legal_entity_id, "batchId");

  const result = await query(
    `SELECT *
     FROM payment_batch_ack_imports
     WHERE tenant_id = ?
       AND batch_id = ?
     ORDER BY id DESC`,
    [tenantId, batchId]
  );

  return { items: (result.rows || []).map(mapDbRowWithJson) };
}

export async function importPaymentBatchAck({
  req,
  tenantId,
  batchId,
  userId,
  input,
  assertScopeAccess,
}) {
  const formatCode = up(input?.fileFormatCode || "GENERIC_CSV_V1");
  const format = getBankPaymentFileFormat(formatCode);

  if (input?.ackRequestId) {
    const existing = await query(
      `SELECT *
       FROM payment_batch_ack_imports
       WHERE tenant_id = ?
         AND batch_id = ?
         AND ack_request_id = ?
       LIMIT 1`,
      [tenantId, batchId, input.ackRequestId]
    );
    const row = existing.rows?.[0] || null;
    if (row) {
      const batchRow = await getPaymentBatchDetailByIdForTenant({
        req,
        tenantId,
        batchId,
        assertScopeAccess,
      });
      return { row: batchRow, ack_import: mapDbRowWithJson(row), idempotent: true };
    }
  }

  const parsed = format.parseAcknowledgement({ ackText: input?.ackText || "" });
  const fileSha = sha256(input?.ackText || "");
  let ackImportId = null;

  await withTransaction(async (tx) => {
    const batch = await findBatchHeader({ tenantId, batchId, runQuery: tx.query, forUpdate: true });
    if (!batch) throw notFound("Payment batch not found");
    assertBatchScope(req, assertScopeAccess, batch.legal_entity_id, "batchId");

    if (!["EXPORTED", "POSTED", "APPROVED"].includes(up(batch.status))) {
      throw badRequest(`Payment batch status ${batch.status} is not eligible for ack import`);
    }

    const exportRow = input?.exportId
      ? await getExportById({ tenantId, batchId, exportId: input.exportId, runQuery: tx.query })
      : await getLatestExportForBatch({ tenantId, batchId, runQuery: tx.query });

    const snapshots = exportRow
      ? await listExportSnapshotsByExportId({
          tenantId,
          legalEntityId: batch.legal_entity_id,
          exportId: exportRow.id,
          runQuery: tx.query,
        })
      : [];

    const linesRes = await tx.query(
      `SELECT
          id,
          line_no,
          amount,
          status,
          external_payment_ref,
          exported_amount,
          executed_amount,
          bank_execution_status,
          bank_reference,
          ack_status
       FROM payment_batch_lines
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND batch_id = ?
       ORDER BY line_no ASC, id ASC
       FOR UPDATE`,
      [tenantId, batch.legal_entity_id, batchId]
    );
    const lines = linesRes.rows || [];

    const lineById = new Map();
    const lineByBatchLineNo = new Map();
    for (const line of lines) {
      const lineId = parsePositiveInt(line.id);
      if (!lineId) continue;
      lineById.set(lineId, { ...line });
      lineByBatchLineNo.set(`${batch.batch_no}|${Number(line.line_no)}`, { ...line });
    }

    const lineByRef = new Map();
    for (const s of snapshots) {
      const lineId = parsePositiveInt(s.payment_batch_line_id);
      if (lineId && s.line_ref) {
        lineByRef.set(String(s.line_ref).trim(), { ...s, line: lineById.get(lineId) || null });
      }
    }
    // Fallback deterministic line refs even if snapshots absent.
    for (const line of lines) {
      const lineId = parsePositiveInt(line.id);
      if (!lineId) continue;
      const key = toLineRef(batchId, lineId);
      if (!lineByRef.has(key)) {
        lineByRef.set(key, { line_ref: key, payment_batch_line_id: lineId, line });
      }
    }

    const ackIns = await tx.query(
      `INSERT INTO payment_batch_ack_imports (
          tenant_id,
          legal_entity_id,
          batch_id,
          payment_batch_export_id,
          ack_request_id,
          file_format_code,
          file_name,
          file_sha256,
          status,
          created_by_user_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'APPLIED', ?)`,
      [
        tenantId,
        batch.legal_entity_id,
        batchId,
        parsePositiveInt(exportRow?.id) || null,
        input?.ackRequestId || null,
        formatCode,
        input?.fileName || null,
        fileSha,
        userId,
      ]
    );
    ackImportId = parsePositiveInt(ackIns.rows?.insertId);
    if (!ackImportId) throw new Error("Failed to create ack import record");

    let totalRows = 0;
    let appliedRows = 0;
    let duplicateRows = 0;
    let errorRows = 0;
    const appliedAckRowsForStatus = [];

    for (const row of parsed.rows || []) {
      totalRows += 1;

      const ackStatus = up(row.ack_status);
      if (!["ACCEPTED", "REJECTED", "PARTIAL", "PAID"].includes(ackStatus)) {
        errorRows += 1;
        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO payment_batch_ack_import_lines (
              tenant_id, legal_entity_id, ack_import_id, payment_batch_line_id, line_ref, bank_reference,
              ack_status, ack_code, ack_message, ack_amount, currency_code, executed_at, row_hash, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            tenantId,
            batch.legal_entity_id,
            ackImportId,
            null,
            row.line_ref || null,
            row.bank_reference || null,
            "INVALID",
            row.ack_code || "UNSUPPORTED_ACK_STATUS",
            row.ack_message || `Unsupported ack_status ${row.ack_status}`,
            row.ack_amount,
            row.currency_code || null,
            row.executed_at || null,
            buildAckImportRowHash({ ...row, ack_status: "INVALID" }),
            safeJson({ row_no: row.row_no, raw_row: row.raw_row }),
          ]
        );
        continue;
      }

      let target = null;
      if (row.line_ref) {
        target = lineByRef.get(String(row.line_ref).trim()) || null;
      }
      if (!target && row.batch_no && Number.isInteger(row.line_no)) {
        const line = lineByBatchLineNo.get(`${String(row.batch_no).trim()}|${Number(row.line_no)}`) || null;
        if (line) target = { payment_batch_line_id: line.id, line };
      }

      const targetLine = target?.line || null;
      const targetLineId = parsePositiveInt(target?.payment_batch_line_id ?? targetLine?.id);
      const rowHash = buildAckImportRowHash(row);

      if (!targetLineId || !targetLine) {
        errorRows += 1;
        try {
          // eslint-disable-next-line no-await-in-loop
          await tx.query(
            `INSERT INTO payment_batch_ack_import_lines (
                tenant_id, legal_entity_id, ack_import_id, payment_batch_line_id, line_ref, bank_reference,
                ack_status, ack_code, ack_message, ack_amount, currency_code, executed_at, row_hash, payload_json
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              tenantId,
              batch.legal_entity_id,
              ackImportId,
              null,
              row.line_ref || null,
              row.bank_reference || null,
              ackStatus,
              row.ack_code || "LINE_NOT_FOUND",
              row.ack_message || "Payment batch line could not be matched",
              row.ack_amount,
              row.currency_code || null,
              row.executed_at || null,
              rowHash,
              safeJson({ row_no: row.row_no, raw_row: row.raw_row }),
            ]
          );
        } catch (err) {
          if (isDuplicateEntryError(err)) duplicateRows += 1;
          else throw err;
        }
        continue;
      }

      const lineAmount = toAmount(targetLine.amount);
      const normalizedAckAmount = normalizeAckAmountForUpdate(ackStatus, row.ack_amount, lineAmount);
      if (normalizedAckAmount > lineAmount + 0.000001) {
        errorRows += 1;
        try {
          // eslint-disable-next-line no-await-in-loop
          await tx.query(
            `INSERT INTO payment_batch_ack_import_lines (
                tenant_id, legal_entity_id, ack_import_id, payment_batch_line_id, line_ref, bank_reference,
                ack_status, ack_code, ack_message, ack_amount, currency_code, executed_at, row_hash, payload_json
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              tenantId,
              batch.legal_entity_id,
              ackImportId,
              targetLineId,
              row.line_ref || toLineRef(batchId, targetLineId),
              row.bank_reference || null,
              ackStatus,
              row.ack_code || "ACK_AMOUNT_OVER_LINE",
              row.ack_message || "ack_amount exceeds line amount",
              row.ack_amount,
              row.currency_code || null,
              row.executed_at || null,
              rowHash,
              safeJson({ row_no: row.row_no, raw_row: row.raw_row }),
            ]
          );
        } catch (err) {
          if (isDuplicateEntryError(err)) duplicateRows += 1;
          else throw err;
        }
        continue;
      }

      try {
        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO payment_batch_ack_import_lines (
              tenant_id, legal_entity_id, ack_import_id, payment_batch_line_id, line_ref, bank_reference,
              ack_status, ack_code, ack_message, ack_amount, currency_code, executed_at, row_hash, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            tenantId,
            batch.legal_entity_id,
            ackImportId,
            targetLineId,
            row.line_ref || toLineRef(batchId, targetLineId),
            row.bank_reference || null,
            ackStatus,
            row.ack_code || null,
            row.ack_message || null,
            row.ack_amount,
            row.currency_code || null,
            row.executed_at || null,
            rowHash,
            safeJson({ row_no: row.row_no, raw_row: row.raw_row }),
          ]
        );
      } catch (err) {
        if (isDuplicateEntryError(err)) {
          duplicateRows += 1;
          continue;
        }
        throw err;
      }

      const currentExecuted = toAmount(targetLine.executed_amount);
      const nextExecuted =
        ackStatus === "REJECTED" ? currentExecuted : Math.min(lineAmount, Math.max(currentExecuted, normalizedAckAmount));
      const bankExecStatus = mapAckToBankExecutionStatus(ackStatus, normalizedAckAmount, lineAmount);

      // eslint-disable-next-line no-await-in-loop
      await tx.query(
        `UPDATE payment_batch_lines
         SET executed_amount = ?,
             bank_execution_status = ?,
             bank_reference = COALESCE(?, bank_reference),
             ack_status = ?,
             ack_code = ?,
             ack_message = ?,
             acknowledged_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND id = ?`,
        [
          nextExecuted,
          bankExecStatus,
          row.bank_reference || null,
          ackStatus,
          row.ack_code || null,
          row.ack_message || null,
          tenantId,
          batch.legal_entity_id,
          targetLineId,
        ]
      );

      const lineUpdate = lineById.get(targetLineId);
      if (lineUpdate) {
        lineUpdate.executed_amount = nextExecuted;
        lineUpdate.bank_execution_status = bankExecStatus;
        lineUpdate.ack_status = ackStatus;
        lineUpdate.bank_reference = row.bank_reference || lineUpdate.bank_reference || null;
      }

      appliedRows += 1;
      appliedAckRowsForStatus.push({
        ack_status: ackStatus,
      });
    }

    const batchAckStatus = computeBatchAckStatusFromAppliedRows(appliedAckRowsForStatus);
    const ackImportStatus = errorRows > 0 && appliedRows > 0 ? "PARTIAL" : errorRows > 0 ? "FAILED" : "APPLIED";

    await tx.query(
      `UPDATE payment_batch_ack_imports
       SET status = ?,
           total_rows = ?,
           applied_rows = ?,
           duplicate_rows = ?,
           error_rows = ?,
           payload_json = ?
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?`,
      [
        ackImportStatus,
        totalRows,
        appliedRows,
        duplicateRows,
        errorRows,
        safeJson({ batch_ack_status_after: batchAckStatus }),
        tenantId,
        batch.legal_entity_id,
        ackImportId,
      ]
    );

    await tx.query(
      `UPDATE payment_batches
       SET bank_ack_status = ?,
           last_ack_imported_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [batchAckStatus, tenantId, batchId]
    );

    await tx.query(
      `INSERT INTO payment_batch_audit (
          tenant_id, legal_entity_id, batch_id, action, payload_json, acted_by_user_id
        )
        VALUES (?, ?, ?, 'STATUS', ?, ?)`,
      [
        tenantId,
        batch.legal_entity_id,
        batchId,
        safeJson({
          event: "BANK_ACK_IMPORTED_B06",
          ack_import_id: ackImportId,
          file_format_code: formatCode,
          applied_rows: appliedRows,
          duplicate_rows: duplicateRows,
          error_rows: errorRows,
          batch_ack_status_after: batchAckStatus,
        }),
        userId,
      ]
    );
  });

  const row = await getPaymentBatchDetailByIdForTenant({
    req,
    tenantId,
    batchId,
    assertScopeAccess,
  });

  const ackImportRes = await query(
    `SELECT *
     FROM payment_batch_ack_imports
     WHERE tenant_id = ?
       AND batch_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, batchId, ackImportId]
  );

  return {
    row,
    ack_import: mapDbRowWithJson(ackImportRes.rows?.[0] || null),
    idempotent: false,
  };
}

export default {
  listBatchExports,
  exportPaymentBatchFile,
  executeApprovedPaymentBatchExportFile,
  listBatchAckImports,
  importPaymentBatchAck,
};
