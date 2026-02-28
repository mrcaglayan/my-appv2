import crypto from "node:crypto";
import { query, withTransaction } from "../db.js";
import {
  assertAccountBelongsToTenant,
  assertCurrencyExists,
  assertLegalEntityBelongsToTenant,
} from "../tenantGuards.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";

const ACTIVE_BATCH_STATUSES = ["DRAFT", "APPROVED", "EXPORTED", "POSTED"];

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function toAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Number(parsed.toFixed(6));
}

function todayDateOnlyUtc() {
  return new Date().toISOString().slice(0, 10);
}

function sha256(value) {
  return crypto.createHash("sha256").update(String(value ?? ""), "utf8").digest("hex");
}

function parseOptionalJson(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  if (typeof value !== "string") {
    return value;
  }
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function isDuplicateEntryError(err) {
  return Number(err?.errno) === 1062 || normalizeUpperText(err?.code) === "ER_DUP_ENTRY";
}

function duplicateKeyName(err) {
  const message = String(err?.sqlMessage || err?.message || "");
  const keyMatch = message.match(/for key ['`"]([^'"`]+)['`"]/i);
  return keyMatch?.[1] || "";
}

function conflictError(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}

function forbiddenError(message) {
  const err = new Error(message);
  err.status = 403;
  return err;
}

function escapeCsvValue(value) {
  const text = String(value ?? "");
  if (/[",\n\r]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

async function countChildAccounts({ accountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT COUNT(*) AS total
     FROM accounts
     WHERE parent_account_id = ?`,
    [accountId]
  );
  return Number(result.rows?.[0]?.total || 0);
}

async function findBankAccountForPayments({ tenantId, bankAccountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        ba.id,
        ba.tenant_id,
        ba.legal_entity_id,
        ba.code,
        ba.name,
        ba.currency_code,
        ba.gl_account_id,
        ba.is_active,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM bank_accounts ba
     JOIN legal_entities le
       ON le.id = ba.legal_entity_id
      AND le.tenant_id = ba.tenant_id
     WHERE ba.tenant_id = ?
       AND ba.id = ?
     LIMIT 1`,
    [tenantId, bankAccountId]
  );
  return result.rows?.[0] || null;
}

async function findPaymentBatchScopeById({ tenantId, batchId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, legal_entity_id
     FROM payment_batches
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, batchId]
  );
  return result.rows?.[0] || null;
}

async function fetchPaymentPostingAccount({
  tenantId,
  legalEntityId,
  accountId,
  label = "payableGlAccountId",
  runQuery = query,
}) {
  await assertAccountBelongsToTenant(tenantId, accountId, label, { runQuery });

  const result = await runQuery(
    `SELECT
        a.id,
        a.code,
        a.name,
        a.account_type,
        a.normal_side,
        a.allow_posting,
        a.parent_account_id,
        a.is_active,
        c.scope AS coa_scope,
        c.legal_entity_id AS coa_legal_entity_id
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE a.id = ?
       AND c.tenant_id = ?
     LIMIT 1`,
    [accountId, tenantId]
  );
  const row = result.rows?.[0] || null;
  if (!row) {
    throw badRequest(`${label} not found for tenant`);
  }

  if (normalizeUpperText(row.coa_scope) !== "LEGAL_ENTITY") {
    throw badRequest(`${label} must belong to a LEGAL_ENTITY chart`);
  }
  if (parsePositiveInt(row.coa_legal_entity_id) !== parsePositiveInt(legalEntityId)) {
    throw badRequest(`${label} must belong to the same legal entity as the bank account`);
  }
  if (!parseDbBoolean(row.is_active)) {
    throw badRequest(`${label} must reference an ACTIVE account`);
  }
  if (!parseDbBoolean(row.allow_posting)) {
    throw badRequest(`${label} must reference a postable account`);
  }

  const childCount = await countChildAccounts({ accountId, runQuery });
  if (childCount > 0) {
    throw badRequest(`${label} must reference a leaf account`);
  }

  return row;
}

async function findPaymentBatchHeaderById({ tenantId, batchId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        pb.id,
        pb.tenant_id,
        pb.legal_entity_id,
        pb.batch_no,
        pb.source_type,
        pb.source_id,
        pb.bank_account_id,
        pb.currency_code,
        pb.total_amount,
        pb.status,
        pb.governance_approval_status,
        pb.governance_approval_request_id,
        pb.governance_approved_at,
        pb.governance_approved_by_user_id,
        pb.bank_file_format_code,
        pb.bank_export_status,
        pb.bank_ack_status,
        pb.idempotency_key,
        pb.last_export_file_name,
        pb.last_export_checksum,
        pb.posted_journal_entry_id,
        pb.notes,
        pb.created_by_user_id,
        pb.approved_by_user_id,
        pb.exported_by_user_id,
        pb.posted_by_user_id,
        pb.cancelled_by_user_id,
        pb.approved_at,
        pb.exported_at,
        pb.last_ack_imported_at,
        pb.posted_at,
        pb.cancelled_at,
        pb.created_at,
        pb.updated_at,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        ba.gl_account_id AS bank_gl_account_id,
        ba.currency_code AS bank_account_currency_code,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM payment_batches pb
     JOIN bank_accounts ba
       ON ba.id = pb.bank_account_id
      AND ba.tenant_id = pb.tenant_id
      AND ba.legal_entity_id = pb.legal_entity_id
     JOIN legal_entities le
       ON le.id = pb.legal_entity_id
      AND le.tenant_id = pb.tenant_id
     WHERE pb.tenant_id = ?
       AND pb.id = ?
     LIMIT 1`,
    [tenantId, batchId]
  );
  return result.rows?.[0] || null;
}

async function findPaymentBatchHeaderForUpdate({ tenantId, batchId, runQuery }) {
  const result = await runQuery(
    `SELECT
        id,
        tenant_id,
        legal_entity_id,
        batch_no,
        source_type,
        source_id,
        bank_account_id,
        currency_code,
        total_amount,
        status,
        bank_file_format_code,
        bank_export_status,
        bank_ack_status,
        idempotency_key,
        last_export_file_name,
        last_export_checksum,
        posted_journal_entry_id,
        notes,
        created_by_user_id,
        approved_by_user_id,
        exported_by_user_id,
        posted_by_user_id,
        cancelled_by_user_id,
        approved_at,
        exported_at,
        last_ack_imported_at,
        posted_at,
        cancelled_at,
        created_at,
        updated_at
     FROM payment_batches
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1
     FOR UPDATE`,
    [tenantId, batchId]
  );
  return result.rows?.[0] || null;
}

async function listPaymentBatchLinesByBatchId({ tenantId, batchId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        l.id,
        l.tenant_id,
        l.legal_entity_id,
        l.batch_id,
        l.line_no,
        l.beneficiary_type,
        l.beneficiary_id,
        l.beneficiary_name,
        l.beneficiary_bank_ref,
        l.payable_entity_type,
        l.payable_entity_id,
        l.payable_gl_account_id,
        a.code AS payable_gl_account_code,
        a.name AS payable_gl_account_name,
        l.payable_ref,
        l.amount,
        l.exported_amount,
        l.executed_amount,
        l.bank_execution_status,
        l.bank_reference,
        l.ack_status,
        l.return_status,
        l.returned_amount,
        l.return_reason_code,
        l.ack_code,
        l.ack_message,
        l.status,
        l.external_payment_ref,
        l.settlement_journal_line_ref,
        l.exported_at,
        l.acknowledged_at,
        l.last_returned_at,
        l.notes,
        pl.id AS payroll_payment_link_id,
        pl.beneficiary_bank_snapshot_id,
        pl.beneficiary_snapshot_status,
        s.account_holder_name AS snap_account_holder_name,
        s.bank_name AS snap_bank_name,
        s.iban AS snap_iban,
        s.account_number AS snap_account_number,
        s.routing_number AS snap_routing_number,
        s.swift_bic AS snap_swift_bic,
        s.account_last4 AS snap_account_last4,
        s.currency_code AS snap_currency_code,
        l.created_at,
        l.updated_at
     FROM payment_batch_lines l
     LEFT JOIN accounts a
       ON a.id = l.payable_gl_account_id
     LEFT JOIN payroll_liability_payment_links pl
       ON pl.tenant_id = l.tenant_id
      AND pl.legal_entity_id = l.legal_entity_id
      AND pl.payment_batch_id = l.batch_id
      AND pl.payment_batch_line_id = l.id
     LEFT JOIN payroll_beneficiary_bank_snapshots s
       ON s.tenant_id = pl.tenant_id
      AND s.legal_entity_id = pl.legal_entity_id
      AND s.id = pl.beneficiary_bank_snapshot_id
     WHERE l.tenant_id = ?
       AND l.batch_id = ?
     ORDER BY l.line_no ASC, l.id ASC`,
    [tenantId, batchId]
  );
  return result.rows || [];
}

async function listPaymentBatchExportsByBatchId({ tenantId, batchId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        e.id,
        e.tenant_id,
        e.legal_entity_id,
        e.batch_id,
        e.export_request_id,
        e.export_format,
        e.bank_file_format_code,
        e.export_status,
        e.file_name,
        e.file_checksum,
        e.export_payload_text,
        e.raw_meta_json,
        e.exported_by_user_id,
        e.exported_at,
        e.created_at
     FROM payment_batch_exports e
     WHERE e.tenant_id = ?
       AND e.batch_id = ?
     ORDER BY e.id DESC`,
    [tenantId, batchId]
  );
  return (result.rows || []).map((row) => ({
    ...row,
    raw_meta_json: parseOptionalJson(row.raw_meta_json),
  }));
}

async function listPaymentBatchAuditByBatchId({ tenantId, batchId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        a.id,
        a.tenant_id,
        a.legal_entity_id,
        a.batch_id,
        a.action,
        a.payload_json,
        a.acted_by_user_id,
        a.acted_at
     FROM payment_batch_audit a
     WHERE a.tenant_id = ?
       AND a.batch_id = ?
     ORDER BY a.id DESC`,
    [tenantId, batchId]
  );
  return (result.rows || []).map((row) => ({
    ...row,
    payload_json: parseOptionalJson(row.payload_json),
  }));
}

async function buildPaymentBatchDetail({ tenantId, batchId, runQuery = query }) {
  const header = await findPaymentBatchHeaderById({ tenantId, batchId, runQuery });
  if (!header) {
    return null;
  }
  const [lines, exports, audit] = await Promise.all([
    listPaymentBatchLinesByBatchId({ tenantId, batchId, runQuery }),
    listPaymentBatchExportsByBatchId({ tenantId, batchId, runQuery }),
    listPaymentBatchAuditByBatchId({ tenantId, batchId, runQuery }),
  ]);

  return {
    ...header,
    lines,
    exports,
    audit,
  };
}

async function writePaymentBatchAudit({
  tenantId,
  legalEntityId,
  batchId,
  action,
  payload = null,
  userId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO payment_batch_audit (
        tenant_id,
        legal_entity_id,
        batch_id,
        action,
        payload_json,
        acted_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, batchId, action, safeJson(payload), userId]
  );
}

async function findExistingBatchByIdempotencyKey({
  tenantId,
  legalEntityId,
  idempotencyKey,
  runQuery = query,
}) {
  if (!idempotencyKey) {
    return null;
  }
  const result = await runQuery(
    `SELECT id
     FROM payment_batches
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND idempotency_key = ?
     LIMIT 1`,
    [tenantId, legalEntityId, idempotencyKey]
  );
  return result.rows?.[0] || null;
}

async function assertNoDuplicatePayableTargets({
  tenantId,
  legalEntityId,
  lines,
  runQuery = query,
}) {
  for (const line of lines) {
    if (!parsePositiveInt(line.payableEntityId)) {
      continue;
    }

    // eslint-disable-next-line no-await-in-loop
    const result = await runQuery(
      `SELECT
          pb.batch_no,
          pb.status AS batch_status
       FROM payment_batch_lines pbl
       JOIN payment_batches pb
         ON pb.id = pbl.batch_id
        AND pb.tenant_id = pbl.tenant_id
        AND pb.legal_entity_id = pbl.legal_entity_id
       WHERE pbl.tenant_id = ?
         AND pbl.legal_entity_id = ?
         AND pbl.payable_entity_type = ?
         AND pbl.payable_entity_id = ?
         AND pb.status IN (${ACTIVE_BATCH_STATUSES.map(() => "?").join(", ")})
         AND pbl.status IN ('PENDING','PAID')
       LIMIT 1`,
      [
        tenantId,
        legalEntityId,
        line.payableEntityType,
        line.payableEntityId,
        ...ACTIVE_BATCH_STATUSES,
      ]
    );
    const duplicate = result.rows?.[0] || null;
    if (duplicate) {
      throw conflictError(
        `Payable target already exists in batch ${duplicate.batch_no} (status=${duplicate.batch_status})`
      );
    }
  }
}

async function nextPaymentBatchNo({ tenantId, legalEntityId, runQuery = query }) {
  const result = await runQuery(
    `SELECT COALESCE(COUNT(*), 0) + 1 AS next_seq
     FROM payment_batches
     WHERE tenant_id = ?
       AND legal_entity_id = ?`,
    [tenantId, legalEntityId]
  );
  const nextSeq = Number(result.rows?.[0]?.next_seq || 1);
  return `PB-${String(legalEntityId)}-${String(nextSeq).padStart(6, "0")}`;
}

function buildExportCsv(batch) {
  const header = [
    "batch_no",
    "line_no",
    "beneficiary_name",
    "beneficiary_bank_ref",
    "amount",
    "currency_code",
    "payable_entity_type",
    "payable_entity_id",
    "payable_ref",
    "external_payment_ref",
  ];
  const rows = [header.join(",")];

  for (const line of batch.lines || []) {
    const isPayrollLine = normalizeUpperText(line?.payable_entity_type) === "PAYROLL_LIABILITY";
    if (isPayrollLine && !parsePositiveInt(line?.beneficiary_bank_snapshot_id)) {
      throw conflictError(
        `Missing beneficiary snapshot for payroll payment batch line ${line?.id || "?"}`
      );
    }

    const exportBeneficiaryName =
      (isPayrollLine ? line?.snap_account_holder_name : null) || line.beneficiary_name;
    const exportBankRef =
      (isPayrollLine
        ? line?.snap_iban || line?.snap_account_number || line?.beneficiary_bank_ref
        : line?.beneficiary_bank_ref) || "";

    rows.push(
      [
        batch.batch_no,
        line.line_no,
        exportBeneficiaryName,
        exportBankRef,
        Number(line.amount || 0).toFixed(2),
        batch.currency_code,
        line.payable_entity_type,
        line.payable_entity_id || "",
        line.payable_ref || "",
        line.external_payment_ref || "",
      ]
        .map(escapeCsvValue)
        .join(",")
    );
  }

  return rows.join("\n");
}

async function getEffectivePeriodStatus(bookId, fiscalPeriodId, runQuery = query) {
  const result = await runQuery(
    `SELECT status
     FROM period_statuses
     WHERE book_id = ?
       AND fiscal_period_id = ?
     LIMIT 1`,
    [bookId, fiscalPeriodId]
  );
  return normalizeUpperText(result.rows?.[0]?.status || "OPEN") || "OPEN";
}

async function ensurePeriodOpen(bookId, fiscalPeriodId, actionLabel, runQuery = query) {
  const status = await getEffectivePeriodStatus(bookId, fiscalPeriodId, runQuery);
  if (status !== "OPEN") {
    throw badRequest(`Period is ${status}; cannot ${actionLabel}`);
  }
}

async function resolveBookAndPeriodForPaymentPostingTx(tx, { tenantId, legalEntityId, postDate }) {
  const bookResult = await tx.query(
    `SELECT id, calendar_id, code, name, base_currency_code, book_type
     FROM books
     WHERE tenant_id = ?
       AND legal_entity_id = ?
     ORDER BY
       CASE WHEN book_type = 'LOCAL' THEN 0 ELSE 1 END,
       id ASC
     LIMIT 1`,
    [tenantId, legalEntityId]
  );
  const book = bookResult.rows?.[0] || null;
  if (!book) {
    throw badRequest("No book found for payment batch legal entity");
  }

  const bookId = parsePositiveInt(book.id);
  const calendarId = parsePositiveInt(book.calendar_id);
  if (!bookId || !calendarId) {
    throw badRequest("Book configuration is invalid for payment batch posting");
  }

  const periodResult = await tx.query(
    `SELECT id, fiscal_year, period_no, period_name
     FROM fiscal_periods
     WHERE calendar_id = ?
       AND ? BETWEEN start_date AND end_date
     ORDER BY is_adjustment ASC, id ASC
     LIMIT 1`,
    [calendarId, postDate]
  );
  const period = periodResult.rows?.[0] || null;
  if (!period) {
    throw badRequest("No fiscal period found for payment batch posting date");
  }

  const fiscalPeriodId = parsePositiveInt(period.id);
  if (!fiscalPeriodId) {
    throw badRequest("Fiscal period configuration is invalid for payment batch posting");
  }

  await ensurePeriodOpen(bookId, fiscalPeriodId, "post payment batch", tx.query.bind(tx));

  return {
    book,
    period,
    bookId,
    fiscalPeriodId,
  };
}

async function createSettlementJournalTx(tx, payload) {
  const tenantId = parsePositiveInt(payload?.tenantId);
  const legalEntityId = parsePositiveInt(payload?.legalEntityId);
  const userId = parsePositiveInt(payload?.userId);
  const batch = payload?.batch || null;
  const postingDate = String(payload?.postingDate || "");
  const note = String(payload?.note || "").trim() || null;
  const externalPaymentRefPrefix = String(payload?.externalPaymentRefPrefix || "").trim() || null;

  if (!tenantId || !legalEntityId || !userId || !batch || !postingDate) {
    throw badRequest("Missing required payload for payment settlement journal posting");
  }

  const batchCurrency = normalizeUpperText(batch.currency_code);
  if (!batchCurrency || batchCurrency.length !== 3) {
    throw badRequest("Payment batch currency is invalid");
  }

  const pendingLines = Array.isArray(batch.lines)
    ? batch.lines.filter((line) => normalizeUpperText(line?.status) === "PENDING")
    : [];
  if (pendingLines.length === 0) {
    throw badRequest("No PENDING payment batch lines to post");
  }

  const total = Number(
    pendingLines.reduce((sum, line) => sum + toAmount(line?.amount || 0), 0).toFixed(6)
  );
  if (!(total > 0)) {
    throw badRequest("Payment batch total must be positive");
  }

  const journalContext = await resolveBookAndPeriodForPaymentPostingTx(tx, {
    tenantId,
    legalEntityId,
    postDate: postingDate,
  });

  const bookBaseCurrencyCode = normalizeUpperText(journalContext.book?.base_currency_code);
  if (bookBaseCurrencyCode && bookBaseCurrencyCode !== batchCurrency) {
    throw badRequest(
      `Batch currency (${batchCurrency}) must match book base currency (${bookBaseCurrencyCode})`
    );
  }

  const journalNo = `PAYB-${batch.id}`;
  const referenceNo = externalPaymentRefPrefix
    ? `${externalPaymentRefPrefix}-${batch.id}`
    : `PB-${batch.id}`;
  const description = note || `Payment batch settlement ${batch.batch_no}`;

  const headerInsert = await tx.query(
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
      tenantId,
      legalEntityId,
      journalContext.bookId,
      journalContext.fiscalPeriodId,
      journalNo,
      postingDate,
      postingDate,
      batchCurrency,
      description,
      referenceNo,
      total,
      total,
      userId,
      userId,
    ]
  );

  const journalEntryId = parsePositiveInt(headerInsert.rows?.insertId);
  if (!journalEntryId) {
    throw new Error("Failed to create payment settlement journal");
  }

  const lineRefByPaymentLineId = new Map();
  let journalLineNo = 1;
  for (const line of pendingLines) {
    const amount = toAmount(line.amount);
    const subledgerReferenceNo = `PAYBATCH:${batch.id}:L${line.line_no}`;
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
        VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, 0, NULL)`,
      [
        journalEntryId,
        journalLineNo,
        parsePositiveInt(line.payable_gl_account_id),
        `Settlement ${batch.batch_no} line ${line.line_no}`,
        subledgerReferenceNo,
        batchCurrency,
        amount,
        amount,
      ]
    );
    lineRefByPaymentLineId.set(parsePositiveInt(line.id), `JE:${journalEntryId}/L${journalLineNo}`);
    journalLineNo += 1;
  }

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
      VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, 0, ?, NULL)`,
    [
      journalEntryId,
      journalLineNo,
      parsePositiveInt(batch.bank_gl_account_id),
      `Settlement ${batch.batch_no} bank credit`,
      `PAYBATCH:${batch.id}`,
      batchCurrency,
      -total,
      total,
    ]
  );

  return {
    journalEntryId,
    journalNo,
    lineRefByPaymentLineId,
  };
}

async function assertAllPayableAccountsValid({
  tenantId,
  legalEntityId,
  lines,
  runQuery = query,
}) {
  const seen = new Set();
  for (const line of lines) {
    const accountId = parsePositiveInt(line.payableGlAccountId);
    if (!accountId || seen.has(accountId)) {
      continue;
    }
    seen.add(accountId);
    // eslint-disable-next-line no-await-in-loop
    await fetchPaymentPostingAccount({
      tenantId,
      legalEntityId,
      accountId,
      label: "lines[].payableGlAccountId",
      runQuery,
    });
  }
}

async function getBatchDetailOrThrow({ tenantId, batchId, runQuery = query }) {
  const row = await buildPaymentBatchDetail({ tenantId, batchId, runQuery });
  if (!row) {
    throw badRequest("Payment batch not found");
  }
  return row;
}

export async function resolvePaymentBatchScope(batchId, tenantId) {
  const parsedBatchId = parsePositiveInt(batchId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedBatchId || !parsedTenantId) {
    return null;
  }

  const row = await findPaymentBatchScopeById({
    tenantId: parsedTenantId,
    batchId: parsedBatchId,
  });
  if (!row) {
    return null;
  }

  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: parsePositiveInt(row.legal_entity_id),
  };
}

export async function listPaymentBatchRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["pb.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "pb.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("pb.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.bankAccountId) {
    const bankAccount = await findBankAccountForPayments({
      tenantId,
      bankAccountId: filters.bankAccountId,
    });
    if (!bankAccount) {
      throw badRequest("bankAccountId not found");
    }
    assertScopeAccess(req, "legal_entity", bankAccount.legal_entity_id, "bankAccountId");
    if (
      filters.legalEntityId &&
      parsePositiveInt(bankAccount.legal_entity_id) !== parsePositiveInt(filters.legalEntityId)
    ) {
      throw badRequest("bankAccountId does not belong to legalEntityId");
    }
    conditions.push("pb.bank_account_id = ?");
    params.push(filters.bankAccountId);
  }

  if (filters.status) {
    conditions.push("pb.status = ?");
    params.push(filters.status);
  }
  if (filters.sourceType) {
    conditions.push("pb.source_type = ?");
    params.push(filters.sourceType);
  }
  if (filters.sourceId) {
    conditions.push("pb.source_id = ?");
    params.push(filters.sourceId);
  }
  if (filters.q) {
    const like = `%${filters.q}%`;
    conditions.push("(pb.batch_no LIKE ? OR pb.notes LIKE ? OR ba.code LIKE ? OR ba.name LIKE ?)");
    params.push(like, like, like, like);
  }

  const whereSql = conditions.join(" AND ");

  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM payment_batches pb
     JOIN bank_accounts ba
       ON ba.id = pb.bank_account_id
      AND ba.tenant_id = pb.tenant_id
      AND ba.legal_entity_id = pb.legal_entity_id
     WHERE ${whereSql}`,
    params
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const safeLimit =
    Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset =
    Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;

  const listResult = await query(
    `SELECT
        pb.id,
        pb.tenant_id,
        pb.legal_entity_id,
        pb.batch_no,
        pb.source_type,
        pb.source_id,
        pb.bank_account_id,
        pb.currency_code,
        pb.total_amount,
        pb.status,
        pb.governance_approval_status,
        pb.governance_approval_request_id,
        pb.governance_approved_at,
        pb.governance_approved_by_user_id,
        pb.posted_journal_entry_id,
        pb.created_by_user_id,
        pb.approved_by_user_id,
        pb.exported_by_user_id,
        pb.posted_by_user_id,
        pb.approved_at,
        pb.exported_at,
        pb.posted_at,
        pb.created_at,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name,
        (SELECT COUNT(*) FROM payment_batch_lines pbl WHERE pbl.batch_id = pb.id AND pbl.tenant_id = pb.tenant_id) AS line_count,
        (SELECT COUNT(*) FROM payment_batch_lines pbl WHERE pbl.batch_id = pb.id AND pbl.tenant_id = pb.tenant_id AND pbl.status = 'PAID') AS paid_line_count,
        (SELECT COUNT(*) FROM payment_batch_lines pbl WHERE pbl.batch_id = pb.id AND pbl.tenant_id = pb.tenant_id AND pbl.status = 'PENDING') AS pending_line_count
     FROM payment_batches pb
     JOIN bank_accounts ba
       ON ba.id = pb.bank_account_id
      AND ba.tenant_id = pb.tenant_id
      AND ba.legal_entity_id = pb.legal_entity_id
     JOIN legal_entities le
       ON le.id = pb.legal_entity_id
      AND le.tenant_id = pb.tenant_id
     WHERE ${whereSql}
     ORDER BY pb.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: listResult.rows || [],
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getPaymentBatchDetailByIdForTenant({
  req,
  tenantId,
  batchId,
  assertScopeAccess,
}) {
  const row = await buildPaymentBatchDetail({ tenantId, batchId });
  if (!row) {
    throw badRequest("Payment batch not found");
  }
  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "batchId");
  return row;
}

export async function createPaymentBatch({
  req,
  payload,
  assertScopeAccess,
}) {
  const bankAccount = await findBankAccountForPayments({
    tenantId: payload.tenantId,
    bankAccountId: payload.bankAccountId,
  });
  if (!bankAccount) {
    throw badRequest("bankAccountId not found");
  }
  assertScopeAccess(req, "legal_entity", bankAccount.legal_entity_id, "bankAccountId");

  await assertLegalEntityBelongsToTenant(payload.tenantId, bankAccount.legal_entity_id, "legalEntityId");
  await assertCurrencyExists(payload.currencyCode, "currencyCode");

  if (!parseDbBoolean(bankAccount.is_active)) {
    throw badRequest("Cannot create payment batch on an inactive bank account");
  }

  if (normalizeUpperText(bankAccount.currency_code) !== normalizeUpperText(payload.currencyCode)) {
    throw badRequest(
      `Currency mismatch. bankAccount.currency_code=${bankAccount.currency_code} payload.currencyCode=${payload.currencyCode}`
    );
  }

  await assertAllPayableAccountsValid({
    tenantId: payload.tenantId,
    legalEntityId: bankAccount.legal_entity_id,
    lines: payload.lines,
  });

  try {
    const batchId = await withTransaction(async (tx) => {
      const existingByKey = await findExistingBatchByIdempotencyKey({
        tenantId: payload.tenantId,
        legalEntityId: bankAccount.legal_entity_id,
        idempotencyKey: payload.idempotencyKey,
        runQuery: tx.query,
      });
      if (existingByKey?.id) {
        return parsePositiveInt(existingByKey.id);
      }

      await assertNoDuplicatePayableTargets({
        tenantId: payload.tenantId,
        legalEntityId: bankAccount.legal_entity_id,
        lines: payload.lines,
        runQuery: tx.query,
      });

      const batchNo = await nextPaymentBatchNo({
        tenantId: payload.tenantId,
        legalEntityId: bankAccount.legal_entity_id,
        runQuery: tx.query,
      });
      const totalAmount = Number(
        payload.lines.reduce((sum, line) => sum + toAmount(line.amount), 0).toFixed(6)
      );

      const insertResult = await tx.query(
        `INSERT INTO payment_batches (
            tenant_id,
            legal_entity_id,
            batch_no,
            source_type,
            source_id,
            bank_account_id,
            currency_code,
            total_amount,
            status,
            idempotency_key,
            notes,
            created_by_user_id
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'DRAFT', ?, ?, ?)`,
        [
          payload.tenantId,
          bankAccount.legal_entity_id,
          batchNo,
          payload.sourceType,
          payload.sourceId,
          payload.bankAccountId,
          payload.currencyCode,
          totalAmount,
          payload.idempotencyKey,
          payload.notes,
          payload.userId,
        ]
      );
      const newBatchId = parsePositiveInt(insertResult.rows?.insertId);
      if (!newBatchId) {
        throw new Error("Failed to create payment batch");
      }

      let lineNo = 1;
      for (const line of payload.lines) {
        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO payment_batch_lines (
              tenant_id,
              legal_entity_id,
              batch_id,
              line_no,
              beneficiary_type,
              beneficiary_id,
              beneficiary_name,
              beneficiary_bank_ref,
              payable_entity_type,
              payable_entity_id,
              payable_gl_account_id,
              payable_ref,
              amount,
              status,
              notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)`,
          [
            payload.tenantId,
            bankAccount.legal_entity_id,
            newBatchId,
            lineNo,
            line.beneficiaryType,
            line.beneficiaryId,
            line.beneficiaryName,
            line.beneficiaryBankRef,
            line.payableEntityType,
            line.payableEntityId,
            line.payableGlAccountId,
            line.payableRef,
            line.amount,
            line.notes,
          ]
        );
        lineNo += 1;
      }

      await writePaymentBatchAudit({
        tenantId: payload.tenantId,
        legalEntityId: bankAccount.legal_entity_id,
        batchId: newBatchId,
        action: "CREATED",
        payload: {
          sourceType: payload.sourceType,
          sourceId: payload.sourceId,
          bankAccountId: payload.bankAccountId,
          currencyCode: payload.currencyCode,
          lineCount: payload.lines.length,
          totalAmount,
        },
        userId: payload.userId,
        runQuery: tx.query,
      });

      return newBatchId;
    });

    return getBatchDetailOrThrow({ tenantId: payload.tenantId, batchId });
  } catch (err) {
    if (isDuplicateEntryError(err)) {
      const keyName = duplicateKeyName(err);
      if (
        payload.idempotencyKey &&
        keyName.includes("uk_payment_batches_tenant_entity_idempotency")
      ) {
        const existing = await findExistingBatchByIdempotencyKey({
          tenantId: payload.tenantId,
          legalEntityId: bankAccount.legal_entity_id,
          idempotencyKey: payload.idempotencyKey,
        });
        if (existing?.id) {
          return getBatchDetailOrThrow({
            tenantId: payload.tenantId,
            batchId: parsePositiveInt(existing.id),
          });
        }
      }
      if (keyName.includes("uk_payment_batches_tenant_entity_batch_no")) {
        throw conflictError("Payment batch number collision; retry create");
      }
    }
    throw err;
  }
}

export async function approvePaymentBatch({
  req,
  tenantId,
  batchId,
  userId,
  approveInput,
  assertScopeAccess,
}) {
  await withTransaction(async (tx) => {
    const current = await findPaymentBatchHeaderForUpdate({
      tenantId,
      batchId,
      runQuery: tx.query,
    });
    if (!current) {
      throw badRequest("Payment batch not found");
    }

    assertScopeAccess(req, "legal_entity", current.legal_entity_id, "batchId");

    if (normalizeUpperText(current.status) !== "DRAFT") {
      throw badRequest("Only DRAFT batches can be approved");
    }

    await tx.query(
      `UPDATE payment_batches
       SET status = 'APPROVED',
           approved_by_user_id = ?,
           approved_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [userId, tenantId, batchId]
    );

    await writePaymentBatchAudit({
      tenantId,
      legalEntityId: current.legal_entity_id,
      batchId,
      action: "APPROVED",
      payload: {
        note: approveInput?.note || null,
        same_user_approval:
          parsePositiveInt(current.created_by_user_id) === parsePositiveInt(userId),
      },
      userId,
      runQuery: tx.query,
    });
  });

  return getPaymentBatchDetailByIdForTenant({
    req,
    tenantId,
    batchId,
    assertScopeAccess,
  });
}

export async function exportPaymentBatch({
  req,
  tenantId,
  batchId,
  userId,
  exportInput,
  assertScopeAccess,
}) {
  const format = normalizeUpperText(exportInput?.format || "CSV");
  if (format !== "CSV") {
    throw badRequest("Only CSV export is supported in v1");
  }

  let exportResult = null;
  await withTransaction(async (tx) => {
    const current = await findPaymentBatchHeaderForUpdate({
      tenantId,
      batchId,
      runQuery: tx.query,
    });
    if (!current) {
      throw badRequest("Payment batch not found");
    }
    assertScopeAccess(req, "legal_entity", current.legal_entity_id, "batchId");

    const currentStatus = normalizeUpperText(current.status);
    if (!["APPROVED", "EXPORTED"].includes(currentStatus)) {
      throw badRequest("Only APPROVED or EXPORTED batches can be exported");
    }

    const batchDetail = await buildPaymentBatchDetail({
      tenantId,
      batchId,
      runQuery: tx.query,
    });
    if (!batchDetail) {
      throw badRequest("Payment batch not found");
    }

    const csv = buildExportCsv(batchDetail);
    const checksum = sha256(csv);
    const fileName = `${batchDetail.batch_no}.csv`;
    const lineCount = Array.isArray(batchDetail.lines) ? batchDetail.lines.length : 0;

    await tx.query(
      `UPDATE payment_batches
       SET status = 'EXPORTED',
           last_export_file_name = ?,
           last_export_checksum = ?,
           exported_by_user_id = ?,
           exported_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [fileName, checksum, userId, tenantId, batchId]
    );

    const exportInsert = await tx.query(
      `INSERT INTO payment_batch_exports (
          tenant_id,
          legal_entity_id,
          batch_id,
          export_format,
          export_status,
          file_name,
          file_checksum,
          export_payload_text,
          raw_meta_json,
          exported_by_user_id
        )
        VALUES (?, ?, ?, ?, 'GENERATED', ?, ?, ?, ?, ?)`,
      [
        tenantId,
        current.legal_entity_id,
        batchId,
        format,
        fileName,
        checksum,
        csv,
        safeJson({ lineCount }),
        userId,
      ]
    );

    exportResult = {
      id: parsePositiveInt(exportInsert.rows?.insertId),
      format,
      file_name: fileName,
      checksum,
      csv,
    };

    await writePaymentBatchAudit({
      tenantId,
      legalEntityId: current.legal_entity_id,
      batchId,
      action: "EXPORTED",
      payload: {
        format,
        file_name: fileName,
        checksum,
        lineCount,
      },
      userId,
      runQuery: tx.query,
    });
  });

  const row = await getPaymentBatchDetailByIdForTenant({
    req,
    tenantId,
    batchId,
    assertScopeAccess,
  });

  return { row, export: exportResult };
}

export async function postPaymentBatch({
  req,
  tenantId,
  batchId,
  userId,
  postInput,
  assertScopeAccess,
}) {
  await withTransaction(async (tx) => {
    const current = await findPaymentBatchHeaderForUpdate({
      tenantId,
      batchId,
      runQuery: tx.query,
    });
    if (!current) {
      throw badRequest("Payment batch not found");
    }
    assertScopeAccess(req, "legal_entity", current.legal_entity_id, "batchId");

    if (
      normalizeUpperText(current.status) === "POSTED" &&
      parsePositiveInt(current.posted_journal_entry_id)
    ) {
      return;
    }

    if (!["APPROVED", "EXPORTED"].includes(normalizeUpperText(current.status))) {
      throw badRequest("Only APPROVED or EXPORTED batches can be posted");
    }

    const batchDetail = await buildPaymentBatchDetail({
      tenantId,
      batchId,
      runQuery: tx.query,
    });
    if (!batchDetail) {
      throw badRequest("Payment batch not found");
    }

    const postingDate = postInput?.postingDate || todayDateOnlyUtc();
    const journalResult = await createSettlementJournalTx(tx, {
      tenantId,
      legalEntityId: current.legal_entity_id,
      userId,
      batch: batchDetail,
      postingDate,
      note: postInput?.note || null,
      externalPaymentRefPrefix: postInput?.externalPaymentRefPrefix || null,
    });

    await tx.query(
      `UPDATE payment_batches
       SET status = 'POSTED',
           posted_journal_entry_id = ?,
           posted_by_user_id = ?,
           posted_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [journalResult.journalEntryId, userId, tenantId, batchId]
    );

    const prefix = postInput?.externalPaymentRefPrefix || `PB${batchId}`;
    const pendingLines = (batchDetail.lines || []).filter(
      (line) => normalizeUpperText(line?.status) === "PENDING"
    );
    for (const line of pendingLines) {
      const lineId = parsePositiveInt(line.id);
      if (!lineId) {
        continue;
      }
      const externalRef =
        line.external_payment_ref || `${prefix}-${String(line.line_no || lineId)}`;
      const settlementRef =
        journalResult.lineRefByPaymentLineId.get(lineId) || `JE:${journalResult.journalEntryId}`;
      // eslint-disable-next-line no-await-in-loop
      await tx.query(
        `UPDATE payment_batch_lines
         SET status = 'PAID',
             external_payment_ref = ?,
             settlement_journal_line_ref = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ?
           AND batch_id = ?
           AND id = ?
           AND status = 'PENDING'`,
        [externalRef, settlementRef, tenantId, batchId, lineId]
      );
    }

    await writePaymentBatchAudit({
      tenantId,
      legalEntityId: current.legal_entity_id,
      batchId,
      action: "POSTED",
      payload: {
        posted_journal_entry_id: journalResult.journalEntryId,
        journal_no: journalResult.journalNo,
        postingDate,
        note: postInput?.note || null,
      },
      userId,
      runQuery: tx.query,
    });
  });

  return getPaymentBatchDetailByIdForTenant({
    req,
    tenantId,
    batchId,
    assertScopeAccess,
  });
}

export async function cancelPaymentBatch({
  req,
  tenantId,
  batchId,
  userId,
  cancelInput,
  assertScopeAccess,
}) {
  await withTransaction(async (tx) => {
    const current = await findPaymentBatchHeaderForUpdate({
      tenantId,
      batchId,
      runQuery: tx.query,
    });
    if (!current) {
      throw badRequest("Payment batch not found");
    }
    assertScopeAccess(req, "legal_entity", current.legal_entity_id, "batchId");

    const currentStatus = normalizeUpperText(current.status);
    if (currentStatus === "POSTED") {
      throw badRequest("Posted payment batch cannot be cancelled");
    }
    if (currentStatus === "CANCELLED") {
      return;
    }

    await tx.query(
      `UPDATE payment_batches
       SET status = 'CANCELLED',
           cancelled_by_user_id = ?,
           cancelled_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [userId, tenantId, batchId]
    );

    await tx.query(
      `UPDATE payment_batch_lines
       SET status = 'CANCELLED',
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND batch_id = ?
         AND status = 'PENDING'`,
      [tenantId, batchId]
    );

    await writePaymentBatchAudit({
      tenantId,
      legalEntityId: current.legal_entity_id,
      batchId,
      action: "CANCELLED",
      payload: { reason: cancelInput?.reason || null },
      userId,
      runQuery: tx.query,
    });
  });

  return getPaymentBatchDetailByIdForTenant({
    req,
    tenantId,
    batchId,
    assertScopeAccess,
  });
}
