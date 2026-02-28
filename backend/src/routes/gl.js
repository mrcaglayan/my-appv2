import express from "express";
import crypto from "node:crypto";
import { query, withTransaction } from "../db.js";
import {
  assertScopeAccess,
  buildScopeFilter,
  requirePermission,
} from "../middleware/rbac.js";
import {
  assertAccountBelongsToTenant,
  assertBookBelongsToTenant,
  assertCoaBelongsToTenant,
  assertFiscalCalendarBelongsToTenant,
  assertFiscalPeriodBelongsToCalendar,
  assertLegalEntityBelongsToTenant,
} from "../tenantGuards.js";
import { recalculateShareholderOwnershipPctTx } from "../services/shareholderOwnership.js";
import {
  asyncHandler,
  assertRequiredFields,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";
import {
  registerGlReadJournalRoutes,
  registerGlReadTrialBalanceRoute,
} from "./gl.read.journal.routes.js";
import { registerGlReadCoreRoutes } from "./gl.read.routes.js";
import { registerGlWriteCoreRoutes } from "./gl.write.routes.js";
import { registerGlWriteJournalRoutes } from "./gl.write.journal.routes.js";
import { registerGlReclassificationRoutes } from "./gl.reclass.routes.js";
import { registerGlPeriodClosingRoutes } from "./gl.period-closing.routes.js";
import { registerGlPurposeMappingsRoutes } from "./gl.purpose-mappings.routes.js";

const router = express.Router();
const CLOSE_RUN_STATUSES = new Set(["IN_PROGRESS", "COMPLETED", "FAILED", "REOPENED"]);
const CLOSE_TARGET_STATUSES = new Set(["SOFT_CLOSED", "HARD_CLOSED"]);
const PERIOD_STATUSES = new Set(["OPEN", "SOFT_CLOSED", "HARD_CLOSED"]);
const JOURNAL_SOURCE_TYPES = new Set([
  "MANUAL",
  "SYSTEM",
  "INTERCOMPANY",
  "ELIMINATION",
  "ADJUSTMENT",
  "CASH",
]);
const RECLASS_ALLOCATION_MODES = new Set(["PERCENT", "AMOUNT"]);
const BALANCE_EPSILON = 0.0001;

function toAmount(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function roundAmount(value, scale = 6) {
  const factor = 10 ** scale;
  return Math.round(toAmount(value) * factor) / factor;
}

function normalizeReclassAllocationMode(value) {
  const mode = String(value || "PERCENT").toUpperCase();
  if (!RECLASS_ALLOCATION_MODES.has(mode)) {
    throw badRequest("allocationMode must be PERCENT or AMOUNT");
  }
  return mode;
}

function toBoolean(value, defaultValue = false) {
  if (value === undefined || value === null || value === "") {
    return Boolean(defaultValue);
  }
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["true", "1", "yes", "y", "on"].includes(normalized)) {
    return true;
  }
  if (["false", "0", "no", "n", "off"].includes(normalized)) {
    return false;
  }
  return Boolean(defaultValue);
}

function toIsoDate(value, fieldLabel = "date") {
  const toLocalYyyyMmDd = (date) =>
    `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(
      date.getDate()
    ).padStart(2, "0")}`;

  if (value === undefined || value === null || value === "") {
    throw badRequest(`${fieldLabel} is required`);
  }

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw badRequest(`${fieldLabel} must be a valid date`);
    }
    return toLocalYyyyMmDd(value);
  }

  const asString = String(value).trim();
  if (!asString) {
    throw badRequest(`${fieldLabel} must be a valid date`);
  }

  const yyyyMmDdMatch = asString.match(/^(\d{4}-\d{2}-\d{2})/);
  if (yyyyMmDdMatch?.[1]) {
    return yyyyMmDdMatch[1];
  }

  const parsed = new Date(asString);
  if (Number.isNaN(parsed.getTime())) {
    throw badRequest(`${fieldLabel} must be a valid date`);
  }
  return toLocalYyyyMmDd(parsed);
}

function toOptionalIsoDate(value, fieldLabel = "date") {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  return toIsoDate(value, fieldLabel);
}

function generateJournalNo() {
  const stamp = Date.now();
  const rand = Math.floor(Math.random() * 1000);
  return `JRN-${stamp}-${rand}`;
}

async function resolveScopeFromBookId(bookId, tenantId) {
  const parsedBookId = parsePositiveInt(bookId);
  if (!parsedBookId) {
    return { scopeType: "TENANT", scopeId: tenantId };
  }

  const result = await query(
    `SELECT legal_entity_id
     FROM books
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [parsedBookId, tenantId]
  );

  const legalEntityId = parsePositiveInt(result.rows[0]?.legal_entity_id);
  if (legalEntityId) {
    return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  }

  return { scopeType: "TENANT", scopeId: tenantId };
}

async function resolveScopeFromJournalId(journalId, tenantId) {
  const parsedJournalId = parsePositiveInt(journalId);
  if (!parsedJournalId) {
    return { scopeType: "TENANT", scopeId: tenantId };
  }

  const result = await query(
    `SELECT legal_entity_id
     FROM journal_entries
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [parsedJournalId, tenantId]
  );

  const legalEntityId = parsePositiveInt(result.rows[0]?.legal_entity_id);
  if (legalEntityId) {
    return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
  }

  return { scopeType: "TENANT", scopeId: tenantId };
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

  return String(result.rows[0]?.status || "OPEN").toUpperCase();
}

async function ensurePeriodOpen(bookId, fiscalPeriodId, actionLabel, runQuery = query) {
  const status = await getEffectivePeriodStatus(bookId, fiscalPeriodId, runQuery);
  if (status !== "OPEN") {
    throw badRequest(`Period is ${status}; cannot ${actionLabel}`);
  }
}

async function loadJournal(tenantId, journalId) {
  const result = await query(
    `SELECT id, tenant_id, legal_entity_id, book_id, fiscal_period_id, journal_no, source_type, status,
            entry_date, document_date, currency_code, description, reference_no,
            total_debit_base, total_credit_base, created_by_user_id, posted_by_user_id,
            posted_at, reversed_by_user_id, reversed_at, reverse_reason,
            reversal_journal_entry_id, intercompany_source_journal_entry_id, created_at, updated_at
     FROM journal_entries
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [journalId, tenantId]
  );
  return result.rows[0] || null;
}

async function applyShareholderCommitmentSyncForPostedJournalTx(tx, payload) {
  const tenantId = parsePositiveInt(payload?.tenantId);
  const journalEntryId = parsePositiveInt(payload?.journalEntryId);
  const createdByUserId = parsePositiveInt(payload?.createdByUserId);
  if (!tenantId || !journalEntryId) {
    return {
      journalEntryId: journalEntryId || null,
      applied: false,
      shareholderCount: 0,
      totalAmount: 0,
      skippedAlreadySyncedCount: 0,
    };
  }

  const journalResult = await tx.query(
    `SELECT id, legal_entity_id, currency_code
     FROM journal_entries
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [journalEntryId, tenantId]
  );
  const journal = journalResult.rows[0] || null;
  const legalEntityId = parsePositiveInt(journal?.legal_entity_id);
  if (!legalEntityId) {
    return {
      journalEntryId,
      applied: false,
      shareholderCount: 0,
      totalAmount: 0,
      skippedAlreadySyncedCount: 0,
    };
  }

  const currencyCode = String(journal?.currency_code || "USD")
    .trim()
    .toUpperCase();
  const lineResult = await tx.query(
    `SELECT account_id, debit_base, credit_base
     FROM journal_lines
     WHERE journal_entry_id = ?`,
    [journalEntryId]
  );
  const journalLines = Array.isArray(lineResult.rows) ? lineResult.rows : [];
  if (journalLines.length === 0) {
    return {
      journalEntryId,
      applied: false,
      shareholderCount: 0,
      totalAmount: 0,
      skippedAlreadySyncedCount: 0,
    };
  }

  const debitByAccountId = new Map();
  const creditByAccountId = new Map();
  for (const line of journalLines) {
    const accountId = parsePositiveInt(line.account_id);
    if (!accountId) {
      continue;
    }

    const debit = roundAmount(line.debit_base || 0);
    const credit = roundAmount(line.credit_base || 0);
    if (debit > BALANCE_EPSILON) {
      debitByAccountId.set(
        accountId,
        roundAmount((debitByAccountId.get(accountId) || 0) + debit)
      );
    }
    if (credit > BALANCE_EPSILON) {
      creditByAccountId.set(
        accountId,
        roundAmount((creditByAccountId.get(accountId) || 0) + credit)
      );
    }
  }

  if (debitByAccountId.size === 0 || creditByAccountId.size === 0) {
    return {
      journalEntryId,
      applied: false,
      shareholderCount: 0,
      totalAmount: 0,
      skippedAlreadySyncedCount: 0,
    };
  }

  const shareholderResult = await tx.query(
    `SELECT
       id,
       code,
       committed_capital,
       capital_sub_account_id,
       commitment_debit_sub_account_id
     FROM shareholders
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND capital_sub_account_id IS NOT NULL
       AND commitment_debit_sub_account_id IS NOT NULL
     FOR UPDATE`,
    [tenantId, legalEntityId]
  );
  const shareholders = Array.isArray(shareholderResult.rows)
    ? shareholderResult.rows
    : [];
  if (shareholders.length === 0) {
    return {
      journalEntryId,
      applied: false,
      shareholderCount: 0,
      totalAmount: 0,
      skippedAlreadySyncedCount: 0,
    };
  }

  const candidateRows = [];
  for (const shareholder of shareholders) {
    const shareholderId = parsePositiveInt(shareholder.id);
    const capitalSubAccountId = parsePositiveInt(shareholder.capital_sub_account_id);
    const commitmentDebitSubAccountId = parsePositiveInt(
      shareholder.commitment_debit_sub_account_id
    );
    if (!shareholderId || !capitalSubAccountId || !commitmentDebitSubAccountId) {
      continue;
    }

    const debitAmount = roundAmount(
      debitByAccountId.get(commitmentDebitSubAccountId) || 0
    );
    const creditAmount = roundAmount(creditByAccountId.get(capitalSubAccountId) || 0);
    const matchedAmount = roundAmount(Math.min(debitAmount, creditAmount));
    if (matchedAmount <= BALANCE_EPSILON) {
      continue;
    }

    candidateRows.push({
      shareholderId,
      shareholderCode: String(shareholder.code || "").trim(),
      matchedAmount,
    });
  }

  if (candidateRows.length === 0) {
    return {
      journalEntryId,
      applied: false,
      shareholderCount: 0,
      totalAmount: 0,
      skippedAlreadySyncedCount: 0,
    };
  }

  const placeholders = candidateRows.map(() => "?").join(", ");
  let existingAuditRows = [];
  try {
    const existingAuditResult = await tx.query(
      `SELECT shareholder_id, amount
       FROM shareholder_commitment_journal_entries
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND journal_entry_id = ?
         AND shareholder_id IN (${placeholders})
       FOR UPDATE`,
      [
        tenantId,
        legalEntityId,
        journalEntryId,
        ...candidateRows.map((row) => row.shareholderId),
      ]
    );
    existingAuditRows = Array.isArray(existingAuditResult.rows)
      ? existingAuditResult.rows
      : [];
  } catch (err) {
    if (Number(err?.errno) === 1146) {
      throw badRequest(
        "Setup required: shareholder commitment audit table is missing (run latest migrations)"
      );
    }
    throw err;
  }

  const existingAmountByShareholderId = new Map(
    existingAuditRows.map((row) => [
      parsePositiveInt(row.shareholder_id),
      roundAmount(row.amount || 0),
    ])
  );

  let appliedShareholderCount = 0;
  let totalAppliedAmount = 0;
  let skippedAlreadySyncedCount = 0;

  for (const row of candidateRows) {
    const alreadySyncedAmount = roundAmount(
      existingAmountByShareholderId.get(row.shareholderId) || 0
    );
    const amountToApply = roundAmount(row.matchedAmount - alreadySyncedAmount);
    if (amountToApply <= BALANCE_EPSILON) {
      skippedAlreadySyncedCount += 1;
      continue;
    }

    // eslint-disable-next-line no-await-in-loop
    await tx.query(
      `UPDATE shareholders
       SET committed_capital = ROUND(COALESCE(committed_capital, 0) + ?, 6),
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?
       LIMIT 1`,
      [amountToApply, tenantId, legalEntityId, row.shareholderId]
    );

    // eslint-disable-next-line no-await-in-loop
    await tx.query(
      `INSERT INTO shareholder_commitment_journal_entries (
          tenant_id,
          shareholder_id,
          legal_entity_id,
          journal_entry_id,
          line_group_key,
          amount,
          currency_code,
          created_by_user_id
       )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         amount = VALUES(amount),
         currency_code = VALUES(currency_code),
         created_by_user_id = VALUES(created_by_user_id)`,
      [
        tenantId,
        row.shareholderId,
        legalEntityId,
        journalEntryId,
        `GL_POST:${journalEntryId}:${row.shareholderCode || row.shareholderId}`.slice(
          0,
          100
        ),
        row.matchedAmount,
        currencyCode,
        createdByUserId || null,
      ]
    );

    appliedShareholderCount += 1;
    totalAppliedAmount = roundAmount(totalAppliedAmount + amountToApply);
  }

  if (appliedShareholderCount > 0) {
    await recalculateShareholderOwnershipPctTx(tx, tenantId, legalEntityId);
  }

  return {
    journalEntryId,
    applied: appliedShareholderCount > 0,
    shareholderCount: appliedShareholderCount,
    totalAmount: totalAppliedAmount,
    skippedAlreadySyncedCount,
  };
}

function isNearlyZero(value) {
  return Math.abs(Number(value || 0)) < BALANCE_EPSILON;
}

function normalizeCloseTargetStatus(value) {
  const status = String(value || "SOFT_CLOSED").toUpperCase();
  if (!CLOSE_TARGET_STATUSES.has(status)) {
    throw badRequest("closeStatus must be SOFT_CLOSED or HARD_CLOSED");
  }
  return status;
}

function parseJsonColumn(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  if (typeof value === "object") {
    return value;
  }
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function mapPeriodCloseRunRow(row) {
  if (!row) {
    return null;
  }

  return {
    id: parsePositiveInt(row.id),
    tenantId: parsePositiveInt(row.tenant_id),
    bookId: parsePositiveInt(row.book_id),
    bookCode: row.book_code || null,
    bookName: row.book_name || null,
    fiscalPeriodId: parsePositiveInt(row.fiscal_period_id),
    nextFiscalPeriodId: parsePositiveInt(row.next_fiscal_period_id),
    fiscalYear: row.fiscal_year === null ? null : Number(row.fiscal_year),
    periodNo: row.period_no === null ? null : Number(row.period_no),
    periodName: row.period_name || null,
    closeStatus: String(row.close_status || "").toUpperCase(),
    status: String(row.status || "").toUpperCase(),
    runHash: String(row.run_hash || ""),
    yearEndClosed: Boolean(row.year_end_closed),
    retainedEarningsAccountId: parsePositiveInt(row.retained_earnings_account_id),
    carryForwardJournalEntryId: parsePositiveInt(row.carry_forward_journal_entry_id),
    yearEndJournalEntryId: parsePositiveInt(row.year_end_journal_entry_id),
    sourceJournalCount: Number(row.source_journal_count || 0),
    sourceDebitTotal: Number(row.source_debit_total || 0),
    sourceCreditTotal: Number(row.source_credit_total || 0),
    startedByUserId: parsePositiveInt(row.started_by_user_id),
    completedByUserId: parsePositiveInt(row.completed_by_user_id),
    reopenedByUserId: parsePositiveInt(row.reopened_by_user_id),
    startedAt: row.started_at || null,
    completedAt: row.completed_at || null,
    reopenedAt: row.reopened_at || null,
    note: row.note || null,
    metadata: parseJsonColumn(row.metadata_json),
  };
}

function buildSystemJournalNo(prefix, scopeId) {
  const rand = Math.floor(Math.random() * 1_679_616)
    .toString(36)
    .padStart(4, "0")
    .toUpperCase();
  const stamp = Date.now().toString(36).toUpperCase();
  return `${String(prefix).toUpperCase()}-${String(scopeId).toUpperCase()}-${stamp}-${rand}`.slice(
    0,
    40
  );
}

function computeCloseRunHash(payload) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(payload))
    .digest("hex");
}

async function writeAuditLog(runQuery, req, event) {
  const tenantId = parsePositiveInt(event.tenantId);
  if (!tenantId) {
    return;
  }

  const userId = parsePositiveInt(event.userId);
  const scopeType = event.scopeType ? String(event.scopeType).toUpperCase() : null;
  const scopeId = parsePositiveInt(event.scopeId);

  const forwardedFor = req.headers["x-forwarded-for"];
  const forwardedIp = Array.isArray(forwardedFor)
    ? forwardedFor[0]
    : String(forwardedFor || "")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean)[0];

  const ipAddress = forwardedIp || req.ip || req.socket?.remoteAddress || null;
  const userAgent = req.headers["user-agent"] || null;

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
      String(event.action || "gl.period_close"),
      String(event.resourceType || "period_close_run"),
      event.resourceId ? String(event.resourceId) : null,
      scopeType,
      scopeId || null,
      req.headers["x-request-id"] ? String(req.headers["x-request-id"]) : null,
      ipAddress,
      userAgent ? String(userAgent).slice(0, 255) : null,
      event.payload ? JSON.stringify(event.payload) : null,
    ]
  );
}

async function getFiscalPeriodDetails(periodId, runQuery = query) {
  const result = await runQuery(
    `SELECT
       id,
       calendar_id,
       fiscal_year,
       period_no,
       period_name,
       start_date,
       end_date,
       is_adjustment
     FROM fiscal_periods
     WHERE id = ?
     LIMIT 1`,
    [periodId]
  );
  return result.rows[0] || null;
}

async function findNextFiscalPeriod(calendarId, periodEndDate, runQuery = query) {
  const result = await runQuery(
    `SELECT
       id,
       calendar_id,
       fiscal_year,
       period_no,
       period_name,
       start_date,
       end_date,
       is_adjustment
     FROM fiscal_periods
     WHERE calendar_id = ?
       AND is_adjustment = FALSE
       AND start_date > ?
     ORDER BY start_date ASC, id ASC
     LIMIT 1`,
    [calendarId, periodEndDate]
  );
  return result.rows[0] || null;
}

async function getPeriodSourceFingerprint(
  tenantId,
  bookId,
  fiscalPeriodId,
  runQuery = query
) {
  const result = await runQuery(
    `SELECT
       COUNT(*) AS source_journal_count,
       COALESCE(SUM(total_debit_base), 0) AS source_debit_total,
       COALESCE(SUM(total_credit_base), 0) AS source_credit_total,
       COALESCE(MAX(updated_at), '1970-01-01 00:00:00') AS source_last_updated_at
     FROM journal_entries
     WHERE tenant_id = ?
       AND book_id = ?
       AND fiscal_period_id = ?
       AND status = 'POSTED'
       AND (reference_no IS NULL OR reference_no NOT LIKE 'PERIOD_CLOSE_RUN:%')`,
    [tenantId, bookId, fiscalPeriodId]
  );

  const row = result.rows[0] || {};
  return {
    sourceJournalCount: Number(row.source_journal_count || 0),
    sourceDebitTotal: Number(row.source_debit_total || 0),
    sourceCreditTotal: Number(row.source_credit_total || 0),
    sourceLastUpdatedAt: row.source_last_updated_at || null,
  };
}

async function getPostedPeriodAccountBalances(
  tenantId,
  bookId,
  fiscalPeriodId,
  runQuery = query
) {
  const result = await runQuery(
    `SELECT
       jl.account_id,
       a.code AS account_code,
       a.name AS account_name,
       a.account_type,
       c.legal_entity_id,
       SUM(jl.debit_base) AS debit_total,
       SUM(jl.credit_base) AS credit_total,
       SUM(jl.debit_base - jl.credit_base) AS closing_balance
     FROM journal_entries je
     JOIN journal_lines jl ON jl.journal_entry_id = je.id
     JOIN accounts a ON a.id = jl.account_id
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE je.tenant_id = ?
       AND je.book_id = ?
       AND je.fiscal_period_id = ?
       AND je.status = 'POSTED'
       AND (je.reference_no IS NULL OR je.reference_no NOT LIKE 'PERIOD_CLOSE_RUN:%')
       AND c.tenant_id = ?
     GROUP BY jl.account_id, a.code, a.name, a.account_type, c.legal_entity_id
     HAVING ABS(SUM(jl.debit_base - jl.credit_base)) >= ?
     ORDER BY a.code, jl.account_id`,
    [tenantId, bookId, fiscalPeriodId, tenantId, BALANCE_EPSILON]
  );

  return result.rows || [];
}

function buildYearEndCloseLine(balanceRow) {
  const closingBalance = Number(balanceRow.closing_balance || 0);
  if (isNearlyZero(closingBalance)) {
    return null;
  }

  if (closingBalance > 0) {
    return {
      accountId: parsePositiveInt(balanceRow.account_id),
      closingBalance,
      debitBase: 0,
      creditBase: closingBalance,
      description: `Year-end close (${String(balanceRow.account_code || "").trim()})`,
    };
  }

  return {
    accountId: parsePositiveInt(balanceRow.account_id),
    closingBalance,
    debitBase: Math.abs(closingBalance),
    creditBase: 0,
    description: `Year-end close (${String(balanceRow.account_code || "").trim()})`,
  };
}

async function createSystemJournalWithLines(tx, payload) {
  const lines = Array.isArray(payload.lines) ? payload.lines : [];
  if (lines.length === 0) {
    return null;
  }

  const entryDate = toIsoDate(payload.entryDate, "entryDate");
  const documentDate = toIsoDate(payload.documentDate, "documentDate");

  let totalDebitBase = 0;
  let totalCreditBase = 0;
  for (const line of lines) {
    totalDebitBase += Number(line.debitBase || 0);
    totalCreditBase += Number(line.creditBase || 0);
  }
  if (Math.abs(totalDebitBase - totalCreditBase) > BALANCE_EPSILON) {
    throw badRequest("System-generated journal is not balanced");
  }

  const entryResult = await tx.query(
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
      entryDate,
      documentDate,
      payload.currencyCode,
      payload.description || null,
      payload.referenceNo || null,
      totalDebitBase,
      totalCreditBase,
      payload.userId,
      payload.userId,
    ]
  );

  const journalEntryId = parsePositiveInt(entryResult.rows.insertId);
  if (!journalEntryId) {
    throw badRequest("Failed to create system journal entry");
  }

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const debitBase = Number(line.debitBase || 0);
    const creditBase = Number(line.creditBase || 0);
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
       VALUES (?, ?, ?, NULL, NULL, ?, NULL, ?, ?, ?, ?, NULL)`,
      [
        journalEntryId,
        i + 1,
        parsePositiveInt(line.accountId),
        line.description ? String(line.description) : null,
        payload.currencyCode,
        debitBase - creditBase,
        debitBase,
        creditBase,
      ]
    );
  }

  return {
    journalEntryId,
    totalDebitBase,
    totalCreditBase,
    lineCount: lines.length,
  };
}

async function reversePostedJournalWithinTransaction(tx, params) {
  const journalId = parsePositiveInt(params.journalId);
  if (!journalId) {
    return null;
  }

  const originalResult = await tx.query(
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
       total_debit_base,
       total_credit_base,
       reversal_journal_entry_id
     FROM journal_entries
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1
     FOR UPDATE`,
    [journalId, params.tenantId]
  );
  const original = originalResult.rows[0];
  if (!original) {
    return null;
  }

  const existingReversalId = parsePositiveInt(original.reversal_journal_entry_id);
  if (String(original.status || "").toUpperCase() === "REVERSED" && existingReversalId) {
    return existingReversalId;
  }

  if (String(original.status || "").toUpperCase() !== "POSTED") {
    throw badRequest(`Journal ${journalId} is not POSTED and cannot be auto-reversed`);
  }

  const lineResult = await tx.query(
    `SELECT
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
     ORDER BY line_no`,
    [journalId]
  );
  const lines = lineResult.rows || [];
  if (lines.length === 0) {
    throw badRequest(`Journal ${journalId} has no lines to auto-reverse`);
  }

  const reversalNo = buildSystemJournalNo("REV", journalId);
  const reason = params.reason || "Period close reopen";

  const reversalResult = await tx.query(
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
        posted_at,
        reverse_reason
     )
     VALUES (?, ?, ?, ?, ?, ?, 'POSTED', ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)`,
    [
      params.tenantId,
      parsePositiveInt(original.legal_entity_id),
      parsePositiveInt(original.book_id),
      parsePositiveInt(original.fiscal_period_id),
      reversalNo,
      String(original.source_type || "SYSTEM").toUpperCase(),
      toIsoDate(original.entry_date, "entry_date"),
      toIsoDate(original.document_date, "document_date"),
      String(original.currency_code || params.currencyCode || "USD").toUpperCase(),
      `Auto-reversal of ${original.journal_no}`,
      original.reference_no ? String(original.reference_no) : null,
      Number(original.total_credit_base || 0),
      Number(original.total_debit_base || 0),
      params.userId,
      params.userId,
      reason,
    ]
  );

  const reversalJournalId = parsePositiveInt(reversalResult.rows.insertId);
  if (!reversalJournalId) {
    throw badRequest(`Failed to create reversal for journal ${journalId}`);
  }

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
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
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        reversalJournalId,
        i + 1,
        parsePositiveInt(line.account_id),
        parsePositiveInt(line.operating_unit_id),
        parsePositiveInt(line.counterparty_legal_entity_id),
        line.description ? String(line.description) : null,
        normalizeOptionalShortText(line.subledger_reference_no, "subledger_reference_no", 100),
        String(line.currency_code || original.currency_code).toUpperCase(),
        Number(line.amount_txn || 0) * -1,
        Number(line.credit_base || 0),
        Number(line.debit_base || 0),
        line.tax_code ? String(line.tax_code) : null,
      ]
    );
  }

  await tx.query(
    `UPDATE journal_entries
     SET status = 'REVERSED',
         reversed_by_user_id = ?,
         reversed_at = CURRENT_TIMESTAMP,
         reversal_journal_entry_id = ?,
         reverse_reason = ?
     WHERE id = ?
       AND tenant_id = ?`,
    [params.userId, reversalJournalId, reason, journalId, params.tenantId]
  );

  return reversalJournalId;
}

async function getRetainedEarningsAccountForBook(
  tenantId,
  bookLegalEntityId,
  accountId,
  runQuery = query
) {
  const parsedAccountId = parsePositiveInt(accountId);
  if (!parsedAccountId) {
    return null;
  }

  const result = await runQuery(
    `SELECT
       a.id,
       a.code,
       a.name,
       a.account_type,
       a.allow_posting,
       a.is_active,
       EXISTS(
         SELECT 1
         FROM accounts child
         WHERE child.parent_account_id = a.id
           AND child.is_active = TRUE
       ) AS has_active_children,
       c.legal_entity_id
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE a.id = ?
       AND c.tenant_id = ?
     LIMIT 1`,
    [parsedAccountId, tenantId]
  );
  const row = result.rows[0];
  if (!row) {
    throw badRequest("retainedEarningsAccountId not found for tenant");
  }

  const accountType = String(row.account_type || "").toUpperCase();
  if (accountType !== "EQUITY") {
    throw badRequest("retainedEarningsAccountId must reference an EQUITY account");
  }
  if (!Boolean(row.is_active)) {
    throw badRequest("retainedEarningsAccountId must reference an active account");
  }
  if (!Boolean(row.allow_posting)) {
    throw badRequest("retainedEarningsAccountId must reference a postable leaf account");
  }
  if (Boolean(row.has_active_children)) {
    throw badRequest("retainedEarningsAccountId must reference a leaf account");
  }

  const accountLegalEntityId = parsePositiveInt(row.legal_entity_id);
  if (accountLegalEntityId && accountLegalEntityId !== bookLegalEntityId) {
    throw badRequest("retainedEarningsAccountId must belong to the same legal entity as bookId");
  }

  return {
    id: parsedAccountId,
    code: String(row.code || ""),
    name: String(row.name || ""),
    accountType,
    legalEntityId: accountLegalEntityId,
  };
}

function parseOptionalPositiveInt(value, fieldLabel) {
  if (value === undefined || value === null || value === "") {
    return null;
  }

  const parsed = parsePositiveInt(value);
  if (!parsed) {
    throw badRequest(`${fieldLabel} must be a positive integer`);
  }
  return parsed;
}

function normalizeOptionalShortText(value, fieldLabel, maxLength = 100) {
  if (value === undefined || value === null) {
    return null;
  }

  const normalized = String(value).trim();
  if (!normalized) {
    return null;
  }
  if (normalized.length > maxLength) {
    throw badRequest(`${fieldLabel} must be at most ${maxLength} characters`);
  }
  return normalized;
}

function parseBooleanFlag(value, defaultValue, fieldLabel) {
  if (value === undefined || value === null || value === "") {
    return Boolean(defaultValue);
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value !== 0;
  }

  const normalized = String(value).trim().toLowerCase();
  if (["true", "1", "yes", "y", "on"].includes(normalized)) {
    return true;
  }
  if (["false", "0", "no", "n", "off"].includes(normalized)) {
    return false;
  }

  throw badRequest(`${fieldLabel} must be a boolean`);
}

function normalizeJournalSourceType(value) {
  const sourceType = String(value || "MANUAL").toUpperCase();
  if (!JOURNAL_SOURCE_TYPES.has(sourceType)) {
    throw badRequest(
      "sourceType must be one of MANUAL, SYSTEM, INTERCOMPANY, ELIMINATION, ADJUSTMENT, CASH"
    );
  }
  return sourceType;
}

async function getLegalEntityIntercompanySettings(tenantId, legalEntityId) {
  const result = await query(
    `SELECT is_intercompany_enabled, intercompany_partner_required
     FROM legal_entities
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId]
  );

  const row = result.rows[0];
  if (!row) {
    throw badRequest("legalEntityId not found for tenant");
  }

  return {
    isIntercompanyEnabled: Boolean(row.is_intercompany_enabled),
    intercompanyPartnerRequired: Boolean(row.intercompany_partner_required),
  };
}

async function getLegalEntityCodeMap(tenantId, legalEntityIds) {
  if (!Array.isArray(legalEntityIds) || legalEntityIds.length === 0) {
    return new Map();
  }

  const ids = Array.from(new Set(legalEntityIds.map((id) => parsePositiveInt(id)).filter(Boolean)));
  if (ids.length === 0) {
    return new Map();
  }

  const placeholders = ids.map(() => "?").join(", ");
  const result = await query(
    `SELECT id, code
     FROM legal_entities
     WHERE tenant_id = ?
       AND id IN (${placeholders})`,
    [tenantId, ...ids]
  );

  const map = new Map();
  for (const row of result.rows || []) {
    const id = parsePositiveInt(row.id);
    if (!id) {
      continue;
    }
    map.set(id, String(row.code || `LE-${id}`));
  }

  return map;
}

async function assertActiveIntercompanyPairs(
  tenantId,
  fromLegalEntityId,
  counterpartyLegalEntityIds
) {
  const ids = Array.from(
    new Set((counterpartyLegalEntityIds || []).map((id) => parsePositiveInt(id)).filter(Boolean))
  );
  if (ids.length === 0) {
    return;
  }

  const placeholders = ids.map(() => "?").join(", ");
  const result = await query(
    `SELECT to_legal_entity_id
     FROM intercompany_pairs
     WHERE tenant_id = ?
       AND from_legal_entity_id = ?
       AND status = 'ACTIVE'
       AND to_legal_entity_id IN (${placeholders})`,
    [tenantId, fromLegalEntityId, ...ids]
  );

  const activeToIds = new Set(
    (result.rows || []).map((row) => parsePositiveInt(row.to_legal_entity_id)).filter(Boolean)
  );
  const missingToIds = ids.filter((id) => !activeToIds.has(id));

  if (missingToIds.length === 0) {
    return;
  }

  const codeMap = await getLegalEntityCodeMap(tenantId, missingToIds);
  const display = missingToIds
    .map((id) => codeMap.get(id) || `LE-${id}`)
    .join(", ");

  throw badRequest(
    `Active intercompany pair mapping is required from legalEntityId=${fromLegalEntityId} to: ${display}`
  );
}

async function validateIntercompanyJournalPolicy(
  tenantId,
  legalEntityId,
  sourceType,
  lines
) {
  const settings = await getLegalEntityIntercompanySettings(tenantId, legalEntityId);
  const counterpartyIds = [];
  const missingCounterpartyLineNumbers = [];

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const counterpartyLegalEntityId = parsePositiveInt(line?.counterpartyLegalEntityId);
    if (counterpartyLegalEntityId) {
      counterpartyIds.push(counterpartyLegalEntityId);
    } else {
      missingCounterpartyLineNumbers.push(i + 1);
    }
  }

  const hasCounterpartyLines = counterpartyIds.length > 0;

  if (!settings.isIntercompanyEnabled) {
    if (sourceType === "INTERCOMPANY") {
      throw badRequest(
        "Selected legal entity has intercompany disabled; INTERCOMPANY source journals are not allowed"
      );
    }
    if (hasCounterpartyLines) {
      throw badRequest(
        "Selected legal entity has intercompany disabled; counterpartyLegalEntityId is not allowed on journal lines"
      );
    }
    return;
  }

  if (sourceType === "INTERCOMPANY" && !hasCounterpartyLines) {
    throw badRequest(
      "INTERCOMPANY source journals require at least one line with counterpartyLegalEntityId"
    );
  }

  if (settings.intercompanyPartnerRequired && sourceType === "INTERCOMPANY") {
    if (missingCounterpartyLineNumbers.length > 0) {
      throw badRequest(
        `Selected legal entity requires intercompany partner on INTERCOMPANY journals. Missing counterparty on line(s): ${missingCounterpartyLineNumbers.join(", ")}`
      );
    }
  }

  if (hasCounterpartyLines) {
    await assertActiveIntercompanyPairs(tenantId, legalEntityId, counterpartyIds);
  }
}

async function getLegalEntityIntercompanySettingsMap(
  tenantId,
  legalEntityIds,
  runQuery = query
) {
  const ids = Array.from(
    new Set((legalEntityIds || []).map((id) => parsePositiveInt(id)).filter(Boolean))
  );
  if (ids.length === 0) {
    return new Map();
  }

  const placeholders = ids.map(() => "?").join(", ");
  const result = await runQuery(
    `SELECT id, is_intercompany_enabled, intercompany_partner_required
     FROM legal_entities
     WHERE tenant_id = ?
       AND id IN (${placeholders})`,
    [tenantId, ...ids]
  );

  const map = new Map();
  for (const row of result.rows || []) {
    const id = parsePositiveInt(row.id);
    if (!id) {
      continue;
    }
    map.set(id, {
      isIntercompanyEnabled: Boolean(row.is_intercompany_enabled),
      intercompanyPartnerRequired: Boolean(row.intercompany_partner_required),
    });
  }

  return map;
}

async function loadActiveIntercompanyPair(
  tenantId,
  fromLegalEntityId,
  toLegalEntityId,
  runQuery = query
) {
  const result = await runQuery(
    `SELECT id, receivable_account_id, payable_account_id
     FROM intercompany_pairs
     WHERE tenant_id = ?
       AND from_legal_entity_id = ?
       AND to_legal_entity_id = ?
       AND status = 'ACTIVE'
     LIMIT 1`,
    [tenantId, fromLegalEntityId, toLegalEntityId]
  );
  return result.rows[0] || null;
}

async function getAccountCodeMapById(
  tenantId,
  legalEntityId,
  accountIds,
  runQuery = query
) {
  const ids = Array.from(new Set((accountIds || []).map((id) => parsePositiveInt(id)).filter(Boolean)));
  if (ids.length === 0) {
    return new Map();
  }

  const placeholders = ids.map(() => "?").join(", ");
  const result = await runQuery(
    `SELECT a.id, a.code
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE c.tenant_id = ?
       AND c.legal_entity_id = ?
       AND a.id IN (${placeholders})`,
    [tenantId, legalEntityId, ...ids]
  );

  const map = new Map();
  for (const row of result.rows || []) {
    const id = parsePositiveInt(row.id);
    if (!id) continue;
    map.set(id, String(row.code || ""));
  }
  return map;
}

async function assertPostableLeafAccountForLegalEntity(
  tenantId,
  legalEntityId,
  accountId,
  fieldLabel,
  runQuery = query
) {
  const parsedAccountId = parsePositiveInt(accountId);
  if (!parsedAccountId) {
    throw badRequest(`${fieldLabel} must be a positive integer`);
  }

  const result = await runQuery(
    `SELECT
       a.id,
       a.is_active,
       a.allow_posting,
       EXISTS(
         SELECT 1
         FROM accounts child
         WHERE child.parent_account_id = a.id
           AND child.is_active = TRUE
       ) AS has_active_children,
       c.legal_entity_id
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE a.id = ?
       AND c.tenant_id = ?
     LIMIT 1`,
    [parsedAccountId, tenantId]
  );

  const row = result.rows[0];
  if (!row) {
    throw badRequest(`${fieldLabel} not found for tenant`);
  }
  if (parsePositiveInt(row.legal_entity_id) !== legalEntityId) {
    throw badRequest(`${fieldLabel} must belong to legalEntityId=${legalEntityId}`);
  }
  if (!Boolean(row.is_active)) {
    throw badRequest(`${fieldLabel} must be active`);
  }
  if (!Boolean(row.allow_posting)) {
    throw badRequest(`${fieldLabel} must be a postable account`);
  }
  if (Boolean(row.has_active_children)) {
    throw badRequest(`${fieldLabel} must be a leaf account`);
  }
}

async function getPostableAccountMapByCode(
  tenantId,
  legalEntityId,
  accountCodes,
  runQuery = query
) {
  const codes = Array.from(
    new Set(
      (accountCodes || [])
        .map((code) => String(code || "").trim())
        .filter((code) => code.length > 0)
    )
  );
  if (codes.length === 0) {
    return new Map();
  }

  const placeholders = codes.map(() => "?").join(", ");
  const result = await runQuery(
    `SELECT a.id, a.code
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE c.tenant_id = ?
       AND c.legal_entity_id = ?
       AND a.code IN (${placeholders})
       AND a.is_active = TRUE
       AND a.allow_posting = TRUE
       AND NOT EXISTS (
         SELECT 1
         FROM accounts child
         WHERE child.parent_account_id = a.id
           AND child.is_active = TRUE
       )
     ORDER BY a.id`,
    [tenantId, legalEntityId, ...codes]
  );

  const map = new Map();
  for (const row of result.rows || []) {
    const code = String(row.code || "").trim();
    const id = parsePositiveInt(row.id);
    if (!code || !id || map.has(code)) {
      continue;
    }
    map.set(code, id);
  }

  return map;
}

async function resolvePreferredBookForLegalEntity(
  tenantId,
  legalEntityId,
  runQuery = query
) {
  const result = await runQuery(
    `SELECT id, calendar_id, code, name, book_type
     FROM books
     WHERE tenant_id = ?
       AND legal_entity_id = ?
     ORDER BY CASE WHEN book_type = 'LOCAL' THEN 0 ELSE 1 END, id
     LIMIT 1`,
    [tenantId, legalEntityId]
  );

  const row = result.rows[0];
  if (!row) {
    throw badRequest(`No book found for legalEntityId=${legalEntityId}`);
  }
  return row;
}

async function loadFiscalPeriodTemplate(fiscalPeriodId, runQuery = query) {
  const parsedPeriodId = parsePositiveInt(fiscalPeriodId);
  if (!parsedPeriodId) {
    throw badRequest("fiscalPeriodId must be a positive integer");
  }

  const result = await runQuery(
    `SELECT id, fiscal_year, period_no, is_adjustment
     FROM fiscal_periods
     WHERE id = ?
     LIMIT 1`,
    [parsedPeriodId]
  );
  const row = result.rows[0];
  if (!row) {
    throw badRequest("fiscalPeriodId not found");
  }
  return row;
}

async function resolveFiscalPeriodForCalendar(calendarId, periodTemplate, runQuery = query) {
  const parsedCalendarId = parsePositiveInt(calendarId);
  if (!parsedCalendarId) {
    throw badRequest("calendarId must be a positive integer");
  }

  const result = await runQuery(
    `SELECT id
     FROM fiscal_periods
     WHERE calendar_id = ?
       AND fiscal_year = ?
       AND period_no = ?
       AND is_adjustment = ?
     LIMIT 1`,
    [
      parsedCalendarId,
      Number(periodTemplate?.fiscal_year),
      Number(periodTemplate?.period_no),
      Boolean(periodTemplate?.is_adjustment),
    ]
  );

  const periodId = parsePositiveInt(result.rows[0]?.id);
  if (!periodId) {
    throw badRequest(
      `No matching fiscal period found in calendarId=${parsedCalendarId} for ${periodTemplate?.fiscal_year}-P${String(
        periodTemplate?.period_no || ""
      ).padStart(2, "0")}`
    );
  }

  return periodId;
}

function groupLinesByCounterparty(lines) {
  const linesByCounterparty = new Map();
  const missingCounterpartyLineNumbers = [];

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const lineNo = i + 1;
    const counterpartyLegalEntityId = parsePositiveInt(line?.counterpartyLegalEntityId);
    if (!counterpartyLegalEntityId) {
      missingCounterpartyLineNumbers.push(lineNo);
      continue;
    }

    if (!linesByCounterparty.has(counterpartyLegalEntityId)) {
      linesByCounterparty.set(counterpartyLegalEntityId, []);
    }
    linesByCounterparty.get(counterpartyLegalEntityId).push({
      ...line,
      _sourceLineNo: lineNo,
    });
  }

  return {
    linesByCounterparty,
    missingCounterpartyLineNumbers,
  };
}

function buildMirrorLineDescription(sourceJournalNo, sourceLineNo, originalDescription) {
  const original = String(originalDescription || "").trim();
  if (original) {
    return `Auto mirror of ${sourceJournalNo} L${sourceLineNo}: ${original}`.slice(0, 500);
  }
  return `Auto mirror of ${sourceJournalNo} line ${sourceLineNo}`.slice(0, 500);
}

function buildMirrorReferenceNo(sourceReferenceNo, sourceJournalNo, targetLegalEntityId) {
  const base = String(sourceReferenceNo || sourceJournalNo || "INTERCOMPANY").trim();
  const suffix = `-MIRROR-LE${targetLegalEntityId}`;
  return `${base}${suffix}`.slice(0, 100);
}

async function buildIntercompanyAutoMirrorDraftSpecs({
  req,
  tenantId,
  sourceLegalEntityId,
  sourceFiscalPeriodId,
  sourceJournalNo,
  sourceEntryDate,
  sourceDocumentDate,
  sourceCurrencyCode,
  sourceReferenceNo,
  sourceDescription,
  sourceLines,
}) {
  const {
    linesByCounterparty,
    missingCounterpartyLineNumbers,
  } = groupLinesByCounterparty(sourceLines);

  if (missingCounterpartyLineNumbers.length > 0) {
    throw badRequest(
      `autoMirror requires counterpartyLegalEntityId on all lines. Missing on line(s): ${missingCounterpartyLineNumbers.join(", ")}`
    );
  }

  const targetLegalEntityIds = Array.from(linesByCounterparty.keys()).filter(Boolean);
  if (targetLegalEntityIds.length === 0) {
    return [];
  }

  const codeMap = await getLegalEntityCodeMap(tenantId, [
    sourceLegalEntityId,
    ...targetLegalEntityIds,
  ]);
  const settingsMap = await getLegalEntityIntercompanySettingsMap(tenantId, targetLegalEntityIds);
  const periodTemplate = await loadFiscalPeriodTemplate(sourceFiscalPeriodId);

  const sourceAccountIdSet = new Set();
  for (const groupedLines of linesByCounterparty.values()) {
    for (const line of groupedLines) {
      const accountId = parsePositiveInt(line?.accountId);
      if (accountId) {
        sourceAccountIdSet.add(accountId);
      }
    }
  }
  const sourceAccountCodeMap = await getAccountCodeMapById(
    tenantId,
    sourceLegalEntityId,
    Array.from(sourceAccountIdSet)
  );

  const specs = [];
  for (const targetLegalEntityId of targetLegalEntityIds) {
    if (targetLegalEntityId === sourceLegalEntityId) {
      throw badRequest("autoMirror does not allow same legal entity as source/counterparty");
    }
    assertScopeAccess(
      req,
      "legal_entity",
      targetLegalEntityId,
      `autoMirror.targetLegalEntityId=${targetLegalEntityId}`
    );

    const targetSettings = settingsMap.get(targetLegalEntityId);
    if (!targetSettings?.isIntercompanyEnabled) {
      const targetCode = codeMap.get(targetLegalEntityId) || `LE-${targetLegalEntityId}`;
      throw badRequest(
        `autoMirror requires target legal entity intercompany enabled: ${targetCode}`
      );
    }

    const reversePair = await loadActiveIntercompanyPair(
      tenantId,
      targetLegalEntityId,
      sourceLegalEntityId
    );
    if (!reversePair) {
      const sourceCode = codeMap.get(sourceLegalEntityId) || `LE-${sourceLegalEntityId}`;
      const targetCode = codeMap.get(targetLegalEntityId) || `LE-${targetLegalEntityId}`;
      throw badRequest(
        `autoMirror requires ACTIVE intercompany pair mapping from ${targetCode} to ${sourceCode}`
      );
    }

    const reverseReceivableAccountId = parsePositiveInt(reversePair.receivable_account_id);
    const reversePayableAccountId = parsePositiveInt(reversePair.payable_account_id);

    if (reverseReceivableAccountId) {
      await assertPostableLeafAccountForLegalEntity(
        tenantId,
        targetLegalEntityId,
        reverseReceivableAccountId,
        "reverse pair receivableAccountId"
      );
    }
    if (reversePayableAccountId) {
      await assertPostableLeafAccountForLegalEntity(
        tenantId,
        targetLegalEntityId,
        reversePayableAccountId,
        "reverse pair payableAccountId"
      );
    }

    const targetBook = await resolvePreferredBookForLegalEntity(tenantId, targetLegalEntityId);
    const targetBookId = parsePositiveInt(targetBook.id);
    const targetCalendarId = parsePositiveInt(targetBook.calendar_id);
    const targetFiscalPeriodId = await resolveFiscalPeriodForCalendar(
      targetCalendarId,
      periodTemplate
    );

    await ensurePeriodOpen(
      targetBookId,
      targetFiscalPeriodId,
      `auto-create mirror draft journal for legalEntityId=${targetLegalEntityId}`
    );

    const groupedLines = linesByCounterparty.get(targetLegalEntityId) || [];
    const sourceAccountCodes = groupedLines
      .map((line) => {
        const sourceAccountId = parsePositiveInt(line?.accountId);
        return sourceAccountCodeMap.get(sourceAccountId) || "";
      })
      .filter(Boolean);

    const targetAccountByCode = await getPostableAccountMapByCode(
      tenantId,
      targetLegalEntityId,
      sourceAccountCodes
    );

    const mirrorLines = [];
    let sourceDebitTotal = 0;
    let sourceCreditTotal = 0;
    let mirrorDebitTotal = 0;
    let mirrorCreditTotal = 0;

    for (const sourceLine of groupedLines) {
      const sourceLineNo = Number(sourceLine?._sourceLineNo || 0);
      const sourceAccountId = parsePositiveInt(sourceLine?.accountId);
      const sourceAccountCode = sourceAccountCodeMap.get(sourceAccountId);
      if (!sourceAccountCode) {
        throw badRequest(
          `autoMirror could not resolve source account code for line ${sourceLineNo}`
        );
      }

      const sourceDebit = toAmount(sourceLine?.debitBase);
      const sourceCredit = toAmount(sourceLine?.creditBase);
      sourceDebitTotal += sourceDebit;
      sourceCreditTotal += sourceCredit;

      const mirrorDebit = sourceCredit;
      const mirrorCredit = sourceDebit;
      mirrorDebitTotal += mirrorDebit;
      mirrorCreditTotal += mirrorCredit;

      let targetAccountId = parsePositiveInt(targetAccountByCode.get(sourceAccountCode));
      if (!targetAccountId) {
        targetAccountId =
          mirrorDebit > 0 ? reverseReceivableAccountId : reversePayableAccountId;
      }
      if (!targetAccountId) {
        const sideLabel = mirrorDebit > 0 ? "DEBIT" : "CREDIT";
        throw badRequest(
          `autoMirror mapping missing for target legalEntityId=${targetLegalEntityId}, source account ${sourceAccountCode}, line ${sourceLineNo}, side ${sideLabel}`
        );
      }

      mirrorLines.push({
        accountId: targetAccountId,
        operatingUnitId: null,
        counterpartyLegalEntityId: sourceLegalEntityId,
        description: buildMirrorLineDescription(
          sourceJournalNo,
          sourceLineNo,
          sourceLine?.description
        ),
        currencyCode: String(sourceLine?.currencyCode || sourceCurrencyCode || "USD").toUpperCase(),
        amountTxn: -toAmount(sourceLine?.amountTxn),
        debitBase: mirrorDebit,
        creditBase: mirrorCredit,
        taxCode: sourceLine?.taxCode ? String(sourceLine.taxCode) : null,
      });
    }

    if (Math.abs(sourceDebitTotal - sourceCreditTotal) > BALANCE_EPSILON) {
      const targetCode = codeMap.get(targetLegalEntityId) || `LE-${targetLegalEntityId}`;
      throw badRequest(
        `autoMirror requires counterparty-specific balance per target legal entity. Unbalanced source block for ${targetCode}`
      );
    }
    if (Math.abs(mirrorDebitTotal - mirrorCreditTotal) > BALANCE_EPSILON) {
      throw badRequest(
        `autoMirror produced an unbalanced mirror block for legalEntityId=${targetLegalEntityId}`
      );
    }

    specs.push({
      legalEntityId: targetLegalEntityId,
      bookId: targetBookId,
      fiscalPeriodId: targetFiscalPeriodId,
      journalNo: buildSystemJournalNo("ICM", `${sourceLegalEntityId}-${targetLegalEntityId}`),
      sourceType: "INTERCOMPANY",
      entryDate: sourceEntryDate,
      documentDate: sourceDocumentDate,
      currencyCode: String(sourceCurrencyCode || "USD").toUpperCase(),
      description: `Auto mirror of ${sourceJournalNo}${sourceDescription ? ` | ${sourceDescription}` : ""}`.slice(
        0,
        500
      ),
      referenceNo: buildMirrorReferenceNo(
        sourceReferenceNo,
        sourceJournalNo,
        targetLegalEntityId
      ),
      totalDebit: mirrorDebitTotal,
      totalCredit: mirrorCreditTotal,
      lines: mirrorLines,
    });
  }

  return specs;
}

async function insertDraftJournalEntry(tx, payload) {
  const entryResult = await tx.query(
    `INSERT INTO journal_entries (
        tenant_id, legal_entity_id, book_id, fiscal_period_id, journal_no,
        source_type, status, entry_date, document_date, currency_code,
        description, reference_no, total_debit_base, total_credit_base, created_by_user_id,
        intercompany_source_journal_entry_id
      )
     VALUES (?, ?, ?, ?, ?, ?, 'DRAFT', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      payload.tenantId,
      payload.legalEntityId,
      payload.bookId,
      payload.fiscalPeriodId,
      payload.journalNo,
      payload.sourceType,
      payload.entryDate,
      payload.documentDate,
      payload.currencyCode,
      payload.description || null,
      payload.referenceNo || null,
      payload.totalDebit,
      payload.totalCredit,
      payload.userId,
      parsePositiveInt(payload.intercompanySourceJournalEntryId) || null,
    ]
  );

  const createdJournalEntryId = parsePositiveInt(entryResult.rows.insertId);
  if (!createdJournalEntryId) {
    throw badRequest("Failed to create journal entry");
  }

  const journalLines = Array.isArray(payload.lines) ? payload.lines : [];
  for (let i = 0; i < journalLines.length; i += 1) {
    const line = journalLines[i];
    // eslint-disable-next-line no-await-in-loop
    await tx.query(
      `INSERT INTO journal_lines (
          journal_entry_id, line_no, account_id, operating_unit_id,
          counterparty_legal_entity_id, description, subledger_reference_no, currency_code,
          amount_txn, debit_base, credit_base, tax_code
        )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        createdJournalEntryId,
        i + 1,
        parsePositiveInt(line.accountId),
        parsePositiveInt(line.operatingUnitId),
        parsePositiveInt(line.counterpartyLegalEntityId),
        line.description ? String(line.description) : null,
        normalizeOptionalShortText(
          line.subledgerReferenceNo,
          `lines[${i}].subledgerReferenceNo`,
          100
        ),
        String(line.currencyCode || payload.currencyCode || "USD").toUpperCase(),
        toAmount(line.amountTxn),
        toAmount(line.debitBase),
        toAmount(line.creditBase),
        line.taxCode ? String(line.taxCode) : null,
      ]
    );
  }

  return createdJournalEntryId;
}

async function loadIntercompanyJournalCluster(tenantId, anchorJournal) {
  const anchorJournalId = parsePositiveInt(anchorJournal?.id);
  if (!anchorJournalId) {
    return { sourceJournalId: null, rows: [] };
  }

  const sourceJournalId =
    parsePositiveInt(anchorJournal?.intercompany_source_journal_entry_id) || anchorJournalId;

  const result = await query(
    `SELECT id, legal_entity_id, book_id, fiscal_period_id, status
     FROM journal_entries
     WHERE tenant_id = ?
       AND (id = ? OR intercompany_source_journal_entry_id = ?)
     ORDER BY id`,
    [tenantId, sourceJournalId, sourceJournalId]
  );

  return {
    sourceJournalId,
    rows: result.rows || [],
  };
}

async function validateJournalLineScope(req, tenantId, legalEntityId, line, index) {
  const lineLabel = `lines[${index}]`;
  const accountId = parsePositiveInt(line?.accountId);
  if (!accountId) {
    throw badRequest(`${lineLabel}.accountId must be a positive integer`);
  }

  const accountResult = await query(
    `SELECT
       a.id,
       a.is_active,
       a.allow_posting,
       EXISTS(
         SELECT 1
         FROM accounts child
         WHERE child.parent_account_id = a.id
           AND child.is_active = TRUE
       ) AS has_active_children,
       c.legal_entity_id
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE a.id = ?
       AND c.tenant_id = ?
     LIMIT 1`,
    [accountId, tenantId]
  );
  const account = accountResult.rows[0];
  if (!account) {
    throw badRequest(`${lineLabel}.accountId not found for tenant`);
  }
  if (!Boolean(account.is_active)) {
    throw badRequest(`${lineLabel}.accountId is inactive`);
  }
  if (!Boolean(account.allow_posting)) {
    throw badRequest(
      `${lineLabel}.accountId is not postable. Select a postable sub-account.`
    );
  }
  if (Boolean(account.has_active_children)) {
    throw badRequest(
      `${lineLabel}.accountId is a parent account. Select a leaf sub-account.`
    );
  }

  const accountLegalEntityId = parsePositiveInt(account.legal_entity_id);
  if (accountLegalEntityId && accountLegalEntityId !== legalEntityId) {
    throw badRequest(`${lineLabel}.accountId does not belong to legalEntityId`);
  }
  if (accountLegalEntityId) {
    assertScopeAccess(req, "legal_entity", accountLegalEntityId, `${lineLabel}.accountId`);
  }

  const operatingUnitId = parseOptionalPositiveInt(
    line?.operatingUnitId,
    `${lineLabel}.operatingUnitId`
  );
  const subledgerReferenceNo = normalizeOptionalShortText(
    line?.subledgerReferenceNo,
    `${lineLabel}.subledgerReferenceNo`,
    100
  );
  let selectedUnitHasSubledger = false;
  if (operatingUnitId) {
    const unitResult = await query(
      `SELECT id, legal_entity_id, has_subledger
       FROM operating_units
       WHERE id = ?
         AND tenant_id = ?
       LIMIT 1`,
      [operatingUnitId, tenantId]
    );
    const unit = unitResult.rows[0];
    if (!unit) {
      throw badRequest(`${lineLabel}.operatingUnitId not found for tenant`);
    }
    if (parsePositiveInt(unit.legal_entity_id) !== legalEntityId) {
      throw badRequest(`${lineLabel}.operatingUnitId does not belong to legalEntityId`);
    }
    selectedUnitHasSubledger = Boolean(unit.has_subledger);
    assertScopeAccess(req, "operating_unit", operatingUnitId, `${lineLabel}.operatingUnitId`);
  }
  if (subledgerReferenceNo && !operatingUnitId) {
    throw badRequest(`${lineLabel}.subledgerReferenceNo requires operatingUnitId`);
  }
  if (selectedUnitHasSubledger && !subledgerReferenceNo) {
    throw badRequest(
      `${lineLabel}.subledgerReferenceNo is required because operatingUnitId has has_subledger enabled`
    );
  }

  const counterpartyLegalEntityId = parseOptionalPositiveInt(
    line?.counterpartyLegalEntityId,
    `${lineLabel}.counterpartyLegalEntityId`
  );
  if (counterpartyLegalEntityId) {
    if (counterpartyLegalEntityId === legalEntityId) {
      throw badRequest(`${lineLabel}.counterpartyLegalEntityId cannot be the same as legalEntityId`);
    }

    const counterpartyResult = await query(
      `SELECT id
       FROM legal_entities
       WHERE id = ?
         AND tenant_id = ?
       LIMIT 1`,
      [counterpartyLegalEntityId, tenantId]
    );
    if (!counterpartyResult.rows[0]) {
      throw badRequest(`${lineLabel}.counterpartyLegalEntityId not found for tenant`);
    }
    assertScopeAccess(
      req,
      "legal_entity",
      counterpartyLegalEntityId,
      `${lineLabel}.counterpartyLegalEntityId`
    );
  }

  const debitBase = toAmount(line?.debitBase);
  const creditBase = toAmount(line?.creditBase);
  if (debitBase < 0 || creditBase < 0) {
    throw badRequest(`${lineLabel}.debitBase/creditBase cannot be negative`);
  }
  if ((debitBase === 0 && creditBase === 0) || (debitBase > 0 && creditBase > 0)) {
    throw badRequest(
      `${lineLabel} must have exactly one side > 0 (either debitBase or creditBase)`
    );
  }
}

registerGlReadCoreRoutes(router);
registerGlWriteCoreRoutes(router);
registerGlPurposeMappingsRoutes(router);
registerGlReadJournalRoutes(router, {
  resolveScopeFromBookId,
  resolveScopeFromJournalId,
});
registerGlReadTrialBalanceRoute(router, {
  resolveScopeFromBookId,
  isNearlyZero,
});

registerGlWriteJournalRoutes(router, {
  applyShareholderCommitmentSyncForPostedJournalTx,
  buildIntercompanyAutoMirrorDraftSpecs,
  ensurePeriodOpen,
  generateJournalNo,
  insertDraftJournalEntry,
  loadIntercompanyJournalCluster,
  loadJournal,
  normalizeJournalSourceType,
  normalizeOptionalShortText,
  parseBooleanFlag,
  resolveScopeFromJournalId,
  toAmount,
  toIsoDate,
  validateIntercompanyJournalPolicy,
  validateJournalLineScope,
});
registerGlReclassificationRoutes(router, {
  assertPostableLeafAccountForLegalEntity,
  buildSystemJournalNo,
  ensurePeriodOpen,
  isNearlyZero,
  normalizeReclassAllocationMode,
  parseJsonColumn,
  resolveScopeFromBookId,
  roundAmount,
  toBoolean,
  toIsoDate,
  toOptionalIsoDate,
});
registerGlPeriodClosingRoutes(router, {
  buildSystemJournalNo,
  buildYearEndCloseLine,
  closeRunStatuses: CLOSE_RUN_STATUSES,
  computeCloseRunHash,
  createSystemJournalWithLines,
  findNextFiscalPeriod,
  getEffectivePeriodStatus,
  getFiscalPeriodDetails,
  getPeriodSourceFingerprint,
  getPostedPeriodAccountBalances,
  getRetainedEarningsAccountForBook,
  isNearlyZero,
  mapPeriodCloseRunRow,
  normalizeCloseTargetStatus,
  parseJsonColumn,
  periodStatuses: PERIOD_STATUSES,
  resolveScopeFromBookId,
  reversePostedJournalWithinTransaction,
  writeAuditLog,
});

export {
  ensurePeriodOpen,
  toAmount,
  toIsoDate,
  validateJournalLineScope,
};
export default router;
