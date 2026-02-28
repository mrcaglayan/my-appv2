import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { matchReconciliationLine, reconcileStatementLineToJournal } from "./bank.reconciliation.service.js";
import { getDifferenceProfileForAutomation } from "./bank.reconciliationDifferenceProfiles.service.js";

const MATCH_EPSILON = 0.005;

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

function parseDate(value) {
  if (value === undefined || value === null || value === "") return "";
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return "";
    return value.toISOString().slice(0, 10);
  }
  const text = String(value).trim();
  if (!text) return "";
  if (/^\d{4}-\d{2}-\d{2}/.test(text)) return text.slice(0, 10);
  const parsed = new Date(text);
  if (!Number.isNaN(parsed.getTime())) return parsed.toISOString().slice(0, 10);
  return "";
}

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function signOfAmount(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n === 0) return 0;
  return n > 0 ? 1 : -1;
}

async function getStatementLineWithBank({ tenantId, lineId, runQuery = query }) {
  const res = await runQuery(
    `SELECT
        l.id,
        l.tenant_id,
        l.legal_entity_id,
        l.bank_account_id,
        l.txn_date,
        l.description,
        l.reference_no,
        l.amount,
        l.currency_code,
        l.recon_status,
        l.reconciliation_difference_type,
        l.reconciliation_difference_amount,
        l.reconciliation_difference_profile_id,
        l.reconciliation_difference_journal_entry_id,
        ba.gl_account_id AS bank_gl_account_id,
        ba.currency_code AS bank_account_currency_code
     FROM bank_statement_lines l
     JOIN bank_accounts ba
       ON ba.tenant_id = l.tenant_id
      AND ba.legal_entity_id = l.legal_entity_id
      AND ba.id = l.bank_account_id
     WHERE l.tenant_id = ?
       AND l.id = ?
     LIMIT 1`,
    [tenantId, lineId]
  );
  return res.rows?.[0] || null;
}

async function getPaymentLineWithBatch({ tenantId, paymentBatchLineId, runQuery = query }) {
  const res = await runQuery(
    `SELECT
        pbl.*,
        pb.batch_no,
        pb.status AS batch_status,
        pb.bank_account_id,
        pb.currency_code AS currency_code
     FROM payment_batch_lines pbl
     JOIN payment_batches pb
       ON pb.tenant_id = pbl.tenant_id
      AND pb.legal_entity_id = pbl.legal_entity_id
      AND pb.id = pbl.batch_id
     WHERE pbl.tenant_id = ?
       AND pbl.id = ?
     LIMIT 1`,
    [tenantId, paymentBatchLineId]
  );
  return res.rows?.[0] || null;
}

async function getActiveMatchedTotalForLine({ tenantId, lineId, runQuery = query }) {
  const res = await runQuery(
    `SELECT COALESCE(SUM(matched_amount), 0) AS total
     FROM bank_reconciliation_matches
     WHERE tenant_id = ?
       AND statement_line_id = ?
       AND status = 'ACTIVE'`,
    [tenantId, lineId]
  );
  return toAmount(res.rows?.[0]?.total || 0);
}

async function findActivePaymentBatchMatchForLine({ tenantId, lineId, paymentBatchId, runQuery = query }) {
  const res = await runQuery(
    `SELECT id, matched_amount, status
     FROM bank_reconciliation_matches
     WHERE tenant_id = ?
       AND statement_line_id = ?
       AND status = 'ACTIVE'
       AND matched_entity_type = 'PAYMENT_BATCH'
       AND matched_entity_id = ?
     ORDER BY id ASC
     LIMIT 1`,
    [tenantId, lineId, paymentBatchId]
  );
  return res.rows?.[0] || null;
}

function remainingAmountAbs(lineAmount, matchedTotal) {
  return Number(Math.max(0, absAmount(lineAmount) - absAmount(matchedTotal)).toFixed(6));
}

function effectiveForDate(profile, dateValue) {
  const d = parseDate(dateValue);
  const from = parseDate(profile?.effective_from);
  const to = parseDate(profile?.effective_to);
  if (from && d && d < from) return false;
  if (to && d && d > to) return false;
  return true;
}

function getExpectedPaymentLineAmountAbs(paymentLine) {
  const executed = absAmount(paymentLine.executed_amount);
  if (executed > MATCH_EPSILON) return executed;
  const exported = absAmount(paymentLine.exported_amount);
  if (exported > MATCH_EPSILON) return exported;
  return absAmount(paymentLine.amount);
}

function chooseFxCounterAccount(profile, statementLineAmount, diffSigned) {
  const stmtSign = signOfAmount(statementLineAmount);
  if (!stmtSign) throw badRequest("Statement line amount must be non-zero");
  if (!parsePositiveInt(profile.fx_gain_account_id) || !parsePositiveInt(profile.fx_loss_account_id)) {
    throw badRequest("FX profile gain/loss accounts are required");
  }

  // Outflow statement: actual > expected => loss, actual < expected => gain
  // Inflow statement: actual > expected => gain, actual < expected => loss
  const isGain = stmtSign < 0 ? diffSigned < 0 : diffSigned > 0;
  return {
    accountId: isGain ? parsePositiveInt(profile.fx_gain_account_id) : parsePositiveInt(profile.fx_loss_account_id),
    category: isGain ? "FX_GAIN" : "FX_LOSS",
  };
}

function buildDifferenceJournalLinePayloads({ profile, statementLine, diffSigned, diffAbs, bankGlAccountId }) {
  if (!(diffAbs > MATCH_EPSILON)) {
    throw badRequest("Difference amount must be non-zero");
  }
  const stmtDirection = signOfAmount(statementLine.amount);
  if (!stmtDirection) throw badRequest("Statement line amount must be non-zero");

  // actual > expected => delta follows statement direction
  // actual < expected => delta is opposite statement direction
  const deltaDirection = diffSigned > 0 ? stmtDirection : -stmtDirection;
  const bankIsDebit = deltaDirection > 0;

  let counter = null;
  if (u(profile.difference_type) === "FEE") {
    const accountId = parsePositiveInt(profile.expense_account_id);
    if (!accountId) throw badRequest("FEE profile requires expenseAccountId");
    counter = { accountId, category: "FEE_EXPENSE" };
  } else if (u(profile.difference_type) === "FX") {
    counter = chooseFxCounterAccount(profile, statementLine.amount, diffSigned);
  } else {
    throw badRequest(`Unsupported difference type: ${profile.difference_type}`);
  }

  const prefix =
    String(profile.description_prefix || "").trim() ||
    (u(profile.difference_type) === "FX" ? "Bank FX diff" : "Bank fee diff");
  const stmtText = String(statementLine.description || statementLine.reference_no || `BSL-${statementLine.id}`).trim();
  const narration = `${prefix}: ${stmtText}`.slice(0, 255);

  const bankLine = {
    accountId: parsePositiveInt(bankGlAccountId),
    debit: bankIsDebit ? diffAbs : 0,
    credit: bankIsDebit ? 0 : diffAbs,
    memo: narration,
    amountTxn: bankIsDebit ? diffAbs : -diffAbs,
    lineNo: 1,
  };
  const counterLine = {
    accountId: counter.accountId,
    debit: bankIsDebit ? 0 : diffAbs,
    credit: bankIsDebit ? diffAbs : 0,
    memo: narration,
    amountTxn: bankIsDebit ? -diffAbs : diffAbs,
    lineNo: 2,
  };

  return { narration, bankLine, counterLine, counterCategory: counter.category };
}

async function resolveBookAndPeriodForPostingTx(tx, { tenantId, legalEntityId, postDate }) {
  const bookRes = await tx.query(
    `SELECT id, calendar_id, base_currency_code, code, name, book_type
     FROM books
     WHERE tenant_id = ?
       AND legal_entity_id = ?
     ORDER BY CASE WHEN book_type = 'LOCAL' THEN 0 ELSE 1 END, id ASC
     LIMIT 1`,
    [tenantId, legalEntityId]
  );
  const book = bookRes.rows?.[0] || null;
  if (!book) throw badRequest("No book found for legal entity");

  const bookId = parsePositiveInt(book.id);
  const calendarId = parsePositiveInt(book.calendar_id);
  if (!bookId || !calendarId) throw badRequest("Book configuration is invalid");

  const periodRes = await tx.query(
    `SELECT id
     FROM fiscal_periods
     WHERE calendar_id = ?
       AND ? BETWEEN start_date AND end_date
     ORDER BY is_adjustment ASC, id ASC
     LIMIT 1`,
    [calendarId, postDate]
  );
  const periodId = parsePositiveInt(periodRes.rows?.[0]?.id);
  if (!periodId) throw badRequest("No fiscal period found for posting date");

  const statusRes = await tx.query(
    `SELECT status
     FROM period_statuses
     WHERE book_id = ?
       AND fiscal_period_id = ?
     LIMIT 1`,
    [bookId, periodId]
  );
  const periodStatus = u(statusRes.rows?.[0]?.status || "OPEN");
  if (periodStatus !== "OPEN") {
    throw badRequest(`Period is ${periodStatus}; cannot post B08-B difference journal`);
  }

  return { book, bookId, fiscalPeriodId: periodId };
}

async function createOrReuseDifferenceJournalAndTrace({
  tenantId,
  statementLine,
  paymentLine,
  profile,
  diffSigned,
  userId,
}) {
  const postDate = parseDate(statementLine.txn_date);
  if (!postDate) throw badRequest("Statement line txn_date is required");
  const bankGlAccountId = parsePositiveInt(statementLine.bank_gl_account_id);
  if (!bankGlAccountId) throw badRequest("Bank account GL mapping is missing");

  return withTransaction(async (tx) => {
    const existingAdjRes = await tx.query(
      `SELECT *
       FROM bank_reconciliation_difference_adjustments
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND bank_statement_line_id = ?
       LIMIT 1
       FOR UPDATE`,
      [tenantId, statementLine.legal_entity_id, statementLine.id]
    );
    const existingAdj = existingAdjRes.rows?.[0] || null;
    if (existingAdj) {
      return {
        idempotent: true,
        adjustment: existingAdj,
        journalEntryId: parsePositiveInt(existingAdj.journal_entry_id),
      };
    }

    const journalCtx = await resolveBookAndPeriodForPostingTx(tx, {
      tenantId,
      legalEntityId: statementLine.legal_entity_id,
      postDate,
    });
    const bookCurrency = u(journalCtx.book?.base_currency_code);
    if (bookCurrency && bookCurrency !== u(statementLine.currency_code)) {
      throw badRequest(
        `Statement currency (${statementLine.currency_code}) must match book base currency (${bookCurrency})`
      );
    }

    const diffAbs = absAmount(diffSigned);
    const built = buildDifferenceJournalLinePayloads({
      profile,
      statementLine,
      diffSigned,
      diffAbs,
      bankGlAccountId,
    });

    const journalNo = `BDIFF-${statementLine.id}`;
    const existingJournalRes = await tx.query(
      `SELECT id, journal_no, status
       FROM journal_entries
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND book_id = ?
         AND journal_no = ?
       LIMIT 1`,
      [tenantId, statementLine.legal_entity_id, journalCtx.bookId, journalNo]
    );
    let journalEntryId = parsePositiveInt(existingJournalRes.rows?.[0]?.id);
    if (journalEntryId) {
      if (u(existingJournalRes.rows?.[0]?.status) !== "POSTED") {
        throw badRequest("Existing B08-B difference journal is not POSTED");
      }
    } else {
      const headerIns = await tx.query(
        `INSERT INTO journal_entries (
            tenant_id, legal_entity_id, book_id, fiscal_period_id, journal_no,
            source_type, status, entry_date, document_date, currency_code,
            description, reference_no, total_debit_base, total_credit_base,
            created_by_user_id, posted_by_user_id, posted_at
          ) VALUES (?, ?, ?, ?, ?, 'SYSTEM', 'POSTED', ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
        [
          tenantId,
          statementLine.legal_entity_id,
          journalCtx.bookId,
          journalCtx.fiscalPeriodId,
          journalNo,
          postDate,
          postDate,
          u(statementLine.currency_code),
          built.narration,
          String(statementLine.reference_no || `BSL-${statementLine.id}`).slice(0, 100),
          diffAbs,
          diffAbs,
          userId || null,
          userId || null,
        ]
      );
      journalEntryId = parsePositiveInt(headerIns.rows?.insertId);
      if (!journalEntryId) throw new Error("Failed to create B08-B difference journal");

      const lines = [built.bankLine, built.counterLine];
      for (const jl of lines) {
        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO journal_lines (
              journal_entry_id, line_no, account_id, operating_unit_id, counterparty_legal_entity_id,
              description, subledger_reference_no, currency_code, amount_txn, debit_base, credit_base, tax_code
            ) VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, NULL)`,
          [
            journalEntryId,
            jl.lineNo,
            jl.accountId,
            jl.memo,
            `BANKDIFF:${statementLine.id}:${jl.lineNo}`,
            u(statementLine.currency_code),
            jl.amountTxn,
            jl.debit,
            jl.credit,
          ]
        );
      }
    }

    const payload = {
      expected_amount_abs: getExpectedPaymentLineAmountAbs(paymentLine),
      actual_amount_abs: absAmount(statementLine.amount),
      diff_signed: toAmount(diffSigned),
    };
    const adjIns = await tx.query(
      `INSERT INTO bank_reconciliation_difference_adjustments (
          tenant_id, legal_entity_id, bank_statement_line_id, payment_batch_id, payment_batch_line_id,
          difference_profile_id, difference_type, difference_amount, currency_code, journal_entry_id,
          status, payload_json, created_by_user_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'POSTED', ?, ?)`,
      [
        tenantId,
        statementLine.legal_entity_id,
        statementLine.id,
        paymentLine.batch_id,
        paymentLine.id,
        profile.id,
        u(profile.difference_type),
        toAmount(diffSigned),
        u(statementLine.currency_code),
        journalEntryId,
        safeJson(payload),
        userId || null,
      ]
    );
    const adjustmentId = parsePositiveInt(adjIns.rows?.insertId);

    await tx.query(
      `UPDATE bank_statement_lines
       SET reconciliation_difference_type = ?,
           reconciliation_difference_amount = ?,
           reconciliation_difference_profile_id = ?,
           reconciliation_difference_journal_entry_id = ?
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?`,
      [
        u(profile.difference_type),
        toAmount(diffSigned),
        profile.id,
        journalEntryId,
        tenantId,
        statementLine.legal_entity_id,
        statementLine.id,
      ]
    );

    return {
      idempotent: false,
      journalEntryId,
      adjustment: {
        id: adjustmentId,
        tenant_id: tenantId,
        legal_entity_id: statementLine.legal_entity_id,
        bank_statement_line_id: statementLine.id,
        payment_batch_id: paymentLine.batch_id,
        payment_batch_line_id: paymentLine.id,
        difference_profile_id: profile.id,
        difference_type: u(profile.difference_type),
        difference_amount: toAmount(diffSigned),
        currency_code: u(statementLine.currency_code),
        journal_entry_id: journalEntryId,
        status: "POSTED",
        payload_json: payload,
      },
    };
  });
}

function validateProfileAgainstLineAndPayment({ profile, statementLine, paymentLine, diffSigned }) {
  if (!profile) throw badRequest("Difference profile not found");
  if (u(profile.status) !== "ACTIVE") throw badRequest("Difference profile is not ACTIVE");
  if (parsePositiveInt(profile.legal_entity_id) !== parsePositiveInt(statementLine.legal_entity_id)) {
    throw badRequest("Difference profile legal entity mismatch");
  }
  if (u(profile.scope_type) === "BANK_ACCOUNT") {
    if (parsePositiveInt(profile.bank_account_id) !== parsePositiveInt(statementLine.bank_account_id)) {
      throw badRequest("Difference profile is not scoped to this bank account");
    }
  }
  if (profile.currency_code && u(profile.currency_code) !== u(statementLine.currency_code)) {
    throw badRequest("Difference profile currency mismatch");
  }
  if (!effectiveForDate(profile, statementLine.txn_date)) {
    throw badRequest("Difference profile not effective for statement line date");
  }
  if (u(profile.direction_policy) === "INCREASE_ONLY" && !(diffSigned > MATCH_EPSILON)) {
    throw badRequest("Difference profile permits increases only");
  }
  if (u(profile.direction_policy) === "DECREASE_ONLY" && !(diffSigned < -MATCH_EPSILON)) {
    throw badRequest("Difference profile permits decreases only");
  }
  if (parsePositiveInt(paymentLine.legal_entity_id) !== parsePositiveInt(statementLine.legal_entity_id)) {
    throw badRequest("Payment line legal entity mismatch");
  }
  if (parsePositiveInt(paymentLine.bank_account_id) !== parsePositiveInt(statementLine.bank_account_id)) {
    throw badRequest("Payment line bank account mismatch");
  }
  if (u(paymentLine.batch_status) !== "POSTED") throw badRequest("Only POSTED payment batches are eligible");
  if (u(paymentLine.currency_code) !== u(statementLine.currency_code)) {
    throw badRequest("Payment line currency mismatch");
  }
}

export async function autoMatchPaymentLineWithDifferenceAndReconcile({
  req,
  tenantId,
  lineId,
  paymentBatchLineId,
  differenceProfileId,
  userId,
  ruleId = null,
  confidence = null,
  assertScopeAccess,
}) {
  const statementLine = await getStatementLineWithBank({ tenantId, lineId });
  if (!statementLine) throw badRequest("Statement line not found");
  assertScopeAccess(req, "legal_entity", statementLine.legal_entity_id, "lineId");

  const paymentLine = await getPaymentLineWithBatch({ tenantId, paymentBatchLineId });
  if (!paymentLine) throw badRequest("Payment batch line not found");
  const profile = await getDifferenceProfileForAutomation({ tenantId, profileId: differenceProfileId });
  if (!profile) throw badRequest("Difference profile not found");

  const activeMatchedTotal = await getActiveMatchedTotalForLine({ tenantId, lineId });
  const remaining = remainingAmountAbs(statementLine.amount, activeMatchedTotal);
  if (remaining <= MATCH_EPSILON) {
    return {
      idempotent: true,
      noRemainingAmount: true,
      statementLine,
      paymentLine,
      profile,
      difference: { expectedAmountAbs: 0, actualAmountAbs: 0, diffSigned: 0, diffAbs: 0 },
    };
  }

  const expectedAmountAbs = getExpectedPaymentLineAmountAbs(paymentLine);
  const actualAmountAbs = remaining;
  const diffSigned = toAmount(actualAmountAbs - expectedAmountAbs);
  const diffAbs = absAmount(diffSigned);

  validateProfileAgainstLineAndPayment({ profile, statementLine, paymentLine, diffSigned });
  if (diffAbs <= MATCH_EPSILON) {
    const existingPaymentMatch = await findActivePaymentBatchMatchForLine({
      tenantId,
      lineId,
      paymentBatchId: paymentLine.batch_id,
    });
    const paymentMatch = existingPaymentMatch
      ? { idempotent: true, line: statementLine }
      : await matchReconciliationLine({
          req,
          tenantId,
          lineId,
          matchInput: {
            matchType: "AUTO_RULE",
            matchedEntityType: "PAYMENT_BATCH",
            matchedEntityId: paymentLine.batch_id,
            matchedAmount: actualAmountAbs,
            notes: `B08-B exact payment line match via payment batch ${paymentLine.batch_no || paymentLine.batch_id}`,
            reconciliationMethod: "RULE_DIFF_EXACT",
            reconciliationRuleId: parsePositiveInt(ruleId) || null,
            reconciliationConfidence:
              confidence === null || confidence === undefined
                ? null
                : Number(Number(confidence).toFixed(2)),
          },
          userId,
          assertScopeAccess,
        });
    return {
      idempotent: false,
      statementLine: paymentMatch.line,
      paymentLine,
      profile,
      paymentMatch,
      difference: { expectedAmountAbs, actualAmountAbs, diffSigned, diffAbs },
      adjustment: null,
      journal: null,
    };
  }

  const maxAbs = Number(profile.max_abs_difference || 0);
  if (diffAbs - maxAbs > MATCH_EPSILON) {
    throw badRequest(`Difference ${diffAbs} exceeds profile max_abs_difference ${profile.max_abs_difference}`);
  }

  const paymentMatchAmount = Number(Math.min(expectedAmountAbs, actualAmountAbs).toFixed(6));
  const diffMatchAmount = Number(Math.max(0, actualAmountAbs - paymentMatchAmount).toFixed(6));
  if (diffMatchAmount <= MATCH_EPSILON) {
    throw badRequest("Difference split computed zero diff match amount");
  }

  let paymentMatchResult = null;
  if (paymentMatchAmount > MATCH_EPSILON) {
    const existingPaymentMatch = await findActivePaymentBatchMatchForLine({
      tenantId,
      lineId,
      paymentBatchId: paymentLine.batch_id,
    });
    paymentMatchResult = existingPaymentMatch
      ? { idempotent: true, line: statementLine, matchedAmount: toAmount(existingPaymentMatch.matched_amount) }
      : await matchReconciliationLine({
          req,
          tenantId,
          lineId,
          matchInput: {
            matchType: "AUTO_RULE",
            matchedEntityType: "PAYMENT_BATCH",
            matchedEntityId: paymentLine.batch_id,
            matchedAmount: paymentMatchAmount,
            notes: `B08-B payment component via payment line #${paymentLine.id}`,
            reconciliationMethod: "RULE_DIFF_PAY",
            reconciliationRuleId: parsePositiveInt(ruleId) || null,
            reconciliationConfidence:
              confidence === null || confidence === undefined
                ? null
                : Number(Number(confidence).toFixed(2)),
          },
          userId,
          assertScopeAccess,
        });
  }

  const adj = await createOrReuseDifferenceJournalAndTrace({
    tenantId,
    statementLine,
    paymentLine,
    profile,
    diffSigned,
    userId,
  });

  const journalReconcile = await reconcileStatementLineToJournal({
    req,
    tenantId,
    lineId,
    journalEntryId: adj.journalEntryId,
    userId,
    notes: `B08-B difference component via profile ${profile.profile_code || profile.id}`,
    reconciliationMethod: "RULE_DIFF_ADJ",
    reconciliationRuleId: parsePositiveInt(ruleId) || null,
    reconciliationConfidence:
      confidence === null || confidence === undefined ? null : Number(Number(confidence).toFixed(2)),
    assertScopeAccess,
  });

  return {
    idempotent: Boolean(adj.idempotent && journalReconcile?.idempotent),
    statementLine: journalReconcile?.line || paymentMatchResult?.line || statementLine,
    paymentLine,
    profile,
    paymentMatch: paymentMatchResult,
    adjustment: adj.adjustment,
    journal: {
      id: parsePositiveInt(adj.journalEntryId),
    },
    journalReconciliation: journalReconcile,
    difference: {
      expectedAmountAbs,
      actualAmountAbs,
      diffSigned,
      diffAbs,
      paymentMatchAmount,
      diffMatchAmount,
    },
  };
}

export async function findPaymentLineCandidatesForDifferenceAutomation({
  tenantId,
  line,
  rule,
}) {
  const lag = Number(
    rule?.conditions_json?.dateLagDays ??
      rule?.conditions_json?.date_lag_days ??
      rule?.conditions_json?.dateWindowDays ??
      7
  );
  const safeLag = Number.isInteger(lag) && lag >= 0 && lag <= 365 ? lag : 7;
  const ref = u(line.reference_no);
  const text = u(line.description);
  const tokens = new Set();
  for (const source of [ref, text]) {
    if (!source) continue;
    for (const token of source.split(/[\s,.;:/\\|()[\]-]+/g)) {
      if (token && token.length >= 3) tokens.add(token);
    }
  }

  const res = await query(
    `SELECT
        pbl.id,
        pbl.tenant_id,
        pbl.legal_entity_id,
        pbl.batch_id,
        pbl.line_no,
        pbl.amount,
        pbl.exported_amount,
        pbl.executed_amount,
        pb.currency_code AS currency_code,
        pbl.bank_reference,
        pbl.external_payment_ref,
        pbl.beneficiary_bank_ref,
        pbl.payable_ref,
        pbl.beneficiary_name,
        pbl.bank_execution_status,
        pbl.return_status,
        pbl.returned_amount,
        pb.batch_no,
        pb.bank_account_id,
        pb.status AS batch_status,
        DATE(pb.posted_at) AS posted_date
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
       AND (pb.posted_at IS NULL OR DATE(pb.posted_at) BETWEEN DATE_SUB(?, INTERVAL ${safeLag} DAY) AND DATE_ADD(?, INTERVAL ${safeLag} DAY))
     ORDER BY pbl.id DESC
     LIMIT 300`,
    [tenantId, line.legal_entity_id, line.bank_account_id, line.currency_code, line.txn_date, line.txn_date]
  );

  const actualAbs = absAmount(line.amount);
  const rows = [];
  for (const row of res.rows || []) {
    const expectedAbs = getExpectedPaymentLineAmountAbs(row);
    const diffAbs = absAmount(actualAbs - expectedAbs);
    const blob = u(
      `${row.batch_no || ""} ${row.bank_reference || ""} ${row.external_payment_ref || ""} ${row.beneficiary_bank_ref || ""} ${row.payable_ref || ""} ${row.beneficiary_name || ""}`
    );
    let score = 0;
    if (ref && blob.includes(ref)) score += 50;
    let tokenHits = 0;
    for (const t of tokens) if (blob.includes(t)) tokenHits += 1;
    score += tokenHits * 10;
    if (score <= 0) continue;

    rows.push({
      entityType: "PAYMENT_BATCH_LINE",
      entityId: parsePositiveInt(row.id),
      paymentBatchId: parsePositiveInt(row.batch_id),
      amount: expectedAbs,
      expectedAmountAbs: expectedAbs,
      actualAmountAbs: actualAbs,
      differenceAmountAbs: diffAbs,
      differenceSigned: toAmount(actualAbs - expectedAbs),
      displayRef: row.bank_reference || row.external_payment_ref || `${row.batch_no || "PB"}#${row.line_no}`,
      displayText: row.beneficiary_name || row.payable_ref || null,
      date: row.posted_date || null,
      confidence: Math.min(99, score + (diffAbs <= 0.01 ? 20 : 0)),
    });
  }

  rows.sort((a, b) => b.confidence - a.confidence || (a.differenceAmountAbs - b.differenceAmountAbs) || b.entityId - a.entityId);
  return rows;
}

export default {
  autoMatchPaymentLineWithDifferenceAndReconcile,
  findPaymentLineCandidatesForDifferenceAutomation,
};
