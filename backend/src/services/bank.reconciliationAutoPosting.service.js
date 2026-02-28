import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { getPostingTemplateByIdForAutoPost } from "./bank.reconciliationPostingTemplates.service.js";
import { reconcileStatementLineToJournal } from "./bank.reconciliation.service.js";

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

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function toDateOnly(value) {
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

function buildNarration(template, line) {
  const statementText = String(line?.description || line?.reference_no || "").trim();
  const mode = u(template?.description_mode || "USE_STATEMENT_TEXT");
  if (mode === "FIXED_TEXT") {
    return String(template?.fixed_description || "").trim() || statementText || "Bank auto-post";
  }
  if (mode === "PREFIXED") {
    const prefix = String(template?.description_prefix || "Bank").trim();
    return [prefix, statementText].filter(Boolean).join(" ").trim() || "Bank auto-post";
  }
  return statementText || String(template?.template_name || "").trim() || "Bank auto-post";
}

function validateDirection(template, line) {
  const policy = u(template?.direction_policy || "BOTH");
  const amount = Number(line?.amount || 0);
  if (!Number.isFinite(amount) || amount === 0) {
    throw badRequest("Statement line amount must be non-zero");
  }
  if (policy === "OUTFLOW_ONLY" && amount >= 0) {
    throw badRequest("Template applies only to outflows");
  }
  if (policy === "INFLOW_ONLY" && amount <= 0) {
    throw badRequest("Template applies only to inflows");
  }
}

function validateTemplateEffective(template, line) {
  const d = toDateOnly(line?.txn_date);
  const from = toDateOnly(template?.effective_from);
  const to = toDateOnly(template?.effective_to);
  if (from && d && d < from) throw badRequest("Template not effective for statement line date");
  if (to && d && d > to) throw badRequest("Template not effective for statement line date");
}

function validateTemplateAmountFilters(template, line) {
  const amount = absAmount(line?.amount);
  const minAmount = template?.min_amount_abs === null ? null : Number(template?.min_amount_abs);
  const maxAmount = template?.max_amount_abs === null ? null : Number(template?.max_amount_abs);
  if (Number.isFinite(minAmount) && amount < Number(minAmount.toFixed(6))) {
    throw badRequest("Statement amount is below template minimum");
  }
  if (Number.isFinite(maxAmount) && amount > Number(maxAmount.toFixed(6))) {
    throw badRequest("Statement amount is above template maximum");
  }
}

async function getStatementLineForAutoPost({ tenantId, lineId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        l.id,
        l.tenant_id,
        l.legal_entity_id,
        l.bank_account_id,
        l.txn_date,
        l.value_date,
        l.description,
        l.reference_no,
        l.amount,
        l.currency_code,
        l.recon_status,
        l.auto_post_template_id,
        l.auto_post_journal_entry_id,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        ba.currency_code AS bank_account_currency_code,
        ba.gl_account_id AS bank_gl_account_id
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
  return result.rows?.[0] || null;
}

async function getAutoPostingByLine({ tenantId, legalEntityId, lineId, runQuery = query }) {
  const result = await runQuery(
    `SELECT *
     FROM bank_reconciliation_auto_postings
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND statement_line_id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, lineId]
  );
  return result.rows?.[0] || null;
}

async function getJournalById({ tenantId, legalEntityId, journalEntryId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, tenant_id, legal_entity_id, journal_no, status, entry_date, currency_code, description, reference_no
     FROM journal_entries
     WHERE id = ?
       AND tenant_id = ?
       AND legal_entity_id = ?
     LIMIT 1`,
    [journalEntryId, tenantId, legalEntityId]
  );
  return result.rows?.[0] || null;
}

async function getCounterAccount({ tenantId, accountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        a.id,
        c.tenant_id AS tenant_id,
        c.legal_entity_id AS legal_entity_id,
        c.scope AS scope,
        a.account_type,
        a.allow_posting,
        a.is_active,
        a.code,
        a.name
     FROM accounts a
     JOIN charts_of_accounts c
       ON c.id = a.coa_id
     WHERE c.tenant_id = ?
       AND a.id = ?
     LIMIT 1`,
    [tenantId, accountId]
  );
  return result.rows?.[0] || null;
}

async function resolveBookAndPeriodForPostingTx(tx, { tenantId, legalEntityId, postDate }) {
  const bookResult = await tx.query(
    `SELECT id, calendar_id, base_currency_code, code, name, book_type
     FROM books
     WHERE tenant_id = ?
       AND legal_entity_id = ?
     ORDER BY CASE WHEN book_type = 'LOCAL' THEN 0 ELSE 1 END, id ASC
     LIMIT 1`,
    [tenantId, legalEntityId]
  );
  const book = bookResult.rows?.[0] || null;
  if (!book) throw badRequest("No book found for legal entity");

  const bookId = parsePositiveInt(book.id);
  const calendarId = parsePositiveInt(book.calendar_id);
  if (!bookId || !calendarId) throw badRequest("Book configuration is invalid");

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
  if (!period) throw badRequest("No fiscal period found for posting date");
  const fiscalPeriodId = parsePositiveInt(period.id);
  if (!fiscalPeriodId) throw badRequest("Fiscal period configuration is invalid");

  const statusResult = await tx.query(
    `SELECT status
     FROM period_statuses
     WHERE book_id = ?
       AND fiscal_period_id = ?
     LIMIT 1`,
    [bookId, fiscalPeriodId]
  );
  const periodStatus = u(statusResult.rows?.[0]?.status || "OPEN") || "OPEN";
  if (periodStatus !== "OPEN") {
    throw badRequest(`Period is ${periodStatus}; cannot auto-post bank statement line`);
  }

  return {
    book,
    period,
    bookId,
    fiscalPeriodId,
  };
}

function resolveTemplateTaxConfig(template) {
  const taxMode = u(template?.tax_mode || "NONE");
  if (taxMode === "NONE") {
    return {
      taxMode: "NONE",
      taxAccountId: null,
      taxRate: null,
      taxRateFraction: 0,
    };
  }
  if (taxMode !== "INCLUDED") {
    throw badRequest(`Unsupported tax_mode for auto-post template: ${taxMode}`);
  }
  const taxAccountId = parsePositiveInt(template?.tax_account_id);
  if (!taxAccountId) {
    throw badRequest("Template tax_account_id is required when tax_mode=INCLUDED");
  }
  const taxRate = Number(template?.tax_rate);
  if (!Number.isFinite(taxRate) || taxRate <= 0 || taxRate >= 100) {
    throw badRequest("Template tax_rate must be > 0 and < 100 when tax_mode=INCLUDED");
  }
  return {
    taxMode: "INCLUDED",
    taxAccountId,
    taxRate: Number(taxRate.toFixed(4)),
    taxRateFraction: Number((taxRate / 100).toFixed(8)),
  };
}

function splitIncludedTaxAmount(totalAmountAbs, taxRateFraction) {
  if (taxRateFraction <= 0) {
    throw badRequest("Tax rate configuration is invalid");
  }
  const baseAmount = Number((totalAmountAbs / (1 + taxRateFraction)).toFixed(6));
  const taxAmount = Number((totalAmountAbs - baseAmount).toFixed(6));
  if (baseAmount <= 0 || taxAmount <= 0) {
    throw badRequest("Tax split produced non-positive components");
  }
  return {
    baseAmount,
    taxAmount,
  };
}

function validateTemplateForLine({ template, line }) {
  if (!template) throw badRequest("Posting template not found");
  if (u(template.status) !== "ACTIVE") throw badRequest("Posting template is not ACTIVE");
  resolveTemplateTaxConfig(template);

  validateTemplateEffective(template, line);
  validateDirection(template, line);
  validateTemplateAmountFilters(template, line);

  if (template.currency_code && u(template.currency_code) !== u(line.currency_code)) {
    throw badRequest("Template currency does not match statement line currency");
  }

  const scopeType = u(template.scope_type);
  if (scopeType === "BANK_ACCOUNT") {
    if (parsePositiveInt(template.bank_account_id) !== parsePositiveInt(line.bank_account_id)) {
      throw badRequest("Template is not scoped to this bank account");
    }
  } else if (scopeType === "LEGAL_ENTITY" || scopeType === "GLOBAL") {
    if (parsePositiveInt(template.legal_entity_id) !== parsePositiveInt(line.legal_entity_id)) {
      throw badRequest("Template is not scoped to this legal entity");
    }
  } else {
    throw badRequest("Unsupported template scope");
  }
}

function buildJournalLinePayloads({ template, line, bankGlAccountId, narration }) {
  const amountAbs = absAmount(line.amount);
  const counterAccountId = parsePositiveInt(template.counter_account_id);
  const parsedBankGlAccountId = parsePositiveInt(bankGlAccountId);
  if (!counterAccountId || !parsedBankGlAccountId) {
    throw badRequest("Bank GL or counter account is missing");
  }

  const taxConfig = resolveTemplateTaxConfig(template);
  let counterAmount = amountAbs;
  let taxAmount = 0;
  if (taxConfig.taxMode === "INCLUDED") {
    const split = splitIncludedTaxAmount(amountAbs, taxConfig.taxRateFraction);
    counterAmount = split.baseAmount;
    taxAmount = split.taxAmount;
  }

  if (Number(line.amount) < 0) {
    // Outflow: Dr counter (+ optional Dr tax) / Cr bank
    if (taxConfig.taxMode === "INCLUDED") {
      return [
        {
          lineNo: 1,
          accountId: counterAccountId,
          amountTxn: counterAmount,
          debitBase: counterAmount,
          creditBase: 0,
          description: narration,
          subledgerReferenceNo: `BANKAUTOP:${line.id}:DR:COUNTER`,
        },
        {
          lineNo: 2,
          accountId: taxConfig.taxAccountId,
          amountTxn: taxAmount,
          debitBase: taxAmount,
          creditBase: 0,
          description: narration,
          subledgerReferenceNo: `BANKAUTOP:${line.id}:DR:TAX`,
        },
        {
          lineNo: 3,
          accountId: parsedBankGlAccountId,
          amountTxn: -amountAbs,
          debitBase: 0,
          creditBase: amountAbs,
          description: narration,
          subledgerReferenceNo: `BANKAUTOP:${line.id}:CR:BANK`,
        },
      ];
    }
    return [
      {
        lineNo: 1,
        accountId: counterAccountId,
        amountTxn: amountAbs,
        debitBase: amountAbs,
        creditBase: 0,
        description: narration,
        subledgerReferenceNo: `BANKAUTOP:${line.id}:DR`,
      },
      {
        lineNo: 2,
        accountId: parsedBankGlAccountId,
        amountTxn: -amountAbs,
        debitBase: 0,
        creditBase: amountAbs,
        description: narration,
        subledgerReferenceNo: `BANKAUTOP:${line.id}:CR`,
      },
    ];
  }

  // Inflow: Dr bank / Cr counter (+ optional Cr tax)
  if (taxConfig.taxMode === "INCLUDED") {
    return [
      {
        lineNo: 1,
        accountId: parsedBankGlAccountId,
        amountTxn: amountAbs,
        debitBase: amountAbs,
        creditBase: 0,
        description: narration,
        subledgerReferenceNo: `BANKAUTOP:${line.id}:DR:BANK`,
      },
      {
        lineNo: 2,
        accountId: counterAccountId,
        amountTxn: -counterAmount,
        debitBase: 0,
        creditBase: counterAmount,
        description: narration,
        subledgerReferenceNo: `BANKAUTOP:${line.id}:CR:COUNTER`,
      },
      {
        lineNo: 3,
        accountId: taxConfig.taxAccountId,
        amountTxn: -taxAmount,
        debitBase: 0,
        creditBase: taxAmount,
        description: narration,
        subledgerReferenceNo: `BANKAUTOP:${line.id}:CR:TAX`,
      },
    ];
  }
  return [
    {
      lineNo: 1,
      accountId: parsedBankGlAccountId,
      amountTxn: amountAbs,
      debitBase: amountAbs,
      creditBase: 0,
      description: narration,
      subledgerReferenceNo: `BANKAUTOP:${line.id}:DR`,
    },
    {
      lineNo: 2,
      accountId: counterAccountId,
      amountTxn: -amountAbs,
      debitBase: 0,
      creditBase: amountAbs,
      description: narration,
      subledgerReferenceNo: `BANKAUTOP:${line.id}:CR`,
    },
  ];
}

async function insertOrReuseAutoPostJournalTx(tx, { tenantId, line, template, userId }) {
  const postDate = toDateOnly(line.txn_date);
  if (!postDate) throw badRequest("Statement line txn_date is required for auto-posting");

  const bankGlAccountId = parsePositiveInt(line.bank_gl_account_id);
  if (!bankGlAccountId) throw badRequest("Bank account GL mapping is missing");

  const counterAccount = await getCounterAccount({
    tenantId,
    accountId: parsePositiveInt(template.counter_account_id),
    runQuery: tx.query,
  });
  function assertTemplateAccount(account, label) {
    if (!account) throw badRequest(`${label} not found`);
    if (!parseDbBoolean(account.is_active)) throw badRequest(`${label} is inactive`);
    if (!parseDbBoolean(account.allow_posting)) throw badRequest(`${label} is not postable`);
    if (u(account.scope) !== "LEGAL_ENTITY") {
      throw badRequest(`${label} must be LEGAL_ENTITY scoped`);
    }
    if (parsePositiveInt(account.legal_entity_id) !== parsePositiveInt(line.legal_entity_id)) {
      throw badRequest(`${label} legal entity mismatch`);
    }
  }
  assertTemplateAccount(counterAccount, "Template counter account");

  const taxConfig = resolveTemplateTaxConfig(template);
  if (taxConfig.taxMode === "INCLUDED") {
    const taxAccount = await getCounterAccount({
      tenantId,
      accountId: taxConfig.taxAccountId,
      runQuery: tx.query,
    });
    assertTemplateAccount(taxAccount, "Template tax account");
  }

  const journalContext = await resolveBookAndPeriodForPostingTx(tx, {
    tenantId,
    legalEntityId: line.legal_entity_id,
    postDate,
  });
  const bookCurrency = u(journalContext.book?.base_currency_code);
  if (bookCurrency && bookCurrency !== u(line.currency_code)) {
    throw badRequest(
      `Statement currency (${line.currency_code}) must match book base currency (${bookCurrency})`
    );
  }

  const journalNo = `BAP-${line.id}`;
  const existingJournalResult = await tx.query(
    `SELECT id, journal_no, status
     FROM journal_entries
     WHERE book_id = ?
       AND journal_no = ?
     LIMIT 1`,
    [journalContext.bookId, journalNo]
  );
  const existingJournal = existingJournalResult.rows?.[0] || null;
  if (existingJournal) {
    if (u(existingJournal.status) !== "POSTED") {
      throw badRequest("Existing auto-post journal is not POSTED");
    }
    return {
      journalEntryId: parsePositiveInt(existingJournal.id),
      journalNo: existingJournal.journal_no,
      reused: true,
      postingDate: postDate,
      narration: buildNarration(template, line),
      totalAmount: absAmount(line.amount),
    };
  }

  const narration = buildNarration(template, line);
  const total = absAmount(line.amount);
  const referenceNo = line.reference_no
    ? String(line.reference_no).slice(0, 100)
    : `BSL-${line.id}`;

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
      line.legal_entity_id,
      journalContext.bookId,
      journalContext.fiscalPeriodId,
      journalNo,
      postDate,
      postDate,
      u(line.currency_code),
      narration,
      referenceNo,
      total,
      total,
      userId,
      userId,
    ]
  );

  const journalEntryId = parsePositiveInt(headerInsert.rows?.insertId);
  if (!journalEntryId) throw new Error("Failed to create auto-post journal");

  const linePayloads = buildJournalLinePayloads({
    template,
    line,
    bankGlAccountId,
    narration,
  });
  for (const jl of linePayloads) {
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
        VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, NULL)`,
      [
        journalEntryId,
        jl.lineNo,
        jl.accountId,
        jl.description,
        jl.subledgerReferenceNo,
        u(line.currency_code),
        jl.amountTxn,
        jl.debitBase,
        jl.creditBase,
      ]
    );
  }

  return {
    journalEntryId,
    journalNo,
    reused: false,
    postingDate: postDate,
    narration,
    totalAmount: total,
  };
}

async function ensureAutoPostingTraceTx(tx, { tenantId, line, template, journal, userId }) {
  const existing = await getAutoPostingByLine({
    tenantId,
    legalEntityId: line.legal_entity_id,
    lineId: line.id,
    runQuery: tx.query,
  });
  if (existing) {
    return existing;
  }

  const insertResult = await tx.query(
    `INSERT INTO bank_reconciliation_auto_postings (
        tenant_id,
        legal_entity_id,
        statement_line_id,
        bank_reconciliation_posting_template_id,
        journal_entry_id,
        status,
        posted_amount,
        currency_code,
        payload_json,
        created_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, 'POSTED', ?, ?, ?, ?)`,
    [
      tenantId,
      line.legal_entity_id,
      line.id,
      template.id,
      journal.journalEntryId,
      journal.totalAmount,
      u(line.currency_code),
      safeJson({
        template_code: template.template_code,
        template_name: template.template_name,
        tax_mode: u(template.tax_mode || "NONE"),
        tax_account_id: parsePositiveInt(template.tax_account_id) || null,
        tax_rate: template.tax_rate === null ? null : Number(Number(template.tax_rate).toFixed(4)),
        bank_account_code: line.bank_account_code,
        bank_account_id: line.bank_account_id,
        statement_ref: line.reference_no || null,
        narration: journal.narration,
      }),
      userId || null,
    ]
  );

  const autoPostingId = parsePositiveInt(insertResult.rows?.insertId);
  const traceResult = await tx.query(
    `SELECT *
     FROM bank_reconciliation_auto_postings
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, line.legal_entity_id, autoPostingId]
  );
  return traceResult.rows?.[0] || null;
}

export async function autoPostTemplateAndReconcileStatementLine({
  req = null,
  tenantId,
  lineId,
  templateId,
  userId,
  ruleId = null,
  confidence = null,
  assertScopeAccess = null,
}) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedLineId = parsePositiveInt(lineId);
  const parsedTemplateId = parsePositiveInt(templateId);
  const parsedUserId = parsePositiveInt(userId);
  if (!parsedTenantId || !parsedLineId || !parsedTemplateId) {
    throw badRequest("tenantId, lineId, and templateId are required");
  }
  if (!parsedUserId) {
    throw badRequest("userId is required for bank auto-posting");
  }

  const lineBefore = await getStatementLineForAutoPost({
    tenantId: parsedTenantId,
    lineId: parsedLineId,
  });
  if (!lineBefore) throw badRequest("Statement line not found");
  if (req && typeof assertScopeAccess === "function") {
    assertScopeAccess(req, "legal_entity", lineBefore.legal_entity_id, "lineId");
  }

  const template = await getPostingTemplateByIdForAutoPost({
    tenantId: parsedTenantId,
    templateId: parsedTemplateId,
  });
  validateTemplateForLine({ template, line: lineBefore });

  const txResult = await withTransaction(async (tx) => {
    const line = await getStatementLineForAutoPost({
      tenantId: parsedTenantId,
      lineId: parsedLineId,
      runQuery: tx.query,
    });
    if (!line) throw badRequest("Statement line not found");

    const existingTrace = await getAutoPostingByLine({
      tenantId: parsedTenantId,
      legalEntityId: line.legal_entity_id,
      lineId: line.id,
      runQuery: tx.query,
    });

    let trace = existingTrace;
    let journalSummary = null;
    let idempotentJournal = false;

    if (trace) {
      const existingJournal = await getJournalById({
        tenantId: parsedTenantId,
        legalEntityId: line.legal_entity_id,
        journalEntryId: trace.journal_entry_id,
        runQuery: tx.query,
      });
      if (!existingJournal) {
        throw badRequest("Auto-post trace references missing journal entry");
      }
      journalSummary = {
        journalEntryId: parsePositiveInt(existingJournal.id),
        journalNo: existingJournal.journal_no,
        reused: true,
        totalAmount: absAmount(line.amount),
        narration: existingJournal.description || buildNarration(template, line),
      };
      idempotentJournal = true;
    } else {
      const journal = await insertOrReuseAutoPostJournalTx(tx, {
        tenantId: parsedTenantId,
        line,
        template,
        userId: parsedUserId,
      });
      journalSummary = journal;
      trace = await ensureAutoPostingTraceTx(tx, {
        tenantId: parsedTenantId,
        line,
        template,
        journal,
        userId: parsedUserId,
      });
      await tx.query(
        `UPDATE bank_statement_lines
         SET auto_post_template_id = ?,
             auto_post_journal_entry_id = ?
         WHERE tenant_id = ?
           AND id = ?`,
        [template.id, journal.journalEntryId, parsedTenantId, line.id]
      );
    }

    const lineAfterTx = await getStatementLineForAutoPost({
      tenantId: parsedTenantId,
      lineId: parsedLineId,
      runQuery: tx.query,
    });

    return {
      line: lineAfterTx,
      trace,
      journal: journalSummary,
      idempotentJournal,
    };
  });

  const reconcileResult = await reconcileStatementLineToJournal({
    req,
    tenantId: parsedTenantId,
    lineId: parsedLineId,
    journalEntryId: txResult.journal.journalEntryId,
    userId: parsedUserId,
    notes: `Auto-posted from B08 template ${template.template_code || template.id}`,
    reconciliationMethod: "RULE_AUTO_POST",
    reconciliationRuleId: parsePositiveInt(ruleId) || null,
    reconciliationConfidence:
      confidence === null || confidence === undefined ? null : Number(Number(confidence).toFixed(2)),
    assertScopeAccess: assertScopeAccess || (() => {}),
  });

  return {
    idempotent: Boolean(txResult.idempotentJournal && reconcileResult?.idempotent),
    journal: {
      id: txResult.journal.journalEntryId,
      journal_no: txResult.journal.journalNo,
      reused: Boolean(txResult.journal.reused),
    },
    template,
    auto_posting: txResult.trace,
    line: reconcileResult?.line || txResult.line,
    matches: reconcileResult?.matches || [],
    reconciliation: {
      idempotent: Boolean(reconcileResult?.idempotent),
      matchedAmount: Number(reconcileResult?.matchedAmount || 0),
    },
  };
}

export default {
  autoPostTemplateAndReconcileStatementLine,
};
