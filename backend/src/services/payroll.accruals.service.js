import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  buildPayrollAccrualComponentAmountsFromRun,
  EXPECTED_SIDE_BY_COMPONENT,
  findApplicablePayrollComponentMapping,
} from "./payroll.mappings.service.js";

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
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

function amountsEqual(a, b) {
  return toAmount(a) === toAmount(b);
}

function toDateOnly(value) {
  if (!value) {
    return null;
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      return null;
    }
    return value.toISOString().slice(0, 10);
  }
  const asString = String(value);
  if (/^\d{4}-\d{2}-\d{2}/.test(asString)) {
    return asString.slice(0, 10);
  }
  const parsed = new Date(asString);
  if (Number.isNaN(parsed.getTime())) {
    return asString.slice(0, 10);
  }
  return parsed.toISOString().slice(0, 10);
}

async function findPayrollRunHeaderById({ tenantId, runId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        r.id,
        r.tenant_id,
        r.legal_entity_id,
        r.run_no,
        r.provider_code,
        r.entity_code,
        r.payroll_period,
        r.pay_date,
        r.currency_code,
        r.status,
        r.total_base_salary,
        r.total_overtime_pay,
        r.total_bonus_pay,
        r.total_allowances,
        r.total_gross_pay,
        r.total_employee_tax,
        r.total_employee_social_security,
        r.total_other_deductions,
        r.total_net_pay,
        r.total_employer_tax,
        r.total_employer_social_security,
        r.reviewed_by_user_id,
        r.reviewed_at,
        r.finalized_by_user_id,
        r.finalized_at,
        r.accrual_journal_entry_id,
        r.accrual_posted_by_user_id,
        r.accrual_posted_at,
        r.imported_at,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM payroll_runs r
     JOIN legal_entities le
       ON le.id = r.legal_entity_id
      AND le.tenant_id = r.tenant_id
     WHERE r.tenant_id = ?
       AND r.id = ?
     LIMIT 1`,
    [tenantId, runId]
  );
  const row = result.rows?.[0] || null;
  if (!row) {
    return null;
  }
  return {
    ...row,
    payroll_period: toDateOnly(row.payroll_period),
    pay_date: toDateOnly(row.pay_date),
    reviewed_at: row.reviewed_at ? String(row.reviewed_at) : null,
    finalized_at: row.finalized_at ? String(row.finalized_at) : null,
    imported_at: row.imported_at ? String(row.imported_at) : null,
  };
}

async function findPayrollRunHeaderForUpdate({ tenantId, runId, runQuery }) {
  const result = await runQuery(
    `SELECT *
     FROM payroll_runs
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1
     FOR UPDATE`,
    [tenantId, runId]
  );
  return result.rows?.[0] || null;
}

async function writePayrollRunAudit({
  tenantId,
  legalEntityId,
  runId,
  action = "STATUS",
  payload = null,
  userId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO payroll_run_audit (
        tenant_id,
        legal_entity_id,
        run_id,
        action,
        payload_json,
        acted_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, runId, action, safeJson(payload), userId]
  );
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

async function resolveBookAndPeriodForPayrollPostingTx(tx, { tenantId, legalEntityId, postDate }) {
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
    throw badRequest("No book found for payroll run legal entity");
  }

  const bookId = parsePositiveInt(book.id);
  const calendarId = parsePositiveInt(book.calendar_id);
  if (!bookId || !calendarId) {
    throw badRequest("Book configuration is invalid for payroll accrual posting");
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
    throw badRequest("No fiscal period found for payroll accrual posting date");
  }

  const fiscalPeriodId = parsePositiveInt(period.id);
  if (!fiscalPeriodId) {
    throw badRequest("Fiscal period configuration is invalid for payroll accrual posting");
  }

  await ensurePeriodOpen(bookId, fiscalPeriodId, "post payroll accrual", tx.query.bind(tx));

  return {
    book,
    period,
    bookId,
    fiscalPeriodId,
  };
}

function validateMappingAccountForAccrual({ mapping, run }) {
  const issues = [];

  if (normalizeUpperText(mapping.coa_scope) !== "LEGAL_ENTITY") {
    issues.push("mapping_gl_account_not_legal_entity_scope");
  }
  if (parsePositiveInt(mapping.coa_legal_entity_id) !== parsePositiveInt(run.legal_entity_id)) {
    issues.push("mapping_gl_account_entity_mismatch");
  }
  if (!parseDbBoolean(mapping.account_is_active)) {
    issues.push("mapping_gl_account_inactive");
  }
  if (!parseDbBoolean(mapping.allow_posting)) {
    issues.push("mapping_gl_account_not_postable");
  }
  if (Number(mapping.child_count || 0) > 0) {
    issues.push("mapping_gl_account_not_leaf");
  }
  return issues;
}

async function buildPayrollAccrualPreviewFromRun({
  run,
  runQuery = query,
}) {
  if (!run) {
    throw badRequest("Payroll run not found");
  }

  const componentAmounts = buildPayrollAccrualComponentAmountsFromRun(run);
  const postingLines = [];
  const missingMappings = [];

  for (const component of componentAmounts) {
    const mapping = await findApplicablePayrollComponentMapping({
      tenantId: parsePositiveInt(run.tenant_id),
      legalEntityId: parsePositiveInt(run.legal_entity_id),
      providerCode: run.provider_code,
      currencyCode: normalizeUpperText(run.currency_code),
      componentCode: component.componentCode,
      asOfDate: toDateOnly(run.pay_date),
      runQuery,
    });

    if (!mapping) {
      missingMappings.push({
        component_code: component.componentCode,
        entry_side: component.entrySide,
        amount: component.amount,
        issue: "missing_mapping",
      });
      continue;
    }

    const expectedSide = EXPECTED_SIDE_BY_COMPONENT[component.componentCode];
    if (expectedSide && normalizeUpperText(mapping.entry_side) !== expectedSide) {
      missingMappings.push({
        component_code: component.componentCode,
        entry_side: component.entrySide,
        amount: component.amount,
        issue: `mapping_entry_side_mismatch_expected_${expectedSide}`,
        mapping_id: parsePositiveInt(mapping.id),
      });
      continue;
    }
    if (normalizeUpperText(mapping.entry_side) !== normalizeUpperText(component.entrySide)) {
      missingMappings.push({
        component_code: component.componentCode,
        entry_side: component.entrySide,
        amount: component.amount,
        issue: "mapping_entry_side_mismatch_component",
        mapping_id: parsePositiveInt(mapping.id),
      });
      continue;
    }

    const accountIssues = validateMappingAccountForAccrual({ mapping, run });
    if (accountIssues.length > 0) {
      missingMappings.push({
        component_code: component.componentCode,
        entry_side: component.entrySide,
        amount: component.amount,
        issue: accountIssues.join(","),
        mapping_id: parsePositiveInt(mapping.id),
      });
      continue;
    }

    postingLines.push({
      component_code: component.componentCode,
      entry_side: normalizeUpperText(component.entrySide),
      amount: component.amount,
      mapping_id: parsePositiveInt(mapping.id),
      provider_code: mapping.provider_code || null,
      gl_account_id: parsePositiveInt(mapping.gl_account_id),
      gl_account_code: mapping.gl_account_code || null,
      gl_account_name: mapping.gl_account_name || null,
      currency_code: normalizeUpperText(run.currency_code),
    });
  }

  const debitTotal = toAmount(
    postingLines
      .filter((line) => line.entry_side === "DEBIT")
      .reduce((sum, line) => sum + toAmount(line.amount), 0)
  );
  const creditTotal = toAmount(
    postingLines
      .filter((line) => line.entry_side === "CREDIT")
      .reduce((sum, line) => sum + toAmount(line.amount), 0)
  );

  const isBalanced = amountsEqual(debitTotal, creditTotal);
  const normalizedStatus = normalizeUpperText(run.status);

  return {
    run: {
      id: parsePositiveInt(run.id),
      run_no: run.run_no,
      status: normalizedStatus,
      pay_date: toDateOnly(run.pay_date),
      payroll_period: toDateOnly(run.payroll_period),
      currency_code: normalizeUpperText(run.currency_code),
      provider_code: normalizeUpperText(run.provider_code),
      accrual_journal_entry_id: parsePositiveInt(run.accrual_journal_entry_id),
      legal_entity_id: parsePositiveInt(run.legal_entity_id),
      legal_entity_code: run.legal_entity_code || run.entity_code || null,
      legal_entity_name: run.legal_entity_name || null,
    },
    component_totals: componentAmounts.map((row) => ({
      component_code: row.componentCode,
      entry_side: row.entrySide,
      amount: row.amount,
    })),
    posting_lines: postingLines,
    missing_mappings: missingMappings,
    debit_total: debitTotal,
    credit_total: creditTotal,
    is_balanced: isBalanced,
    can_finalize:
      postingLines.length > 0 &&
      missingMappings.length === 0 &&
      isBalanced &&
      normalizedStatus === "REVIEWED",
  };
}

async function findExistingPayrollAccrualJournalTx(tx, {
  tenantId,
  legalEntityId,
  bookId,
  journalNo,
}) {
  const result = await tx.query(
    `SELECT id, status
     FROM journal_entries
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND book_id = ?
       AND journal_no = ?
     LIMIT 1`,
    [tenantId, legalEntityId, bookId, journalNo]
  );
  return result.rows?.[0] || null;
}

async function createPayrollAccrualJournalTx(tx, {
  run,
  preview,
  tenantId,
  legalEntityId,
  userId,
  postingDate,
  note = null,
}) {
  const journalContext = await resolveBookAndPeriodForPayrollPostingTx(tx, {
    tenantId,
    legalEntityId,
    postDate: postingDate,
  });

  const bookBaseCurrencyCode = normalizeUpperText(journalContext.book?.base_currency_code);
  const runCurrency = normalizeUpperText(run.currency_code);
  if (bookBaseCurrencyCode && runCurrency && bookBaseCurrencyCode !== runCurrency) {
    throw badRequest(
      `Payroll run currency (${runCurrency}) must match book base currency (${bookBaseCurrencyCode})`
    );
  }

  const journalNo = `PRACR-${run.id}`;
  const existingJournal = await findExistingPayrollAccrualJournalTx(tx, {
    tenantId,
    legalEntityId,
    bookId: journalContext.bookId,
    journalNo,
  });

  if (existingJournal?.id) {
    return {
      journalEntryId: parsePositiveInt(existingJournal.id),
      journalNo,
      idempotentReplay: true,
      bookId: journalContext.bookId,
      fiscalPeriodId: journalContext.fiscalPeriodId,
    };
  }

  const description = note || `Payroll accrual ${run.run_no}`;
  const referenceNo = `PAYROLL-RUN:${run.id}`;
  const debitTotal = toAmount(preview.debit_total);
  const creditTotal = toAmount(preview.credit_total);

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
      runCurrency,
      description,
      referenceNo,
      debitTotal,
      creditTotal,
      userId,
      userId,
    ]
  );

  const journalEntryId = parsePositiveInt(headerInsert.rows?.insertId);
  if (!journalEntryId) {
    throw new Error("Failed to create payroll accrual journal");
  }

  let lineNo = 1;
  for (const line of preview.posting_lines || []) {
    const amount = toAmount(line.amount);
    const isDebit = normalizeUpperText(line.entry_side) === "DEBIT";
    const amountTxn = isDebit ? amount : -amount;
    const debitBase = isDebit ? amount : 0;
    const creditBase = isDebit ? 0 : amount;
    const subledgerRef = `PAYROLL_RUN:${run.id}:${line.component_code}`;
    const descriptionLine = `Payroll accrual ${run.run_no} ${line.component_code}`;

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
        lineNo,
        parsePositiveInt(line.gl_account_id),
        descriptionLine,
        subledgerRef,
        runCurrency,
        amountTxn,
        debitBase,
        creditBase,
      ]
    );

    lineNo += 1;
  }

  return {
    journalEntryId,
    journalNo,
    idempotentReplay: false,
    bookId: journalContext.bookId,
    fiscalPeriodId: journalContext.fiscalPeriodId,
  };
}

export async function getPayrollRunAccrualPreview({
  req,
  tenantId,
  runId,
  assertScopeAccess,
}) {
  const run = await findPayrollRunHeaderById({ tenantId, runId });
  if (!run) {
    throw badRequest("Payroll run not found");
  }

  assertScopeAccess(req, "legal_entity", parsePositiveInt(run.legal_entity_id), "runId");
  return buildPayrollAccrualPreviewFromRun({ run });
}

export async function markPayrollRunReviewed({
  req,
  tenantId,
  runId,
  userId,
  note,
  assertScopeAccess,
}) {
  return withTransaction(async (tx) => {
    const current = await findPayrollRunHeaderForUpdate({
      tenantId,
      runId,
      runQuery: tx.query,
    });
    if (!current) {
      throw badRequest("Payroll run not found");
    }

    assertScopeAccess(req, "legal_entity", parsePositiveInt(current.legal_entity_id), "runId");
    const currentStatus = normalizeUpperText(current.status);
    if (currentStatus === "FINALIZED") {
      return {
        runId: parsePositiveInt(current.id),
        idempotentReplay: true,
        status: currentStatus,
      };
    }
    if (currentStatus === "REVIEWED") {
      return {
        runId: parsePositiveInt(current.id),
        idempotentReplay: true,
        status: currentStatus,
      };
    }
    if (currentStatus !== "IMPORTED") {
      throw badRequest(`Payroll run status ${currentStatus} cannot be reviewed`);
    }

    await tx.query(
      `UPDATE payroll_runs
       SET status = 'REVIEWED',
           reviewed_by_user_id = ?,
           reviewed_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [userId, tenantId, runId]
    );

    await writePayrollRunAudit({
      tenantId,
      legalEntityId: parsePositiveInt(current.legal_entity_id),
      runId,
      action: "STATUS",
      payload: {
        fromStatus: currentStatus,
        toStatus: "REVIEWED",
        note: note || null,
      },
      userId,
      runQuery: tx.query,
    });

    return {
      runId,
      idempotentReplay: false,
      status: "REVIEWED",
    };
  });
}

export async function finalizePayrollRunAccrual({
  req,
  tenantId,
  runId,
  userId,
  note,
  forceFromImported = false,
  assertScopeAccess,
}) {
  return withTransaction(async (tx) => {
    const current = await findPayrollRunHeaderForUpdate({
      tenantId,
      runId,
      runQuery: tx.query,
    });
    if (!current) {
      throw badRequest("Payroll run not found");
    }

    assertScopeAccess(req, "legal_entity", parsePositiveInt(current.legal_entity_id), "runId");

    const currentStatus = normalizeUpperText(current.status);
    const currentAccrualJeId = parsePositiveInt(current.accrual_journal_entry_id);
    if (currentStatus === "FINALIZED" && currentAccrualJeId) {
      return {
        runId,
        accrualJournalEntryId: currentAccrualJeId,
        idempotentReplay: true,
      };
    }

    if (currentStatus === "IMPORTED" && !forceFromImported) {
      throw badRequest("Payroll run must be REVIEWED before finalize");
    }
    if (!["IMPORTED", "REVIEWED", "FINALIZED"].includes(currentStatus)) {
      throw badRequest(`Payroll run status ${currentStatus} cannot be finalized`);
    }

    const currentForPreview = {
      ...current,
      legal_entity_code: current.entity_code,
      legal_entity_name: null,
    };
    const preview = await buildPayrollAccrualPreviewFromRun({
      run: currentForPreview,
      runQuery: tx.query,
    });

    if (preview.missing_mappings.length > 0) {
      const missingCodes = Array.from(
        new Set(preview.missing_mappings.map((m) => m.component_code).filter(Boolean))
      );

      await writePayrollRunAudit({
        tenantId,
        legalEntityId: parsePositiveInt(current.legal_entity_id),
        runId,
        action: "VALIDATION",
        payload: {
          type: "ACCRUAL_FINALIZE_BLOCKED",
          reason: "MISSING_MAPPINGS",
          missingComponents: missingCodes,
          missingMappings: preview.missing_mappings,
        },
        userId,
        runQuery: tx.query,
      });

      throw badRequest(`Missing payroll component mappings: ${missingCodes.join(", ")}`);
    }
    if ((preview.posting_lines || []).length === 0) {
      await writePayrollRunAudit({
        tenantId,
        legalEntityId: parsePositiveInt(current.legal_entity_id),
        runId,
        action: "VALIDATION",
        payload: {
          type: "ACCRUAL_FINALIZE_BLOCKED",
          reason: "NO_NONZERO_COMPONENTS",
        },
        userId,
        runQuery: tx.query,
      });
      throw badRequest("No non-zero payroll accrual components to post");
    }
    if (!preview.is_balanced) {
      await writePayrollRunAudit({
        tenantId,
        legalEntityId: parsePositiveInt(current.legal_entity_id),
        runId,
        action: "VALIDATION",
        payload: {
          type: "ACCRUAL_FINALIZE_BLOCKED",
          reason: "UNBALANCED_PREVIEW",
          debitTotal: preview.debit_total,
          creditTotal: preview.credit_total,
        },
        userId,
        runQuery: tx.query,
      });
      throw badRequest("Payroll accrual preview is not balanced");
    }

    const postingDate = toDateOnly(current.pay_date);
    const journalResult = await createPayrollAccrualJournalTx(tx, {
      run: current,
      preview,
      tenantId,
      legalEntityId: parsePositiveInt(current.legal_entity_id),
      userId,
      postingDate,
      note,
    });

    const shouldBackfillReview = currentStatus === "IMPORTED" && forceFromImported;

    await tx.query(
      `UPDATE payroll_runs
       SET status = 'FINALIZED',
           reviewed_by_user_id = COALESCE(reviewed_by_user_id, ?),
           reviewed_at = COALESCE(reviewed_at, CURRENT_TIMESTAMP),
           finalized_by_user_id = ?,
           finalized_at = CURRENT_TIMESTAMP,
           accrual_journal_entry_id = ?,
           accrual_posted_by_user_id = ?,
           accrual_posted_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [
        shouldBackfillReview ? userId : null,
        userId,
        journalResult.journalEntryId,
        userId,
        tenantId,
        runId,
      ]
    );

    await writePayrollRunAudit({
      tenantId,
      legalEntityId: parsePositiveInt(current.legal_entity_id),
      runId,
      action: "STATUS",
      payload: {
        fromStatus: currentStatus,
        toStatus: "FINALIZED",
        forceFromImported: Boolean(forceFromImported),
        note: note || null,
        accrualJournalEntryId: journalResult.journalEntryId,
        journalNo: journalResult.journalNo,
        postingDate,
        debitTotal: preview.debit_total,
        creditTotal: preview.credit_total,
        lineCount: (preview.posting_lines || []).length,
        idempotentJournalReplay: Boolean(journalResult.idempotentReplay),
      },
      userId,
      runQuery: tx.query,
    });

    return {
      runId,
      accrualJournalEntryId: journalResult.journalEntryId,
      idempotentReplay: Boolean(journalResult.idempotentReplay),
      preview,
    };
  });
}
