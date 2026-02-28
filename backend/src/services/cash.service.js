import crypto from "node:crypto";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  ensurePeriodOpen,
  toAmount,
  toIsoDate,
  validateJournalLineScope,
} from "../routes/gl.js";

const BALANCE_EPSILON = 0.0001;
const CASH_TXN_SUBLEDGER_PREFIX = "CASH_TXN:";

function asUpper(value) {
  return String(value || "").trim().toUpperCase();
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

function ensureBalanced(lines) {
  let totalDebit = 0;
  let totalCredit = 0;
  for (const line of lines) {
    totalDebit += toAmount(line.debitBase);
    totalCredit += toAmount(line.creditBase);
  }
  if (Math.abs(totalDebit - totalCredit) > BALANCE_EPSILON) {
    throw badRequest("Cash posting journal is not balanced");
  }
  return {
    totalDebit: Number(totalDebit.toFixed(6)),
    totalCredit: Number(totalCredit.toFixed(6)),
  };
}

function buildCashJournalNo(cashTxn) {
  const txnNo = String(cashTxn.txn_no || "").trim().toUpperCase();
  if (txnNo) {
    const candidate = `CASH-${txnNo}`.slice(0, 40);
    if (candidate.length <= 40) {
      return candidate;
    }
  }

  const hash = crypto
    .createHash("sha1")
    .update(`${cashTxn.tenant_id}:${cashTxn.id}:${cashTxn.txn_no || ""}`)
    .digest("hex")
    .slice(0, 10)
    .toUpperCase();
  return `CASH-${cashTxn.id}-${hash}`.slice(0, 40);
}

function requireAccountId(value, label) {
  const id = parsePositiveInt(value);
  if (!id) {
    throw badRequest(`${label} is required`);
  }
  return id;
}

function resolveTransferPostingMode(cashTxn) {
  const sourceOu = parsePositiveInt(cashTxn.operating_unit_id);
  const counterOu = parsePositiveInt(cashTxn.counter_cash_register_operating_unit_id);
  const sourceLegalEntityId = parsePositiveInt(cashTxn.legal_entity_id);
  const counterLegalEntityId = parsePositiveInt(cashTxn.counter_cash_register_legal_entity_id);
  const sourceCurrency = asUpper(cashTxn.currency_code);
  const counterCurrency = asUpper(cashTxn.counter_cash_register_currency_code);

  if (!counterLegalEntityId || counterLegalEntityId !== sourceLegalEntityId) {
    throw badRequest("Direct transfer is only supported within the same legal entity in v1");
  }

  if (counterCurrency && sourceCurrency && counterCurrency !== sourceCurrency) {
    throw badRequest("Transfer register currencies must match");
  }

  if (sourceOu === counterOu) {
    return "DIRECT";
  }

  const sourceEntityType = asUpper(cashTxn.source_entity_type);
  const hasTransitLink = sourceEntityType === "CASH_TRANSIT_TRANSFER";
  if (!hasTransitLink) {
    throw badRequest("Cross-OU transfer requires CASH_IN_TRANSIT workflow");
  }

  requireAccountId(cashTxn.counter_account_id, "counterAccountId (CASH_IN_TRANSIT)");
  return "TRANSIT";
}

function buildBaseLine({
  accountId,
  operatingUnitId,
  debitBase,
  creditBase,
  description,
  subledgerReferenceNo,
}) {
  const resolvedOperatingUnitId = parsePositiveInt(operatingUnitId);
  // GL line validation requires operatingUnitId when subledger_reference_no is present.
  const resolvedSubledgerReferenceNo = resolvedOperatingUnitId
    ? normalizeOptionalShortText(subledgerReferenceNo, "line.subledgerReferenceNo", 100)
    : null;

  return {
    accountId: requireAccountId(accountId, "line.accountId"),
    operatingUnitId: resolvedOperatingUnitId,
    counterpartyLegalEntityId: null,
    description: normalizeOptionalShortText(description, "line.description", 255),
    subledgerReferenceNo: resolvedSubledgerReferenceNo,
    debitBase: Number(toAmount(debitBase).toFixed(6)),
    creditBase: Number(toAmount(creditBase).toFixed(6)),
  };
}

function invertLines(lines) {
  return lines.map((line) => ({
    ...line,
    debitBase: Number(toAmount(line.creditBase).toFixed(6)),
    creditBase: Number(toAmount(line.debitBase).toFixed(6)),
  }));
}

function buildCashPostingLines(cashTxn) {
  const txnType = asUpper(cashTxn.txn_type);
  const amount = Number(toAmount(cashTxn.amount).toFixed(6));
  if (!(amount > 0)) {
    throw badRequest("Cash transaction amount must be > 0 for posting");
  }

  const registerAccountId = requireAccountId(
    cashTxn.register_account_id,
    "register account"
  );
  const counterAccountId = parsePositiveInt(cashTxn.counter_account_id);
  const counterRegisterAccountId = parsePositiveInt(
    cashTxn.counter_cash_register_account_id
  );
  const registerVarianceGainAccountId = parsePositiveInt(
    cashTxn.register_variance_gain_account_id
  );
  const registerVarianceLossAccountId = parsePositiveInt(
    cashTxn.register_variance_loss_account_id
  );

  const baseDescription = normalizeOptionalShortText(
    cashTxn.description,
    "cashTransaction.description",
    255
  );
  const lineDescription = baseDescription || `Cash ${txnType}`;
  const subledgerReferenceNo = `${CASH_TXN_SUBLEDGER_PREFIX}${cashTxn.id}`;

  let lines;

  if (txnType === "RECEIPT" || txnType === "WITHDRAWAL_FROM_BANK" || txnType === "OPENING_FLOAT") {
    lines = [
      buildBaseLine({
        accountId: registerAccountId,
        operatingUnitId: cashTxn.operating_unit_id,
        debitBase: amount,
        creditBase: 0,
        description: lineDescription,
        subledgerReferenceNo,
      }),
      buildBaseLine({
        accountId: requireAccountId(counterAccountId, "counterAccountId"),
        operatingUnitId: cashTxn.operating_unit_id,
        debitBase: 0,
        creditBase: amount,
        description: lineDescription,
        subledgerReferenceNo,
      }),
    ];
  } else if (
    txnType === "PAYOUT" ||
    txnType === "DEPOSIT_TO_BANK" ||
    txnType === "CLOSING_ADJUSTMENT"
  ) {
    lines = [
      buildBaseLine({
        accountId: requireAccountId(counterAccountId, "counterAccountId"),
        operatingUnitId: cashTxn.operating_unit_id,
        debitBase: amount,
        creditBase: 0,
        description: lineDescription,
        subledgerReferenceNo,
      }),
      buildBaseLine({
        accountId: registerAccountId,
        operatingUnitId: cashTxn.operating_unit_id,
        debitBase: 0,
        creditBase: amount,
        description: lineDescription,
        subledgerReferenceNo,
      }),
    ];
  } else if (txnType === "VARIANCE") {
    const resolvedCounterAccountId = requireAccountId(counterAccountId, "counterAccountId");
    let isOverVariance = false;
    if (registerVarianceGainAccountId && resolvedCounterAccountId === registerVarianceGainAccountId) {
      isOverVariance = true;
    } else if (
      registerVarianceLossAccountId &&
      resolvedCounterAccountId === registerVarianceLossAccountId
    ) {
      isOverVariance = false;
    } else if (registerVarianceGainAccountId || registerVarianceLossAccountId) {
      throw badRequest(
        "Variance counterAccountId must match register variance gain/loss account configuration"
      );
    }

    if (isOverVariance) {
      // Counted > expected: increase cash (debit register), credit variance gain account.
      lines = [
        buildBaseLine({
          accountId: registerAccountId,
          operatingUnitId: cashTxn.operating_unit_id,
          debitBase: amount,
          creditBase: 0,
          description: lineDescription,
          subledgerReferenceNo,
        }),
        buildBaseLine({
          accountId: resolvedCounterAccountId,
          operatingUnitId: cashTxn.operating_unit_id,
          debitBase: 0,
          creditBase: amount,
          description: lineDescription,
          subledgerReferenceNo,
        }),
      ];
    } else {
      // Counted < expected: credit cash register, debit variance loss account.
      lines = [
        buildBaseLine({
          accountId: resolvedCounterAccountId,
          operatingUnitId: cashTxn.operating_unit_id,
          debitBase: amount,
          creditBase: 0,
          description: lineDescription,
          subledgerReferenceNo,
        }),
        buildBaseLine({
          accountId: registerAccountId,
          operatingUnitId: cashTxn.operating_unit_id,
          debitBase: 0,
          creditBase: amount,
          description: lineDescription,
          subledgerReferenceNo,
        }),
      ];
    }
  } else if (txnType === "TRANSFER_OUT") {
    const transferPostingMode = resolveTransferPostingMode(cashTxn);
    if (transferPostingMode === "DIRECT") {
      lines = [
        buildBaseLine({
          accountId: requireAccountId(
            counterRegisterAccountId,
            "counterCashRegisterId account"
          ),
          operatingUnitId: cashTxn.counter_cash_register_operating_unit_id,
          debitBase: amount,
          creditBase: 0,
          description: lineDescription,
          subledgerReferenceNo,
        }),
        buildBaseLine({
          accountId: registerAccountId,
          operatingUnitId: cashTxn.operating_unit_id,
          debitBase: 0,
          creditBase: amount,
          description: lineDescription,
          subledgerReferenceNo,
        }),
      ];
    } else {
      lines = [
        buildBaseLine({
          accountId: requireAccountId(
            counterAccountId,
            "counterAccountId (CASH_IN_TRANSIT)"
          ),
          operatingUnitId: cashTxn.operating_unit_id,
          debitBase: amount,
          creditBase: 0,
          description: lineDescription,
          subledgerReferenceNo,
        }),
        buildBaseLine({
          accountId: registerAccountId,
          operatingUnitId: cashTxn.operating_unit_id,
          debitBase: 0,
          creditBase: amount,
          description: lineDescription,
          subledgerReferenceNo,
        }),
      ];
    }
  } else if (txnType === "TRANSFER_IN") {
    const transferPostingMode = resolveTransferPostingMode(cashTxn);
    if (transferPostingMode === "DIRECT") {
      lines = [
        buildBaseLine({
          accountId: registerAccountId,
          operatingUnitId: cashTxn.operating_unit_id,
          debitBase: amount,
          creditBase: 0,
          description: lineDescription,
          subledgerReferenceNo,
        }),
        buildBaseLine({
          accountId: requireAccountId(
            counterRegisterAccountId,
            "counterCashRegisterId account"
          ),
          operatingUnitId: cashTxn.counter_cash_register_operating_unit_id,
          debitBase: 0,
          creditBase: amount,
          description: lineDescription,
          subledgerReferenceNo,
        }),
      ];
    } else {
      lines = [
        buildBaseLine({
          accountId: registerAccountId,
          operatingUnitId: cashTxn.operating_unit_id,
          debitBase: amount,
          creditBase: 0,
          description: lineDescription,
          subledgerReferenceNo,
        }),
        buildBaseLine({
          accountId: requireAccountId(
            counterAccountId,
            "counterAccountId (CASH_IN_TRANSIT)"
          ),
          operatingUnitId: cashTxn.operating_unit_id,
          debitBase: 0,
          creditBase: amount,
          description: lineDescription,
          subledgerReferenceNo,
        }),
      ];
    }
  } else {
    throw badRequest(`Unsupported cash transaction type for posting: ${txnType}`);
  }

  if (parsePositiveInt(cashTxn.reversal_of_transaction_id)) {
    lines = invertLines(lines);
  }

  ensureBalanced(lines);
  return lines;
}

async function resolveBookAndPeriodForCashPostingTx(tx, payload) {
  const bookResult = await tx.query(
    `SELECT id, calendar_id, code, name, base_currency_code, book_type
     FROM books
     WHERE tenant_id = ?
       AND legal_entity_id = ?
     ORDER BY
       CASE WHEN book_type = 'LOCAL' THEN 0 ELSE 1 END,
       id ASC
     LIMIT 1`,
    [payload.tenantId, payload.legalEntityId]
  );
  const book = bookResult.rows?.[0] || null;
  if (!book) {
    throw badRequest("No book found for cash transaction legal entity");
  }

  const bookId = parsePositiveInt(book.id);
  const calendarId = parsePositiveInt(book.calendar_id);
  if (!bookId || !calendarId) {
    throw badRequest("Book configuration is invalid for cash transaction posting");
  }

  const periodResult = await tx.query(
    `SELECT id, fiscal_year, period_no, period_name
     FROM fiscal_periods
     WHERE calendar_id = ?
       AND ? BETWEEN start_date AND end_date
     ORDER BY is_adjustment ASC, id ASC
     LIMIT 1`,
    [calendarId, payload.bookDate]
  );
  const period = periodResult.rows?.[0] || null;
  if (!period) {
    throw badRequest("No fiscal period found for cash transaction book_date");
  }

  const fiscalPeriodId = parsePositiveInt(period.id);
  if (!fiscalPeriodId) {
    throw badRequest("Fiscal period configuration is invalid for cash transaction posting");
  }

  await ensurePeriodOpen(
    bookId,
    fiscalPeriodId,
    "post cash transaction",
    tx.query.bind(tx)
  );

  return {
    bookId,
    fiscalPeriodId,
    calendarId,
    book,
    period,
  };
}

export async function createAndPostCashJournalTx(tx, payload) {
  const tenantId = parsePositiveInt(payload?.tenantId);
  const userId = parsePositiveInt(payload?.userId);
  const legalEntityId = parsePositiveInt(payload?.legalEntityId);
  const cashTxn = payload?.cashTxn || null;
  const req = payload?.req;

  if (!tenantId || !userId || !legalEntityId || !cashTxn || !req) {
    throw badRequest("Missing required payload for cash journal posting");
  }

  const txnId = parsePositiveInt(cashTxn.id);
  if (!txnId) {
    throw badRequest("cashTxn.id is required for posting");
  }

  const bookDate = toIsoDate(cashTxn.book_date, "cashTransaction.book_date");
  const entryDate = bookDate;
  const documentDate = bookDate;
  const currencyCode = asUpper(cashTxn.currency_code);
  if (!currencyCode || currencyCode.length !== 3) {
    throw badRequest("cashTransaction.currency_code is invalid");
  }

  const journalContext = await resolveBookAndPeriodForCashPostingTx(tx, {
    tenantId,
    legalEntityId,
    bookDate,
  });
  const bookBaseCurrencyCode = asUpper(journalContext.book?.base_currency_code);
  if (bookBaseCurrencyCode && bookBaseCurrencyCode !== currencyCode) {
    throw badRequest(
      `cashTransaction.currency_code (${currencyCode}) must match book base currency (${bookBaseCurrencyCode})`
    );
  }

  const lines = buildCashPostingLines(cashTxn);
  for (let i = 0; i < lines.length; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    await validateJournalLineScope(req, tenantId, legalEntityId, lines[i], i);
  }

  const totals = ensureBalanced(lines);
  const referenceNo = normalizeOptionalShortText(
    cashTxn.reference_no,
    "cashTransaction.reference_no",
    100
  );
  const entryDescription = normalizeOptionalShortText(
    cashTxn.description,
    "cashTransaction.description",
    255
  );
  const postingReference = `${CASH_TXN_SUBLEDGER_PREFIX}${txnId}`;
  const effectiveReferenceNo = referenceNo || postingReference;
  const effectiveDescription = entryDescription || `Cash ${asUpper(cashTxn.txn_type)} ${cashTxn.txn_no}`;

  const journalResult = await tx.query(
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
     VALUES (?, ?, ?, ?, ?, 'CASH', 'POSTED', ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
    [
      tenantId,
      legalEntityId,
      journalContext.bookId,
      journalContext.fiscalPeriodId,
      buildCashJournalNo(cashTxn),
      entryDate,
      documentDate,
      currencyCode,
      effectiveDescription,
      effectiveReferenceNo,
      totals.totalDebit,
      totals.totalCredit,
      userId,
      userId,
    ]
  );

  const journalEntryId = parsePositiveInt(journalResult.rows?.insertId);
  if (!journalEntryId) {
    throw badRequest("Failed to create posted cash journal");
  }

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const debitBase = Number(toAmount(line.debitBase).toFixed(6));
    const creditBase = Number(toAmount(line.creditBase).toFixed(6));
    const amountTxn = Number((debitBase - creditBase).toFixed(6));

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
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
      [
        journalEntryId,
        i + 1,
        parsePositiveInt(line.accountId),
        parsePositiveInt(line.operatingUnitId),
        parsePositiveInt(line.counterpartyLegalEntityId),
        line.description || null,
        line.subledgerReferenceNo || postingReference,
        currencyCode,
        amountTxn,
        debitBase,
        creditBase,
      ]
    );
  }

  return {
    journalEntryId,
    bookId: journalContext.bookId,
    fiscalPeriodId: journalContext.fiscalPeriodId,
    sourceType: "CASH",
    lineCount: lines.length,
    totalDebit: totals.totalDebit,
    totalCredit: totals.totalCredit,
    subledgerReferenceNo: postingReference,
  };
}
