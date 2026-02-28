import { badRequest, parsePositiveInt } from "../routes/_utils.js";

const SHAREHOLDER_CAPITAL_CREDIT_PARENT_PURPOSE =
  "SHAREHOLDER_CAPITAL_CREDIT_PARENT";
const SHAREHOLDER_COMMITMENT_DEBIT_PARENT_PURPOSE =
  "SHAREHOLDER_COMMITMENT_DEBIT_PARENT";

export function normalizeAccountNormalSide(value) {
  return String(value || "").trim().toUpperCase();
}

export async function resolveShareholderParentMappings(tx, tenantId, legalEntityId) {
  const result = await tx.query(
    `SELECT purpose_code, account_id
     FROM journal_purpose_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND purpose_code IN (?, ?)`,
    [
      tenantId,
      legalEntityId,
      SHAREHOLDER_CAPITAL_CREDIT_PARENT_PURPOSE,
      SHAREHOLDER_COMMITMENT_DEBIT_PARENT_PURPOSE,
    ]
  );

  const byPurpose = new Map(
    (result.rows || []).map((row) => [
      String(row.purpose_code || ""),
      parsePositiveInt(row.account_id),
    ])
  );
  const capitalCreditParentAccountId = parsePositiveInt(
    byPurpose.get(SHAREHOLDER_CAPITAL_CREDIT_PARENT_PURPOSE)
  );
  const commitmentDebitParentAccountId = parsePositiveInt(
    byPurpose.get(SHAREHOLDER_COMMITMENT_DEBIT_PARENT_PURPOSE)
  );

  return {
    capitalCreditParentAccountId,
    commitmentDebitParentAccountId,
  };
}

export async function assertShareholderParentAccount(
  tx,
  tenantId,
  legalEntityId,
  accountId,
  fieldLabel,
  expectedNormalSide
) {
  const normalizedAccountId = parsePositiveInt(accountId);
  if (!normalizedAccountId) {
    throw badRequest(`${fieldLabel} must be a positive integer`);
  }

  const result = await tx.query(
    `SELECT
       a.id,
       a.code,
       a.name,
       a.coa_id,
       a.account_type,
       a.normal_side,
       a.is_active,
       a.allow_posting,
       c.legal_entity_id
      FROM accounts a
      JOIN charts_of_accounts c ON c.id = a.coa_id
      WHERE a.id = ?
       AND c.tenant_id = ?
     LIMIT 1`,
    [normalizedAccountId, tenantId]
  );
  const account = result.rows[0];
  if (!account) {
    throw badRequest(`${fieldLabel} not found for tenant`);
  }
  if (parsePositiveInt(account.legal_entity_id) !== legalEntityId) {
    throw badRequest(`${fieldLabel} must belong to selected legalEntityId`);
  }
  if (!Boolean(account.is_active)) {
    throw badRequest(`${fieldLabel} must reference an active account`);
  }
  if (String(account.account_type || "").toUpperCase() !== "EQUITY") {
    throw badRequest(`${fieldLabel} must reference an EQUITY account`);
  }
  if (
    expectedNormalSide &&
    normalizeAccountNormalSide(account.normal_side) !== expectedNormalSide
  ) {
    throw badRequest(
      `${fieldLabel} must reference a ${expectedNormalSide} normal-side account`
    );
  }
  if (Boolean(account.allow_posting)) {
    throw badRequest(
      `${fieldLabel} must reference a non-postable/header account (allow_posting=false)`
    );
  }

  return {
    id: normalizedAccountId,
    code: String(account.code || ""),
    name: String(account.name || ""),
    coaId: parsePositiveInt(account.coa_id),
    normalSide: normalizeAccountNormalSide(account.normal_side),
  };
}

export async function loadLegalEntityAccountHierarchy(tx, tenantId, legalEntityId) {
  const result = await tx.query(
    `SELECT
       a.id,
       a.parent_account_id
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE c.tenant_id = ?
       AND c.legal_entity_id = ?`,
    [tenantId, legalEntityId]
  );

  return new Map(
    (result.rows || []).map((row) => [
      parsePositiveInt(row.id),
      parsePositiveInt(row.parent_account_id),
    ])
  );
}

export function isDescendantOfParentAccount(parentById, accountId, parentAccountId) {
  const normalizedAccountId = parsePositiveInt(accountId);
  const normalizedParentAccountId = parsePositiveInt(parentAccountId);
  if (!normalizedAccountId || !normalizedParentAccountId) {
    return false;
  }

  const visited = new Set();
  let currentParentId = parsePositiveInt(parentById.get(normalizedAccountId));
  while (currentParentId) {
    if (currentParentId === normalizedParentAccountId) {
      return true;
    }
    if (visited.has(currentParentId)) {
      break;
    }
    visited.add(currentParentId);
    currentParentId = parsePositiveInt(parentById.get(currentParentId));
  }

  return false;
}

export function createBatchRowIssue(code, message) {
  return {
    code: String(code || "").trim().toUpperCase(),
    message: String(message || "").trim(),
  };
}

export function validateShareholderMappedLeafAccount({
  account,
  tenantId,
  legalEntityId,
  expectedNormalSide,
  expectedParentAccountId,
  parentById,
  fieldLabel,
}) {
  if (!account) {
    return createBatchRowIssue(
      "MISSING_ACCOUNTS",
      `${fieldLabel} is missing for selected shareholder`
    );
  }
  if (
    parsePositiveInt(account.account_tenant_id) !== tenantId ||
    parsePositiveInt(account.account_legal_entity_id) !== legalEntityId
  ) {
    return createBatchRowIssue(
      "INVALID_PARENT_MAPPING",
      `${fieldLabel} must belong to selected legalEntityId`
    );
  }
  if (String(account.account_type || "").toUpperCase() !== "EQUITY") {
    return createBatchRowIssue(
      "INVALID_PARENT_MAPPING",
      `${fieldLabel} must reference an EQUITY account`
    );
  }
  if (normalizeAccountNormalSide(account.normal_side) !== expectedNormalSide) {
    return createBatchRowIssue(
      "INVALID_PARENT_MAPPING",
      `${fieldLabel} must reference a ${expectedNormalSide} normal-side account`
    );
  }
  if (!Boolean(account.is_active)) {
    return createBatchRowIssue(
      "INACTIVE_ACCOUNTS",
      `${fieldLabel} must reference an active account`
    );
  }
  if (!Boolean(account.allow_posting)) {
    return createBatchRowIssue(
      "NON_POSTABLE_MAPPED_CHILD_ACCOUNT",
      `${fieldLabel} must reference a postable account`
    );
  }
  if (Boolean(account.has_active_children)) {
    return createBatchRowIssue(
      "NON_POSTABLE_MAPPED_CHILD_ACCOUNT",
      `${fieldLabel} must reference a leaf/postable account`
    );
  }
  if (
    !isDescendantOfParentAccount(
      parentById,
      parsePositiveInt(account.id),
      expectedParentAccountId
    )
  ) {
    return createBatchRowIssue(
      "INVALID_PARENT_MAPPING",
      `${fieldLabel} must be a child/descendant of configured parent account`
    );
  }
  return null;
}

export function normalizeShareholderChildSequenceFromCode(parentCode, childCode) {
  const normalizedParentCode = String(parentCode || "").trim();
  const normalizedChildCode = String(childCode || "").trim();
  if (!normalizedParentCode || !normalizedChildCode) {
    return null;
  }
  const prefix = `${normalizedParentCode}.`;
  if (!normalizedChildCode.startsWith(prefix)) {
    return null;
  }
  const suffix = normalizedChildCode.slice(prefix.length);
  if (!/^\d+$/.test(suffix)) {
    return null;
  }
  const parsed = Number(suffix);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

export function buildShareholderChildCode(parentCode, sequence) {
  const normalizedParentCode = String(parentCode || "").trim();
  if (!normalizedParentCode) {
    throw badRequest("Parent account code is required to generate child account code");
  }
  const prefix = `${normalizedParentCode}.`;
  const maxSuffixLength = 50 - prefix.length;
  if (maxSuffixLength < 1) {
    throw badRequest(
      `Parent account code ${normalizedParentCode} is too long to generate child account code`
    );
  }

  const numericSequence = Number(sequence);
  if (!Number.isInteger(numericSequence) || numericSequence <= 0) {
    throw badRequest("sequence must be a positive integer");
  }

  let suffix = String(numericSequence);
  if (maxSuffixLength >= 2) {
    suffix = suffix.padStart(2, "0");
  }
  if (suffix.length > maxSuffixLength) {
    throw badRequest(
      `No available child account code capacity under parent ${normalizedParentCode}`
    );
  }
  return `${prefix}${suffix}`;
}

function toLocalYyyyMmDd(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(
    date.getDate()
  ).padStart(2, "0")}`;
}

function toIsoDate(value, fieldLabel = "date") {
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

export function generateAutoJournalNo(prefix = "TAA") {
  const stamp = Date.now().toString(36).toUpperCase();
  const rand = Math.floor(Math.random() * 1_679_616)
    .toString(36)
    .toUpperCase()
    .padStart(4, "0");
  return `${String(prefix).slice(0, 8).toUpperCase()}-${stamp}-${rand}`.slice(0, 40);
}

export function normalizeMoney(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Math.round(parsed * 1_000_000) / 1_000_000;
}

function pushValidationIssue(collection, issue) {
  const normalizedCode = String(issue?.code || "").trim().toUpperCase();
  const normalizedMessage = String(issue?.message || "").trim();
  if (!normalizedCode || !normalizedMessage) {
    return;
  }
  const existing = collection.find(
    (row) => row.code === normalizedCode && row.message === normalizedMessage
  );
  if (!existing) {
    collection.push({
      code: normalizedCode,
      message: normalizedMessage,
      details: issue?.details ? [issue.details] : [],
    });
    return;
  }
  if (issue?.details) {
    existing.details = Array.isArray(existing.details) ? existing.details : [];
    existing.details.push(issue.details);
  }
}

export function normalizeCurrencyCode(value, fallback = "USD") {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  return normalized || fallback;
}

async function loadShareholderCommitmentJournalizedAmountByShareholder(
  tx,
  tenantId,
  legalEntityId,
  shareholderIds
) {
  if (!Array.isArray(shareholderIds) || shareholderIds.length === 0) {
    return new Map();
  }

  const placeholders = shareholderIds.map(() => "?").join(",");
  try {
    const result = await tx.query(
      `SELECT
         shareholder_id,
         SUM(amount) AS total_amount
       FROM shareholder_commitment_journal_entries
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND shareholder_id IN (${placeholders})
       GROUP BY shareholder_id`,
      [tenantId, legalEntityId, ...shareholderIds]
    );

    return new Map(
      (result.rows || []).map((row) => [
        parsePositiveInt(row.shareholder_id),
        normalizeMoney(row.total_amount || 0),
      ])
    );
  } catch (err) {
    if (Number(err?.errno) === 1146) {
      throw badRequest(
        "Setup required: shareholder commitment audit table is missing (run latest migrations)"
      );
    }
    throw err;
  }
}

export function parseBatchShareholderIds(rawShareholderIds) {
  const shareholderIds = Array.from(
    new Set(
      (Array.isArray(rawShareholderIds) ? rawShareholderIds : [])
        .map((value) => parsePositiveInt(value))
        .filter(Boolean)
    )
  );
  if (shareholderIds.length === 0) {
    throw badRequest("shareholderIds must include at least one valid id");
  }
  if (shareholderIds.length > 200) {
    throw badRequest("shareholderIds cannot exceed 200 entries");
  }
  return shareholderIds;
}

function toJournalContextRow(row) {
  if (!row) {
    return null;
  }
  return {
    bookId: parsePositiveInt(row.book_id),
    bookCode: String(row.book_code || ""),
    fiscalPeriodId: parsePositiveInt(row.fiscal_period_id),
    startDate: toIsoDate(row.start_date, "fiscal_period.start_date"),
    endDate: toIsoDate(row.end_date, "fiscal_period.end_date"),
    baseCurrencyCode: String(row.base_currency_code || "USD").toUpperCase(),
  };
}

async function resolveOpenBookPeriodForLegalEntity(tx, tenantId, legalEntityId, asOfDate) {
  const currentResult = await tx.query(
    `SELECT
       b.id AS book_id,
       b.code AS book_code,
       b.base_currency_code,
       fp.id AS fiscal_period_id,
       fp.start_date,
       fp.end_date
     FROM books b
     JOIN fiscal_periods fp
       ON fp.calendar_id = b.calendar_id
      AND fp.is_adjustment = FALSE
     LEFT JOIN period_statuses ps
       ON ps.book_id = b.id
      AND ps.fiscal_period_id = fp.id
     WHERE b.tenant_id = ?
       AND b.legal_entity_id = ?
       AND ? BETWEEN fp.start_date AND fp.end_date
       AND COALESCE(ps.status, 'OPEN') = 'OPEN'
     ORDER BY b.id, fp.start_date DESC
     LIMIT 1`,
    [tenantId, legalEntityId, asOfDate]
  );
  const current = toJournalContextRow(currentResult.rows[0]);
  if (current) {
    return current;
  }

  const pastResult = await tx.query(
    `SELECT
       b.id AS book_id,
       b.code AS book_code,
       b.base_currency_code,
       fp.id AS fiscal_period_id,
       fp.start_date,
       fp.end_date
     FROM books b
     JOIN fiscal_periods fp
       ON fp.calendar_id = b.calendar_id
      AND fp.is_adjustment = FALSE
     LEFT JOIN period_statuses ps
       ON ps.book_id = b.id
      AND ps.fiscal_period_id = fp.id
     WHERE b.tenant_id = ?
       AND b.legal_entity_id = ?
       AND fp.start_date <= ?
       AND COALESCE(ps.status, 'OPEN') = 'OPEN'
     ORDER BY fp.start_date DESC
     LIMIT 1`,
    [tenantId, legalEntityId, asOfDate]
  );
  const past = toJournalContextRow(pastResult.rows[0]);
  if (past) {
    return past;
  }

  const futureResult = await tx.query(
    `SELECT
       b.id AS book_id,
       b.code AS book_code,
       b.base_currency_code,
       fp.id AS fiscal_period_id,
       fp.start_date,
       fp.end_date
     FROM books b
     JOIN fiscal_periods fp
       ON fp.calendar_id = b.calendar_id
      AND fp.is_adjustment = FALSE
     LEFT JOIN period_statuses ps
       ON ps.book_id = b.id
      AND ps.fiscal_period_id = fp.id
     WHERE b.tenant_id = ?
       AND b.legal_entity_id = ?
       AND fp.start_date > ?
       AND COALESCE(ps.status, 'OPEN') = 'OPEN'
     ORDER BY fp.start_date ASC
     LIMIT 1`,
    [tenantId, legalEntityId, asOfDate]
  );
  return toJournalContextRow(futureResult.rows[0]);
}

export async function buildShareholderCommitmentBatchPreviewTx(tx, payload) {
  const tenantId = parsePositiveInt(payload?.tenantId);
  const legalEntityId = parsePositiveInt(payload?.legalEntityId);
  const shareholderIds = parseBatchShareholderIds(payload?.shareholderIds);
  const commitmentDate = toIsoDate(payload?.commitmentDate, "commitmentDate");
  const lockShareholders = Boolean(payload?.lockShareholders);

  if (!tenantId || !legalEntityId) {
    throw badRequest("tenantId and legalEntityId are required");
  }

  const blockingErrors = [];
  const warnings = [];
  const lockClause = lockShareholders ? "FOR UPDATE" : "";
  const placeholders = shareholderIds.map(() => "?").join(",");
  const shareholdersResult = await tx.query(
    `SELECT
       s.id,
       s.code,
       s.name,
       s.committed_capital,
       s.currency_code,
       s.capital_sub_account_id,
       s.commitment_debit_sub_account_id
     FROM shareholders s
     WHERE s.tenant_id = ?
       AND s.legal_entity_id = ?
       AND s.id IN (${placeholders})
     ORDER BY s.id
     ${lockClause}`,
    [tenantId, legalEntityId, ...shareholderIds]
  );
  const shareholdersById = new Map(
    (shareholdersResult.rows || []).map((row) => [parsePositiveInt(row.id), row])
  );
  const missingIds = shareholderIds.filter((id) => !shareholdersById.has(id));
  if (missingIds.length > 0) {
    throw badRequest(
      `Some shareholders were not found in legalEntityId=${legalEntityId}: ${missingIds.join(",")}`
    );
  }

  const selectedShareholders = shareholderIds
    .map((id) => shareholdersById.get(id))
    .filter(Boolean);

  const currencyGroups = new Map();
  for (const row of selectedShareholders) {
    const currencyCode = normalizeCurrencyCode(row.currency_code || "");
    if (!currencyGroups.has(currencyCode)) {
      currencyGroups.set(currencyCode, []);
    }
    currencyGroups
      .get(currencyCode)
      .push({
        shareholder_id: parsePositiveInt(row.id),
        code: String(row.code || ""),
        name: String(row.name || ""),
      });
  }
  if (currencyGroups.size > 1) {
    const mixedCurrencyDetails = Array.from(currencyGroups.entries()).map(
      ([currency_code, shareholders]) => ({
        currency_code,
        shareholders,
      })
    );
    pushValidationIssue(blockingErrors, {
      code: "MIXED_CURRENCY",
      message:
        "Queued shareholders contain mixed currencies. Create separate batches per currency.",
      details: mixedCurrencyDetails,
    });
  }

  const shareholderParentMappings = await resolveShareholderParentMappings(
    tx,
    tenantId,
    legalEntityId
  );
  const capitalCreditParentAccountId = parsePositiveInt(
    shareholderParentMappings.capitalCreditParentAccountId
  );
  const commitmentDebitParentAccountId = parsePositiveInt(
    shareholderParentMappings.commitmentDebitParentAccountId
  );

  let capitalParentAccount = null;
  let commitmentParentAccount = null;
  if (!capitalCreditParentAccountId || !commitmentDebitParentAccountId) {
    pushValidationIssue(blockingErrors, {
      code: "INVALID_PARENT_MAPPING",
      message:
        "Setup required: configure shareholder parent account mapping for selected legalEntityId",
      details: {
        capital_credit_parent_account_id: capitalCreditParentAccountId || null,
        commitment_debit_parent_account_id: commitmentDebitParentAccountId || null,
      },
    });
  } else {
    try {
      capitalParentAccount = await assertShareholderParentAccount(
        tx,
        tenantId,
        legalEntityId,
        capitalCreditParentAccountId,
        "capitalCreditParentAccountId",
        "CREDIT"
      );
    } catch (err) {
      pushValidationIssue(blockingErrors, {
        code: "INVALID_PARENT_MAPPING",
        message: err?.message || "capitalCreditParentAccountId is invalid",
      });
    }
    try {
      commitmentParentAccount = await assertShareholderParentAccount(
        tx,
        tenantId,
        legalEntityId,
        commitmentDebitParentAccountId,
        "commitmentDebitParentAccountId",
        "DEBIT"
      );
    } catch (err) {
      pushValidationIssue(blockingErrors, {
        code: "INVALID_PARENT_MAPPING",
        message: err?.message || "commitmentDebitParentAccountId is invalid",
      });
    }
  }

  const journalContext = await resolveOpenBookPeriodForLegalEntity(
    tx,
    tenantId,
    legalEntityId,
    commitmentDate
  );
  if (!journalContext?.bookId || !journalContext?.fiscalPeriodId) {
    pushValidationIssue(blockingErrors, {
      code: "NO_OPEN_BOOK_PERIOD",
      message: "No OPEN book/fiscal period found for legalEntityId",
    });
  } else if (
    commitmentDate < journalContext.startDate ||
    commitmentDate > journalContext.endDate
  ) {
    pushValidationIssue(blockingErrors, {
      code: "COMMITMENT_DATE_OUTSIDE_OPEN_PERIOD",
      message: "commitmentDate must be within an OPEN fiscal period for legalEntityId",
      details: {
        fiscal_period_id: journalContext.fiscalPeriodId,
        period_start_date: journalContext.startDate,
        period_end_date: journalContext.endDate,
      },
    });
  }

  const accountIds = Array.from(
    new Set(
      selectedShareholders.flatMap((row) => [
        parsePositiveInt(row.capital_sub_account_id),
        parsePositiveInt(row.commitment_debit_sub_account_id),
      ])
    )
  ).filter(Boolean);
  const accountById = new Map();
  if (accountIds.length > 0) {
    const accountPlaceholders = accountIds.map(() => "?").join(",");
    const accountsResult = await tx.query(
      `SELECT
         a.id,
         a.code,
         a.name,
         a.account_type,
         a.normal_side,
         a.allow_posting,
         a.is_active,
         a.parent_account_id,
         EXISTS(
           SELECT 1
           FROM accounts child
           WHERE child.parent_account_id = a.id
             AND child.is_active = TRUE
         ) AS has_active_children,
         c.tenant_id AS account_tenant_id,
         c.legal_entity_id AS account_legal_entity_id
       FROM accounts a
       JOIN charts_of_accounts c ON c.id = a.coa_id
       WHERE a.id IN (${accountPlaceholders})`,
      accountIds
    );
    for (const row of accountsResult.rows || []) {
      accountById.set(parsePositiveInt(row.id), row);
    }
  }

  const accountParentById = await loadLegalEntityAccountHierarchy(
    tx,
    tenantId,
    legalEntityId
  );
  const alreadyJournaledByShareholderId =
    await loadShareholderCommitmentJournalizedAmountByShareholder(
      tx,
      tenantId,
      legalEntityId,
      shareholderIds
    );

  const rows = [];
  const includedShareholders = [];
  const skippedShareholders = [];
  let totalDebit = 0;
  let totalCredit = 0;
  let totalsCurrencyCode = normalizeCurrencyCode(selectedShareholders[0]?.currency_code || "");
  let zeroDeltaSkippedCount = 0;

  for (const shareholder of selectedShareholders) {
    const shareholderId = parsePositiveInt(shareholder.id);
    const code = String(shareholder.code || "");
    const name = String(shareholder.name || "");
    const currencyCode = normalizeCurrencyCode(shareholder.currency_code || "");
    const committedCapital = normalizeMoney(shareholder.committed_capital || 0);
    const alreadyJournaledAmount = normalizeMoney(
      alreadyJournaledByShareholderId.get(shareholderId) || 0
    );
    const deltaAmount = normalizeMoney(committedCapital - alreadyJournaledAmount);

    const debitAccountId = parsePositiveInt(shareholder.commitment_debit_sub_account_id);
    const creditAccountId = parsePositiveInt(shareholder.capital_sub_account_id);
    const debitAccount = accountById.get(debitAccountId) || null;
    const creditAccount = accountById.get(creditAccountId) || null;

    const rowIssues = [];
    if (!creditAccountId) {
      rowIssues.push(
        createBatchRowIssue(
          "MISSING_ACCOUNTS",
          "capital_sub_account_id is missing for selected shareholder"
        )
      );
    }
    if (!debitAccountId) {
      rowIssues.push(
        createBatchRowIssue(
          "MISSING_ACCOUNTS",
          "commitment_debit_sub_account_id is missing for selected shareholder"
        )
      );
    }

    if (creditAccountId) {
      const capitalIssue = validateShareholderMappedLeafAccount({
        account: creditAccount,
        tenantId,
        legalEntityId,
        expectedNormalSide: "CREDIT",
        expectedParentAccountId: capitalParentAccount?.id || capitalCreditParentAccountId,
        parentById: accountParentById,
        fieldLabel: "capital_sub_account_id",
      });
      if (capitalIssue) {
        rowIssues.push(capitalIssue);
      }
    }
    if (debitAccountId) {
      const debitIssue = validateShareholderMappedLeafAccount({
        account: debitAccount,
        tenantId,
        legalEntityId,
        expectedNormalSide: "DEBIT",
        expectedParentAccountId:
          commitmentParentAccount?.id || commitmentDebitParentAccountId,
        parentById: accountParentById,
        fieldLabel: "commitment_debit_sub_account_id",
      });
      if (debitIssue) {
        rowIssues.push(debitIssue);
      }
    }

    for (const issue of rowIssues) {
      pushValidationIssue(blockingErrors, {
        code: issue.code,
        message: issue.message,
        details: {
          shareholder_id: shareholderId,
          code,
          name,
        },
      });
    }

    const rowPreview = {
      shareholder_id: shareholderId,
      code,
      name,
      currency_code: currencyCode,
      committed_capital: committedCapital,
      already_journaled_amount: alreadyJournaledAmount,
      delta_amount: deltaAmount,
      debit_account_id: debitAccountId || null,
      debit_account_code: debitAccount ? String(debitAccount.code || "") : null,
      debit_account_name: debitAccount ? String(debitAccount.name || "") : null,
      credit_account_id: creditAccountId || null,
      credit_account_code: creditAccount ? String(creditAccount.code || "") : null,
      credit_account_name: creditAccount ? String(creditAccount.name || "") : null,
      validation_issues: rowIssues,
    };

    let skippedReason = null;
    if (deltaAmount <= 0) {
      skippedReason = committedCapital <= 0 ? "committed capital is zero" : "already fully journaled";
      zeroDeltaSkippedCount += 1;
    } else if (rowIssues.length > 0) {
      skippedReason = rowIssues.map((issue) => issue.message).join("; ");
    }

    if (skippedReason) {
      rowPreview.status = "SKIPPED";
      rowPreview.skipped_reason = skippedReason;
      skippedShareholders.push(rowPreview);
    } else {
      rowPreview.status = "INCLUDED";
      rowPreview.skipped_reason = null;
      includedShareholders.push(rowPreview);
      totalDebit = normalizeMoney(totalDebit + deltaAmount);
      totalCredit = normalizeMoney(totalCredit + deltaAmount);
    }

    rows.push(rowPreview);
  }

  if (zeroDeltaSkippedCount > 0) {
    warnings.push({
      code: "ALREADY_FULLY_JOURNALED",
      message:
        "Some queued shareholders were skipped because they are already fully journaled",
      count: zeroDeltaSkippedCount,
    });
  }

  if (includedShareholders.length === 0) {
    pushValidationIssue(blockingErrors, {
      code: "NO_JOURNALIZABLE_ROWS",
      message: "No shareholder has a positive journalizable commitment delta",
    });
  }

  if (currencyGroups.size === 1) {
    totalsCurrencyCode = Array.from(currencyGroups.keys())[0];
  } else if (includedShareholders[0]?.currency_code) {
    totalsCurrencyCode = includedShareholders[0].currency_code;
  }

  return {
    legal_entity_id: legalEntityId,
    commitment_date: commitmentDate,
    parent_mapping: {
      capital_credit_parent_account_id: capitalCreditParentAccountId || null,
      commitment_debit_parent_account_id: commitmentDebitParentAccountId || null,
      capital_credit_parent_account_code: capitalParentAccount?.code || null,
      capital_credit_parent_account_name: capitalParentAccount?.name || null,
      commitment_debit_parent_account_code: commitmentParentAccount?.code || null,
      commitment_debit_parent_account_name: commitmentParentAccount?.name || null,
    },
    rows,
    included_shareholders: includedShareholders,
    skipped_shareholders: skippedShareholders,
    totals: {
      total_debit: totalDebit,
      total_credit: totalCredit,
      currency_code: totalsCurrencyCode || null,
    },
    journal_context: journalContext
      ? {
          book_id: journalContext.bookId,
          book_code: journalContext.bookCode,
          fiscal_period_id: journalContext.fiscalPeriodId,
          period_start_date: journalContext.startDate,
          period_end_date: journalContext.endDate,
          base_currency_code: journalContext.baseCurrencyCode,
        }
      : null,
    validation: {
      has_blocking_errors: blockingErrors.length > 0,
      blocking_errors: blockingErrors,
      warnings,
      mixed_currency:
        currencyGroups.size > 1
          ? Array.from(currencyGroups.entries()).map(([currency_code, members]) => ({
              currency_code,
              shareholders: members,
            }))
          : [],
    },
  };
}

export function toCommitmentJournalFailureMessage(reason) {
  switch (String(reason || "")) {
    case "CAPITAL_SUB_ACCOUNT_REQUIRED":
      return "capitalSubAccountId is required to create commitment journal";
    case "COMMITMENT_DEBIT_SUB_ACCOUNT_REQUIRED":
      return "commitmentDebitSubAccountId is required to create commitment journal";
    case "AUTH_USER_REQUIRED":
      return "Authenticated user is required to create commitment journal";
    case "NO_OPEN_BOOK_PERIOD":
      return "No OPEN book/fiscal period found for legalEntityId";
    case "COMMITMENT_DATE_OUTSIDE_OPEN_PERIOD":
      return "commitmentDate must be within an OPEN fiscal period for legalEntityId";
    case "COMMITMENT_DEBIT_SUB_ACCOUNT_INVALID":
      return "commitmentDebitSubAccountId must reference an active, postable, leaf EQUITY account in the same legal entity";
    default:
      return "Commitment journal could not be created";
  }
}

export async function createShareholderCommitmentDraftJournal(tx, payload) {
  const amount = normalizeMoney(payload.amount);
  if (amount <= 0) {
    return {
      attempted: false,
      created: false,
      reason: "NO_COMMITTED_CAPITAL_INCREASE",
      amount: 0,
    };
  }

  if (!payload.capitalSubAccountId) {
    return {
      attempted: true,
      created: false,
      reason: "CAPITAL_SUB_ACCOUNT_REQUIRED",
      amount,
    };
  }

  const commitmentDebitSubAccountId = parsePositiveInt(payload.commitmentDebitSubAccountId);
  if (!commitmentDebitSubAccountId) {
    return {
      attempted: true,
      created: false,
      reason: "COMMITMENT_DEBIT_SUB_ACCOUNT_REQUIRED",
      amount,
    };
  }

  if (!payload.userId) {
    return {
      attempted: true,
      created: false,
      reason: "AUTH_USER_REQUIRED",
      amount,
    };
  }

  const commitmentDate = payload.commitmentDate
    ? toIsoDate(payload.commitmentDate, "commitmentDate")
    : toIsoDate(new Date(), "commitmentDate");
  const journalContext = await resolveOpenBookPeriodForLegalEntity(
    tx,
    payload.tenantId,
    payload.legalEntityId,
    commitmentDate
  );
  if (!journalContext?.bookId || !journalContext?.fiscalPeriodId) {
    return {
      attempted: true,
      created: false,
      reason: "NO_OPEN_BOOK_PERIOD",
      amount,
    };
  }

  if (commitmentDate < journalContext.startDate || commitmentDate > journalContext.endDate) {
    return {
      attempted: true,
      created: false,
      reason: "COMMITMENT_DATE_OUTSIDE_OPEN_PERIOD",
      amount,
      bookId: journalContext.bookId,
      fiscalPeriodId: journalContext.fiscalPeriodId,
    };
  }

  const debitAccountResult = await tx.query(
    `SELECT
       a.id,
       a.code,
       a.name,
       a.account_type,
       a.is_active,
       a.allow_posting,
       EXISTS(
         SELECT 1
         FROM accounts child
         WHERE child.parent_account_id = a.id
           AND child.is_active = TRUE
       ) AS has_active_children,
       c.tenant_id AS account_tenant_id,
       c.legal_entity_id AS account_legal_entity_id
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE a.id = ?
       AND c.tenant_id = ?
     LIMIT 1`,
    [commitmentDebitSubAccountId, payload.tenantId]
  );
  const debitAccountRow = debitAccountResult.rows[0];
  const debitAccountId = parsePositiveInt(debitAccountRow?.id);
  const debitAccountTenantId = parsePositiveInt(debitAccountRow?.account_tenant_id);
  const debitAccountLegalEntityId = parsePositiveInt(debitAccountRow?.account_legal_entity_id);
  const debitAccountValid =
    debitAccountId &&
    debitAccountTenantId === payload.tenantId &&
    debitAccountLegalEntityId === payload.legalEntityId &&
    String(debitAccountRow?.account_type || "").toUpperCase() === "EQUITY" &&
    Boolean(debitAccountRow?.is_active) &&
    Boolean(debitAccountRow?.allow_posting) &&
    !Boolean(debitAccountRow?.has_active_children);

  if (!debitAccountValid) {
    return {
      attempted: true,
      created: false,
      reason: "COMMITMENT_DEBIT_SUB_ACCOUNT_INVALID",
      amount,
      bookId: journalContext.bookId,
      fiscalPeriodId: journalContext.fiscalPeriodId,
    };
  }
  const debitAccount = {
    id: debitAccountId,
    code: String(debitAccountRow?.code || ""),
    name: String(debitAccountRow?.name || ""),
  };

  const entryDate = commitmentDate;
  const documentDate = commitmentDate;
  const journalNo = generateAutoJournalNo("TAAHHUT");
  const description = `Shareholder commitment (${payload.shareholderCode})`;
  const referenceNo = `SHAREHOLDER_COMMITMENT:${payload.shareholderId}:${Date.now()}`.slice(
    0,
    100
  );
  const currencyCode = String(
    journalContext.baseCurrencyCode || payload.currencyCode || "USD"
  ).toUpperCase();

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
        created_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, 'SYSTEM', 'DRAFT', ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      payload.tenantId,
      payload.legalEntityId,
      journalContext.bookId,
      journalContext.fiscalPeriodId,
      journalNo,
      entryDate,
      documentDate,
      currencyCode,
      description,
      referenceNo,
      amount,
      amount,
      payload.userId,
    ]
  );
  const journalEntryId = parsePositiveInt(entryResult.rows.insertId);
  if (!journalEntryId) {
    throw badRequest("Failed to create shareholder commitment journal");
  }

  await tx.query(
    `INSERT INTO journal_lines (
        journal_entry_id,
        line_no,
        account_id,
        operating_unit_id,
        counterparty_legal_entity_id,
        description,
        currency_code,
        amount_txn,
        debit_base,
        credit_base,
        tax_code
      )
      VALUES (?, 1, ?, NULL, NULL, ?, ?, ?, ?, 0, NULL)`,
    [
      journalEntryId,
      debitAccount.id,
      `Shareholder commitment receivable (${payload.shareholderCode})`,
      currencyCode,
      amount,
      amount,
    ]
  );

  await tx.query(
    `INSERT INTO journal_lines (
        journal_entry_id,
        line_no,
        account_id,
        operating_unit_id,
        counterparty_legal_entity_id,
        description,
        currency_code,
        amount_txn,
        debit_base,
        credit_base,
        tax_code
      )
      VALUES (?, 2, ?, NULL, NULL, ?, ?, ?, 0, ?, NULL)`,
    [
      journalEntryId,
      payload.capitalSubAccountId,
      `Committed capital (${payload.shareholderCode})`,
      currencyCode,
      amount * -1,
      amount,
    ]
  );

  if (parsePositiveInt(payload.shareholderId)) {
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        payload.tenantId,
        payload.shareholderId,
        payload.legalEntityId,
        journalEntryId,
        "SINGLE",
        amount,
        currencyCode,
        payload.userId,
      ]
    );
  }

  return {
    attempted: true,
    created: true,
    reason: null,
    journalEntryId,
    journalNo,
    bookId: journalContext.bookId,
    bookCode: journalContext.bookCode,
    fiscalPeriodId: journalContext.fiscalPeriodId,
    entryDate,
    amount,
    debitAccountId: debitAccount.id,
    debitAccountCode: debitAccount.code,
    creditAccountId: payload.capitalSubAccountId,
  };
}
