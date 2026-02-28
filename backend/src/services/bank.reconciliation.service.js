import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  decodeCursorToken,
  encodeCursorToken,
  requireCursorDateOnly,
  requireCursorId,
  toCursorDateOnly,
} from "../utils/cursorPagination.js";
import { autoResolveOpenReconciliationExceptionsForLine } from "./bank.reconciliationExceptions.service.js";

const MATCH_EPSILON = 0.005;
const SUGGESTION_LIMIT = 20;

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

function absAmount(value) {
  return Math.abs(toAmount(value));
}

function lineStatusFromMatchedTotal(lineAmountAbs, matchedTotal) {
  const target = absAmount(lineAmountAbs);
  const matched = absAmount(matchedTotal);
  if (matched <= MATCH_EPSILON) {
    return "UNMATCHED";
  }
  if (Math.abs(matched - target) <= MATCH_EPSILON) {
    return "MATCHED";
  }
  return "PARTIAL";
}

async function findBankAccountScopeById({ tenantId, bankAccountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, legal_entity_id
     FROM bank_accounts
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, bankAccountId]
  );
  return result.rows?.[0] || null;
}

async function findStatementLineScopeById({ tenantId, lineId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, legal_entity_id, bank_account_id
     FROM bank_statement_lines
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, lineId]
  );
  return result.rows?.[0] || null;
}

async function getStatementLineCore({ tenantId, lineId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        l.id,
        l.tenant_id,
        l.legal_entity_id,
        l.import_id,
        l.bank_account_id,
        l.line_no,
        l.txn_date,
        l.value_date,
        l.description,
        l.reference_no,
        l.amount,
        l.currency_code,
        l.balance_after,
        l.recon_status,
        l.reconciliation_method,
        l.reconciliation_rule_id,
        l.reconciliation_confidence,
        l.auto_post_template_id,
        l.auto_post_journal_entry_id,
        l.reconciliation_difference_type,
        l.reconciliation_difference_amount,
        l.reconciliation_difference_profile_id,
        l.reconciliation_difference_journal_entry_id,
        l.raw_row_json,
        l.created_at,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        ba.gl_account_id AS bank_gl_account_id,
        ba.currency_code AS bank_account_currency_code,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM bank_statement_lines l
     JOIN bank_accounts ba
       ON ba.id = l.bank_account_id
      AND ba.tenant_id = l.tenant_id
      AND ba.legal_entity_id = l.legal_entity_id
     JOIN legal_entities le
       ON le.id = l.legal_entity_id
      AND le.tenant_id = l.tenant_id
     WHERE l.tenant_id = ?
       AND l.id = ?
     LIMIT 1`,
    [tenantId, lineId]
  );
  return result.rows?.[0] || null;
}

async function getActiveMatchesForLine({ tenantId, lineId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        m.id,
        m.tenant_id,
        m.legal_entity_id,
        m.statement_line_id,
        m.match_type,
        m.matched_entity_type,
        m.matched_entity_id,
        m.reconciliation_rule_id,
        m.reconciliation_confidence,
        m.matched_amount,
        m.status,
        m.notes,
        m.matched_by_user_id,
        m.matched_at,
        m.reversed_by_user_id,
        m.reversed_at,
        m.created_at,
        m.updated_at
     FROM bank_reconciliation_matches m
     WHERE m.tenant_id = ?
       AND m.statement_line_id = ?
       AND m.status = 'ACTIVE'
     ORDER BY m.id ASC`,
    [tenantId, lineId]
  );
  return result.rows || [];
}

async function getActiveMatchedTotalForLine({ tenantId, lineId, runQuery = query }) {
  const result = await runQuery(
    `SELECT COALESCE(SUM(matched_amount), 0) AS matched_total
     FROM bank_reconciliation_matches
     WHERE tenant_id = ?
       AND statement_line_id = ?
       AND status = 'ACTIVE'`,
    [tenantId, lineId]
  );
  return toAmount(result.rows?.[0]?.matched_total || 0);
}

async function writeReconciliationAudit({
  tenantId,
  legalEntityId,
  statementLineId,
  action,
  payload = null,
  userId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO bank_reconciliation_audit (
        tenant_id,
        legal_entity_id,
        statement_line_id,
        action,
        payload_json,
        acted_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, statementLineId, action, safeJson(payload), userId]
  );
}

async function recomputeLineReconStatus({ tenantId, lineId, userId = null, runQuery = query }) {
  const line = await getStatementLineCore({ tenantId, lineId, runQuery });
  if (!line) {
    throw badRequest("Statement line not found");
  }
  if (normalizeUpperText(line.recon_status) === "IGNORED") {
    return line;
  }

  const matchedTotal = await getActiveMatchedTotalForLine({ tenantId, lineId, runQuery });
  const nextStatus = lineStatusFromMatchedTotal(line.amount, matchedTotal);
  const currentStatus = normalizeUpperText(line.recon_status) || "UNMATCHED";
  if (nextStatus !== currentStatus) {
    await runQuery(
      `UPDATE bank_statement_lines
       SET recon_status = ?
       WHERE tenant_id = ?
         AND id = ?`,
      [nextStatus, tenantId, lineId]
    );
    await writeReconciliationAudit({
      tenantId,
      legalEntityId: line.legal_entity_id,
      statementLineId: lineId,
      action: "AUTO_STATUS",
      payload: {
        from: currentStatus,
        to: nextStatus,
        matchedTotal,
        targetAmount: absAmount(line.amount),
      },
      userId,
      runQuery,
    });
  }

  return getStatementLineCore({ tenantId, lineId, runQuery });
}

async function assertBankAccountFilterScope({
  req,
  tenantId,
  bankAccountId,
  assertScopeAccess,
}) {
  const row = await findBankAccountScopeById({ tenantId, bankAccountId });
  if (!row) {
    throw badRequest("bankAccountId not found");
  }
  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "bankAccountId");
  return row;
}

async function assertStatementLineFilterScope({
  req,
  tenantId,
  statementLineId,
  assertScopeAccess,
}) {
  const row = await findStatementLineScopeById({ tenantId, lineId: statementLineId });
  if (!row) {
    throw badRequest("statementLineId not found");
  }
  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "statementLineId");
  return row;
}

async function assertMatchTargetExists({ tenantId, legalEntityId, matchInput, runQuery = query }) {
  if (matchInput.matchedEntityType === "JOURNAL") {
    const result = await runQuery(
      `SELECT id, status, tenant_id, legal_entity_id
       FROM journal_entries
       WHERE id = ?
         AND tenant_id = ?
         AND legal_entity_id = ?
       LIMIT 1`,
      [matchInput.matchedEntityId, tenantId, legalEntityId]
    );
    const row = result.rows?.[0] || null;
    if (!row) {
      throw badRequest("Journal not found for tenant/legal entity");
    }
    if (normalizeUpperText(row.status) !== "POSTED") {
      throw badRequest("Only POSTED journals can be reconciled");
    }
    return row;
  }

  if (matchInput.matchedEntityType === "PAYMENT_BATCH") {
    const result = await runQuery(
      `SELECT id, status, tenant_id, legal_entity_id
       FROM payment_batches
       WHERE id = ?
         AND tenant_id = ?
         AND legal_entity_id = ?
       LIMIT 1`,
      [matchInput.matchedEntityId, tenantId, legalEntityId]
    );
    const row = result.rows?.[0] || null;
    if (!row) {
      throw badRequest("Payment batch not found for tenant/legal entity");
    }
    if (normalizeUpperText(row.status) !== "POSTED") {
      throw badRequest("Only POSTED payment batches can be reconciled");
    }
    return row;
  }

  if (matchInput.matchedEntityType === "CASH_TXN") {
    const result = await runQuery(
      `SELECT ct.id, ct.status, ct.tenant_id, cr.legal_entity_id
       FROM cash_transactions ct
       JOIN cash_registers cr
         ON cr.id = ct.cash_register_id
        AND cr.tenant_id = ct.tenant_id
       WHERE ct.id = ?
         AND ct.tenant_id = ?
         AND cr.legal_entity_id = ?
       LIMIT 1`,
      [matchInput.matchedEntityId, tenantId, legalEntityId]
    );
    const row = result.rows?.[0] || null;
    if (!row) {
      throw badRequest("Cash transaction not found for tenant/legal entity");
    }
    if (normalizeUpperText(row.status) !== "POSTED") {
      throw badRequest("Only POSTED cash transactions can be reconciled");
    }
    return row;
  }

  if (matchInput.matchedEntityType === "MANUAL_ADJUSTMENT") {
    const result = await runQuery(
      `SELECT id, status, tenant_id, legal_entity_id
       FROM bank_reconciliation_difference_adjustments
       WHERE id = ?
         AND tenant_id = ?
         AND legal_entity_id = ?
       LIMIT 1`,
      [matchInput.matchedEntityId, tenantId, legalEntityId]
    );
    const row = result.rows?.[0] || null;
    if (!row) {
      throw badRequest("Manual adjustment not found for tenant/legal entity");
    }
    if (normalizeUpperText(row.status) !== "POSTED") {
      throw badRequest("Only POSTED manual adjustments can be reconciled");
    }
    return row;
  }

  throw badRequest(`${matchInput.matchedEntityType} matching is not enabled yet`);
}

function buildJournalSuggestionScore(line, row) {
  let score = 0;
  const lineAbs = absAmount(line.amount);
  const candAbs = absAmount(row.bank_gl_amount);
  const diff = Math.abs(candAbs - lineAbs);
  if (diff <= 0.005) {
    score += 100;
  } else if (diff <= 0.01) {
    score += 70;
  }

  const entryDate = String(row.entry_date || "");
  const txnDate = String(line.txn_date || "");
  if (entryDate && txnDate && entryDate === txnDate) {
    score += 20;
  }

  const ref = String(line.reference_no || "")
    .trim()
    .toUpperCase();
  if (ref) {
    const hay = [row.journal_no, row.reference_no, row.description]
      .map((v) => String(v || "").toUpperCase())
      .join(" ");
    if (hay.includes(ref)) {
      score += 10;
    }
  }

  const desc = String(line.description || "")
    .trim()
    .toUpperCase();
  if (desc) {
    const token = desc.split(/\s+/).find((part) => part.length >= 4) || "";
    if (token) {
      const hay = String(row.description || "").toUpperCase();
      if (hay.includes(token)) {
        score += 5;
      }
    }
  }
  return score;
}

export async function listReconciliationQueueRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["l.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "l.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("l.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.bankAccountId) {
    const bankScope = await assertBankAccountFilterScope({
      req,
      tenantId,
      bankAccountId: filters.bankAccountId,
      assertScopeAccess,
    });
    if (
      filters.legalEntityId &&
      parsePositiveInt(bankScope.legal_entity_id) !== parsePositiveInt(filters.legalEntityId)
    ) {
      throw badRequest("bankAccountId does not belong to legalEntityId");
    }
    conditions.push("l.bank_account_id = ?");
    params.push(filters.bankAccountId);
  }

  if (filters.reconStatus) {
    conditions.push("l.recon_status = ?");
    params.push(filters.reconStatus);
  } else {
    conditions.push("l.recon_status IN ('UNMATCHED','PARTIAL','IGNORED')");
  }

  if (filters.q) {
    conditions.push("(l.description LIKE ? OR l.reference_no LIKE ?)");
    const like = `%${filters.q}%`;
    params.push(like, like);
  }

  const baseConditions = [...conditions];
  const baseParams = [...params];
  const cursorToken = filters.cursor || null;
  const cursor = decodeCursorToken(cursorToken);
  const pageConditions = [...baseConditions];
  const pageParams = [...baseParams];
  if (cursorToken) {
    const cursorTxnDate = requireCursorDateOnly(cursor, "txnDate");
    const cursorId = requireCursorId(cursor, "id");
    pageConditions.push("(l.txn_date < ? OR (l.txn_date = ? AND l.id < ?))");
    pageParams.push(cursorTxnDate, cursorTxnDate, cursorId);
  }
  const whereSql = pageConditions.join(" AND ");
  const countWhereSql = baseConditions.join(" AND ");
  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM bank_statement_lines l
     WHERE ${countWhereSql}`,
    baseParams
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const safeLimit =
    Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset =
    Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const effectiveOffset = cursorToken ? 0 : safeOffset;

  const listResult = await query(
    `SELECT
        l.id,
        l.tenant_id,
        l.legal_entity_id,
        l.import_id,
        l.bank_account_id,
        l.txn_date,
        l.value_date,
        l.description,
        l.reference_no,
        l.amount,
        l.currency_code,
        l.balance_after,
        l.recon_status,
        l.reconciliation_method,
        l.reconciliation_rule_id,
        l.reconciliation_confidence,
        l.auto_post_template_id,
        l.auto_post_journal_entry_id,
        l.reconciliation_difference_type,
        l.reconciliation_difference_amount,
        l.reconciliation_difference_profile_id,
        l.reconciliation_difference_journal_entry_id,
        l.created_at,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        ba.gl_account_id AS bank_gl_account_id,
        i.original_filename,
        COALESCE(m.active_match_count, 0) AS active_match_count,
        COALESCE(m.active_matched_total, 0) AS active_matched_total
     FROM bank_statement_lines l
     JOIN bank_accounts ba
       ON ba.id = l.bank_account_id
      AND ba.tenant_id = l.tenant_id
      AND ba.legal_entity_id = l.legal_entity_id
     JOIN bank_statement_imports i
       ON i.id = l.import_id
      AND i.tenant_id = l.tenant_id
      AND i.legal_entity_id = l.legal_entity_id
     LEFT JOIN (
       SELECT
         tenant_id,
         statement_line_id,
         COUNT(*) AS active_match_count,
         COALESCE(SUM(matched_amount), 0) AS active_matched_total
       FROM bank_reconciliation_matches
       WHERE tenant_id = ?
         AND status = 'ACTIVE'
       GROUP BY tenant_id, statement_line_id
     ) m
       ON m.tenant_id = l.tenant_id
      AND m.statement_line_id = l.id
     WHERE ${whereSql}
     ORDER BY l.txn_date DESC, l.id DESC
     LIMIT ${safeLimit} OFFSET ${effectiveOffset}`,
    [tenantId, ...pageParams]
  );

  const rows = listResult.rows || [];
  const lastRow = rows.length > 0 ? rows[rows.length - 1] : null;
  const nextCursor =
    cursorToken || safeOffset === 0
      ? rows.length === safeLimit && lastRow
        ? encodeCursorToken({
            txnDate: toCursorDateOnly(lastRow.txn_date),
            id: parsePositiveInt(lastRow.id),
          })
        : null
      : null;

  return {
    rows,
    total,
    limit: filters.limit,
    offset: cursorToken ? 0 : filters.offset,
    pageMode: cursorToken ? "CURSOR" : "OFFSET",
    nextCursor,
  };
}

export async function getReconciliationSuggestionsForLine({
  req,
  tenantId,
  lineId,
  userId = null,
  assertScopeAccess,
}) {
  const line = await getStatementLineCore({ tenantId, lineId });
  if (!line) {
    throw badRequest("Statement line not found");
  }
  assertScopeAccess(req, "legal_entity", line.legal_entity_id, "lineId");

  const activeMatches = await getActiveMatchesForLine({ tenantId, lineId });
  const activeMatchedTotal = activeMatches.reduce((sum, row) => sum + toAmount(row.matched_amount), 0);
  const remainingAmount = Math.max(0, absAmount(line.amount) - absAmount(activeMatchedTotal));

  const journalResult = await query(
    `SELECT
        je.id AS journal_id,
        je.journal_no,
        je.entry_date,
        je.document_date,
        je.posted_at,
        je.description,
        je.reference_no,
        ABS(COALESCE(SUM(jl.debit_base - jl.credit_base), 0)) AS bank_gl_amount
     FROM journal_entries je
     JOIN journal_lines jl
       ON jl.journal_entry_id = je.id
     WHERE je.tenant_id = ?
       AND je.legal_entity_id = ?
       AND je.status = 'POSTED'
       AND jl.account_id = ?
       AND je.entry_date BETWEEN DATE_SUB(?, INTERVAL 7 DAY) AND DATE_ADD(?, INTERVAL 7 DAY)
     GROUP BY je.id, je.journal_no, je.entry_date, je.document_date, je.posted_at, je.description, je.reference_no
     HAVING ABS(ABS(COALESCE(SUM(jl.debit_base - jl.credit_base), 0)) - ABS(?)) <= 0.01
     ORDER BY je.entry_date DESC, je.id DESC
     LIMIT ${SUGGESTION_LIMIT}`,
    [
      tenantId,
      line.legal_entity_id,
      line.bank_gl_account_id,
      line.txn_date,
      line.txn_date,
      line.amount,
    ]
  );

  const suggestions = (journalResult.rows || [])
    .map((row) => ({
      suggestionType: "JOURNAL",
      matchedEntityType: "JOURNAL",
      matchedEntityId: parsePositiveInt(row.journal_id),
      displayRef: row.journal_no || `JE#${row.journal_id}`,
      displayText: row.description || row.reference_no || row.journal_no || null,
      suggestedAmount: Number(Math.max(remainingAmount, 0).toFixed(6)),
      bankGlAmount: toAmount(row.bank_gl_amount),
      score: buildJournalSuggestionScore(line, row),
      entryDate: row.entry_date,
      postedAt: row.posted_at,
      referenceNo: row.reference_no,
    }))
    .sort((a, b) => b.score - a.score || b.matchedEntityId - a.matchedEntityId);

  await writeReconciliationAudit({
    tenantId,
    legalEntityId: line.legal_entity_id,
    statementLineId: lineId,
    action: "SUGGESTED",
    payload: {
      engine: "journal-v1",
      suggestionCount: suggestions.length,
      activeMatchedTotal,
      remainingAmount,
    },
    userId,
  });

  return {
    line,
    matches: activeMatches,
    suggestions,
  };
}

export async function matchReconciliationLine({
  req,
  tenantId,
  lineId,
  matchInput,
  userId,
  assertScopeAccess,
}) {
  const line = await getStatementLineCore({ tenantId, lineId });
  if (!line) {
    throw badRequest("Statement line not found");
  }
  assertScopeAccess(req, "legal_entity", line.legal_entity_id, "lineId");

  if (normalizeUpperText(line.recon_status) === "IGNORED") {
    throw badRequest("Ignored line cannot be matched (unignore flow is not implemented yet)");
  }

  const currentMatched = await getActiveMatchedTotalForLine({ tenantId, lineId });
  const targetAbs = absAmount(line.amount);
  const nextTotal = toAmount(currentMatched + toAmount(matchInput.matchedAmount));
  if (nextTotal - targetAbs > MATCH_EPSILON) {
    throw badRequest("Matched amount exceeds statement line amount");
  }

  return withTransaction(async (tx) => {
    await assertMatchTargetExists({
      tenantId,
      legalEntityId: line.legal_entity_id,
      matchInput,
      runQuery: tx.query,
    });

    const reconciliationRuleId = parsePositiveInt(matchInput.reconciliationRuleId);
    const reconciliationConfidence =
      matchInput.reconciliationConfidence === undefined ||
      matchInput.reconciliationConfidence === null ||
      matchInput.reconciliationConfidence === ""
        ? null
        : Number(Number(matchInput.reconciliationConfidence).toFixed(2));
    const reconciliationMethod = matchInput.reconciliationMethod
      ? normalizeUpperText(matchInput.reconciliationMethod)
      : null;

    const insertResult = await tx.query(
      `INSERT INTO bank_reconciliation_matches (
          tenant_id,
          legal_entity_id,
          statement_line_id,
          match_type,
          matched_entity_type,
          matched_entity_id,
          reconciliation_rule_id,
          reconciliation_confidence,
          matched_amount,
          status,
          notes,
          matched_by_user_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)`,
      [
        tenantId,
        line.legal_entity_id,
        lineId,
        matchInput.matchType || "MANUAL",
        matchInput.matchedEntityType,
        matchInput.matchedEntityId,
        reconciliationRuleId || null,
        reconciliationConfidence,
        toAmount(matchInput.matchedAmount),
        matchInput.notes || null,
        userId,
      ]
    );

    if (reconciliationMethod || reconciliationRuleId || reconciliationConfidence !== null) {
      await tx.query(
        `UPDATE bank_statement_lines
         SET reconciliation_method = ?,
             reconciliation_rule_id = ?,
             reconciliation_confidence = ?
         WHERE tenant_id = ?
           AND id = ?`,
        [
          reconciliationMethod || null,
          reconciliationRuleId || null,
          reconciliationConfidence,
          tenantId,
          lineId,
        ]
      );
    }

    const matchId = parsePositiveInt(insertResult.rows?.insertId);
    await writeReconciliationAudit({
      tenantId,
      legalEntityId: line.legal_entity_id,
      statementLineId: lineId,
      action: "MATCHED",
      payload: {
        matchId,
        matchType: matchInput.matchType || "MANUAL",
        matchedEntityType: matchInput.matchedEntityType,
        matchedEntityId: matchInput.matchedEntityId,
        matchedAmount: toAmount(matchInput.matchedAmount),
        reconciliationMethod: reconciliationMethod || null,
        reconciliationRuleId: reconciliationRuleId || null,
        reconciliationConfidence,
        notes: matchInput.notes || null,
      },
      userId,
      runQuery: tx.query,
    });

    const updatedLine = await recomputeLineReconStatus({
      tenantId,
      lineId,
      userId,
      runQuery: tx.query,
    });
    if (normalizeUpperText(updatedLine?.recon_status) === "MATCHED") {
      await autoResolveOpenReconciliationExceptionsForLine({
        tenantId,
        legalEntityId: line.legal_entity_id,
        statementLineId: lineId,
        userId,
        runQuery: tx.query,
      });
    }
    const matches = await getActiveMatchesForLine({ tenantId, lineId, runQuery: tx.query });

    return {
      line: updatedLine,
      matches,
    };
  });
}

export async function reconcileStatementLineToJournal({
  req,
  tenantId,
  lineId,
  journalEntryId,
  userId,
  notes = null,
  reconciliationMethod = "RULE_AUTO_POST",
  reconciliationRuleId = null,
  reconciliationConfidence = null,
  assertScopeAccess,
}) {
  const parsedLineId = parsePositiveInt(lineId);
  const parsedJournalEntryId = parsePositiveInt(journalEntryId);
  if (!parsedLineId || !parsedJournalEntryId) {
    throw badRequest("lineId and journalEntryId are required");
  }

  const line = await getStatementLineCore({ tenantId, lineId: parsedLineId });
  if (!line) {
    throw badRequest("Statement line not found");
  }
  if (req && typeof assertScopeAccess === "function") {
    assertScopeAccess(req, "legal_entity", line.legal_entity_id, "lineId");
  }

  const activeMatches = await getActiveMatchesForLine({ tenantId, lineId: parsedLineId });
  const existingJournalMatch = activeMatches.find(
    (row) =>
      normalizeUpperText(row?.status) === "ACTIVE" &&
      normalizeUpperText(row?.matched_entity_type) === "JOURNAL" &&
      parsePositiveInt(row?.matched_entity_id) === parsedJournalEntryId
  );

  if (existingJournalMatch) {
    return {
      idempotent: true,
      line,
      matches: activeMatches,
      matchedAmount: toAmount(existingJournalMatch.matched_amount),
    };
  }

  const activeMatchedTotal = activeMatches.reduce(
    (sum, row) => sum + toAmount(row?.matched_amount || 0),
    0
  );
  const remaining = Number(
    Math.max(0, absAmount(line.amount) - absAmount(activeMatchedTotal)).toFixed(6)
  );
  if (remaining <= MATCH_EPSILON) {
    return {
      idempotent: true,
      line,
      matches: activeMatches,
      matchedAmount: 0,
    };
  }

  const result = await matchReconciliationLine({
    req,
    tenantId,
    lineId: parsedLineId,
    matchInput: {
      matchType: "AUTO_RULE",
      matchedEntityType: "JOURNAL",
      matchedEntityId: parsedJournalEntryId,
      matchedAmount: remaining,
      notes: notes || `Auto-post journal reconciliation JE#${parsedJournalEntryId}`,
      reconciliationMethod,
      reconciliationRuleId: parsePositiveInt(reconciliationRuleId) || null,
      reconciliationConfidence:
        reconciliationConfidence === null || reconciliationConfidence === undefined
          ? null
          : Number(Number(reconciliationConfidence).toFixed(2)),
    },
    userId,
    assertScopeAccess: assertScopeAccess || (() => {}),
  });

  return {
    idempotent: false,
    matchedAmount: remaining,
    ...result,
  };
}

export async function unmatchReconciliationLine({
  req,
  tenantId,
  lineId,
  unmatchInput,
  userId,
  assertScopeAccess,
}) {
  const line = await getStatementLineCore({ tenantId, lineId });
  if (!line) {
    throw badRequest("Statement line not found");
  }
  assertScopeAccess(req, "legal_entity", line.legal_entity_id, "lineId");

  return withTransaction(async (tx) => {
    let sql = `UPDATE bank_reconciliation_matches
               SET status = 'REVERSED',
                   reversed_by_user_id = ?,
                   reversed_at = CURRENT_TIMESTAMP
               WHERE tenant_id = ?
                 AND statement_line_id = ?
                 AND status = 'ACTIVE'`;
    const params = [userId, tenantId, lineId];
    if (unmatchInput.matchId) {
      sql += " AND id = ?";
      params.push(unmatchInput.matchId);
    }

    const updateResult = await tx.query(sql, params);
    const affectedRows = Number(updateResult.rows?.affectedRows || 0);
    if (!affectedRows) {
      throw badRequest("No active match found to unmatch");
    }

    await writeReconciliationAudit({
      tenantId,
      legalEntityId: line.legal_entity_id,
      statementLineId: lineId,
      action: "UNMATCHED",
      payload: {
        matchId: unmatchInput.matchId || null,
        reversedCount: affectedRows,
        notes: unmatchInput.notes || null,
      },
      userId,
      runQuery: tx.query,
    });

    let updatedLine;
    if (normalizeUpperText(line.recon_status) === "IGNORED") {
      updatedLine = await getStatementLineCore({ tenantId, lineId, runQuery: tx.query });
    } else {
      updatedLine = await recomputeLineReconStatus({
        tenantId,
        lineId,
        userId,
        runQuery: tx.query,
      });
    }
    const matches = await getActiveMatchesForLine({ tenantId, lineId, runQuery: tx.query });

    return {
      line: updatedLine,
      matches,
    };
  });
}

export async function ignoreReconciliationLine({
  req,
  tenantId,
  lineId,
  ignoreInput,
  userId,
  assertScopeAccess,
}) {
  const line = await getStatementLineCore({ tenantId, lineId });
  if (!line) {
    throw badRequest("Statement line not found");
  }
  assertScopeAccess(req, "legal_entity", line.legal_entity_id, "lineId");

  const activeMatchedTotal = await getActiveMatchedTotalForLine({ tenantId, lineId });
  if (activeMatchedTotal > MATCH_EPSILON) {
    throw badRequest("Line has active matches; unmatch first before ignore");
  }

  if (normalizeUpperText(line.recon_status) === "IGNORED") {
    return {
      line,
      matches: await getActiveMatchesForLine({ tenantId, lineId }),
    };
  }

  return withTransaction(async (tx) => {
    await tx.query(
      `UPDATE bank_statement_lines
       SET recon_status = 'IGNORED'
       WHERE tenant_id = ?
         AND id = ?`,
      [tenantId, lineId]
    );

    await writeReconciliationAudit({
      tenantId,
      legalEntityId: line.legal_entity_id,
      statementLineId: lineId,
      action: "IGNORE",
      payload: {
        reason: ignoreInput.reason || null,
      },
      userId,
      runQuery: tx.query,
    });

    return {
      line: await getStatementLineCore({ tenantId, lineId, runQuery: tx.query }),
      matches: await getActiveMatchesForLine({ tenantId, lineId, runQuery: tx.query }),
    };
  });
}

export async function unignoreReconciliationLine({
  req,
  tenantId,
  lineId,
  unignoreInput,
  userId,
  assertScopeAccess,
}) {
  const line = await getStatementLineCore({ tenantId, lineId });
  if (!line) {
    throw badRequest("Statement line not found");
  }
  assertScopeAccess(req, "legal_entity", line.legal_entity_id, "lineId");

  if (normalizeUpperText(line.recon_status) !== "IGNORED") {
    return {
      line,
      matches: await getActiveMatchesForLine({ tenantId, lineId }),
    };
  }

  return withTransaction(async (tx) => {
    const matchedTotal = await getActiveMatchedTotalForLine({ tenantId, lineId, runQuery: tx.query });
    const nextStatus = lineStatusFromMatchedTotal(line.amount, matchedTotal);
    await tx.query(
      `UPDATE bank_statement_lines
       SET recon_status = ?
       WHERE tenant_id = ?
         AND id = ?`,
      [nextStatus, tenantId, lineId]
    );

    await writeReconciliationAudit({
      tenantId,
      legalEntityId: line.legal_entity_id,
      statementLineId: lineId,
      action: "UNIGNORE",
      payload: {
        reason: unignoreInput.reason || null,
        from: "IGNORED",
        to: nextStatus,
        matchedTotal,
        targetAmount: absAmount(line.amount),
      },
      userId,
      runQuery: tx.query,
    });

    const updatedLine = await getStatementLineCore({ tenantId, lineId, runQuery: tx.query });
    if (normalizeUpperText(updatedLine?.recon_status) === "MATCHED") {
      await autoResolveOpenReconciliationExceptionsForLine({
        tenantId,
        legalEntityId: line.legal_entity_id,
        statementLineId: lineId,
        userId,
        runQuery: tx.query,
      });
    }

    return {
      line: updatedLine,
      matches: await getActiveMatchesForLine({ tenantId, lineId, runQuery: tx.query }),
    };
  });
}

export async function listReconciliationAuditRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["a.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "a.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("a.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.statementLineId) {
    const lineScope = await assertStatementLineFilterScope({
      req,
      tenantId,
      statementLineId: filters.statementLineId,
      assertScopeAccess,
    });
    if (
      filters.legalEntityId &&
      parsePositiveInt(lineScope.legal_entity_id) !== parsePositiveInt(filters.legalEntityId)
    ) {
      throw badRequest("statementLineId does not belong to legalEntityId");
    }
    conditions.push("a.statement_line_id = ?");
    params.push(filters.statementLineId);
  }

  if (filters.action) {
    conditions.push("a.action = ?");
    params.push(filters.action);
  }

  const whereSql = conditions.join(" AND ");
  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM bank_reconciliation_audit a
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
        a.id,
        a.tenant_id,
        a.legal_entity_id,
        a.statement_line_id,
        a.action,
        a.payload_json,
        a.acted_by_user_id,
        a.acted_at,
        l.bank_account_id,
        l.txn_date,
        l.description AS statement_description,
        l.reference_no AS statement_reference_no,
        l.amount AS statement_amount,
        l.currency_code AS statement_currency_code,
        l.recon_status AS statement_recon_status,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM bank_reconciliation_audit a
     JOIN bank_statement_lines l
       ON l.id = a.statement_line_id
      AND l.tenant_id = a.tenant_id
      AND l.legal_entity_id = a.legal_entity_id
     JOIN bank_accounts ba
       ON ba.id = l.bank_account_id
      AND ba.tenant_id = l.tenant_id
      AND ba.legal_entity_id = l.legal_entity_id
     JOIN legal_entities le
       ON le.id = a.legal_entity_id
      AND le.tenant_id = a.tenant_id
     WHERE ${whereSql}
     ORDER BY a.acted_at DESC, a.id DESC
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
