import { query } from "../db.js";
import {
  assertScopeAccess,
  buildScopeFilter,
  requirePermission,
} from "../middleware/rbac.js";
import {
  assertBookBelongsToTenant,
  assertFiscalPeriodBelongsToCalendar,
} from "../tenantGuards.js";
import {
  asyncHandler,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";

export function registerGlReadJournalRoutes(router, deps = {}) {
  const { resolveScopeFromBookId, resolveScopeFromJournalId } = deps;

  if (typeof resolveScopeFromBookId !== "function") {
    throw new Error("registerGlReadJournalRoutes requires resolveScopeFromBookId");
  }
  if (typeof resolveScopeFromJournalId !== "function") {
    throw new Error("registerGlReadJournalRoutes requires resolveScopeFromJournalId");
  }

  router.get(
    "/journals",
    requirePermission("gl.journal.read", {
      resolveScope: async (req, tenantId) => {
        const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
        if (legalEntityId) {
          return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
        }

        const bookId = parsePositiveInt(req.query?.bookId);
        if (bookId) {
          return resolveScopeFromBookId(bookId, tenantId);
        }

        return null;
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) throw badRequest("tenantId is required");

      const legalEntityId = parsePositiveInt(req.query.legalEntityId);
      const bookId = parsePositiveInt(req.query.bookId);
      const fiscalPeriodId = parsePositiveInt(req.query.fiscalPeriodId);
      const status = req.query.status ? String(req.query.status).toUpperCase() : null;
      const includeLines = String(req.query.includeLines || "").toLowerCase() === "true";

      if (legalEntityId) {
        assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
      }
      if (status && !["DRAFT", "POSTED", "REVERSED"].includes(status)) {
        throw badRequest("status must be one of DRAFT, POSTED, REVERSED");
      }

      const limitRaw = Number(req.query.limit);
      const limit =
        Number.isInteger(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 200) : 50;
      const offsetRaw = Number(req.query.offset);
      const offset = Number.isInteger(offsetRaw) && offsetRaw >= 0 ? offsetRaw : 0;

      const conditions = ["je.tenant_id = ?"];
      const params = [tenantId];
      conditions.push(buildScopeFilter(req, "legal_entity", "je.legal_entity_id", params));

      if (legalEntityId) {
        conditions.push("je.legal_entity_id = ?");
        params.push(legalEntityId);
      }
      if (bookId) {
        conditions.push("je.book_id = ?");
        params.push(bookId);
      }
      if (fiscalPeriodId) {
        conditions.push("je.fiscal_period_id = ?");
        params.push(fiscalPeriodId);
      }
      if (status) {
        conditions.push("je.status = ?");
        params.push(status);
      }

      const whereSql = conditions.join(" AND ");
      const countResult = await query(
        `SELECT COUNT(*) AS total
         FROM journal_entries je
         WHERE ${whereSql}`,
        params
      );
      const total = Number(countResult.rows[0]?.total || 0);

      const rowsResult = await query(
        `SELECT
           je.id, je.tenant_id, je.legal_entity_id, je.book_id, je.fiscal_period_id,
           je.journal_no, je.source_type, je.status, je.entry_date, je.document_date,
           je.currency_code, je.description, je.reference_no,
           je.total_debit_base, je.total_credit_base,
           je.created_by_user_id, je.posted_by_user_id, je.posted_at,
           je.reversed_by_user_id, je.reversed_at, je.reverse_reason,
           je.reversal_journal_entry_id, je.intercompany_source_journal_entry_id, je.created_at, je.updated_at,
           le.code AS legal_entity_code, le.name AS legal_entity_name,
           b.code AS book_code, b.name AS book_name,
           fp.fiscal_year, fp.period_no, fp.period_name,
           (
             SELECT COUNT(*)
             FROM journal_lines jl
             WHERE jl.journal_entry_id = je.id
           ) AS line_count
         FROM journal_entries je
         JOIN legal_entities le ON le.id = je.legal_entity_id
         JOIN books b ON b.id = je.book_id
         JOIN fiscal_periods fp ON fp.id = je.fiscal_period_id
         WHERE ${whereSql}
         ORDER BY je.id DESC
         LIMIT ${limit}
         OFFSET ${offset}`,
        params
      );

      const rows = rowsResult.rows || [];

      if (includeLines && rows.length > 0) {
        const journalIds = rows
          .map((row) => parsePositiveInt(row.id))
          .filter((value) => Boolean(value));

        if (journalIds.length > 0) {
          const placeholders = journalIds.map(() => "?").join(", ");
          const lineResult = await query(
            `SELECT
               jl.id, jl.journal_entry_id, jl.line_no, jl.account_id,
               jl.operating_unit_id, jl.counterparty_legal_entity_id,
               jl.description, jl.subledger_reference_no, jl.currency_code, jl.amount_txn, jl.debit_base,
               jl.credit_base, jl.tax_code, jl.created_at,
               a.code AS account_code, a.name AS account_name,
               ou.code AS operating_unit_code, ou.name AS operating_unit_name,
               cle.code AS counterparty_legal_entity_code,
               cle.name AS counterparty_legal_entity_name
             FROM journal_lines jl
             JOIN accounts a ON a.id = jl.account_id
             LEFT JOIN operating_units ou ON ou.id = jl.operating_unit_id
             LEFT JOIN legal_entities cle ON cle.id = jl.counterparty_legal_entity_id
             WHERE jl.journal_entry_id IN (${placeholders})
             ORDER BY jl.journal_entry_id, jl.line_no`,
            journalIds
          );

          const linesByJournalId = new Map();
          for (const line of lineResult.rows || []) {
            const journalEntryId = parsePositiveInt(line.journal_entry_id);
            if (!journalEntryId) continue;
            if (!linesByJournalId.has(journalEntryId)) {
              linesByJournalId.set(journalEntryId, []);
            }
            linesByJournalId.get(journalEntryId).push(line);
          }

          for (const row of rows) {
            row.lines = linesByJournalId.get(parsePositiveInt(row.id)) || [];
          }
        }
      }

      return res.json({
        tenantId,
        rows,
        total,
        limit,
        offset,
      });
    })
  );

  router.get(
    "/journals/:journalId",
    requirePermission("gl.journal.read", {
      resolveScope: async (req, tenantId) => {
        return resolveScopeFromJournalId(req.params?.journalId, tenantId);
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) throw badRequest("tenantId is required");

      const journalId = parsePositiveInt(req.params.journalId);
      if (!journalId) {
        throw badRequest("journalId must be a positive integer");
      }

      const rowResult = await query(
        `SELECT
           je.id, je.tenant_id, je.legal_entity_id, je.book_id, je.fiscal_period_id,
           je.journal_no, je.source_type, je.status, je.entry_date, je.document_date,
           je.currency_code, je.description, je.reference_no,
           je.total_debit_base, je.total_credit_base,
           je.created_by_user_id, je.posted_by_user_id, je.posted_at,
           je.reversed_by_user_id, je.reversed_at, je.reverse_reason,
           je.reversal_journal_entry_id, je.intercompany_source_journal_entry_id, je.created_at, je.updated_at,
           le.code AS legal_entity_code, le.name AS legal_entity_name,
           b.code AS book_code, b.name AS book_name,
           fp.fiscal_year, fp.period_no, fp.period_name
         FROM journal_entries je
         JOIN legal_entities le ON le.id = je.legal_entity_id
         JOIN books b ON b.id = je.book_id
         JOIN fiscal_periods fp ON fp.id = je.fiscal_period_id
         WHERE je.id = ?
           AND je.tenant_id = ?
         LIMIT 1`,
        [journalId, tenantId]
      );
      const journal = rowResult.rows[0];
      if (!journal) {
        throw badRequest("Journal not found");
      }

      assertScopeAccess(req, "legal_entity", journal.legal_entity_id, "journal.legalEntityId");

      const lineResult = await query(
        `SELECT
           jl.id, jl.journal_entry_id, jl.line_no, jl.account_id,
           jl.operating_unit_id, jl.counterparty_legal_entity_id,
           jl.description, jl.subledger_reference_no, jl.currency_code, jl.amount_txn, jl.debit_base,
           jl.credit_base, jl.tax_code, jl.created_at,
           a.code AS account_code, a.name AS account_name,
           ou.code AS operating_unit_code, ou.name AS operating_unit_name,
           cle.code AS counterparty_legal_entity_code,
           cle.name AS counterparty_legal_entity_name
         FROM journal_lines jl
         JOIN accounts a ON a.id = jl.account_id
         LEFT JOIN operating_units ou ON ou.id = jl.operating_unit_id
         LEFT JOIN legal_entities cle ON cle.id = jl.counterparty_legal_entity_id
         WHERE jl.journal_entry_id = ?
         ORDER BY jl.line_no`,
        [journalId]
      );

      return res.json({
        tenantId,
        row: {
          ...journal,
          lines: lineResult.rows || [],
        },
      });
    })
  );
}

export function registerGlReadTrialBalanceRoute(router, deps = {}) {
  const { resolveScopeFromBookId, isNearlyZero } = deps;

  if (typeof resolveScopeFromBookId !== "function") {
    throw new Error("registerGlReadTrialBalanceRoute requires resolveScopeFromBookId");
  }
  if (typeof isNearlyZero !== "function") {
    throw new Error("registerGlReadTrialBalanceRoute requires isNearlyZero");
  }

  router.get(
    "/trial-balance",
    requirePermission("gl.trial_balance.read", {
      resolveScope: async (req, tenantId) => {
        return resolveScopeFromBookId(req.query?.bookId, tenantId);
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) throw badRequest("tenantId is required");

      const bookId = parsePositiveInt(req.query.bookId);
      const fiscalPeriodId = parsePositiveInt(req.query.fiscalPeriodId);
      const includeRollupRaw = req.query.includeRollup;
      const includeRollup =
        includeRollupRaw === undefined || includeRollupRaw === null || includeRollupRaw === ""
          ? true
          : String(includeRollupRaw).toLowerCase() === "true";
      if (!bookId || !fiscalPeriodId) {
        throw badRequest("bookId and fiscalPeriodId query params are required");
      }

      const book = await assertBookBelongsToTenant(tenantId, bookId, "bookId");
      await assertFiscalPeriodBelongsToCalendar(
        parsePositiveInt(book.calendar_id),
        fiscalPeriodId,
        "fiscalPeriodId"
      );

      const result = await query(
        `SELECT
           a.id AS account_id,
           a.code AS account_code,
           a.name AS account_name,
           SUM(jl.debit_base) AS debit_total,
           SUM(jl.credit_base) AS credit_total,
           SUM(jl.debit_base - jl.credit_base) AS balance
         FROM journal_entries je
         JOIN journal_lines jl ON jl.journal_entry_id = je.id
         JOIN accounts a ON a.id = jl.account_id
         WHERE je.tenant_id = ?
           AND je.book_id = ?
           AND je.fiscal_period_id = ?
           AND je.status = 'POSTED'
         GROUP BY a.id, a.code, a.name
         ORDER BY a.code`,
        [tenantId, bookId, fiscalPeriodId]
      );

      const postedRows = (result.rows || []).map((row) => ({
        account_id: parsePositiveInt(row.account_id),
        account_code: row.account_code,
        account_name: row.account_name,
        debit_total: Number(row.debit_total || 0),
        credit_total: Number(row.credit_total || 0),
        balance: Number(row.balance || 0),
        is_rollup: false,
        direct_debit_total: Number(row.debit_total || 0),
        direct_credit_total: Number(row.credit_total || 0),
        direct_balance: Number(row.balance || 0),
      }));

      const summary = postedRows.reduce(
        (acc, row) => {
          acc.debitTotal += Number(row.debit_total || 0);
          acc.creditTotal += Number(row.credit_total || 0);
          acc.balanceTotal += Number(row.balance || 0);
          return acc;
        },
        { debitTotal: 0, creditTotal: 0, balanceTotal: 0 }
      );

      if (!includeRollup) {
        return res.json({
          bookId,
          fiscalPeriodId,
          includeRollup,
          summary,
          rows: postedRows,
        });
      }

      const bookLegalEntityId = parsePositiveInt(book.legal_entity_id);
      const hierarchyParams = [tenantId];
      const hierarchyConditions = ["c.tenant_id = ?"];
      if (bookLegalEntityId) {
        hierarchyConditions.push("(c.legal_entity_id IS NULL OR c.legal_entity_id = ?)");
        hierarchyParams.push(bookLegalEntityId);
      }

      const hierarchyResult = await query(
        `SELECT
           a.id,
           a.parent_account_id,
           a.code,
           a.name
         FROM accounts a
         JOIN charts_of_accounts c ON c.id = a.coa_id
         WHERE ${hierarchyConditions.join(" AND ")}`,
        hierarchyParams
      );

      const accountById = new Map();
      for (const row of hierarchyResult.rows || []) {
        const accountId = parsePositiveInt(row.id);
        if (!accountId) {
          continue;
        }
        accountById.set(accountId, {
          id: accountId,
          parentAccountId: parsePositiveInt(row.parent_account_id),
          code: String(row.code || `ACC-${accountId}`),
          name: String(row.name || `Account ${accountId}`),
        });
      }

      const aggregateByAccountId = new Map();
      for (const row of postedRows) {
        const accountId = parsePositiveInt(row.account_id);
        if (!accountId) {
          continue;
        }

        if (!accountById.has(accountId)) {
          accountById.set(accountId, {
            id: accountId,
            parentAccountId: null,
            code: String(row.account_code || `ACC-${accountId}`),
            name: String(row.account_name || `Account ${accountId}`),
          });
        }

        const current = aggregateByAccountId.get(accountId) || {
          debitTotal: 0,
          creditTotal: 0,
          balance: 0,
          directDebitTotal: 0,
          directCreditTotal: 0,
          directBalance: 0,
        };
        current.debitTotal += Number(row.debit_total || 0);
        current.creditTotal += Number(row.credit_total || 0);
        current.balance += Number(row.balance || 0);
        current.directDebitTotal += Number(row.debit_total || 0);
        current.directCreditTotal += Number(row.credit_total || 0);
        current.directBalance += Number(row.balance || 0);
        aggregateByAccountId.set(accountId, current);

        let parentAccountId = parsePositiveInt(accountById.get(accountId)?.parentAccountId);
        const visited = new Set([accountId]);
        while (parentAccountId && !visited.has(parentAccountId)) {
          visited.add(parentAccountId);
          const parentCurrent = aggregateByAccountId.get(parentAccountId) || {
            debitTotal: 0,
            creditTotal: 0,
            balance: 0,
            directDebitTotal: 0,
            directCreditTotal: 0,
            directBalance: 0,
          };
          parentCurrent.debitTotal += Number(row.debit_total || 0);
          parentCurrent.creditTotal += Number(row.credit_total || 0);
          parentCurrent.balance += Number(row.balance || 0);
          aggregateByAccountId.set(parentAccountId, parentCurrent);

          const parentAccount = accountById.get(parentAccountId);
          if (!parentAccount) {
            break;
          }
          parentAccountId = parsePositiveInt(parentAccount.parentAccountId);
        }
      }

      const rows = [];
      for (const [accountId, totals] of aggregateByAccountId.entries()) {
        const debitTotal = Number(totals.debitTotal || 0);
        const creditTotal = Number(totals.creditTotal || 0);
        const balance = Number(totals.balance || 0);
        if (isNearlyZero(debitTotal) && isNearlyZero(creditTotal) && isNearlyZero(balance)) {
          continue;
        }

        const account = accountById.get(accountId);
        rows.push({
          account_id: accountId,
          account_code: account?.code || `ACC-${accountId}`,
          account_name: account?.name || `Account ${accountId}`,
          debit_total: debitTotal,
          credit_total: creditTotal,
          balance,
          is_rollup:
            isNearlyZero(Number(totals.directDebitTotal || 0)) &&
            isNearlyZero(Number(totals.directCreditTotal || 0)),
          direct_debit_total: Number(totals.directDebitTotal || 0),
          direct_credit_total: Number(totals.directCreditTotal || 0),
          direct_balance: Number(totals.directBalance || 0),
        });
      }

      rows.sort((a, b) => {
        const codeCompare = String(a.account_code || "").localeCompare(
          String(b.account_code || "")
        );
        if (codeCompare !== 0) {
          return codeCompare;
        }
        return Number(a.account_id) - Number(b.account_id);
      });

      return res.json({
        bookId,
        fiscalPeriodId,
        includeRollup,
        summary,
        rows,
      });
    })
  );
}
