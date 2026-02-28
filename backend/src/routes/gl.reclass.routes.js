import { query, withTransaction } from "../db.js";
import {
  assertScopeAccess,
  buildScopeFilter,
  requirePermission,
} from "../middleware/rbac.js";
import {
  assertAccountBelongsToTenant,
  assertBookBelongsToTenant,
  assertFiscalPeriodBelongsToCalendar,
  assertLegalEntityBelongsToTenant,
} from "../tenantGuards.js";
import {
  asyncHandler,
  assertRequiredFields,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";

export function registerGlReclassificationRoutes(router, deps = {}) {
  const {
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
  } = deps;

  if (typeof assertPostableLeafAccountForLegalEntity !== "function") {
    throw new Error("registerGlReclassificationRoutes requires assertPostableLeafAccountForLegalEntity");
  }
  if (typeof buildSystemJournalNo !== "function") {
    throw new Error("registerGlReclassificationRoutes requires buildSystemJournalNo");
  }
  if (typeof ensurePeriodOpen !== "function") {
    throw new Error("registerGlReclassificationRoutes requires ensurePeriodOpen");
  }
  if (typeof isNearlyZero !== "function") {
    throw new Error("registerGlReclassificationRoutes requires isNearlyZero");
  }
  if (typeof normalizeReclassAllocationMode !== "function") {
    throw new Error("registerGlReclassificationRoutes requires normalizeReclassAllocationMode");
  }
  if (typeof parseJsonColumn !== "function") {
    throw new Error("registerGlReclassificationRoutes requires parseJsonColumn");
  }
  if (typeof resolveScopeFromBookId !== "function") {
    throw new Error("registerGlReclassificationRoutes requires resolveScopeFromBookId");
  }
  if (typeof roundAmount !== "function") {
    throw new Error("registerGlReclassificationRoutes requires roundAmount");
  }
  if (typeof toBoolean !== "function") {
    throw new Error("registerGlReclassificationRoutes requires toBoolean");
  }
  if (typeof toIsoDate !== "function") {
    throw new Error("registerGlReclassificationRoutes requires toIsoDate");
  }
  if (typeof toOptionalIsoDate !== "function") {
    throw new Error("registerGlReclassificationRoutes requires toOptionalIsoDate");
  }

  router.post(
    "/reclassifications/balance-split",
    requirePermission("gl.journal.create", {
      resolveScope: (req, tenantId) => {
        const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
        return legalEntityId
          ? { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId }
          : { scopeType: "TENANT", scopeId: tenantId };
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) {
        throw badRequest("tenantId is required");
      }

      assertRequiredFields(req.body, [
        "legalEntityId",
        "bookId",
        "fiscalPeriodId",
        "sourceAccountId",
        "entryDate",
        "documentDate",
        "targets",
      ]);

      const legalEntityId = parsePositiveInt(req.body.legalEntityId);
      const bookId = parsePositiveInt(req.body.bookId);
      const fiscalPeriodId = parsePositiveInt(req.body.fiscalPeriodId);
      const sourceAccountId = parsePositiveInt(req.body.sourceAccountId);
      if (!legalEntityId || !bookId || !fiscalPeriodId || !sourceAccountId) {
        throw badRequest(
          "legalEntityId, bookId, fiscalPeriodId, and sourceAccountId must be positive integers"
        );
      }

      await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
      assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");

      const book = await assertBookBelongsToTenant(tenantId, bookId, "bookId");
      if (parsePositiveInt(book.legal_entity_id) !== legalEntityId) {
        throw badRequest("bookId must belong to selected legalEntityId");
      }
      await assertFiscalPeriodBelongsToCalendar(
        parsePositiveInt(book.calendar_id),
        fiscalPeriodId,
        "fiscalPeriodId"
      );

      const sourceAccount = await assertAccountBelongsToTenant(
        tenantId,
        sourceAccountId,
        "sourceAccountId"
      );
      if (parsePositiveInt(sourceAccount.legal_entity_id) !== legalEntityId) {
        throw badRequest("sourceAccountId must belong to selected legalEntityId");
      }

      const userId = parsePositiveInt(req.user?.userId);
      if (!userId) {
        throw badRequest("Authenticated user is required");
      }

      const allocationMode = normalizeReclassAllocationMode(req.body.allocationMode);
      const rawTargets = Array.isArray(req.body.targets) ? req.body.targets : [];
      if (rawTargets.length === 0) {
        throw badRequest("At least one target allocation line is required");
      }

      const parsedTargets = [];
      const seenTargetAccountIds = new Set();
      for (let index = 0; index < rawTargets.length; index += 1) {
        const target = rawTargets[index] || {};
        const targetAccountId = parsePositiveInt(target.accountId);
        if (!targetAccountId) {
          throw badRequest(`targets[${index}].accountId must be a positive integer`);
        }
        if (targetAccountId === sourceAccountId) {
          throw badRequest(`targets[${index}].accountId cannot be same as sourceAccountId`);
        }
        if (seenTargetAccountIds.has(targetAccountId)) {
          throw badRequest(`targets[${index}].accountId is duplicated`);
        }
        seenTargetAccountIds.add(targetAccountId);

        if (allocationMode === "PERCENT") {
          const allocationPct = roundAmount(
            target.percentage ?? target.allocationPct ?? target.pct
          );
          if (!Number.isFinite(allocationPct) || allocationPct <= 0) {
            throw badRequest(`targets[${index}].percentage must be greater than 0`);
          }
          parsedTargets.push({
            index,
            targetAccountId,
            allocationPct,
            allocationAmount: null,
          });
        } else {
          const allocationAmount = roundAmount(
            target.amount ?? target.allocationAmount
          );
          if (!Number.isFinite(allocationAmount) || allocationAmount <= 0) {
            throw badRequest(`targets[${index}].amount must be greater than 0`);
          }
          parsedTargets.push({
            index,
            targetAccountId,
            allocationPct: null,
            allocationAmount,
          });
        }
      }

      if (allocationMode === "PERCENT") {
        const totalPct = roundAmount(
          parsedTargets.reduce((sum, row) => sum + Number(row.allocationPct || 0), 0)
        );
        if (Math.abs(totalPct - 100) > 0.0001) {
          throw badRequest("targets percentage total must be exactly 100");
        }
      }

      const entryDate = toIsoDate(req.body.entryDate, "entryDate");
      const documentDate = toIsoDate(req.body.documentDate, "documentDate");
      const currencyCode = String(
        req.body.currencyCode || book.base_currency_code || "USD"
      )
        .trim()
        .toUpperCase();
      if (!currencyCode || currencyCode.length !== 3) {
        throw badRequest("currencyCode must be a 3-letter ISO code");
      }

      const requestedJournalNo = req.body.journalNo
        ? String(req.body.journalNo).trim()
        : "";
      const journalNo = requestedJournalNo
        ? requestedJournalNo.slice(0, 40)
        : buildSystemJournalNo("RCLS", legalEntityId);
      const description = req.body.description ? String(req.body.description).slice(0, 500) : null;
      const referenceNo = req.body.referenceNo ? String(req.body.referenceNo).slice(0, 100) : null;
      const note = req.body.note ? String(req.body.note).slice(0, 500) : null;

      const result = await withTransaction(async (tx) => {
        await ensurePeriodOpen(
          bookId,
          fiscalPeriodId,
          "create reclassification draft journal",
          tx.query.bind(tx)
        );

        const sourceInfoResult = await tx.query(
          `SELECT
             a.id,
             a.code,
             a.name,
             c.legal_entity_id
           FROM accounts a
           JOIN charts_of_accounts c ON c.id = a.coa_id
           WHERE a.id = ?
             AND c.tenant_id = ?
           LIMIT 1`,
          [sourceAccountId, tenantId]
        );
        const sourceInfo = sourceInfoResult.rows[0];
        if (!sourceInfo) {
          throw badRequest("sourceAccountId not found for tenant");
        }
        if (parsePositiveInt(sourceInfo.legal_entity_id) !== legalEntityId) {
          throw badRequest("sourceAccountId must belong to selected legalEntityId");
        }

        for (let i = 0; i < parsedTargets.length; i += 1) {
          const row = parsedTargets[i];
          // eslint-disable-next-line no-await-in-loop
          await assertPostableLeafAccountForLegalEntity(
            tenantId,
            legalEntityId,
            row.targetAccountId,
            `targets[${i}].accountId`,
            tx.query.bind(tx)
          );
        }

        const targetAccountIds = parsedTargets.map((row) => row.targetAccountId);
        const targetPlaceholders = targetAccountIds.map(() => "?").join(", ");
        const targetInfoResult = await tx.query(
          `SELECT a.id, a.code, a.name
           FROM accounts a
           JOIN charts_of_accounts c ON c.id = a.coa_id
           WHERE c.tenant_id = ?
             AND c.legal_entity_id = ?
             AND a.id IN (${targetPlaceholders})`,
          [tenantId, legalEntityId, ...targetAccountIds]
        );
        const targetById = new Map(
          (targetInfoResult.rows || []).map((row) => [parsePositiveInt(row.id), row])
        );
        if (targetById.size !== targetAccountIds.length) {
          throw badRequest("One or more target account ids are invalid for selected legalEntityId");
        }

        const sourceBalanceResult = await tx.query(
          `SELECT
             COALESCE(SUM(jl.debit_base - jl.credit_base), 0) AS balance,
             COALESCE(SUM(jl.debit_base), 0) AS debit_total,
             COALESCE(SUM(jl.credit_base), 0) AS credit_total
           FROM journal_entries je
           JOIN journal_lines jl ON jl.journal_entry_id = je.id
           WHERE je.tenant_id = ?
             AND je.book_id = ?
             AND je.fiscal_period_id = ?
             AND je.status = 'POSTED'
             AND jl.account_id = ?`,
          [tenantId, bookId, fiscalPeriodId, sourceAccountId]
        );
        const sourceBalance = roundAmount(sourceBalanceResult.rows[0]?.balance);
        const sourceDebitTotal = roundAmount(sourceBalanceResult.rows[0]?.debit_total);
        const sourceCreditTotal = roundAmount(sourceBalanceResult.rows[0]?.credit_total);

        if (isNearlyZero(sourceBalance)) {
          throw badRequest(
            "sourceAccountId has zero posted balance in selected book/fiscal period"
          );
        }

        const sourceBalanceSide = sourceBalance > 0 ? "DEBIT" : "CREDIT";
        const reclassAmount = roundAmount(Math.abs(sourceBalance));
        if (reclassAmount <= 0) {
          throw badRequest("Calculated reclassification amount must be greater than 0");
        }

        const appliedTargets = [];
        if (allocationMode === "PERCENT") {
          let runningAmount = 0;
          for (let i = 0; i < parsedTargets.length; i += 1) {
            const target = parsedTargets[i];
            let appliedAmount;
            if (i === parsedTargets.length - 1) {
              appliedAmount = roundAmount(reclassAmount - runningAmount);
            } else {
              appliedAmount = roundAmount((reclassAmount * target.allocationPct) / 100);
              runningAmount = roundAmount(runningAmount + appliedAmount);
            }

            if (appliedAmount <= 0) {
              throw badRequest(
                `targets[${target.index}].percentage produced zero/negative allocated amount`
              );
            }

            if (i === parsedTargets.length - 1) {
              runningAmount = roundAmount(runningAmount + appliedAmount);
            }

            appliedTargets.push({
              ...target,
              appliedAmount,
            });
          }

          if (Math.abs(runningAmount - reclassAmount) > 0.0001) {
            throw badRequest("Failed to allocate full reclassification amount by percentage");
          }
        } else {
          const baseAmountTotal = roundAmount(
            parsedTargets.reduce((sum, row) => sum + Number(row.allocationAmount || 0), 0)
          );
          const difference = roundAmount(reclassAmount - baseAmountTotal);
          if (Math.abs(difference) > 0.01) {
            throw badRequest(
              "targets amount total must match source balance amount (max difference 0.01)"
            );
          }

          let runningAmount = 0;
          for (let i = 0; i < parsedTargets.length; i += 1) {
            const target = parsedTargets[i];
            let appliedAmount = roundAmount(target.allocationAmount);
            if (i === parsedTargets.length - 1) {
              appliedAmount = roundAmount(appliedAmount + difference);
            }
            if (appliedAmount <= 0) {
              throw badRequest(`targets[${target.index}].amount produced zero/negative value`);
            }
            runningAmount = roundAmount(runningAmount + appliedAmount);
            appliedTargets.push({
              ...target,
              appliedAmount,
            });
          }

          if (Math.abs(runningAmount - reclassAmount) > 0.0001) {
            throw badRequest("Failed to allocate full reclassification amount by amount mode");
          }
        }

        const totalDebit = reclassAmount;
        const totalCredit = reclassAmount;
        const journalDescription =
          description ||
          `Balance reclassification: ${String(sourceInfo.code || sourceAccountId)}`.slice(0, 500);
        const journalReferenceNo =
          referenceNo ||
          `RECLASS:${sourceAccountId}:${fiscalPeriodId}:${Date.now()}`.slice(0, 100);

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
              created_by_user_id
            )
            VALUES (?, ?, ?, ?, ?, 'ADJUSTMENT', 'DRAFT', ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            tenantId,
            legalEntityId,
            bookId,
            fiscalPeriodId,
            journalNo,
            entryDate,
            documentDate,
            currencyCode,
            journalDescription,
            journalReferenceNo,
            totalDebit,
            totalCredit,
            userId,
          ]
        );
        const journalEntryId = parsePositiveInt(journalResult.rows.insertId);
        if (!journalEntryId) {
          throw badRequest("Failed to create reclassification draft journal");
        }

        const sourceLineDebit = sourceBalance < 0 ? reclassAmount : 0;
        const sourceLineCredit = sourceBalance > 0 ? reclassAmount : 0;
        const sourceLineAmountTxn = sourceLineDebit > 0 ? sourceLineDebit : sourceLineCredit * -1;
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
            1,
            sourceAccountId,
            `Reclass source: ${String(sourceInfo.code || sourceAccountId)}`.slice(0, 500),
            currencyCode,
            sourceLineAmountTxn,
            sourceLineDebit,
            sourceLineCredit,
          ]
        );

        for (let i = 0; i < appliedTargets.length; i += 1) {
          const target = appliedTargets[i];
          const targetInfo = targetById.get(target.targetAccountId);
          const targetDebit = sourceBalance > 0 ? target.appliedAmount : 0;
          const targetCredit = sourceBalance < 0 ? target.appliedAmount : 0;
          const targetAmountTxn = targetDebit > 0 ? targetDebit : targetCredit * -1;

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
              i + 2,
              target.targetAccountId,
              `Reclass target: ${String(targetInfo?.code || target.targetAccountId)}`.slice(
                0,
                500
              ),
              currencyCode,
              targetAmountTxn,
              targetDebit,
              targetCredit,
            ]
          );
        }

        const runResult = await tx.query(
          `INSERT INTO gl_reclassification_runs (
              tenant_id,
              legal_entity_id,
              book_id,
              fiscal_period_id,
              source_account_id,
              source_balance,
              source_balance_side,
              reclass_amount,
              allocation_mode,
              entry_date,
              document_date,
              currency_code,
              journal_entry_id,
              status,
              description,
              reference_no,
              note,
              metadata_json,
              created_by_user_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'DRAFT_CREATED', ?, ?, ?, ?, ?)`,
          [
            tenantId,
            legalEntityId,
            bookId,
            fiscalPeriodId,
            sourceAccountId,
            sourceBalance,
            sourceBalanceSide,
            reclassAmount,
            allocationMode,
            entryDate,
            documentDate,
            currencyCode,
            journalEntryId,
            journalDescription,
            journalReferenceNo,
            note,
            JSON.stringify({
              sourceDebitTotal,
              sourceCreditTotal,
              sourceAccountCode: String(sourceInfo.code || ""),
              sourceAccountName: String(sourceInfo.name || ""),
              targetCount: appliedTargets.length,
            }),
            userId,
          ]
        );
        const runId = parsePositiveInt(runResult.rows.insertId);
        if (!runId) {
          throw badRequest("Failed to persist reclassification run");
        }

        for (const target of appliedTargets) {
          const targetDebit = sourceBalance > 0 ? target.appliedAmount : 0;
          const targetCredit = sourceBalance < 0 ? target.appliedAmount : 0;

          // eslint-disable-next-line no-await-in-loop
          await tx.query(
            `INSERT INTO gl_reclassification_run_targets (
                reclassification_run_id,
                tenant_id,
                target_account_id,
                allocation_pct,
                allocation_amount,
                applied_amount,
                debit_base,
                credit_base
              )
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              runId,
              tenantId,
              target.targetAccountId,
              target.allocationPct,
              target.allocationAmount,
              target.appliedAmount,
              targetDebit,
              targetCredit,
            ]
          );
        }

        return {
          runId,
          journalEntryId,
          journalNo,
          sourceBalance,
          sourceBalanceSide,
          reclassAmount,
          entryDate,
          documentDate,
          currencyCode,
          sourceAccount: {
            id: sourceAccountId,
            code: String(sourceInfo.code || ""),
            name: String(sourceInfo.name || ""),
          },
          targets: appliedTargets.map((target) => {
            const targetInfo = targetById.get(target.targetAccountId);
            return {
              accountId: target.targetAccountId,
              accountCode: String(targetInfo?.code || ""),
              accountName: String(targetInfo?.name || ""),
              allocationPct: target.allocationPct,
              allocationAmount: target.allocationAmount,
              appliedAmount: target.appliedAmount,
              debitBase: sourceBalance > 0 ? target.appliedAmount : 0,
              creditBase: sourceBalance < 0 ? target.appliedAmount : 0,
            };
          }),
        };
      });

      return res.status(201).json({
        ok: true,
        ...result,
      });
    })
  );

  router.get(
    "/reclassifications/source-lines",
    requirePermission("gl.journal.read", {
      resolveScope: async (req, tenantId) => {
        const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
        if (legalEntityId) {
          return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
        }
        return resolveScopeFromBookId(req.query?.bookId, tenantId);
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) {
        throw badRequest("tenantId is required");
      }

      const legalEntityId = parsePositiveInt(req.query.legalEntityId);
      const bookId = parsePositiveInt(req.query.bookId);
      const sourceAccountId = parsePositiveInt(req.query.sourceAccountId);
      if (!legalEntityId || !bookId || !sourceAccountId) {
        throw badRequest(
          "legalEntityId, bookId, and sourceAccountId query params are required"
        );
      }

      const dateFrom = toOptionalIsoDate(req.query.dateFrom, "dateFrom");
      const dateTo = toOptionalIsoDate(req.query.dateTo, "dateTo");
      if (dateFrom && dateTo && dateFrom > dateTo) {
        throw badRequest("dateFrom must be earlier than or equal to dateTo");
      }

      const limitRaw = req.query.limit;
      const parsedLimit = limitRaw ? parsePositiveInt(limitRaw) : null;
      if (limitRaw !== undefined && limitRaw !== null && limitRaw !== "" && !parsedLimit) {
        throw badRequest("limit must be a positive integer");
      }
      const limit = Math.min(parsedLimit || 300, 1000);

      await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
      assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");

      const book = await assertBookBelongsToTenant(tenantId, bookId, "bookId");
      if (parsePositiveInt(book.legal_entity_id) !== legalEntityId) {
        throw badRequest("bookId must belong to selected legalEntityId");
      }

      const sourceAccount = await assertAccountBelongsToTenant(
        tenantId,
        sourceAccountId,
        "sourceAccountId"
      );
      if (parsePositiveInt(sourceAccount.legal_entity_id) !== legalEntityId) {
        throw badRequest("sourceAccountId must belong to selected legalEntityId");
      }

      const params = [tenantId, legalEntityId, bookId, sourceAccountId];
      const conditions = [
        "je.tenant_id = ?",
        "je.legal_entity_id = ?",
        "je.book_id = ?",
        "je.status = 'POSTED'",
        "jl.account_id = ?",
      ];
      if (dateFrom) {
        conditions.push("je.entry_date >= ?");
        params.push(dateFrom);
      }
      if (dateTo) {
        conditions.push("je.entry_date <= ?");
        params.push(dateTo);
      }
      const result = await query(
        `SELECT
           jl.id AS journal_line_id,
           jl.journal_entry_id,
           jl.debit_base,
           jl.credit_base,
           jl.amount_txn,
           jl.description,
           jl.operating_unit_id,
           jl.counterparty_legal_entity_id,
           jl.subledger_reference_no,
           jl.tax_code,
           jl.currency_code,
           je.journal_no,
           je.entry_date,
           je.document_date,
           je.source_type
         FROM journal_lines jl
         JOIN journal_entries je ON je.id = jl.journal_entry_id
         WHERE ${conditions.join(" AND ")}
         ORDER BY je.entry_date ASC, jl.id ASC
         LIMIT ${limit}`,
        params
      );

      const rows = (result.rows || []).map((row) => {
        const debitBase = Number(row.debit_base || 0);
        const creditBase = Number(row.credit_base || 0);
        return {
          journalLineId: parsePositiveInt(row.journal_line_id),
          journalEntryId: parsePositiveInt(row.journal_entry_id),
          journalNo: row.journal_no || null,
          entryDate: row.entry_date || null,
          documentDate: row.document_date || null,
          sourceType: row.source_type || null,
          debitBase,
          creditBase,
          amountBase: roundAmount(Math.abs(debitBase || creditBase)),
          amountTxn: Number(row.amount_txn || 0),
          currencyCode: row.currency_code || null,
          description: row.description || null,
          operatingUnitId: parsePositiveInt(row.operating_unit_id),
          counterpartyLegalEntityId: parsePositiveInt(row.counterparty_legal_entity_id),
          subledgerReferenceNo: row.subledger_reference_no || null,
          taxCode: row.tax_code || null,
        };
      });

      return res.json({
        tenantId,
        legalEntityId,
        bookId,
        sourceAccountId,
        dateFrom,
        dateTo,
        rows,
      });
    })
  );

  router.post(
    "/reclassifications/transaction-lines",
    requirePermission("gl.journal.create", {
      resolveScope: (req, tenantId) => {
        const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
        return legalEntityId
          ? { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId }
          : { scopeType: "TENANT", scopeId: tenantId };
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) {
        throw badRequest("tenantId is required");
      }

      assertRequiredFields(req.body, [
        "legalEntityId",
        "bookId",
        "fiscalPeriodId",
        "sourceAccountId",
        "entryDate",
        "documentDate",
        "currencyCode",
        "lineMappings",
      ]);

      const legalEntityId = parsePositiveInt(req.body.legalEntityId);
      const bookId = parsePositiveInt(req.body.bookId);
      const fiscalPeriodId = parsePositiveInt(req.body.fiscalPeriodId);
      const sourceAccountId = parsePositiveInt(req.body.sourceAccountId);
      if (!legalEntityId || !bookId || !fiscalPeriodId || !sourceAccountId) {
        throw badRequest(
          "legalEntityId, bookId, fiscalPeriodId, and sourceAccountId must be positive integers"
        );
      }

      const dateFrom = toOptionalIsoDate(req.body.dateFrom, "dateFrom");
      const dateTo = toOptionalIsoDate(req.body.dateTo, "dateTo");
      if (dateFrom && dateTo && dateFrom > dateTo) {
        throw badRequest("dateFrom must be earlier than or equal to dateTo");
      }

      const rawLineMappings = Array.isArray(req.body.lineMappings)
        ? req.body.lineMappings
        : [];
      if (rawLineMappings.length === 0) {
        throw badRequest("lineMappings must contain at least one selected source line");
      }
      if (rawLineMappings.length > 1000) {
        throw badRequest("lineMappings exceeds maximum allowed size (1000)");
      }

      await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
      assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");

      const book = await assertBookBelongsToTenant(tenantId, bookId, "bookId");
      if (parsePositiveInt(book.legal_entity_id) !== legalEntityId) {
        throw badRequest("bookId must belong to selected legalEntityId");
      }
      await assertFiscalPeriodBelongsToCalendar(
        parsePositiveInt(book.calendar_id),
        fiscalPeriodId,
        "fiscalPeriodId"
      );

      const sourceAccount = await assertAccountBelongsToTenant(
        tenantId,
        sourceAccountId,
        "sourceAccountId"
      );
      if (parsePositiveInt(sourceAccount.legal_entity_id) !== legalEntityId) {
        throw badRequest("sourceAccountId must belong to selected legalEntityId");
      }

      const lineMappings = [];
      const seenLineIds = new Set();
      const targetAccountIds = new Set();
      for (let i = 0; i < rawLineMappings.length; i += 1) {
        const row = rawLineMappings[i] || {};
        const journalLineId = parsePositiveInt(
          row.journalLineId || row.sourceJournalLineId || row.lineId
        );
        const targetAccountId = parsePositiveInt(row.targetAccountId || row.accountId);
        if (!journalLineId || !targetAccountId) {
          throw badRequest(
            `lineMappings[${i}] must include positive journalLineId and targetAccountId`
          );
        }
        if (seenLineIds.has(journalLineId)) {
          throw badRequest(`lineMappings[${i}] contains duplicated journalLineId`);
        }
        seenLineIds.add(journalLineId);
        if (targetAccountId === sourceAccountId) {
          throw badRequest(`lineMappings[${i}] targetAccountId cannot equal sourceAccountId`);
        }
        targetAccountIds.add(targetAccountId);
        lineMappings.push({
          journalLineId,
          targetAccountId,
        });
      }

      for (const targetAccountId of targetAccountIds) {
        // eslint-disable-next-line no-await-in-loop
        await assertPostableLeafAccountForLegalEntity(
          tenantId,
          legalEntityId,
          targetAccountId,
          "targetAccountId"
        );
      }

      const userId = parsePositiveInt(req.user?.userId);
      if (!userId) {
        throw badRequest("Authenticated user is required");
      }

      const entryDate = toIsoDate(req.body.entryDate, "entryDate");
      const documentDate = toIsoDate(req.body.documentDate, "documentDate");
      const currencyCode = String(req.body.currencyCode || "USD").trim().toUpperCase();
      if (currencyCode.length !== 3) {
        throw badRequest("currencyCode must be a 3-letter ISO code");
      }
      const journalNo = req.body.journalNo
        ? String(req.body.journalNo).trim().slice(0, 40)
        : buildSystemJournalNo("RCLST", legalEntityId);
      const description =
        req.body.description && String(req.body.description).trim()
          ? String(req.body.description).slice(0, 500)
          : `Transaction reclassification: ${String(sourceAccount.code || sourceAccountId)}`.slice(
              0,
              500
            );
      const referenceNo =
        req.body.referenceNo && String(req.body.referenceNo).trim()
          ? String(req.body.referenceNo).slice(0, 100)
          : `RECLASS_TXN:${sourceAccountId}:${Date.now()}`.slice(0, 100);
      const note = req.body.note ? String(req.body.note).slice(0, 500) : null;

      const result = await withTransaction(async (tx) => {
        await ensurePeriodOpen(
          bookId,
          fiscalPeriodId,
          "create transaction reclassification draft journal",
          tx.query.bind(tx)
        );

        const lineIds = lineMappings.map((row) => row.journalLineId);
        const linePlaceholders = lineIds.map(() => "?").join(",");
        const selectedLinesParams = [
          tenantId,
          legalEntityId,
          bookId,
          sourceAccountId,
          ...lineIds,
        ];
        let dateClause = "";
        if (dateFrom) {
          dateClause += " AND je.entry_date >= ?";
          selectedLinesParams.push(dateFrom);
        }
        if (dateTo) {
          dateClause += " AND je.entry_date <= ?";
          selectedLinesParams.push(dateTo);
        }

        const selectedLinesResult = await tx.query(
          `SELECT
             jl.id,
             jl.journal_entry_id,
             jl.operating_unit_id,
             jl.counterparty_legal_entity_id,
             jl.description,
             jl.subledger_reference_no,
             jl.currency_code,
             jl.amount_txn,
             jl.debit_base,
             jl.credit_base,
             jl.tax_code,
             je.journal_no,
             je.entry_date
           FROM journal_lines jl
           JOIN journal_entries je ON je.id = jl.journal_entry_id
           WHERE je.tenant_id = ?
             AND je.legal_entity_id = ?
             AND je.book_id = ?
             AND je.status = 'POSTED'
             AND jl.account_id = ?
             AND jl.id IN (${linePlaceholders})
             ${dateClause}`,
          selectedLinesParams
        );

        const lineById = new Map(
          (selectedLinesResult.rows || []).map((row) => [parsePositiveInt(row.id), row])
        );
        if (lineById.size !== lineMappings.length) {
          throw badRequest(
            "One or more selected source lines are invalid or out of scope for selected filters"
          );
        }

        const targetInfoResult = await tx.query(
          `SELECT a.id, a.code, a.name
           FROM accounts a
           JOIN charts_of_accounts c ON c.id = a.coa_id
           WHERE c.tenant_id = ?
             AND c.legal_entity_id = ?
             AND a.id IN (${Array.from(targetAccountIds).map(() => "?").join(",")})`,
          [tenantId, legalEntityId, ...Array.from(targetAccountIds)]
        );
        const targetById = new Map(
          (targetInfoResult.rows || []).map((row) => [parsePositiveInt(row.id), row])
        );
        if (targetById.size !== targetAccountIds.size) {
          throw badRequest("One or more target accounts are invalid for selected legal entity");
        }

        let totalDebit = 0;
        let totalCredit = 0;
        let sourceNetBalance = 0;
        const plannedLines = [];
        for (const mapping of lineMappings) {
          const sourceLine = lineById.get(mapping.journalLineId);
          const sourceDebit = roundAmount(sourceLine.debit_base);
          const sourceCredit = roundAmount(sourceLine.credit_base);
          const amount = roundAmount(Math.abs(sourceDebit || sourceCredit));
          if (amount <= 0) {
            throw badRequest(`Selected source line ${mapping.journalLineId} has zero amount`);
          }

          sourceNetBalance = roundAmount(sourceNetBalance + (sourceDebit - sourceCredit));
          totalDebit = roundAmount(totalDebit + amount);
          totalCredit = roundAmount(totalCredit + amount);

          if (sourceDebit > 0) {
            plannedLines.push({
              source: {
                accountId: sourceAccountId,
                debitBase: 0,
                creditBase: amount,
                amountTxn: amount * -1,
              },
              target: {
                accountId: mapping.targetAccountId,
                debitBase: amount,
                creditBase: 0,
                amountTxn: amount,
              },
              sourceLine,
            });
          } else {
            plannedLines.push({
              source: {
                accountId: sourceAccountId,
                debitBase: amount,
                creditBase: 0,
                amountTxn: amount,
              },
              target: {
                accountId: mapping.targetAccountId,
                debitBase: 0,
                creditBase: amount,
                amountTxn: amount * -1,
              },
              sourceLine,
            });
          }
        }

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
              created_by_user_id
            )
            VALUES (?, ?, ?, ?, ?, 'ADJUSTMENT', 'DRAFT', ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            tenantId,
            legalEntityId,
            bookId,
            fiscalPeriodId,
            journalNo,
            entryDate,
            documentDate,
            currencyCode,
            description,
            referenceNo,
            totalDebit,
            totalCredit,
            userId,
          ]
        );
        const journalEntryId = parsePositiveInt(journalResult.rows.insertId);
        if (!journalEntryId) {
          throw badRequest("Failed to create transaction reclassification draft journal");
        }

        const targetAggregationById = new Map();
        let lineNo = 1;
        for (const planned of plannedLines) {
          const sourceLine = planned.sourceLine;
          const sourceDescription = `Reclass source #${sourceLine.id}: ${String(
            sourceLine.description || ""
          )}`.slice(0, 500);
          const targetAccount = targetById.get(planned.target.accountId);
          const targetDescription = `Reclass target ${String(
            targetAccount?.code || planned.target.accountId
          )} from #${sourceLine.id}`.slice(0, 500);

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
              journalEntryId,
              lineNo,
              planned.source.accountId,
              parsePositiveInt(sourceLine.operating_unit_id),
              parsePositiveInt(sourceLine.counterparty_legal_entity_id),
              sourceDescription,
              sourceLine.subledger_reference_no
                ? String(sourceLine.subledger_reference_no).slice(0, 100)
                : null,
              currencyCode,
              planned.source.amountTxn,
              planned.source.debitBase,
              planned.source.creditBase,
              sourceLine.tax_code ? String(sourceLine.tax_code) : null,
            ]
          );
          lineNo += 1;

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
              journalEntryId,
              lineNo,
              planned.target.accountId,
              parsePositiveInt(sourceLine.operating_unit_id),
              parsePositiveInt(sourceLine.counterparty_legal_entity_id),
              targetDescription,
              sourceLine.subledger_reference_no
                ? String(sourceLine.subledger_reference_no).slice(0, 100)
                : null,
              currencyCode,
              planned.target.amountTxn,
              planned.target.debitBase,
              planned.target.creditBase,
              sourceLine.tax_code ? String(sourceLine.tax_code) : null,
            ]
          );
          lineNo += 1;

          const currentAgg = targetAggregationById.get(planned.target.accountId) || {
            targetAccountId: planned.target.accountId,
            appliedAmount: 0,
            debitBase: 0,
            creditBase: 0,
          };
          currentAgg.appliedAmount = roundAmount(
            currentAgg.appliedAmount + Math.abs(planned.target.debitBase || planned.target.creditBase)
          );
          currentAgg.debitBase = roundAmount(currentAgg.debitBase + planned.target.debitBase);
          currentAgg.creditBase = roundAmount(currentAgg.creditBase + planned.target.creditBase);
          targetAggregationById.set(planned.target.accountId, currentAgg);
        }

        const reclassAmount = roundAmount(totalDebit);
        const sourceBalanceSide = sourceNetBalance >= 0 ? "DEBIT" : "CREDIT";
        const runResult = await tx.query(
          `INSERT INTO gl_reclassification_runs (
              tenant_id,
              legal_entity_id,
              book_id,
              fiscal_period_id,
              source_account_id,
              source_balance,
              source_balance_side,
              reclass_amount,
              allocation_mode,
              entry_date,
              document_date,
              currency_code,
              journal_entry_id,
              status,
              description,
              reference_no,
              note,
              metadata_json,
              created_by_user_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'AMOUNT', ?, ?, ?, ?, 'DRAFT_CREATED', ?, ?, ?, ?, ?)`,
          [
            tenantId,
            legalEntityId,
            bookId,
            fiscalPeriodId,
            sourceAccountId,
            sourceNetBalance,
            sourceBalanceSide,
            reclassAmount,
            entryDate,
            documentDate,
            currencyCode,
            journalEntryId,
            description,
            referenceNo,
            note,
            JSON.stringify({
              reclassType: "TRANSACTION_LINES",
              sourceLineCount: plannedLines.length,
              dateFrom,
              dateTo,
            }),
            userId,
          ]
        );
        const runId = parsePositiveInt(runResult.rows.insertId);
        if (!runId) {
          throw badRequest("Failed to persist transaction reclassification run");
        }

        for (const agg of targetAggregationById.values()) {
          // eslint-disable-next-line no-await-in-loop
          await tx.query(
            `INSERT INTO gl_reclassification_run_targets (
                reclassification_run_id,
                tenant_id,
                target_account_id,
                allocation_pct,
                allocation_amount,
                applied_amount,
                debit_base,
                credit_base
              )
              VALUES (?, ?, ?, NULL, ?, ?, ?, ?)`,
            [
              runId,
              tenantId,
              agg.targetAccountId,
              agg.appliedAmount,
              agg.appliedAmount,
              agg.debitBase,
              agg.creditBase,
            ]
          );
        }

        return {
          runId,
          journalEntryId,
          journalNo,
          sourceLineCount: plannedLines.length,
          sourceBalance: sourceNetBalance,
          sourceBalanceSide,
          reclassAmount,
          entryDate,
          documentDate,
          currencyCode,
          targetSummaries: Array.from(targetAggregationById.values()).map((agg) => {
            const targetInfo = targetById.get(agg.targetAccountId);
            return {
              accountId: agg.targetAccountId,
              accountCode: String(targetInfo?.code || ""),
              accountName: String(targetInfo?.name || ""),
              appliedAmount: agg.appliedAmount,
              debitBase: agg.debitBase,
              creditBase: agg.creditBase,
            };
          }),
        };
      });

      return res.status(201).json({
        ok: true,
        ...result,
      });
    })
  );

  router.get(
    "/reclassifications/runs",
    requirePermission("gl.journal.read", {
      resolveScope: async (req, tenantId) => {
        const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
        if (legalEntityId) {
          return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
        }
        return resolveScopeFromBookId(req.query?.bookId, tenantId);
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) {
        throw badRequest("tenantId is required");
      }

      const legalEntityId = parsePositiveInt(req.query.legalEntityId);
      const bookId = parsePositiveInt(req.query.bookId);
      const fiscalPeriodId = parsePositiveInt(req.query.fiscalPeriodId);
      const includeTargets = toBoolean(req.query.includeTargets, false);

      const limitRaw = req.query.limit;
      const parsedLimit = limitRaw ? parsePositiveInt(limitRaw) : null;
      if (limitRaw !== undefined && limitRaw !== null && limitRaw !== "" && !parsedLimit) {
        throw badRequest("limit must be a positive integer");
      }
      const limit = Math.min(parsedLimit || 50, 200);

      if (legalEntityId) {
        await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
        assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
      }

      if (bookId) {
        const book = await assertBookBelongsToTenant(tenantId, bookId, "bookId");
        const bookLegalEntityId = parsePositiveInt(book.legal_entity_id);
        if (bookLegalEntityId) {
          assertScopeAccess(req, "legal_entity", bookLegalEntityId, "bookId");
        }
        if (legalEntityId && bookLegalEntityId !== legalEntityId) {
          throw badRequest("bookId does not belong to selected legalEntityId");
        }
      }

      const params = [tenantId];
      const conditions = ["r.tenant_id = ?"];
      conditions.push(buildScopeFilter(req, "legal_entity", "r.legal_entity_id", params));

      if (legalEntityId) {
        conditions.push("r.legal_entity_id = ?");
        params.push(legalEntityId);
      }
      if (bookId) {
        conditions.push("r.book_id = ?");
        params.push(bookId);
      }
      if (fiscalPeriodId) {
        conditions.push("r.fiscal_period_id = ?");
        params.push(fiscalPeriodId);
      }

      const runResult = await query(
        `SELECT
           r.id,
           r.tenant_id,
           r.legal_entity_id,
           r.book_id,
           r.fiscal_period_id,
           r.source_account_id,
           r.source_balance,
           r.source_balance_side,
           r.reclass_amount,
           r.allocation_mode,
           r.entry_date,
           r.document_date,
           r.currency_code,
           r.journal_entry_id,
           r.status,
           r.description,
           r.reference_no,
           r.note,
           r.metadata_json,
           r.created_by_user_id,
           r.created_at,
           le.code AS legal_entity_code,
           le.name AS legal_entity_name,
           b.code AS book_code,
           b.name AS book_name,
           fp.fiscal_year,
           fp.period_no,
           fp.period_name,
           sa.code AS source_account_code,
           sa.name AS source_account_name,
           je.journal_no
         FROM gl_reclassification_runs r
         JOIN legal_entities le ON le.id = r.legal_entity_id
         JOIN books b ON b.id = r.book_id
         JOIN fiscal_periods fp ON fp.id = r.fiscal_period_id
         JOIN accounts sa ON sa.id = r.source_account_id
         LEFT JOIN journal_entries je ON je.id = r.journal_entry_id
         WHERE ${conditions.join(" AND ")}
         ORDER BY r.id DESC
         LIMIT ${limit}`,
        params
      );

      const rows = (runResult.rows || []).map((row) => ({
        id: parsePositiveInt(row.id),
        tenantId: parsePositiveInt(row.tenant_id),
        legalEntityId: parsePositiveInt(row.legal_entity_id),
        legalEntityCode: row.legal_entity_code || null,
        legalEntityName: row.legal_entity_name || null,
        bookId: parsePositiveInt(row.book_id),
        bookCode: row.book_code || null,
        bookName: row.book_name || null,
        fiscalPeriodId: parsePositiveInt(row.fiscal_period_id),
        fiscalYear: row.fiscal_year === null ? null : Number(row.fiscal_year),
        periodNo: row.period_no === null ? null : Number(row.period_no),
        periodName: row.period_name || null,
        sourceAccountId: parsePositiveInt(row.source_account_id),
        sourceAccountCode: row.source_account_code || null,
        sourceAccountName: row.source_account_name || null,
        sourceBalance: Number(row.source_balance || 0),
        sourceBalanceSide: String(row.source_balance_side || "").toUpperCase(),
        reclassAmount: Number(row.reclass_amount || 0),
        allocationMode: String(row.allocation_mode || "").toUpperCase(),
        entryDate: row.entry_date || null,
        documentDate: row.document_date || null,
        currencyCode: row.currency_code || null,
        journalEntryId: parsePositiveInt(row.journal_entry_id),
        journalNo: row.journal_no || null,
        status: String(row.status || "").toUpperCase(),
        description: row.description || null,
        referenceNo: row.reference_no || null,
        note: row.note || null,
        metadata: parseJsonColumn(row.metadata_json),
        createdByUserId: parsePositiveInt(row.created_by_user_id),
        createdAt: row.created_at || null,
      }));

      if (includeTargets && rows.length > 0) {
        const runIds = rows.map((row) => row.id).filter(Boolean);
        if (runIds.length > 0) {
          const placeholders = runIds.map(() => "?").join(", ");
          const targetResult = await query(
            `SELECT
               t.reclassification_run_id,
               t.target_account_id,
               t.allocation_pct,
               t.allocation_amount,
               t.applied_amount,
               t.debit_base,
               t.credit_base,
               t.created_at,
               a.code AS target_account_code,
               a.name AS target_account_name
             FROM gl_reclassification_run_targets t
             JOIN accounts a ON a.id = t.target_account_id
             WHERE t.reclassification_run_id IN (${placeholders})
             ORDER BY t.reclassification_run_id, a.code`,
            runIds
          );

          const targetsByRunId = new Map();
          for (const target of targetResult.rows || []) {
            const runId = parsePositiveInt(target.reclassification_run_id);
            if (!runId) {
              continue;
            }
            if (!targetsByRunId.has(runId)) {
              targetsByRunId.set(runId, []);
            }
            targetsByRunId.get(runId).push({
              accountId: parsePositiveInt(target.target_account_id),
              accountCode: target.target_account_code || null,
              accountName: target.target_account_name || null,
              allocationPct:
                target.allocation_pct === null ? null : Number(target.allocation_pct),
              allocationAmount:
                target.allocation_amount === null ? null : Number(target.allocation_amount),
              appliedAmount: Number(target.applied_amount || 0),
              debitBase: Number(target.debit_base || 0),
              creditBase: Number(target.credit_base || 0),
              createdAt: target.created_at || null,
            });
          }

          for (const row of rows) {
            row.targets = targetsByRunId.get(row.id) || [];
          }
        }
      }

      return res.json({
        tenantId,
        rows,
      });
    })
  );
}
