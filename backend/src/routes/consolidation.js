import express from "express";
import { query, withTransaction } from "../db.js";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import {
  assertAccountBelongsToTenant,
  assertCoaBelongsToTenant,
  assertConsolidationGroupBelongsToTenant,
  assertFiscalCalendarBelongsToTenant,
  assertFiscalPeriodBelongsToCalendar,
  assertGroupCompanyBelongsToTenant,
  assertLegalEntityBelongsToTenant,
  assertUserBelongsToTenant,
} from "../tenantGuards.js";
import {
  asyncHandler,
  assertRequiredFields,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";

const router = express.Router();

const VALID_FX_RATE_TYPES = new Set(["SPOT", "AVERAGE", "CLOSING"]);
const BALANCE_EPSILON = 0.0001;

function normalizeRateType(value) {
  const rateType = String(value || "CLOSING").toUpperCase();
  if (!VALID_FX_RATE_TYPES.has(rateType)) {
    throw badRequest("rateType must be one of SPOT, AVERAGE, CLOSING");
  }
  return rateType;
}

function parseBooleanLike(value, fallback = false, fieldLabel = "flag") {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  if (typeof value === "boolean") {
    return value;
  }

  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(normalized)) {
    return false;
  }

  throw badRequest(`${fieldLabel} must be true or false`);
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

function normalizeBalanceByAccountType(accountType, balance) {
  const type = String(accountType || "").toUpperCase();
  const amount = Number(balance || 0);
  if (["LIABILITY", "EQUITY", "REVENUE"].includes(type)) {
    return amount * -1;
  }
  return amount;
}

function normalizeDraftPostingStatus(value) {
  const status = String(value || "ALL").toUpperCase();
  if (!["ALL", "DRAFT", "POSTED"].includes(status)) {
    throw badRequest("status must be one of ALL, DRAFT, POSTED");
  }
  return status;
}

function assertRunNotLocked(run) {
  const status = String(run?.status || "").toUpperCase();
  if (status === "LOCKED") {
    throw badRequest("Consolidation run is LOCKED; no further posting is allowed");
  }
}

function ownershipFactor(consolidationMethod, ownershipPct) {
  const normalizedMethod = String(consolidationMethod || "FULL").toUpperCase();
  const pct = Number(ownershipPct);
  const safePct = Number.isFinite(pct) ? Math.max(0, Math.min(pct, 1)) : 1;

  if (normalizedMethod === "FULL") {
    return 1;
  }
  return safePct;
}

async function getRunWithContext(tenantId, runId) {
  const result = await query(
    `SELECT
       cr.id,
       cr.consolidation_group_id,
       cr.fiscal_period_id,
       cr.run_name,
       cr.status,
       cr.presentation_currency_code,
       cr.started_by_user_id,
       cr.started_at,
       cr.finished_at,
       cr.notes,
       cg.tenant_id,
       cg.group_company_id,
       cg.code AS consolidation_group_code,
       cg.name AS consolidation_group_name,
       fp.start_date AS period_start_date,
       fp.end_date AS period_end_date
     FROM consolidation_runs cr
     JOIN consolidation_groups cg ON cg.id = cr.consolidation_group_id
     JOIN fiscal_periods fp ON fp.id = cr.fiscal_period_id
     WHERE cr.id = ?
       AND cg.tenant_id = ?
     LIMIT 1`,
    [runId, tenantId]
  );

  return result.rows[0] || null;
}

async function requireRun(tenantId, runId) {
  const run = await getRunWithContext(tenantId, runId);
  if (!run) {
    throw badRequest("Consolidation run not found");
  }
  return run;
}

async function resolveRunScope(runId, tenantId) {
  const parsedRunId = parsePositiveInt(runId);
  if (!parsedRunId) {
    return { scopeType: "TENANT", scopeId: tenantId };
  }

  const run = await getRunWithContext(tenantId, parsedRunId);
  const groupCompanyId = parsePositiveInt(run?.group_company_id);
  if (groupCompanyId) {
    return { scopeType: "GROUP", scopeId: groupCompanyId };
  }

  return { scopeType: "TENANT", scopeId: tenantId };
}

async function resolveFxRate({
  tenantId,
  rateDate,
  fromCurrencyCode,
  toCurrencyCode,
  preferredRateType,
  runQuery = query,
}) {
  const fromCode = String(fromCurrencyCode || "").toUpperCase();
  const toCode = String(toCurrencyCode || "").toUpperCase();

  if (!fromCode || !toCode) {
    throw badRequest("Currency codes are required for FX translation");
  }

  if (fromCode === toCode) {
    return {
      rate: 1,
      rateType: "IDENTITY",
      rateDate,
    };
  }

  const fallbackOrder = [preferredRateType, "CLOSING", "SPOT", "AVERAGE"].filter(
    (value, index, arr) => VALID_FX_RATE_TYPES.has(value) && arr.indexOf(value) === index
  );
  if (fallbackOrder.length === 0) {
    fallbackOrder.push("CLOSING", "SPOT", "AVERAGE");
  }

  const result = await runQuery(
    `SELECT rate, rate_type, rate_date
     FROM fx_rates
     WHERE tenant_id = ?
       AND from_currency_code = ?
       AND to_currency_code = ?
       AND rate_type IN (${fallbackOrder.map(() => "?").join(", ")})
       AND rate_date <= ?
     ORDER BY rate_date DESC,
              FIELD(rate_type, ${fallbackOrder.map(() => "?").join(", ")})
     LIMIT 1`,
    [
      tenantId,
      fromCode,
      toCode,
      ...fallbackOrder,
      rateDate,
      ...fallbackOrder,
    ]
  );

  const row = result.rows[0];
  if (!row) {
    throw badRequest(
      `FX rate not found for ${fromCode}->${toCode} on or before ${rateDate}`
    );
  }

  return {
    rate: Number(row.rate),
    rateType: String(row.rate_type),
    rateDate: row.rate_date,
  };
}

async function loadMemberMappedBalances({
  tenantId,
  consolidationGroupId,
  fiscalPeriodId,
  legalEntityId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
       je.legal_entity_id,
       group_acc.id AS group_account_id,
       SUM(jl.debit_base) AS local_debit_base,
       SUM(jl.credit_base) AS local_credit_base,
       SUM(jl.debit_base - jl.credit_base) AS local_balance_base
     FROM journal_entries je
     JOIN journal_lines jl ON jl.journal_entry_id = je.id
     JOIN accounts local_acc ON local_acc.id = jl.account_id
     JOIN group_coa_mappings gcm ON gcm.tenant_id = je.tenant_id
       AND gcm.consolidation_group_id = ?
       AND gcm.legal_entity_id = je.legal_entity_id
       AND gcm.local_coa_id = local_acc.coa_id
       AND gcm.status = 'ACTIVE'
     JOIN accounts group_acc ON group_acc.coa_id = gcm.group_coa_id
       AND group_acc.code = local_acc.code
       AND group_acc.is_active = TRUE
     WHERE je.tenant_id = ?
       AND je.status = 'POSTED'
       AND je.fiscal_period_id = ?
       AND je.legal_entity_id = ?
     GROUP BY je.legal_entity_id, group_acc.id`,
    [consolidationGroupId, tenantId, fiscalPeriodId, legalEntityId]
  );

  return result.rows || [];
}

async function executeConsolidationRun({
  tenantId,
  runId,
  preferredRateType,
  executedByUserId,
}) {
  const run = await getRunWithContext(tenantId, runId);
  if (!run) {
    throw badRequest("Consolidation run not found");
  }

  const consolidationGroupId = parsePositiveInt(run.consolidation_group_id);
  const fiscalPeriodId = parsePositiveInt(run.fiscal_period_id);
  const presentationCurrencyCode = String(
    run.presentation_currency_code || ""
  ).toUpperCase();
  const periodStartDate = toIsoDate(run.period_start_date, "periodStartDate");
  const periodEndDate = toIsoDate(run.period_end_date, "periodEndDate");

  const { insertedRowCount, totals } = await withTransaction(async (tx) => {
    await tx.query(
      `UPDATE consolidation_runs
       SET status = 'IN_PROGRESS',
           notes = ?
       WHERE id = ?`,
      [`Execution started by user ${executedByUserId}`, runId]
    );

    const memberResult = await tx.query(
      `SELECT
         cgm.legal_entity_id,
         cgm.consolidation_method,
         cgm.ownership_pct,
         le.functional_currency_code
       FROM consolidation_group_members cgm
       JOIN legal_entities le ON le.id = cgm.legal_entity_id
       WHERE cgm.consolidation_group_id = ?
         AND cgm.effective_from <= ?
         AND (cgm.effective_to IS NULL OR cgm.effective_to >= ?)`,
      [consolidationGroupId, periodEndDate, periodStartDate]
    );

    await tx.query(
      `DELETE FROM consolidation_run_entries
       WHERE consolidation_run_id = ?`,
      [runId]
    );

    let inserted = 0;

    for (const member of memberResult.rows) {
      const legalEntityId = parsePositiveInt(member.legal_entity_id);
      if (!legalEntityId) {
        continue;
      }

      const method = String(member.consolidation_method || "FULL").toUpperCase();
      const ownershipPct = Number(member.ownership_pct || 1);
      const factor = ownershipFactor(method, ownershipPct);
      const sourceCurrencyCode = String(
        member.functional_currency_code || ""
      ).toUpperCase();

      // eslint-disable-next-line no-await-in-loop
      const fx = await resolveFxRate({
        tenantId,
        rateDate: periodEndDate,
        fromCurrencyCode: sourceCurrencyCode,
        toCurrencyCode: presentationCurrencyCode,
        preferredRateType,
        runQuery: tx.query,
      });

      // eslint-disable-next-line no-await-in-loop
      const rows = await loadMemberMappedBalances({
        tenantId,
        consolidationGroupId,
        fiscalPeriodId,
        legalEntityId,
        runQuery: tx.query,
      });

      for (const row of rows) {
        const localDebitBase = Number(row.local_debit_base || 0);
        const localCreditBase = Number(row.local_credit_base || 0);
        const localBalanceBase = Number(row.local_balance_base || 0);
        const translationRate = Number(fx.rate || 0);

        const translatedDebit = localDebitBase * translationRate * factor;
        const translatedCredit = localCreditBase * translationRate * factor;
        const translatedBalance = localBalanceBase * translationRate * factor;

        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO consolidation_run_entries (
              consolidation_run_id,
              tenant_id,
              consolidation_group_id,
              fiscal_period_id,
              legal_entity_id,
              group_account_id,
              source_currency_code,
              presentation_currency_code,
              consolidation_method,
              ownership_pct,
              translation_rate,
              local_debit_base,
              local_credit_base,
              local_balance_base,
              translated_debit,
              translated_credit,
              translated_balance
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             source_currency_code = VALUES(source_currency_code),
             presentation_currency_code = VALUES(presentation_currency_code),
             consolidation_method = VALUES(consolidation_method),
             ownership_pct = VALUES(ownership_pct),
             translation_rate = VALUES(translation_rate),
             local_debit_base = VALUES(local_debit_base),
             local_credit_base = VALUES(local_credit_base),
             local_balance_base = VALUES(local_balance_base),
             translated_debit = VALUES(translated_debit),
             translated_credit = VALUES(translated_credit),
             translated_balance = VALUES(translated_balance)`,
          [
            runId,
            tenantId,
            consolidationGroupId,
            fiscalPeriodId,
            legalEntityId,
            parsePositiveInt(row.group_account_id),
            sourceCurrencyCode,
            presentationCurrencyCode,
            method,
            ownershipPct,
            translationRate,
            localDebitBase,
            localCreditBase,
            localBalanceBase,
            translatedDebit,
            translatedCredit,
            translatedBalance,
          ]
        );
        inserted += 1;
      }
    }

    const totalResult = await tx.query(
      `SELECT
         SUM(translated_debit) AS translated_debit_total,
         SUM(translated_credit) AS translated_credit_total,
         SUM(translated_balance) AS translated_balance_total
       FROM consolidation_run_entries
       WHERE consolidation_run_id = ?`,
      [runId]
    );
    const calculatedTotals = totalResult.rows[0] || {
      translated_debit_total: 0,
      translated_credit_total: 0,
      translated_balance_total: 0,
    };

    await tx.query(
      `UPDATE consolidation_runs
       SET status = 'COMPLETED',
           finished_at = CURRENT_TIMESTAMP,
           notes = ?
       WHERE id = ?`,
      [
        `Execution completed by user ${executedByUserId}; inserted_rows=${inserted}; rate_type=${preferredRateType}`,
        runId,
      ]
    );

    return {
      insertedRowCount: inserted,
      totals: calculatedTotals,
    };
  });

  return {
    run,
    insertedRowCount,
    totals: {
      translatedDebitTotal: Number(totals.translated_debit_total || 0),
      translatedCreditTotal: Number(totals.translated_credit_total || 0),
      translatedBalanceTotal: Number(totals.translated_balance_total || 0),
    },
  };
}

async function loadRunReportAccountBalances({
  tenantId,
  run,
  includeDraft = false,
  preferredRateType = "CLOSING",
  runQuery = query,
}) {
  const runId = parsePositiveInt(run?.id);
  if (!runId) {
    throw badRequest("Consolidation run not found");
  }

  const presentationCurrencyCode = String(
    run.presentation_currency_code || ""
  ).toUpperCase();
  const rateDate = toIsoDate(run.period_end_date, "periodEndDate");
  const statusFilter = includeDraft ? ["DRAFT", "POSTED"] : ["POSTED"];
  const statusPlaceholders = statusFilter.map(() => "?").join(", ");

  const accountMap = new Map();
  const fxRateCache = new Map();

  function ensureAccount(accountId, accountCode, accountName, accountType) {
    if (!accountMap.has(accountId)) {
      accountMap.set(accountId, {
        accountId,
        accountCode: String(accountCode || `ACC-${accountId}`),
        accountName: accountName ? String(accountName) : null,
        accountType: String(accountType || "").toUpperCase(),
        baseDebit: 0,
        baseCredit: 0,
        baseBalance: 0,
        adjustmentDebit: 0,
        adjustmentCredit: 0,
        adjustmentBalance: 0,
        eliminationDebit: 0,
        eliminationCredit: 0,
        eliminationBalance: 0,
        finalDebit: 0,
        finalCredit: 0,
        finalBalance: 0,
      });
    }
    return accountMap.get(accountId);
  }

  function addAmounts(row, component, debit, credit, balance) {
    row[`${component}Debit`] += debit;
    row[`${component}Credit`] += credit;
    row[`${component}Balance`] += balance;
    row.finalDebit += debit;
    row.finalCredit += credit;
    row.finalBalance += balance;
  }

  async function resolveCachedRate(fromCurrencyCode) {
    const source = String(fromCurrencyCode || "").toUpperCase();
    const key = `${source}->${presentationCurrencyCode}:${preferredRateType}:${rateDate}`;
    if (fxRateCache.has(key)) {
      return fxRateCache.get(key);
    }

    const fx = await resolveFxRate({
      tenantId,
      rateDate,
      fromCurrencyCode: source,
      toCurrencyCode: presentationCurrencyCode,
      preferredRateType,
      runQuery,
    });
    const numericRate = Number(fx.rate || 0);
    fxRateCache.set(key, numericRate);
    return numericRate;
  }

  const baseResult = await runQuery(
    `SELECT
       cre.group_account_id AS account_id,
       a.code AS account_code,
       a.name AS account_name,
       a.account_type,
       SUM(cre.translated_debit) AS debit_total,
       SUM(cre.translated_credit) AS credit_total,
       SUM(cre.translated_balance) AS balance_total
     FROM consolidation_run_entries cre
     JOIN accounts a ON a.id = cre.group_account_id
     WHERE cre.consolidation_run_id = ?
     GROUP BY cre.group_account_id, a.code, a.name, a.account_type`,
    [runId]
  );

  for (const row of baseResult.rows || []) {
    const accountId = parsePositiveInt(row.account_id);
    if (!accountId) {
      continue;
    }
    const target = ensureAccount(
      accountId,
      row.account_code,
      row.account_name,
      row.account_type
    );
    addAmounts(
      target,
      "base",
      Number(row.debit_total || 0),
      Number(row.credit_total || 0),
      Number(row.balance_total || 0)
    );
  }

  const adjustmentResult = await runQuery(
    `SELECT
       ca.account_id,
       a.code AS account_code,
       a.name AS account_name,
       a.account_type,
       ca.currency_code,
       SUM(ca.debit_amount) AS debit_total,
       SUM(ca.credit_amount) AS credit_total
     FROM consolidation_adjustments ca
     JOIN accounts a ON a.id = ca.account_id
     WHERE ca.consolidation_run_id = ?
       AND ca.status IN (${statusPlaceholders})
     GROUP BY
       ca.account_id,
       a.code,
       a.name,
       a.account_type,
       ca.currency_code`,
    [runId, ...statusFilter]
  );

  for (const row of adjustmentResult.rows || []) {
    const accountId = parsePositiveInt(row.account_id);
    if (!accountId) {
      continue;
    }

    const rate = await resolveCachedRate(row.currency_code);
    const debit = Number(row.debit_total || 0) * rate;
    const credit = Number(row.credit_total || 0) * rate;
    const balance = (Number(row.debit_total || 0) - Number(row.credit_total || 0)) * rate;

    const target = ensureAccount(
      accountId,
      row.account_code,
      row.account_name,
      row.account_type
    );
    addAmounts(target, "adjustment", debit, credit, balance);
  }

  const eliminationResult = await runQuery(
    `SELECT
       el.account_id,
       a.code AS account_code,
       a.name AS account_name,
       a.account_type,
       el.currency_code,
       SUM(el.debit_amount) AS debit_total,
       SUM(el.credit_amount) AS credit_total
     FROM elimination_entries ee
     JOIN elimination_lines el ON el.elimination_entry_id = ee.id
     JOIN accounts a ON a.id = el.account_id
     WHERE ee.consolidation_run_id = ?
       AND ee.status IN (${statusPlaceholders})
     GROUP BY
       el.account_id,
       a.code,
       a.name,
       a.account_type,
       el.currency_code`,
    [runId, ...statusFilter]
  );

  for (const row of eliminationResult.rows || []) {
    const accountId = parsePositiveInt(row.account_id);
    if (!accountId) {
      continue;
    }

    const rate = await resolveCachedRate(row.currency_code);
    const debit = Number(row.debit_total || 0) * rate;
    const credit = Number(row.credit_total || 0) * rate;
    const balance = (Number(row.debit_total || 0) - Number(row.credit_total || 0)) * rate;

    const target = ensureAccount(
      accountId,
      row.account_code,
      row.account_name,
      row.account_type
    );
    addAmounts(target, "elimination", debit, credit, balance);
  }

  return {
    statusFilter,
    rows: Array.from(accountMap.values()),
  };
}

router.get(
  "/groups",
  requirePermission("consolidation.group.read"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const params = [tenantId];
    const groupFilter = buildScopeFilter(req, "group", "group_company_id", params);

    const result = await query(
      `SELECT
         id,
         tenant_id,
         group_company_id,
         calendar_id,
         code,
         name,
         presentation_currency_code,
         status,
         created_at
       FROM consolidation_groups
       WHERE tenant_id = ?
         AND ${groupFilter}
       ORDER BY id`,
      params
    );

    return res.json({
      tenantId,
      rows: result.rows,
    });
  })
);

router.post(
  "/groups",
  requirePermission("consolidation.group.upsert", {
    resolveScope: (req, tenantId) => {
      const groupCompanyId = parsePositiveInt(req.body?.groupCompanyId);
      if (groupCompanyId) {
        return { scopeType: "GROUP", scopeId: groupCompanyId };
      }
      return { scopeType: "TENANT", scopeId: tenantId };
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    assertRequiredFields(req.body, [
      "groupCompanyId",
      "calendarId",
      "code",
      "name",
      "presentationCurrencyCode",
    ]);

    const groupCompanyId = parsePositiveInt(req.body.groupCompanyId);
    const calendarId = parsePositiveInt(req.body.calendarId);
    if (!groupCompanyId || !calendarId) {
      throw badRequest("groupCompanyId and calendarId must be positive integers");
    }

    await assertGroupCompanyBelongsToTenant(tenantId, groupCompanyId, "groupCompanyId");
    await assertFiscalCalendarBelongsToTenant(tenantId, calendarId, "calendarId");
    assertScopeAccess(req, "group", groupCompanyId, "groupCompanyId");

    const result = await query(
      `INSERT INTO consolidation_groups (
          tenant_id, group_company_id, calendar_id, code, name, presentation_currency_code
       )
       VALUES (?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         group_company_id = VALUES(group_company_id),
         calendar_id = VALUES(calendar_id),
         name = VALUES(name),
         presentation_currency_code = VALUES(presentation_currency_code)`,
      [
        tenantId,
        groupCompanyId,
        calendarId,
        String(req.body.code).trim(),
        String(req.body.name).trim(),
        String(req.body.presentationCurrencyCode).toUpperCase(),
      ]
    );

    return res.status(201).json({ ok: true, id: result.rows.insertId || null });
  })
);

router.post(
  "/groups/:groupId/members",
  requirePermission("consolidation.group_member.upsert"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const groupId = parsePositiveInt(req.params.groupId);
    if (!groupId) {
      throw badRequest("groupId must be a positive integer");
    }
    const group = await assertConsolidationGroupBelongsToTenant(tenantId, groupId, "groupId");
    assertScopeAccess(req, "group", group.group_company_id, "groupCompanyId");

    assertRequiredFields(req.body, ["legalEntityId", "effectiveFrom"]);
    const legalEntityId = parsePositiveInt(req.body.legalEntityId);
    if (!legalEntityId) {
      throw badRequest("legalEntityId must be a positive integer");
    }
    await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
    assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");

    const consolidationMethod = String(
      req.body.consolidationMethod || "FULL"
    ).toUpperCase();
    const ownershipPct =
      req.body.ownershipPct === undefined ? 1 : Number(req.body.ownershipPct);

    const result = await query(
      `INSERT INTO consolidation_group_members (
          consolidation_group_id, legal_entity_id, consolidation_method, ownership_pct, effective_from, effective_to
       )
       VALUES (?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         consolidation_method = VALUES(consolidation_method),
         ownership_pct = VALUES(ownership_pct),
         effective_to = VALUES(effective_to)`,
      [
        groupId,
        legalEntityId,
        consolidationMethod,
        ownershipPct,
        String(req.body.effectiveFrom),
        req.body.effectiveTo ? String(req.body.effectiveTo) : null,
      ]
    );

    return res.status(201).json({ ok: true, id: result.rows.insertId || null });
  })
);

router.get(
  "/groups/:groupId/members",
  requirePermission("consolidation.group.read"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const groupId = parsePositiveInt(req.params.groupId);
    if (!groupId) {
      throw badRequest("groupId must be a positive integer");
    }

    const group = await assertConsolidationGroupBelongsToTenant(
      tenantId,
      groupId,
      "groupId"
    );
    assertScopeAccess(req, "group", group.group_company_id, "groupCompanyId");

    const legalEntityId = parsePositiveInt(req.query.legalEntityId);
    if (legalEntityId) {
      await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
      assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
    }

    const conditions = ["cgm.consolidation_group_id = ?"];
    const params = [groupId];
    if (legalEntityId) {
      conditions.push("cgm.legal_entity_id = ?");
      params.push(legalEntityId);
    }

    const result = await query(
      `SELECT
         cgm.id,
         cgm.consolidation_group_id,
         cgm.legal_entity_id,
         le.code AS legal_entity_code,
         le.name AS legal_entity_name,
         cgm.consolidation_method,
         cgm.ownership_pct,
         cgm.effective_from,
         cgm.effective_to
       FROM consolidation_group_members cgm
       JOIN legal_entities le ON le.id = cgm.legal_entity_id
       WHERE ${conditions.join(" AND ")}
       ORDER BY cgm.effective_from DESC, cgm.id DESC`,
      params
    );

    return res.json({
      tenantId,
      groupId,
      rows: result.rows,
    });
  })
);

router.get(
  "/groups/:groupId/coa-mappings",
  requirePermission("consolidation.coa_mapping.read"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const groupId = parsePositiveInt(req.params.groupId);
    if (!groupId) {
      throw badRequest("groupId must be a positive integer");
    }
    const group = await assertConsolidationGroupBelongsToTenant(tenantId, groupId, "groupId");
    assertScopeAccess(req, "group", group.group_company_id, "groupCompanyId");

    const legalEntityId = parsePositiveInt(req.query.legalEntityId);
    if (legalEntityId) {
      await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
      assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
    }

    const conditions = ["tenant_id = ?", "consolidation_group_id = ?"];
    const params = [tenantId, groupId];

    if (legalEntityId) {
      conditions.push("legal_entity_id = ?");
      params.push(legalEntityId);
    }

    const result = await query(
      `SELECT
         id,
         tenant_id,
         consolidation_group_id,
         legal_entity_id,
         group_coa_id,
         local_coa_id,
         status,
         created_at,
         updated_at
       FROM group_coa_mappings
       WHERE ${conditions.join(" AND ")}
       ORDER BY id`,
      params
    );

    return res.json({
      tenantId,
      groupId,
      rows: result.rows,
    });
  })
);

router.post(
  "/groups/:groupId/coa-mappings",
  requirePermission("consolidation.coa_mapping.upsert"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const groupId = parsePositiveInt(req.params.groupId);
    if (!groupId) {
      throw badRequest("groupId must be a positive integer");
    }
    const group = await assertConsolidationGroupBelongsToTenant(tenantId, groupId, "groupId");
    assertScopeAccess(req, "group", group.group_company_id, "groupCompanyId");

    assertRequiredFields(req.body, ["legalEntityId", "groupCoaId", "localCoaId"]);
    const legalEntityId = parsePositiveInt(req.body.legalEntityId);
    const groupCoaId = parsePositiveInt(req.body.groupCoaId);
    const localCoaId = parsePositiveInt(req.body.localCoaId);
    if (!legalEntityId || !groupCoaId || !localCoaId) {
      throw badRequest("legalEntityId, groupCoaId and localCoaId must be positive integers");
    }

    await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
    assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");

    const groupCoa = await assertCoaBelongsToTenant(tenantId, groupCoaId, "groupCoaId");
    const localCoa = await assertCoaBelongsToTenant(tenantId, localCoaId, "localCoaId");
    if (String(groupCoa.scope || "").toUpperCase() !== "GROUP") {
      throw badRequest("groupCoaId must reference a GROUP scoped chart of accounts");
    }
    if (parsePositiveInt(localCoa.legal_entity_id) !== legalEntityId) {
      throw badRequest("localCoaId must belong to legalEntityId");
    }

    const status = String(req.body.status || "ACTIVE").toUpperCase();

    const result = await query(
      `INSERT INTO group_coa_mappings (
          tenant_id, consolidation_group_id, legal_entity_id, group_coa_id, local_coa_id, status
       )
       VALUES (?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         status = VALUES(status),
         updated_at = CURRENT_TIMESTAMP`,
      [tenantId, groupId, legalEntityId, groupCoaId, localCoaId, status]
    );

    return res.status(201).json({
      ok: true,
      id: result.rows.insertId || null,
    });
  })
);

router.get(
  "/groups/:groupId/elimination-placeholders",
  requirePermission("consolidation.elimination_placeholder.read"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const groupId = parsePositiveInt(req.params.groupId);
    if (!groupId) {
      throw badRequest("groupId must be a positive integer");
    }
    const group = await assertConsolidationGroupBelongsToTenant(tenantId, groupId, "groupId");
    assertScopeAccess(req, "group", group.group_company_id, "groupCompanyId");

    const result = await query(
      `SELECT
         id,
         tenant_id,
         consolidation_group_id,
         placeholder_code,
         name,
         account_id,
         default_direction,
         description,
         is_active,
         created_at,
         updated_at
       FROM elimination_placeholders
       WHERE tenant_id = ?
         AND consolidation_group_id = ?
       ORDER BY placeholder_code`,
      [tenantId, groupId]
    );

    return res.json({
      tenantId,
      groupId,
      rows: result.rows,
    });
  })
);

router.post(
  "/groups/:groupId/elimination-placeholders",
  requirePermission("consolidation.elimination_placeholder.upsert"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const groupId = parsePositiveInt(req.params.groupId);
    if (!groupId) {
      throw badRequest("groupId must be a positive integer");
    }
    const group = await assertConsolidationGroupBelongsToTenant(tenantId, groupId, "groupId");
    assertScopeAccess(req, "group", group.group_company_id, "groupCompanyId");

    assertRequiredFields(req.body, ["placeholderCode", "name"]);
    const accountId = req.body.accountId ? parsePositiveInt(req.body.accountId) : null;
    if (req.body.accountId && !accountId) {
      throw badRequest("accountId must be a positive integer");
    }
    if (accountId) {
      await assertAccountBelongsToTenant(tenantId, accountId, "accountId");
    }
    const placeholderCode = String(req.body.placeholderCode).trim().toUpperCase();
    const name = String(req.body.name).trim();
    const defaultDirection = String(req.body.defaultDirection || "AUTO").toUpperCase();
    const description = req.body.description ? String(req.body.description) : null;
    const isActive =
      req.body.isActive === undefined ? true : Boolean(req.body.isActive);

    const result = await query(
      `INSERT INTO elimination_placeholders (
          tenant_id,
          consolidation_group_id,
          placeholder_code,
          name,
          account_id,
          default_direction,
          description,
          is_active
       )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         name = VALUES(name),
         account_id = VALUES(account_id),
         default_direction = VALUES(default_direction),
         description = VALUES(description),
         is_active = VALUES(is_active),
         updated_at = CURRENT_TIMESTAMP`,
      [
        tenantId,
        groupId,
        placeholderCode,
        name,
        accountId,
        defaultDirection,
        description,
        isActive,
      ]
    );

    return res.status(201).json({
      ok: true,
      id: result.rows.insertId || null,
    });
  })
);

router.get(
  "/runs",
  requirePermission("consolidation.run.read"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const consolidationGroupId = parsePositiveInt(req.query.consolidationGroupId);
    const fiscalPeriodId = parsePositiveInt(req.query.fiscalPeriodId);
    const status = req.query.status ? String(req.query.status).toUpperCase() : null;

    const params = [tenantId];
    const conditions = ["cg.tenant_id = ?"];
    conditions.push(buildScopeFilter(req, "group", "cg.group_company_id", params));

    if (consolidationGroupId) {
      conditions.push("cr.consolidation_group_id = ?");
      params.push(consolidationGroupId);
    }
    if (fiscalPeriodId) {
      conditions.push("cr.fiscal_period_id = ?");
      params.push(fiscalPeriodId);
    }
    if (status) {
      conditions.push("cr.status = ?");
      params.push(status);
    }

    const result = await query(
      `SELECT
         cr.id,
         cr.consolidation_group_id,
         cr.fiscal_period_id,
         cr.run_name,
         cr.status,
         cr.presentation_currency_code,
         cr.started_by_user_id,
         cr.started_at,
         cr.finished_at,
         cr.notes,
         cg.group_company_id,
         cg.code AS consolidation_group_code,
         cg.name AS consolidation_group_name,
         fp.fiscal_year,
         fp.period_no,
         fp.period_name
       FROM consolidation_runs cr
       JOIN consolidation_groups cg ON cg.id = cr.consolidation_group_id
       JOIN fiscal_periods fp ON fp.id = cr.fiscal_period_id
       WHERE ${conditions.join(" AND ")}
       ORDER BY cr.started_at DESC, cr.id DESC`,
      params
    );

    return res.json({
      tenantId,
      rows: result.rows,
    });
  })
);

router.post(
  "/runs",
  requirePermission("consolidation.run.create"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    assertRequiredFields(req.body, [
      "consolidationGroupId",
      "fiscalPeriodId",
      "runName",
      "presentationCurrencyCode",
    ]);

    const consolidationGroupId = parsePositiveInt(req.body.consolidationGroupId);
    const fiscalPeriodId = parsePositiveInt(req.body.fiscalPeriodId);
    const startedByUserId = parsePositiveInt(req.user?.userId);
    const presentationCurrencyCode = String(
      req.body.presentationCurrencyCode || ""
    ).toUpperCase();

    if (!consolidationGroupId || !fiscalPeriodId || !startedByUserId) {
      throw badRequest(
        "consolidationGroupId, fiscalPeriodId and authenticated user are required"
      );
    }

    await assertUserBelongsToTenant(tenantId, startedByUserId, "startedByUserId");
    const group = await assertConsolidationGroupBelongsToTenant(
      tenantId,
      consolidationGroupId,
      "consolidationGroupId"
    );

    const groupCompanyId = parsePositiveInt(group.group_company_id);
    if (groupCompanyId) {
      assertScopeAccess(req, "group", groupCompanyId, "groupCompanyId");
    }

    await assertFiscalPeriodBelongsToCalendar(
      parsePositiveInt(group.calendar_id),
      fiscalPeriodId,
      "fiscalPeriodId"
    );

    const result = await query(
      `INSERT INTO consolidation_runs (
          consolidation_group_id, fiscal_period_id, run_name, status, presentation_currency_code, started_by_user_id
       )
       VALUES (?, ?, ?, 'DRAFT', ?, ?)`,
      [
        consolidationGroupId,
        fiscalPeriodId,
        String(req.body.runName),
        presentationCurrencyCode,
        startedByUserId,
      ]
    );

    return res.status(201).json({
      ok: true,
      tenantId,
      runId: result.rows.insertId || null,
    });
  })
);

router.get(
  "/runs/:runId",
  requirePermission("consolidation.run.read", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    if (!runId) {
      throw badRequest("runId must be a positive integer");
    }

    const run = await getRunWithContext(tenantId, runId);
    if (!run) {
      throw badRequest("Consolidation run not found");
    }

    const entryCountResult = await query(
      `SELECT COUNT(*) AS entry_count
       FROM consolidation_run_entries
       WHERE consolidation_run_id = ?`,
      [runId]
    );
    const totalsResult = await query(
      `SELECT
         SUM(translated_debit) AS translated_debit_total,
         SUM(translated_credit) AS translated_credit_total,
         SUM(translated_balance) AS translated_balance_total
       FROM consolidation_run_entries
       WHERE consolidation_run_id = ?`,
      [runId]
    );

    return res.json({
      tenantId,
      run: {
        ...run,
        entryCount: Number(entryCountResult.rows[0]?.entry_count || 0),
        totals: {
          translatedDebitTotal: Number(
            totalsResult.rows[0]?.translated_debit_total || 0
          ),
          translatedCreditTotal: Number(
            totalsResult.rows[0]?.translated_credit_total || 0
          ),
          translatedBalanceTotal: Number(
            totalsResult.rows[0]?.translated_balance_total || 0
          ),
        },
      },
    });
  })
);

router.post(
  "/runs/:runId/execute",
  requirePermission("consolidation.run.execute", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    if (!runId) {
      throw badRequest("runId must be a positive integer");
    }

    const executedByUserId = parsePositiveInt(req.user?.userId);
    if (!executedByUserId) {
      throw badRequest("Authenticated user is required");
    }

    const preferredRateType = normalizeRateType(req.body?.rateType);

    try {
      const execution = await executeConsolidationRun({
        tenantId,
        runId,
        preferredRateType,
        executedByUserId,
      });

      return res.json({
        ok: true,
        runId,
        status: "COMPLETED",
        preferredRateType,
        insertedRowCount: execution.insertedRowCount,
        totals: execution.totals,
      });
    } catch (err) {
      await query(
        `UPDATE consolidation_runs
         SET status = 'FAILED',
             finished_at = CURRENT_TIMESTAMP,
             notes = ?
         WHERE id = ?`,
        [String(err.message || "Execution failed").slice(0, 500), runId]
      );
      throw err;
    }
  })
);

router.get(
  "/runs/:runId/eliminations",
  requirePermission("consolidation.run.read", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    if (!runId) {
      throw badRequest("runId must be a positive integer");
    }
    await requireRun(tenantId, runId);

    const status = normalizeDraftPostingStatus(req.query.status);
    const includeLines = parseBooleanLike(req.query.includeLines, false, "includeLines");

    const params = [runId];
    const conditions = ["ee.consolidation_run_id = ?"];
    if (status !== "ALL") {
      conditions.push("ee.status = ?");
      params.push(status);
    }

    const result = await query(
      `SELECT
         ee.id,
         ee.status,
         ee.description,
         ee.reference_no,
         ee.created_by_user_id,
         creator.name AS created_by_user_name,
         ee.posted_by_user_id,
         poster.name AS posted_by_user_name,
         ee.created_at,
         ee.posted_at,
         COALESCE(SUM(el.debit_amount), 0) AS debit_total,
         COALESCE(SUM(el.credit_amount), 0) AS credit_total,
         COUNT(el.id) AS line_count
       FROM elimination_entries ee
       LEFT JOIN elimination_lines el ON el.elimination_entry_id = ee.id
       LEFT JOIN users creator ON creator.id = ee.created_by_user_id
       LEFT JOIN users poster ON poster.id = ee.posted_by_user_id
       WHERE ${conditions.join(" AND ")}
       GROUP BY
         ee.id,
         ee.status,
         ee.description,
         ee.reference_no,
         ee.created_by_user_id,
         creator.name,
         ee.posted_by_user_id,
         poster.name,
         ee.created_at,
         ee.posted_at
       ORDER BY ee.id DESC`,
      params
    );

    const rows = (result.rows || []).map((row) => ({
      id: parsePositiveInt(row.id),
      status: String(row.status || "").toUpperCase(),
      description: row.description || null,
      referenceNo: row.reference_no || null,
      createdByUserId: parsePositiveInt(row.created_by_user_id),
      createdByUserName: row.created_by_user_name || null,
      postedByUserId: parsePositiveInt(row.posted_by_user_id),
      postedByUserName: row.posted_by_user_name || null,
      createdAt: row.created_at || null,
      postedAt: row.posted_at || null,
      debitTotal: Number(row.debit_total || 0),
      creditTotal: Number(row.credit_total || 0),
      lineCount: Number(row.line_count || 0),
    }));

    if (includeLines && rows.length > 0) {
      const entryIds = rows.map((row) => row.id).filter(Boolean);
      if (entryIds.length > 0) {
        const placeholders = entryIds.map(() => "?").join(", ");
        const lineResult = await query(
          `SELECT
             el.elimination_entry_id,
             el.line_no,
             el.account_id,
             a.code AS account_code,
             a.name AS account_name,
             el.legal_entity_id,
             le.code AS legal_entity_code,
             le.name AS legal_entity_name,
             el.counterparty_legal_entity_id,
             cle.code AS counterparty_legal_entity_code,
             cle.name AS counterparty_legal_entity_name,
             el.debit_amount,
             el.credit_amount,
             el.currency_code,
             el.description
           FROM elimination_lines el
           JOIN accounts a ON a.id = el.account_id
           LEFT JOIN legal_entities le ON le.id = el.legal_entity_id
           LEFT JOIN legal_entities cle ON cle.id = el.counterparty_legal_entity_id
           WHERE el.elimination_entry_id IN (${placeholders})
           ORDER BY el.elimination_entry_id, el.line_no`,
          entryIds
        );

        const linesByEntryId = new Map();
        for (const line of lineResult.rows || []) {
          const entryId = parsePositiveInt(line.elimination_entry_id);
          if (!entryId) {
            continue;
          }
          if (!linesByEntryId.has(entryId)) {
            linesByEntryId.set(entryId, []);
          }
          linesByEntryId.get(entryId).push({
            lineNo: Number(line.line_no || 0),
            accountId: parsePositiveInt(line.account_id),
            accountCode: line.account_code || null,
            accountName: line.account_name || null,
            legalEntityId: parsePositiveInt(line.legal_entity_id),
            legalEntityCode: line.legal_entity_code || null,
            legalEntityName: line.legal_entity_name || null,
            counterpartyLegalEntityId: parsePositiveInt(line.counterparty_legal_entity_id),
            counterpartyLegalEntityCode: line.counterparty_legal_entity_code || null,
            counterpartyLegalEntityName: line.counterparty_legal_entity_name || null,
            debitAmount: Number(line.debit_amount || 0),
            creditAmount: Number(line.credit_amount || 0),
            currencyCode: String(line.currency_code || "").toUpperCase(),
            description: line.description || null,
          });
        }

        for (const row of rows) {
          row.lines = linesByEntryId.get(row.id) || [];
        }
      }
    }

    return res.json({
      runId,
      status,
      rows,
    });
  })
);

router.post(
  "/runs/:runId/eliminations",
  requirePermission("consolidation.elimination.create", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    const userId = parsePositiveInt(req.user?.userId);
    if (!runId || !userId) {
      throw badRequest("runId and authenticated user are required");
    }
    await assertUserBelongsToTenant(tenantId, userId, "userId");
    await requireRun(tenantId, runId);

    assertRequiredFields(req.body, ["description", "lines"]);
    const lines = Array.isArray(req.body.lines) ? req.body.lines : [];
    if (lines.length === 0) {
      throw badRequest("lines must be a non-empty array");
    }

    const normalizedLines = [];
    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      const accountId = parsePositiveInt(line.accountId);
      if (!accountId) {
        throw badRequest(`Invalid accountId on elimination line ${i + 1}`);
      }
      await assertAccountBelongsToTenant(tenantId, accountId, `lines[${i}].accountId`);

      const legalEntityId = parsePositiveInt(line.legalEntityId);
      const counterpartyLegalEntityId = parsePositiveInt(
        line.counterpartyLegalEntityId
      );
      if (legalEntityId) {
        await assertLegalEntityBelongsToTenant(
          tenantId,
          legalEntityId,
          `lines[${i}].legalEntityId`
        );
        assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
      }
      if (counterpartyLegalEntityId) {
        await assertLegalEntityBelongsToTenant(
          tenantId,
          counterpartyLegalEntityId,
          `lines[${i}].counterpartyLegalEntityId`
        );
        assertScopeAccess(
          req,
          "legal_entity",
          counterpartyLegalEntityId,
          "counterpartyLegalEntityId"
        );
      }

      normalizedLines.push({
        accountId,
        legalEntityId,
        counterpartyLegalEntityId,
        debitAmount: Number(line.debitAmount || 0),
        creditAmount: Number(line.creditAmount || 0),
        currencyCode: String(line.currencyCode || "USD").toUpperCase(),
        description: line.description ? String(line.description) : null,
      });
    }

    const eliminationEntryId = await withTransaction(async (tx) => {
      const entryResult = await tx.query(
        `INSERT INTO elimination_entries (
            consolidation_run_id, status, description, reference_no, created_by_user_id
         )
         VALUES (?, 'DRAFT', ?, ?, ?)`,
        [runId, String(req.body.description), req.body.referenceNo || null, userId]
      );
      const createdEntryId = parsePositiveInt(entryResult.rows.insertId);
      if (!createdEntryId) {
        throw badRequest("Failed to create elimination entry");
      }

      for (let i = 0; i < normalizedLines.length; i += 1) {
        const line = normalizedLines[i];
        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO elimination_lines (
              elimination_entry_id, line_no, account_id, legal_entity_id,
              counterparty_legal_entity_id, debit_amount, credit_amount, currency_code, description
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            createdEntryId,
            i + 1,
            line.accountId,
            line.legalEntityId,
            line.counterpartyLegalEntityId,
            line.debitAmount,
            line.creditAmount,
            line.currencyCode,
            line.description,
          ]
        );
      }

      return createdEntryId;
    });

    return res.status(201).json({
      ok: true,
      eliminationEntryId,
      lineCount: lines.length,
    });
  })
);

router.post(
  "/runs/:runId/eliminations/:eliminationEntryId/post",
  requirePermission("consolidation.elimination.post", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    const eliminationEntryId = parsePositiveInt(req.params.eliminationEntryId);
    const userId = parsePositiveInt(req.user?.userId);
    if (!runId || !eliminationEntryId || !userId) {
      throw badRequest("runId, eliminationEntryId and authenticated user are required");
    }
    await assertUserBelongsToTenant(tenantId, userId, "userId");
    await requireRun(tenantId, runId);

    const postResult = await withTransaction(async (tx) => {
      const entryResult = await tx.query(
        `SELECT
           ee.id,
           ee.status,
           ee.consolidation_run_id,
           cr.status AS run_status,
           ee.posted_by_user_id,
           ee.posted_at
         FROM elimination_entries ee
         JOIN consolidation_runs cr ON cr.id = ee.consolidation_run_id
         JOIN consolidation_groups cg ON cg.id = cr.consolidation_group_id
         WHERE ee.id = ?
           AND ee.consolidation_run_id = ?
           AND cg.tenant_id = ?
         LIMIT 1
         FOR UPDATE`,
        [eliminationEntryId, runId, tenantId]
      );
      const entry = entryResult.rows[0];
      if (!entry) {
        throw badRequest("eliminationEntryId not found for runId and tenant");
      }

      assertRunNotLocked({ status: entry.run_status });

      if (String(entry.status || "").toUpperCase() === "POSTED") {
        return {
          idempotent: true,
          eliminationEntryId,
          status: "POSTED",
          postedByUserId: parsePositiveInt(entry.posted_by_user_id),
          postedAt: entry.posted_at || null,
        };
      }

      const lineResult = await tx.query(
        `SELECT id, debit_amount, credit_amount
         FROM elimination_lines
         WHERE elimination_entry_id = ?
         FOR UPDATE`,
        [eliminationEntryId]
      );
      const lines = lineResult.rows || [];
      if (lines.length === 0) {
        throw badRequest("Cannot post elimination entry with no lines");
      }

      let debitTotal = 0;
      let creditTotal = 0;
      for (const line of lines) {
        debitTotal += Number(line.debit_amount || 0);
        creditTotal += Number(line.credit_amount || 0);
      }
      if (Math.abs(debitTotal - creditTotal) > BALANCE_EPSILON) {
        throw badRequest("Elimination entry is not balanced and cannot be posted");
      }

      await tx.query(
        `UPDATE elimination_entries
         SET status = 'POSTED',
             posted_by_user_id = ?,
             posted_at = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [userId, eliminationEntryId]
      );

      const postedResult = await tx.query(
        `SELECT posted_by_user_id, posted_at
         FROM elimination_entries
         WHERE id = ?
         LIMIT 1`,
        [eliminationEntryId]
      );
      const postedRow = postedResult.rows[0] || {};

      return {
        idempotent: false,
        eliminationEntryId,
        status: "POSTED",
        postedByUserId: parsePositiveInt(postedRow.posted_by_user_id),
        postedAt: postedRow.posted_at || null,
      };
    });

    return res.json({
      ok: true,
      ...postResult,
    });
  })
);

router.get(
  "/runs/:runId/adjustments",
  requirePermission("consolidation.run.read", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    if (!runId) {
      throw badRequest("runId must be a positive integer");
    }
    await requireRun(tenantId, runId);

    const status = normalizeDraftPostingStatus(req.query.status);
    const params = [runId];
    const conditions = ["ca.consolidation_run_id = ?"];
    if (status !== "ALL") {
      conditions.push("ca.status = ?");
      params.push(status);
    }

    const result = await query(
      `SELECT
         ca.id,
         ca.adjustment_type,
         ca.status,
         ca.legal_entity_id,
         le.code AS legal_entity_code,
         le.name AS legal_entity_name,
         ca.account_id,
         a.code AS account_code,
         a.name AS account_name,
         a.account_type,
         ca.debit_amount,
         ca.credit_amount,
         ca.currency_code,
         ca.description,
         ca.created_by_user_id,
         creator.name AS created_by_user_name,
         ca.posted_by_user_id,
         poster.name AS posted_by_user_name,
         ca.created_at,
         ca.posted_at
       FROM consolidation_adjustments ca
       JOIN accounts a ON a.id = ca.account_id
       LEFT JOIN legal_entities le ON le.id = ca.legal_entity_id
       LEFT JOIN users creator ON creator.id = ca.created_by_user_id
       LEFT JOIN users poster ON poster.id = ca.posted_by_user_id
       WHERE ${conditions.join(" AND ")}
       ORDER BY ca.id DESC`,
      params
    );

    return res.json({
      runId,
      status,
      rows: (result.rows || []).map((row) => ({
        id: parsePositiveInt(row.id),
        adjustmentType: String(row.adjustment_type || "").toUpperCase(),
        status: String(row.status || "").toUpperCase(),
        legalEntityId: parsePositiveInt(row.legal_entity_id),
        legalEntityCode: row.legal_entity_code || null,
        legalEntityName: row.legal_entity_name || null,
        accountId: parsePositiveInt(row.account_id),
        accountCode: row.account_code || null,
        accountName: row.account_name || null,
        accountType: String(row.account_type || "").toUpperCase(),
        debitAmount: Number(row.debit_amount || 0),
        creditAmount: Number(row.credit_amount || 0),
        currencyCode: String(row.currency_code || "").toUpperCase(),
        description: row.description || null,
        createdByUserId: parsePositiveInt(row.created_by_user_id),
        createdByUserName: row.created_by_user_name || null,
        postedByUserId: parsePositiveInt(row.posted_by_user_id),
        postedByUserName: row.posted_by_user_name || null,
        createdAt: row.created_at || null,
        postedAt: row.posted_at || null,
      })),
    });
  })
);

router.post(
  "/runs/:runId/adjustments",
  requirePermission("consolidation.adjustment.create", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    const userId = parsePositiveInt(req.user?.userId);
    if (!runId || !userId) {
      throw badRequest("runId and authenticated user are required");
    }
    await assertUserBelongsToTenant(tenantId, userId, "userId");
    await requireRun(tenantId, runId);

    assertRequiredFields(req.body, [
      "accountId",
      "currencyCode",
      "description",
      "debitAmount",
      "creditAmount",
    ]);

    const accountId = parsePositiveInt(req.body.accountId);
    if (!accountId) {
      throw badRequest("accountId must be a positive integer");
    }
    await assertAccountBelongsToTenant(tenantId, accountId, "accountId");
    const legalEntityId = parsePositiveInt(req.body.legalEntityId);
    if (legalEntityId) {
      await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
      assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
    }

    const result = await query(
      `INSERT INTO consolidation_adjustments (
          consolidation_run_id, adjustment_type, status, legal_entity_id, account_id,
          debit_amount, credit_amount, currency_code, description, created_by_user_id
       )
       VALUES (?, ?, 'DRAFT', ?, ?, ?, ?, ?, ?, ?)`,
      [
        runId,
        String(req.body.adjustmentType || "TOPSIDE").toUpperCase(),
        legalEntityId,
        accountId,
        Number(req.body.debitAmount || 0),
        Number(req.body.creditAmount || 0),
        String(req.body.currencyCode).toUpperCase(),
        String(req.body.description),
        userId,
      ]
    );

    return res.status(201).json({
      ok: true,
      adjustmentId: result.rows.insertId || null,
    });
  })
);

router.post(
  "/runs/:runId/adjustments/:adjustmentId/post",
  requirePermission("consolidation.adjustment.post", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    const adjustmentId = parsePositiveInt(req.params.adjustmentId);
    const userId = parsePositiveInt(req.user?.userId);
    if (!runId || !adjustmentId || !userId) {
      throw badRequest("runId, adjustmentId and authenticated user are required");
    }
    await assertUserBelongsToTenant(tenantId, userId, "userId");
    await requireRun(tenantId, runId);

    const postResult = await withTransaction(async (tx) => {
      const adjustmentResult = await tx.query(
        `SELECT
           ca.id,
           ca.status,
           ca.debit_amount,
           ca.credit_amount,
           ca.posted_by_user_id,
           ca.posted_at,
           cr.status AS run_status
         FROM consolidation_adjustments ca
         JOIN consolidation_runs cr ON cr.id = ca.consolidation_run_id
         JOIN consolidation_groups cg ON cg.id = cr.consolidation_group_id
         WHERE ca.id = ?
           AND ca.consolidation_run_id = ?
           AND cg.tenant_id = ?
         LIMIT 1
         FOR UPDATE`,
        [adjustmentId, runId, tenantId]
      );
      const adjustment = adjustmentResult.rows[0];
      if (!adjustment) {
        throw badRequest("adjustmentId not found for runId and tenant");
      }

      assertRunNotLocked({ status: adjustment.run_status });

      if (String(adjustment.status || "").toUpperCase() === "POSTED") {
        return {
          idempotent: true,
          adjustmentId,
          status: "POSTED",
          postedByUserId: parsePositiveInt(adjustment.posted_by_user_id),
          postedAt: adjustment.posted_at || null,
        };
      }

      const debitAmount = Number(adjustment.debit_amount || 0);
      const creditAmount = Number(adjustment.credit_amount || 0);
      const validOneSided =
        (debitAmount > 0 && Math.abs(creditAmount) < BALANCE_EPSILON) ||
        (creditAmount > 0 && Math.abs(debitAmount) < BALANCE_EPSILON);
      if (!validOneSided) {
        throw badRequest("Adjustment must be one-sided and cannot be posted");
      }

      await tx.query(
        `UPDATE consolidation_adjustments
         SET status = 'POSTED',
             posted_by_user_id = ?,
             posted_at = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [userId, adjustmentId]
      );

      const postedResult = await tx.query(
        `SELECT posted_by_user_id, posted_at
         FROM consolidation_adjustments
         WHERE id = ?
         LIMIT 1`,
        [adjustmentId]
      );
      const postedRow = postedResult.rows[0] || {};

      return {
        idempotent: false,
        adjustmentId,
        status: "POSTED",
        postedByUserId: parsePositiveInt(postedRow.posted_by_user_id),
        postedAt: postedRow.posted_at || null,
      };
    });

    return res.json({
      ok: true,
      ...postResult,
    });
  })
);

router.post(
  "/runs/:runId/finalize",
  requirePermission("consolidation.run.finalize", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    if (!runId) {
      throw badRequest("runId must be a positive integer");
    }
    await requireRun(tenantId, runId);

    await query(
      `UPDATE consolidation_runs
       SET status = 'LOCKED', finished_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [runId]
    );

    return res.json({ ok: true, runId, status: "LOCKED" });
  })
);

router.get(
  "/runs/:runId/reports/trial-balance",
  requirePermission("consolidation.report.trial_balance.read", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    if (!runId) {
      throw badRequest("runId must be a positive integer");
    }
    await requireRun(tenantId, runId);

    const result = await query(
      `SELECT
         cre.group_account_id AS account_id,
         a.code AS account_code,
         a.name AS account_name,
         SUM(cre.translated_debit) AS debit_total,
         SUM(cre.translated_credit) AS credit_total,
         SUM(cre.translated_balance) AS balance
       FROM consolidation_run_entries cre
       JOIN accounts a ON a.id = cre.group_account_id
       WHERE cre.consolidation_run_id = ?
       GROUP BY cre.group_account_id, a.code, a.name
       ORDER BY a.code`,
      [runId]
    );

    return res.json({
      runId,
      rows: result.rows,
    });
  })
);

router.get(
  "/runs/:runId/reports/summary",
  requirePermission("consolidation.report.summary.read", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    if (!runId) {
      throw badRequest("runId must be a positive integer");
    }

    const run = await getRunWithContext(tenantId, runId);
    if (!run) {
      throw badRequest("Consolidation run not found");
    }

    const groupBy = String(req.query.groupBy || "account_entity").toLowerCase();
    if (!["account", "entity", "account_entity"].includes(groupBy)) {
      throw badRequest("groupBy must be one of account, entity, account_entity");
    }

    let selectClause = "";
    let groupClause = "";
    let orderClause = "";

    if (groupBy === "account") {
      selectClause = `
        cre.group_account_id AS account_id,
        a.code AS account_code,
        a.name AS account_name,
        NULL AS legal_entity_id,
        NULL AS legal_entity_code,
        NULL AS legal_entity_name
      `;
      groupClause = "cre.group_account_id, a.code, a.name";
      orderClause = "a.code";
    } else if (groupBy === "entity") {
      selectClause = `
        NULL AS account_id,
        NULL AS account_code,
        NULL AS account_name,
        cre.legal_entity_id AS legal_entity_id,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
      `;
      groupClause = "cre.legal_entity_id, le.code, le.name";
      orderClause = "le.code";
    } else {
      selectClause = `
        cre.group_account_id AS account_id,
        a.code AS account_code,
        a.name AS account_name,
        cre.legal_entity_id AS legal_entity_id,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
      `;
      groupClause =
        "cre.group_account_id, a.code, a.name, cre.legal_entity_id, le.code, le.name";
      orderClause = "a.code, le.code";
    }

    const rowsResult = await query(
      `SELECT
         ${selectClause},
         SUM(cre.local_debit_base) AS local_debit_total,
         SUM(cre.local_credit_base) AS local_credit_total,
         SUM(cre.local_balance_base) AS local_balance_total,
         SUM(cre.translated_debit) AS translated_debit_total,
         SUM(cre.translated_credit) AS translated_credit_total,
         SUM(cre.translated_balance) AS translated_balance_total
       FROM consolidation_run_entries cre
       JOIN accounts a ON a.id = cre.group_account_id
       JOIN legal_entities le ON le.id = cre.legal_entity_id
       WHERE cre.consolidation_run_id = ?
       GROUP BY ${groupClause}
       ORDER BY ${orderClause}`,
      [runId]
    );

    const totalsResult = await query(
      `SELECT
         SUM(local_debit_base) AS local_debit_total,
         SUM(local_credit_base) AS local_credit_total,
         SUM(local_balance_base) AS local_balance_total,
         SUM(translated_debit) AS translated_debit_total,
         SUM(translated_credit) AS translated_credit_total,
         SUM(translated_balance) AS translated_balance_total
       FROM consolidation_run_entries
       WHERE consolidation_run_id = ?`,
      [runId]
    );

    return res.json({
      runId,
      groupBy,
      run: {
        id: run.id,
        consolidationGroupId: run.consolidation_group_id,
        consolidationGroupCode: run.consolidation_group_code,
        consolidationGroupName: run.consolidation_group_name,
        fiscalPeriodId: run.fiscal_period_id,
        periodStartDate: run.period_start_date,
        periodEndDate: run.period_end_date,
        presentationCurrencyCode: run.presentation_currency_code,
        status: run.status,
      },
      totals: {
        localDebitTotal: Number(totalsResult.rows[0]?.local_debit_total || 0),
        localCreditTotal: Number(totalsResult.rows[0]?.local_credit_total || 0),
        localBalanceTotal: Number(totalsResult.rows[0]?.local_balance_total || 0),
        translatedDebitTotal: Number(
          totalsResult.rows[0]?.translated_debit_total || 0
        ),
        translatedCreditTotal: Number(
          totalsResult.rows[0]?.translated_credit_total || 0
        ),
        translatedBalanceTotal: Number(
          totalsResult.rows[0]?.translated_balance_total || 0
        ),
      },
      rows: rowsResult.rows,
    });
  })
);

router.get(
  "/runs/:runId/reports/balance-sheet",
  requirePermission("consolidation.report.balance_sheet.read", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    if (!runId) {
      throw badRequest("runId must be a positive integer");
    }

    const run = await requireRun(tenantId, runId);
    const includeDraft = parseBooleanLike(
      req.query.includeDraft,
      false,
      "includeDraft"
    );
    const includeZero = parseBooleanLike(req.query.includeZero, false, "includeZero");
    const preferredRateType = normalizeRateType(req.query.rateType);

    const reportData = await loadRunReportAccountBalances({
      tenantId,
      run,
      includeDraft,
      preferredRateType,
    });

    const mappedRows = reportData.rows
      .filter((row) => ["ASSET", "LIABILITY", "EQUITY"].includes(row.accountType))
      .map((row) => {
        const normalizedBaseBalance = normalizeBalanceByAccountType(
          row.accountType,
          row.baseBalance
        );
        const normalizedAdjustmentBalance = normalizeBalanceByAccountType(
          row.accountType,
          row.adjustmentBalance
        );
        const normalizedEliminationBalance = normalizeBalanceByAccountType(
          row.accountType,
          row.eliminationBalance
        );
        const normalizedFinalBalance = normalizeBalanceByAccountType(
          row.accountType,
          row.finalBalance
        );

        return {
          accountId: row.accountId,
          accountCode: row.accountCode,
          accountName: row.accountName,
          accountType: row.accountType,
          baseBalance: row.baseBalance,
          adjustmentBalance: row.adjustmentBalance,
          eliminationBalance: row.eliminationBalance,
          finalBalance: row.finalBalance,
          normalizedBaseBalance,
          normalizedAdjustmentBalance,
          normalizedEliminationBalance,
          normalizedFinalBalance,
        };
      })
      .filter(
        (row) => includeZero || Math.abs(Number(row.normalizedFinalBalance || 0)) >= BALANCE_EPSILON
      )
      .sort((a, b) => String(a.accountCode).localeCompare(String(b.accountCode)));

    const assetsTotal = mappedRows
      .filter((row) => row.accountType === "ASSET")
      .reduce((sum, row) => sum + Number(row.normalizedFinalBalance || 0), 0);
    const liabilitiesTotal = mappedRows
      .filter((row) => row.accountType === "LIABILITY")
      .reduce((sum, row) => sum + Number(row.normalizedFinalBalance || 0), 0);
    const equityTotal = mappedRows
      .filter((row) => row.accountType === "EQUITY")
      .reduce((sum, row) => sum + Number(row.normalizedFinalBalance || 0), 0);

    const incomeStatementRows = reportData.rows.filter((row) =>
      ["REVENUE", "EXPENSE"].includes(row.accountType)
    );
    const revenueTotal = incomeStatementRows
      .filter((row) => row.accountType === "REVENUE")
      .reduce(
        (sum, row) =>
          sum + normalizeBalanceByAccountType(row.accountType, row.finalBalance),
        0
      );
    const expenseTotal = incomeStatementRows
      .filter((row) => row.accountType === "EXPENSE")
      .reduce(
        (sum, row) =>
          sum + normalizeBalanceByAccountType(row.accountType, row.finalBalance),
        0
      );
    const currentPeriodEarnings = revenueTotal - expenseTotal;
    const equationDelta =
      assetsTotal - (liabilitiesTotal + equityTotal + currentPeriodEarnings);

    return res.json({
      runId,
      run: {
        id: parsePositiveInt(run.id),
        consolidationGroupId: parsePositiveInt(run.consolidation_group_id),
        consolidationGroupCode: run.consolidation_group_code || null,
        consolidationGroupName: run.consolidation_group_name || null,
        fiscalPeriodId: parsePositiveInt(run.fiscal_period_id),
        periodStartDate: run.period_start_date || null,
        periodEndDate: run.period_end_date || null,
        status: String(run.status || "").toUpperCase(),
        presentationCurrencyCode: String(run.presentation_currency_code || "").toUpperCase(),
      },
      options: {
        includeDraft,
        includeZero,
        rateType: preferredRateType,
        includedStatuses: reportData.statusFilter,
      },
      totals: {
        assetsTotal,
        liabilitiesTotal,
        equityTotal,
        currentPeriodEarnings,
        equationDelta,
      },
      rows: mappedRows,
    });
  })
);

router.get(
  "/runs/:runId/reports/income-statement",
  requirePermission("consolidation.report.income_statement.read", {
    resolveScope: async (req, tenantId) => {
      return resolveRunScope(req.params?.runId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const runId = parsePositiveInt(req.params.runId);
    if (!runId) {
      throw badRequest("runId must be a positive integer");
    }

    const run = await requireRun(tenantId, runId);
    const includeDraft = parseBooleanLike(
      req.query.includeDraft,
      false,
      "includeDraft"
    );
    const includeZero = parseBooleanLike(req.query.includeZero, false, "includeZero");
    const preferredRateType = normalizeRateType(req.query.rateType);

    const reportData = await loadRunReportAccountBalances({
      tenantId,
      run,
      includeDraft,
      preferredRateType,
    });

    const mappedRows = reportData.rows
      .filter((row) => ["REVENUE", "EXPENSE"].includes(row.accountType))
      .map((row) => {
        const normalizedBaseBalance = normalizeBalanceByAccountType(
          row.accountType,
          row.baseBalance
        );
        const normalizedAdjustmentBalance = normalizeBalanceByAccountType(
          row.accountType,
          row.adjustmentBalance
        );
        const normalizedEliminationBalance = normalizeBalanceByAccountType(
          row.accountType,
          row.eliminationBalance
        );
        const normalizedFinalBalance = normalizeBalanceByAccountType(
          row.accountType,
          row.finalBalance
        );

        return {
          accountId: row.accountId,
          accountCode: row.accountCode,
          accountName: row.accountName,
          accountType: row.accountType,
          baseBalance: row.baseBalance,
          adjustmentBalance: row.adjustmentBalance,
          eliminationBalance: row.eliminationBalance,
          finalBalance: row.finalBalance,
          normalizedBaseBalance,
          normalizedAdjustmentBalance,
          normalizedEliminationBalance,
          normalizedFinalBalance,
        };
      })
      .filter(
        (row) => includeZero || Math.abs(Number(row.normalizedFinalBalance || 0)) >= BALANCE_EPSILON
      )
      .sort((a, b) => String(a.accountCode).localeCompare(String(b.accountCode)));

    const revenueTotal = mappedRows
      .filter((row) => row.accountType === "REVENUE")
      .reduce((sum, row) => sum + Number(row.normalizedFinalBalance || 0), 0);
    const expenseTotal = mappedRows
      .filter((row) => row.accountType === "EXPENSE")
      .reduce((sum, row) => sum + Number(row.normalizedFinalBalance || 0), 0);
    const netIncome = revenueTotal - expenseTotal;

    return res.json({
      runId,
      run: {
        id: parsePositiveInt(run.id),
        consolidationGroupId: parsePositiveInt(run.consolidation_group_id),
        consolidationGroupCode: run.consolidation_group_code || null,
        consolidationGroupName: run.consolidation_group_name || null,
        fiscalPeriodId: parsePositiveInt(run.fiscal_period_id),
        periodStartDate: run.period_start_date || null,
        periodEndDate: run.period_end_date || null,
        status: String(run.status || "").toUpperCase(),
        presentationCurrencyCode: String(run.presentation_currency_code || "").toUpperCase(),
      },
      options: {
        includeDraft,
        includeZero,
        rateType: preferredRateType,
        includedStatuses: reportData.statusFilter,
      },
      totals: {
        revenueTotal,
        expenseTotal,
        netIncome,
      },
      rows: mappedRows,
    });
  })
);

export default router;
