import express from "express";
import { query } from "../db.js";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import {
  assertAccountBelongsToTenant,
  assertLegalEntityBelongsToTenant,
} from "../tenantGuards.js";
import {
  asyncHandler,
  assertRequiredFields,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";

const router = express.Router();
const BALANCE_EPSILON = 0.0001;

function parseBooleanLike(value, fallback = false) {
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

  throw badRequest("Boolean flag values must be true/false");
}

function buildComplianceBaseConditions(req, tenantId, options, params) {
  params.push(tenantId);

  const conditions = ["je.tenant_id = ?"];
  if (options.includeDraft) {
    conditions.push("je.status IN ('DRAFT', 'POSTED')");
  } else {
    conditions.push("je.status = 'POSTED'");
  }

  conditions.push(buildScopeFilter(req, "legal_entity", "je.legal_entity_id", params));

  if (options.legalEntityId) {
    conditions.push("je.legal_entity_id = ?");
    params.push(options.legalEntityId);
  }
  if (options.fiscalPeriodId) {
    conditions.push("je.fiscal_period_id = ?");
    params.push(options.fiscalPeriodId);
  }

  return conditions;
}

function normalizeComplianceIssueRows(rows = [], issueCode, suggestedActions = []) {
  return rows.map((row) => ({
    issueCode,
    issueMessage: row.issue_message || "",
    suggestedActions,
    journalId: parsePositiveInt(row.journal_id),
    journalNo: row.journal_no || null,
    journalStatus: String(row.journal_status || "").toUpperCase(),
    sourceType: String(row.source_type || "").toUpperCase(),
    fiscalPeriodId: parsePositiveInt(row.fiscal_period_id),
    entryDate: row.entry_date || null,
    fromLegalEntityId: parsePositiveInt(row.from_legal_entity_id),
    fromLegalEntityCode: row.from_legal_entity_code || null,
    toLegalEntityId: parsePositiveInt(row.to_legal_entity_id),
    toLegalEntityCode: row.to_legal_entity_code || null,
    lineNo: Number(row.line_no || 0),
    accountId: parsePositiveInt(row.account_id),
    accountCode: row.account_code || null,
    accountName: row.account_name || null,
  }));
}

function summarizeComplianceIssues(rows = []) {
  const summary = {
    totalIssues: rows.length,
    byIssueCode: {},
  };

  for (const row of rows) {
    const issueCode = String(row.issueCode || "UNKNOWN");
    summary.byIssueCode[issueCode] = (summary.byIssueCode[issueCode] || 0) + 1;
  }

  return summary;
}

router.get(
  "/entity-flags",
  requirePermission("intercompany.flag.read", {
    resolveScope: (req) => {
      const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const legalEntityId = parsePositiveInt(req.query.legalEntityId);
    if (legalEntityId) {
      assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
    }

    const conditions = ["tenant_id = ?"];
    const params = [tenantId];
    conditions.push(buildScopeFilter(req, "legal_entity", "id", params));

    if (legalEntityId) {
      conditions.push("id = ?");
      params.push(legalEntityId);
    }

    const result = await query(
      `SELECT
         id AS legal_entity_id,
         code,
         name,
         is_intercompany_enabled,
         intercompany_partner_required,
         status
       FROM legal_entities
       WHERE ${conditions.join(" AND ")}
       ORDER BY id`,
      params
    );

    return res.json({
      tenantId,
      rows: result.rows,
    });
  })
);

router.patch(
  "/entity-flags/:legalEntityId",
  requirePermission("intercompany.flag.upsert", {
    resolveScope: (req, tenantId) => {
      const legalEntityId = parsePositiveInt(req.params?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return { scopeType: "TENANT", scopeId: tenantId };
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const legalEntityId = parsePositiveInt(req.params.legalEntityId);
    if (!legalEntityId) {
      throw badRequest("legalEntityId must be a positive integer");
    }
    await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
    assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");

    const isIntercompanyEnabled =
      req.body?.isIntercompanyEnabled === undefined
        ? null
        : Boolean(req.body.isIntercompanyEnabled);
    const intercompanyPartnerRequired =
      req.body?.intercompanyPartnerRequired === undefined
        ? null
        : Boolean(req.body.intercompanyPartnerRequired);

    if (isIntercompanyEnabled === null && intercompanyPartnerRequired === null) {
      throw badRequest(
        "Provide isIntercompanyEnabled and/or intercompanyPartnerRequired"
      );
    }

    await query(
      `UPDATE legal_entities
       SET
         is_intercompany_enabled = COALESCE(?, is_intercompany_enabled),
         intercompany_partner_required = COALESCE(?, intercompany_partner_required)
       WHERE tenant_id = ?
         AND id = ?`,
      [isIntercompanyEnabled, intercompanyPartnerRequired, tenantId, legalEntityId]
    );

    const result = await query(
      `SELECT
         id AS legal_entity_id,
         code,
         name,
         is_intercompany_enabled,
         intercompany_partner_required
       FROM legal_entities
       WHERE tenant_id = ?
         AND id = ?
       LIMIT 1`,
      [tenantId, legalEntityId]
    );

    return res.json({
      ok: true,
      row: result.rows[0] || null,
    });
  })
);

router.get(
  "/compliance-issues",
  requirePermission("intercompany.flag.read", {
    resolveScope: (req) => {
      const legalEntityId = parsePositiveInt(req.query?.legalEntityId);
      if (legalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
      }
      return null;
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const legalEntityId = parsePositiveInt(req.query.legalEntityId);
    const fiscalPeriodId = parsePositiveInt(req.query.fiscalPeriodId);
    const includeDraft = parseBooleanLike(req.query.includeDraft, true);
    const limitRaw = Number(req.query.limit);
    const limit = Number.isInteger(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 500) : 200;
    const perIssueLimit = Math.max(50, Math.min(limit * 2, 1000));

    if (legalEntityId) {
      await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
      assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
    }

    const queryOptions = {
      legalEntityId,
      fiscalPeriodId,
      includeDraft,
    };

    const disabledParams = [];
    const disabledConditions = buildComplianceBaseConditions(
      req,
      tenantId,
      queryOptions,
      disabledParams
    );
    disabledConditions.push("jl.counterparty_legal_entity_id IS NOT NULL");
    disabledConditions.push(
      buildScopeFilter(req, "legal_entity", "jl.counterparty_legal_entity_id", disabledParams)
    );
    disabledConditions.push("le.is_intercompany_enabled = FALSE");

    const missingPartnerParams = [];
    const missingPartnerConditions = buildComplianceBaseConditions(
      req,
      tenantId,
      queryOptions,
      missingPartnerParams
    );
    missingPartnerConditions.push("je.source_type = 'INTERCOMPANY'");
    missingPartnerConditions.push("le.intercompany_partner_required = TRUE");
    missingPartnerConditions.push("jl.counterparty_legal_entity_id IS NULL");

    const missingPairParams = [];
    const missingPairConditions = buildComplianceBaseConditions(
      req,
      tenantId,
      queryOptions,
      missingPairParams
    );
    missingPairConditions.push("jl.counterparty_legal_entity_id IS NOT NULL");
    missingPairConditions.push(
      buildScopeFilter(req, "legal_entity", "jl.counterparty_legal_entity_id", missingPairParams)
    );
    missingPairConditions.push("le.is_intercompany_enabled = TRUE");
    missingPairConditions.push("icp.id IS NULL");

    const [disabledRows, missingPartnerRows, missingPairRows] = await Promise.all([
      query(
        `SELECT
           'Selected legal entity has intercompany disabled but line has counterparty.' AS issue_message,
           je.id AS journal_id,
           je.journal_no,
           je.status AS journal_status,
           je.source_type,
           je.fiscal_period_id,
           je.entry_date,
           je.legal_entity_id AS from_legal_entity_id,
           le.code AS from_legal_entity_code,
           jl.counterparty_legal_entity_id AS to_legal_entity_id,
           cle.code AS to_legal_entity_code,
           jl.line_no,
           jl.account_id,
           a.code AS account_code,
           a.name AS account_name
         FROM journal_entries je
         JOIN legal_entities le ON le.id = je.legal_entity_id
         JOIN journal_lines jl ON jl.journal_entry_id = je.id
         JOIN accounts a ON a.id = jl.account_id
         LEFT JOIN legal_entities cle ON cle.id = jl.counterparty_legal_entity_id
         WHERE ${disabledConditions.join(" AND ")}
         ORDER BY je.entry_date DESC, je.id DESC, jl.line_no ASC
         LIMIT ${perIssueLimit}`,
        disabledParams
      ),
      query(
        `SELECT
           'Entity requires intercompany partner on INTERCOMPANY journals, but line has no counterparty.' AS issue_message,
           je.id AS journal_id,
           je.journal_no,
           je.status AS journal_status,
           je.source_type,
           je.fiscal_period_id,
           je.entry_date,
           je.legal_entity_id AS from_legal_entity_id,
           le.code AS from_legal_entity_code,
           NULL AS to_legal_entity_id,
           NULL AS to_legal_entity_code,
           jl.line_no,
           jl.account_id,
           a.code AS account_code,
           a.name AS account_name
         FROM journal_entries je
         JOIN legal_entities le ON le.id = je.legal_entity_id
         JOIN journal_lines jl ON jl.journal_entry_id = je.id
         JOIN accounts a ON a.id = jl.account_id
         WHERE ${missingPartnerConditions.join(" AND ")}
         ORDER BY je.entry_date DESC, je.id DESC, jl.line_no ASC
         LIMIT ${perIssueLimit}`,
        missingPartnerParams
      ),
      query(
        `SELECT
           'No active intercompany pair mapping found for source/counterparty.' AS issue_message,
           je.id AS journal_id,
           je.journal_no,
           je.status AS journal_status,
           je.source_type,
           je.fiscal_period_id,
           je.entry_date,
           je.legal_entity_id AS from_legal_entity_id,
           le.code AS from_legal_entity_code,
           jl.counterparty_legal_entity_id AS to_legal_entity_id,
           cle.code AS to_legal_entity_code,
           jl.line_no,
           jl.account_id,
           a.code AS account_code,
           a.name AS account_name
         FROM journal_entries je
         JOIN legal_entities le ON le.id = je.legal_entity_id
         JOIN journal_lines jl ON jl.journal_entry_id = je.id
         JOIN accounts a ON a.id = jl.account_id
         LEFT JOIN legal_entities cle ON cle.id = jl.counterparty_legal_entity_id
         LEFT JOIN intercompany_pairs icp ON icp.tenant_id = je.tenant_id
           AND icp.from_legal_entity_id = je.legal_entity_id
           AND icp.to_legal_entity_id = jl.counterparty_legal_entity_id
           AND icp.status = 'ACTIVE'
         WHERE ${missingPairConditions.join(" AND ")}
         ORDER BY je.entry_date DESC, je.id DESC, jl.line_no ASC
         LIMIT ${perIssueLimit}`,
        missingPairParams
      ),
    ]);

    const rows = [
      ...normalizeComplianceIssueRows(
        disabledRows.rows,
        "ENTITY_INTERCOMPANY_DISABLED",
        ["ENABLE_ENTITY_INTERCOMPANY"]
      ),
      ...normalizeComplianceIssueRows(
        missingPartnerRows.rows,
        "PARTNER_REQUIRED_MISSING_COUNTERPARTY",
        ["SET_COUNTERPARTY_ON_LINES", "DISABLE_PARTNER_REQUIRED"]
      ),
      ...normalizeComplianceIssueRows(
        missingPairRows.rows,
        "MISSING_ACTIVE_PAIR",
        ["CREATE_ACTIVE_PAIR"]
      ),
    ]
      .sort((a, b) => {
        const dateA = String(a.entryDate || "");
        const dateB = String(b.entryDate || "");
        if (dateA !== dateB) {
          return dateA < dateB ? 1 : -1;
        }

        const journalA = Number(a.journalId || 0);
        const journalB = Number(b.journalId || 0);
        if (journalA !== journalB) {
          return journalB - journalA;
        }

        return Number(a.lineNo || 0) - Number(b.lineNo || 0);
      })
      .slice(0, limit);

    return res.json({
      tenantId,
      includeDraft,
      legalEntityId: legalEntityId || null,
      fiscalPeriodId: fiscalPeriodId || null,
      limit,
      summary: summarizeComplianceIssues(rows),
      rows,
    });
  })
);

router.post(
  "/pairs",
  requirePermission("intercompany.pair.upsert", {
    resolveScope: (req, tenantId) => {
      const fromLegalEntityId = parsePositiveInt(req.body?.fromLegalEntityId);
      if (fromLegalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: fromLegalEntityId };
      }
      return { scopeType: "TENANT", scopeId: tenantId };
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    assertRequiredFields(req.body, ["fromLegalEntityId", "toLegalEntityId"]);
    const fromLegalEntityId = parsePositiveInt(req.body.fromLegalEntityId);
    const toLegalEntityId = parsePositiveInt(req.body.toLegalEntityId);

    if (!fromLegalEntityId || !toLegalEntityId) {
      throw badRequest("fromLegalEntityId and toLegalEntityId must be positive integers");
    }

    await assertLegalEntityBelongsToTenant(tenantId, fromLegalEntityId, "fromLegalEntityId");
    await assertLegalEntityBelongsToTenant(tenantId, toLegalEntityId, "toLegalEntityId");

    assertScopeAccess(req, "legal_entity", fromLegalEntityId, "fromLegalEntityId");
    assertScopeAccess(req, "legal_entity", toLegalEntityId, "toLegalEntityId");

    const receivableAccountId = req.body.receivableAccountId
      ? parsePositiveInt(req.body.receivableAccountId)
      : null;
    const payableAccountId = req.body.payableAccountId
      ? parsePositiveInt(req.body.payableAccountId)
      : null;

    if (receivableAccountId) {
      await assertAccountBelongsToTenant(tenantId, receivableAccountId, "receivableAccountId");
    }
    if (payableAccountId) {
      await assertAccountBelongsToTenant(tenantId, payableAccountId, "payableAccountId");
    }

    const status = String(req.body.status || "ACTIVE").toUpperCase();

    const result = await query(
      `INSERT INTO intercompany_pairs (
          tenant_id, from_legal_entity_id, to_legal_entity_id,
          receivable_account_id, payable_account_id, status
       )
       VALUES (?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         receivable_account_id = VALUES(receivable_account_id),
         payable_account_id = VALUES(payable_account_id),
         status = VALUES(status)`,
      [
        tenantId,
        fromLegalEntityId,
        toLegalEntityId,
        receivableAccountId,
        payableAccountId,
        status,
      ]
    );

    return res.status(201).json({
      ok: true,
      id: result.rows.insertId || null,
      tenantId,
    });
  })
);

router.post(
  "/reconcile",
  requirePermission("intercompany.reconcile.run", {
    resolveScope: (req, tenantId) => {
      const fromLegalEntityId = parsePositiveInt(req.body?.fromLegalEntityId);
      if (fromLegalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: fromLegalEntityId };
      }

      const toLegalEntityId = parsePositiveInt(req.body?.toLegalEntityId);
      if (toLegalEntityId) {
        return { scopeType: "LEGAL_ENTITY", scopeId: toLegalEntityId };
      }

      return { scopeType: "TENANT", scopeId: tenantId };
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    assertRequiredFields(req.body, ["fiscalPeriodId"]);
    const fiscalPeriodId = parsePositiveInt(req.body.fiscalPeriodId);
    if (!fiscalPeriodId) {
      throw badRequest("fiscalPeriodId must be a positive integer");
    }

    const fromLegalEntityId = parsePositiveInt(req.body?.fromLegalEntityId);
    const toLegalEntityId = parsePositiveInt(req.body?.toLegalEntityId);
    if (fromLegalEntityId) {
      await assertLegalEntityBelongsToTenant(tenantId, fromLegalEntityId, "fromLegalEntityId");
      assertScopeAccess(req, "legal_entity", fromLegalEntityId, "fromLegalEntityId");
    }
    if (toLegalEntityId) {
      await assertLegalEntityBelongsToTenant(tenantId, toLegalEntityId, "toLegalEntityId");
      assertScopeAccess(req, "legal_entity", toLegalEntityId, "toLegalEntityId");
    }

    const tolerance = Number(req.body?.tolerance ?? 0.01);
    if (!Number.isFinite(tolerance) || tolerance < 0) {
      throw badRequest("tolerance must be a non-negative number");
    }

    const includeMatched = parseBooleanLike(req.body?.includeMatched, false);
    const includeAccountBreakdown = parseBooleanLike(
      req.body?.includeAccountBreakdown,
      true
    );

    const periodResult = await query(
      `SELECT id, fiscal_year, period_no, period_name, start_date, end_date
       FROM fiscal_periods
       WHERE id = ?
       LIMIT 1`,
      [fiscalPeriodId]
    );
    const period = periodResult.rows[0];
    if (!period) {
      throw badRequest("fiscalPeriodId not found");
    }

    const params = [tenantId, fiscalPeriodId];
    const conditions = [
      "je.tenant_id = ?",
      "je.fiscal_period_id = ?",
      "je.status = 'POSTED'",
      "jl.counterparty_legal_entity_id IS NOT NULL",
    ];
    conditions.push(buildScopeFilter(req, "legal_entity", "je.legal_entity_id", params));
    conditions.push(
      buildScopeFilter(req, "legal_entity", "jl.counterparty_legal_entity_id", params)
    );

    if (fromLegalEntityId && toLegalEntityId) {
      conditions.push(
        "((je.legal_entity_id = ? AND jl.counterparty_legal_entity_id = ?) OR (je.legal_entity_id = ? AND jl.counterparty_legal_entity_id = ?))"
      );
      params.push(fromLegalEntityId, toLegalEntityId, toLegalEntityId, fromLegalEntityId);
    } else if (fromLegalEntityId) {
      conditions.push("je.legal_entity_id = ?");
      params.push(fromLegalEntityId);
    } else if (toLegalEntityId) {
      conditions.push("jl.counterparty_legal_entity_id = ?");
      params.push(toLegalEntityId);
    }

    const directionalResult = await query(
      `SELECT
         je.legal_entity_id AS from_legal_entity_id,
         jl.counterparty_legal_entity_id AS to_legal_entity_id,
         SUM(jl.debit_base) AS debit_total,
         SUM(jl.credit_base) AS credit_total,
         SUM(jl.debit_base - jl.credit_base) AS net_base,
         COUNT(*) AS line_count,
         COUNT(DISTINCT je.id) AS journal_count
       FROM journal_entries je
       JOIN journal_lines jl ON jl.journal_entry_id = je.id
       LEFT JOIN intercompany_pairs icp ON icp.tenant_id = je.tenant_id
         AND icp.from_legal_entity_id = je.legal_entity_id
         AND icp.to_legal_entity_id = jl.counterparty_legal_entity_id
         AND icp.status = 'ACTIVE'
       WHERE ${conditions.join(" AND ")}
         AND (
           icp.id IS NULL OR
           jl.account_id = icp.receivable_account_id OR
           jl.account_id = icp.payable_account_id
         )
       GROUP BY je.legal_entity_id, jl.counterparty_legal_entity_id
       ORDER BY je.legal_entity_id, jl.counterparty_legal_entity_id`,
      params
    );

    const pairMap = new Map();
    const legalEntityIds = new Set();

    for (const row of directionalResult.rows || []) {
      const sourceId = parsePositiveInt(row.from_legal_entity_id);
      const counterpartyId = parsePositiveInt(row.to_legal_entity_id);
      if (!sourceId || !counterpartyId || sourceId === counterpartyId) {
        continue;
      }

      const entityAId = Math.min(sourceId, counterpartyId);
      const entityBId = Math.max(sourceId, counterpartyId);
      const key = `${entityAId}:${entityBId}`;

      if (!pairMap.has(key)) {
        pairMap.set(key, {
          entityAId,
          entityBId,
          directionAB: {
            fromLegalEntityId: entityAId,
            toLegalEntityId: entityBId,
            debitTotal: 0,
            creditTotal: 0,
            netBase: 0,
            lineCount: 0,
            journalCount: 0,
          },
          directionBA: {
            fromLegalEntityId: entityBId,
            toLegalEntityId: entityAId,
            debitTotal: 0,
            creditTotal: 0,
            netBase: 0,
            lineCount: 0,
            journalCount: 0,
          },
        });
      }

      const pair = pairMap.get(key);
      const target = sourceId === entityAId ? pair.directionAB : pair.directionBA;
      target.debitTotal += Number(row.debit_total || 0);
      target.creditTotal += Number(row.credit_total || 0);
      target.netBase += Number(row.net_base || 0);
      target.lineCount += Number(row.line_count || 0);
      target.journalCount += Number(row.journal_count || 0);

      legalEntityIds.add(entityAId);
      legalEntityIds.add(entityBId);
    }

    const legalEntityMap = new Map();
    if (legalEntityIds.size > 0) {
      const ids = Array.from(legalEntityIds);
      const placeholders = ids.map(() => "?").join(", ");
      const entityResult = await query(
        `SELECT id, code, name
         FROM legal_entities
         WHERE tenant_id = ?
           AND id IN (${placeholders})`,
        [tenantId, ...ids]
      );

      for (const row of entityResult.rows || []) {
        legalEntityMap.set(parsePositiveInt(row.id), {
          id: parsePositiveInt(row.id),
          code: String(row.code || ""),
          name: String(row.name || ""),
        });
      }
    }

    const breakdownByPair = new Map();
    if (includeAccountBreakdown && pairMap.size > 0) {
      const accountResult = await query(
        `SELECT
           je.legal_entity_id AS from_legal_entity_id,
           jl.counterparty_legal_entity_id AS to_legal_entity_id,
           jl.account_id,
           a.code AS account_code,
           a.name AS account_name,
           SUM(jl.debit_base) AS debit_total,
           SUM(jl.credit_base) AS credit_total,
           SUM(jl.debit_base - jl.credit_base) AS net_base,
           COUNT(*) AS line_count
         FROM journal_entries je
         JOIN journal_lines jl ON jl.journal_entry_id = je.id
         JOIN accounts a ON a.id = jl.account_id
         LEFT JOIN intercompany_pairs icp ON icp.tenant_id = je.tenant_id
           AND icp.from_legal_entity_id = je.legal_entity_id
           AND icp.to_legal_entity_id = jl.counterparty_legal_entity_id
           AND icp.status = 'ACTIVE'
         WHERE ${conditions.join(" AND ")}
           AND (
             icp.id IS NULL OR
             jl.account_id = icp.receivable_account_id OR
             jl.account_id = icp.payable_account_id
           )
         GROUP BY
           je.legal_entity_id,
           jl.counterparty_legal_entity_id,
           jl.account_id,
           a.code,
           a.name
         ORDER BY a.code, je.legal_entity_id, jl.counterparty_legal_entity_id`,
        params
      );

      for (const row of accountResult.rows || []) {
        const sourceId = parsePositiveInt(row.from_legal_entity_id);
        const counterpartyId = parsePositiveInt(row.to_legal_entity_id);
        const accountId = parsePositiveInt(row.account_id);
        if (!sourceId || !counterpartyId || !accountId || sourceId === counterpartyId) {
          continue;
        }

        const entityAId = Math.min(sourceId, counterpartyId);
        const entityBId = Math.max(sourceId, counterpartyId);
        const pairKey = `${entityAId}:${entityBId}`;
        const accountKey = `${pairKey}:${accountId}`;

        if (!breakdownByPair.has(accountKey)) {
          breakdownByPair.set(accountKey, {
            pairKey,
            accountId,
            accountCode: String(row.account_code || ""),
            accountName: String(row.account_name || ""),
            directionABNetBase: 0,
            directionBANetBase: 0,
            directionABLineCount: 0,
            directionBALineCount: 0,
          });
        }

        const breakdown = breakdownByPair.get(accountKey);
        const netBase = Number(row.net_base || 0);
        const lineCount = Number(row.line_count || 0);
        if (sourceId === entityAId) {
          breakdown.directionABNetBase += netBase;
          breakdown.directionABLineCount += lineCount;
        } else {
          breakdown.directionBANetBase += netBase;
          breakdown.directionBALineCount += lineCount;
        }
      }
    }

    const rows = [];
    for (const pair of pairMap.values()) {
      const differenceBase = pair.directionAB.netBase + pair.directionBA.netBase;
      const absoluteDifferenceBase = Math.abs(differenceBase);
      const hasAB = pair.directionAB.lineCount > 0;
      const hasBA = pair.directionBA.lineCount > 0;

      let status = "MISMATCHED";
      if (!hasAB || !hasBA) {
        status = "UNILATERAL";
      } else if (absoluteDifferenceBase <= tolerance + BALANCE_EPSILON) {
        status = "MATCHED";
      }

      if (!includeMatched && status === "MATCHED") {
        continue;
      }

      const pairKey = `${pair.entityAId}:${pair.entityBId}`;
      const accountBreakdown = includeAccountBreakdown
        ? Array.from(breakdownByPair.values())
            .filter((row) => row.pairKey === pairKey)
            .map((row) => {
              const accountDiff = row.directionABNetBase + row.directionBANetBase;
              const accountAbsDiff = Math.abs(accountDiff);
              const accountHasAB = row.directionABLineCount > 0;
              const accountHasBA = row.directionBALineCount > 0;
              let accountStatus = "MISMATCHED";
              if (!accountHasAB || !accountHasBA) {
                accountStatus = "UNILATERAL";
              } else if (accountAbsDiff <= tolerance + BALANCE_EPSILON) {
                accountStatus = "MATCHED";
              }

              return {
                accountId: row.accountId,
                accountCode: row.accountCode,
                accountName: row.accountName,
                directionABNetBase: row.directionABNetBase,
                directionBANetBase: row.directionBANetBase,
                differenceBase: accountDiff,
                absoluteDifferenceBase: accountAbsDiff,
                status: accountStatus,
              };
            })
            .sort(
              (a, b) =>
                Math.abs(Number(b.absoluteDifferenceBase || 0)) -
                Math.abs(Number(a.absoluteDifferenceBase || 0))
            )
        : [];

      rows.push({
        entityA: legalEntityMap.get(pair.entityAId) || {
          id: pair.entityAId,
          code: `LE-${pair.entityAId}`,
          name: null,
        },
        entityB: legalEntityMap.get(pair.entityBId) || {
          id: pair.entityBId,
          code: `LE-${pair.entityBId}`,
          name: null,
        },
        directionAB: pair.directionAB,
        directionBA: pair.directionBA,
        differenceBase,
        absoluteDifferenceBase,
        status,
        accountBreakdown,
      });
    }

    rows.sort(
      (a, b) =>
        Math.abs(Number(b.absoluteDifferenceBase || 0)) -
        Math.abs(Number(a.absoluteDifferenceBase || 0))
    );

    const summary = rows.reduce(
      (acc, row) => {
        acc.pairCount += 1;
        acc.totalAbsoluteDifferenceBase += Number(row.absoluteDifferenceBase || 0);
        if (row.status === "MATCHED") {
          acc.matchedPairCount += 1;
        } else if (row.status === "UNILATERAL") {
          acc.unilateralPairCount += 1;
        } else {
          acc.mismatchedPairCount += 1;
        }
        return acc;
      },
      {
        pairCount: 0,
        matchedPairCount: 0,
        mismatchedPairCount: 0,
        unilateralPairCount: 0,
        totalAbsoluteDifferenceBase: 0,
      }
    );

    return res.json({
      tenantId,
      fiscalPeriodId,
      period: {
        id: parsePositiveInt(period.id),
        fiscalYear: Number(period.fiscal_year || 0),
        periodNo: Number(period.period_no || 0),
        periodName: String(period.period_name || ""),
        startDate: period.start_date || null,
        endDate: period.end_date || null,
      },
      tolerance,
      includeMatched,
      includeAccountBreakdown,
      summary,
      rows,
    });
  })
);

export default router;
