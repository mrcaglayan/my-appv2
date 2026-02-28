import { query } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { evaluateBankApprovalNeed } from "./bank.governance.service.js";
import { submitBankApprovalRequest } from "./bank.approvals.service.js";

function u(value) {
  return String(value || "").trim().toUpperCase();
}

function parseJson(value, fallback = null) {
  if (value === undefined || value === null) return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function hydrateRuleRow(row) {
  if (!row) return null;
  return {
    ...row,
    conditions_json: parseJson(row.conditions_json, {}),
    action_payload_json: parseJson(row.action_payload_json, {}),
  };
}

function getAllowedLegalEntityIdsFromReq(req) {
  const ids = Array.from(req?.rbac?.scopeContext?.legalEntities || []);
  return ids.map((id) => parsePositiveInt(id)).filter(Boolean);
}

function buildRuleScopeWhere(req, alias, params) {
  const tenantWide = Boolean(req?.rbac?.scopeContext?.tenantWide);
  if (tenantWide) {
    return "1 = 1";
  }
  const allowedLegalEntityIds = getAllowedLegalEntityIdsFromReq(req);
  if (allowedLegalEntityIds.length === 0) {
    return `${alias}.scope_type = 'GLOBAL'`;
  }
  params.push(...allowedLegalEntityIds);
  return `(${alias}.scope_type = 'GLOBAL' OR ${alias}.legal_entity_id IN (${allowedLegalEntityIds
    .map(() => "?")
    .join(", ")}))`;
}

async function getRuleRowById({ tenantId, ruleId, runQuery = query }) {
  const result = await runQuery(
    `SELECT r.*
     FROM bank_reconciliation_rules r
     WHERE r.tenant_id = ?
       AND r.id = ?
     LIMIT 1`,
    [tenantId, ruleId]
  );
  return hydrateRuleRow(result.rows?.[0] || null);
}

async function assertLegalEntityExistsForRule({ tenantId, legalEntityId }) {
  if (!legalEntityId) return;
  const result = await query(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId]
  );
  if (!result.rows?.[0]) {
    throw badRequest("legalEntityId not found");
  }
}

async function loadBankAccountRuleScope({ tenantId, bankAccountId }) {
  if (!bankAccountId) return null;
  const result = await query(
    `SELECT id, legal_entity_id
     FROM bank_accounts
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, bankAccountId]
  );
  return result.rows?.[0] || null;
}

function normalizeRuleScopeForWrite(input, current = null) {
  const next = {
    scopeType: input.scopeType ?? current?.scope_type ?? "GLOBAL",
    legalEntityId:
      input.legalEntityId !== undefined ? input.legalEntityId : current?.legal_entity_id ?? null,
    bankAccountId:
      input.bankAccountId !== undefined ? input.bankAccountId : current?.bank_account_id ?? null,
  };

  if (next.scopeType === "GLOBAL") {
    next.legalEntityId = null;
    next.bankAccountId = null;
  } else if (next.scopeType === "LEGAL_ENTITY") {
    next.bankAccountId = null;
    if (!next.legalEntityId) {
      throw badRequest("legalEntityId is required for LEGAL_ENTITY scope");
    }
  } else if (next.scopeType === "BANK_ACCOUNT") {
    if (!next.bankAccountId) {
      throw badRequest("bankAccountId is required for BANK_ACCOUNT scope");
    }
  } else {
    throw badRequest("scopeType is invalid");
  }
  return next;
}

async function maybeStageRuleChangeApproval({
  req,
  tenantId,
  userId,
  row,
  actionType,
  assertScopeAccess,
}) {
  if (!row) return { row, approval_required: false };
  const legalEntityId = parsePositiveInt(row.legal_entity_id) || null;
  const bankAccountId = parsePositiveInt(row.bank_account_id) || null;
  if (legalEntityId) {
    assertScopeAccess(req, "legal_entity", legalEntityId, "ruleId");
  }

  const gov = await evaluateBankApprovalNeed({
    tenantId,
    targetType: "RECON_RULE",
    actionType,
    legalEntityId,
    bankAccountId,
    thresholdAmount: null,
    currencyCode: null,
  });
  if (!gov?.approvalRequired && !gov?.approval_required) {
    return { row, approval_required: false };
  }

  const submitRes = await submitBankApprovalRequest({
    tenantId,
    userId,
    requestInput: {
      requestKey: `B09:RULE:${tenantId}:${row.id}:${u(actionType)}:v${Number(row.version_no || 1)}:${String(
        row.updated_at || ""
      )}`,
      targetType: "RECON_RULE",
      targetId: row.id,
      actionType: u(actionType),
      legalEntityId,
      bankAccountId,
      actionPayload: {
        ruleId: row.id,
      },
    },
    snapshotBuilder: async () => ({
      rule_id: row.id,
      rule_code: row.rule_code,
      rule_name: row.rule_name,
      status: row.status,
      approval_state: row.approval_state || "APPROVED",
      version_no: Number(row.version_no || 1),
      scope_type: row.scope_type,
      legal_entity_id: legalEntityId,
      bank_account_id: bankAccountId,
      match_type: row.match_type,
      action_type: row.action_type,
      conditions_json: row.conditions_json || {},
      action_payload_json: row.action_payload_json || {},
    }),
    policyOverride: gov,
  });

  const approvalRequestId = parsePositiveInt(submitRes?.item?.id) || null;
  if (!approvalRequestId) {
    return { row, approval_required: false };
  }

  await query(
    `UPDATE bank_reconciliation_rules
     SET approval_state = 'PENDING_APPROVAL',
         approval_request_id = ?,
         status = CASE WHEN status = 'ACTIVE' THEN 'PAUSED' ELSE status END,
         version_no = CASE WHEN ? = 'UPDATE' THEN version_no + 1 ELSE version_no END,
         updated_by_user_id = COALESCE(?, updated_by_user_id)
     WHERE tenant_id = ?
       AND id = ?`,
    [approvalRequestId, u(actionType), userId || null, tenantId, row.id]
  );

  const staged = await getRuleRowById({ tenantId, ruleId: row.id });
  return {
    row: staged,
    approval_required: true,
    approval_request: submitRes.item,
    idempotent: Boolean(submitRes?.idempotent),
  };
}

export async function listReconciliationRuleRows({
  req,
  tenantId,
  filters,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["r.tenant_id = ?"];
  conditions.push(buildRuleScopeWhere(req, "r", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("(r.legal_entity_id = ? OR r.scope_type = 'GLOBAL')");
    params.push(filters.legalEntityId);
  }

  if (filters.bankAccountId) {
    const bankAccount = await loadBankAccountRuleScope({
      tenantId,
      bankAccountId: filters.bankAccountId,
    });
    if (!bankAccount) {
      throw badRequest("bankAccountId not found");
    }
    assertScopeAccess(req, "legal_entity", bankAccount.legal_entity_id, "bankAccountId");
    if (
      filters.legalEntityId &&
      parsePositiveInt(filters.legalEntityId) !== parsePositiveInt(bankAccount.legal_entity_id)
    ) {
      throw badRequest("bankAccountId does not belong to legalEntityId");
    }
    conditions.push("(r.bank_account_id = ? OR r.scope_type = 'GLOBAL')");
    params.push(filters.bankAccountId);
  }

  if (filters.status) {
    conditions.push("r.status = ?");
    params.push(filters.status);
  }

  if (filters.q) {
    const like = `%${filters.q}%`;
    conditions.push("(r.rule_code LIKE ? OR r.rule_name LIKE ?)");
    params.push(like, like);
  }

  const whereSql = conditions.join(" AND ");
  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM bank_reconciliation_rules r
     WHERE ${whereSql}`,
    params
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset = Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const listResult = await query(
    `SELECT r.*
     FROM bank_reconciliation_rules r
     WHERE ${whereSql}
     ORDER BY r.priority ASC, r.id ASC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: (listResult.rows || []).map(hydrateRuleRow),
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getReconciliationRuleById({
  req,
  tenantId,
  ruleId,
  assertScopeAccess,
}) {
  const row = await getRuleRowById({ tenantId, ruleId });
  if (!row) {
    throw badRequest("Reconciliation rule not found");
  }
  if (row.legal_entity_id) {
    assertScopeAccess(req, "legal_entity", row.legal_entity_id, "ruleId");
  }
  return row;
}

export async function createReconciliationRule({
  req,
  input,
  assertScopeAccess,
}) {
  const scope = normalizeRuleScopeForWrite(input);
  if (scope.legalEntityId) {
    await assertLegalEntityExistsForRule({
      tenantId: input.tenantId,
      legalEntityId: scope.legalEntityId,
    });
    assertScopeAccess(req, "legal_entity", scope.legalEntityId, "legalEntityId");
  }
  if (scope.bankAccountId) {
    const bankAccount = await loadBankAccountRuleScope({
      tenantId: input.tenantId,
      bankAccountId: scope.bankAccountId,
    });
    if (!bankAccount) {
      throw badRequest("bankAccountId not found");
    }
    scope.legalEntityId = parsePositiveInt(bankAccount.legal_entity_id);
    assertScopeAccess(req, "legal_entity", scope.legalEntityId, "bankAccountId");
  }

  const insertResult = await query(
    `INSERT INTO bank_reconciliation_rules (
        tenant_id,
        legal_entity_id,
        rule_code,
        rule_name,
        status,
        priority,
        scope_type,
        bank_account_id,
        match_type,
        conditions_json,
        action_type,
        action_payload_json,
        stop_on_match,
        effective_from,
        effective_to,
        created_by_user_id,
        updated_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      input.tenantId,
      scope.legalEntityId || null,
      input.ruleCode,
      input.ruleName,
      input.status,
      input.priority,
      scope.scopeType,
      scope.bankAccountId || null,
      input.matchType,
      JSON.stringify(input.conditions || {}),
      input.actionType,
      JSON.stringify(input.actionPayload || {}),
      input.stopOnMatch ? 1 : 0,
      input.effectiveFrom || null,
      input.effectiveTo || null,
      input.userId || null,
      input.userId || null,
    ]
  );

  const ruleId = parsePositiveInt(insertResult.rows?.insertId);
  const row = await getRuleRowById({ tenantId: input.tenantId, ruleId });
  return maybeStageRuleChangeApproval({
    req,
    tenantId: input.tenantId,
    userId: input.userId,
    row,
    actionType: "CREATE",
    assertScopeAccess,
  });
}

export async function updateReconciliationRule({
  req,
  input,
  assertScopeAccess,
}) {
  const current = await getRuleRowById({
    tenantId: input.tenantId,
    ruleId: input.ruleId,
  });
  if (!current) {
    throw badRequest("Reconciliation rule not found");
  }
  if (current.legal_entity_id) {
    assertScopeAccess(req, "legal_entity", current.legal_entity_id, "ruleId");
  }

  const scope = normalizeRuleScopeForWrite(input, current);
  if (scope.legalEntityId) {
    await assertLegalEntityExistsForRule({
      tenantId: input.tenantId,
      legalEntityId: scope.legalEntityId,
    });
    assertScopeAccess(req, "legal_entity", scope.legalEntityId, "legalEntityId");
  }
  if (scope.bankAccountId) {
    const bankAccount = await loadBankAccountRuleScope({
      tenantId: input.tenantId,
      bankAccountId: scope.bankAccountId,
    });
    if (!bankAccount) {
      throw badRequest("bankAccountId not found");
    }
    scope.legalEntityId = parsePositiveInt(bankAccount.legal_entity_id);
    assertScopeAccess(req, "legal_entity", scope.legalEntityId, "bankAccountId");
  }

  await query(
    `UPDATE bank_reconciliation_rules
     SET rule_name = ?,
         status = ?,
         priority = ?,
         scope_type = ?,
         legal_entity_id = ?,
         bank_account_id = ?,
         match_type = ?,
         conditions_json = ?,
         action_type = ?,
         action_payload_json = ?,
         stop_on_match = ?,
         effective_from = ?,
         effective_to = ?,
         updated_by_user_id = ?
     WHERE tenant_id = ?
       AND id = ?`,
    [
      input.ruleName ?? current.rule_name,
      input.status ?? current.status,
      input.priority ?? current.priority,
      scope.scopeType,
      scope.legalEntityId || null,
      scope.bankAccountId || null,
      input.matchType ?? current.match_type,
      JSON.stringify(
        input.conditions !== undefined ? input.conditions || {} : current.conditions_json || {}
      ),
      input.actionType ?? current.action_type,
      JSON.stringify(
        input.actionPayload !== undefined
          ? input.actionPayload || {}
          : current.action_payload_json || {}
      ),
      input.stopOnMatch !== undefined ? (input.stopOnMatch ? 1 : 0) : Number(current.stop_on_match ? 1 : 0),
      input.effectiveFrom !== undefined ? input.effectiveFrom || null : current.effective_from || null,
      input.effectiveTo !== undefined ? input.effectiveTo || null : current.effective_to || null,
      input.userId || null,
      input.tenantId,
      input.ruleId,
    ]
  );

  const row = await getRuleRowById({ tenantId: input.tenantId, ruleId: input.ruleId });
  return maybeStageRuleChangeApproval({
    req,
    tenantId: input.tenantId,
    userId: input.userId,
    row,
    actionType: "UPDATE",
    assertScopeAccess,
  });
}

export async function listActiveRulesForAutomation({
  tenantId,
  legalEntityId = null,
  bankAccountId = null,
  asOfDate = null,
}) {
  const params = [tenantId, "ACTIVE", "APPROVED"];
  const conditions = ["tenant_id = ?", "status = ?", "COALESCE(approval_state, 'APPROVED') = ?"];

  if (legalEntityId) {
    conditions.push(
      `(scope_type = 'GLOBAL' OR (scope_type = 'LEGAL_ENTITY' AND legal_entity_id = ?) OR (scope_type = 'BANK_ACCOUNT' AND legal_entity_id = ?))`
    );
    params.push(legalEntityId, legalEntityId);
  }
  if (bankAccountId) {
    conditions.push(`(scope_type != 'BANK_ACCOUNT' OR bank_account_id = ?)`);
    params.push(bankAccountId);
  }
  if (asOfDate) {
    conditions.push(`(effective_from IS NULL OR effective_from <= ?)`);
    conditions.push(`(effective_to IS NULL OR effective_to >= ?)`);
    params.push(asOfDate, asOfDate);
  }

  const result = await query(
    `SELECT *
     FROM bank_reconciliation_rules
     WHERE ${conditions.join(" AND ")}
     ORDER BY priority ASC, id ASC`,
    params
  );
  return (result.rows || []).map(hydrateRuleRow);
}

export default {
  listReconciliationRuleRows,
  getReconciliationRuleById,
  createReconciliationRule,
  updateReconciliationRule,
  listActiveRulesForAutomation,
};

export async function activateApprovedRuleChange({
  tenantId,
  ruleId,
  approvalRequestId,
  approvedByUserId,
}) {
  const current = await getRuleRowById({ tenantId, ruleId });
  if (!current) throw badRequest("Reconciliation rule not found");
  await query(
    `UPDATE bank_reconciliation_rules
     SET approval_state = 'APPROVED',
         approval_request_id = ?,
         status = CASE WHEN status = 'PAUSED' THEN 'ACTIVE' ELSE status END,
         updated_by_user_id = ?
     WHERE tenant_id = ?
       AND id = ?`,
    [approvalRequestId || null, approvedByUserId || null, tenantId, ruleId]
  );
  return { rule_id: ruleId, activated: true };
}
