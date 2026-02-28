import { query } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";

function u(value) {
  return String(value || "").trim().toUpperCase();
}

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function toAmount(value) {
  if (value === undefined || value === null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? Number(n.toFixed(6)) : null;
}

function hydrate(row) {
  if (!row) return null;
  return {
    ...row,
    module_code: u(row.module_code || "BANK"),
    min_amount: toAmount(row.min_amount),
    max_amount: toAmount(row.max_amount),
    maker_checker_required: parseDbBoolean(row.maker_checker_required),
    auto_execute_on_final_approval: parseDbBoolean(row.auto_execute_on_final_approval),
    required_approvals: Number(row.required_approvals || 1),
  };
}

function getAllowedLegalEntityIdsFromReq(req) {
  const ids = Array.from(req?.rbac?.scopeContext?.legalEntities || []);
  return ids.map((id) => parsePositiveInt(id)).filter(Boolean);
}

function buildPolicyScopeWhere(req, alias, params) {
  if (req?.rbac?.scopeContext?.tenantWide) return "1 = 1";
  const ids = getAllowedLegalEntityIdsFromReq(req);
  if (ids.length === 0) {
    return `${alias}.scope_type = 'GLOBAL'`;
  }
  params.push(...ids);
  return `(${alias}.scope_type = 'GLOBAL' OR ${alias}.legal_entity_id IN (${ids.map(() => "?").join(", ")}))`;
}

async function getPolicyByIdRaw({ tenantId, policyId, runQuery = query }) {
  const res = await runQuery(
    `SELECT
        p.*,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name
     FROM bank_approval_policies p
     LEFT JOIN legal_entities le
       ON le.tenant_id = p.tenant_id
      AND le.id = p.legal_entity_id
     LEFT JOIN bank_accounts ba
       ON ba.tenant_id = p.tenant_id
      AND ba.legal_entity_id = p.legal_entity_id
      AND ba.id = p.bank_account_id
     WHERE p.tenant_id = ?
       AND p.id = ?
     LIMIT 1`,
    [tenantId, policyId]
  );
  return hydrate(res.rows?.[0] || null);
}

async function getLegalEntityRow({ tenantId, legalEntityId }) {
  if (!parsePositiveInt(legalEntityId)) return null;
  const res = await query(
    `SELECT id, tenant_id, code, name
     FROM legal_entities
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId]
  );
  return res.rows?.[0] || null;
}

async function getBankAccountRow({ tenantId, bankAccountId }) {
  if (!parsePositiveInt(bankAccountId)) return null;
  const res = await query(
    `SELECT id, tenant_id, legal_entity_id, code, name, is_active
     FROM bank_accounts
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, bankAccountId]
  );
  return res.rows?.[0] || null;
}

function normalizePolicyScopeForWrite(input, current = null) {
  const next = {
    scopeType: u(input.scopeType ?? current?.scope_type ?? "GLOBAL"),
    legalEntityId:
      input.legalEntityId !== undefined ? parsePositiveInt(input.legalEntityId) : parsePositiveInt(current?.legal_entity_id),
    bankAccountId:
      input.bankAccountId !== undefined ? parsePositiveInt(input.bankAccountId) : parsePositiveInt(current?.bank_account_id),
  };

  if (!["GLOBAL", "LEGAL_ENTITY", "BANK_ACCOUNT"].includes(next.scopeType)) {
    throw badRequest("scopeType is invalid");
  }

  if (next.scopeType === "GLOBAL") {
    next.legalEntityId = null;
    next.bankAccountId = null;
  } else if (next.scopeType === "LEGAL_ENTITY") {
    if (!next.legalEntityId) throw badRequest("legalEntityId is required for LEGAL_ENTITY scope");
    next.bankAccountId = null;
  } else if (next.scopeType === "BANK_ACCOUNT") {
    if (!next.bankAccountId) throw badRequest("bankAccountId is required for BANK_ACCOUNT scope");
  }

  return next;
}

async function validatePolicyWriteContext({ req, tenantId, input, current = null, assertScopeAccess }) {
  const scope = normalizePolicyScopeForWrite(input, current);
  let legalEntityId = scope.legalEntityId;
  let bankAccountId = scope.bankAccountId;

  if (scope.scopeType === "BANK_ACCOUNT") {
    const ba = await getBankAccountRow({ tenantId, bankAccountId });
    if (!ba) throw badRequest("bankAccountId not found");
    if (!parseDbBoolean(ba.is_active)) throw badRequest("bankAccountId is inactive");
    legalEntityId = parsePositiveInt(ba.legal_entity_id);
  }

  if (legalEntityId) {
    const le = await getLegalEntityRow({ tenantId, legalEntityId });
    if (!le) throw badRequest("legalEntityId not found");
    if (typeof assertScopeAccess === "function") {
      assertScopeAccess(req, "legal_entity", legalEntityId, bankAccountId ? "bankAccountId" : "legalEntityId");
    }
  }

  if (input.minAmount !== undefined && input.maxAmount !== undefined) {
    if (input.minAmount !== null && input.maxAmount !== null && Number(input.minAmount) > Number(input.maxAmount)) {
      throw badRequest("minAmount cannot exceed maxAmount");
    }
  }

  if (parsePositiveInt(input.requiredApprovals) && Number(input.requiredApprovals) < 1) {
    throw badRequest("requiredApprovals must be >= 1");
  }

  return {
    scopeType: scope.scopeType,
    legalEntityId: legalEntityId || null,
    bankAccountId: bankAccountId || null,
  };
}

export async function resolveBankApprovalPolicyScope(policyId, tenantId) {
  const parsedPolicyId = parsePositiveInt(policyId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedPolicyId || !parsedTenantId) return null;
  const row = await getPolicyByIdRaw({ tenantId: parsedTenantId, policyId: parsedPolicyId });
  if (!row) return null;
  if (parsePositiveInt(row.legal_entity_id)) {
    return { scopeType: "LEGAL_ENTITY", scopeId: parsePositiveInt(row.legal_entity_id) };
  }
  return null;
}

export async function listBankApprovalPolicies({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const where = ["p.tenant_id = ?"];
  where.push(buildPolicyScopeWhere(req, "p", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    where.push("(p.legal_entity_id = ? OR p.scope_type = 'GLOBAL')");
    params.push(filters.legalEntityId);
  }
  if (filters.bankAccountId) {
    const ba = await getBankAccountRow({ tenantId, bankAccountId: filters.bankAccountId });
    if (!ba) throw badRequest("bankAccountId not found");
    assertScopeAccess(req, "legal_entity", ba.legal_entity_id, "bankAccountId");
    where.push("(p.bank_account_id = ? OR p.scope_type = 'GLOBAL')");
    params.push(filters.bankAccountId);
  }
  if (filters.status) {
    where.push("p.status = ?");
    params.push(filters.status);
  }
  if (filters.moduleCode) {
    where.push("COALESCE(p.module_code, 'BANK') = ?");
    params.push(u(filters.moduleCode));
  }
  if (filters.targetType) {
    where.push("p.target_type = ?");
    params.push(filters.targetType);
  }
  if (filters.actionType) {
    where.push("p.action_type = ?");
    params.push(filters.actionType);
  }
  if (filters.scopeType) {
    where.push("p.scope_type = ?");
    params.push(filters.scopeType);
  }
  if (filters.q) {
    const like = `%${filters.q}%`;
    where.push("(p.policy_code LIKE ? OR p.policy_name LIKE ?)");
    params.push(like, like);
  }

  const whereSql = where.join(" AND ");
  const countRes = await query(
    `SELECT COUNT(*) AS total
     FROM bank_approval_policies p
     WHERE ${whereSql}`,
    params
  );
  const total = Number(countRes.rows?.[0]?.total || 0);

  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset = Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const listRes = await query(
    `SELECT
        p.*,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name
     FROM bank_approval_policies p
     LEFT JOIN legal_entities le
       ON le.tenant_id = p.tenant_id
      AND le.id = p.legal_entity_id
     LEFT JOIN bank_accounts ba
       ON ba.tenant_id = p.tenant_id
      AND ba.legal_entity_id = p.legal_entity_id
      AND ba.id = p.bank_account_id
     WHERE ${whereSql}
     ORDER BY
       CASE p.status WHEN 'ACTIVE' THEN 0 WHEN 'PAUSED' THEN 1 ELSE 2 END,
       p.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: (listRes.rows || []).map(hydrate),
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getBankApprovalPolicyById({
  req,
  tenantId,
  policyId,
  assertScopeAccess,
}) {
  const row = await getPolicyByIdRaw({ tenantId, policyId });
  if (!row) throw badRequest("Approval policy not found");
  if (parsePositiveInt(row.legal_entity_id)) {
    assertScopeAccess(req, "legal_entity", row.legal_entity_id, "policyId");
  }
  return row;
}

export async function createBankApprovalPolicy({
  req,
  input,
  assertScopeAccess,
}) {
  const ctx = await validatePolicyWriteContext({
    req,
    tenantId: input.tenantId,
    input,
    current: null,
    assertScopeAccess,
  });

  const res = await query(
    `INSERT INTO bank_approval_policies (
        tenant_id,
        policy_code,
        policy_name,
        module_code,
        status,
        target_type,
        action_type,
        scope_type,
        legal_entity_id,
        bank_account_id,
        currency_code,
        min_amount,
        max_amount,
        required_approvals,
        maker_checker_required,
        approver_permission_code,
        auto_execute_on_final_approval,
        effective_from,
        effective_to,
        created_by_user_id,
        updated_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      input.tenantId,
      input.policyCode,
      input.policyName,
      u(input.moduleCode || "BANK"),
      input.status,
      input.targetType,
      input.actionType,
      ctx.scopeType,
      ctx.legalEntityId,
      ctx.bankAccountId,
      input.currencyCode || null,
      input.minAmount === null ? null : toAmount(input.minAmount),
      input.maxAmount === null ? null : toAmount(input.maxAmount),
      input.requiredApprovals || 1,
      input.makerCheckerRequired ? 1 : 0,
      input.approverPermissionCode || "bank.approvals.requests.approve",
      input.autoExecuteOnFinalApproval ? 1 : 0,
      input.effectiveFrom || null,
      input.effectiveTo || null,
      input.userId || null,
      input.userId || null,
    ]
  );

  return getPolicyByIdRaw({
    tenantId: input.tenantId,
    policyId: parsePositiveInt(res.rows?.insertId),
  });
}

export async function updateBankApprovalPolicy({
  req,
  input,
  assertScopeAccess,
}) {
  const current = await getPolicyByIdRaw({
    tenantId: input.tenantId,
    policyId: input.policyId,
  });
  if (!current) throw badRequest("Approval policy not found");
  if (parsePositiveInt(current.legal_entity_id)) {
    assertScopeAccess(req, "legal_entity", current.legal_entity_id, "policyId");
  }

  const merged = {
    ...current,
    ...input,
    moduleCode: input.moduleCode !== undefined ? input.moduleCode : current.module_code,
    scopeType: input.scopeType ?? current.scope_type,
    legalEntityId:
      input.legalEntityId !== undefined ? input.legalEntityId : current.legal_entity_id,
    bankAccountId:
      input.bankAccountId !== undefined ? input.bankAccountId : current.bank_account_id,
    minAmount: input.minAmount !== undefined ? input.minAmount : current.min_amount,
    maxAmount: input.maxAmount !== undefined ? input.maxAmount : current.max_amount,
    requiredApprovals:
      input.requiredApprovals !== undefined ? input.requiredApprovals : current.required_approvals,
    makerCheckerRequired:
      input.makerCheckerRequired !== undefined
        ? input.makerCheckerRequired
        : parseDbBoolean(current.maker_checker_required),
    autoExecuteOnFinalApproval:
      input.autoExecuteOnFinalApproval !== undefined
        ? input.autoExecuteOnFinalApproval
        : parseDbBoolean(current.auto_execute_on_final_approval),
  };
  const ctx = await validatePolicyWriteContext({
    req,
    tenantId: input.tenantId,
    input: merged,
    current,
    assertScopeAccess,
  });

  const nextMin = input.minAmount !== undefined ? input.minAmount : current.min_amount;
  const nextMax = input.maxAmount !== undefined ? input.maxAmount : current.max_amount;
  if (nextMin !== null && nextMin !== undefined && nextMax !== null && nextMax !== undefined) {
    if (Number(nextMin) > Number(nextMax)) throw badRequest("minAmount cannot exceed maxAmount");
  }

  await query(
    `UPDATE bank_approval_policies
     SET policy_name = ?,
         module_code = ?,
         status = ?,
         scope_type = ?,
         legal_entity_id = ?,
         bank_account_id = ?,
         currency_code = ?,
         min_amount = ?,
         max_amount = ?,
         required_approvals = ?,
         maker_checker_required = ?,
         approver_permission_code = ?,
         auto_execute_on_final_approval = ?,
         effective_from = ?,
         effective_to = ?,
         updated_by_user_id = ?
     WHERE tenant_id = ?
       AND id = ?`,
    [
      input.policyName !== undefined ? input.policyName : current.policy_name,
      input.moduleCode !== undefined ? u(input.moduleCode || "BANK") : u(current.module_code || "BANK"),
      input.status !== undefined ? input.status : current.status,
      ctx.scopeType,
      ctx.legalEntityId,
      ctx.bankAccountId,
      input.currencyCode !== undefined ? input.currencyCode : current.currency_code,
      nextMin === null ? null : toAmount(nextMin),
      nextMax === null ? null : toAmount(nextMax),
      input.requiredApprovals !== undefined ? input.requiredApprovals : current.required_approvals,
      input.makerCheckerRequired !== undefined
        ? (input.makerCheckerRequired ? 1 : 0)
        : Number(current.maker_checker_required ? 1 : 0),
      input.approverPermissionCode !== undefined
        ? input.approverPermissionCode || "bank.approvals.requests.approve"
        : current.approver_permission_code,
      input.autoExecuteOnFinalApproval !== undefined
        ? (input.autoExecuteOnFinalApproval ? 1 : 0)
        : Number(current.auto_execute_on_final_approval ? 1 : 0),
      input.effectiveFrom !== undefined ? input.effectiveFrom || null : current.effective_from || null,
      input.effectiveTo !== undefined ? input.effectiveTo || null : current.effective_to || null,
      input.userId || null,
      input.tenantId,
      input.policyId,
    ]
  );

  return getPolicyByIdRaw({ tenantId: input.tenantId, policyId: input.policyId });
}

export default {
  resolveBankApprovalPolicyScope,
  listBankApprovalPolicies,
  getBankApprovalPolicyById,
  createBankApprovalPolicy,
  updateBankApprovalPolicy,
};
