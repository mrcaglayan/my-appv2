import crypto from "node:crypto";
import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { evaluateBankApprovalNeed, snapshotBankApprovalPolicy } from "./bank.governance.service.js";

function u(value) {
  return String(value || "").trim().toUpperCase();
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
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

function toAmount(value) {
  if (value === undefined || value === null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? Number(n.toFixed(6)) : null;
}

function notFound(message) {
  const err = new Error(message);
  err.status = 404;
  return err;
}

function conflict(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}

function forbidden(message) {
  const err = new Error(message);
  err.status = 403;
  return err;
}

function isDuplicateKeyError(err) {
  return Number(err?.errno) === 1062 || u(err?.code) === "ER_DUP_ENTRY";
}

function randomRequestCode() {
  return `BAR-${Date.now()}-${crypto.randomBytes(4).toString("hex").toUpperCase()}`;
}

function hydrateApprovalRequestRow(row) {
  if (!row) return null;
  return {
    ...row,
    module_code: u(row.module_code || "BANK"),
    threshold_amount: toAmount(row.threshold_amount),
    target_snapshot_json: parseJson(row.target_snapshot_json, {}),
    action_payload_json: parseJson(row.action_payload_json, null),
    policy_snapshot_json: parseJson(row.policy_snapshot_json, {}),
    execution_result_json: parseJson(row.execution_result_json, null),
  };
}

function normalizeRequestScopeFields({
  legalEntityId = null,
  bankAccountId = null,
  thresholdAmount = null,
  currencyCode = null,
} = {}) {
  return {
    legalEntityId: parsePositiveInt(legalEntityId) || null,
    bankAccountId: parsePositiveInt(bankAccountId) || null,
    thresholdAmount: toAmount(thresholdAmount),
    currencyCode: u(currencyCode || "") || null,
  };
}

async function getBankAccountScopeInfo({ tenantId, bankAccountId, runQuery = query }) {
  const parsedId = parsePositiveInt(bankAccountId);
  if (!parsedId) return null;
  const res = await runQuery(
    `SELECT id, tenant_id, legal_entity_id
     FROM bank_accounts
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, parsedId]
  );
  return res.rows?.[0] || null;
}

async function getApprovalPolicyRow({ tenantId, policyId, runQuery = query }) {
  const res = await runQuery(
    `SELECT *
     FROM bank_approval_policies
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, policyId]
  );
  return res.rows?.[0] || null;
}

async function getApprovalPolicyByCode({ tenantId, policyCode, runQuery = query }) {
  if (!String(policyCode || "").trim()) return null;
  const res = await runQuery(
    `SELECT *
     FROM bank_approval_policies
     WHERE tenant_id = ?
       AND policy_code = ?
     LIMIT 1`,
    [tenantId, String(policyCode).trim().toUpperCase()]
  );
  return res.rows?.[0] || null;
}

async function getApprovalRequestRowById({ tenantId, requestId, runQuery = query, forUpdate = false }) {
  const res = await runQuery(
    `SELECT
        r.*,
        p.policy_code,
        p.policy_name
     FROM bank_approval_requests r
     JOIN bank_approval_policies p
       ON p.id = r.policy_id
     WHERE r.tenant_id = ?
       AND r.id = ?
     LIMIT 1
     ${forUpdate ? "FOR UPDATE" : ""}`,
    [tenantId, requestId]
  );
  return hydrateApprovalRequestRow(res.rows?.[0] || null);
}

async function getApprovalRequestByKey({ tenantId, requestKey, runQuery = query }) {
  if (!String(requestKey || "").trim()) return null;
  const res = await runQuery(
    `SELECT
        r.*,
        p.policy_code,
        p.policy_name
     FROM bank_approval_requests r
     JOIN bank_approval_policies p
       ON p.id = r.policy_id
     WHERE r.tenant_id = ?
       AND r.request_key = ?
     LIMIT 1`,
    [tenantId, String(requestKey).trim()]
  );
  return hydrateApprovalRequestRow(res.rows?.[0] || null);
}

async function listApprovalRequestDecisions({ tenantId, requestId, runQuery = query }) {
  const res = await runQuery(
    `SELECT *
     FROM bank_approval_request_decisions
     WHERE tenant_id = ?
       AND bank_approval_request_id = ?
     ORDER BY id ASC`,
    [tenantId, requestId]
  );
  return res.rows || [];
}

async function loadUserPermissionCodes({ tenantId, userId, runQuery = query }) {
  if (!parsePositiveInt(tenantId) || !parsePositiveInt(userId)) return [];
  const result = await runQuery(
    `SELECT
       p.code,
       SUM(CASE WHEN urs.effect = 'ALLOW' THEN 1 ELSE 0 END) AS allow_count,
       SUM(CASE WHEN urs.effect = 'DENY' AND urs.scope_type = 'TENANT' THEN 1 ELSE 0 END) AS tenant_deny_count
     FROM user_role_scopes urs
     JOIN roles r ON r.id = urs.role_id
     JOIN role_permissions rp ON rp.role_id = r.id
     JOIN permissions p ON p.id = rp.permission_id
     WHERE urs.user_id = ?
       AND urs.tenant_id = ?
     GROUP BY p.code
     HAVING allow_count > 0
        AND tenant_deny_count = 0`,
    [userId, tenantId]
  );
  return (result.rows || []).map((row) => String(row.code || "").trim()).filter(Boolean);
}

async function assertApproverPermission({ tenantId, userId, approverPermissionCode, runQuery = query }) {
  const code = String(approverPermissionCode || "").trim();
  if (!code) return;
  const perms = await loadUserPermissionCodes({ tenantId, userId, runQuery });
  if (!perms.includes(code)) {
    throw forbidden(`Missing permission: ${code}`);
  }
}

async function countApprovalDecisionStats({ tenantId, requestId, runQuery = query }) {
  const res = await runQuery(
    `SELECT
        SUM(CASE WHEN decision = 'APPROVE' THEN 1 ELSE 0 END) AS approve_count,
        SUM(CASE WHEN decision = 'REJECT' THEN 1 ELSE 0 END) AS reject_count
     FROM bank_approval_request_decisions
     WHERE tenant_id = ?
       AND bank_approval_request_id = ?`,
    [tenantId, requestId]
  );
  return {
    approveCount: Number(res.rows?.[0]?.approve_count || 0),
    rejectCount: Number(res.rows?.[0]?.reject_count || 0),
  };
}

async function hydrateRequestForResponse({ tenantId, requestId, runQuery = query }) {
  const row = await getApprovalRequestRowById({ tenantId, requestId, runQuery });
  if (!row) return null;
  const decisions = await listApprovalRequestDecisions({ tenantId, requestId, runQuery });
  return {
    ...row,
    decisions,
    approvals_granted: decisions.filter((d) => u(d.decision) === "APPROVE").length,
    rejections_granted: decisions.filter((d) => u(d.decision) === "REJECT").length,
  };
}

async function finalizeExecutionResult({ tenantId, requestId, userId, success, result = null, errorText = null }) {
  await query(
    `UPDATE bank_approval_requests
     SET request_status = ?,
         execution_status = ?,
         executed_at = CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE executed_at END,
         executed_by_user_id = CASE WHEN ? THEN ? ELSE executed_by_user_id END,
         execution_result_json = ?,
         execution_error_text = ?
     WHERE tenant_id = ?
       AND id = ?`,
    [
      success ? "EXECUTED" : "FAILED",
      success ? "EXECUTED" : "FAILED",
      success ? 1 : 0,
      success ? 1 : 0,
      success ? (userId || null) : null,
      safeJson(result),
      errorText || null,
      tenantId,
      requestId,
    ]
  );
}

async function markExecutionInProgress({ tenantId, requestId }) {
  await query(
    `UPDATE bank_approval_requests
     SET execution_status = 'EXECUTING'
     WHERE tenant_id = ?
       AND id = ?
       AND execution_status <> 'EXECUTED'`,
    [tenantId, requestId]
  );
}

async function executeApprovalAction({ requestRow, approvedByUserId }) {
  const moduleCode = u(requestRow.module_code || "BANK");
  const targetType = u(requestRow.target_type);
  const actionType = u(requestRow.action_type);
  const payload = parseJson(requestRow.action_payload_json, {}) || {};
  const tenantId = parsePositiveInt(requestRow.tenant_id);
  const requestId = parsePositiveInt(requestRow.id);

  if (targetType === "PAYMENT_BATCH" && actionType === "SUBMIT_EXPORT") {
    const mod = await import("./bank.paymentFiles.service.js");
    return mod.executeApprovedPaymentBatchExportFile({
      tenantId,
      batchId: parsePositiveInt(payload.batchId ?? payload.batch_id ?? requestRow.target_id),
      approvalRequestId: requestId,
      approvedByUserId,
      payload,
    });
  }

  if (targetType === "RECON_RULE" && ["CREATE", "UPDATE"].includes(actionType)) {
    const mod = await import("./bank.reconciliationRules.service.js");
    return mod.activateApprovedRuleChange({
      tenantId,
      ruleId: parsePositiveInt(payload.ruleId ?? payload.rule_id ?? requestRow.target_id),
      approvalRequestId: requestId,
      approvedByUserId,
    });
  }

  if (targetType === "POST_TEMPLATE" && ["CREATE", "UPDATE"].includes(actionType)) {
    const mod = await import("./bank.reconciliationPostingTemplates.service.js");
    return mod.activateApprovedPostingTemplateChange({
      tenantId,
      templateId: parsePositiveInt(payload.templateId ?? payload.template_id ?? requestRow.target_id),
      approvalRequestId: requestId,
      approvedByUserId,
    });
  }

  if (targetType === "DIFF_PROFILE" && ["CREATE", "UPDATE"].includes(actionType)) {
    const mod = await import("./bank.reconciliationDifferenceProfiles.service.js");
    return mod.activateApprovedDifferenceProfileChange({
      tenantId,
      profileId: parsePositiveInt(payload.profileId ?? payload.profile_id ?? requestRow.target_id),
      approvalRequestId: requestId,
      approvedByUserId,
    });
  }

  if (targetType === "MANUAL_RETURN" && actionType === "CREATE") {
    const mod = await import("./bank.paymentReturns.service.js");
    return mod.executeApprovedManualPaymentReturn({
      tenantId,
      approvalRequestId: requestId,
      approvedByUserId,
      payload,
    });
  }

  if (targetType === "RECON_EXCEPTION_OVERRIDE" && ["RESOLVE", "IGNORE"].includes(actionType)) {
    const mod = await import("./bank.reconciliationExceptions.service.js");
    return mod.executeApprovedExceptionOverride({
      tenantId,
      approvalRequestId: requestId,
      approvedByUserId,
      payload,
    });
  }

  if (moduleCode === "PAYROLL" && targetType === "PAYROLL_MANUAL_SETTLEMENT_OVERRIDE" && actionType === "APPLY") {
    const mod = await import("./payroll.settlementOverrides.service.js");
    return mod.executeApprovedPayrollManualSettlementOverride({
      tenantId,
      approvalRequestId: requestId,
      approvedByUserId,
      payload,
    });
  }

  if (moduleCode === "PAYROLL" && targetType === "PAYROLL_PERIOD_CLOSE" && actionType === "APPROVE_CLOSE") {
    const mod = await import("./payroll.close.service.js");
    return mod.executeApprovedPayrollPeriodClose({
      tenantId,
      approvalRequestId: requestId,
      approvedByUserId,
      payload,
    });
  }

  if (moduleCode === "PAYROLL" && targetType === "PAYROLL_PERIOD_CLOSE" && actionType === "REOPEN") {
    const mod = await import("./payroll.close.service.js");
    return mod.executeApprovedPayrollPeriodReopen({
      tenantId,
      approvalRequestId: requestId,
      approvedByUserId,
      payload,
    });
  }

  if (moduleCode === "PAYROLL" && targetType === "PAYROLL_PROVIDER_IMPORT" && actionType === "APPLY") {
    const mod = await import("./payroll.providers.service.js");
    return mod.executeApprovedPayrollProviderImportApply({
      tenantId,
      approvalRequestId: requestId,
      approvedByUserId,
      payload,
    });
  }

  throw conflict(`No approval execution resolver for ${moduleCode}/${targetType}/${actionType}`);
}

function inferRequestScopeFromInput({ legalEntityId = null, bankAccountId = null } = {}) {
  if (parsePositiveInt(legalEntityId)) return { legalEntityId: parsePositiveInt(legalEntityId) };
  return { legalEntityId: null };
}

async function resolvePolicyForSubmission({
  tenantId,
  moduleCode = "BANK",
  policyCode = null,
  targetType,
  actionType,
  legalEntityId = null,
  bankAccountId = null,
  thresholdAmount = null,
  currencyCode = null,
  runQuery = query,
}) {
  if (policyCode) {
    const policy = await getApprovalPolicyByCode({ tenantId, policyCode, runQuery });
    if (!policy) throw badRequest("policyCode not found");
    if (u(policy.status) !== "ACTIVE") {
      throw badRequest("policyCode must reference an ACTIVE bank approval policy");
    }
    return {
      approvalRequired: true,
      approval_required: true,
      policy,
      policySnapshot: snapshotBankApprovalPolicy(policy),
    };
  }
  return evaluateBankApprovalNeed({
    tenantId,
    moduleCode,
    targetType,
    actionType,
    legalEntityId,
    bankAccountId,
    thresholdAmount,
    currencyCode,
    runQuery,
  });
}

export async function resolveBankApprovalRequestScope(requestId, tenantId) {
  const parsedRequestId = parsePositiveInt(requestId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedRequestId || !parsedTenantId) return null;
  const row = await getApprovalRequestRowById({ tenantId: parsedTenantId, requestId: parsedRequestId });
  if (!row) return null;
  if (parsePositiveInt(row.legal_entity_id)) {
    return { scopeType: "LEGAL_ENTITY", scopeId: parsePositiveInt(row.legal_entity_id) };
  }
  return null;
}

export async function listBankApprovalRequestRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const where = ["r.tenant_id = ?"];
  if (typeof buildScopeFilter === "function") {
    where.push(buildScopeFilter(req, "legal_entity", "r.legal_entity_id", params));
  }
  if (filters.requestStatus) {
    where.push("r.request_status = ?");
    params.push(filters.requestStatus);
  }
  if (filters.moduleCode) {
    where.push("COALESCE(r.module_code, 'BANK') = ?");
    params.push(u(filters.moduleCode));
  }
  if (filters.targetType) {
    where.push("r.target_type = ?");
    params.push(filters.targetType);
  }
  if (filters.actionType) {
    where.push("r.action_type = ?");
    params.push(filters.actionType);
  }
  if (filters.mineOnly) {
    where.push("r.requested_by_user_id = ?");
    params.push(parsePositiveInt(req?.user?.userId) || -1);
  }

  const whereSql = where.join(" AND ");
  const countRes = await query(
    `SELECT COUNT(*) AS total
     FROM bank_approval_requests r
     WHERE ${whereSql}`,
    params
  );
  const total = Number(countRes.rows?.[0]?.total || 0);
  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset = Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const listRes = await query(
    `SELECT
        r.*,
        p.policy_code,
        p.policy_name,
        (
          SELECT COUNT(*)
          FROM bank_approval_request_decisions d
          WHERE d.tenant_id = r.tenant_id
            AND d.bank_approval_request_id = r.id
            AND d.decision = 'APPROVE'
        ) AS approve_count,
        (
          SELECT COUNT(*)
          FROM bank_approval_request_decisions d
          WHERE d.tenant_id = r.tenant_id
            AND d.bank_approval_request_id = r.id
            AND d.decision = 'REJECT'
        ) AS reject_count
     FROM bank_approval_requests r
     JOIN bank_approval_policies p
       ON p.id = r.policy_id
     WHERE ${whereSql}
     ORDER BY
       CASE r.request_status WHEN 'PENDING' THEN 0 WHEN 'APPROVED' THEN 1 ELSE 2 END,
       r.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: (listRes.rows || []).map((row) => ({
      ...hydrateApprovalRequestRow(row),
      approve_count: Number(row.approve_count || 0),
      reject_count: Number(row.reject_count || 0),
    })),
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getBankApprovalRequestById({
  req,
  tenantId,
  requestId,
  assertScopeAccess,
}) {
  const row = await getApprovalRequestRowById({ tenantId, requestId });
  if (!row) throw badRequest("Approval request not found");
  if (parsePositiveInt(row.legal_entity_id)) {
    assertScopeAccess(req, "legal_entity", row.legal_entity_id, "requestId");
  }
  const decisions = await listApprovalRequestDecisions({ tenantId, requestId });
  return {
    ...row,
    decisions,
    approvals_granted: decisions.filter((d) => u(d.decision) === "APPROVE").length,
    rejections_granted: decisions.filter((d) => u(d.decision) === "REJECT").length,
  };
}

export async function submitBankApprovalRequest({
  tenantId,
  userId,
  requestInput,
  snapshotBuilder = null,
  policyOverride = null,
  runQuery = query,
}) {
  const normalized = normalizeRequestScopeFields({
    legalEntityId: requestInput.legalEntityId,
    bankAccountId: requestInput.bankAccountId,
    thresholdAmount: requestInput.thresholdAmount,
    currencyCode: requestInput.currencyCode,
  });

  let legalEntityId = normalized.legalEntityId;
  if (!legalEntityId && normalized.bankAccountId) {
    const ba = await getBankAccountScopeInfo({
      tenantId,
      bankAccountId: normalized.bankAccountId,
      runQuery,
    });
    legalEntityId = parsePositiveInt(ba?.legal_entity_id) || null;
  }

  const governance =
    policyOverride ||
    (await resolvePolicyForSubmission({
      tenantId,
      moduleCode: requestInput.moduleCode || "BANK",
      policyCode: requestInput.policyCode || null,
      targetType: requestInput.targetType,
      actionType: requestInput.actionType,
      legalEntityId,
      bankAccountId: normalized.bankAccountId,
      thresholdAmount: normalized.thresholdAmount,
      currencyCode: normalized.currencyCode,
      runQuery,
    }));

  if (!governance?.approvalRequired && !governance?.approval_required) {
    return { approval_required: false, approvalRequired: false, item: null };
  }

  const policy = governance.policy;
  if (!policy) throw badRequest("Applicable approval policy not found");
  const policySnapshot = governance.policySnapshot || snapshotBankApprovalPolicy(policy);
  const requestKey = String(requestInput.requestKey || "").trim() || null;

  if (requestKey) {
    const existing = await getApprovalRequestByKey({
      tenantId,
      requestKey,
      runQuery,
    });
    if (existing) {
      return {
        approval_required: true,
        approvalRequired: true,
        item: existing,
        idempotent: true,
      };
    }
  }

  const targetSnapshot =
    typeof snapshotBuilder === "function"
      ? (await snapshotBuilder()) || {}
      : requestInput.targetSnapshot || requestInput.target_snapshot || {};

  const requestCode = randomRequestCode();
  try {
    const ins = await runQuery(
      `INSERT INTO bank_approval_requests (
          tenant_id,
          module_code,
          request_code,
          request_key,
          policy_id,
          target_type,
          target_id,
          action_type,
          request_status,
          execution_status,
          legal_entity_id,
          bank_account_id,
          threshold_amount,
          currency_code,
          required_approvals,
          maker_checker_required,
          approver_permission_code,
          auto_execute_on_final_approval,
          requested_by_user_id,
          target_snapshot_json,
          action_payload_json,
          policy_snapshot_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 'NOT_EXECUTED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        tenantId,
        u(requestInput.moduleCode || "BANK"),
        requestCode,
        requestKey,
        parsePositiveInt(policy.id),
        u(requestInput.targetType),
        parsePositiveInt(requestInput.targetId) || null,
        u(requestInput.actionType),
        legalEntityId || null,
        normalized.bankAccountId,
        normalized.thresholdAmount,
        normalized.currencyCode,
        Number(policySnapshot.required_approvals || 1),
        policySnapshot.maker_checker_required ? 1 : 0,
        policySnapshot.approver_permission_code || "bank.approvals.requests.approve",
        policySnapshot.auto_execute_on_final_approval ? 1 : 0,
        userId,
        safeJson(targetSnapshot || {}),
        safeJson(requestInput.actionPayload ?? requestInput.action_payload ?? null),
        safeJson(policySnapshot || {}),
      ]
    );
    const requestId = parsePositiveInt(ins.rows?.insertId);
    return {
      approval_required: true,
      approvalRequired: true,
      idempotent: false,
      item: await getApprovalRequestRowById({ tenantId, requestId, runQuery }),
    };
  } catch (err) {
    if (requestKey && isDuplicateKeyError(err)) {
      const existing = await getApprovalRequestByKey({ tenantId, requestKey, runQuery });
      if (existing) {
        return {
          approval_required: true,
          approvalRequired: true,
          item: existing,
          idempotent: true,
        };
      }
    }
    throw err;
  }
}

export async function submitBankApprovalRequestFromRoute({
  req,
  tenantId,
  userId,
  input,
  assertScopeAccess,
}) {
  const bankAccount = input.bankAccountId
    ? await getBankAccountScopeInfo({ tenantId, bankAccountId: input.bankAccountId })
    : null;
  const legalEntityId = parsePositiveInt(input.legalEntityId) || parsePositiveInt(bankAccount?.legal_entity_id) || null;
  if (legalEntityId && typeof assertScopeAccess === "function") {
    assertScopeAccess(req, "legal_entity", legalEntityId, input.bankAccountId ? "bankAccountId" : "legalEntityId");
  }
  const result = await submitBankApprovalRequest({
    tenantId,
    userId,
    requestInput: {
      ...input,
      moduleCode: input.moduleCode || "BANK",
      legalEntityId,
      bankAccountId: input.bankAccountId || null,
      thresholdAmount: input.thresholdAmount ?? null,
      currencyCode: input.currencyCode || null,
      targetSnapshot: input.targetSnapshot || {
        module_code: input.moduleCode || "BANK",
        target_type: input.targetType,
        target_id: input.targetId || null,
      },
    },
    snapshotBuilder: async () =>
      input.targetSnapshot || {
        module_code: input.moduleCode || "BANK",
        target_type: input.targetType,
        target_id: input.targetId || null,
      },
  });

  if (!result.approval_required) {
    throw badRequest("No active approval policy matched for the submitted request");
  }
  return result;
}

export async function executeBankApprovalRequest({
  tenantId,
  requestId,
  userId,
}) {
  const locked = await withTransaction(async (tx) => {
    const row = await getApprovalRequestRowById({
      tenantId,
      requestId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!row) throw notFound("Approval request not found");

    if (u(row.execution_status) === "EXECUTED" || u(row.request_status) === "EXECUTED") {
      return { row, alreadyExecuted: true };
    }
    if (u(row.request_status) !== "APPROVED") {
      throw conflict(`Approval request status ${row.request_status} is not executable`);
    }

    await tx.query(
      `UPDATE bank_approval_requests
       SET execution_status = 'EXECUTING'
       WHERE tenant_id = ?
         AND id = ?`,
      [tenantId, requestId]
    );

    return { row, alreadyExecuted: false };
  });

  if (locked.alreadyExecuted) {
    return {
      item: await hydrateRequestForResponse({ tenantId, requestId }),
      idempotent: true,
    };
  }

  try {
    const executionResult = await executeApprovalAction({
      requestRow: locked.row,
      approvedByUserId: userId,
    });
    await finalizeExecutionResult({
      tenantId,
      requestId,
      userId,
      success: true,
      result: executionResult || null,
    });
    return {
      item: await hydrateRequestForResponse({ tenantId, requestId }),
      execution_result: executionResult || null,
      idempotent: false,
    };
  } catch (err) {
    await finalizeExecutionResult({
      tenantId,
      requestId,
      userId,
      success: false,
      result: null,
      errorText: String(err?.message || err),
    });
    throw err;
  }
}

async function upsertDecisionTx({
  tenantId,
  requestId,
  userId,
  decision,
  decisionComment = null,
  runQuery,
}) {
  await runQuery(
    `INSERT INTO bank_approval_request_decisions (
        tenant_id,
        bank_approval_request_id,
        decided_by_user_id,
        decision,
        decision_comment
      ) VALUES (?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        decision = VALUES(decision),
        decision_comment = VALUES(decision_comment),
        created_at = CURRENT_TIMESTAMP`,
    [tenantId, requestId, userId, u(decision), decisionComment || null]
  );
}

export async function approveBankApprovalRequest({
  req,
  tenantId,
  requestId,
  userId,
  decisionComment = null,
  assertScopeAccess,
}) {
  const phase1 = await withTransaction(async (tx) => {
    const row = await getApprovalRequestRowById({
      tenantId,
      requestId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!row) throw notFound("Approval request not found");

    if (parsePositiveInt(row.legal_entity_id)) {
      assertScopeAccess(req, "legal_entity", row.legal_entity_id, "requestId");
    }

    const requestStatus = u(row.request_status);
    if (requestStatus === "REJECTED") {
      throw conflict("Rejected approval request cannot be approved");
    }
    if (requestStatus === "CANCELLED") {
      throw conflict("Cancelled approval request cannot be approved");
    }

    if (u(row.execution_status) === "EXECUTED" || requestStatus === "EXECUTED") {
      return { row, shouldExecute: false, alreadyDone: true };
    }

    const policySnapshot = parseJson(row.policy_snapshot_json, {}) || {};
    if (policySnapshot.maker_checker_required && parsePositiveInt(row.requested_by_user_id) === parsePositiveInt(userId)) {
      throw forbidden("Maker-checker violation: requester cannot approve own request");
    }

    await assertApproverPermission({
      tenantId,
      userId,
      approverPermissionCode:
        policySnapshot.approver_permission_code || row.approver_permission_code || "bank.approvals.requests.approve",
      runQuery: tx.query,
    });

    await upsertDecisionTx({
      tenantId,
      requestId,
      userId,
      decision: "APPROVE",
      decisionComment,
      runQuery: tx.query,
    });

    const stats = await countApprovalDecisionStats({ tenantId, requestId, runQuery: tx.query });
    const requiredApprovals = Math.max(1, Number(policySnapshot.required_approvals || row.required_approvals || 1));

    if (stats.rejectCount > 0) {
      await tx.query(
        `UPDATE bank_approval_requests
         SET request_status = 'REJECTED',
             execution_status = 'NOT_EXECUTED',
             rejected_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ?
           AND id = ?`,
        [tenantId, requestId]
      );
      return { row, shouldExecute: false, alreadyDone: false };
    }

    if (stats.approveCount >= requiredApprovals) {
      await tx.query(
        `UPDATE bank_approval_requests
         SET request_status = 'APPROVED',
             approved_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ?
           AND id = ?`,
        [tenantId, requestId]
      );
      return {
        row,
        shouldExecute: Boolean(
          policySnapshot.auto_execute_on_final_approval ?? row.auto_execute_on_final_approval ?? true
        ),
        alreadyDone: false,
      };
    }

    return { row, shouldExecute: false, alreadyDone: false };
  });

  let execution = null;
  if (phase1.shouldExecute) {
    execution = await executeBankApprovalRequest({ tenantId, requestId, userId });
  }

  const item = await hydrateRequestForResponse({ tenantId, requestId });
  return {
    item,
    execution_result: execution?.execution_result || item?.execution_result_json || null,
    idempotent: Boolean(phase1.alreadyDone || execution?.idempotent),
  };
}

export async function rejectBankApprovalRequest({
  req,
  tenantId,
  requestId,
  userId,
  decisionComment = null,
  assertScopeAccess,
}) {
  await withTransaction(async (tx) => {
    const row = await getApprovalRequestRowById({
      tenantId,
      requestId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!row) throw notFound("Approval request not found");

    if (parsePositiveInt(row.legal_entity_id)) {
      assertScopeAccess(req, "legal_entity", row.legal_entity_id, "requestId");
    }

    if (u(row.execution_status) === "EXECUTED" || u(row.request_status) === "EXECUTED") {
      throw conflict("Executed approval request cannot be rejected");
    }
    if (u(row.request_status) === "REJECTED") {
      await upsertDecisionTx({
        tenantId,
        requestId,
        userId,
        decision: "REJECT",
        decisionComment,
        runQuery: tx.query,
      });
      return;
    }

    const policySnapshot = parseJson(row.policy_snapshot_json, {}) || {};
    await assertApproverPermission({
      tenantId,
      userId,
      approverPermissionCode:
        policySnapshot.approver_permission_code || row.approver_permission_code || "bank.approvals.requests.approve",
      runQuery: tx.query,
    });

    await upsertDecisionTx({
      tenantId,
      requestId,
      userId,
      decision: "REJECT",
      decisionComment,
      runQuery: tx.query,
    });

    await tx.query(
      `UPDATE bank_approval_requests
       SET request_status = 'REJECTED',
           execution_status = 'NOT_EXECUTED',
           rejected_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [tenantId, requestId]
    );
  });

  return {
    item: await hydrateRequestForResponse({ tenantId, requestId }),
    idempotent: false,
  };
}

export default {
  resolveBankApprovalRequestScope,
  listBankApprovalRequestRows,
  getBankApprovalRequestById,
  submitBankApprovalRequest,
  submitBankApprovalRequestFromRoute,
  approveBankApprovalRequest,
  rejectBankApprovalRequest,
  executeBankApprovalRequest,
};
