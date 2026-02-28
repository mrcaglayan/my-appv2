import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  approveBankApprovalRequest,
  getBankApprovalRequestById,
  rejectBankApprovalRequest,
  resolveBankApprovalRequestScope,
  submitBankApprovalRequest,
} from "../src/services/bank.approvals.service.js";
import { createBankApprovalPolicy } from "../src/services/bank.approvalPolicies.service.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function noScopeGuard() {
  return true;
}

async function expectFailure(work, { status, includes }) {
  try {
    await work();
  } catch (error) {
    if (status !== undefined && Number(error?.status || 0) !== Number(status)) {
      throw new Error(
        `Expected error status ${status} but got ${String(error?.status)} message=${String(
          error?.message || ""
        )}`
      );
    }
    if (includes && !String(error?.message || "").includes(includes)) {
      throw new Error(
        `Expected error message to include "${includes}" but got "${String(error?.message || "")}"`
      );
    }
    return;
  }
  throw new Error("Expected operation to fail, but it succeeded");
}

async function createTenantWithLegalEntity(stamp) {
  const tenantCode = `B09_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `B09 Tenant ${stamp}`]
  );
  const tenantRows = await query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [tenantCode]
  );
  const tenantId = toNumber(tenantRows.rows?.[0]?.id);
  assert(tenantId > 0, "Failed to create tenant fixture");

  const countryRows = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'TR'
     LIMIT 1`
  );
  const countryId = toNumber(countryRows.rows?.[0]?.id);
  const currencyCode = String(countryRows.rows?.[0]?.default_currency_code || "TRY");
  assert(countryId > 0, "Missing country seed row (TR)");

  await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `B09_G_${stamp}`, `B09 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `B09_G_${stamp}`]
  );
  const groupCompanyId = toNumber(groupRows.rows?.[0]?.id);
  assert(groupCompanyId > 0, "Failed to create group company fixture");

  await query(
    `INSERT INTO legal_entities (
        tenant_id,
        group_company_id,
        code,
        name,
        country_id,
        functional_currency_code,
        status
      )
      VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      groupCompanyId,
      `B09_LE_${stamp}`,
      `B09 Legal Entity ${stamp}`,
      countryId,
      currencyCode,
    ]
  );
  const legalEntityRows = await query(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `B09_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  return { tenantId, legalEntityId };
}

async function createUser({ tenantId, email, name, passwordHash }) {
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, email, passwordHash, name]
  );
  const rows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, email]
  );
  const userId = toNumber(rows.rows?.[0]?.id);
  assert(userId > 0, `Failed to create user: ${email}`);
  return userId;
}

async function assignTenantAdminRole({ tenantId, userId }) {
  const roleRows = await query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = 'TenantAdmin'
     LIMIT 1`,
    [tenantId]
  );
  const roleId = toNumber(roleRows.rows?.[0]?.id);
  assert(roleId > 0, "TenantAdmin role not found for tenant");

  await query(
    `INSERT INTO user_role_scopes (
        tenant_id, user_id, role_id, scope_type, scope_id, effect
      )
      VALUES (?, ?, ?, 'TENANT', ?, 'ALLOW')
      ON DUPLICATE KEY UPDATE
        effect = VALUES(effect)`,
    [tenantId, userId, roleId, tenantId]
  );
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const { tenantId, legalEntityId } = await createTenantWithLegalEntity(stamp);

  await seedCore({ ensureDefaultTenantIfMissing: true });

  const passwordHash = await bcrypt.hash("B09#Smoke123", 10);
  const requesterUserId = await createUser({
    tenantId,
    email: `b09_requester_${stamp}@example.com`,
    name: "B09 Requester",
    passwordHash,
  });
  const approverUserId = await createUser({
    tenantId,
    email: `b09_approver_${stamp}@example.com`,
    name: "B09 Approver",
    passwordHash,
  });

  await assignTenantAdminRole({ tenantId, userId: approverUserId });

  const policy = await createBankApprovalPolicy({
    req: null,
    assertScopeAccess: noScopeGuard,
    input: {
      tenantId,
      userId: requesterUserId,
      policyCode: `B09_POLICY_${stamp}`,
      policyName: "B09 smoke policy",
      moduleCode: "BANK",
      status: "ACTIVE",
      targetType: "RECON_RULE",
      actionType: "UPDATE",
      scopeType: "LEGAL_ENTITY",
      legalEntityId,
      bankAccountId: null,
      currencyCode: "TRY",
      minAmount: 0,
      maxAmount: 50000,
      requiredApprovals: 1,
      makerCheckerRequired: true,
      approverPermissionCode: "bank.approvals.requests.approve",
      autoExecuteOnFinalApproval: false,
      effectiveFrom: null,
      effectiveTo: null,
    },
  });
  assert(policy && toNumber(policy.id) > 0, "Failed to create bank approval policy");

  const requestKey = `B09_REQ_${stamp}`;
  const submit1 = await submitBankApprovalRequest({
    tenantId,
    userId: requesterUserId,
    requestInput: {
      moduleCode: "BANK",
      requestKey,
      policyCode: policy.policy_code,
      targetType: "RECON_RULE",
      targetId: 9001,
      actionType: "UPDATE",
      legalEntityId,
      thresholdAmount: 1250.5,
      currencyCode: "TRY",
      targetSnapshot: {
        target_type: "RECON_RULE",
        target_id: 9001,
        legal_entity_id: legalEntityId,
      },
      actionPayload: {
        ruleId: 9001,
        draftVersion: 2,
      },
    },
  });
  assert(submit1.approval_required === true, "Approval request submit should require approval");
  assert(submit1.idempotent === false, "First submit should not be idempotent");
  const requestId = toNumber(submit1.item?.id);
  assert(requestId > 0, "Approval request id missing");

  const submit2 = await submitBankApprovalRequest({
    tenantId,
    userId: requesterUserId,
    requestInput: {
      moduleCode: "BANK",
      requestKey,
      policyCode: policy.policy_code,
      targetType: "RECON_RULE",
      targetId: 9001,
      actionType: "UPDATE",
      legalEntityId,
      thresholdAmount: 1250.5,
      currencyCode: "TRY",
      targetSnapshot: { target_id: 9001 },
      actionPayload: { ruleId: 9001 },
    },
  });
  assert(submit2.approval_required === true, "Idempotent submit should still report approval_required");
  assert(submit2.idempotent === true, "Repeated requestKey should be idempotent");
  assert(
    toNumber(submit2.item?.id) === requestId,
    "Repeated requestKey should return the same approval request"
  );

  const requestScope = await resolveBankApprovalRequestScope(requestId, tenantId);
  assert(requestScope?.scopeType === "LEGAL_ENTITY", "Approval request scope should resolve to LEGAL_ENTITY");
  assert(
    toNumber(requestScope?.scopeId) === legalEntityId,
    "Approval request scopeId should match legalEntityId"
  );

  await expectFailure(
    () =>
      approveBankApprovalRequest({
        req: null,
        tenantId,
        requestId,
        userId: requesterUserId,
        decisionComment: "self-approval should fail",
        assertScopeAccess: noScopeGuard,
      }),
    { status: 403, includes: "Maker-checker violation" }
  );

  const approveRes = await approveBankApprovalRequest({
    req: null,
    tenantId,
    requestId,
    userId: approverUserId,
    decisionComment: "approved by checker",
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(approveRes?.item?.request_status || "").toUpperCase() === "APPROVED",
    "Approval request should be APPROVED after checker decision"
  );
  assert(
    String(approveRes?.item?.execution_status || "").toUpperCase() === "NOT_EXECUTED",
    "Auto execution should stay NOT_EXECUTED when policy autoExecuteOnFinalApproval is false"
  );
  assert(
    Number(approveRes?.item?.approvals_granted || 0) >= 1,
    "Approved request should record at least one approval decision"
  );

  const readBackApproved = await getBankApprovalRequestById({
    req: null,
    tenantId,
    requestId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(readBackApproved?.request_status || "").toUpperCase() === "APPROVED",
    "Approval request readback should be APPROVED"
  );

  const rejectSubmit = await submitBankApprovalRequest({
    tenantId,
    userId: requesterUserId,
    requestInput: {
      moduleCode: "BANK",
      requestKey: `B09_REQ_REJECT_${stamp}`,
      policyCode: policy.policy_code,
      targetType: "RECON_RULE",
      targetId: 9002,
      actionType: "UPDATE",
      legalEntityId,
      thresholdAmount: 300,
      currencyCode: "TRY",
      targetSnapshot: { target_id: 9002 },
      actionPayload: { ruleId: 9002 },
    },
  });
  const rejectRequestId = toNumber(rejectSubmit.item?.id);
  assert(rejectRequestId > 0, "Reject test request creation failed");

  const rejectRes = await rejectBankApprovalRequest({
    req: null,
    tenantId,
    requestId: rejectRequestId,
    userId: approverUserId,
    decisionComment: "rejecting for smoke coverage",
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(rejectRes?.item?.request_status || "").toUpperCase() === "REJECTED",
    "Rejected request should become REJECTED"
  );

  const decisionStats = await query(
    `SELECT
        SUM(CASE WHEN decision = 'APPROVE' THEN 1 ELSE 0 END) AS approve_count,
        SUM(CASE WHEN decision = 'REJECT' THEN 1 ELSE 0 END) AS reject_count
     FROM bank_approval_request_decisions
     WHERE tenant_id = ?`,
    [tenantId]
  );
  const approveCount = toNumber(decisionStats.rows?.[0]?.approve_count);
  const rejectCount = toNumber(decisionStats.rows?.[0]?.reject_count);
  assert(approveCount >= 1, "Expected at least one APPROVE decision row");
  assert(rejectCount >= 1, "Expected at least one REJECT decision row");

  console.log("PR-B09 smoke test passed (policy + submit + idempotency + maker-checker + approve/reject).");
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
