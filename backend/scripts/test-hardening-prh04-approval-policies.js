import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  approveApprovalRequest,
  createApprovalPolicy,
  evaluateApprovalNeed,
  getApprovalRequestById,
  listApprovalPolicies,
  listApprovalRequestRows,
  rejectApprovalRequest,
  submitApprovalRequest,
} from "../src/services/approvalPolicies.service.js";
import {
  createPayrollProviderConnection,
  getPayrollProviderImportJobDetail,
  previewPayrollProviderImport,
  upsertPayrollEmployeeProviderRef,
} from "../src/services/payroll.providers.service.js";

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

function denyScopeGuard() {
  const err = new Error("Scope access denied");
  err.status = 403;
  throw err;
}

function allowAllScopeFilter() {
  return "1 = 1";
}

function tenantWideReq() {
  return {
    rbac: {
      scopeContext: {
        tenantWide: true,
        legalEntities: new Set(),
      },
    },
  };
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
  const tenantCode = `PRH04_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRH04 Tenant ${stamp}`]
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
    [tenantId, `PRH04_G_${stamp}`, `PRH04 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRH04_G_${stamp}`]
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
      `PRH04_LE_${stamp}`,
      `PRH04 Legal Entity ${stamp}`,
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
    [tenantId, `PRH04_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  return { tenantId, legalEntityId, currencyCode };
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
  assert(userId > 0, `Failed to create user fixture: ${email}`);
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

function buildProviderPayload({
  alphaName = "Alpha User",
  betaName = "Beta User",
} = {}) {
  return {
    employees: [
      {
        external_employee_id: "EXT-E001",
        external_employee_code: "EXT-CODE-E001",
        employee_name: alphaName,
        cost_center_code: "CC-01",
        base_salary: 1000,
        overtime_pay: 100,
        bonus_pay: 50,
        allowances_total: 50,
        gross_pay: 1200,
        employee_tax: 120,
        employee_social_security: 80,
        other_deductions: 20,
        employer_tax: 150,
        employer_social_security: 100,
        net_pay: 980,
      },
      {
        external_employee_id: "EXT-E002",
        external_employee_code: "EXT-CODE-E002",
        employee_name: betaName,
        cost_center_code: "CC-02",
        base_salary: 900,
        overtime_pay: 0,
        bonus_pay: 0,
        allowances_total: 100,
        gross_pay: 1000,
        employee_tax: 100,
        employee_social_security: 50,
        other_deductions: 10,
        employer_tax: 120,
        employer_social_security: 90,
        net_pay: 840,
      },
    ],
  };
}

async function setupProviderConnectionAndRefs({
  tenantId,
  legalEntityId,
  userId,
}) {
  const providerCode = "GENERIC_JSON";
  const connection = await createPayrollProviderConnection({
    req: null,
    tenantId,
    userId,
    input: {
      legalEntityId,
      providerCode,
      providerName: "PRH04 Generic JSON Provider",
      adapterVersion: "v1",
      status: "ACTIVE",
      isDefault: true,
      settingsJson: {},
    },
    assertScopeAccess: noScopeGuard,
  });
  const connectionId = toNumber(connection?.id);
  assert(connectionId > 0, "Provider connection should be created");

  await upsertPayrollEmployeeProviderRef({
    req: null,
    tenantId,
    userId,
    input: {
      legalEntityId,
      providerCode,
      externalEmployeeId: "EXT-E001",
      externalEmployeeCode: "EXT-CODE-E001",
      internalEmployeeCode: "E001",
      internalEmployeeName: "Alpha User",
      status: "ACTIVE",
      isPrimary: true,
      payloadJson: null,
    },
    assertScopeAccess: noScopeGuard,
  });
  await upsertPayrollEmployeeProviderRef({
    req: null,
    tenantId,
    userId,
    input: {
      legalEntityId,
      providerCode,
      externalEmployeeId: "EXT-E002",
      externalEmployeeCode: "EXT-CODE-E002",
      internalEmployeeCode: "E002",
      internalEmployeeName: "Beta User",
      status: "ACTIVE",
      isPrimary: true,
      payloadJson: null,
    },
    assertScopeAccess: noScopeGuard,
  });

  return { providerCode, connectionId };
}

async function previewProviderImport({
  tenantId,
  userId,
  connectionId,
  currencyCode,
  payrollPeriod,
  payDate,
  sourceSuffix,
  importKey,
}) {
  const preview = await previewPayrollProviderImport({
    req: null,
    tenantId,
    userId,
    input: {
      payrollProviderConnectionId: connectionId,
      payrollPeriod,
      periodStart: payrollPeriod,
      periodEnd: payrollPeriod,
      payDate,
      currencyCode,
      sourceFormat: "JSON",
      sourceFilename: `prh04-${sourceSuffix}.json`,
      sourceBatchRef: `PRH04-${sourceSuffix}`,
      rawPayloadText: JSON.stringify(buildProviderPayload(), null, 2),
      importKey,
    },
    assertScopeAccess: noScopeGuard,
  });
  const importJobId = toNumber(preview?.job?.id);
  assert(importJobId > 0, "Provider preview should create import job");
  assert(preview?.preview_summary?.apply_blocked === false, "Provider preview should be applyable");
  return importJobId;
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithLegalEntity(stamp);

  // Ensure role/permission bindings are created for the newly inserted tenant.
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const passwordHash = await bcrypt.hash("PRH04#Smoke123", 10);
  const requesterUserId = await createUser({
    tenantId: fixture.tenantId,
    email: `prh04_requester_${stamp}@example.com`,
    name: "PRH04 Requester",
    passwordHash,
  });
  const approver1UserId = await createUser({
    tenantId: fixture.tenantId,
    email: `prh04_approver1_${stamp}@example.com`,
    name: "PRH04 Approver One",
    passwordHash,
  });
  const approver2UserId = await createUser({
    tenantId: fixture.tenantId,
    email: `prh04_approver2_${stamp}@example.com`,
    name: "PRH04 Approver Two",
    passwordHash,
  });

  await assignTenantAdminRole({ tenantId: fixture.tenantId, userId: requesterUserId });
  await assignTenantAdminRole({ tenantId: fixture.tenantId, userId: approver1UserId });
  await assignTenantAdminRole({ tenantId: fixture.tenantId, userId: approver2UserId });

  const providerFixture = await setupProviderConnectionAndRefs({
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
    userId: requesterUserId,
  });

  const importJobId1 = await previewProviderImport({
    tenantId: fixture.tenantId,
    userId: requesterUserId,
    connectionId: providerFixture.connectionId,
    currencyCode: fixture.currencyCode,
    payrollPeriod: "2026-02-01",
    payDate: "2026-02-28",
    sourceSuffix: `${stamp}-1`,
    importKey: `PRH04-IMPORT-${stamp}-1`,
  });
  const importJobId2 = await previewProviderImport({
    tenantId: fixture.tenantId,
    userId: requesterUserId,
    connectionId: providerFixture.connectionId,
    currencyCode: fixture.currencyCode,
    payrollPeriod: "2026-03-01",
    payDate: "2026-03-31",
    sourceSuffix: `${stamp}-2`,
    importKey: `PRH04-IMPORT-${stamp}-2`,
  });

  const lowPolicy = await createApprovalPolicy({
    req: null,
    input: {
      tenantId: fixture.tenantId,
      userId: requesterUserId,
      policyCode: `PRH04_LOW_${stamp}`,
      policyName: "PRH04 Payroll Import Low Threshold",
      moduleCode: "PAYROLL",
      status: "ACTIVE",
      targetType: "PAYROLL_PROVIDER_IMPORT",
      actionType: "APPLY",
      scopeType: "LEGAL_ENTITY",
      legalEntityId: fixture.legalEntityId,
      bankAccountId: null,
      currencyCode: fixture.currencyCode,
      minAmount: 0,
      maxAmount: 1000,
      requiredApprovals: 1,
      makerCheckerRequired: true,
      approverPermissionCode: "approvals.requests.approve",
      autoExecuteOnFinalApproval: true,
      effectiveFrom: null,
      effectiveTo: null,
    },
    assertScopeAccess: noScopeGuard,
  });
  const highPolicy = await createApprovalPolicy({
    req: null,
    input: {
      tenantId: fixture.tenantId,
      userId: requesterUserId,
      policyCode: `PRH04_HIGH_${stamp}`,
      policyName: "PRH04 Payroll Import High Threshold",
      moduleCode: "PAYROLL",
      status: "ACTIVE",
      targetType: "PAYROLL_PROVIDER_IMPORT",
      actionType: "APPLY",
      scopeType: "LEGAL_ENTITY",
      legalEntityId: fixture.legalEntityId,
      bankAccountId: null,
      currencyCode: fixture.currencyCode,
      minAmount: 1000.01,
      maxAmount: 50000,
      requiredApprovals: 2,
      makerCheckerRequired: true,
      approverPermissionCode: "approvals.requests.approve",
      autoExecuteOnFinalApproval: true,
      effectiveFrom: null,
      effectiveTo: null,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(toNumber(lowPolicy?.id) > 0, "Low-threshold policy should be created");
  assert(toNumber(highPolicy?.id) > 0, "High-threshold policy should be created");

  const policiesList = await listApprovalPolicies({
    req: tenantWideReq(),
    tenantId: fixture.tenantId,
    filters: {
      tenantId: fixture.tenantId,
      legalEntityId: fixture.legalEntityId,
      bankAccountId: null,
      status: "ACTIVE",
      moduleCode: "PAYROLL",
      targetType: "PAYROLL_PROVIDER_IMPORT",
      actionType: "APPLY",
      scopeType: "LEGAL_ENTITY",
      q: null,
      limit: 50,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert((policiesList?.rows || []).length >= 2, "Unified policy list should include created payroll policies");

  const evalHigh = await evaluateApprovalNeed({
    moduleCode: "PAYROLL",
    tenantId: fixture.tenantId,
    targetType: "PAYROLL_PROVIDER_IMPORT",
    actionType: "APPLY",
    legalEntityId: fixture.legalEntityId,
    thresholdAmount: 2500,
    currencyCode: fixture.currencyCode,
  });
  assert(evalHigh?.approval_required === true, "High threshold evaluation should require approval");
  assert(
    String(evalHigh?.policy?.policy_code || "") === String(highPolicy.policy_code),
    "High threshold evaluation should resolve high policy"
  );

  const evalLow = await evaluateApprovalNeed({
    moduleCode: "PAYROLL",
    tenantId: fixture.tenantId,
    targetType: "PAYROLL_PROVIDER_IMPORT",
    actionType: "APPLY",
    legalEntityId: fixture.legalEntityId,
    thresholdAmount: 500,
    currencyCode: fixture.currencyCode,
  });
  assert(evalLow?.approval_required === true, "Low threshold evaluation should require approval");
  assert(
    String(evalLow?.policy?.policy_code || "") === String(lowPolicy.policy_code),
    "Low threshold evaluation should resolve low policy"
  );

  const requestKey1 = `PRH04_REQ_${stamp}_1`;
  const submitted1 = await submitApprovalRequest({
    tenantId: fixture.tenantId,
    userId: requesterUserId,
    requestInput: {
      moduleCode: "PAYROLL",
      requestKey: requestKey1,
      targetType: "PAYROLL_PROVIDER_IMPORT",
      targetId: importJobId1,
      actionType: "APPLY",
      legalEntityId: fixture.legalEntityId,
      thresholdAmount: 2500,
      currencyCode: fixture.currencyCode,
      actionPayload: {
        importJobId: importJobId1,
        note: "PRH04 auto-exec apply",
        allowSameUserApply: false,
      },
      targetSnapshot: {
        module_code: "PAYROLL",
        target_type: "PAYROLL_PROVIDER_IMPORT",
        target_id: importJobId1,
        legal_entity_id: fixture.legalEntityId,
      },
    },
  });
  const requestId1 = toNumber(submitted1?.item?.id);
  assert(requestId1 > 0, "First unified approval request should be created");
  assert(submitted1?.approval_required === true, "First request should require approval");
  assert(submitted1?.idempotent === false, "First submit should not be idempotent");
  assert(
    toNumber(submitted1?.item?.required_approvals) === 2,
    "First request should inherit required_approvals=2 from high policy"
  );

  const submitted1Dup = await submitApprovalRequest({
    tenantId: fixture.tenantId,
    userId: requesterUserId,
    requestInput: {
      moduleCode: "PAYROLL",
      requestKey: requestKey1,
      targetType: "PAYROLL_PROVIDER_IMPORT",
      targetId: importJobId1,
      actionType: "APPLY",
      legalEntityId: fixture.legalEntityId,
      thresholdAmount: 2500,
      currencyCode: fixture.currencyCode,
      actionPayload: { importJobId: importJobId1 },
      targetSnapshot: { target_id: importJobId1 },
    },
  });
  assert(submitted1Dup?.idempotent === true, "Duplicate requestKey submit should be idempotent");
  assert(
    toNumber(submitted1Dup?.item?.id) === requestId1,
    "Duplicate requestKey submit should return same request id"
  );

  await expectFailure(
    () =>
      approveApprovalRequest({
        req: null,
        tenantId: fixture.tenantId,
        requestId: requestId1,
        userId: requesterUserId,
        decisionComment: "self-approve should fail",
        assertScopeAccess: noScopeGuard,
      }),
    { status: 403, includes: "Maker-checker violation" }
  );

  const firstApproval = await approveApprovalRequest({
    req: null,
    tenantId: fixture.tenantId,
    requestId: requestId1,
    userId: approver1UserId,
    decisionComment: "first checker approve",
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(firstApproval?.item?.request_status || "").toUpperCase() === "PENDING",
    "After first approval of required_approvals=2, request should remain PENDING"
  );
  assert(
    toNumber(firstApproval?.item?.approvals_granted) === 1,
    "After first approval, approvals_granted should be 1"
  );

  const finalApproval = await approveApprovalRequest({
    req: null,
    tenantId: fixture.tenantId,
    requestId: requestId1,
    userId: approver2UserId,
    decisionComment: "second checker approve + execute",
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(finalApproval?.item?.request_status || "").toUpperCase() === "EXECUTED",
    "Final approval should auto-execute and set request_status=EXECUTED"
  );
  assert(
    String(finalApproval?.item?.execution_status || "").toUpperCase() === "EXECUTED",
    "Final approval should auto-execute and set execution_status=EXECUTED"
  );
  assert(
    toNumber(finalApproval?.item?.approvals_granted) >= 2,
    "Final approval should record at least two approve decisions"
  );
  assert(finalApproval?.execution_result, "Final approval should return execution_result");

  const readExecuted = await getApprovalRequestById({
    req: null,
    tenantId: fixture.tenantId,
    requestId: requestId1,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(readExecuted?.request_status || "").toUpperCase() === "EXECUTED",
    "Readback after final approval should be EXECUTED"
  );

  const importDetail1 = await getPayrollProviderImportJobDetail({
    req: null,
    tenantId: fixture.tenantId,
    importJobId: importJobId1,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(importDetail1?.job?.status || "").toUpperCase() === "APPLIED",
    "Auto-executed approval should apply payroll provider import"
  );
  assert(
    toNumber(importDetail1?.job?.applied_payroll_run_id) > 0,
    "Auto-executed approval should link applied payroll run id"
  );

  const requestKey2 = `PRH04_REQ_${stamp}_2`;
  const submitted2 = await submitApprovalRequest({
    tenantId: fixture.tenantId,
    userId: requesterUserId,
    requestInput: {
      moduleCode: "PAYROLL",
      requestKey: requestKey2,
      targetType: "PAYROLL_PROVIDER_IMPORT",
      targetId: importJobId2,
      actionType: "APPLY",
      legalEntityId: fixture.legalEntityId,
      thresholdAmount: 500,
      currencyCode: fixture.currencyCode,
      actionPayload: {
        importJobId: importJobId2,
        note: "PRH04 rejection flow",
      },
      targetSnapshot: {
        module_code: "PAYROLL",
        target_type: "PAYROLL_PROVIDER_IMPORT",
        target_id: importJobId2,
        legal_entity_id: fixture.legalEntityId,
      },
    },
  });
  const requestId2 = toNumber(submitted2?.item?.id);
  assert(requestId2 > 0, "Second unified approval request should be created");
  assert(
    toNumber(submitted2?.item?.required_approvals) === 1,
    "Second request should inherit required_approvals=1 from low policy"
  );

  const rejected2 = await rejectApprovalRequest({
    req: null,
    tenantId: fixture.tenantId,
    requestId: requestId2,
    userId: approver1UserId,
    decisionComment: "rejecting in H04 smoke",
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(rejected2?.item?.request_status || "").toUpperCase() === "REJECTED",
    "Reject path should set request_status=REJECTED"
  );
  assert(
    String(rejected2?.item?.execution_status || "").toUpperCase() === "NOT_EXECUTED",
    "Reject path should keep execution_status=NOT_EXECUTED"
  );

  await expectFailure(
    () =>
      approveApprovalRequest({
        req: null,
        tenantId: fixture.tenantId,
        requestId: requestId2,
        userId: approver2UserId,
        decisionComment: "approve rejected should fail",
        assertScopeAccess: noScopeGuard,
      }),
    { status: 409, includes: "Rejected approval request cannot be approved" }
  );

  const requestsList = await listApprovalRequestRows({
    req: { user: { userId: requesterUserId } },
    tenantId: fixture.tenantId,
    filters: {
      tenantId: fixture.tenantId,
      moduleCode: "PAYROLL",
      requestStatus: null,
      targetType: "PAYROLL_PROVIDER_IMPORT",
      actionType: "APPLY",
      mineOnly: false,
      limit: 50,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  const listedIds = new Set((requestsList?.rows || []).map((row) => toNumber(row?.id)));
  assert(listedIds.has(requestId1), "Request list should include first request");
  assert(listedIds.has(requestId2), "Request list should include second request");

  const decisionStats = await query(
    `SELECT
        SUM(CASE WHEN decision = 'APPROVE' THEN 1 ELSE 0 END) AS approve_count,
        SUM(CASE WHEN decision = 'REJECT' THEN 1 ELSE 0 END) AS reject_count
     FROM bank_approval_request_decisions
     WHERE tenant_id = ?`,
    [fixture.tenantId]
  );
  assert(toNumber(decisionStats.rows?.[0]?.approve_count) >= 2, "Expected at least two approve decisions");
  assert(toNumber(decisionStats.rows?.[0]?.reject_count) >= 1, "Expected at least one reject decision");

  await expectFailure(
    () =>
      createApprovalPolicy({
        req: null,
        input: {
          tenantId: fixture.tenantId,
          userId: requesterUserId,
          policyCode: `PRH04_DENY_${stamp}`,
          policyName: "Denied policy create",
          moduleCode: "PAYROLL",
          status: "ACTIVE",
          targetType: "PAYROLL_PROVIDER_IMPORT",
          actionType: "APPLY",
          scopeType: "LEGAL_ENTITY",
          legalEntityId: fixture.legalEntityId,
          bankAccountId: null,
          currencyCode: fixture.currencyCode,
          minAmount: 0,
          maxAmount: 10,
          requiredApprovals: 1,
          makerCheckerRequired: true,
          approverPermissionCode: "approvals.requests.approve",
          autoExecuteOnFinalApproval: true,
          effectiveFrom: null,
          effectiveTo: null,
        },
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  await expectFailure(
    () =>
      getApprovalRequestById({
        req: null,
        tenantId: fixture.tenantId,
        requestId: requestId1,
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  console.log(
    "PR-H04 smoke test passed (policy precedence by threshold, request idempotency, maker-checker + multi-approver, auto-execution to payroll import apply, reject flow, scope checks)."
  );
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
