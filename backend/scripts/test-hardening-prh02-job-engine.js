import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  cancelJob,
  enqueueJob,
  getJobById,
  listJobs,
  requeueJob,
  runOneAvailableJob,
} from "../src/services/jobs.service.js";
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
  const tenantCode = `PRH02_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRH02 Tenant ${stamp}`]
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
    [tenantId, `PRH02_G_${stamp}`, `PRH02 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRH02_G_${stamp}`]
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
      `PRH02_LE_${stamp}`,
      `PRH02 Legal Entity ${stamp}`,
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
    [tenantId, `PRH02_LE_${stamp}`]
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

function buildProviderPayload() {
  return {
    employees: [
      {
        external_employee_id: "EXT-E001",
        external_employee_code: "EXT-CODE-E001",
        employee_name: "Alpha User",
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
        employee_name: "Beta User",
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

async function prepareProviderImportFixture({
  tenantId,
  legalEntityId,
  makerUserId,
  currencyCode,
  stamp,
}) {
  const providerCode = "GENERIC_JSON";
  const connection = await createPayrollProviderConnection({
    req: null,
    tenantId,
    userId: makerUserId,
    input: {
      legalEntityId,
      providerCode,
      providerName: "PRH02 Generic JSON Provider",
      adapterVersion: "v1",
      status: "ACTIVE",
      isDefault: true,
      settingsJson: {},
    },
    assertScopeAccess: noScopeGuard,
  });
  const connectionId = toNumber(connection?.id);
  assert(connectionId > 0, "Provider connection fixture should be created");

  await upsertPayrollEmployeeProviderRef({
    req: null,
    tenantId,
    userId: makerUserId,
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
    userId: makerUserId,
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

  const preview = await previewPayrollProviderImport({
    req: null,
    tenantId,
    userId: makerUserId,
    input: {
      payrollProviderConnectionId: connectionId,
      payrollPeriod: "2026-02-01",
      periodStart: "2026-02-01",
      periodEnd: "2026-02-28",
      payDate: "2026-02-28",
      currencyCode,
      sourceFormat: "JSON",
      sourceFilename: `prh02-${stamp}.json`,
      sourceBatchRef: `PRH02-${stamp}`,
      rawPayloadText: JSON.stringify(buildProviderPayload(), null, 2),
      importKey: `PRH02-IMPORT-${stamp}`,
    },
    assertScopeAccess: noScopeGuard,
  });

  const importJobId = toNumber(preview?.job?.id);
  assert(importJobId > 0, "Provider preview fixture should create import job");
  assert(preview?.preview_summary?.apply_blocked === false, "Provider preview fixture should be applyable");
  return { importJobId, providerCode };
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithLegalEntity(stamp);
  const passwordHash = await bcrypt.hash("PRH02#Smoke123", 10);

  const makerUserId = await createUser({
    tenantId: fixture.tenantId,
    email: `prh02_maker_${stamp}@example.com`,
    name: "PRH02 Maker",
    passwordHash,
  });
  const checkerUserId = await createUser({
    tenantId: fixture.tenantId,
    email: `prh02_checker_${stamp}@example.com`,
    name: "PRH02 Checker",
    passwordHash,
  });

  const { importJobId } = await prepareProviderImportFixture({
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
    makerUserId,
    currencyCode: fixture.currencyCode,
    stamp,
  });

  const applyIdemKey = `PRH02-APPLY-JOB-${stamp}`;
  const applyJob1 = await enqueueJob({
    tenantId: fixture.tenantId,
    userId: makerUserId,
    spec: {
      queue_name: "payroll.imports",
      module_code: "PAYROLL",
      job_type: "PAYROLL_IMPORT_APPLY",
      priority: 50,
      run_after_at: new Date(Date.now() - 60 * 1000),
      idempotency_key: applyIdemKey,
      max_attempts: 5,
      payload: {
        import_job_id: importJobId,
        acting_user_id: checkerUserId,
        allow_same_user_apply: false,
        apply_note: "PRH02 async apply integration",
      },
    },
  });
  const applyJob1Id = toNumber(applyJob1?.job?.id);
  assert(applyJob1Id > 0, "enqueueJob should create payroll apply job");
  assert(applyJob1?.idempotent === false, "First enqueue should not be idempotent");

  const applyJob2 = await enqueueJob({
    tenantId: fixture.tenantId,
    userId: makerUserId,
    spec: {
      queue_name: "payroll.imports",
      module_code: "PAYROLL",
      job_type: "PAYROLL_IMPORT_APPLY",
      priority: 50,
      run_after_at: new Date(Date.now() - 60 * 1000),
      idempotency_key: applyIdemKey,
      max_attempts: 5,
      payload: {
        import_job_id: importJobId,
        acting_user_id: checkerUserId,
      },
    },
  });
  assert(applyJob2?.idempotent === true, "Second enqueue with same idempotency key should be idempotent");
  assert(toNumber(applyJob2?.job?.id) === applyJob1Id, "Idempotent enqueue should return same job id");

  const queueBeforeRun = await listJobs({
    tenantId: fixture.tenantId,
    filters: {
      queueName: "payroll.imports",
      status: "QUEUED",
      limit: 50,
      offset: 0,
    },
  });
  assert(
    (queueBeforeRun?.items || []).some((row) => toNumber(row?.id) === applyJob1Id),
    "Queued payroll apply job should be listed"
  );

  const runApply = await runOneAvailableJob({
    tenantId: fixture.tenantId,
    workerId: `prh02-worker-${stamp}`,
    queueNames: ["payroll.imports"],
  });
  assert(runApply?.idle === false, "Worker should claim payroll apply job");
  assert(runApply?.ok === true, "Payroll apply job should succeed");
  assert(String(runApply?.status || "").toUpperCase() === "SUCCEEDED", "Run result should be SUCCEEDED");
  assert(toNumber(runApply?.job_id) === applyJob1Id, "Succeeded run should report payroll apply job id");

  const applyJobDetail = await getJobById({
    tenantId: fixture.tenantId,
    jobId: applyJob1Id,
  });
  assert(
    String(applyJobDetail?.item?.status || "").toUpperCase() === "SUCCEEDED",
    "Payroll apply job status should be SUCCEEDED after worker run"
  );
  assert(toNumber(applyJobDetail?.item?.attempt_count) === 1, "Successful job attempt_count should be 1");
  assert((applyJobDetail?.attempts || []).length >= 1, "Successful job should have attempt history");
  assert(
    String(applyJobDetail?.attempts?.[0]?.status || "").toUpperCase() === "SUCCEEDED",
    "Successful job latest attempt status should be SUCCEEDED"
  );

  const importDetail = await getPayrollProviderImportJobDetail({
    req: null,
    tenantId: fixture.tenantId,
    importJobId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(importDetail?.job?.status || "").toUpperCase() === "APPLIED",
    "PAYROLL_IMPORT_APPLY handler should move provider import job to APPLIED"
  );
  assert(
    toNumber(importDetail?.job?.applied_payroll_run_id) > 0,
    "PAYROLL_IMPORT_APPLY handler should create linked payroll run"
  );

  const runApplyIdle = await runOneAvailableJob({
    tenantId: fixture.tenantId,
    workerId: `prh02-worker-${stamp}`,
    queueNames: ["payroll.imports"],
  });
  assert(runApplyIdle?.idle === true, "Worker should report idle when queue has no due jobs");

  const badJob = await enqueueJob({
    tenantId: fixture.tenantId,
    userId: makerUserId,
    spec: {
      queue_name: "ops.default",
      module_code: "OPS",
      job_type: "PRH02_UNKNOWN_JOB",
      priority: 100,
      run_after_at: new Date(Date.now() - 60 * 1000),
      idempotency_key: `PRH02-UNKNOWN-${stamp}`,
      max_attempts: 2,
      payload: { note: "unsupported handler smoke" },
    },
  });
  const badJobId = toNumber(badJob?.job?.id);
  assert(badJobId > 0, "Unsupported job fixture should be created");

  const runBad1 = await runOneAvailableJob({
    tenantId: fixture.tenantId,
    workerId: `prh02-worker-${stamp}`,
    queueNames: ["ops.default"],
  });
  assert(runBad1?.idle === false, "Worker should claim unsupported job");
  assert(runBad1?.ok === false, "Unsupported job should fail");
  assert(String(runBad1?.status || "").toUpperCase() === "FAILED_FINAL", "Unsupported job should be FAILED_FINAL");
  assert(runBad1?.retryable === false, "Unsupported job failure should be non-retryable");

  const badJobAfterRun1 = await getJobById({
    tenantId: fixture.tenantId,
    jobId: badJobId,
  });
  assert(
    String(badJobAfterRun1?.item?.status || "").toUpperCase() === "FAILED_FINAL",
    "Unsupported job status should be FAILED_FINAL"
  );
  assert(toNumber(badJobAfterRun1?.item?.attempt_count) === 1, "Unsupported job attempt_count should be 1");
  assert((badJobAfterRun1?.attempts || []).length >= 1, "Unsupported job should have attempt history");
  assert(
    String(badJobAfterRun1?.attempts?.[0]?.status || "").toUpperCase() === "FAILED_FINAL",
    "Unsupported job latest attempt should be FAILED_FINAL"
  );

  const requeued = await requeueJob({
    tenantId: fixture.tenantId,
    jobId: badJobId,
    userId: checkerUserId,
    delaySeconds: 0,
    maxAttempts: 3,
  });
  assert(String(requeued?.item?.status || "").toUpperCase() === "QUEUED", "Requeued job should be QUEUED");
  await query(
    `UPDATE app_jobs
     SET run_after_at = DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 MINUTE)
     WHERE tenant_id = ? AND id = ?`,
    [fixture.tenantId, badJobId]
  );

  const runBad2 = await runOneAvailableJob({
    tenantId: fixture.tenantId,
    workerId: `prh02-worker-${stamp}`,
    queueNames: ["ops.default"],
  });
  assert(runBad2?.idle === false, "Worker should claim requeued job");
  assert(String(runBad2?.status || "").toUpperCase() === "FAILED_FINAL", "Requeued unsupported job should fail");
  assert(toNumber(runBad2?.attempt_no) >= 2, "Requeued job should increment attempt number");

  const badJobAfterRun2 = await getJobById({
    tenantId: fixture.tenantId,
    jobId: badJobId,
  });
  assert(
    toNumber(badJobAfterRun2?.item?.attempt_count) >= 2,
    "Requeued + rerun job should have attempt_count >= 2"
  );

  const cancelFixture = await enqueueJob({
    tenantId: fixture.tenantId,
    userId: makerUserId,
    spec: {
      queue_name: "ops.default",
      module_code: "OPS",
      job_type: "PRH02_CANCEL_FIXTURE",
      priority: 200,
      idempotency_key: `PRH02-CANCEL-${stamp}`,
      max_attempts: 1,
      run_after_at: new Date(Date.now() + 60 * 60 * 1000),
      payload: { note: "cancel fixture" },
    },
  });
  const cancelFixtureJobId = toNumber(cancelFixture?.job?.id);
  assert(cancelFixtureJobId > 0, "Cancel fixture job should be created");

  const cancelled = await cancelJob({
    tenantId: fixture.tenantId,
    jobId: cancelFixtureJobId,
    userId: checkerUserId,
  });
  assert(
    String(cancelled?.item?.status || "").toUpperCase() === "CANCELLED",
    "cancelJob should move queued job to CANCELLED"
  );

  const cancelledAgain = await cancelJob({
    tenantId: fixture.tenantId,
    jobId: cancelFixtureJobId,
    userId: checkerUserId,
  });
  assert(
    String(cancelledAgain?.item?.status || "").toUpperCase() === "CANCELLED",
    "Second cancel should remain CANCELLED (idempotent)"
  );

  await expectFailure(
    () =>
      cancelJob({
        tenantId: fixture.tenantId,
        jobId: applyJob1Id,
        userId: checkerUserId,
      }),
    { status: 409, includes: "SUCCEEDED jobs cannot be cancelled" }
  );

  const cancelledList = await listJobs({
    tenantId: fixture.tenantId,
    filters: {
      status: "CANCELLED",
      limit: 50,
      offset: 0,
    },
  });
  assert(
    (cancelledList?.items || []).some((row) => toNumber(row?.id) === cancelFixtureJobId),
    "CANCELLED jobs list should include cancelled fixture job"
  );

  console.log(
    "PR-H02 smoke test passed (enqueue idempotency, worker run success/failure, attempts/audit state, requeue/cancel lifecycle, payroll import apply integration)."
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
