import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  applyPayrollProviderImport,
  createPayrollProviderConnection,
  listPayrollEmployeeProviderRefs,
  listPayrollProviderConnections,
  listPayrollProviderImportJobs,
  listSupportedPayrollProviders,
  previewPayrollProviderImport,
  upsertPayrollEmployeeProviderRef,
} from "../src/services/payroll.providers.service.js";
import { getPayrollRunByIdForTenant } from "../src/services/payroll.runs.service.js";

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

function allowAllScopeFilter() {
  return "1 = 1";
}

function denyScopeGuard() {
  const err = new Error("Scope access denied");
  err.status = 403;
  throw err;
}

async function expectFailure(work, { status, code, includes }) {
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
    if (code !== undefined && String(error?.code || "") !== String(code)) {
      throw new Error(`Expected error code ${code} but got ${String(error?.code || "")}`);
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
  const tenantCode = `PRP09_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRP09 Tenant ${stamp}`]
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
    [tenantId, `PRP09_G_${stamp}`, `PRP09 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP09_G_${stamp}`]
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
      `PRP09_LE_${stamp}`,
      `PRP09 Legal Entity ${stamp}`,
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
    [tenantId, `PRP09_LE_${stamp}`]
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

function buildProviderPayload({
  alphaName = "Alpha User",
  betaName = "Beta User",
  includeUnmapped = false,
}) {
  const rows = [
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
  ];
  if (includeUnmapped) {
    rows.push({
      external_employee_id: "EXT-UNMAPPED",
      employee_name: "Unmapped User",
      cost_center_code: "CC-03",
      base_salary: 800,
      overtime_pay: 0,
      bonus_pay: 0,
      allowances_total: 100,
      gross_pay: 900,
      employee_tax: 90,
      employee_social_security: 45,
      other_deductions: 10,
      employer_tax: 100,
      employer_social_security: 80,
      net_pay: 755,
    });
  }
  return { employees: rows };
}

async function seedClosedMarchPeriodLock({ tenantId, legalEntityId, userId }) {
  await query(
    `INSERT INTO payroll_period_closes (
        tenant_id,
        legal_entity_id,
        period_start,
        period_end,
        status,
        lock_run_changes,
        lock_manual_settlements,
        lock_payment_prep,
        close_note,
        closed_by_user_id,
        closed_at
      )
      VALUES (?, ?, '2026-03-01', '2026-03-31', 'CLOSED', 1, 1, 0, ?, ?, CURRENT_TIMESTAMP)`,
    [tenantId, legalEntityId, "Seeded lock for PRP09 smoke", userId]
  );
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const supported = listSupportedPayrollProviders();
  const supportedCodes = new Set((supported || []).map((row) => String(row?.provider_code || "")));
  assert(supportedCodes.has("GENERIC_JSON"), "GENERIC_JSON adapter must be registered");
  assert(supportedCodes.has("GENERIC_CSV"), "GENERIC_CSV adapter must be registered");

  const stamp = Date.now();
  const fixture = await createTenantWithLegalEntity(stamp);

  const passwordHash = await bcrypt.hash("PRP09#Smoke123", 10);
  const makerUserId = await createUser({
    tenantId: fixture.tenantId,
    email: `prp09_maker_${stamp}@example.com`,
    name: "PRP09 Maker",
    passwordHash,
  });
  const checkerUserId = await createUser({
    tenantId: fixture.tenantId,
    email: `prp09_checker_${stamp}@example.com`,
    name: "PRP09 Checker",
    passwordHash,
  });

  const providerCode = "GENERIC_JSON";
  const connection = await createPayrollProviderConnection({
    req: null,
    tenantId: fixture.tenantId,
    userId: makerUserId,
    input: {
      legalEntityId: fixture.legalEntityId,
      providerCode,
      providerName: "Generic JSON Payroll Provider",
      adapterVersion: "v1",
      status: "ACTIVE",
      isDefault: true,
      settingsJson: { delimiter: "," },
    },
    assertScopeAccess: noScopeGuard,
  });
  const connectionId = toNumber(connection?.id);
  assert(connectionId > 0, "Provider connection should be created");
  assert(connection?.has_secrets === false, "Provider connection should be created without secrets");

  const connectionsList = await listPayrollProviderConnections({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      tenantId: fixture.tenantId,
      legalEntityId: fixture.legalEntityId,
      providerCode,
      status: "ACTIVE",
      limit: 50,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert((connectionsList?.rows || []).length === 1, "Should list exactly one active provider connection");
  assert(toNumber(connectionsList?.rows?.[0]?.id) === connectionId, "Listed connection id should match");

  await upsertPayrollEmployeeProviderRef({
    req: null,
    tenantId: fixture.tenantId,
    userId: makerUserId,
    input: {
      legalEntityId: fixture.legalEntityId,
      providerCode,
      externalEmployeeId: "EXT-E001",
      externalEmployeeCode: "EXT-CODE-E001",
      internalEmployeeCode: "E001",
      internalEmployeeName: "Alpha User",
      status: "ACTIVE",
      isPrimary: true,
      payloadJson: { source: "PRP09 smoke" },
    },
    assertScopeAccess: noScopeGuard,
  });
  await upsertPayrollEmployeeProviderRef({
    req: null,
    tenantId: fixture.tenantId,
    userId: makerUserId,
    input: {
      legalEntityId: fixture.legalEntityId,
      providerCode,
      externalEmployeeId: "EXT-E002",
      externalEmployeeCode: "EXT-CODE-E002",
      internalEmployeeCode: "E002",
      internalEmployeeName: "Beta User",
      status: "ACTIVE",
      isPrimary: true,
      payloadJson: { source: "PRP09 smoke" },
    },
    assertScopeAccess: noScopeGuard,
  });

  const refs = await listPayrollEmployeeProviderRefs({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      tenantId: fixture.tenantId,
      legalEntityId: fixture.legalEntityId,
      providerCode,
      q: null,
      limit: 100,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert((refs?.rows || []).length === 2, "Two provider refs should be listed");

  const validPayload = buildProviderPayload({});
  const previewA = await previewPayrollProviderImport({
    req: null,
    tenantId: fixture.tenantId,
    userId: makerUserId,
    input: {
      payrollProviderConnectionId: connectionId,
      payrollPeriod: "2026-02-01",
      periodStart: "2026-02-01",
      periodEnd: "2026-02-28",
      payDate: "2026-02-28",
      currencyCode: fixture.currencyCode,
      sourceFormat: "JSON",
      sourceFilename: `prp09-valid-${stamp}.json`,
      sourceBatchRef: `PRP09-VALID-${stamp}`,
      rawPayloadText: JSON.stringify(validPayload, null, 2),
      importKey: `PRP09-IMPORT-A-${stamp}`,
    },
    assertScopeAccess: noScopeGuard,
  });
  const previewAJobId = toNumber(previewA?.job?.id);
  assert(previewAJobId > 0, "Preview A should create import job");
  assert(previewA?.idempotent_preview === false, "First preview should not be idempotent");
  assert(String(previewA?.job?.status || "").toUpperCase() === "PREVIEWED", "Preview job status must be PREVIEWED");
  assert(
    Number(previewA?.preview_summary?.employee_count_mapped || 0) === 2,
    "Preview summary should report 2 mapped employees"
  );
  assert(previewA?.preview_summary?.apply_blocked === false, "Valid preview should be applyable");

  const previewARepeat = await previewPayrollProviderImport({
    req: null,
    tenantId: fixture.tenantId,
    userId: makerUserId,
    input: {
      payrollProviderConnectionId: connectionId,
      payrollPeriod: "2026-02-01",
      periodStart: "2026-02-01",
      periodEnd: "2026-02-28",
      payDate: "2026-02-28",
      currencyCode: fixture.currencyCode,
      sourceFormat: "JSON",
      sourceFilename: `prp09-valid-${stamp}.json`,
      sourceBatchRef: `PRP09-VALID-${stamp}`,
      rawPayloadText: JSON.stringify(validPayload, null, 2),
      importKey: `PRP09-IMPORT-A-${stamp}`,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(previewARepeat?.idempotent_preview === true, "Repeat preview should be idempotent");
  assert(toNumber(previewARepeat?.job?.id) === previewAJobId, "Repeat preview should return same import job");

  const unmatchedPayload = buildProviderPayload({ includeUnmapped: true });
  const previewB = await previewPayrollProviderImport({
    req: null,
    tenantId: fixture.tenantId,
    userId: makerUserId,
    input: {
      payrollProviderConnectionId: connectionId,
      payrollPeriod: "2026-02-01",
      periodStart: "2026-02-01",
      periodEnd: "2026-02-28",
      payDate: "2026-02-28",
      currencyCode: fixture.currencyCode,
      sourceFormat: "JSON",
      sourceFilename: `prp09-unmatched-${stamp}.json`,
      sourceBatchRef: `PRP09-UNMATCHED-${stamp}`,
      rawPayloadText: JSON.stringify(unmatchedPayload, null, 2),
      importKey: `PRP09-IMPORT-B-${stamp}`,
    },
    assertScopeAccess: noScopeGuard,
  });
  const previewBJobId = toNumber(previewB?.job?.id);
  assert(previewBJobId > 0, "Preview B should create import job");
  assert(previewB?.preview_summary?.apply_blocked === true, "Unmatched preview must be blocked");
  assert(
    Number(previewB?.preview_summary?.employee_count_unmatched || 0) >= 1,
    "Unmatched preview should report unmatched employees"
  );

  await expectFailure(
    () =>
      applyPayrollProviderImport({
        req: null,
        tenantId: fixture.tenantId,
        userId: makerUserId,
        importJobId: previewAJobId,
        input: {
          applyIdempotencyKey: `PRP09-APPLY-SELF-${stamp}`,
          note: "maker should be blocked",
          allowSameUserApply: false,
        },
        assertScopeAccess: noScopeGuard,
        skipUnifiedApprovalGate: true,
      }),
    { status: 403, includes: "Maker-checker violation" }
  );

  const applyKey = `PRP09-APPLY-A-${stamp}`;
  const applied = await applyPayrollProviderImport({
    req: null,
    tenantId: fixture.tenantId,
    userId: checkerUserId,
    importJobId: previewAJobId,
    input: {
      applyIdempotencyKey: applyKey,
      note: "apply preview A",
      allowSameUserApply: false,
    },
    assertScopeAccess: noScopeGuard,
    skipUnifiedApprovalGate: true,
  });
  assert(String(applied?.job?.status || "").toUpperCase() === "APPLIED", "Preview A apply should mark APPLIED");
  const appliedRunId = toNumber(applied?.job?.applied_payroll_run_id || applied?.applied_payroll_run?.id);
  assert(appliedRunId > 0, "Applied preview should produce payroll run");

  const appliedRun = await getPayrollRunByIdForTenant({
    req: null,
    tenantId: fixture.tenantId,
    runId: appliedRunId,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(appliedRun?.source_type || "").toUpperCase() === "PROVIDER_IMPORT",
    "Applied run should be marked as PROVIDER_IMPORT"
  );
  assert(
    String(appliedRun?.source_provider_code || "").toUpperCase() === providerCode,
    "Applied run should keep provider code traceability"
  );
  assert(
    toNumber(appliedRun?.source_provider_import_job_id) === previewAJobId,
    "Applied run should keep provider import job traceability"
  );

  const appliedRetry = await applyPayrollProviderImport({
    req: null,
    tenantId: fixture.tenantId,
    userId: checkerUserId,
    importJobId: previewAJobId,
    input: {
      applyIdempotencyKey: applyKey,
      note: "idempotent replay",
      allowSameUserApply: false,
    },
    assertScopeAccess: noScopeGuard,
    skipUnifiedApprovalGate: true,
  });
  assert(
    String(appliedRetry?.job?.status || "").toUpperCase() === "APPLIED",
    "Idempotent apply replay should remain APPLIED"
  );
  assert(
    toNumber(appliedRetry?.job?.applied_payroll_run_id || appliedRetry?.applied_payroll_run?.id) === appliedRunId,
    "Idempotent apply replay should return same payroll run"
  );

  await expectFailure(
    () =>
      applyPayrollProviderImport({
        req: null,
        tenantId: fixture.tenantId,
        userId: checkerUserId,
        importJobId: previewBJobId,
        input: {
          applyIdempotencyKey: `PRP09-APPLY-B-${stamp}`,
          note: "blocked preview should fail",
          allowSameUserApply: false,
        },
        assertScopeAccess: noScopeGuard,
        skipUnifiedApprovalGate: true,
      }),
    { status: 409, includes: "preview contains validation/matching errors" }
  );

  await seedClosedMarchPeriodLock({
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
    userId: checkerUserId,
  });

  const marchPayload = buildProviderPayload({
    alphaName: "Alpha User March",
    betaName: "Beta User March",
  });
  const previewC = await previewPayrollProviderImport({
    req: null,
    tenantId: fixture.tenantId,
    userId: makerUserId,
    input: {
      payrollProviderConnectionId: connectionId,
      payrollPeriod: "2026-03-01",
      periodStart: "2026-03-01",
      periodEnd: "2026-03-31",
      payDate: "2026-03-31",
      currencyCode: fixture.currencyCode,
      sourceFormat: "JSON",
      sourceFilename: `prp09-march-${stamp}.json`,
      sourceBatchRef: `PRP09-MARCH-${stamp}`,
      rawPayloadText: JSON.stringify(marchPayload, null, 2),
      importKey: `PRP09-IMPORT-C-${stamp}`,
    },
    assertScopeAccess: noScopeGuard,
  });
  const previewCJobId = toNumber(previewC?.job?.id);
  assert(previewCJobId > 0, "Preview C should create import job");
  assert(previewC?.preview_summary?.apply_blocked === false, "Preview C should be valid before lock gate");

  await expectFailure(
    () =>
      applyPayrollProviderImport({
        req: null,
        tenantId: fixture.tenantId,
        userId: checkerUserId,
        importJobId: previewCJobId,
        input: {
          applyIdempotencyKey: `PRP09-APPLY-C-${stamp}`,
          note: "should be blocked by closed-period run lock",
          allowSameUserApply: false,
        },
        assertScopeAccess: noScopeGuard,
        skipUnifiedApprovalGate: true,
      }),
    { status: 409, code: "PAYROLL_PERIOD_LOCKED" }
  );

  const importsList = await listPayrollProviderImportJobs({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      tenantId: fixture.tenantId,
      legalEntityId: fixture.legalEntityId,
      providerCode,
      status: null,
      payrollPeriod: null,
      cursor: null,
      limit: 20,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert((importsList?.rows || []).length >= 3, "Import jobs list should include created previews");

  await expectFailure(
    () =>
      listPayrollProviderConnections({
        req: null,
        tenantId: fixture.tenantId,
        filters: {
          tenantId: fixture.tenantId,
          legalEntityId: fixture.legalEntityId,
          providerCode,
          status: "ACTIVE",
          limit: 20,
          offset: 0,
        },
        buildScopeFilter: allowAllScopeFilter,
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  await expectFailure(
    () =>
      upsertPayrollEmployeeProviderRef({
        req: null,
        tenantId: fixture.tenantId,
        userId: makerUserId,
        input: {
          legalEntityId: fixture.legalEntityId,
          providerCode,
          externalEmployeeId: "EXT-DENY",
          externalEmployeeCode: "EXT-DENY",
          internalEmployeeCode: "E099",
          internalEmployeeName: "Denied User",
          status: "ACTIVE",
          isPrimary: true,
          payloadJson: null,
        },
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  await expectFailure(
    () =>
      previewPayrollProviderImport({
        req: null,
        tenantId: fixture.tenantId,
        userId: makerUserId,
        input: {
          payrollProviderConnectionId: connectionId,
          payrollPeriod: "2026-04-01",
          periodStart: "2026-04-01",
          periodEnd: "2026-04-30",
          payDate: "2026-04-30",
          currencyCode: fixture.currencyCode,
          sourceFormat: "JSON",
          sourceFilename: `prp09-deny-${stamp}.json`,
          sourceBatchRef: `PRP09-DENY-${stamp}`,
          rawPayloadText: JSON.stringify(validPayload, null, 2),
          importKey: `PRP09-IMPORT-DENY-${stamp}`,
        },
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  await expectFailure(
    () =>
      applyPayrollProviderImport({
        req: null,
        tenantId: fixture.tenantId,
        userId: checkerUserId,
        importJobId: previewBJobId,
        input: {
          applyIdempotencyKey: `PRP09-APPLY-DENY-${stamp}`,
          note: "deny guard check",
          allowSameUserApply: false,
        },
        assertScopeAccess: denyScopeGuard,
        skipUnifiedApprovalGate: true,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  console.log(
    "PR-P09 smoke test passed (provider adapters/connections/refs, preview+idempotency, apply maker-checker+idempotency, period-lock gating, list pagination paths, permission denial checks)."
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
