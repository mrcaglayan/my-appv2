import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  getPayrollRunByIdForTenant,
  importPayrollRunCsv,
  listPayrollRunLineRows,
  listPayrollRunRows,
} from "../src/services/payroll.runs.service.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function allowAllScopeFilter() {
  return "1 = 1";
}

function noScopeGuard() {
  return true;
}

function denyScopeGuard() {
  const err = new Error("Scope access denied");
  err.status = 403;
  throw err;
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

function buildValidCsv() {
  return [
    "employee_code,employee_name,cost_center_code,base_salary,overtime_pay,bonus_pay,allowances_total,gross_pay,employee_tax,employee_social_security,other_deductions,employer_tax,employer_social_security,net_pay",
    "E001,Alpha User,CC-01,1000,100,50,50,1200,120,80,0,150,100,1000",
    "E002,Beta User,CC-02,1500,0,0,0,1500,150,120,30,200,130,1200",
    "E002,Beta User,CC-02,1500,0,0,0,1500,150,120,30,200,130,1200",
  ].join("\n");
}

function buildInvalidCsvMissingColumn() {
  return [
    "employee_code,employee_name,cost_center_code,base_salary,overtime_pay,bonus_pay,allowances_total,gross_pay,employee_tax,employee_social_security,other_deductions,employer_tax,net_pay",
    "E001,Alpha User,CC-01,1000,100,50,50,1200,120,80,0,150,1000",
  ].join("\n");
}

async function createTenantWithLegalEntityAndUser(stamp) {
  const tenantCode = `PRP01_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRP01 Tenant ${stamp}`]
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
    [tenantId, `PRP01_G_${stamp}`, `PRP01 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP01_G_${stamp}`]
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
      `PRP01_LE_${stamp}`,
      `PRP01 Legal Entity ${stamp}`,
      countryId,
      currencyCode,
    ]
  );
  const legalEntityRows = await query(
    `SELECT id, code
     FROM legal_entities
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP01_LE_${stamp}`]
  );
  const legalEntityId = toNumber(legalEntityRows.rows?.[0]?.id);
  const legalEntityCode = String(legalEntityRows.rows?.[0]?.code || "");
  assert(legalEntityId > 0, "Failed to create legal entity fixture");
  assert(Boolean(legalEntityCode), "Legal entity code should exist");

  const passwordHash = await bcrypt.hash("PRP01#Smoke123", 10);
  const email = `prp01_user_${stamp}@example.com`;
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, email, passwordHash, "PRP01 User"]
  );
  const userRows = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, email]
  );
  const userId = toNumber(userRows.rows?.[0]?.id);
  assert(userId > 0, "Failed to create user fixture");

  return { tenantId, legalEntityId, legalEntityCode, userId, currencyCode };
}

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const fixture = await createTenantWithLegalEntityAndUser(stamp);

  const importPayload = {
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    legalEntityId: fixture.legalEntityId,
    providerCode: `PRP01_${stamp}`,
    payrollPeriod: "2026-02-01",
    payDate: "2026-02-28",
    currencyCode: fixture.currencyCode,
    sourceBatchRef: `SRC-${stamp}`,
    originalFilename: `prp01-${stamp}.csv`,
    csvText: buildValidCsv(),
  };

  const imported = await importPayrollRunCsv({
    req: null,
    payload: importPayload,
    assertScopeAccess: noScopeGuard,
  });
  const runId = toNumber(imported?.id);
  assert(runId > 0, "importPayrollRunCsv should return run id");
  assert(String(imported?.status || "").toUpperCase() === "IMPORTED", "Imported run should be IMPORTED");
  assert(
    String(imported?.entity_code || "").toUpperCase() === String(fixture.legalEntityCode).toUpperCase(),
    "Imported run entity_code should match legal entity code"
  );
  assert(toNumber(imported?.line_count_total) === 3, "line_count_total should include all parsed rows");
  assert(toNumber(imported?.line_count_inserted) === 2, "line_count_inserted should skip duplicate payroll line");
  assert(
    toNumber(imported?.line_count_duplicates) === 1,
    "line_count_duplicates should count duplicate payroll line hash"
  );
  assert(Array.isArray(imported?.lines) && imported.lines.length === 2, "Imported run detail should include 2 lines");
  assert(toNumber(imported?.employee_count) === 2, "employee_count should equal unique employees in CSV");

  const listed = await listPayrollRunRows({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      tenantId: fixture.tenantId,
      legalEntityId: fixture.legalEntityId,
      entityCode: null,
      providerCode: importPayload.providerCode,
      payrollPeriod: importPayload.payrollPeriod,
      status: "IMPORTED",
      q: null,
      limit: 100,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert((listed.rows || []).length === 1, "listPayrollRunRows should return the imported run");
  assert(toNumber(listed.rows?.[0]?.id) === runId, "Listed run id should match imported run");

  const detail = await getPayrollRunByIdForTenant({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    assertScopeAccess: noScopeGuard,
  });
  assert(toNumber(detail?.id) === runId, "getPayrollRunByIdForTenant should return the same run");
  const auditActions = new Set((detail?.audit || []).map((row) => String(row?.action || "").toUpperCase()));
  assert(auditActions.has("VALIDATION"), "Run audit should include VALIDATION");
  assert(auditActions.has("IMPORTED"), "Run audit should include IMPORTED");

  const lines = await listPayrollRunLineRows({
    req: null,
    tenantId: fixture.tenantId,
    runId,
    filters: {
      costCenterCode: null,
      q: null,
      limit: 200,
      offset: 0,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert((lines.rows || []).length === 2, "listPayrollRunLineRows should return inserted lines");

  await expectFailure(
    () =>
      importPayrollRunCsv({
        req: null,
        payload: { ...importPayload },
        assertScopeAccess: noScopeGuard,
      }),
    { status: 409, includes: "already imported" }
  );

  await expectFailure(
    () =>
      importPayrollRunCsv({
        req: null,
        payload: {
          ...importPayload,
          providerCode: `${importPayload.providerCode}_BAD`,
          sourceBatchRef: `${importPayload.sourceBatchRef}-BAD`,
          originalFilename: `invalid-${stamp}.csv`,
          csvText: buildInvalidCsvMissingColumn(),
        },
        assertScopeAccess: noScopeGuard,
      }),
    { status: 400, includes: "Missing CSV column" }
  );

  await expectFailure(
    () =>
      importPayrollRunCsv({
        req: null,
        payload: {
          ...importPayload,
          providerCode: `${importPayload.providerCode}_DENY`,
          sourceBatchRef: `${importPayload.sourceBatchRef}-DENY`,
          originalFilename: `deny-${stamp}.csv`,
        },
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  await expectFailure(
    () =>
      listPayrollRunRows({
        req: null,
        tenantId: fixture.tenantId,
        filters: {
          tenantId: fixture.tenantId,
          legalEntityId: fixture.legalEntityId,
          entityCode: null,
          providerCode: null,
          payrollPeriod: null,
          status: null,
          q: null,
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
      getPayrollRunByIdForTenant({
        req: null,
        tenantId: fixture.tenantId,
        runId,
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  console.log(
    "PR-P01 smoke test passed (import/list/detail/lines + checksum conflict + CSV validation + scope permission checks)."
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
