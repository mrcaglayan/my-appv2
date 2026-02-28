import bcrypt from "bcrypt";
import crypto from "node:crypto";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import { assertScopeAccess, buildScopeFilter } from "../src/middleware/rbac.js";
import {
  createBankAccount,
  getBankAccountByIdForTenant,
  listBankAccountRows,
} from "../src/services/bank.accounts.service.js";
import {
  getPayrollProviderImportJobDetail,
  listPayrollProviderConnections,
  listPayrollProviderImportJobs,
} from "../src/services/payroll.providers.service.js";
import {
  claimExceptionWorkbench,
  listExceptionWorkbenchRows,
} from "../src/services/exceptions.workbench.service.js";

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function toInt(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function sha256(value) {
  return crypto.createHash("sha256").update(String(value || ""), "utf8").digest("hex");
}

function buildReq({ tenantWide = false, legalEntityIds = [] } = {}) {
  return {
    rbac: {
      scopeContext: {
        tenantWide: Boolean(tenantWide),
        groups: new Set(),
        countries: new Set(),
        legalEntities: new Set((legalEntityIds || []).map((id) => toInt(id)).filter(Boolean)),
        operatingUnits: new Set(),
      },
    },
  };
}

async function expectFailure(work, { status, includes }) {
  try {
    await work();
  } catch (error) {
    if (status !== undefined && Number(error?.status || 0) !== Number(status)) {
      throw new Error(`Expected status=${status} got status=${String(error?.status)} msg=${String(error?.message || "")}`);
    }
    if (includes) {
      const msg = String(error?.message || "").toLowerCase();
      if (!msg.includes(String(includes).toLowerCase())) {
        throw new Error(`Expected message to include "${includes}", got "${String(error?.message || "")}"`);
      }
    }
    return;
  }
  throw new Error("Expected operation to fail, but it succeeded");
}

async function createTenantWithLegalEntities({ stamp, tenantSuffix, legalEntityCount }) {
  const tenantCode = `PRH09_T_${tenantSuffix}_${stamp}`;
  await query(`INSERT INTO tenants (code, name) VALUES (?, ?)`, [
    tenantCode,
    `PRH09 Tenant ${tenantSuffix} ${stamp}`,
  ]);
  const tenantId = toInt(
    (
      await query(
        `SELECT id
         FROM tenants
         WHERE code = ?
         LIMIT 1`,
        [tenantCode]
      )
    ).rows?.[0]?.id
  );
  assert(tenantId > 0, `Failed to create tenant ${tenantSuffix}`);

  const country = (
    await query(
      `SELECT id, default_currency_code
       FROM countries
       WHERE iso2 = 'TR'
       LIMIT 1`
    )
  ).rows?.[0];
  const countryId = toInt(country?.id);
  const currencyCode = String(country?.default_currency_code || "TRY");
  assert(countryId > 0, "Missing country seed row (TR)");

  const groupCode = `PRH09_G_${tenantSuffix}_${stamp}`;
  await query(`INSERT INTO group_companies (tenant_id, code, name) VALUES (?, ?, ?)`, [
    tenantId,
    groupCode,
    `PRH09 Group ${tenantSuffix} ${stamp}`,
  ]);
  const groupCompanyId = toInt(
    (
      await query(
        `SELECT id
         FROM group_companies
         WHERE tenant_id = ?
           AND code = ?
         LIMIT 1`,
        [tenantId, groupCode]
      )
    ).rows?.[0]?.id
  );
  assert(groupCompanyId > 0, `Failed to create group company for tenant ${tenantSuffix}`);

  const legalEntityIds = [];
  for (let i = 1; i <= Number(legalEntityCount || 0); i += 1) {
    const leCode = `PRH09_LE_${tenantSuffix}_${i}_${stamp}`;
    await query(
      `INSERT INTO legal_entities (
          tenant_id, group_company_id, code, name, country_id, functional_currency_code, status
        ) VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
      [
        tenantId,
        groupCompanyId,
        leCode,
        `PRH09 Legal Entity ${tenantSuffix}-${i} ${stamp}`,
        countryId,
        currencyCode,
      ]
    );
    const leId = toInt(
      (
        await query(
          `SELECT id
           FROM legal_entities
           WHERE tenant_id = ?
             AND code = ?
           LIMIT 1`,
          [tenantId, leCode]
        )
      ).rows?.[0]?.id
    );
    assert(leId > 0, `Failed to create legal entity ${tenantSuffix}-${i}`);
    legalEntityIds.push(leId);
  }

  return { tenantId, legalEntityIds, currencyCode };
}

async function createUser({ tenantId, stamp, label }) {
  const passwordHash = await bcrypt.hash("PRH09#Smoke123", 10);
  const email = `prh09_${label}_${stamp}@example.com`;
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, email, passwordHash, `PRH09 ${label}`]
  );
  const userId = toInt(
    (
      await query(
        `SELECT id
         FROM users
         WHERE tenant_id = ?
           AND email = ?
         LIMIT 1`,
        [tenantId, email]
      )
    ).rows?.[0]?.id
  );
  assert(userId > 0, `Failed to create user ${label}`);
  return userId;
}

async function createBankFixture({ tenantId, legalEntityId, userId, currencyCode, stamp, label }) {
  await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [
      tenantId,
      legalEntityId,
      `PRH09_COA_${label}_${stamp}`,
      `PRH09 CoA ${label} ${stamp}`,
    ]
  );
  const coaId = toInt(
    (
      await query(
        `SELECT id
         FROM charts_of_accounts
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND code = ?
         LIMIT 1`,
        [tenantId, legalEntityId, `PRH09_COA_${label}_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(coaId > 0, `Failed to create CoA for ${label}`);

  await query(
    `INSERT INTO accounts (coa_id, code, name, account_type, normal_side, allow_posting, is_active)
     VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, TRUE)`,
    [coaId, `PRH09_BANK_GL_${label}_${stamp}`, `PRH09 Bank GL ${label} ${stamp}`]
  );
  const bankGlAccountId = toInt(
    (
      await query(
        `SELECT id
         FROM accounts
         WHERE coa_id = ?
           AND code = ?
         LIMIT 1`,
        [coaId, `PRH09_BANK_GL_${label}_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(bankGlAccountId > 0, `Failed to create bank GL account for ${label}`);

  await query(
    `INSERT INTO bank_accounts (
        tenant_id, legal_entity_id, code, name, currency_code, gl_account_id,
        bank_name, branch_name, iban, account_no, is_active, created_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, 'PRH09 Bank', 'Main', ?, ?, TRUE, ?)`,
    [
      tenantId,
      legalEntityId,
      `PRH09_BANK_${label}_${stamp}`,
      `PRH09 Bank ${label} ${stamp}`,
      currencyCode,
      bankGlAccountId,
      `TR${String(stamp).slice(-12)}${String(label).slice(-4)}`,
      `ACCT-${label}-${stamp}`,
      userId,
    ]
  );
  const bankAccountId = toInt(
    (
      await query(
        `SELECT id
         FROM bank_accounts
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND code = ?
         LIMIT 1`,
        [tenantId, legalEntityId, `PRH09_BANK_${label}_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(bankAccountId > 0, `Failed to create bank account for ${label}`);

  await query(
    `INSERT INTO bank_statement_imports (
        tenant_id, legal_entity_id, bank_account_id, original_filename, file_checksum, imported_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?)`,
    [
      tenantId,
      legalEntityId,
      bankAccountId,
      `prh09-${label}-${stamp}.csv`,
      sha256(`PRH09-IMP-${label}-${stamp}`),
      userId,
    ]
  );
  const statementImportId = toInt(
    (
      await query(
        `SELECT id
         FROM bank_statement_imports
         WHERE tenant_id = ?
           AND legal_entity_id = ?
         ORDER BY id DESC
         LIMIT 1`,
        [tenantId, legalEntityId]
      )
    ).rows?.[0]?.id
  );
  assert(statementImportId > 0, `Failed to create statement import for ${label}`);

  await query(
    `INSERT INTO bank_statement_lines (
        tenant_id, legal_entity_id, import_id, bank_account_id, line_no, txn_date,
        description, amount, currency_code, line_hash, recon_status
      ) VALUES (?, ?, ?, ?, 1, CURDATE(), ?, 125.50, ?, ?, 'UNMATCHED')`,
    [
      tenantId,
      legalEntityId,
      statementImportId,
      bankAccountId,
      `PRH09 stmt ${label}`,
      currencyCode,
      sha256(`PRH09-LINE-${label}-${stamp}`),
    ]
  );
  const statementLineId = toInt(
    (
      await query(
        `SELECT id
         FROM bank_statement_lines
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND import_id = ?
           AND line_no = 1
         LIMIT 1`,
        [tenantId, legalEntityId, statementImportId]
      )
    ).rows?.[0]?.id
  );
  assert(statementLineId > 0, `Failed to create statement line for ${label}`);

  await query(
    `INSERT INTO bank_reconciliation_exceptions (
        tenant_id, legal_entity_id, statement_line_id, bank_account_id,
        status, severity, reason_code, reason_message, first_seen_at
      ) VALUES (?, ?, ?, ?, 'OPEN', 'HIGH', 'RULE_MISS', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 HOUR))`,
    [tenantId, legalEntityId, statementLineId, bankAccountId, `PRH09 reason ${label}`]
  );

  const exceptionId = toInt(
    (
      await query(
        `SELECT id
         FROM bank_reconciliation_exceptions
         WHERE tenant_id = ?
           AND legal_entity_id = ?
         ORDER BY id DESC
         LIMIT 1`,
        [tenantId, legalEntityId]
      )
    ).rows?.[0]?.id
  );
  assert(exceptionId > 0, `Failed to create reconciliation exception for ${label}`);

  return { bankAccountId, bankGlAccountId, exceptionId };
}

async function createPayrollProviderFixture({
  tenantId,
  legalEntityId,
  userId,
  currencyCode,
  stamp,
  label,
}) {
  await query(
    `INSERT INTO payroll_provider_connections (
        tenant_id, legal_entity_id, provider_code, provider_name,
        status, is_default, created_by_user_id, updated_by_user_id
      ) VALUES (?, ?, 'GENERIC_JSON', ?, 'ACTIVE', 1, ?, ?)`,
    [tenantId, legalEntityId, `PRH09 Provider ${label} ${stamp}`, userId, userId]
  );
  const connectionId = toInt(
    (
      await query(
        `SELECT id
         FROM payroll_provider_connections
         WHERE tenant_id = ?
           AND legal_entity_id = ?
         ORDER BY id DESC
         LIMIT 1`,
        [tenantId, legalEntityId]
      )
    ).rows?.[0]?.id
  );
  assert(connectionId > 0, `Failed to create provider connection for ${label}`);

  await query(
    `INSERT INTO payroll_provider_import_jobs (
        tenant_id, legal_entity_id, payroll_provider_connection_id,
        provider_code, adapter_version, payroll_period,
        period_start, period_end, pay_date, currency_code,
        import_key, raw_payload_hash, normalized_payload_hash,
        source_format, source_filename, status,
        preview_summary_json, validation_errors_json, match_errors_json, match_warnings_json,
        raw_payload_text, normalized_payload_json, requested_by_user_id, requested_at
      ) VALUES (?, ?, ?, 'GENERIC_JSON', 'v1', '2026-02-01', '2026-02-01', '2026-02-28', '2026-02-28', ?, ?, ?, ?, 'JSON', ?, 'PREVIEWED', '{}', '[]', '[]', '[]', '{}', '{}', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 HOUR))`,
    [
      tenantId,
      legalEntityId,
      connectionId,
      currencyCode,
      `PRH09_IMPORT_${label}_${stamp}`,
      sha256(`PRH09-RAW-${label}-${stamp}`),
      sha256(`PRH09-NORM-${label}-${stamp}`),
      `prh09-${label}-${stamp}.json`,
      userId,
    ]
  );
  const importJobId = toInt(
    (
      await query(
        `SELECT id
         FROM payroll_provider_import_jobs
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND import_key = ?
         LIMIT 1`,
        [tenantId, legalEntityId, `PRH09_IMPORT_${label}_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(importJobId > 0, `Failed to create provider import job for ${label}`);

  return { connectionId, importJobId };
}

async function setupFixture(stamp) {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const tenantA = await createTenantWithLegalEntities({
    stamp,
    tenantSuffix: "A",
    legalEntityCount: 2,
  });
  const tenantB = await createTenantWithLegalEntities({
    stamp,
    tenantSuffix: "B",
    legalEntityCount: 1,
  });

  const userA = await createUser({ tenantId: tenantA.tenantId, stamp, label: "A" });
  const userB = await createUser({ tenantId: tenantB.tenantId, stamp, label: "B" });

  const bankA1 = await createBankFixture({
    tenantId: tenantA.tenantId,
    legalEntityId: tenantA.legalEntityIds[0],
    userId: userA,
    currencyCode: tenantA.currencyCode,
    stamp,
    label: "A1",
  });
  const bankA2 = await createBankFixture({
    tenantId: tenantA.tenantId,
    legalEntityId: tenantA.legalEntityIds[1],
    userId: userA,
    currencyCode: tenantA.currencyCode,
    stamp,
    label: "A2",
  });
  const bankB1 = await createBankFixture({
    tenantId: tenantB.tenantId,
    legalEntityId: tenantB.legalEntityIds[0],
    userId: userB,
    currencyCode: tenantB.currencyCode,
    stamp,
    label: "B1",
  });

  const payrollA1 = await createPayrollProviderFixture({
    tenantId: tenantA.tenantId,
    legalEntityId: tenantA.legalEntityIds[0],
    userId: userA,
    currencyCode: tenantA.currencyCode,
    stamp,
    label: "A1",
  });
  const payrollA2 = await createPayrollProviderFixture({
    tenantId: tenantA.tenantId,
    legalEntityId: tenantA.legalEntityIds[1],
    userId: userA,
    currencyCode: tenantA.currencyCode,
    stamp,
    label: "A2",
  });
  const payrollB1 = await createPayrollProviderFixture({
    tenantId: tenantB.tenantId,
    legalEntityId: tenantB.legalEntityIds[0],
    userId: userB,
    currencyCode: tenantB.currencyCode,
    stamp,
    label: "B1",
  });

  return {
    tenantA: {
      ...tenantA,
      userId: userA,
      bank: { le1: bankA1, le2: bankA2 },
      payroll: { le1: payrollA1, le2: payrollA2 },
    },
    tenantB: {
      ...tenantB,
      userId: userB,
      bank: { le1: bankB1 },
      payroll: { le1: payrollB1 },
    },
  };
}

async function main() {
  const stamp = Date.now();
  const fixture = await setupFixture(stamp);

  const reqTenantWideA = buildReq({ tenantWide: true });
  const reqTenantWideB = buildReq({ tenantWide: true });
  const reqAOnlyLe1 = buildReq({ legalEntityIds: [fixture.tenantA.legalEntityIds[0]] });

  const bankScoped = await listBankAccountRows({
    req: reqAOnlyLe1,
    tenantId: fixture.tenantA.tenantId,
    filters: { legalEntityId: null, isActive: null, q: "", limit: 100, offset: 0 },
    buildScopeFilter,
    assertScopeAccess,
  });
  assert(toInt(bankScoped.total) === 1, "Scoped bank list should include only one legal entity");
  assert(toInt(bankScoped.rows?.[0]?.legal_entity_id) === fixture.tenantA.legalEntityIds[0], "Scoped bank row legal_entity_id mismatch");

  const bankWide = await listBankAccountRows({
    req: reqTenantWideA,
    tenantId: fixture.tenantA.tenantId,
    filters: { legalEntityId: null, isActive: null, q: "", limit: 100, offset: 0 },
    buildScopeFilter,
    assertScopeAccess,
  });
  assert(toInt(bankWide.total) === 2, "Tenant-wide bank list should include both legal entities");

  await expectFailure(
    () =>
      getBankAccountByIdForTenant({
        req: reqAOnlyLe1,
        tenantId: fixture.tenantA.tenantId,
        bankAccountId: fixture.tenantA.bank.le2.bankAccountId,
        assertScopeAccess,
      }),
    { status: 403, includes: "access denied" }
  );

  await expectFailure(
    () =>
      getBankAccountByIdForTenant({
        req: reqTenantWideA,
        tenantId: fixture.tenantA.tenantId,
        bankAccountId: fixture.tenantB.bank.le1.bankAccountId,
        assertScopeAccess,
      }),
    { status: 400, includes: "not found" }
  );

  await expectFailure(
    () =>
      createBankAccount({
        req: reqTenantWideA,
        payload: {
          tenantId: fixture.tenantA.tenantId,
          legalEntityId: fixture.tenantA.legalEntityIds[0],
          code: `PRH09_BAD_${stamp}`,
          name: "PRH09 Invalid Cross-Entity GL Link",
          currencyCode: fixture.tenantA.currencyCode,
          glAccountId: fixture.tenantA.bank.le2.bankGlAccountId,
          bankName: "PRH09",
          branchName: "Main",
          iban: `TR${String(stamp).slice(-12)}BAD`,
          accountNo: `BAD-${stamp}`,
          isActive: true,
          userId: fixture.tenantA.userId,
        },
        assertScopeAccess,
      }),
    { status: 400, includes: "must belong to legalentityid" }
  );

  const connectionsScoped = await listPayrollProviderConnections({
    req: reqAOnlyLe1,
    tenantId: fixture.tenantA.tenantId,
    filters: { legalEntityId: null, providerCode: null, status: null, limit: 100, offset: 0 },
    buildScopeFilter,
    assertScopeAccess,
  });
  assert(toInt(connectionsScoped.total) === 1, "Scoped provider connection list should include only one LE");
  assert(toInt(connectionsScoped.rows?.[0]?.legal_entity_id) === fixture.tenantA.legalEntityIds[0], "Scoped provider connection LE mismatch");

  const importJobsScoped = await listPayrollProviderImportJobs({
    req: reqAOnlyLe1,
    tenantId: fixture.tenantA.tenantId,
    filters: {
      legalEntityId: null,
      providerCode: null,
      status: null,
      payrollPeriod: null,
      limit: 100,
      offset: 0,
      cursor: null,
    },
    buildScopeFilter,
    assertScopeAccess,
  });
  assert((importJobsScoped.rows || []).length === 1, "Scoped provider import list should include only one LE");
  assert(toInt(importJobsScoped.rows?.[0]?.id) === fixture.tenantA.payroll.le1.importJobId, "Scoped provider import job id mismatch");

  await expectFailure(
    () =>
      getPayrollProviderImportJobDetail({
        req: reqAOnlyLe1,
        tenantId: fixture.tenantA.tenantId,
        importJobId: fixture.tenantA.payroll.le2.importJobId,
        assertScopeAccess,
      }),
    { status: 403, includes: "access denied" }
  );

  await expectFailure(
    () =>
      getPayrollProviderImportJobDetail({
        req: reqTenantWideA,
        tenantId: fixture.tenantA.tenantId,
        importJobId: fixture.tenantB.payroll.le1.importJobId,
        assertScopeAccess,
      }),
    { status: 404, includes: "not found" }
  );

  const workbenchScoped = await listExceptionWorkbenchRows({
    req: reqAOnlyLe1,
    tenantId: fixture.tenantA.tenantId,
    filters: { refresh: true, days: 30, limit: 100, offset: 0 },
    buildScopeFilter,
    assertScopeAccess,
  });
  assert(toInt(workbenchScoped.total) === 1, "Scoped workbench should include one exception row");
  assert(toInt(workbenchScoped.summary?.by_module?.BANK) === 1, "Scoped workbench BANK summary mismatch");

  const workbenchWideA = await listExceptionWorkbenchRows({
    req: reqTenantWideA,
    tenantId: fixture.tenantA.tenantId,
    filters: { refresh: true, days: 30, limit: 100, offset: 0 },
    buildScopeFilter,
    assertScopeAccess,
  });
  assert(toInt(workbenchWideA.total) === 2, "Tenant A workbench should include two legal entities");

  const workbenchWideB = await listExceptionWorkbenchRows({
    req: reqTenantWideB,
    tenantId: fixture.tenantB.tenantId,
    filters: { refresh: true, days: 30, limit: 100, offset: 0 },
    buildScopeFilter,
    assertScopeAccess,
  });
  assert(toInt(workbenchWideB.total) === 1, "Tenant B workbench should include only its own rows");

  const le2WorkbenchRow = (workbenchWideA.rows || []).find(
    (row) => toInt(row.legal_entity_id) === fixture.tenantA.legalEntityIds[1]
  );
  assert(le2WorkbenchRow?.id, "Expected LE2 exception row in tenant-wide workbench");

  await expectFailure(
    () =>
      claimExceptionWorkbench({
        req: reqAOnlyLe1,
        tenantId: fixture.tenantA.tenantId,
        exceptionId: le2WorkbenchRow.id,
        actorUserId: fixture.tenantA.userId,
        ownerUserId: fixture.tenantA.userId,
        note: "Should be blocked by LE scope",
        assertScopeAccess,
      }),
    { status: 403, includes: "access denied" }
  );

  console.log("PR-H09 smoke test passed (tenant/entity isolation, scoped lists, entity-bound ids, cross-tenant safety).");
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
