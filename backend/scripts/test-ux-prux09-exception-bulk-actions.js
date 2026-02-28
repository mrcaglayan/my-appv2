import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import { bulkActionExceptionWorkbench, getExceptionWorkbenchById } from "../src/services/exceptions.workbench.service.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toInt(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
}

function noScopeGuard() {
  return true;
}

function denyScopeGuard() {
  const err = new Error("Scope access denied");
  err.status = 403;
  throw err;
}

async function setupFixture(stamp) {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  await query(`INSERT INTO tenants (code, name) VALUES (?, ?)`, [`PRUX09_T_${stamp}`, `PRUX09 Tenant ${stamp}`]);
  const tenantId = toInt(
    (
      await query(
        `SELECT id
         FROM tenants
         WHERE code = ?
         LIMIT 1`,
        [`PRUX09_T_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(tenantId > 0, "Failed to create tenant fixture");

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
  assert(countryId > 0, "Missing TR country seed fixture");

  await query(`INSERT INTO group_companies (tenant_id, code, name) VALUES (?, ?, ?)`, [
    tenantId,
    `PRUX09_G_${stamp}`,
    `PRUX09 Group ${stamp}`,
  ]);
  const groupCompanyId = toInt(
    (
      await query(
        `SELECT id
         FROM group_companies
         WHERE tenant_id = ?
           AND code = ?
         LIMIT 1`,
        [tenantId, `PRUX09_G_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(groupCompanyId > 0, "Failed to create group company fixture");

  const legalEntityCode = `PRUX09_LE_${stamp}`;
  await query(
    `INSERT INTO legal_entities (
        tenant_id,
        group_company_id,
        code,
        name,
        country_id,
        functional_currency_code,
        status
      ) VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, groupCompanyId, legalEntityCode, `PRUX09 Legal Entity ${stamp}`, countryId, currencyCode]
  );
  const legalEntityId = toInt(
    (
      await query(
        `SELECT id
         FROM legal_entities
         WHERE tenant_id = ?
           AND code = ?
         LIMIT 1`,
        [tenantId, legalEntityCode]
      )
    ).rows?.[0]?.id
  );
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  const passwordHash = await bcrypt.hash("PRUX09#Smoke123", 10);
  const userEmail = `prux09_${stamp}@example.com`;
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, userEmail, passwordHash, "PRUX09 User"]
  );
  const userId = toInt(
    (
      await query(
        `SELECT id
         FROM users
         WHERE tenant_id = ?
           AND email = ?
         LIMIT 1`,
        [tenantId, userEmail]
      )
    ).rows?.[0]?.id
  );
  assert(userId > 0, "Failed to create user fixture");

  return { tenantId, legalEntityId, userId };
}

async function insertWorkbenchRow({ tenantId, legalEntityId, suffix, status, severity = "MEDIUM" }) {
  const sourceKey = `PRUX09:${suffix}`;
  await query(
    `INSERT INTO exception_workbench (
        tenant_id,
        legal_entity_id,
        module_code,
        exception_type,
        source_type,
        source_key,
        severity,
        status,
        title,
        description
      ) VALUES (?, ?, 'BANK', 'BANK_RECON_EXCEPTION', 'BANK_RECON_EXCEPTION', ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, sourceKey, severity, status, `PRUX09 ${suffix}`, `Fixture ${suffix}`]
  );

  const id = toInt(
    (
      await query(
        `SELECT id
         FROM exception_workbench
         WHERE tenant_id = ?
           AND source_key = ?
         LIMIT 1`,
        [tenantId, sourceKey]
      )
    ).rows?.[0]?.id
  );
  assert(id > 0, `Failed to create exception_workbench row for ${suffix}`);
  return id;
}

async function getStatus(tenantId, exceptionId) {
  const row = (
    await query(
      `SELECT status, owner_user_id
       FROM exception_workbench
       WHERE tenant_id = ?
         AND id = ?
       LIMIT 1`,
      [tenantId, exceptionId]
    )
  ).rows?.[0];

  return {
    status: String(row?.status || ""),
    ownerUserId: toInt(row?.owner_user_id, 0),
  };
}

async function main() {
  const stamp = Date.now();
  const fixture = await setupFixture(stamp);

  const openId = await insertWorkbenchRow({
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
    suffix: `${stamp}:OPEN`,
    status: "OPEN",
    severity: "HIGH",
  });
  const open2Id = await insertWorkbenchRow({
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
    suffix: `${stamp}:OPEN2`,
    status: "OPEN",
    severity: "MEDIUM",
  });
  const resolvedId = await insertWorkbenchRow({
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
    suffix: `${stamp}:RESOLVED`,
    status: "RESOLVED",
    severity: "LOW",
  });

  const claimResult = await bulkActionExceptionWorkbench({
    req: null,
    tenantId: fixture.tenantId,
    action: "claim",
    exceptionIds: [openId, resolvedId, 999999999, openId],
    actorUserId: fixture.userId,
    note: "Bulk claim note",
    continueOnError: true,
    assertScopeAccess: noScopeGuard,
  });
  assert(toInt(claimResult.requested) === 3, "Claim bulk requested should de-duplicate ids");
  assert(toInt(claimResult.succeeded) === 1, "Claim bulk should succeed for one OPEN item");
  assert(toInt(claimResult.failed) === 2, "Claim bulk should report two failures");

  const openAfterClaim = await getStatus(fixture.tenantId, openId);
  assert(openAfterClaim.status === "IN_REVIEW", "OPEN row should transition to IN_REVIEW after bulk claim");
  assert(openAfterClaim.ownerUserId === fixture.userId, "Bulk claim should set owner_user_id");

  const resolveResult = await bulkActionExceptionWorkbench({
    req: null,
    tenantId: fixture.tenantId,
    action: "resolve",
    exceptionIds: [openId, open2Id],
    actorUserId: fixture.userId,
    resolutionAction: "MANUAL_RESOLVE",
    resolutionNote: "Bulk resolve",
    continueOnError: true,
    assertScopeAccess: noScopeGuard,
  });
  assert(toInt(resolveResult.succeeded) === 2, "Resolve bulk should succeed for both rows");
  assert(toInt(resolveResult.failed) === 0, "Resolve bulk should not fail for OPEN/IN_REVIEW rows");

  const openAfterResolve = await getStatus(fixture.tenantId, openId);
  const open2AfterResolve = await getStatus(fixture.tenantId, open2Id);
  assert(openAfterResolve.status === "RESOLVED", "First row should be RESOLVED after bulk resolve");
  assert(open2AfterResolve.status === "RESOLVED", "Second row should be RESOLVED after bulk resolve");

  const stopOnErrorResult = await bulkActionExceptionWorkbench({
    req: null,
    tenantId: fixture.tenantId,
    action: "claim",
    exceptionIds: [999999999, open2Id],
    actorUserId: fixture.userId,
    continueOnError: false,
    assertScopeAccess: noScopeGuard,
  });
  assert(toInt(stopOnErrorResult.succeeded) === 0, "stopOnError claim should stop before success");
  assert(toInt(stopOnErrorResult.failed) === 1, "stopOnError claim should report first failure only");
  assert(Boolean(stopOnErrorResult.stopped_on_error), "stopOnError should set stopped_on_error=true");
  assert((stopOnErrorResult.results || []).length === 1, "stopOnError should only return first attempted result");

  const open2AfterStop = await getStatus(fixture.tenantId, open2Id);
  assert(open2AfterStop.status === "RESOLVED", "Second item should remain unchanged when stopOnError interrupts");

  const reopenResult = await bulkActionExceptionWorkbench({
    req: null,
    tenantId: fixture.tenantId,
    action: "reopen",
    exceptionIds: [openId, resolvedId],
    actorUserId: fixture.userId,
    resolutionNote: "Bulk reopen",
    continueOnError: true,
    assertScopeAccess: noScopeGuard,
  });
  assert(toInt(reopenResult.succeeded) === 2, "Reopen bulk should succeed for resolved rows");

  const scopeDeniedResult = await bulkActionExceptionWorkbench({
    req: null,
    tenantId: fixture.tenantId,
    action: "ignore",
    exceptionIds: [openId],
    actorUserId: fixture.userId,
    resolutionAction: "MANUAL_IGNORE",
    resolutionNote: "Scope denied run",
    continueOnError: true,
    assertScopeAccess: denyScopeGuard,
  });
  assert(toInt(scopeDeniedResult.succeeded) === 0, "Scope denied run should not succeed");
  assert(toInt(scopeDeniedResult.failed) === 1, "Scope denied run should return one failed result");
  assert(toInt(scopeDeniedResult.results?.[0]?.status, 0) === 403, "Scope denied run should report 403 status");

  const detail = await getExceptionWorkbenchById({
    req: null,
    tenantId: fixture.tenantId,
    exceptionId: openId,
    assertScopeAccess: noScopeGuard,
  });
  const auditEvents = new Set((detail.audit || []).map((row) => String(row.event_type || "")));
  assert(auditEvents.has("CLAIM"), "Audit trail should include CLAIM");
  assert(auditEvents.has("RESOLVED"), "Audit trail should include RESOLVED");
  assert(auditEvents.has("REOPEN"), "Audit trail should include REOPEN");

  console.log("PR-UX09 smoke test passed (exception workbench bulk actions with partial-success handling).");
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
