import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  approveAndClosePayrollPeriod,
  assertPayrollPeriodActionAllowed,
  preparePayrollPeriodClose,
  reopenPayrollPeriodClose,
  requestPayrollPeriodClose,
} from "../src/services/payroll.close.service.js";

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
  const tenantCode = `PRP08_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRP08 Tenant ${stamp}`]
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
    [tenantId, `PRP08_G_${stamp}`, `PRP08 Group ${stamp}`]
  );
  const groupRows = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, `PRP08_G_${stamp}`]
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
      `PRP08_LE_${stamp}`,
      `PRP08 Legal Entity ${stamp}`,
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
    [tenantId, `PRP08_LE_${stamp}`]
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

async function main() {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const stamp = Date.now();
  const { tenantId, legalEntityId } = await createTenantWithLegalEntity(stamp);

  await seedCore({ ensureDefaultTenantIfMissing: true });

  const passwordHash = await bcrypt.hash("PRP08#Smoke123", 10);
  const makerUserId = await createUser({
    tenantId,
    email: `prp08_maker_${stamp}@example.com`,
    name: "PRP08 Maker",
    passwordHash,
  });
  const checkerUserId = await createUser({
    tenantId,
    email: `prp08_checker_${stamp}@example.com`,
    name: "PRP08 Checker",
    passwordHash,
  });

  const periodStart = "2026-02-01";
  const periodEnd = "2026-02-28";

  const prepared = await preparePayrollPeriodClose({
    req: null,
    tenantId,
    userId: makerUserId,
    input: {
      legalEntityId,
      periodStart,
      periodEnd,
      lockRunChanges: true,
      lockManualSettlements: true,
      lockPaymentPrep: false,
      note: "prepare for PRP08 smoke",
    },
    assertScopeAccess: noScopeGuard,
  });
  const closeId = toNumber(prepared?.close?.id);
  assert(closeId > 0, "preparePayrollPeriodClose did not return close id");
  assert(
    String(prepared?.close?.status || "").toUpperCase() === "READY",
    "Prepared payroll period close should be READY when checklist passes"
  );

  const requestIdempotencyKey = `PRP08_REQ_${stamp}`;
  const requested1 = await requestPayrollPeriodClose({
    req: null,
    tenantId,
    userId: makerUserId,
    closeId,
    note: "request close",
    requestIdempotencyKey,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(requested1?.close?.status || "").toUpperCase() === "REQUESTED",
    "requestPayrollPeriodClose should move close to REQUESTED"
  );

  const requested2 = await requestPayrollPeriodClose({
    req: null,
    tenantId,
    userId: makerUserId,
    closeId,
    note: "request close idempotent retry",
    requestIdempotencyKey,
    assertScopeAccess: noScopeGuard,
  });
  assert(
    String(requested2?.close?.status || "").toUpperCase() === "REQUESTED",
    "Idempotent request-close retry should keep REQUESTED status"
  );

  await expectFailure(
    () =>
      approveAndClosePayrollPeriod({
        req: null,
        tenantId,
        userId: makerUserId,
        closeId,
        note: "self-approve should fail",
        closeIdempotencyKey: `PRP08_CLOSE_SELF_${stamp}`,
        assertScopeAccess: noScopeGuard,
        skipUnifiedApprovalGate: true,
      }),
    { status: 403, includes: "Maker-checker violation" }
  );

  const closeIdempotencyKey = `PRP08_CLOSE_${stamp}`;
  const closed = await approveAndClosePayrollPeriod({
    req: null,
    tenantId,
    userId: checkerUserId,
    closeId,
    note: "approved and closed",
    closeIdempotencyKey,
    assertScopeAccess: noScopeGuard,
    skipUnifiedApprovalGate: true,
  });
  assert(
    String(closed?.close?.status || "").toUpperCase() === "CLOSED",
    "approveAndClosePayrollPeriod should move close to CLOSED"
  );

  const closedRetry = await approveAndClosePayrollPeriod({
    req: null,
    tenantId,
    userId: checkerUserId,
    closeId,
    note: "close retry",
    closeIdempotencyKey,
    assertScopeAccess: noScopeGuard,
    skipUnifiedApprovalGate: true,
  });
  assert(
    String(closedRetry?.close?.status || "").toUpperCase() === "CLOSED",
    "Idempotent close retry should remain CLOSED"
  );

  await expectFailure(
    () =>
      assertPayrollPeriodActionAllowed({
        tenantId,
        legalEntityId,
        payrollPeriod: "2026-02-10",
        actionType: "RUN_IMPORT",
      }),
    { status: 409, code: "PAYROLL_PERIOD_LOCKED" }
  );

  await expectFailure(
    () =>
      assertPayrollPeriodActionAllowed({
        tenantId,
        legalEntityId,
        payrollPeriod: "2026-02-10",
        actionType: "MANUAL_SETTLEMENT_OVERRIDE",
      }),
    { status: 409, code: "PAYROLL_PERIOD_LOCKED" }
  );

  const paymentPrepAllowed = await assertPayrollPeriodActionAllowed({
    tenantId,
    legalEntityId,
    payrollPeriod: "2026-02-10",
    actionType: "PAYMENT_PREP_BUILD",
  });
  assert(paymentPrepAllowed?.allowed === true, "PAYMENT_PREP action should be allowed when lock flag is false");

  const reopened = await reopenPayrollPeriodClose({
    req: null,
    tenantId,
    userId: checkerUserId,
    closeId,
    reason: "reopen for correction cycle",
    assertScopeAccess: noScopeGuard,
    skipUnifiedApprovalGate: true,
  });
  assert(
    String(reopened?.close?.status || "").toUpperCase() === "REOPENED",
    "reopenPayrollPeriodClose should move close to REOPENED"
  );

  const runAllowedAfterReopen = await assertPayrollPeriodActionAllowed({
    tenantId,
    legalEntityId,
    payrollPeriod: "2026-02-10",
    actionType: "RUN_IMPORT",
  });
  assert(
    runAllowedAfterReopen?.allowed === true,
    "RUN action should be allowed after period is reopened"
  );

  console.log(
    "PR-P08 smoke test passed (prepare/request/approve/reopen + maker-checker + lock enforcement + idempotency)."
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
