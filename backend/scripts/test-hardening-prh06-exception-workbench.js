import crypto from "node:crypto";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  claimExceptionWorkbench,
  getExceptionWorkbenchById,
  listExceptionWorkbenchRows,
  refreshExceptionWorkbench,
  reopenExceptionWorkbench,
  resolveExceptionWorkbench,
} from "../src/services/exceptions.workbench.service.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toInt(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
}

function sha256(value) {
  return crypto.createHash("sha256").update(String(value || ""), "utf8").digest("hex");
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

async function expectFailure(work, { status, includes }) {
  try {
    await work();
  } catch (error) {
    if (status !== undefined && Number(error?.status || 0) !== Number(status)) {
      throw new Error(`Expected ${status}, got ${String(error?.status)} ${String(error?.message || "")}`);
    }
    if (includes && !String(error?.message || "").includes(includes)) {
      throw new Error(`Expected message containing "${includes}", got "${String(error?.message || "")}"`);
    }
    return;
  }
  throw new Error("Expected operation to fail, but it succeeded");
}

async function setupFixture(stamp) {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  await query(`INSERT INTO tenants (code, name) VALUES (?, ?)`, [`PRH06_T_${stamp}`, `PRH06 Tenant ${stamp}`]);
  const tenantId = toInt(
    (
      await query(
        `SELECT id
         FROM tenants
         WHERE code = ?
         LIMIT 1`,
        [`PRH06_T_${stamp}`]
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
    `PRH06_G_${stamp}`,
    `PRH06 Group ${stamp}`,
  ]);
  const groupCompanyId = toInt(
    (
      await query(
        `SELECT id
         FROM group_companies
         WHERE tenant_id = ?
           AND code = ?
         LIMIT 1`,
        [tenantId, `PRH06_G_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(groupCompanyId > 0, "Failed to create group company fixture");

  const legalEntityCode = `PRH06_LE_${stamp}`;
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
    [tenantId, groupCompanyId, legalEntityCode, `PRH06 Legal Entity ${stamp}`, countryId, currencyCode]
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

  const passwordHash = await bcrypt.hash("PRH06#Smoke123", 10);
  const userEmail = `prh06_${stamp}@example.com`;
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, userEmail, passwordHash, "PRH06 User"]
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

  await query(
    `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
     VALUES (?, ?, 'LEGAL_ENTITY', ?, ?)`,
    [tenantId, legalEntityId, `PRH06_COA_${stamp}`, `PRH06 CoA ${stamp}`]
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
        [tenantId, legalEntityId, `PRH06_COA_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(coaId > 0, "Failed to create CoA fixture");

  await query(
    `INSERT INTO accounts (coa_id, code, name, account_type, normal_side, allow_posting, is_active)
     VALUES (?, ?, ?, 'ASSET', 'DEBIT', TRUE, TRUE)`,
    [coaId, `PRH06_BANK_${stamp}`, `PRH06 Bank GL ${stamp}`]
  );
  await query(
    `INSERT INTO accounts (coa_id, code, name, account_type, normal_side, allow_posting, is_active)
     VALUES (?, ?, ?, 'LIABILITY', 'CREDIT', TRUE, TRUE)`,
    [coaId, `PRH06_PAYABLE_${stamp}`, `PRH06 Payable GL ${stamp}`]
  );
  const accountRows = (
    await query(
      `SELECT id, code
       FROM accounts
       WHERE coa_id = ?
         AND code IN (?, ?)`,
      [coaId, `PRH06_BANK_${stamp}`, `PRH06_PAYABLE_${stamp}`]
    )
  ).rows;
  const bankGlAccountId = toInt(
    (accountRows || []).find((row) => String(row.code) === `PRH06_BANK_${stamp}`)?.id
  );
  const payableGlAccountId = toInt(
    (accountRows || []).find((row) => String(row.code) === `PRH06_PAYABLE_${stamp}`)?.id
  );
  assert(bankGlAccountId > 0 && payableGlAccountId > 0, "Failed to resolve account fixtures");

  await query(
    `INSERT INTO bank_accounts (
        tenant_id,
        legal_entity_id,
        code,
        name,
        currency_code,
        gl_account_id,
        bank_name,
        branch_name,
        iban,
        account_no,
        is_active,
        created_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, 'PRH06 Bank', 'Main', ?, ?, TRUE, ?)`,
    [
      tenantId,
      legalEntityId,
      `PRH06_BA_${stamp}`,
      `PRH06 Bank Account ${stamp}`,
      currencyCode,
      bankGlAccountId,
      `TR${String(stamp).slice(-20)}`,
      String(stamp),
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
        [tenantId, legalEntityId, `PRH06_BA_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(bankAccountId > 0, "Failed to create bank account fixture");

  await query(
    `INSERT INTO bank_statement_imports (
        tenant_id,
        legal_entity_id,
        bank_account_id,
        original_filename,
        file_checksum,
        imported_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, bankAccountId, `prh06-${stamp}.csv`, sha256(`PRH06-STMT-${stamp}`), userId]
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
  assert(statementImportId > 0, "Failed to create statement import fixture");

  await query(
    `INSERT INTO bank_statement_lines (
        tenant_id,
        legal_entity_id,
        import_id,
        bank_account_id,
        line_no,
        txn_date,
        description,
        amount,
        currency_code,
        line_hash,
        recon_status
      ) VALUES (?, ?, ?, ?, 1, CURDATE(), ?, 120.50, ?, ?, 'UNMATCHED')`,
    [tenantId, legalEntityId, statementImportId, bankAccountId, `PRH06 stmt line ${stamp}`, currencyCode, sha256(`PRH06-LINE-${stamp}`)]
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
  assert(statementLineId > 0, "Failed to create statement line fixture");

  await query(
    `INSERT INTO bank_reconciliation_exceptions (
        tenant_id,
        legal_entity_id,
        statement_line_id,
        bank_account_id,
        reason_code,
        reason_message,
        status,
        severity,
        first_seen_at
      ) VALUES (?, ?, ?, ?, 'RULE_MISS', 'No rule hit', 'OPEN', 'HIGH', DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 3 HOUR))`,
    [tenantId, legalEntityId, statementLineId, bankAccountId]
  );
  const reconExceptionId = toInt(
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
  assert(reconExceptionId > 0, "Failed to create bank reconciliation exception fixture");

  await query(
    `INSERT INTO payment_batches (
        tenant_id,
        legal_entity_id,
        batch_no,
        source_type,
        bank_account_id,
        currency_code,
        status,
        bank_export_status,
        bank_ack_status,
        created_by_user_id
      ) VALUES (?, ?, ?, 'MANUAL', ?, ?, 'APPROVED', 'EXPORTED', 'PARTIAL', ?)`,
    [tenantId, legalEntityId, `PRH06_BATCH_${stamp}`, bankAccountId, currencyCode, userId]
  );
  const paymentBatchId = toInt(
    (
      await query(
        `SELECT id
         FROM payment_batches
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND batch_no = ?
         LIMIT 1`,
        [tenantId, legalEntityId, `PRH06_BATCH_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(paymentBatchId > 0, "Failed to create payment batch fixture");

  await query(
    `INSERT INTO payment_batch_lines (
        tenant_id,
        legal_entity_id,
        batch_id,
        line_no,
        beneficiary_type,
        beneficiary_name,
        payable_entity_type,
        payable_gl_account_id,
        amount,
        bank_execution_status,
        ack_status
      ) VALUES (?, ?, ?, 1, 'VENDOR', ?, 'MANUAL', ?, 120.50, 'FAILED', 'REJECTED')`,
    [tenantId, legalEntityId, paymentBatchId, `PRH06 Beneficiary ${stamp}`, payableGlAccountId]
  );
  const paymentBatchLineId = toInt(
    (
      await query(
        `SELECT id
         FROM payment_batch_lines
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND batch_id = ?
           AND line_no = 1
         LIMIT 1`,
        [tenantId, legalEntityId, paymentBatchId]
      )
    ).rows?.[0]?.id
  );
  assert(paymentBatchLineId > 0, "Failed to create payment batch line fixture");

  await query(
    `INSERT INTO bank_payment_return_events (
        tenant_id,
        legal_entity_id,
        event_request_id,
        source_type,
        source_ref,
        payment_batch_id,
        payment_batch_line_id,
        bank_statement_line_id,
        event_type,
        event_status,
        amount,
        currency_code,
        reason_code,
        reason_message,
        created_by_user_id
      ) VALUES (?, ?, ?, 'BANK_ACK', ?, ?, ?, ?, 'PAYMENT_REJECTED', 'CONFIRMED', 120.50, ?, 'BANK_REJECT', 'Rejected by bank', ?)`,
    [
      tenantId,
      legalEntityId,
      `PRH06-RET-${stamp}`,
      `RET-${stamp}`,
      paymentBatchId,
      paymentBatchLineId,
      statementLineId,
      currencyCode,
      userId,
    ]
  );
  const paymentReturnEventId = toInt(
    (
      await query(
        `SELECT id
         FROM bank_payment_return_events
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND event_request_id = ?
         LIMIT 1`,
        [tenantId, legalEntityId, `PRH06-RET-${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(paymentReturnEventId > 0, "Failed to create payment return event fixture");

  await query(
    `INSERT INTO payroll_provider_connections (
        tenant_id,
        legal_entity_id,
        provider_code,
        provider_name,
        status,
        is_default,
        created_by_user_id,
        updated_by_user_id
      ) VALUES (?, ?, 'GENERIC_JSON', ?, 'ACTIVE', 1, ?, ?)`,
    [tenantId, legalEntityId, `PRH06 Provider ${stamp}`, userId, userId]
  );
  const providerConnectionId = toInt(
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
  assert(providerConnectionId > 0, "Failed to create payroll provider connection fixture");

  await query(
    `INSERT INTO payroll_provider_import_jobs (
        tenant_id,
        legal_entity_id,
        payroll_provider_connection_id,
        provider_code,
        adapter_version,
        payroll_period,
        period_start,
        period_end,
        pay_date,
        currency_code,
        import_key,
        raw_payload_hash,
        normalized_payload_hash,
        source_format,
        source_filename,
        status,
        preview_summary_json,
        validation_errors_json,
        match_errors_json,
        match_warnings_json,
        raw_payload_text,
        normalized_payload_json,
        requested_by_user_id,
        requested_at,
        applied_at,
        failure_message
      ) VALUES
      (?, ?, ?, 'GENERIC_JSON', 'v1', CURDATE(), CURDATE(), CURDATE(), CURDATE(), ?, ?, ?, ?, 'JSON', ?, 'FAILED', '{}', '[]', '[]', '[]', '{}', '{}', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 HOUR), NULL, 'Import failed'),
      (?, ?, ?, 'GENERIC_JSON', 'v1', CURDATE(), CURDATE(), CURDATE(), CURDATE(), ?, ?, ?, ?, 'JSON', ?, 'REJECTED', '{}', '[]', '[\"No employee match\"]', '[]', '{}', '{}', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 HOUR), NULL, NULL),
      (?, ?, ?, 'GENERIC_JSON', 'v1', CURDATE(), CURDATE(), CURDATE(), CURDATE(), ?, ?, ?, ?, 'JSON', ?, 'APPLIED', '{}', '[]', '[]', '[]', '{}', '{}', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 50 MINUTE), DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 10 MINUTE), NULL)`,
    [
      tenantId, legalEntityId, providerConnectionId, currencyCode, `PRH06_IMP_FAIL_${stamp}`, sha256(`PRH06-IMP-FAIL-${stamp}`), sha256(`PRH06-IMP-FAIL-N-${stamp}`), `prh06-fail-${stamp}.json`, userId,
      tenantId, legalEntityId, providerConnectionId, currencyCode, `PRH06_IMP_REJECT_${stamp}`, sha256(`PRH06-IMP-REJECT-${stamp}`), sha256(`PRH06-IMP-REJECT-N-${stamp}`), `prh06-reject-${stamp}.json`, userId,
      tenantId, legalEntityId, providerConnectionId, currencyCode, `PRH06_IMP_APPLIED_${stamp}`, sha256(`PRH06-IMP-APPLIED-${stamp}`), sha256(`PRH06-IMP-APPLIED-N-${stamp}`), `prh06-applied-${stamp}.json`, userId,
    ]
  );

  await query(
    `INSERT INTO payroll_period_closes (
        tenant_id,
        legal_entity_id,
        period_start,
        period_end,
        status,
        failed_checks,
        warning_checks
      ) VALUES (?, ?, DATE_SUB(CURDATE(), INTERVAL 15 DAY), DATE_ADD(CURDATE(), INTERVAL 14 DAY), 'DRAFT', 1, 0)`,
    [tenantId, legalEntityId]
  );
  const payrollCloseId = toInt(
    (
      await query(
        `SELECT id
         FROM payroll_period_closes
         WHERE tenant_id = ?
           AND legal_entity_id = ?
         ORDER BY id DESC
         LIMIT 1`,
        [tenantId, legalEntityId]
      )
    ).rows?.[0]?.id
  );
  assert(payrollCloseId > 0, "Failed to create payroll close fixture");

  await query(
    `INSERT INTO payroll_period_close_checks (
        tenant_id,
        legal_entity_id,
        payroll_period_close_id,
        check_code,
        check_name,
        severity,
        status,
        sort_order
      ) VALUES
      (?, ?, ?, 'RUNS_FINALIZED', 'Runs finalized', 'ERROR', 'FAIL', 10),
      (?, ?, ?, 'INFO_NOTE', 'Informational', 'INFO', 'PASS', 20)`,
    [tenantId, legalEntityId, payrollCloseId, tenantId, legalEntityId, payrollCloseId]
  );

  return {
    tenantId,
    legalEntityId,
    userId,
    reconExceptionId,
    paymentReturnEventId,
  };
}

async function main() {
  const stamp = Date.now();
  const fixture = await setupFixture(stamp);

  const refresh1 = await refreshExceptionWorkbench({
    req: null,
    tenantId: fixture.tenantId,
    filters: { legalEntityId: fixture.legalEntityId, days: 30 },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(toInt(refresh1.processed) === 7, "refresh1 processed should be 7");
  assert(toInt(refresh1.by_source.bank_reconciliation_exceptions) === 1, "bank recon source count mismatch");
  assert(toInt(refresh1.by_source.bank_payment_return_events) === 1, "bank return source count mismatch");
  assert(toInt(refresh1.by_source.payroll_provider_import_jobs) === 3, "payroll import source count mismatch");
  assert(toInt(refresh1.by_source.payroll_close_checks) === 2, "payroll close checks source count mismatch");

  const refresh2 = await refreshExceptionWorkbench({
    req: null,
    tenantId: fixture.tenantId,
    filters: { legalEntityId: fixture.legalEntityId, days: 30 },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(toInt(refresh2.processed) === 7, "refresh2 processed should remain 7 (upsert idempotency)");

  const queue = await listExceptionWorkbenchRows({
    req: null,
    tenantId: fixture.tenantId,
    filters: {
      legalEntityId: fixture.legalEntityId,
      refresh: false,
      limit: 50,
      offset: 0,
    },
    buildScopeFilter: allowAllScopeFilter,
    assertScopeAccess: noScopeGuard,
  });
  assert(toInt(queue.total) === 7, "workbench total should be 7");
  assert(toInt(queue.summary?.by_module?.BANK) === 2, "BANK module summary mismatch");
  assert(toInt(queue.summary?.by_module?.PAYROLL) === 5, "PAYROLL module summary mismatch");
  assert(toInt(queue.summary?.by_status?.OPEN) === 5, "OPEN status summary mismatch");
  assert(toInt(queue.summary?.by_status?.RESOLVED) === 2, "RESOLVED status summary mismatch");

  const target = (queue.rows || []).find((row) => String(row.source_type) === "BANK_PAYMENT_RETURN_EVENT");
  assert(target?.id, "Expected BANK_PAYMENT_RETURN_EVENT row in workbench");
  assert(String(target.status) === "OPEN", "BANK_PAYMENT_RETURN_EVENT should start OPEN");
  assert(String(target.severity) === "HIGH", "BANK_PAYMENT_RETURN_EVENT should normalize to HIGH");

  const claimed = await claimExceptionWorkbench({
    req: null,
    tenantId: fixture.tenantId,
    exceptionId: target.id,
    actorUserId: fixture.userId,
    ownerUserId: fixture.userId,
    note: "Claiming for investigation",
    assertScopeAccess: noScopeGuard,
  });
  assert(String(claimed.status) === "IN_REVIEW", "Claim should transition to IN_REVIEW");
  assert(toInt(claimed.owner_user_id) === fixture.userId, "Claim should set owner_user_id");

  const resolved = await resolveExceptionWorkbench({
    req: null,
    tenantId: fixture.tenantId,
    exceptionId: target.id,
    actorUserId: fixture.userId,
    resolutionAction: "MANUAL_REVIEW",
    resolutionNote: "Handled",
    assertScopeAccess: noScopeGuard,
  });
  assert(String(resolved.status) === "RESOLVED", "Resolve should transition to RESOLVED");
  assert(toInt(resolved.resolved_by_user_id) === fixture.userId, "Resolve should set resolved_by_user_id");

  const reopened = await reopenExceptionWorkbench({
    req: null,
    tenantId: fixture.tenantId,
    exceptionId: target.id,
    actorUserId: fixture.userId,
    resolutionNote: "Need to re-open",
    assertScopeAccess: noScopeGuard,
  });
  assert(String(reopened.status) === "OPEN", "Reopen should transition back to OPEN");

  const detail = await getExceptionWorkbenchById({
    req: null,
    tenantId: fixture.tenantId,
    exceptionId: target.id,
    assertScopeAccess: noScopeGuard,
  });
  const auditEvents = (detail.audit || []).map((row) => String(row.event_type || ""));
  assert(auditEvents.includes("CLAIM"), "Audit should include CLAIM");
  assert(auditEvents.includes("RESOLVED"), "Audit should include RESOLVED");
  assert(auditEvents.includes("REOPEN"), "Audit should include REOPEN");

  const sourceRecon = (
    await query(
      `SELECT status
       FROM bank_reconciliation_exceptions
       WHERE id = ?
         AND tenant_id = ?`,
      [fixture.reconExceptionId, fixture.tenantId]
    )
  ).rows?.[0];
  assert(String(sourceRecon?.status || "") === "OPEN", "Source bank reconciliation exception must remain unchanged");

  const sourceReturn = (
    await query(
      `SELECT event_status
       FROM bank_payment_return_events
       WHERE id = ?
         AND tenant_id = ?`,
      [fixture.paymentReturnEventId, fixture.tenantId]
    )
  ).rows?.[0];
  assert(String(sourceReturn?.event_status || "") === "CONFIRMED", "Source payment return event must remain unchanged");

  const sourceImportBuckets = (
    await query(
      `SELECT status, COUNT(*) AS total
       FROM payroll_provider_import_jobs
       WHERE tenant_id = ?
         AND legal_entity_id = ?
       GROUP BY status`,
      [fixture.tenantId, fixture.legalEntityId]
    )
  ).rows;
  const statusMap = Object.fromEntries((sourceImportBuckets || []).map((row) => [String(row.status), toInt(row.total)]));
  assert(toInt(statusMap.FAILED) === 1, "Source payroll import FAILED row should remain 1");
  assert(toInt(statusMap.REJECTED) === 1, "Source payroll import REJECTED row should remain 1");
  assert(toInt(statusMap.APPLIED) === 1, "Source payroll import APPLIED row should remain 1");

  await expectFailure(
    () =>
      listExceptionWorkbenchRows({
        req: null,
        tenantId: fixture.tenantId,
        filters: { legalEntityId: fixture.legalEntityId, refresh: false },
        buildScopeFilter: allowAllScopeFilter,
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  await expectFailure(
    () =>
      claimExceptionWorkbench({
        req: null,
        tenantId: fixture.tenantId,
        exceptionId: target.id,
        actorUserId: fixture.userId,
        assertScopeAccess: denyScopeGuard,
      }),
    { status: 403, includes: "Scope access denied" }
  );

  console.log("PR-H06 smoke test passed (unified exception refresh/queue/lifecycle/audit/source-integrity).");
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
