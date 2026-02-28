import bcrypt from "bcrypt";
import crypto from "node:crypto";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import {
  createRetentionPolicy,
  executeDataRetentionPolicyRun,
  getRetentionRunDetail,
} from "../src/services/retentionPolicies.service.js";
import {
  createPayrollPeriodExportSnapshot,
  getPeriodExportSnapshotDetail,
} from "../src/services/exportSnapshots.service.js";
import { enqueueJob, runOneAvailableJob } from "../src/services/jobs.service.js";

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

async function setupFixture(stamp) {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  await query(`INSERT INTO tenants (code, name) VALUES (?, ?)`, [`PRH07_T_${stamp}`, `PRH07 Tenant ${stamp}`]);
  const tenantId = toInt(
    (
      await query(
        `SELECT id
         FROM tenants
         WHERE code = ?
         LIMIT 1`,
        [`PRH07_T_${stamp}`]
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
    `PRH07_G_${stamp}`,
    `PRH07 Group ${stamp}`,
  ]);
  const groupCompanyId = toInt(
    (
      await query(
        `SELECT id
         FROM group_companies
         WHERE tenant_id = ?
           AND code = ?
         LIMIT 1`,
        [tenantId, `PRH07_G_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(groupCompanyId > 0, "Failed to create group company fixture");

  const legalEntityCode = `PRH07_LE_${stamp}`;
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
    [tenantId, groupCompanyId, legalEntityCode, `PRH07 Legal Entity ${stamp}`, countryId, currencyCode]
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

  const passwordHash = await bcrypt.hash("PRH07#Smoke123", 10);
  const userEmail = `prh07_${stamp}@example.com`;
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, userEmail, passwordHash, "PRH07 User"]
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
    [tenantId, legalEntityId, `PRH07 Provider ${stamp}`, userId, userId]
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
        requested_at
      ) VALUES
      (?, ?, ?, 'GENERIC_JSON', 'v1', CURDATE(), CURDATE(), CURDATE(), CURDATE(), ?, ?, ?, ?, 'JSON', ?, 'FAILED', '{}', '[]', '[]', '[]', ?, '{}', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 5 DAY)),
      (?, ?, ?, 'GENERIC_JSON', 'v1', CURDATE(), CURDATE(), CURDATE(), CURDATE(), ?, ?, ?, ?, 'JSON', ?, 'PREVIEWED', '{}', '[]', '[]', '[]', ?, '{}', ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 4 DAY))`,
    [
      tenantId, legalEntityId, providerConnectionId, currencyCode, `PRH07_IMP_FAIL_${stamp}`, sha256(`PRH07-IMP-FAIL-${stamp}`), sha256(`PRH07-IMP-FAIL-N-${stamp}`), `prh07-fail-${stamp}.json`, JSON.stringify({ pii: "old-raw-1" }), userId,
      tenantId, legalEntityId, providerConnectionId, currencyCode, `PRH07_IMP_PREV_${stamp}`, sha256(`PRH07-IMP-PREV-${stamp}`), sha256(`PRH07-IMP-PREV-N-${stamp}`), `prh07-prev-${stamp}.json`, JSON.stringify({ pii: "old-raw-2" }), userId,
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
        warning_checks,
        closed_by_user_id,
        closed_at
      ) VALUES (?, ?, DATE_SUB(CURDATE(), INTERVAL 7 DAY), CURDATE(), 'CLOSED', 0, 0, ?, DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 HOUR))`,
    [tenantId, legalEntityId, userId]
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
      ) VALUES (?, ?, ?, 'FINAL_OK', 'Finalized OK', 'INFO', 'PASS', 10)`,
    [tenantId, legalEntityId, payrollCloseId]
  );

  await query(
    `INSERT INTO payroll_period_close_audit (
        tenant_id,
        legal_entity_id,
        payroll_period_close_id,
        action,
        action_status,
        note,
        acted_by_user_id
      ) VALUES (?, ?, ?, 'CLOSED', 'CONFIRMED', ?, ?)`,
    [tenantId, legalEntityId, payrollCloseId, "Closed for export", userId]
  );

  const runNo = `PRH07_RUN_${stamp}`;
  await query(
    `INSERT INTO payroll_runs (
        tenant_id,
        legal_entity_id,
        run_no,
        provider_code,
        entity_code,
        payroll_period,
        pay_date,
        currency_code,
        source_batch_ref,
        original_filename,
        file_checksum,
        status,
        source_type,
        imported_by_user_id
      ) VALUES (?, ?, ?, 'GENERIC_JSON', ?, CURDATE(), CURDATE(), ?, ?, ?, ?, 'FINALIZED', 'MANUAL', ?)`,
    [
      tenantId,
      legalEntityId,
      runNo,
      legalEntityCode,
      currencyCode,
      `PRH07-SRC-${stamp}`,
      `prh07-run-${stamp}.csv`,
      sha256(`PRH07-RUN-${stamp}`),
      userId,
    ]
  );
  const payrollRunId = toInt(
    (
      await query(
        `SELECT id
         FROM payroll_runs
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND run_no = ?
         LIMIT 1`,
        [tenantId, legalEntityId, runNo]
      )
    ).rows?.[0]?.id
  );
  assert(payrollRunId > 0, "Failed to create payroll run fixture");

  await query(
    `INSERT INTO payroll_run_lines (
        tenant_id,
        legal_entity_id,
        run_id,
        line_no,
        employee_code,
        employee_name,
        gross_pay,
        net_pay,
        line_hash
      ) VALUES (?, ?, ?, 1, 'E001', 'PRH07 Employee', 1000.00, 800.00, ?)`,
    [tenantId, legalEntityId, payrollRunId, sha256(`PRH07-RUN-LINE-${stamp}`)]
  );

  return {
    tenantId,
    legalEntityId,
    userId,
    payrollCloseId,
    payrollRunId,
  };
}

async function main() {
  const stamp = Date.now();
  const fixture = await setupFixture(stamp);

  const policyResult = await createRetentionPolicy({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    input: {
      policyCode: `PRH07_RET_${stamp}`,
      policyName: "PRH07 Payroll Import Raw Mask",
      datasetCode: "PAYROLL_PROVIDER_IMPORT_RAW",
      actionCode: "MASK",
      retentionDays: 1,
      legalEntityId: fixture.legalEntityId,
      status: "ACTIVE",
      configJson: { mode: "smoke" },
    },
    assertScopeAccess: noScopeGuard,
  });
  const policyId = toInt(policyResult?.row?.id);
  assert(policyId > 0, "Retention policy should be created");

  const runManual = await executeDataRetentionPolicyRun({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    policyId,
    input: {
      triggerMode: "MANUAL",
      runIdempotencyKey: `PRH07-MANUAL-${stamp}`,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(Boolean(runManual?.idempotent) === false, "Manual retention run should not be idempotent on first call");
  assert(String(runManual?.row?.status || "") === "COMPLETED", "Manual retention run should complete");
  assert(toInt(runManual?.row?.masked_rows) === 2, "Manual retention run should mask 2 rows");

  const runManualReplay = await executeDataRetentionPolicyRun({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    policyId,
    input: {
      triggerMode: "MANUAL",
      runIdempotencyKey: `PRH07-MANUAL-${stamp}`,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(Boolean(runManualReplay?.idempotent) === true, "Manual replay must return idempotent run");
  assert(toInt(runManualReplay?.row?.id) === toInt(runManual?.row?.id), "Idempotent run id mismatch");

  const retentionRowsAfterManual = (
    await query(
      `SELECT raw_payload_retention_status, raw_payload_text
       FROM payroll_provider_import_jobs
       WHERE tenant_id = ?
         AND legal_entity_id = ?
       ORDER BY id ASC`,
      [fixture.tenantId, fixture.legalEntityId]
    )
  ).rows;
  assert((retentionRowsAfterManual || []).length === 2, "Expected 2 provider import rows after manual retention run");
  for (const row of retentionRowsAfterManual || []) {
    assert(String(row.raw_payload_retention_status) === "MASKED", "Provider import row should be MASKED");
    assert(String(row.raw_payload_text || "") === "[MASKED_BY_RETENTION_POLICY]", "Provider raw payload should be masked marker");
  }

  const queuedJob = await enqueueJob({
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    spec: {
      queue_name: "ops.retention",
      module_code: "OPS",
      job_type: "DATA_RETENTION_RUN",
      idempotency_key: `PRH07-JOB-${stamp}`,
      run_after_at: new Date(Date.now() - 60 * 1000),
      payload: {
        tenant_id: fixture.tenantId,
        policy_id: policyId,
        acting_user_id: fixture.userId,
        run_idempotency_key: `PRH07-JOB-RUN-${stamp}`,
      },
    },
  });
  assert(toInt(queuedJob?.job?.id) > 0, "DATA_RETENTION_RUN job should be enqueued");

  const jobRun = await runOneAvailableJob({
    workerId: "prh07-worker",
    queueNames: ["ops.retention"],
    tenantId: fixture.tenantId,
  });
  assert(Boolean(jobRun?.ok) === true, "DATA_RETENTION_RUN job should succeed");
  assert(String(jobRun?.status || "") === "SUCCEEDED", "DATA_RETENTION_RUN should finish SUCCEEDED");
  const asyncRunId = toInt(jobRun?.result?.retention_run_id);
  assert(asyncRunId > 0, "DATA_RETENTION_RUN should produce a retention_run_id");

  const asyncRunDetail = await getRetentionRunDetail({
    req: null,
    tenantId: fixture.tenantId,
    runId: asyncRunId,
    assertScopeAccess: noScopeGuard,
  });
  assert(String(asyncRunDetail?.row?.trigger_mode || "") === "JOB", "Async retention run must have JOB trigger mode");
  assert(String(asyncRunDetail?.row?.status || "") === "COMPLETED", "Async retention run should complete");

  const snapshot1 = await createPayrollPeriodExportSnapshot({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    input: {
      payrollPeriodCloseId: fixture.payrollCloseId,
      snapshotType: "PAYROLL_CLOSE_PERIOD",
      idempotencyKey: `PRH07-SNAP-A-${stamp}`,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(Boolean(snapshot1?.idempotent) === false, "First snapshot should not be idempotent");
  const snapshot1Id = toInt(snapshot1?.snapshot?.id);
  assert(snapshot1Id > 0, "First snapshot id should be present");
  assert(String(snapshot1?.snapshot?.status || "") === "READY", "First snapshot should be READY");
  assert(String(snapshot1?.snapshot?.snapshot_hash || "").length === 64, "First snapshot hash should be sha256 length");
  assert((snapshot1?.items || []).length === 6, "Snapshot should include 6 deterministic item buckets");

  const snapshot1Replay = await createPayrollPeriodExportSnapshot({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    input: {
      payrollPeriodCloseId: fixture.payrollCloseId,
      snapshotType: "PAYROLL_CLOSE_PERIOD",
      idempotencyKey: `PRH07-SNAP-A-${stamp}`,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(Boolean(snapshot1Replay?.idempotent) === true, "Snapshot replay with same key must be idempotent");
  assert(toInt(snapshot1Replay?.snapshot?.id) === snapshot1Id, "Idempotent snapshot replay id mismatch");
  assert(
    String(snapshot1Replay?.snapshot?.snapshot_hash || "") === String(snapshot1?.snapshot?.snapshot_hash || ""),
    "Idempotent snapshot replay hash mismatch"
  );

  const snapshot2 = await createPayrollPeriodExportSnapshot({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    input: {
      payrollPeriodCloseId: fixture.payrollCloseId,
      snapshotType: "PAYROLL_CLOSE_PERIOD",
      idempotencyKey: `PRH07-SNAP-B-${stamp}`,
    },
    assertScopeAccess: noScopeGuard,
  });
  assert(Boolean(snapshot2?.idempotent) === false, "Second snapshot with new key should create new row");
  assert(toInt(snapshot2?.snapshot?.id) !== snapshot1Id, "Second snapshot should have a different id");
  assert(
    String(snapshot2?.snapshot?.snapshot_hash || "") === String(snapshot1?.snapshot?.snapshot_hash || ""),
    "Snapshot hash must be repeatable for unchanged period data"
  );

  const snapshot1Detail = await getPeriodExportSnapshotDetail({
    req: null,
    tenantId: fixture.tenantId,
    snapshotId: snapshot1Id,
    assertScopeAccess: noScopeGuard,
  });
  const snapshot2Detail = await getPeriodExportSnapshotDetail({
    req: null,
    tenantId: fixture.tenantId,
    snapshotId: snapshot2?.snapshot?.id,
    assertScopeAccess: noScopeGuard,
  });
  const itemHashMap1 = Object.fromEntries(
    (snapshot1Detail?.items || []).map((item) => [String(item.item_code), String(item.item_hash || "")])
  );
  const itemHashMap2 = Object.fromEntries(
    (snapshot2Detail?.items || []).map((item) => [String(item.item_code), String(item.item_hash || "")])
  );
  assert(
    JSON.stringify(itemHashMap1) === JSON.stringify(itemHashMap2),
    "Snapshot item hashes must be repeatable between exports"
  );

  const runCount = toInt(
    (
      await query(
        `SELECT COUNT(*) AS total
         FROM payroll_runs
         WHERE tenant_id = ?
           AND legal_entity_id = ?`,
        [fixture.tenantId, fixture.legalEntityId]
      )
    ).rows?.[0]?.total
  );
  const closeCount = toInt(
    (
      await query(
        `SELECT COUNT(*) AS total
         FROM payroll_period_closes
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND id = ?`,
        [fixture.tenantId, fixture.legalEntityId, fixture.payrollCloseId]
      )
    ).rows?.[0]?.total
  );
  const runLineCount = toInt(
    (
      await query(
        `SELECT COUNT(*) AS total
         FROM payroll_run_lines
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND run_id = ?`,
        [fixture.tenantId, fixture.legalEntityId, fixture.payrollRunId]
      )
    ).rows?.[0]?.total
  );
  assert(runCount === 1, "Core payroll_runs rows should not be deleted");
  assert(closeCount === 1, "Core payroll_period_closes row should not be deleted");
  assert(runLineCount === 1, "Core payroll_run_lines row should not be deleted");

  console.log("PR-H07 smoke test passed (retention policy/manual+job run + snapshot idempotency/hash stability).");
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
