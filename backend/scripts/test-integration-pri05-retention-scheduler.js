#!/usr/bin/env node

import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import { createRetentionPolicy } from "../src/services/retentionPolicies.service.js";
import { enqueueDueRetentionPolicyJobs } from "../src/services/retentionScheduler.service.js";
import { runOneAvailableJob } from "../src/services/jobs.service.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toInt(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
}

function noScopeGuard() {
  return true;
}

async function createFixture(stamp) {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const tenantCode = `PRI05_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRI05 Tenant ${stamp}`]
  );
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
  assert(tenantId > 0, "Failed to create tenant fixture");

  const countryRow = (
    await query(
      `SELECT id, default_currency_code
       FROM countries
       WHERE iso2 = 'TR'
       LIMIT 1`
    )
  ).rows?.[0];
  const countryId = toInt(countryRow?.id);
  const currencyCode = String(countryRow?.default_currency_code || "TRY")
    .trim()
    .toUpperCase();
  assert(countryId > 0, "Missing TR country fixture");

  await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `PRI05_G_${stamp}`, `PRI05 Group ${stamp}`]
  );
  const groupCompanyId = toInt(
    (
      await query(
        `SELECT id
         FROM group_companies
         WHERE tenant_id = ?
           AND code = ?
         LIMIT 1`,
        [tenantId, `PRI05_G_${stamp}`]
      )
    ).rows?.[0]?.id
  );
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
      ) VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, groupCompanyId, `PRI05_LE_${stamp}`, `PRI05 Legal Entity ${stamp}`, countryId, currencyCode]
  );
  const legalEntityId = toInt(
    (
      await query(
        `SELECT id
         FROM legal_entities
         WHERE tenant_id = ?
           AND code = ?
         LIMIT 1`,
        [tenantId, `PRI05_LE_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  const passwordHash = await bcrypt.hash("PRI05#Smoke123", 10);
  const userEmail = `pri05_user_${stamp}@example.com`;
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, userEmail, passwordHash, "PRI05 User"]
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

  return {
    tenantId,
    legalEntityId,
    userId,
  };
}

async function main() {
  const stamp = Date.now();
  const fixture = await createFixture(stamp);

  const duePolicy = await createRetentionPolicy({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    input: {
      policyCode: `PRI05_DUE_${stamp}`,
      policyName: "PRI05 Scheduled Due Policy",
      datasetCode: "JOB_EXECUTION_LOG",
      actionCode: "MASK",
      retentionDays: 1,
      legalEntityId: null,
      status: "ACTIVE",
      configJson: {
        schedule_enabled: true,
        schedule_interval_minutes: 1,
      },
    },
    assertScopeAccess: noScopeGuard,
  });
  const duePolicyId = toInt(duePolicy?.row?.id);
  assert(duePolicyId > 0, "Due policy should be created");

  const notDuePolicy = await createRetentionPolicy({
    req: null,
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    input: {
      policyCode: `PRI05_NOTDUE_${stamp}`,
      policyName: "PRI05 Scheduled Not Due Policy",
      datasetCode: "JOB_EXECUTION_LOG",
      actionCode: "MASK",
      retentionDays: 1,
      legalEntityId: null,
      status: "ACTIVE",
      configJson: {
        schedule_enabled: true,
        schedule_interval_minutes: 180,
      },
    },
    assertScopeAccess: noScopeGuard,
  });
  const notDuePolicyId = toInt(notDuePolicy?.row?.id);
  assert(notDuePolicyId > 0, "Not-due policy should be created");

  // Force due policy to be due.
  await query(
    `UPDATE data_retention_policies
     SET last_run_at = DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 3 MINUTE),
         updated_at = CURRENT_TIMESTAMP
     WHERE tenant_id = ?
       AND id = ?`,
    [fixture.tenantId, duePolicyId]
  );

  // Force not-due policy to be fresh.
  await query(
    `UPDATE data_retention_policies
     SET last_run_at = CURRENT_TIMESTAMP,
         updated_at = CURRENT_TIMESTAMP
     WHERE tenant_id = ?
       AND id = ?`,
    [fixture.tenantId, notDuePolicyId]
  );

  const fixedNow = new Date();
  const tick1 = await enqueueDueRetentionPolicyJobs({
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    limit: 100,
    dryRun: false,
    now: fixedNow,
  });
  assert(toInt(tick1.due_policies) >= 1, "First tick should find at least one due policy");
  assert(toInt(tick1.queued_jobs) === 1, "First tick should enqueue exactly one job");
  assert(toInt(tick1.idempotent_hits) === 0, "First tick should have zero idempotent hits");

  const dueRowTick1 = (tick1.rows || []).find((row) => toInt(row.policy_id) === duePolicyId);
  assert(Boolean(dueRowTick1?.queued), "Due policy row should be queued on first tick");

  const tick2 = await enqueueDueRetentionPolicyJobs({
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    limit: 100,
    dryRun: false,
    now: fixedNow,
  });
  assert(toInt(tick2.queued_jobs) === 0, "Second tick same bucket should not queue a new job");
  assert(toInt(tick2.idempotent_hits) === 1, "Second tick should return one idempotent hit");

  const jobRun = await runOneAvailableJob({
    tenantId: fixture.tenantId,
    workerId: "pri05-worker",
    queueNames: ["ops.retention"],
  });
  assert(Boolean(jobRun?.ok) === true, "Scheduled DATA_RETENTION_RUN job should succeed");
  assert(String(jobRun?.status || "") === "SUCCEEDED", "Scheduled job should finish SUCCEEDED");

  const runRows = (
    await query(
      `SELECT trigger_mode, status
       FROM data_retention_runs
       WHERE tenant_id = ?
         AND data_retention_policy_id = ?
       ORDER BY id DESC
       LIMIT 1`,
      [fixture.tenantId, duePolicyId]
    )
  ).rows || [];
  const runRow = runRows[0] || null;
  assert(runRow, "Retention run row should exist for due policy");
  assert(String(runRow.trigger_mode || "") === "SCHEDULED", "Run trigger_mode should be SCHEDULED");
  assert(
    ["COMPLETED", "PARTIAL"].includes(String(runRow.status || "")),
    "Scheduled retention run status should be COMPLETED or PARTIAL"
  );

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        ok: true,
        step: "PR-I05",
        tenant_id: fixture.tenantId,
        due_policy_id: duePolicyId,
        tick1: {
          due_policies: tick1.due_policies,
          queued_jobs: tick1.queued_jobs,
          idempotent_hits: tick1.idempotent_hits,
        },
        tick2: {
          due_policies: tick2.due_policies,
          queued_jobs: tick2.queued_jobs,
          idempotent_hits: tick2.idempotent_hits,
        },
        job_status: jobRun?.status,
        retention_run_status: runRow.status,
        retention_run_trigger_mode: runRow.trigger_mode,
      },
      null,
      2
    )
  );
}

main()
  .catch((err) => {
    // eslint-disable-next-line no-console
    console.error("[PR-I05 smoke] failed", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await closePool();
    } catch {
      // ignore close failures during shutdown
    }
  });
