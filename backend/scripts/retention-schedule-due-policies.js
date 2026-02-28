#!/usr/bin/env node

import { closePool } from "../src/db.js";
import { enqueueDueRetentionPolicyJobs } from "../src/services/retentionScheduler.service.js";

function parseOptionalPositiveInt(value) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

function parseBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  const text = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(text)) return true;
  if (["0", "false", "no", "off"].includes(text)) return false;
  return fallback;
}

async function main() {
  const tenantId = parseOptionalPositiveInt(process.env.RETENTION_SCHED_TENANT_ID);
  const legalEntityId = parseOptionalPositiveInt(process.env.RETENTION_SCHED_LEGAL_ENTITY_ID);
  const userId = parseOptionalPositiveInt(process.env.RETENTION_SCHED_USER_ID);
  const limit = parseOptionalPositiveInt(process.env.RETENTION_SCHED_LIMIT) || 200;
  const dryRun = parseBoolean(process.env.RETENTION_SCHED_DRY_RUN, false);

  const result = await enqueueDueRetentionPolicyJobs({
    tenantId,
    legalEntityId,
    userId,
    limit,
    dryRun,
  });

  // eslint-disable-next-line no-console
  console.log("[retention-scheduler] tick completed");
  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        tenant_id: result.tenant_id,
        legal_entity_id: result.legal_entity_id,
        now: result.now,
        dry_run: result.dry_run,
        total_policies_scanned: result.total_policies_scanned,
        due_policies: result.due_policies,
        queued_jobs: result.queued_jobs,
        idempotent_hits: result.idempotent_hits,
        skipped_not_due: result.skipped_not_due,
        skipped_schedule_disabled: result.skipped_schedule_disabled,
        skipped_running: result.skipped_running,
      },
      null,
      2
    )
  );
}

main()
  .catch((err) => {
    // eslint-disable-next-line no-console
    console.error("[retention-scheduler] fatal", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await closePool();
    } catch {
      // ignore close failures during shutdown
    }
  });
