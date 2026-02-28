#!/usr/bin/env node

import os from "node:os";
import { closePool } from "../src/db.js";
import { enqueueDueRetentionPolicyJobs } from "../src/services/retentionScheduler.service.js";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseOptionalPositiveInt(value) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

async function main() {
  const pollMs = Math.max(1000, Number(process.env.RETENTION_SCHED_POLL_MS || 60000));
  const tenantId = parseOptionalPositiveInt(process.env.RETENTION_SCHED_TENANT_ID);
  const legalEntityId = parseOptionalPositiveInt(process.env.RETENTION_SCHED_LEGAL_ENTITY_ID);
  const userId = parseOptionalPositiveInt(process.env.RETENTION_SCHED_USER_ID);
  const limit = parseOptionalPositiveInt(process.env.RETENTION_SCHED_LIMIT) || 200;
  const schedulerId = `retention-scheduler:${os.hostname()}:${process.pid}`;

  // eslint-disable-next-line no-console
  console.log("[retention-scheduler] started", {
    schedulerId,
    pollMs,
    tenantId,
    legalEntityId,
    userId,
    limit,
  });

  let shuttingDown = false;
  const stop = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    // eslint-disable-next-line no-console
    console.log(`[retention-scheduler] stopping (${signal})`);
    try {
      await closePool();
    } finally {
      process.exit(0);
    }
  };

  process.on("SIGINT", () => stop("SIGINT"));
  process.on("SIGTERM", () => stop("SIGTERM"));

  while (!shuttingDown) {
    try {
      const tick = await enqueueDueRetentionPolicyJobs({
        tenantId,
        legalEntityId,
        userId,
        limit,
      });

      // eslint-disable-next-line no-console
      console.log("[retention-scheduler] tick", {
        now: tick.now,
        scanned: tick.total_policies_scanned,
        due: tick.due_policies,
        queued: tick.queued_jobs,
        idempotent: tick.idempotent_hits,
        skipped_not_due: tick.skipped_not_due,
        skipped_disabled: tick.skipped_schedule_disabled,
        skipped_running: tick.skipped_running,
      });
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error("[retention-scheduler] tick error", err?.message || err);
    }

    // eslint-disable-next-line no-await-in-loop
    await sleep(pollMs);
  }
}

main().catch(async (err) => {
  // eslint-disable-next-line no-console
  console.error("[retention-scheduler] fatal", err?.message || err);
  try {
    await closePool();
  } catch {
    // ignore close errors during fatal shutdown
  }
  process.exit(1);
});
