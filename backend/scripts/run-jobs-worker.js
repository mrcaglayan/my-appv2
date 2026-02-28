import os from "node:os";
import { closePool } from "../src/db.js";
import jobsService from "../src/services/jobs.service.js";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseQueueNames(envValue) {
  if (!envValue) return [];
  return String(envValue)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

async function main() {
  const pollMs = Math.max(250, Number(process.env.JOBS_WORKER_POLL_MS || 2000));
  const queueNames = parseQueueNames(process.env.JOBS_WORKER_QUEUES);
  const tenantId =
    Number.isInteger(Number(process.env.JOBS_WORKER_TENANT_ID)) &&
    Number(process.env.JOBS_WORKER_TENANT_ID) > 0
      ? Number(process.env.JOBS_WORKER_TENANT_ID)
      : null;
  const workerId = `jobs-worker:${os.hostname()}:${process.pid}`;

  // eslint-disable-next-line no-console
  console.log("[jobs-worker] started", { workerId, pollMs, queueNames, tenantId });

  let shuttingDown = false;
  const stop = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    // eslint-disable-next-line no-console
    console.log(`[jobs-worker] stopping (${signal})`);
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
      const result = await jobsService.runOneAvailableJob({ workerId, queueNames, tenantId });
      if (result?.idle) {
        await sleep(pollMs);
        continue;
      }

      // eslint-disable-next-line no-console
      console.log("[jobs-worker] job", {
        job_id: result?.job_id || null,
        tenant_id: result?.tenant_id || null,
        status: result?.status || null,
        ok: Boolean(result?.ok),
      });
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error("[jobs-worker] loop error", err?.message || err);
      await sleep(pollMs);
    }
  }
}

main().catch(async (err) => {
  // eslint-disable-next-line no-console
  console.error("[jobs-worker] fatal", err);
  try {
    await closePool();
  } catch {
    // ignore close error during fatal shutdown
  }
  process.exit(1);
});
