import { query } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { enqueueJob } from "./jobs.service.js";

const DEFAULT_SCHEDULE_INTERVAL_MINUTES = 24 * 60;
const MIN_SCHEDULE_INTERVAL_MINUTES = 1;
const MAX_SCHEDULE_INTERVAL_MINUTES = 60 * 24 * 30;

function parseJsonMaybe(value) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function toBool(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  if (value === true || value === false) return value;
  const text = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(text)) return true;
  if (["0", "false", "no", "off"].includes(text)) return false;
  return fallback;
}

function normalizeLimit(value, fallback = 200, maxLimit = 2000) {
  const n = Number(value);
  if (!Number.isInteger(n) || n <= 0) return fallback;
  return Math.min(n, maxLimit);
}

function clampIntervalMinutes(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_SCHEDULE_INTERVAL_MINUTES;
  return Math.max(
    MIN_SCHEDULE_INTERVAL_MINUTES,
    Math.min(MAX_SCHEDULE_INTERVAL_MINUTES, Math.floor(n))
  );
}

function addMinutes(dateLike, minutes) {
  const date = new Date(dateLike);
  if (Number.isNaN(date.getTime())) return null;
  return new Date(date.getTime() + Number(minutes || 0) * 60000);
}

function toIso(value) {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

function buildPolicyScheduleMeta(policyRow = {}, now) {
  const config = parseJsonMaybe(policyRow.config_json) || {};
  const scheduleEnabled = toBool(config.schedule_enabled, true);
  const intervalMinutes = clampIntervalMinutes(
    config.schedule_interval_minutes ?? config.scheduleIntervalMinutes
  );

  const lastRunAt = policyRow.last_run_at || null;
  const createdAt = policyRow.created_at || policyRow.updated_at || null;
  const baseline = lastRunAt || createdAt || null;

  const dueAt = baseline ? addMinutes(baseline, intervalMinutes) : now;
  const due = scheduleEnabled && (!dueAt || dueAt.getTime() <= now.getTime());
  const bucket = Math.floor(now.getTime() / (intervalMinutes * 60000));

  return {
    config,
    scheduleEnabled,
    intervalMinutes,
    lastRunAt: toIso(lastRunAt),
    dueAt: toIso(dueAt),
    due,
    bucket,
  };
}

async function listActiveRetentionPolicies({
  tenantId = null,
  legalEntityId = null,
  limit = 200,
  runQuery = query,
}) {
  const params = [];
  const where = ["p.status = 'ACTIVE'"];
  if (parsePositiveInt(tenantId)) {
    where.push("p.tenant_id = ?");
    params.push(parsePositiveInt(tenantId));
  }
  if (parsePositiveInt(legalEntityId)) {
    where.push("p.legal_entity_id = ?");
    params.push(parsePositiveInt(legalEntityId));
  }

  const res = await runQuery(
    `SELECT
        p.id,
        p.tenant_id,
        p.legal_entity_id,
        p.policy_code,
        p.policy_name,
        p.dataset_code,
        p.action_code,
        p.retention_days,
        p.status,
        p.config_json,
        p.last_run_at,
        p.last_run_status,
        p.created_at,
        p.updated_at
     FROM data_retention_policies p
     WHERE ${where.join(" AND ")}
     ORDER BY p.id ASC
     LIMIT ${normalizeLimit(limit)}`,
    params
  );
  return res.rows || [];
}

async function getRunningPolicyIdsByTenant({ tenantId, policyIds, runQuery = query }) {
  const tId = parsePositiveInt(tenantId);
  if (!tId) return new Set();
  const ids = (Array.isArray(policyIds) ? policyIds : [])
    .map((id) => parsePositiveInt(id))
    .filter(Boolean);
  if (!ids.length) return new Set();

  const res = await runQuery(
    `SELECT DISTINCT r.data_retention_policy_id
     FROM data_retention_runs r
     WHERE r.tenant_id = ?
       AND r.status = 'RUNNING'
       AND r.data_retention_policy_id IN (${ids.map(() => "?").join(", ")})`,
    [tId, ...ids]
  );
  return new Set((res.rows || []).map((row) => parsePositiveInt(row.data_retention_policy_id)).filter(Boolean));
}

function buildScheduledJobIdempotencyKey({ policyId, intervalBucket }) {
  return `RETENTION_SCHED|POLICY:${policyId}|BUCKET:${intervalBucket}`;
}

function buildScheduledRunIdempotencyKey({ policyId, intervalBucket }) {
  return `RETENTION_RUN_SCHED|POLICY:${policyId}|BUCKET:${intervalBucket}`;
}

export async function enqueueDueRetentionPolicyJobs({
  tenantId = null,
  legalEntityId = null,
  userId = null,
  limit = 200,
  dryRun = false,
  now = null,
} = {}) {
  const safeLimit = normalizeLimit(limit);
  const tickNow = now ? new Date(now) : new Date();
  if (Number.isNaN(tickNow.getTime())) {
    throw badRequest("now must be a valid datetime");
  }

  const policies = await listActiveRetentionPolicies({
    tenantId,
    legalEntityId,
    limit: safeLimit,
  });

  const policyIdsByTenant = new Map();
  for (const policy of policies) {
    const tId = parsePositiveInt(policy?.tenant_id);
    const pId = parsePositiveInt(policy?.id);
    if (!tId || !pId) continue;
    const list = policyIdsByTenant.get(tId) || [];
    list.push(pId);
    policyIdsByTenant.set(tId, list);
  }

  const runningByTenant = new Map();
  for (const [tId, ids] of policyIdsByTenant.entries()) {
    // eslint-disable-next-line no-await-in-loop
    const runningIds = await getRunningPolicyIdsByTenant({
      tenantId: tId,
      policyIds: ids,
    });
    runningByTenant.set(tId, runningIds);
  }

  const rows = [];
  let dueCount = 0;
  let queuedCount = 0;
  let idempotentCount = 0;
  let skippedNotDueCount = 0;
  let skippedDisabledCount = 0;
  let skippedRunningCount = 0;

  for (const policy of policies) {
    const tId = parsePositiveInt(policy?.tenant_id);
    const pId = parsePositiveInt(policy?.id);
    if (!tId || !pId) continue;

    const schedule = buildPolicyScheduleMeta(policy, tickNow);
    const runningSet = runningByTenant.get(tId) || new Set();
    const isRunning = runningSet.has(pId);

    const item = {
      policy_id: pId,
      tenant_id: tId,
      legal_entity_id: parsePositiveInt(policy?.legal_entity_id),
      policy_code: String(policy?.policy_code || ""),
      status: String(policy?.status || ""),
      schedule_enabled: schedule.scheduleEnabled,
      interval_minutes: schedule.intervalMinutes,
      last_run_at: schedule.lastRunAt,
      due_at: schedule.dueAt,
      running: isRunning,
      due: false,
      queued: false,
      idempotent: false,
      skipped_reason: null,
      job_id: null,
    };

    if (!schedule.scheduleEnabled) {
      item.skipped_reason = "SCHEDULE_DISABLED";
      skippedDisabledCount += 1;
      rows.push(item);
      continue;
    }
    if (isRunning) {
      item.skipped_reason = "RUN_ALREADY_IN_PROGRESS";
      skippedRunningCount += 1;
      rows.push(item);
      continue;
    }
    if (!schedule.due) {
      item.skipped_reason = "NOT_DUE";
      skippedNotDueCount += 1;
      rows.push(item);
      continue;
    }

    dueCount += 1;
    item.due = true;

    const jobIdempotencyKey = buildScheduledJobIdempotencyKey({
      policyId: pId,
      intervalBucket: schedule.bucket,
    });
    const runIdempotencyKey = buildScheduledRunIdempotencyKey({
      policyId: pId,
      intervalBucket: schedule.bucket,
    });

    if (toBool(dryRun, false)) {
      item.queued = false;
      item.idempotent = false;
      item.skipped_reason = "DRY_RUN";
      rows.push(item);
      continue;
    }

    // eslint-disable-next-line no-await-in-loop
    const queued = await enqueueJob({
      tenantId: tId,
      userId: parsePositiveInt(userId) || null,
      spec: {
        queue_name: "ops.retention",
        module_code: "OPS",
        job_type: "DATA_RETENTION_RUN",
        run_after_at: new Date(tickNow.getTime() - 1000),
        idempotency_key: jobIdempotencyKey,
        payload: {
          tenant_id: tId,
          policy_id: pId,
          acting_user_id: parsePositiveInt(userId) || null,
          run_idempotency_key: runIdempotencyKey,
          trigger_mode: "SCHEDULED",
          schedule_due_at: schedule.dueAt,
          schedule_interval_minutes: schedule.intervalMinutes,
        },
      },
    });

    item.job_id = parsePositiveInt(queued?.job?.id) || null;
    item.idempotent = Boolean(queued?.idempotent);
    item.queued = !item.idempotent;
    item.skipped_reason = null;

    if (item.idempotent) {
      idempotentCount += 1;
    } else {
      queuedCount += 1;
    }
    rows.push(item);
  }

  return {
    now: tickNow.toISOString(),
    dry_run: toBool(dryRun, false),
    tenant_id: parsePositiveInt(tenantId),
    legal_entity_id: parsePositiveInt(legalEntityId),
    total_policies_scanned: policies.length,
    due_policies: dueCount,
    queued_jobs: queuedCount,
    idempotent_hits: idempotentCount,
    skipped_not_due: skippedNotDueCount,
    skipped_schedule_disabled: skippedDisabledCount,
    skipped_running: skippedRunningCount,
    rows,
  };
}

export default {
  enqueueDueRetentionPolicyJobs,
};
