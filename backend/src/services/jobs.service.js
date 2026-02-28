import crypto from "node:crypto";
import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { redactObject } from "../utils/redaction.js";
import { getJobHandler } from "./jobHandlers/index.js";

const TERMINAL_JOB_STATUSES = new Set(["SUCCEEDED", "FAILED_FINAL", "CANCELLED"]);
const CLAIMABLE_JOB_STATUSES = new Set(["QUEUED", "FAILED_RETRYABLE"]);

function up(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function now() {
  return new Date();
}

function addSeconds(date, seconds) {
  return new Date(date.getTime() + Math.max(0, Number(seconds) || 0) * 1000);
}

function hashPayload(payload) {
  return crypto.createHash("sha256").update(JSON.stringify(payload ?? null)).digest("hex");
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function parseJsonMaybe(value) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function toDbDate(value) {
  if (!value) return null;
  if (value instanceof Date) return value;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw badRequest("Invalid date value");
  }
  return parsed;
}

function notFound(message) {
  const err = new Error(message);
  err.status = 404;
  return err;
}

function conflict(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}

function isDup(err) {
  return Number(err?.errno) === 1062 || up(err?.code) === "ER_DUP_ENTRY";
}

function truncate(value, maxLen) {
  const text = String(value ?? "");
  return text.length > maxLen ? text.slice(0, maxLen) : text;
}

function normalizeQueueNames(value) {
  const raw = Array.isArray(value)
    ? value
    : typeof value === "string"
      ? value.split(",")
      : [];
  const items = raw
    .map((item) => String(item || "").trim())
    .filter(Boolean)
    .slice(0, 50);
  return Array.from(new Set(items));
}

function normalizeStatus(value) {
  const status = up(value);
  return status || null;
}

function mapJobRow(row) {
  if (!row) return null;
  return {
    ...row,
    payload_json: parseJsonMaybe(row.payload_json),
    last_error_json: parseJsonMaybe(row.last_error_json),
    result_json: parseJsonMaybe(row.result_json),
    attempt_count: Number(row.attempt_count || 0),
    max_attempts: Number(row.max_attempts || 0),
    priority: Number(row.priority || 0),
  };
}

function mapAttemptRow(row) {
  if (!row) return null;
  return {
    ...row,
    attempt_no: Number(row.attempt_no || 0),
    error_json: parseJsonMaybe(row.error_json),
    result_json: parseJsonMaybe(row.result_json),
  };
}

function isTerminalStatus(status) {
  return TERMINAL_JOB_STATUSES.has(up(status));
}

function isRetryableError(err) {
  if (typeof err?.retryable === "boolean") return err.retryable;
  const status = Number(err?.status || err?.statusCode || 0);
  if (status >= 400 && status < 500) return false;
  return true;
}

function computeRetryDelaySeconds(attemptNo, job) {
  const payload = typeof job?.payload_json === "object" && job?.payload_json ? job.payload_json : {};
  const base = Math.max(1, Number(payload.retry_base_seconds || 30));
  const cap = Math.max(base, Number(payload.retry_max_seconds || 3600));
  const n = Math.max(1, Number(attemptNo || 1));
  const delay = Math.min(cap, base * Math.pow(2, Math.max(0, n - 1)));
  return Math.floor(delay);
}

function buildErrorPayload(err) {
  const raw = {
    name: err?.name || "Error",
    message: err?.message || "Job execution failed",
    code: err?.errorCode || err?.code || null,
    status: Number(err?.status || err?.statusCode || 0) || null,
    retryable: typeof err?.retryable === "boolean" ? err.retryable : null,
    details: err?.details || null,
    stack: typeof err?.stack === "string" ? err.stack.split("\n").slice(0, 10).join("\n") : null,
  };
  return redactObject(raw);
}

function requireTenantId(value, label = "tenantId") {
  const tenantId = parsePositiveInt(value);
  if (!tenantId) throw badRequest(`${label} is required`);
  return tenantId;
}

async function getJobRow({
  tenantId = null,
  jobId,
  runQuery = query,
  forUpdate = false,
}) {
  const id = parsePositiveInt(jobId);
  if (!id) throw badRequest("jobId must be a positive integer");

  const where = ["j.id = ?"];
  const params = [id];
  if (parsePositiveInt(tenantId)) {
    where.push("j.tenant_id = ?");
    params.push(parsePositiveInt(tenantId));
  }

  const sql = `
    SELECT j.*
    FROM app_jobs j
    WHERE ${where.join(" AND ")}
    LIMIT 1
    ${forUpdate ? "FOR UPDATE" : ""}
  `;
  const res = await runQuery(sql, params);
  return mapJobRow(res.rows?.[0] || null);
}

async function getJobAttempts({ tenantId, jobId, runQuery = query }) {
  const id = parsePositiveInt(jobId);
  if (!id) throw badRequest("jobId must be a positive integer");
  const tId = requireTenantId(tenantId);
  const res = await runQuery(
    `SELECT *
     FROM app_job_attempts
     WHERE tenant_id = ? AND app_job_id = ?
     ORDER BY attempt_no DESC, id DESC`,
    [tId, id]
  );
  return (res.rows || []).map(mapAttemptRow);
}

async function insertAttemptRunning({
  tenantId,
  jobId,
  attemptNo,
  workerId,
  runQuery,
}) {
  await runQuery(
    `INSERT INTO app_job_attempts
     (tenant_id, app_job_id, attempt_no, worker_id, status, started_at)
     VALUES (?, ?, ?, ?, 'RUNNING', CURRENT_TIMESTAMP)`,
    [tenantId, jobId, attemptNo, truncate(workerId || "worker", 120)]
  );
}

async function updateAttemptFinal({
  tenantId,
  jobId,
  attemptNo,
  status,
  errorCode = null,
  errorMessage = null,
  errorJson = null,
  resultJson = null,
  runQuery,
}) {
  await runQuery(
    `UPDATE app_job_attempts
     SET status = ?,
         finished_at = CURRENT_TIMESTAMP,
         error_code = ?,
         error_message = ?,
         error_json = ?,
         result_json = ?
     WHERE tenant_id = ? AND app_job_id = ? AND attempt_no = ?`,
    [
      up(status),
      errorCode ? truncate(errorCode, 80) : null,
      errorMessage ? truncate(errorMessage, 500) : null,
      errorJson === undefined ? null : safeJson(errorJson),
      resultJson === undefined ? null : safeJson(resultJson),
      tenantId,
      jobId,
      attemptNo,
    ]
  );
}

async function claimNextAvailableJob({
  tenantId = null,
  workerId,
  queueNames = [],
}) {
  const queues = normalizeQueueNames(queueNames);
  const claimed = await withTransaction(async (tx) => {
    const where = [
      `(j.status = 'QUEUED' OR j.status = 'FAILED_RETRYABLE')`,
      `j.run_after_at <= CURRENT_TIMESTAMP`,
    ];
    const params = [];

    const scopedTenantId = parsePositiveInt(tenantId);
    if (scopedTenantId) {
      where.push("j.tenant_id = ?");
      params.push(scopedTenantId);
    }
    if (queues.length) {
      where.push(`j.queue_name IN (${queues.map(() => "?").join(", ")})`);
      params.push(...queues);
    }

    const sql = `
      SELECT j.*
      FROM app_jobs j
      WHERE ${where.join(" AND ")}
      ORDER BY j.priority ASC, j.run_after_at ASC, j.id ASC
      LIMIT 1
      FOR UPDATE
    `;
    const selected = await tx.query(sql, params);
    const job = mapJobRow(selected.rows?.[0] || null);
    if (!job) return null;

    const status = up(job.status);
    if (!CLAIMABLE_JOB_STATUSES.has(status)) {
      return null;
    }

    const attemptNo = Number(job.attempt_count || 0) + 1;
    await tx.query(
      `UPDATE app_jobs
       SET status = 'RUNNING',
           attempt_count = ?,
           locked_by = ?,
           locked_at = CURRENT_TIMESTAMP,
           started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
           finished_at = NULL,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [attemptNo, truncate(workerId || "worker", 120), job.id]
    );

    await insertAttemptRunning({
      tenantId: parsePositiveInt(job.tenant_id),
      jobId: job.id,
      attemptNo,
      workerId,
      runQuery: tx.query,
    });

    const refreshed = await getJobRow({
      jobId: job.id,
      tenantId: parsePositiveInt(job.tenant_id),
      runQuery: tx.query,
      forUpdate: false,
    });
    return refreshed ? { ...refreshed, __attemptNo: attemptNo } : null;
  });

  return claimed;
}

async function markJobSucceeded({ job, attemptNo, result }) {
  await withTransaction(async (tx) => {
    await tx.query(
      `UPDATE app_jobs
       SET status = 'SUCCEEDED',
           locked_by = NULL,
           locked_at = NULL,
           finished_at = CURRENT_TIMESTAMP,
           last_error_code = NULL,
           last_error_message = NULL,
           last_error_json = NULL,
           result_json = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND id = ?`,
      [safeJson(result), parsePositiveInt(job.tenant_id), job.id]
    );
    await updateAttemptFinal({
      tenantId: parsePositiveInt(job.tenant_id),
      jobId: job.id,
      attemptNo,
      status: "SUCCEEDED",
      resultJson: result,
      runQuery: tx.query,
    });
  });
}

async function markJobFailed({ job, attemptNo, err }) {
  const retryable = isRetryableError(err);
  const exhausted = Number(attemptNo || 0) >= Number(job.max_attempts || 0);
  const finalStatus = retryable && !exhausted ? "FAILED_RETRYABLE" : "FAILED_FINAL";
  const errorPayload = buildErrorPayload(err);
  const errorCode = truncate(String(err?.errorCode || err?.code || "JOB_HANDLER_ERROR"), 80);
  const errorMessage = truncate(String(err?.message || "Job execution failed"), 500);
  const nextRunAt =
    finalStatus === "FAILED_RETRYABLE"
      ? addSeconds(now(), computeRetryDelaySeconds(attemptNo, job))
      : null;

  await withTransaction(async (tx) => {
    await tx.query(
      `UPDATE app_jobs
       SET status = ?,
           locked_by = NULL,
           locked_at = NULL,
           finished_at = CASE WHEN ? = 'FAILED_FINAL' THEN CURRENT_TIMESTAMP ELSE NULL END,
           run_after_at = CASE WHEN ? = 'FAILED_RETRYABLE' THEN ? ELSE run_after_at END,
           last_error_code = ?,
           last_error_message = ?,
           last_error_json = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND id = ?`,
      [
        finalStatus,
        finalStatus,
        finalStatus,
        nextRunAt,
        errorCode,
        errorMessage,
        safeJson(errorPayload),
        parsePositiveInt(job.tenant_id),
        job.id,
      ]
    );
    await updateAttemptFinal({
      tenantId: parsePositiveInt(job.tenant_id),
      jobId: job.id,
      attemptNo,
      status: finalStatus,
      errorCode,
      errorMessage,
      errorJson: errorPayload,
      runQuery: tx.query,
    });
  });

  return { finalStatus, retryable: finalStatus === "FAILED_RETRYABLE", nextRunAt };
}

export async function enqueueJob({ tenantId, spec, userId = null, runQuery = query }) {
  const tId = requireTenantId(tenantId);
  if (!spec || typeof spec !== "object") {
    throw badRequest("spec is required");
  }

  const queueName = truncate(String(spec.queue_name || "ops.default").trim(), 60);
  if (!queueName) throw badRequest("queue_name is required");

  const moduleCode = truncate(String(spec.module_code || "OPS").trim().toUpperCase(), 40);
  const jobType = truncate(String(spec.job_type || "").trim().toUpperCase(), 60);
  if (!jobType) throw badRequest("job_type is required");

  const payload = spec.payload && typeof spec.payload === "object" ? spec.payload : {};
  const payloadHash = hashPayload(payload);
  const idempotencyKey =
    spec.idempotency_key === undefined || spec.idempotency_key === null
      ? null
      : truncate(String(spec.idempotency_key).trim(), 190) || null;
  const priority = Number.isInteger(spec.priority) ? spec.priority : 100;
  const maxAttempts = Number.isInteger(spec.max_attempts) ? spec.max_attempts : 5;
  const runAfterAt = toDbDate(spec.run_after_at || new Date());

  if (idempotencyKey) {
    const existing = await runQuery(
      `SELECT *
       FROM app_jobs
       WHERE tenant_id = ? AND queue_name = ? AND idempotency_key = ?
       LIMIT 1`,
      [tId, queueName, idempotencyKey]
    );
    const row = mapJobRow(existing.rows?.[0] || null);
    if (row) return { job: row, idempotent: true };
  }

  try {
    const ins = await runQuery(
      `INSERT INTO app_jobs
       (tenant_id, queue_name, module_code, job_type, status, priority, run_after_at,
        idempotency_key, payload_json, payload_hash, max_attempts, created_by)
       VALUES (?, ?, ?, ?, 'QUEUED', ?, ?, ?, ?, ?, ?, ?)`,
      [
        tId,
        queueName,
        moduleCode,
        jobType,
        priority,
        runAfterAt,
        idempotencyKey,
        safeJson(payload),
        payloadHash,
        Math.max(1, maxAttempts),
        parsePositiveInt(userId) || null,
      ]
    );
    const createdId = parsePositiveInt(ins.rows?.insertId);
    const row = await getJobRow({ tenantId: tId, jobId: createdId, runQuery });
    return { job: row, idempotent: false };
  } catch (err) {
    if (!idempotencyKey || !isDup(err)) throw err;
    const existing = await runQuery(
      `SELECT *
       FROM app_jobs
       WHERE tenant_id = ? AND queue_name = ? AND idempotency_key = ?
       LIMIT 1`,
      [tId, queueName, idempotencyKey]
    );
    const row = mapJobRow(existing.rows?.[0] || null);
    if (row) return { job: row, idempotent: true };
    throw err;
  }
}

export async function listJobs({ tenantId, filters = {}, runQuery = query }) {
  const tId = requireTenantId(tenantId);
  const where = ["j.tenant_id = ?"];
  const params = [tId];

  const status = normalizeStatus(filters.status);
  if (status) {
    where.push("j.status = ?");
    params.push(status);
  }
  const moduleCode = up(filters.moduleCode || filters.module_code);
  if (moduleCode) {
    where.push("j.module_code = ?");
    params.push(moduleCode);
  }
  const jobType = up(filters.jobType || filters.job_type);
  if (jobType) {
    where.push("j.job_type = ?");
    params.push(jobType);
  }
  const queueName = String(filters.queueName || filters.queue_name || "").trim();
  if (queueName) {
    where.push("j.queue_name = ?");
    params.push(queueName);
  }

  const limit = Math.min(200, Math.max(1, Number(filters.limit || 50)));
  const offset = Math.max(0, Number(filters.offset || 0));

  const countRes = await runQuery(
    `SELECT COUNT(*) AS total
     FROM app_jobs j
     WHERE ${where.join(" AND ")}`,
    params
  );

  const listRes = await runQuery(
    `SELECT j.*
     FROM app_jobs j
     WHERE ${where.join(" AND ")}
     ORDER BY
       CASE j.status
         WHEN 'RUNNING' THEN 0
         WHEN 'FAILED_RETRYABLE' THEN 1
         WHEN 'QUEUED' THEN 2
         ELSE 3
       END,
       j.run_after_at ASC,
       j.priority ASC,
       j.id DESC
     LIMIT ${Math.trunc(limit)}
     OFFSET ${Math.trunc(offset)}`,
    params
  );

  return {
    items: (listRes.rows || []).map(mapJobRow),
    total: Number(countRes.rows?.[0]?.total || 0),
    limit: Math.trunc(limit),
    offset: Math.trunc(offset),
  };
}

export async function getJobById({ tenantId, jobId, runQuery = query }) {
  const tId = requireTenantId(tenantId);
  const job = await getJobRow({ tenantId: tId, jobId, runQuery });
  if (!job) throw notFound("Job not found");
  const attempts = await getJobAttempts({ tenantId: tId, jobId: job.id, runQuery });
  return { item: job, attempts };
}

export async function cancelJob({ tenantId, jobId, userId = null }) {
  const tId = requireTenantId(tenantId);
  const id = parsePositiveInt(jobId);
  if (!id) throw badRequest("jobId must be a positive integer");

  const row = await withTransaction(async (tx) => {
    const job = await getJobRow({ tenantId: tId, jobId: id, runQuery: tx.query, forUpdate: true });
    if (!job) throw notFound("Job not found");
    const status = up(job.status);
    if (status === "RUNNING") {
      throw conflict("RUNNING jobs cannot be cancelled; wait for worker completion or requeue after failure");
    }
    if (status === "CANCELLED") {
      return job;
    }
    if (status === "SUCCEEDED") {
      throw conflict("SUCCEEDED jobs cannot be cancelled");
    }

    await tx.query(
      `UPDATE app_jobs
       SET status = 'CANCELLED',
           cancelled_by = ?,
           cancelled_at = CURRENT_TIMESTAMP,
           locked_by = NULL,
           locked_at = NULL,
           finished_at = CURRENT_TIMESTAMP,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND id = ?`,
      [parsePositiveInt(userId) || null, tId, id]
    );
    return getJobRow({ tenantId: tId, jobId: id, runQuery: tx.query });
  });

  return { item: row };
}

export async function requeueJob({
  tenantId,
  jobId,
  userId = null,
  delaySeconds = 0,
  maxAttempts = null,
}) {
  const tId = requireTenantId(tenantId);
  const id = parsePositiveInt(jobId);
  if (!id) throw badRequest("jobId must be a positive integer");
  const delay = Math.max(0, Number(delaySeconds || 0));
  const nextRunAt = addSeconds(now(), delay);

  const row = await withTransaction(async (tx) => {
    const job = await getJobRow({ tenantId: tId, jobId: id, runQuery: tx.query, forUpdate: true });
    if (!job) throw notFound("Job not found");
    if (up(job.status) === "RUNNING") {
      throw conflict("RUNNING jobs cannot be requeued");
    }

    await tx.query(
      `UPDATE app_jobs
       SET status = 'QUEUED',
           run_after_at = ?,
           locked_by = NULL,
           locked_at = NULL,
           finished_at = NULL,
           cancelled_by = NULL,
           cancelled_at = NULL,
           last_error_code = NULL,
           last_error_message = NULL,
           last_error_json = NULL,
           max_attempts = COALESCE(?, max_attempts),
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND id = ?`,
      [
        nextRunAt,
        Number.isInteger(maxAttempts) && maxAttempts > 0 ? maxAttempts : null,
        tId,
        id,
      ]
    );
    return getJobRow({ tenantId: tId, jobId: id, runQuery: tx.query });
  });

  return { item: row, requeuedByUserId: parsePositiveInt(userId) || null };
}

export async function runOneAvailableJob({
  workerId,
  queueNames = [],
  tenantId = null,
}) {
  const normalizedWorkerId = truncate(
    String(workerId || `worker:${process.pid}`).trim() || `worker:${process.pid}`,
    120
  );
  const claimed = await claimNextAvailableJob({
    tenantId: parsePositiveInt(tenantId) || null,
    workerId: normalizedWorkerId,
    queueNames,
  });
  if (!claimed) {
    return { idle: true };
  }

  const attemptNo = Number(claimed.__attemptNo || 0);
  const job = { ...claimed };
  delete job.__attemptNo;

  let handlerResult = null;
  try {
    const handler = getJobHandler(job.job_type);
    handlerResult = await handler.run({
      job,
      payload: typeof job.payload_json === "object" && job.payload_json ? job.payload_json : {},
      workerId: normalizedWorkerId,
    });

    await markJobSucceeded({ job, attemptNo, result: handlerResult });
    return {
      idle: false,
      ok: true,
      job_id: job.id,
      tenant_id: job.tenant_id,
      status: "SUCCEEDED",
      attempt_no: attemptNo,
      result: handlerResult,
    };
  } catch (err) {
    const failed = await markJobFailed({ job, attemptNo, err });
    return {
      idle: false,
      ok: false,
      job_id: job.id,
      tenant_id: job.tenant_id,
      status: failed.finalStatus,
      attempt_no: attemptNo,
      retryable: failed.retryable,
      next_run_at: failed.nextRunAt || null,
      error: {
        code: truncate(String(err?.errorCode || err?.code || "JOB_HANDLER_ERROR"), 80),
        message: truncate(String(err?.message || "Job execution failed"), 500),
      },
    };
  }
}

export default {
  enqueueJob,
  listJobs,
  getJobById,
  cancelJob,
  requeueJob,
  runOneAvailableJob,
};
