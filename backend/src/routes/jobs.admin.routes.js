import express from "express";
import { requirePermission } from "../middleware/rbac.js";
import { asyncHandler, badRequest, parsePositiveInt, resolveTenantId } from "./_utils.js";
import {
  cancelJob,
  getJobById,
  listJobs,
  requeueJob,
  runOneAvailableJob,
} from "../services/jobs.service.js";

const router = express.Router();

function requireTenantIdFromReq(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }
  return tenantId;
}

function parseJobIdParam(req) {
  const jobId = parsePositiveInt(req.params?.id);
  if (!jobId) throw badRequest("id must be a positive integer");
  return jobId;
}

function parseQueueNamesInput(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  if (typeof value === "string") {
    return value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }
  return [];
}

router.get(
  "/",
  requirePermission("ops.jobs.read"),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantIdFromReq(req);
    const result = await listJobs({
      tenantId,
      filters: {
        status: req.query?.status,
        moduleCode: req.query?.moduleCode ?? req.query?.module_code,
        jobType: req.query?.jobType ?? req.query?.job_type,
        queueName: req.query?.queueName ?? req.query?.queue_name,
        limit: req.query?.limit,
        offset: req.query?.offset,
      },
    });
    return res.json({ tenantId, ...result });
  })
);

router.get(
  "/:id",
  requirePermission("ops.jobs.read"),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantIdFromReq(req);
    const jobId = parseJobIdParam(req);
    const result = await getJobById({ tenantId, jobId });
    return res.json({ tenantId, ...result });
  })
);

router.post(
  "/:id/cancel",
  requirePermission("ops.jobs.manage"),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantIdFromReq(req);
    const jobId = parseJobIdParam(req);
    const result = await cancelJob({
      tenantId,
      jobId,
      userId: parsePositiveInt(req.user?.id) || null,
    });
    return res.json({ tenantId, ...result });
  })
);

router.post(
  "/:id/requeue",
  requirePermission("ops.jobs.manage"),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantIdFromReq(req);
    const jobId = parseJobIdParam(req);
    const delaySeconds = req.body?.delaySeconds ?? req.body?.delay_seconds ?? 0;
    const maxAttempts = req.body?.maxAttempts ?? req.body?.max_attempts ?? null;
    const result = await requeueJob({
      tenantId,
      jobId,
      userId: parsePositiveInt(req.user?.id) || null,
      delaySeconds: Number(delaySeconds || 0),
      maxAttempts:
        Number.isInteger(Number(maxAttempts)) && Number(maxAttempts) > 0
          ? Number(maxAttempts)
          : null,
    });
    return res.json({ tenantId, ...result });
  })
);

router.post(
  "/run-once",
  requirePermission("ops.jobs.run"),
  asyncHandler(async (req, res) => {
    const tenantId = requireTenantIdFromReq(req);
    const workerId =
      String(req.body?.workerId || "").trim() ||
      `http:${tenantId}:${parsePositiveInt(req.user?.id) || "system"}`;
    const queueNames = parseQueueNamesInput(req.body?.queueNames ?? req.body?.queue_names);
    const result = await runOneAvailableJob({ tenantId, workerId, queueNames });
    return res.json({ tenantId, ...result });
  })
);

export default router;
