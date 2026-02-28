import express from "express";
import { assertScopeAccess, buildScopeFilter, requirePermission } from "../middleware/rbac.js";
import { asyncHandler, parsePositiveInt } from "./_utils.js";
import { enqueueJob } from "../services/jobs.service.js";
import {
  createRetentionPolicy,
  executeDataRetentionPolicyRun,
  getRetentionRunDetail,
  listRetentionPolicyRows,
  listRetentionRunRows,
  resolveDataRetentionPolicyScope,
  resolveDataRetentionRunScope,
  updateRetentionPolicy,
} from "../services/retentionPolicies.service.js";
import {
  createPayrollPeriodExportSnapshot,
  getPeriodExportSnapshotDetail,
  listPeriodExportSnapshotRows,
  resolvePayrollCloseScopeForSnapshot,
  resolvePeriodExportSnapshotScope,
} from "../services/exportSnapshots.service.js";
import {
  parseExportSnapshotCreateInput,
  parseExportSnapshotListInput,
  parseExportSnapshotReadInput,
  parseRetentionPolicyCreateInput,
  parseRetentionPolicyListInput,
  parseRetentionPolicyUpdateInput,
  parseRetentionRunExecuteInput,
  parseRetentionRunListInput,
  parseRetentionRunReadInput,
} from "./retention.admin.validators.js";

const router = express.Router();

function resolveLegalEntityScope(input = {}) {
  const legalEntityId = parsePositiveInt(input.legalEntityId ?? input.legal_entity_id);
  if (!legalEntityId) return null;
  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: legalEntityId,
  };
}

router.get(
  "/retention/policies",
  requirePermission("ops.retention.read", {
    resolveScope: async (req) => resolveLegalEntityScope(req.query),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseRetentionPolicyListInput(req);
    const result = await listRetentionPolicyRows({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: filters.tenantId, ...result });
  })
);

router.post(
  "/retention/policies",
  requirePermission("ops.retention.manage", {
    resolveScope: async (req) => resolveLegalEntityScope(req.body),
  }),
  asyncHandler(async (req, res) => {
    const input = parseRetentionPolicyCreateInput(req);
    const result = await createRetentionPolicy({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      input,
      assertScopeAccess,
    });
    return res.status(201).json({ tenantId: input.tenantId, ...result });
  })
);

router.patch(
  "/retention/policies/:policyId",
  requirePermission("ops.retention.manage", {
    resolveScope: async (req, tenantId) =>
      resolveDataRetentionPolicyScope(req.params?.policyId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseRetentionPolicyUpdateInput(req);
    const result = await updateRetentionPolicy({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      policyId: input.policyId,
      input,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.post(
  "/retention/policies/:policyId/run",
  requirePermission("ops.retention.manage", {
    resolveScope: async (req, tenantId) =>
      resolveDataRetentionPolicyScope(req.params?.policyId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseRetentionRunExecuteInput(req);

    if (input.asyncMode) {
      const jobIdempotencyKey = input.runIdempotencyKey
        ? `RETENTION_RUN:${input.policyId}:${input.runIdempotencyKey}`
        : null;

      const queued = await enqueueJob({
        tenantId: input.tenantId,
        userId: input.userId,
        spec: {
          queue_name: "ops.retention",
          module_code: "OPS",
          job_type: "DATA_RETENTION_RUN",
          idempotency_key: jobIdempotencyKey,
          payload: {
            policy_id: input.policyId,
            acting_user_id: input.userId,
            run_idempotency_key: input.runIdempotencyKey || null,
            trigger_mode: "JOB",
          },
        },
      });

      return res.status(202).json({
        tenantId: input.tenantId,
        queued: true,
        idempotent: Boolean(queued?.idempotent),
        job: queued?.job || null,
      });
    }

    const result = await executeDataRetentionPolicyRun({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      policyId: input.policyId,
      input,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.get(
  "/retention/runs",
  requirePermission("ops.retention.read", {
    resolveScope: async (req) => resolveLegalEntityScope(req.query),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseRetentionRunListInput(req);
    const result = await listRetentionRunRows({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: filters.tenantId, ...result });
  })
);

router.get(
  "/retention/runs/:runId",
  requirePermission("ops.retention.read", {
    resolveScope: async (req, tenantId) =>
      resolveDataRetentionRunScope(req.params?.runId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseRetentionRunReadInput(req);
    const result = await getRetentionRunDetail({
      req,
      tenantId: input.tenantId,
      runId: input.runId,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.get(
  "/export-snapshots",
  requirePermission("ops.export_snapshot.read", {
    resolveScope: async (req) => resolveLegalEntityScope(req.query),
  }),
  asyncHandler(async (req, res) => {
    const filters = parseExportSnapshotListInput(req);
    const result = await listPeriodExportSnapshotRows({
      req,
      tenantId: filters.tenantId,
      filters,
      buildScopeFilter,
      assertScopeAccess,
    });
    return res.json({ tenantId: filters.tenantId, ...result });
  })
);

router.get(
  "/export-snapshots/:snapshotId",
  requirePermission("ops.export_snapshot.read", {
    resolveScope: async (req, tenantId) =>
      resolvePeriodExportSnapshotScope(req.params?.snapshotId, tenantId),
  }),
  asyncHandler(async (req, res) => {
    const input = parseExportSnapshotReadInput(req);
    const result = await getPeriodExportSnapshotDetail({
      req,
      tenantId: input.tenantId,
      snapshotId: input.snapshotId,
      assertScopeAccess,
    });
    return res.json({ tenantId: input.tenantId, ...result });
  })
);

router.post(
  "/export-snapshots",
  requirePermission("ops.export_snapshot.create", {
    resolveScope: async (req, tenantId) => {
      const directScope = resolveLegalEntityScope(req.body);
      if (directScope) return directScope;
      const closeId = parsePositiveInt(
        req.body?.payrollPeriodCloseId ?? req.body?.payroll_period_close_id ?? req.body?.closeId
      );
      if (!closeId) return null;
      return resolvePayrollCloseScopeForSnapshot(closeId, tenantId);
    },
  }),
  asyncHandler(async (req, res) => {
    const input = parseExportSnapshotCreateInput(req);
    const result = await createPayrollPeriodExportSnapshot({
      req,
      tenantId: input.tenantId,
      userId: input.userId,
      input,
      assertScopeAccess,
    });
    return res.status(201).json({ tenantId: input.tenantId, ...result });
  })
);

export default router;
