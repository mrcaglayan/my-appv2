import { parsePositiveInt } from "../../routes/_utils.js";
import { executeDataRetentionPolicyRun } from "../retentionPolicies.service.js";

function badJobPayload(message, code) {
  const err = new Error(message);
  err.status = 400;
  err.errorCode = code || "JOB_PAYLOAD_INVALID";
  err.retryable = false;
  return err;
}

function noopScopeAccess() {
  return true;
}

function normalizeTriggerMode(value, fallback = "JOB") {
  const mode = String(value || fallback)
    .trim()
    .toUpperCase();
  if (["MANUAL", "SCHEDULED", "JOB"].includes(mode)) return mode;
  return fallback;
}

const dataRetentionRunHandler = {
  async run({ job, payload }) {
    const tenantId = parsePositiveInt(job?.tenant_id ?? payload?.tenant_id);
    const policyId = parsePositiveInt(payload?.policy_id ?? payload?.policyId);
    const actingUserId = parsePositiveInt(payload?.acting_user_id ?? payload?.actingUserId) || null;
    const runIdempotencyKeyRaw = String(
      (payload?.run_idempotency_key ?? payload?.runIdempotencyKey) || ""
    ).trim();
    const runIdempotencyKey = runIdempotencyKeyRaw ? runIdempotencyKeyRaw.slice(0, 190) : null;
    const triggerMode = normalizeTriggerMode(payload?.trigger_mode ?? payload?.triggerMode, "JOB");

    if (!tenantId) {
      throw badJobPayload("tenant_id is required for DATA_RETENTION_RUN", "JOB_RETENTION_MISSING_TENANT");
    }
    if (!policyId) {
      throw badJobPayload("policy_id is required for DATA_RETENTION_RUN", "JOB_RETENTION_MISSING_POLICY_ID");
    }

    const result = await executeDataRetentionPolicyRun({
      req: null,
      tenantId,
      userId: actingUserId,
      policyId,
      input: {
        triggerMode,
        runIdempotencyKey: runIdempotencyKey || (job?.id ? `JOB:${job.id}` : null),
      },
      assertScopeAccess: noopScopeAccess,
    });

    return {
      ok: true,
      retention_run_id: parsePositiveInt(result?.row?.id) || null,
      retention_status: result?.row?.status || null,
      retention_policy_id: parsePositiveInt(result?.row?.data_retention_policy_id) || policyId,
      idempotent: Boolean(result?.idempotent),
    };
  },
};

export default dataRetentionRunHandler;
