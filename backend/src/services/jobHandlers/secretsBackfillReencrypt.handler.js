import { parsePositiveInt } from "../../routes/_utils.js";
import { runSecretsMigrationUntilStable } from "../secretsMigration.service.js";

function badJobPayload(message, code) {
  const err = new Error(message);
  err.status = 400;
  err.errorCode = code || "JOB_PAYLOAD_INVALID";
  err.retryable = false;
  return err;
}

function toBool(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  if (value === true || value === false) return value;
  const text = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(text)) return true;
  if (["0", "false", "no", "off"].includes(text)) return false;
  return fallback;
}

function toPositiveInt(value, fallback = null) {
  const parsed = parsePositiveInt(value);
  return parsed || fallback;
}

const secretsBackfillReencryptHandler = {
  async run({ job, payload }) {
    const tenantId = toPositiveInt(job?.tenant_id ?? payload?.tenant_id);
    if (!tenantId) {
      throw badJobPayload(
        "tenant_id is required for SECRETS_BACKFILL_REENCRYPT",
        "JOB_SECRETS_MISSING_TENANT"
      );
    }

    const legalEntityId = toPositiveInt(
      payload?.legal_entity_id ?? payload?.legalEntityId,
      null
    );
    const actingUserId = toPositiveInt(payload?.acting_user_id ?? payload?.actingUserId, null);
    const limit = toPositiveInt(payload?.limit, 200) || 200;
    const maxPasses = toPositiveInt(payload?.max_passes ?? payload?.maxPasses, 20) || 20;
    const mode = String(payload?.mode || "BOTH")
      .trim()
      .toUpperCase();

    const result = await runSecretsMigrationUntilStable({
      tenantId,
      legalEntityId,
      userId: actingUserId,
      limit,
      mode,
      forceReencrypt: toBool(payload?.force_reencrypt ?? payload?.forceReencrypt, false),
      maxPasses,
    });

    return {
      ok: true,
      tenant_id: tenantId,
      legal_entity_id: legalEntityId,
      mode,
      pass_count: Number(result?.pass_count || 0),
      totals: result?.totals || null,
      post_check: result?.post_check || null,
      error_count: Number(result?.totals?.error_count || 0),
    };
  },
};

export default secretsBackfillReencryptHandler;
