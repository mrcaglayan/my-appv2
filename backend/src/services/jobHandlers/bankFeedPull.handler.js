import { runBankConnectorStatementSync } from "../bank.connectors.service.js";

function badJobPayload(message) {
  const err = new Error(message);
  err.status = 400;
  err.errorCode = "JOB_PAYLOAD_INVALID";
  err.retryable = false;
  return err;
}

function parsePositiveInt(value) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

function parseBoolean(value) {
  if (value === true || value === false) return value;
  const text = String(value || "").trim().toLowerCase();
  return text === "1" || text === "true" || text === "yes";
}

const bankFeedPullHandler = {
  async run({ job, payload }) {
    const tenantId = parsePositiveInt(job?.tenant_id ?? payload?.tenant_id);
    const connectorId = parsePositiveInt(
      payload?.connector_id ?? payload?.bank_connector_id ?? payload?.provider_connection_id
    );

    if (!tenantId) {
      throw badJobPayload("tenant_id is required for BANK_FEED_PULL");
    }
    if (!connectorId) {
      throw badJobPayload("connector_id (or bank_connector_id) is required for BANK_FEED_PULL");
    }

    const result = await runBankConnectorStatementSync({
      tenantId,
      connectorId,
      userId: null,
      input: {
        fromDate: payload?.from_date ?? payload?.fromDate ?? null,
        toDate: payload?.to_date ?? payload?.toDate ?? null,
        requestId:
          payload?.request_id ??
          payload?.requestId ??
          (job?.id ? `JOB|BANK_FEED_PULL|${job.id}` : null),
        forceFull: parseBoolean(payload?.force_full ?? payload?.forceFull),
      },
    });

    return {
      tenant_id: tenantId,
      connector_id: connectorId,
      idempotent: Boolean(result?.idempotent),
      sync_run_id: result?.sync_run?.id || null,
      sync_status: result?.sync_run?.status || null,
    };
  },
};

export default bankFeedPullHandler;
