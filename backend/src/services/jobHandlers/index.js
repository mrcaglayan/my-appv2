import bankFeedPullHandler from "./bankFeedPull.handler.js";
import bankWebhookProcessHandler from "./bankWebhookProcess.handler.js";
import dataRetentionRunHandler from "./dataRetentionRun.handler.js";
import paymentSyncRetryHandler from "./paymentSyncRetry.handler.js";
import payrollImportApplyHandler from "./payrollImportApply.handler.js";
import secretsBackfillReencryptHandler from "./secretsBackfillReencrypt.handler.js";

const HANDLERS = {
  BANK_FEED_PULL: bankFeedPullHandler,
  BANK_WEBHOOK_PROCESS: bankWebhookProcessHandler,
  DATA_RETENTION_RUN: dataRetentionRunHandler,
  PAYMENT_SYNC_RETRY: paymentSyncRetryHandler,
  PAYROLL_IMPORT_APPLY: payrollImportApplyHandler,
  SECRETS_BACKFILL_REENCRYPT: secretsBackfillReencryptHandler,
};

export function getJobHandler(jobType) {
  const key = String(jobType || "")
    .trim()
    .toUpperCase();
  const handler = HANDLERS[key];
  if (handler) return handler;

  const err = new Error(`Unsupported job type: ${jobType}`);
  err.status = 400;
  err.errorCode = "JOB_HANDLER_UNSUPPORTED";
  err.retryable = false;
  throw err;
}

export default {
  getJobHandler,
};
