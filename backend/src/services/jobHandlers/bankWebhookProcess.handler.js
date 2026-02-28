function notImplementedError(message) {
  const err = new Error(message);
  err.status = 501;
  err.errorCode = "JOB_HANDLER_NOT_IMPLEMENTED";
  err.retryable = false;
  return err;
}

const bankWebhookProcessHandler = {
  async run({ payload }) {
    throw notImplementedError(
      `BANK_WEBHOOK_PROCESS handler is not wired yet for this repo (webhook_event_id=${payload?.webhook_event_id ?? "?"})`
    );
  },
};

export default bankWebhookProcessHandler;
