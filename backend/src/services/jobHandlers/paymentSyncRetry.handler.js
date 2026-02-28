function notImplementedError(message) {
  const err = new Error(message);
  err.status = 501;
  err.errorCode = "JOB_HANDLER_NOT_IMPLEMENTED";
  err.retryable = false;
  return err;
}

const paymentSyncRetryHandler = {
  async run({ payload }) {
    throw notImplementedError(
      `PAYMENT_SYNC_RETRY handler is not wired yet for this repo (payment_batch_id=${payload?.payment_batch_id ?? "?"})`
    );
  },
};

export default paymentSyncRetryHandler;
