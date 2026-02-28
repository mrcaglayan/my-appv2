export function toCariQueryString(params = {}) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") continue;
    searchParams.set(key, String(value));
  }
  const query = searchParams.toString();
  return query ? `?${query}` : "";
}

export function parseCariApiError(error) {
  const status = Number(error?.response?.status || error?.status || 0) || null;
  const data = error?.response?.data || error?.data || {};
  const message = String(data?.message || error?.message || "Request failed");
  const requestId = data?.requestId || error?.requestId || null;
  const isIdempotentReplay = Boolean(data?.idempotentReplay);
  const followUpRisks = Array.isArray(data?.followUpRisks) ? data.followUpRisks : [];

  // Keep legacy compatibility for pages/utilities that still read
  // err.response.data.message/requestId (Axios-style shape).
  const response = {
    status,
    data: {
      ...data,
      message,
      requestId,
      idempotentReplay: isIdempotentReplay,
      followUpRisks,
    },
  };

  return {
    status,
    message,
    requestId,
    isValidation: status === 400,
    isPermission: status === 401 || status === 403,
    isIdempotentReplay,
    followUpRisks,
    response,
    originalError: error,
  };
}

export function extractCariReplayAndRisks(payload) {
  return {
    idempotentReplay: Boolean(payload?.idempotentReplay),
    followUpRisks: Array.isArray(payload?.followUpRisks) ? payload.followUpRisks : [],
  };
}
