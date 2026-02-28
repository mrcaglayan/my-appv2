import axios from "axios";

let onUnauthorized = null;
export function setOnUnauthorized(fn) {
  onUnauthorized = fn;
}

const apiErrorListeners = new Set();

export function subscribeApiError(listener) {
  if (typeof listener !== "function") {
    return () => {};
  }
  apiErrorListeners.add(listener);
  return () => {
    apiErrorListeners.delete(listener);
  };
}

function emitApiError(payload) {
  for (const listener of apiErrorListeners) {
    try {
      listener(payload);
    } catch {
      // Ignore listener failures so one broken subscriber does not affect requests.
    }
  }
}

function extractRequestId(error) {
  return (
    error?.response?.data?.requestId ||
    error?.response?.headers?.["x-request-id"] ||
    null
  );
}

function resolveFallbackMessage(status) {
  if (!status) return "Network error. Please try again.";
  if (status >= 500) return "Server error. Please try again.";
  if (status === 404) return "Resource not found.";
  if (status === 403) return "You do not have permission for this action.";
  if (status === 401) return "Session expired. Please sign in again.";
  if (status === 400 || status === 422) return "Request is invalid.";
  return "Request failed.";
}

function normalizeApiError(error) {
  const status = Number(error?.response?.status || 0) || null;
  const data = error?.response?.data || {};
  const requestId = extractRequestId(error);
  const message = String(
    data?.message || error?.message || resolveFallbackMessage(status)
  ).trim();
  const code = String(data?.code || error?.code || "").trim() || null;
  const details = data?.details ?? null;

  return {
    status,
    message: message || resolveFallbackMessage(status),
    code,
    details,
    requestId,
  };
}

export function getNormalizedApiError(error) {
  const existing = error?.normalizedError;
  if (existing && typeof existing === "object") {
    return existing;
  }
  return normalizeApiError(error);
}

function shouldNotifyApiError(error, normalized) {
  if (error?.code === "ERR_CANCELED") return false;
  if (normalized.status === 401) return false;
  if (error?.config?.skipGlobalErrorToast) return false;

  const mode = String(error?.config?.errorToastMode || "all")
    .trim()
    .toLowerCase();
  if (mode === "none") return false;
  if (mode === "server") {
    return !normalized.status || normalized.status >= 500;
  }
  return true;
}

export const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || "http://localhost:3000",
  timeout: 20000,
  withCredentials: true,
});

api.interceptors.response.use(
  (res) => res,
  (err) => {
    const status = err?.response?.status;
    const skipAuthRedirect = Boolean(err?.config?.skipAuthRedirect);
    const normalized = normalizeApiError(err);
    err.normalizedError = normalized;
    err.requestId = normalized.requestId;
    if (normalized.code) {
      err.errorCode = normalized.code;
    }

    if (shouldNotifyApiError(err, normalized)) {
      emitApiError({
        ...normalized,
        at: Date.now(),
        method: String(err?.config?.method || "").toUpperCase() || null,
        url: err?.config?.url || null,
      });
    }

    if (status === 401) {
      // Cookie session expired/invalid.
      if (!skipAuthRedirect && typeof onUnauthorized === "function") onUnauthorized();
    }
    return Promise.reject(err);
  }
);
