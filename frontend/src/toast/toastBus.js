const toastListeners = new Set();

function normalizeType(value) {
  const normalized = String(value || "info")
    .trim()
    .toLowerCase();
  if (normalized === "success" || normalized === "warning" || normalized === "error") {
    return normalized;
  }
  return "info";
}

function normalizeMessage(value) {
  return String(value || "").trim();
}

function normalizeDuration(value, fallbackMs) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallbackMs;
}

export function subscribeToast(listener) {
  if (typeof listener !== "function") {
    return () => {};
  }
  toastListeners.add(listener);
  return () => {
    toastListeners.delete(listener);
  };
}

export function emitToast(payload) {
  const message = normalizeMessage(payload?.message);
  if (!message) {
    return;
  }

  const type = normalizeType(payload?.type);
  const toast = {
    type,
    message,
    title: String(payload?.title || "").trim() || null,
    durationMs: normalizeDuration(payload?.durationMs, type === "error" ? 9000 : 4500),
    dedupeKey: String(payload?.dedupeKey || "").trim() || null,
    at: Date.now(),
    source: String(payload?.source || "app").trim().toLowerCase(),
  };

  for (const listener of toastListeners) {
    try {
      listener(toast);
    } catch {
      // Ignore listener failures so one broken subscriber does not affect others.
    }
  }
}

export function toastSuccess(message, options = {}) {
  emitToast({ ...options, type: "success", message });
}

export function toastInfo(message, options = {}) {
  emitToast({ ...options, type: "info", message });
}

export function toastWarning(message, options = {}) {
  emitToast({ ...options, type: "warning", message });
}

export function toastError(message, options = {}) {
  emitToast({ ...options, type: "error", message });
}
