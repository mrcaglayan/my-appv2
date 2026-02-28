import { useCallback, useEffect, useRef, useState } from "react";
import { subscribeApiError } from "../api/client.js";
import { subscribeToast } from "../toast/toastBus.js";

const MAX_TOASTS = 5;

function toSignature(payload) {
  return [
    payload?.source ?? "",
    payload?.type ?? "",
    payload?.dedupeKey ?? "",
    payload?.status ?? "",
    payload?.code ?? "",
    payload?.requestId ?? "",
    payload?.message ?? "",
    payload?.title ?? "",
  ].join("|");
}

function toCopyValue(toast) {
  return JSON.stringify(
    {
      message: toast.message,
      code: toast.code,
      status: toast.status,
      requestId: toast.requestId,
      details: toast.details,
      method: toast.method,
      url: toast.url,
      at: new Date(toast.at).toISOString(),
    },
    null,
    2
  );
}

function isServerError(status) {
  const parsed = Number(status || 0);
  return Number.isInteger(parsed) && parsed >= 500;
}

function getTypeMeta(type) {
  const normalized = String(type || "").toLowerCase();
  if (normalized === "success") {
    return {
      label: "Success",
      containerClassName: "border-emerald-200 bg-white",
      headingClassName: "text-emerald-700",
    };
  }
  if (normalized === "warning") {
    return {
      label: "Warning",
      containerClassName: "border-amber-200 bg-white",
      headingClassName: "text-amber-700",
    };
  }
  if (normalized === "info") {
    return {
      label: "Info",
      containerClassName: "border-sky-200 bg-white",
      headingClassName: "text-sky-700",
    };
  }
  return {
    label: "Error",
    containerClassName: "border-rose-200 bg-white",
    headingClassName: "text-rose-700",
  };
}

function createToastId(prefix = "toast") {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function resolveDurationMs(payload) {
  const configured = Number(payload?.durationMs);
  if (Number.isFinite(configured) && configured > 0) {
    return configured;
  }
  const type = String(payload?.type || "").toLowerCase();
  if (type === "error") {
    return isServerError(payload?.status) ? 12000 : 9000;
  }
  if (type === "warning") {
    return 7000;
  }
  return 4500;
}

function canCopyDetails(toast) {
  return Boolean(
    toast?.source === "api" ||
      toast?.requestId ||
      toast?.code ||
      toast?.status ||
      toast?.details
  );
}

export default function ApiErrorToasts() {
  const [toasts, setToasts] = useState([]);
  const [copiedToastId, setCopiedToastId] = useState("");
  const timeoutMapRef = useRef(new Map());

  const dismissToast = useCallback((toastId) => {
    setToasts((previous) => previous.filter((item) => item.id !== toastId));
    const timeoutId = timeoutMapRef.current.get(toastId);
    if (timeoutId) {
      window.clearTimeout(timeoutId);
      timeoutMapRef.current.delete(toastId);
    }
  }, []);

  useEffect(() => {
    const timeoutMap = timeoutMapRef.current;
    function queueToast(payload, idPrefix) {
      const message = String(payload?.message || "").trim();
      if (!message) {
        return;
      }
      const id = createToastId(idPrefix);
      const type = String(payload?.type || "info").toLowerCase();
      const source = String(payload?.source || "app").toLowerCase();
      const signature = toSignature(payload);
      const nextToast = {
        id,
        at: Date.now(),
        type,
        source,
        title: String(payload?.title || "").trim() || null,
        status: payload?.status || null,
        message,
        code: payload?.code || null,
        requestId: payload?.requestId || null,
        details: payload?.details ?? null,
        method: payload?.method || null,
        url: payload?.url || null,
        signature,
      };

      setToasts((previous) => {
        if (previous.some((item) => item.signature === signature)) {
          return previous;
        }
        return [nextToast, ...previous].slice(0, MAX_TOASTS);
      });

      const timeoutId = window.setTimeout(() => {
        dismissToast(id);
      }, resolveDurationMs(payload));
      timeoutMap.set(id, timeoutId);
    }

    const unsubscribeApi = subscribeApiError((payload) => {
      queueToast(
        {
          ...payload,
          type: "error",
          source: "api",
          title: "API Error",
        },
        "api-toast"
      );
    });
    const unsubscribeApp = subscribeToast((payload) => {
      queueToast(payload, "app-toast");
    });

    return () => {
      unsubscribeApi();
      unsubscribeApp();
      for (const timeoutId of timeoutMap.values()) {
        window.clearTimeout(timeoutId);
      }
      timeoutMap.clear();
    };
  }, [dismissToast]);

  async function handleCopy(toast) {
    const value = toCopyValue(toast);
    if (!navigator?.clipboard?.writeText) {
      return;
    }
    try {
      await navigator.clipboard.writeText(value);
      setCopiedToastId(toast.id);
      window.setTimeout(() => setCopiedToastId(""), 1400);
    } catch {
      // Ignore clipboard failures.
    }
  }

  if (toasts.length === 0) {
    return null;
  }

  return (
    <div className="pointer-events-none fixed right-3 top-3 z-[70] flex w-[min(28rem,92vw)] flex-col gap-2">
      {toasts.map((toast) => (
        <div
          key={toast.id}
          className={`pointer-events-auto rounded-lg border p-3 shadow-lg ${getTypeMeta(toast.type).containerClassName}`}
        >
          <p className={`text-sm font-semibold ${getTypeMeta(toast.type).headingClassName}`}>
            {toast.title || getTypeMeta(toast.type).label}
          </p>
          <p className="mt-1 text-sm text-slate-800">{toast.message}</p>
          <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-600">
            {toast.status ? <span>Status: {toast.status}</span> : null}
            {toast.code ? <span>Code: {toast.code}</span> : null}
            {toast.requestId ? <span>Request ID: {toast.requestId}</span> : null}
          </div>
          <div className="mt-3 flex items-center gap-2">
            {canCopyDetails(toast) ? (
              <button
                type="button"
                onClick={() => handleCopy(toast)}
                className="rounded border border-slate-300 px-2 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-50"
              >
                {copiedToastId === toast.id ? "Copied" : "Copy details"}
              </button>
            ) : null}
            <button
              type="button"
              onClick={() => dismissToast(toast.id)}
              className="rounded border border-slate-300 px-2 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-50"
            >
              Dismiss
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}
