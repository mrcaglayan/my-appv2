import { useCallback, useMemo, useState } from "react";
import { emitToast } from "../toast/toastBus.js";

function resolveNextValue(nextValue, previousValue) {
  if (typeof nextValue === "function") {
    return nextValue(previousValue);
  }
  return nextValue;
}

function normalizeMessage(value) {
  return String(value || "").trim();
}

export function useToastMessage(initialValue = "", options = {}) {
  const inline = Boolean(options?.inline);
  const toastType = String(options?.toastType || "success").trim().toLowerCase();
  const toastTitle = String(options?.toastTitle || "").trim() || null;
  const toastSource = String(options?.toastSource || "page-message")
    .trim()
    .toLowerCase();
  const dedupeKey = String(options?.dedupeKey || "").trim() || null;
  const durationMs = options?.durationMs;

  const [inlineMessage, setInlineMessage] = useState(initialValue);

  const setMessage = useCallback(
    (nextValue) => {
      setInlineMessage((previousValue) => {
        const resolvedValue = resolveNextValue(nextValue, previousValue);
        const message = normalizeMessage(resolvedValue);
        if (message) {
          emitToast({
            type: toastType,
            title: toastTitle,
            message,
            dedupeKey,
            durationMs,
            source: toastSource,
          });
        }
        return inline ? resolvedValue : "";
      });
    },
    [dedupeKey, durationMs, inline, toastSource, toastTitle, toastType]
  );

  const message = useMemo(() => (inline ? inlineMessage : ""), [inline, inlineMessage]);
  return [message, setMessage];
}
