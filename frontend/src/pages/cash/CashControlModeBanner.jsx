import { useEffect, useState } from "react";
import { getCashConfig } from "../../api/cashAdmin.js";
import { useI18n } from "../../i18n/useI18n.js";

const VALID_MODES = new Set(["OFF", "WARN", "ENFORCE"]);

function normalizeCashControlMode(value) {
  const normalized = String(value || "").trim().toUpperCase();
  if (VALID_MODES.has(normalized)) {
    return normalized;
  }
  return null;
}

function extractRequestId(err) {
  return (
    err?.response?.data?.requestId ||
    err?.response?.headers?.["x-request-id"] ||
    null
  );
}

export default function CashControlModeBanner() {
  const { t } = useI18n();
  const [mode, setMode] = useState(null);
  const [loadErrorRequestId, setLoadErrorRequestId] = useState(null);
  const [showLoadError, setShowLoadError] = useState(false);

  useEffect(() => {
    let active = true;

    (async () => {
      try {
        const result = await getCashConfig();
        if (!active) {
          return;
        }
        setMode(normalizeCashControlMode(result?.cashControlMode));
        setShowLoadError(false);
        setLoadErrorRequestId(null);
      } catch (err) {
        if (!active) {
          return;
        }

        const status = Number(err?.response?.status || 0);
        if (status === 404 || status === 405) {
          setMode(null);
          setShowLoadError(false);
          setLoadErrorRequestId(null);
          return;
        }

        setMode(null);
        setShowLoadError(true);
        setLoadErrorRequestId(extractRequestId(err));
      }
    })();

    return () => {
      active = false;
    };
  }, []);

  if (mode) {
    const modeKey = `cashControlMode.modes.${mode}`;
    const descriptionKey = `cashControlMode.descriptions.${mode}`;

    const className =
      mode === "ENFORCE"
        ? "border-emerald-200 bg-emerald-50 text-emerald-800"
        : mode === "WARN"
        ? "border-amber-200 bg-amber-50 text-amber-800"
        : "border-slate-200 bg-slate-50 text-slate-700";

    return (
      <div className={`rounded-lg border px-3 py-2 text-sm ${className}`}>
        <p className="font-semibold">
          {t("cashControlMode.title", { mode: t(modeKey) })}
        </p>
        <p className="mt-1">{t(descriptionKey)}</p>
      </div>
    );
  }

  if (!showLoadError) {
    return null;
  }

  return (
    <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
      <p>{t("cashControlMode.unavailable")}</p>
      {loadErrorRequestId ? (
        <p className="mt-1 text-xs font-medium text-amber-800">
          {t("cashControlMode.requestId", { requestId: loadErrorRequestId })}
        </p>
      ) : null}
    </div>
  );
}
