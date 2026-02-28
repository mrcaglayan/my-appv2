import { Navigate, useLocation } from "react-router-dom";
import { useI18n } from "../i18n/useI18n.js";
import { useTenantReadiness } from "./useTenantReadiness.js";

const SETUP_ALLOWLIST = new Set([
  "/app/ayarlar/sirket-ayarlari",
  "/app/ayarlar/organizasyon-yonetimi",
  "/app/ayarlar/hesap-plani-ayarlari",
]);

export default function RequireTenantReadiness({ children }) {
  const location = useLocation();
  const { t } = useI18n();
  const { loading, error, readiness, refresh } = useTenantReadiness();

  if (loading) {
    return (
      <div className="grid min-h-[40vh] place-items-center">
        <div className="text-slate-600">{t("readinessGuard.checking")}</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="mx-auto max-w-2xl rounded-xl border border-amber-200 bg-amber-50 p-5">
        <h2 className="text-lg font-semibold text-amber-900">
          {t("readinessGuard.failedTitle")}
        </h2>
        <p className="mt-1 text-sm text-amber-800">{error}</p>
        <button
          type="button"
          onClick={() => refresh()}
          className="mt-3 rounded-lg border border-amber-300 bg-white px-3 py-1.5 text-sm font-semibold text-amber-900"
        >
          {t("readinessGuard.retry")}
        </button>
      </div>
    );
  }

  if (readiness?.ready) {
    return children;
  }

  const isSetupPage = SETUP_ALLOWLIST.has(location.pathname);
  if (!isSetupPage) {
    return <Navigate to="/app/ayarlar/sirket-ayarlari" replace />;
  }

  return children;
}
