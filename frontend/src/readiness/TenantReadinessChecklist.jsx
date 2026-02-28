import { useState } from "react";
import { Link } from "react-router-dom";
import { useAuth } from "../auth/useAuth.js";
import { useI18n } from "../i18n/useI18n.js";
import { useTenantReadiness } from "./useTenantReadiness.js";

function getReadinessCheckLabel(t, check) {
  return t(
    ["readinessChecklist", "checkLabels", check?.key],
    check?.label || check?.key || ""
  );
}

export default function TenantReadinessChecklist() {
  const { hasPermission } = useAuth();
  const { t } = useI18n();
  const [detailsOpen, setDetailsOpen] = useState(false);
  const {
    loading,
    error,
    readiness,
    missingChecks,
    refresh,
    bootstrapping,
    bootstrapError,
    bootstrapResult,
    runBaselineBootstrap,
  } = useTenantReadiness();

  const canBootstrap = hasPermission("onboarding.company.setup");
  const setupRouteByCheckKey = {
    groupCompanies: "/app/ayarlar/organizasyon-yonetimi",
    legalEntities: "/app/ayarlar/organizasyon-yonetimi",
    fiscalCalendars: "/app/ayarlar/organizasyon-yonetimi",
    fiscalPeriods: "/app/ayarlar/organizasyon-yonetimi",
    books: "/app/ayarlar/hesap-plani-ayarlari",
    openBookPeriods: "/app/ayarlar/organizasyon-yonetimi",
    chartsOfAccounts: "/app/ayarlar/hesap-plani-ayarlari",
    accounts: "/app/ayarlar/hesap-plani-ayarlari",
    shareholders: "/app/ayarlar/organizasyon-yonetimi",
    shareholderCommitmentConfigs: "/app/ayarlar/organizasyon-yonetimi",
  };
  const missingStepLinks = Array.from(
    new Map(
      missingChecks
        .map((check) => {
          const to = setupRouteByCheckKey[check.key];
          if (!to) {
            return null;
          }
          return [
            check.key,
            { key: check.key, to, label: getReadinessCheckLabel(t, check) },
          ];
        })
        .filter(Boolean)
    ).values()
  );

  if (loading) {
    return (
      <section className="rounded-xl border border-slate-200 bg-white p-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="text-sm font-semibold text-slate-700">
            {t("readinessChecklist.title")}
          </h2>
          <p className="text-xs text-slate-500">{t("readinessChecklist.loading")}</p>
        </div>
      </section>
    );
  }

  if (error) {
    return (
      <section className="rounded-xl border border-amber-200 bg-amber-50 p-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="text-sm font-semibold text-amber-900">
            {t("readinessChecklist.title")}
          </h2>
          <button
            type="button"
            onClick={() => refresh()}
            className="rounded-lg border border-amber-300 bg-white px-3 py-1.5 text-xs font-semibold text-amber-900"
          >
            {t("readinessChecklist.retry")}
          </button>
        </div>
        <p className="mt-2 text-sm text-amber-800">{error}</p>
      </section>
    );
  }

  if (!readiness) {
    return null;
  }

  const checks = readiness.checks || [];
  const readyChecksCount = checks.filter((check) => Boolean(check.ready)).length;
  const totalChecksCount = checks.length;

  return (
    <section
      className={`rounded-xl border p-3 ${
        readiness.ready
          ? "border-emerald-200 bg-emerald-50"
          : "border-amber-200 bg-amber-50"
      }`}
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <h2
            className={`text-sm font-semibold ${
              readiness.ready ? "text-emerald-900" : "text-amber-900"
            }`}
          >
            {t("readinessChecklist.title")}
          </h2>
          <span
            className={`rounded-full px-2 py-1 text-xs font-semibold ${
              readiness.ready
                ? "bg-emerald-100 text-emerald-800"
                : "bg-amber-100 text-amber-800"
            }`}
          >
            {readiness.ready
              ? t("readinessChecklist.badges.ready")
              : t("readinessChecklist.badges.setupRequired")}
          </span>
          <span
            className={`rounded-full border px-2 py-1 text-xs font-semibold ${
              readiness.ready
                ? "border-emerald-200 text-emerald-800"
                : "border-amber-300 text-amber-900"
            }`}
          >
            {t("readinessChecklist.summary", {
              ready: readyChecksCount,
              total: totalChecksCount,
            })}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => refresh()}
            className="rounded-lg border border-slate-300 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700"
          >
            {t("readinessChecklist.refresh")}
          </button>
          <button
            type="button"
            onClick={() => setDetailsOpen((prev) => !prev)}
            className="rounded-lg border border-slate-300 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700"
            aria-expanded={detailsOpen}
          >
            {detailsOpen
              ? t("readinessChecklist.hideDetails")
              : t("readinessChecklist.showDetails")}
          </button>
        </div>
      </div>

      {detailsOpen && (
        <div className="mt-3 space-y-3">
          {!readiness.ready && (
            <p className="text-sm text-amber-900">
              {t("readinessChecklist.description")}
            </p>
          )}

          <div className="grid gap-2 md:grid-cols-2">
            {checks.map((check) => (
              <div
                key={check.key}
                className="rounded-lg border border-white/60 bg-white/70 px-3 py-2 text-sm"
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="font-medium text-slate-800">
                    {getReadinessCheckLabel(t, check)}
                  </span>
                  <span
                    className={`rounded px-2 py-0.5 text-xs font-semibold ${
                      check.ready
                        ? "bg-emerald-100 text-emerald-700"
                        : "bg-rose-100 text-rose-700"
                    }`}
                  >
                    {check.ready
                      ? t("readinessChecklist.badges.ok")
                      : t("readinessChecklist.badges.missing")}
                  </span>
                </div>
                <p className="mt-1 text-xs text-slate-600">
                  {t("readinessChecklist.minimum", {
                    count: check.count,
                    minimum: check.minimum,
                  })}
                </p>
              </div>
            ))}
          </div>

          {!readiness.ready && (
            <div className="text-xs text-amber-900">
              {t("readinessChecklist.missing")}{" "}
              <span className="font-semibold">
                {missingChecks.map((check) => getReadinessCheckLabel(t, check)).join(", ")}
              </span>
            </div>
          )}
          {!readiness.ready && missingStepLinks.length > 0 && (
            <div className="rounded-lg border border-white/60 bg-white/70 p-3">
              <p className="text-xs font-semibold text-slate-700">
                {t("readinessChecklist.setupStepsTitle")}
              </p>
              <div className="mt-2 flex flex-wrap gap-2">
                {missingStepLinks.map((item) => (
                  <Link
                    key={item.key}
                    to={item.to}
                    className="rounded border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-semibold text-slate-700"
                  >
                    {item.label}
                  </Link>
                ))}
              </div>
            </div>
          )}

          <div className="flex flex-wrap gap-2 text-xs">
            <Link
              to="/app/ayarlar/sirket-ayarlari"
              className="rounded border border-slate-300 bg-white px-2.5 py-1.5 font-semibold text-slate-700"
            >
              {t("readinessChecklist.links.company")}
            </Link>
            <Link
              to="/app/ayarlar/organizasyon-yonetimi"
              className="rounded border border-slate-300 bg-white px-2.5 py-1.5 font-semibold text-slate-700"
            >
              {t("readinessChecklist.links.org")}
            </Link>
            <Link
              to="/app/ayarlar/hesap-plani-ayarlari"
              className="rounded border border-slate-300 bg-white px-2.5 py-1.5 font-semibold text-slate-700"
            >
              {t("readinessChecklist.links.gl")}
            </Link>
          </div>

          {!readiness.ready && (
            <div className="rounded-lg border border-white/60 bg-white/70 p-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <p className="text-sm font-medium text-slate-800">
                  {t("readinessChecklist.bootstrap.title")}
                </p>
                <button
                  type="button"
                  onClick={() => runBaselineBootstrap()}
                  disabled={!canBootstrap || bootstrapping}
                  className="rounded-lg bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white disabled:opacity-60"
                >
                  {bootstrapping
                    ? t("readinessChecklist.bootstrap.running")
                    : t("readinessChecklist.bootstrap.run")}
                </button>
              </div>
              {!canBootstrap && (
                <p className="mt-1 text-xs text-amber-900">
                  {t("readinessChecklist.bootstrap.missingPermission")}
                </p>
              )}
              {bootstrapError && (
                <p className="mt-1 text-xs text-rose-700">{bootstrapError}</p>
              )}
              {bootstrapResult?.ok && (
                <p className="mt-1 text-xs text-emerald-700">
                  {t("readinessChecklist.bootstrap.completed")}
                </p>
              )}
            </div>
          )}
        </div>
      )}
    </section>
  );
}
