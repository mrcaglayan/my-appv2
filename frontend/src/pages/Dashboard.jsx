import { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  getOpsBankPaymentBatchesHealth,
  getOpsBankReconciliationSummary,
  getOpsJobsHealth,
  getOpsPayrollCloseStatus,
  getOpsPayrollImportHealth,
} from "../api/opsDashboard.js";
import { listExceptionWorkbench } from "../api/exceptionsWorkbench.js";
import { useAuth } from "../auth/useAuth.js";
import { useWorkingContext } from "../context/useWorkingContext.js";
import { useI18n } from "../i18n/useI18n.js";
import { useModuleReadiness } from "../readiness/useModuleReadiness.js";
import { useTenantReadiness } from "../readiness/useTenantReadiness.js";

function toInt(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
}

function formatCount(value) {
  const parsed = toInt(value, 0);
  return parsed.toLocaleString();
}

function formatWindowLabel(window) {
  const dateFrom = String(window?.dateFrom || "").trim();
  const dateTo = String(window?.dateTo || "").trim();
  if (!dateFrom || !dateTo) {
    return "";
  }
  return `${dateFrom} - ${dateTo}`;
}

function resolveScopeParams(workingContext) {
  const params = {};
  const legalEntityId = Number(workingContext?.legalEntityId || 0);
  if (Number.isInteger(legalEntityId) && legalEntityId > 0) {
    params.legalEntityId = legalEntityId;
  }

  const dateFrom = String(workingContext?.dateFrom || "").trim();
  const dateTo = String(workingContext?.dateTo || "").trim();
  if (dateFrom) {
    params.dateFrom = dateFrom;
  }
  if (dateTo) {
    params.dateTo = dateTo;
  }

  if (!params.dateFrom && !params.dateTo) {
    params.days = 30;
  }

  return params;
}

function MetricCard({ title, value, subtitle, to, ctaLabel, locked }) {
  const baseClassName =
    "rounded-xl border p-4 transition-colors bg-white border-slate-200 shadow-sm";
  const lockedClassName = "border-amber-200 bg-amber-50/70";

  const content = (
    <>
      <p className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">
        {title}
      </p>
      <p className="mt-2 text-3xl font-semibold text-slate-900">{value}</p>
      <p className="mt-2 text-sm text-slate-600">{subtitle}</p>
      {locked ? (
        <p className="mt-2 text-xs font-medium text-amber-900">Permission required</p>
      ) : null}
      {!locked && to && ctaLabel ? (
        <span className="mt-3 inline-flex text-xs font-semibold text-cyan-700">
          {ctaLabel}
        </span>
      ) : null}
    </>
  );

  if (locked || !to) {
    return (
      <article className={`${baseClassName} ${locked ? lockedClassName : ""}`}>
        {content}
      </article>
    );
  }

  return (
    <Link
      to={to}
      className={`${baseClassName} hover:border-cyan-300 hover:bg-cyan-50/30`}
    >
      {content}
    </Link>
  );
}

export default function Dashboard() {
  const { t } = useI18n();
  const { hasPermission } = useAuth();
  const { workingContext } = useWorkingContext();
  const {
    readiness: moduleReadinessPayload,
    loading: moduleReadinessLoading,
    error: moduleReadinessError,
  } = useModuleReadiness();
  const {
    missingChecks,
    loading: tenantReadinessLoading,
    error: tenantReadinessError,
  } = useTenantReadiness();

  const canReadOps = hasPermission("ops.dashboard.read");
  const canReadExceptions = hasPermission("ops.exceptions.read");
  const canReadReadiness = hasPermission("org.tree.read");

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [lastRefreshedAt, setLastRefreshedAt] = useState(null);
  const [snapshot, setSnapshot] = useState({
    bankReconciliation: null,
    bankPayments: null,
    payrollImports: null,
    payrollClose: null,
    jobs: null,
    exceptions: null,
  });

  const scopeParams = useMemo(
    () => resolveScopeParams(workingContext),
    [workingContext]
  );

  const load = useCallback(async () => {
    const requestEntries = [];
    if (canReadOps) {
      requestEntries.push(
        {
          key: "bankReconciliation",
          run: () => getOpsBankReconciliationSummary(scopeParams),
        },
        {
          key: "bankPayments",
          run: () => getOpsBankPaymentBatchesHealth(scopeParams),
        },
        {
          key: "payrollImports",
          run: () => getOpsPayrollImportHealth(scopeParams),
        },
        {
          key: "payrollClose",
          run: () => getOpsPayrollCloseStatus(scopeParams),
        },
        {
          key: "jobs",
          run: () => getOpsJobsHealth(scopeParams),
        }
      );
    }

    if (canReadExceptions) {
      requestEntries.push({
        key: "exceptions",
        run: () =>
          listExceptionWorkbench({
            limit: 1,
            offset: 0,
            refresh: 0,
            days: scopeParams.days || 90,
            legalEntityId: scopeParams.legalEntityId,
          }),
      });
    }

    if (requestEntries.length === 0) {
      setError("");
      setLoading(false);
      setSnapshot({
        bankReconciliation: null,
        bankPayments: null,
        payrollImports: null,
        payrollClose: null,
        jobs: null,
        exceptions: null,
      });
      setLastRefreshedAt(new Date().toISOString());
      return;
    }

    setLoading(true);
    setError("");
    try {
      const settled = await Promise.allSettled(
        requestEntries.map((entry) => entry.run())
      );

      const nextSnapshot = {
        bankReconciliation: null,
        bankPayments: null,
        payrollImports: null,
        payrollClose: null,
        jobs: null,
        exceptions: null,
      };

      const failedKeys = [];
      settled.forEach((result, index) => {
        const entry = requestEntries[index];
        if (result.status === "fulfilled") {
          nextSnapshot[entry.key] = result.value || null;
          return;
        }
        failedKeys.push(entry.key);
      });

      setSnapshot(nextSnapshot);
      if (failedKeys.length > 0) {
        setError(
          t(
            "dashboard.widgetsPartialError",
            "Some dashboard widgets could not be loaded: {{widgets}}",
            { widgets: failedKeys.join(", ") }
          )
        );
      }
      setLastRefreshedAt(new Date().toISOString());
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          err?.message ||
          t("dashboard.loadFailed", "Dashboard data could not be loaded.")
      );
    } finally {
      setLoading(false);
    }
  }, [canReadExceptions, canReadOps, scopeParams, t]);

  useEffect(() => {
    load();
  }, [load]);

  const moduleReadinessRows = useMemo(() => {
    const modules = moduleReadinessPayload?.modules || {};
    const legalEntityId = Number(scopeParams.legalEntityId || 0);
    const rows = [];

    for (const [moduleKey, moduleValue] of Object.entries(modules)) {
      for (const row of moduleValue?.byLegalEntity || []) {
        const rowLegalEntityId = Number(row?.legalEntityId || 0);
        if (
          Number.isInteger(legalEntityId) &&
          legalEntityId > 0 &&
          rowLegalEntityId !== legalEntityId
        ) {
          continue;
        }
        rows.push({
          moduleKey,
          ...row,
        });
      }
    }

    return rows;
  }, [moduleReadinessPayload, scopeParams.legalEntityId]);

  const moduleBlockerCount = useMemo(
    () => moduleReadinessRows.filter((row) => !row?.ready).length,
    [moduleReadinessRows]
  );

  const openExceptionsCount = useMemo(() => {
    const byStatus = snapshot.exceptions?.summary?.by_status || {};
    return (
      toInt(byStatus.OPEN, 0) +
      toInt(byStatus.IN_REVIEW, 0) +
      toInt(byStatus.ASSIGNED, 0)
    );
  }, [snapshot.exceptions]);

  const toPostCount = useMemo(() => {
    const bankPending = toInt(snapshot.bankPayments?.sla?.pending_export_batches, 0);
    const payrollPreviewed = toInt(snapshot.payrollImports?.sla?.previewed_jobs, 0);
    const payrollApplying = toInt(snapshot.payrollImports?.sla?.applying_jobs, 0);
    return bankPending + payrollPreviewed + payrollApplying;
  }, [snapshot.bankPayments, snapshot.payrollImports]);

  const toSettleCount = useMemo(() => {
    const unmatched = toInt(snapshot.bankReconciliation?.sla?.unmatched_open_total, 0);
    const awaitingAck = toInt(snapshot.bankPayments?.sla?.awaiting_ack_batches, 0);
    return unmatched + awaitingAck;
  }, [snapshot.bankPayments, snapshot.bankReconciliation]);

  const periodCloseBlockerCount = useMemo(() => {
    const payrollFailedChecks = toInt(
      snapshot.payrollClose?.checks?.failed_checks_open_periods,
      0
    );
    const tenantMissing = Array.isArray(missingChecks) ? missingChecks.length : 0;
    return payrollFailedChecks + tenantMissing + moduleBlockerCount;
  }, [missingChecks, moduleBlockerCount, snapshot.payrollClose]);

  const windowLabel = useMemo(() => {
    const firstWindow =
      snapshot.bankReconciliation?.window ||
      snapshot.bankPayments?.window ||
      snapshot.payrollImports?.window ||
      snapshot.payrollClose?.window ||
      snapshot.jobs?.window ||
      null;
    return formatWindowLabel(firstWindow);
  }, [snapshot]);

  const quickLinks = useMemo(
    () => [
      {
        to: "/app/ayarlar/exception-workbench",
        title: t("dashboard.links.exceptions", "Exception Workbench"),
        hint: `${formatCount(openExceptionsCount)} ${t("dashboard.openItems", "open items")}`,
        enabled: canReadExceptions,
      },
      {
        to: "/app/banka-mutabakat",
        title: t("dashboard.links.bankReconciliation", "Bank Reconciliation"),
        hint: `${formatCount(
          snapshot.bankReconciliation?.sla?.unmatched_open_total || 0
        )} ${t("dashboard.unmatchedLines", "unmatched lines")}`,
        enabled: canReadOps,
      },
      {
        to: "/app/odeme-batchleri",
        title: t("dashboard.links.paymentBatches", "Payment Batches"),
        hint: `${formatCount(
          snapshot.bankPayments?.sla?.pending_export_batches || 0
        )} ${t("dashboard.pendingExport", "pending export")}`,
        enabled: canReadOps,
      },
      {
        to: "/app/payroll-close-controls",
        title: t("dashboard.links.payrollClose", "Payroll Close Controls"),
        hint: `${formatCount(
          snapshot.payrollClose?.checks?.failed_checks_open_periods || 0
        )} ${t("dashboard.failedChecks", "failed checks")}`,
        enabled: canReadOps,
      },
      {
        to: "/app/ayarlar/operasyon-dashboard",
        title: t("dashboard.links.opsDetail", "Ops Dashboard Detail"),
        hint: `${formatCount(
          (snapshot.jobs?.sla?.queued_due_now || 0) +
            (snapshot.jobs?.sla?.retries_due_now || 0)
        )} ${t("dashboard.jobsDueNow", "jobs due now")}`,
        enabled: canReadOps,
      },
    ],
    [canReadExceptions, canReadOps, openExceptionsCount, snapshot, t]
  );

  const readinessInlineError = moduleReadinessError || tenantReadinessError || "";

  return (
    <section className="space-y-5">
      <header className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold text-slate-900">
              {t("dashboard.financeConsoleTitle", "Finance Console")}
            </h1>
            <p className="mt-1 text-sm text-slate-600">
              {t(
                "dashboard.financeConsoleSubtitle",
                "Today's operational queues and blockers in one place."
              )}
            </p>
            {windowLabel ? (
              <p className="mt-2 text-xs font-medium text-slate-500">
                {t("dashboard.window", "Window")}: {windowLabel}
              </p>
            ) : null}
          </div>
          <div className="flex items-center gap-2">
            {lastRefreshedAt ? (
              <span className="text-xs text-slate-500">
                {t("dashboard.lastUpdated", "Last updated")}:{" "}
                {new Date(lastRefreshedAt).toLocaleTimeString()}
              </span>
            ) : null}
            <button
              type="button"
              onClick={load}
              disabled={loading}
              className="rounded-lg border border-slate-300 bg-white px-3 py-1.5 text-sm font-semibold text-slate-700 hover:bg-slate-50 disabled:opacity-60"
            >
              {loading
                ? t("dashboard.refreshing", "Refreshing...")
                : t("dashboard.refresh", "Refresh")}
            </button>
          </div>
        </div>
        {error ? (
          <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
            {error}
          </div>
        ) : null}
      </header>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          title={t("dashboard.cards.toPost", "To Post")}
          value={formatCount(toPostCount)}
          subtitle={t(
            "dashboard.cards.toPostHint",
            "Batches and payroll imports waiting posting actions."
          )}
          to="/app/odeme-batchleri"
          ctaLabel={t("dashboard.openQueue", "Open queue")}
          locked={!canReadOps}
        />
        <MetricCard
          title={t("dashboard.cards.toSettle", "To Settle")}
          value={formatCount(toSettleCount)}
          subtitle={t(
            "dashboard.cards.toSettleHint",
            "Unmatched statements and exported batches awaiting bank ack."
          )}
          to="/app/banka-mutabakat"
          ctaLabel={t("dashboard.openQueue", "Open queue")}
          locked={!canReadOps}
        />
        <MetricCard
          title={t("dashboard.cards.exceptions", "Exceptions")}
          value={formatCount(openExceptionsCount)}
          subtitle={t(
            "dashboard.cards.exceptionsHint",
            "Open, in-review, and assigned exception workload."
          )}
          to="/app/ayarlar/exception-workbench"
          ctaLabel={t("dashboard.openQueue", "Open queue")}
          locked={!canReadExceptions}
        />
        <MetricCard
          title={t("dashboard.cards.periodCloseBlockers", "Period Close Blockers")}
          value={formatCount(periodCloseBlockerCount)}
          subtitle={t(
            "dashboard.cards.periodCloseBlockersHint",
            "Failed close checks plus tenant/module readiness blockers."
          )}
          to="/app/payroll-close-controls"
          ctaLabel={t("dashboard.openQueue", "Open queue")}
          locked={!canReadOps && !canReadReadiness}
        />
      </div>

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <h2 className="text-sm font-semibold uppercase tracking-[0.12em] text-slate-600">
          {t("dashboard.recentActivity", "Recent Activity Links")}
        </h2>
        <div className="mt-3 grid gap-2 md:grid-cols-2 xl:grid-cols-3">
          {quickLinks.map((link) =>
            link.enabled ? (
              <Link
                key={link.to}
                to={link.to}
                className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 hover:border-cyan-300 hover:bg-cyan-50/30"
              >
                <p className="text-sm font-semibold text-slate-900">{link.title}</p>
                <p className="mt-1 text-xs text-slate-600">{link.hint}</p>
              </Link>
            ) : (
              <article
                key={link.to}
                className="rounded-lg border border-amber-200 bg-amber-50/70 px-3 py-2"
              >
                <p className="text-sm font-semibold text-amber-900">{link.title}</p>
                <p className="mt-1 text-xs text-amber-800">
                  {t("dashboard.permissionRequired", "Permission required")}
                </p>
              </article>
            )
          )}
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <h2 className="text-sm font-semibold uppercase tracking-[0.12em] text-slate-600">
          {t("dashboard.readiness", "Readiness")}
        </h2>
        <div className="mt-3 grid gap-3 md:grid-cols-3">
          <article className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
            <p className="text-xs font-semibold uppercase tracking-[0.08em] text-slate-500">
              {t("dashboard.readinessTenantMissing", "Tenant Missing Checks")}
            </p>
            <p className="mt-2 text-2xl font-semibold text-slate-900">
              {formatCount(Array.isArray(missingChecks) ? missingChecks.length : 0)}
            </p>
          </article>
          <article className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
            <p className="text-xs font-semibold uppercase tracking-[0.08em] text-slate-500">
              {t("dashboard.readinessModuleBlockers", "Module Blockers")}
            </p>
            <p className="mt-2 text-2xl font-semibold text-slate-900">
              {formatCount(moduleBlockerCount)}
            </p>
          </article>
          <article className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
            <p className="text-xs font-semibold uppercase tracking-[0.08em] text-slate-500">
              {t("dashboard.readinessMonitoredRows", "Monitored Rows")}
            </p>
            <p className="mt-2 text-2xl font-semibold text-slate-900">
              {formatCount(moduleReadinessRows.length)}
            </p>
          </article>
        </div>
        {moduleReadinessLoading || tenantReadinessLoading ? (
          <p className="mt-3 text-sm text-slate-600">
            {t("dashboard.readinessLoading", "Refreshing readiness data...")}
          </p>
        ) : null}
        {readinessInlineError ? (
          <p className="mt-3 text-sm text-amber-700">{readinessInlineError}</p>
        ) : null}
      </section>
    </section>
  );
}
