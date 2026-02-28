import { useEffect, useMemo, useState } from "react";
import {
  getConsolidatedBalanceSheet,
  getConsolidatedIncomeStatement,
  listConsolidationRuns,
  listConsolidationAdjustments,
  listConsolidationEliminations,
  postConsolidationAdjustment,
  postConsolidationElimination,
} from "../api/glAdmin.js";
import { useAuth } from "../auth/useAuth.js";
import { useI18n } from "../i18n/useI18n.js";

function toInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function formatAmount(value) {
  return Number(value || 0).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function formatPeriodNo(value) {
  return String(value || "").padStart(2, "0");
}

export default function ConsolidationReportsPage() {
  const { hasPermission } = useAuth();
  const { t } = useI18n();
  const canReadRun = hasPermission("consolidation.run.read");
  const canReadBalanceSheet = hasPermission("consolidation.report.balance_sheet.read");
  const canReadIncomeStatement = hasPermission(
    "consolidation.report.income_statement.read"
  );
  const canPostAdjustment = hasPermission("consolidation.adjustment.post");
  const canPostElimination = hasPermission("consolidation.elimination.post");

  const [saving, setSaving] = useState("");
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [loadingRuns, setLoadingRuns] = useState(false);
  const [runs, setRuns] = useState([]);
  const [form, setForm] = useState({
    runId: "",
    includeDraft: false,
    includeZero: false,
    rateType: "CLOSING",
  });
  const [balanceSheetReport, setBalanceSheetReport] = useState(null);
  const [incomeStatementReport, setIncomeStatementReport] = useState(null);
  const [adjustments, setAdjustments] = useState([]);
  const [eliminations, setEliminations] = useState([]);

  const selectedRun = useMemo(() => {
    const selectedId = toInt(form.runId);
    if (!selectedId) {
      return null;
    }
    return runs.find((row) => Number(row.id) === selectedId) || null;
  }, [form.runId, runs]);

  async function loadRuns() {
    if (!canReadRun) {
      return;
    }

    setLoadingRuns(true);
    setError("");
    try {
      const res = await listConsolidationRuns();
      const rows = res?.rows || [];
      setRuns(rows);
      setForm((prev) => {
        const currentRunId = toInt(prev.runId);
        if (currentRunId && rows.some((row) => Number(row.id) === currentRunId)) {
          return prev;
        }
        return { ...prev, runId: String(rows[0]?.id || prev.runId || "") };
      });
    } catch (err) {
      setError(
        err?.response?.data?.message || t("consolidationReports.loadRunsFailed")
      );
    } finally {
      setLoadingRuns(false);
    }
  }

  useEffect(() => {
    loadRuns();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadRun]);

  function resolveRunId() {
    const runId = toInt(form.runId);
    if (!runId) {
      setError(t("consolidationReports.runRequired"));
      return null;
    }
    return runId;
  }

  async function onLoadBalanceSheet() {
    if (!canReadBalanceSheet) {
      setError(t("consolidationReports.missingPermissionBs"));
      return;
    }

    const runId = resolveRunId();
    if (!runId) {
      return;
    }

    setSaving("balanceSheet");
    setError("");
    setMessage("");
    try {
      const res = await getConsolidatedBalanceSheet(runId, {
        includeDraft: form.includeDraft,
        includeZero: form.includeZero,
        rateType: form.rateType,
      });
      setBalanceSheetReport(res || null);
      setMessage(t("consolidationReports.loadBsSuccess"));
    } catch (err) {
      setError(err?.response?.data?.message || t("consolidationReports.loadBsFailed"));
    } finally {
      setSaving("");
    }
  }

  async function onLoadIncomeStatement() {
    if (!canReadIncomeStatement) {
      setError(t("consolidationReports.missingPermissionIs"));
      return;
    }

    const runId = resolveRunId();
    if (!runId) {
      return;
    }

    setSaving("incomeStatement");
    setError("");
    setMessage("");
    try {
      const res = await getConsolidatedIncomeStatement(runId, {
        includeDraft: form.includeDraft,
        includeZero: form.includeZero,
        rateType: form.rateType,
      });
      setIncomeStatementReport(res || null);
      setMessage(t("consolidationReports.loadIsSuccess"));
    } catch (err) {
      setError(err?.response?.data?.message || t("consolidationReports.loadIsFailed"));
    } finally {
      setSaving("");
    }
  }

  async function onLoadDraftWorklist() {
    if (!canReadRun) {
      setError(t("consolidationReports.missingPermissionRun"));
      return;
    }

    const runId = resolveRunId();
    if (!runId) {
      return;
    }

    setSaving("worklist");
    setError("");
    setMessage("");
    try {
      const [adjustmentRes, eliminationRes] = await Promise.all([
        listConsolidationAdjustments(runId, { status: "ALL" }),
        listConsolidationEliminations(runId, { status: "ALL", includeLines: false }),
      ]);
      setAdjustments(adjustmentRes?.rows || []);
      setEliminations(eliminationRes?.rows || []);
      setMessage(t("consolidationReports.loadWorklistSuccess"));
    } catch (err) {
      setError(
        err?.response?.data?.message || t("consolidationReports.loadWorklistFailed")
      );
    } finally {
      setSaving("");
    }
  }

  async function onPostAdjustment(adjustmentId) {
    if (!canPostAdjustment) {
      setError(t("consolidationReports.missingPermissionAdj"));
      return;
    }
    const runId = resolveRunId();
    if (!runId) {
      return;
    }

    setSaving(`postAdjustment:${adjustmentId}`);
    setError("");
    setMessage("");
    try {
      await postConsolidationAdjustment(runId, adjustmentId);
      setMessage(t("consolidationReports.postAdjSuccess", { id: adjustmentId }));
      await onLoadDraftWorklist();
    } catch (err) {
      setError(err?.response?.data?.message || t("consolidationReports.postAdjFailed"));
    } finally {
      setSaving("");
    }
  }

  async function onPostElimination(eliminationEntryId) {
    if (!canPostElimination) {
      setError(t("consolidationReports.missingPermissionElim"));
      return;
    }
    const runId = resolveRunId();
    if (!runId) {
      return;
    }

    setSaving(`postElimination:${eliminationEntryId}`);
    setError("");
    setMessage("");
    try {
      await postConsolidationElimination(runId, eliminationEntryId);
      setMessage(t("consolidationReports.postElimSuccess", { id: eliminationEntryId }));
      await onLoadDraftWorklist();
    } catch (err) {
      setError(err?.response?.data?.message || t("consolidationReports.postElimFailed"));
    } finally {
      setSaving("");
    }
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">
          {t("consolidationReports.title")}
        </h1>
        <p className="mt-1 text-sm text-slate-600">{t("consolidationReports.subtitle")}</p>
      </div>

      {error && (
        <div className="rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
          {error}
        </div>
      )}
      {message && (
        <div className="rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
          {message}
        </div>
      )}

      <section className="space-y-3 rounded-xl border border-slate-200 bg-white p-4">
        <div className="grid gap-2 md:grid-cols-2">
          <div className="space-y-1">
            <div className="text-xs font-medium uppercase tracking-wide text-slate-500">
              {t("consolidationReports.runLabel")}
            </div>
            {runs.length > 0 ? (
              <select
                value={form.runId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, runId: event.target.value }))
                }
                className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
                disabled={loadingRuns}
                required
              >
                <option value="">{t("consolidationReports.runPlaceholder")}</option>
                {runs.map((row) => (
                  <option key={row.id} value={row.id}>
                    #{row.id} | {row.consolidation_group_code} | {row.fiscal_year}-P
                    {formatPeriodNo(row.period_no)} | {row.status}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min={1}
                value={form.runId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, runId: event.target.value }))
                }
                className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("consolidationReports.runIdPlaceholder")}
                required
              />
            )}
          </div>

          <div className="space-y-1">
            <div className="text-xs font-medium uppercase tracking-wide text-slate-500">
              {t("consolidationReports.rateTypeLabel")}
            </div>
            <select
              value={form.rateType}
              onChange={(event) =>
                setForm((prev) => ({ ...prev, rateType: event.target.value }))
              }
              className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
            >
              {["CLOSING", "SPOT", "AVERAGE"].map((rateType) => (
                <option key={rateType} value={rateType}>
                  {rateType}
                </option>
              ))}
            </select>
          </div>

          <label className="inline-flex items-center gap-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={form.includeDraft}
              onChange={(event) =>
                setForm((prev) => ({ ...prev, includeDraft: event.target.checked }))
              }
            />
            {t("consolidationReports.includeDraft")}
          </label>
          <label className="inline-flex items-center gap-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={form.includeZero}
              onChange={(event) =>
                setForm((prev) => ({ ...prev, includeZero: event.target.checked }))
              }
            />
            {t("consolidationReports.includeZero")}
          </label>

          <div className="flex flex-wrap items-center gap-2 md:col-span-2">
            <button
              type="button"
              onClick={onLoadBalanceSheet}
              disabled={saving === "balanceSheet" || !canReadBalanceSheet}
              className="rounded bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving === "balanceSheet"
                ? t("consolidationReports.loadBsLoading")
                : t("consolidationReports.loadBsButton")}
            </button>
            <button
              type="button"
              onClick={onLoadIncomeStatement}
              disabled={saving === "incomeStatement" || !canReadIncomeStatement}
              className="rounded bg-cyan-700 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving === "incomeStatement"
                ? t("consolidationReports.loadIsLoading")
                : t("consolidationReports.loadIsButton")}
            </button>
            <button
              type="button"
              onClick={onLoadDraftWorklist}
              disabled={saving === "worklist" || !canReadRun}
              className="rounded border border-slate-300 px-3 py-2 text-sm font-semibold text-slate-700 disabled:opacity-60"
            >
              {saving === "worklist"
                ? t("consolidationReports.loadWorklistLoading")
                : t("consolidationReports.loadWorklistButton")}
            </button>
            <button
              type="button"
              onClick={loadRuns}
              disabled={loadingRuns || !canReadRun}
              className="rounded border border-slate-300 px-3 py-2 text-sm font-semibold text-slate-700 disabled:opacity-60"
            >
              {loadingRuns
                ? t("consolidationReports.refreshRunsLoading")
                : t("consolidationReports.refreshRunsButton")}
            </button>
          </div>
        </div>

        {selectedRun && (
          <div className="rounded border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700">
            {t("consolidationReports.selectedRunSummary", {
              id: selectedRun.id,
              groupCode: selectedRun.consolidation_group_code || "-",
              groupName: selectedRun.consolidation_group_name || "-",
              fiscalYear: selectedRun.fiscal_year || "-",
              periodNo: formatPeriodNo(selectedRun.period_no),
              periodName: selectedRun.period_name || "-",
              status: selectedRun.status || "-",
            })}
          </div>
        )}

        {balanceSheetReport?.totals && (
          <div className="rounded border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700">
            {t("consolidationReports.bsTotals", {
              assets: formatAmount(balanceSheetReport.totals.assetsTotal),
              liabilities: formatAmount(balanceSheetReport.totals.liabilitiesTotal),
              equity: formatAmount(balanceSheetReport.totals.equityTotal),
              earnings: formatAmount(balanceSheetReport.totals.currentPeriodEarnings),
              delta: formatAmount(balanceSheetReport.totals.equationDelta),
            })}
          </div>
        )}
        {incomeStatementReport?.totals && (
          <div className="rounded border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700">
            {t("consolidationReports.isTotals", {
              revenue: formatAmount(incomeStatementReport.totals.revenueTotal),
              expense: formatAmount(incomeStatementReport.totals.expenseTotal),
              net: formatAmount(incomeStatementReport.totals.netIncome),
            })}
          </div>
        )}

        <div className="grid gap-4 xl:grid-cols-2">
          <div className="overflow-x-auto rounded border border-slate-200">
            <table className="min-w-full text-xs">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-2 py-2">{t("consolidationReports.tables.account")}</th>
                  <th className="px-2 py-2">{t("consolidationReports.tables.type")}</th>
                  <th className="px-2 py-2">{t("consolidationReports.tables.normalized")}</th>
                </tr>
              </thead>
              <tbody>
                {(balanceSheetReport?.rows || []).slice(0, 10).map((row) => (
                  <tr key={`bs-${row.accountId}`} className="border-t border-slate-100">
                    <td className="px-2 py-2">
                      {row.accountCode} - {row.accountName}
                    </td>
                    <td className="px-2 py-2">{row.accountType}</td>
                    <td className="px-2 py-2">
                      {formatAmount(row.normalizedFinalBalance || 0)}
                    </td>
                  </tr>
                ))}
                {(balanceSheetReport?.rows || []).length === 0 && (
                  <tr>
                    <td colSpan={3} className="px-2 py-3 text-slate-500">
                      {t("consolidationReports.tables.bsEmpty")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          <div className="overflow-x-auto rounded border border-slate-200">
            <table className="min-w-full text-xs">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-2 py-2">{t("consolidationReports.tables.account")}</th>
                  <th className="px-2 py-2">{t("consolidationReports.tables.type")}</th>
                  <th className="px-2 py-2">{t("consolidationReports.tables.normalized")}</th>
                </tr>
              </thead>
              <tbody>
                {(incomeStatementReport?.rows || []).slice(0, 10).map((row) => (
                  <tr key={`is-${row.accountId}`} className="border-t border-slate-100">
                    <td className="px-2 py-2">
                      {row.accountCode} - {row.accountName}
                    </td>
                    <td className="px-2 py-2">{row.accountType}</td>
                    <td className="px-2 py-2">
                      {formatAmount(row.normalizedFinalBalance || 0)}
                    </td>
                  </tr>
                ))}
                {(incomeStatementReport?.rows || []).length === 0 && (
                  <tr>
                    <td colSpan={3} className="px-2 py-3 text-slate-500">
                      {t("consolidationReports.tables.isEmpty")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <div className="grid gap-4 xl:grid-cols-2">
        <section className="space-y-2 rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="text-sm font-semibold text-slate-700">
            {t("consolidationReports.tables.adjustmentsTitle")}
          </h2>
          <div className="overflow-x-auto rounded border border-slate-200">
            <table className="min-w-full text-xs">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-2 py-2">{t("consolidationReports.tables.id")}</th>
                  <th className="px-2 py-2">{t("consolidationReports.tables.status")}</th>
                  <th className="px-2 py-2">{t("consolidationReports.tables.account")}</th>
                  <th className="px-2 py-2">{t("consolidationReports.tables.debit")}</th>
                  <th className="px-2 py-2">{t("consolidationReports.tables.credit")}</th>
                  <th className="px-2 py-2">{t("consolidationReports.tables.action")}</th>
                </tr>
              </thead>
              <tbody>
                {adjustments.map((row) => (
                  <tr key={row.id} className="border-t border-slate-100">
                    <td className="px-2 py-2">#{row.id}</td>
                    <td className="px-2 py-2">{row.status}</td>
                    <td className="px-2 py-2">
                      {row.accountCode} - {row.accountName}
                    </td>
                    <td className="px-2 py-2">{formatAmount(row.debitAmount)}</td>
                    <td className="px-2 py-2">{formatAmount(row.creditAmount)}</td>
                    <td className="px-2 py-2">
                      {row.status === "DRAFT" ? (
                        <button
                          type="button"
                          onClick={() => onPostAdjustment(row.id)}
                          disabled={
                            saving === `postAdjustment:${row.id}` || !canPostAdjustment
                          }
                          className="rounded bg-amber-600 px-2 py-1 text-[11px] font-semibold text-white disabled:opacity-60"
                        >
                          {saving === `postAdjustment:${row.id}`
                            ? t("consolidationReports.tables.posting")
                            : t("consolidationReports.tables.post")}
                        </button>
                      ) : (
                        <span className="text-slate-500">
                          {t("consolidationReports.tables.none")}
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
                {adjustments.length === 0 && (
                  <tr>
                    <td colSpan={6} className="px-2 py-3 text-slate-500">
                      {t("consolidationReports.tables.adjustmentsEmpty")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="space-y-2 rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="text-sm font-semibold text-slate-700">
            {t("consolidationReports.tables.eliminationsTitle")}
          </h2>
          <div className="overflow-x-auto rounded border border-slate-200">
            <table className="min-w-full text-xs">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-2 py-2">{t("consolidationReports.tables.id")}</th>
                  <th className="px-2 py-2">{t("consolidationReports.tables.status")}</th>
                  <th className="px-2 py-2">
                    {t("consolidationReports.tables.description")}
                  </th>
                  <th className="px-2 py-2">{t("consolidationReports.tables.lines")}</th>
                  <th className="px-2 py-2">{t("consolidationReports.tables.action")}</th>
                </tr>
              </thead>
              <tbody>
                {eliminations.map((row) => (
                  <tr key={row.id} className="border-t border-slate-100">
                    <td className="px-2 py-2">#{row.id}</td>
                    <td className="px-2 py-2">{row.status}</td>
                    <td className="px-2 py-2">{row.description}</td>
                    <td className="px-2 py-2">{Number(row.lineCount || 0)}</td>
                    <td className="px-2 py-2">
                      {row.status === "DRAFT" ? (
                        <button
                          type="button"
                          onClick={() => onPostElimination(row.id)}
                          disabled={
                            saving === `postElimination:${row.id}` || !canPostElimination
                          }
                          className="rounded bg-amber-600 px-2 py-1 text-[11px] font-semibold text-white disabled:opacity-60"
                        >
                          {saving === `postElimination:${row.id}`
                            ? t("consolidationReports.tables.posting")
                            : t("consolidationReports.tables.post")}
                        </button>
                      ) : (
                        <span className="text-slate-500">
                          {t("consolidationReports.tables.none")}
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
                {eliminations.length === 0 && (
                  <tr>
                    <td colSpan={5} className="px-2 py-3 text-slate-500">
                      {t("consolidationReports.tables.eliminationsEmpty")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
  );
}
