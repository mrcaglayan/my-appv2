import { useEffect, useState } from "react";
import { runIntercompanyReconciliation } from "../api/glAdmin.js";
import {
  listFiscalCalendars,
  listFiscalPeriods,
  listLegalEntities,
} from "../api/orgAdmin.js";
import { useAuth } from "../auth/useAuth.js";
import { useI18n } from "../i18n/useI18n.js";

function toInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function toOptionalInt(value) {
  if (value === undefined || value === null || value === "") return null;
  return toInt(value);
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

export default function IntercompanyReconciliationPage() {
  const { hasPermission } = useAuth();
  const { t } = useI18n();
  const canRunIntercompanyReconcile = hasPermission("intercompany.reconcile.run");
  const canReadOrgTree = hasPermission("org.tree.read");
  const canReadFiscalCalendars = hasPermission("org.fiscal_calendar.read");
  const canReadFiscalPeriods = hasPermission("org.fiscal_period.read");

  const [loadingLookups, setLoadingLookups] = useState(false);
  const [loadingPeriods, setLoadingPeriods] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [legalEntities, setLegalEntities] = useState([]);
  const [calendars, setCalendars] = useState([]);
  const [periods, setPeriods] = useState([]);
  const [form, setForm] = useState({
    calendarId: "",
    fiscalPeriodId: "",
    fromLegalEntityId: "",
    toLegalEntityId: "",
    tolerance: "0.01",
    includeMatched: false,
    includeAccountBreakdown: true,
  });
  const [summary, setSummary] = useState(null);
  const [rows, setRows] = useState([]);

  useEffect(() => {
    let cancelled = false;

    async function loadLookups() {
      setLoadingLookups(true);
      setError("");
      try {
        const [entityRes, calendarRes] = await Promise.all([
          canReadOrgTree ? listLegalEntities() : Promise.resolve({ rows: [] }),
          canReadFiscalCalendars ? listFiscalCalendars() : Promise.resolve({ rows: [] }),
        ]);

        if (cancelled) {
          return;
        }

        const nextEntities = entityRes?.rows || [];
        const nextCalendars = calendarRes?.rows || [];

        setLegalEntities(nextEntities);
        setCalendars(nextCalendars);

        setForm((prev) => {
          const hasCalendar = nextCalendars.some(
            (row) => Number(row.id) === Number(prev.calendarId)
          );
          return {
            ...prev,
            calendarId: hasCalendar
              ? prev.calendarId
              : String(nextCalendars[0]?.id || prev.calendarId || ""),
          };
        });
      } catch (err) {
        if (!cancelled) {
          setError(err?.response?.data?.message || t("intercompanyReconciliation.runFailed"));
        }
      } finally {
        if (!cancelled) {
          setLoadingLookups(false);
        }
      }
    }

    loadLookups();
    return () => {
      cancelled = true;
    };
  }, [canReadOrgTree, canReadFiscalCalendars, t]);

  useEffect(() => {
    let cancelled = false;

    async function loadPeriodsByCalendar() {
      const calendarId = toInt(form.calendarId);
      if (!canReadFiscalPeriods || !calendarId) {
        setPeriods([]);
        return;
      }

      setLoadingPeriods(true);
      try {
        const res = await listFiscalPeriods(calendarId);
        if (cancelled) {
          return;
        }
        const nextPeriods = res?.rows || [];
        setPeriods(nextPeriods);
        setForm((prev) => {
          const hasPeriod = nextPeriods.some(
            (row) => Number(row.id) === Number(prev.fiscalPeriodId)
          );
          return {
            ...prev,
            fiscalPeriodId: hasPeriod
              ? prev.fiscalPeriodId
              : String(nextPeriods[0]?.id || prev.fiscalPeriodId || ""),
          };
        });
      } catch (err) {
        if (!cancelled) {
          setError(err?.response?.data?.message || t("intercompanyReconciliation.runFailed"));
        }
      } finally {
        if (!cancelled) {
          setLoadingPeriods(false);
        }
      }
    }

    loadPeriodsByCalendar();
    return () => {
      cancelled = true;
    };
  }, [canReadFiscalPeriods, form.calendarId, t]);

  async function onSubmit(event) {
    event.preventDefault();
    if (!canRunIntercompanyReconcile) {
      setError(t("intercompanyReconciliation.missingPermission"));
      return;
    }

    const fiscalPeriodId = toInt(form.fiscalPeriodId);
    if (!fiscalPeriodId) {
      setError(t("intercompanyReconciliation.fiscalPeriodRequired"));
      return;
    }

    const fromLegalEntityId = toOptionalInt(form.fromLegalEntityId);
    const toLegalEntityId = toOptionalInt(form.toLegalEntityId);
    const tolerance = Number(form.tolerance);
    if (!Number.isFinite(tolerance) || tolerance < 0) {
      setError(t("intercompanyReconciliation.toleranceInvalid"));
      return;
    }

    setSaving(true);
    setError("");
    setMessage("");
    try {
      const res = await runIntercompanyReconciliation({
        fiscalPeriodId,
        fromLegalEntityId: fromLegalEntityId || undefined,
        toLegalEntityId: toLegalEntityId || undefined,
        tolerance,
        includeMatched: form.includeMatched,
        includeAccountBreakdown: form.includeAccountBreakdown,
      });
      setSummary(res?.summary || null);
      setRows(res?.rows || []);
      setMessage(
        t("intercompanyReconciliation.runSuccess", {
          count: Number(res?.summary?.pairCount || 0),
        })
      );
    } catch (err) {
      setError(err?.response?.data?.message || t("intercompanyReconciliation.runFailed"));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">
          {t("intercompanyReconciliation.title")}
        </h1>
        <p className="mt-1 text-sm text-slate-600">
          {t("intercompanyReconciliation.subtitle")}
        </p>
      </div>

      {(loadingLookups || loadingPeriods) && (
        <div className="text-xs text-slate-500">
          {t("intercompanyReconciliation.loadingLookups")}
        </div>
      )}

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
        <form onSubmit={onSubmit} className="grid gap-2 md:grid-cols-2">
          <div className="space-y-1">
            <div className="text-xs font-medium uppercase tracking-wide text-slate-500">
              {t("intercompanyReconciliation.calendarLabel")}
            </div>
            {calendars.length > 0 ? (
              <select
                value={form.calendarId}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    calendarId: event.target.value,
                    fiscalPeriodId: "",
                  }))
                }
                className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
              >
                <option value="">{t("intercompanyReconciliation.calendarPlaceholder")}</option>
                {calendars.map((calendar) => (
                  <option key={calendar.id} value={calendar.id}>
                    {calendar.code} - {calendar.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min={1}
                value={form.calendarId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, calendarId: event.target.value }))
                }
                className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("intercompanyReconciliation.calendarIdPlaceholder")}
              />
            )}
          </div>

          <div className="space-y-1">
            <div className="text-xs font-medium uppercase tracking-wide text-slate-500">
              {t("intercompanyReconciliation.periodLabel")}
            </div>
            {periods.length > 0 ? (
              <select
                value={form.fiscalPeriodId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, fiscalPeriodId: event.target.value }))
                }
                className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
                required
              >
                <option value="">{t("intercompanyReconciliation.periodPlaceholder")}</option>
                {periods.map((period) => (
                  <option key={period.id} value={period.id}>
                    {period.fiscal_year}-P{formatPeriodNo(period.period_no)} ({period.period_name})
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min={1}
                value={form.fiscalPeriodId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, fiscalPeriodId: event.target.value }))
                }
                className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("intercompanyReconciliation.periodIdPlaceholder")}
                required
              />
            )}
          </div>

          <div className="space-y-1">
            <div className="text-xs font-medium uppercase tracking-wide text-slate-500">
              {t("intercompanyReconciliation.fromEntityLabel")}
            </div>
            {legalEntities.length > 0 ? (
              <select
                value={form.fromLegalEntityId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, fromLegalEntityId: event.target.value }))
                }
                className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
              >
                <option value="">{t("intercompanyReconciliation.fromEntityPlaceholder")}</option>
                {legalEntities.map((entity) => (
                  <option key={entity.id} value={entity.id}>
                    {entity.code} - {entity.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min={1}
                value={form.fromLegalEntityId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, fromLegalEntityId: event.target.value }))
                }
                className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("intercompanyReconciliation.fromEntityIdPlaceholder")}
              />
            )}
          </div>

          <div className="space-y-1">
            <div className="text-xs font-medium uppercase tracking-wide text-slate-500">
              {t("intercompanyReconciliation.toEntityLabel")}
            </div>
            {legalEntities.length > 0 ? (
              <select
                value={form.toLegalEntityId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, toLegalEntityId: event.target.value }))
                }
                className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
              >
                <option value="">{t("intercompanyReconciliation.toEntityPlaceholder")}</option>
                {legalEntities.map((entity) => (
                  <option key={entity.id} value={entity.id}>
                    {entity.code} - {entity.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min={1}
                value={form.toLegalEntityId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, toLegalEntityId: event.target.value }))
                }
                className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("intercompanyReconciliation.toEntityIdPlaceholder")}
              />
            )}
          </div>

          <div className="space-y-1">
            <div className="text-xs font-medium uppercase tracking-wide text-slate-500">
              {t("intercompanyReconciliation.toleranceLabel")}
            </div>
            <input
              value={form.tolerance}
              onChange={(event) =>
                setForm((prev) => ({ ...prev, tolerance: event.target.value }))
              }
              className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
              placeholder={t("intercompanyReconciliation.toleranceLabel")}
            />
          </div>

          <label className="inline-flex items-center gap-2 self-end pb-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={form.includeMatched}
              onChange={(event) =>
                setForm((prev) => ({ ...prev, includeMatched: event.target.checked }))
              }
            />
            {t("intercompanyReconciliation.includeMatched")}
          </label>
          <label className="inline-flex items-center gap-2 self-end pb-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={form.includeAccountBreakdown}
              onChange={(event) =>
                setForm((prev) => ({
                  ...prev,
                  includeAccountBreakdown: event.target.checked,
                }))
              }
            />
            {t("intercompanyReconciliation.includeAccountBreakdown")}
          </label>

          <button
            type="submit"
            disabled={saving || !canRunIntercompanyReconcile}
            className="rounded bg-cyan-700 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60 md:col-span-2"
          >
            {saving
              ? t("intercompanyReconciliation.runningButton")
              : t("intercompanyReconciliation.runButton")}
          </button>
        </form>

        {summary && (
          <div className="rounded border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700">
            {t("intercompanyReconciliation.summary", {
              pairCount: summary.pairCount || 0,
              matchedPairCount: summary.matchedPairCount || 0,
              mismatchedPairCount: summary.mismatchedPairCount || 0,
              unilateralPairCount: summary.unilateralPairCount || 0,
              total: formatAmount(summary.totalAbsoluteDifferenceBase || 0),
            })}
          </div>
        )}

        <div className="overflow-x-auto rounded border border-slate-200">
          <table className="min-w-full text-xs">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-2 py-2">{t("intercompanyReconciliation.table.pair")}</th>
                <th className="px-2 py-2">{t("intercompanyReconciliation.table.status")}</th>
                <th className="px-2 py-2">{t("intercompanyReconciliation.table.abNet")}</th>
                <th className="px-2 py-2">{t("intercompanyReconciliation.table.baNet")}</th>
                <th className="px-2 py-2">
                  {t("intercompanyReconciliation.table.difference")}
                </th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr
                  key={`${row.entityA?.id}-${row.entityB?.id}-${index}`}
                  className="border-t border-slate-100"
                >
                  <td className="px-2 py-2">
                    {row.entityA?.code || row.entityA?.id} /{" "}
                    {row.entityB?.code || row.entityB?.id}
                  </td>
                  <td className="px-2 py-2">{row.status}</td>
                  <td className="px-2 py-2">{formatAmount(row.directionAB?.netBase || 0)}</td>
                  <td className="px-2 py-2">{formatAmount(row.directionBA?.netBase || 0)}</td>
                  <td className="px-2 py-2">{formatAmount(row.differenceBase || 0)}</td>
                </tr>
              ))}
              {rows.length === 0 && (
                <tr>
                  <td colSpan={5} className="px-2 py-3 text-slate-500">
                    {t("intercompanyReconciliation.table.empty")}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
