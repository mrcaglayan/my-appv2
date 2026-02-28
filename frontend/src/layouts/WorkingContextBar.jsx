import { useAuth } from "../auth/useAuth.js";
import { useWorkingContext } from "../context/useWorkingContext.js";
import { useI18n } from "../i18n/useI18n.js";

function toOptionLabel(row, fallbackPrefix) {
  const code = String(row?.code || "").trim();
  const name = String(row?.name || "").trim();
  if (code && name) return `${code} - ${name}`;
  if (code) return code;
  if (name) return name;
  return `${fallbackPrefix} #${row?.id || "?"}`;
}

function renderPeriodLabel(row) {
  if (!row) return "-";
  const year = row?.fiscal_year ?? row?.fiscalYear ?? "-";
  const periodNo = row?.period_no ?? row?.periodNo ?? "-";
  const periodName = row?.period_name ?? row?.periodName ?? "";
  const periodPart =
    Number.isFinite(Number(periodNo)) && Number(periodNo) > 0
      ? `P${String(periodNo).padStart(2, "0")}`
      : `P${periodNo}`;
  return periodName ? `FY${year} ${periodPart} - ${periodName}` : `FY${year} ${periodPart}`;
}

export default function WorkingContextBar() {
  const { isAuthed } = useAuth();
  const { t } = useI18n();
  const {
    workingContext,
    setWorkingContext,
    refreshLookups,
    legalEntities,
    operatingUnits,
    fiscalCalendars,
    fiscalPeriods,
    loading,
    error,
  } = useWorkingContext();

  if (!isAuthed) {
    return null;
  }

  return (
    <div className="flex flex-wrap items-end gap-2">
      <label className="min-w-[12rem] flex-1 text-[11px] font-semibold uppercase tracking-wide text-slate-600">
        {t("workingContext.legalEntity", "Legal entity")}
        <select
          className="mt-1 w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-sm font-normal text-slate-700"
          value={workingContext.legalEntityId}
          onChange={(event) =>
            setWorkingContext({
              legalEntityId: event.target.value,
            })
          }
          disabled={loading && legalEntities.length === 0}
        >
          <option value="">
            {t("workingContext.selectLegalEntity", "Select legal entity")}
          </option>
          {legalEntities.map((row) => (
            <option key={`working-context-le-${row.id}`} value={String(row.id)}>
              {toOptionLabel(row, "LE")}
            </option>
          ))}
        </select>
      </label>

      <label className="min-w-[12rem] flex-1 text-[11px] font-semibold uppercase tracking-wide text-slate-600">
        {t("workingContext.operatingUnit", "Operating unit")}
        <select
          className="mt-1 w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-sm font-normal text-slate-700"
          value={workingContext.operatingUnitId}
          onChange={(event) =>
            setWorkingContext({
              operatingUnitId: event.target.value,
            })
          }
          disabled={!workingContext.legalEntityId || (loading && operatingUnits.length === 0)}
        >
          <option value="">{t("workingContext.allOperatingUnits", "All operating units")}</option>
          {operatingUnits.map((row) => (
            <option key={`working-context-ou-${row.id}`} value={String(row.id)}>
              {toOptionLabel(row, "OU")}
            </option>
          ))}
        </select>
      </label>

      <label className="min-w-[11rem] flex-1 text-[11px] font-semibold uppercase tracking-wide text-slate-600">
        {t("workingContext.fiscalCalendar", "Fiscal calendar")}
        <select
          className="mt-1 w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-sm font-normal text-slate-700"
          value={workingContext.fiscalCalendarId}
          onChange={(event) =>
            setWorkingContext({
              fiscalCalendarId: event.target.value,
            })
          }
          disabled={loading && fiscalCalendars.length === 0}
        >
          <option value="">
            {t("workingContext.selectFiscalCalendar", "Select fiscal calendar")}
          </option>
          {fiscalCalendars.map((row) => (
            <option key={`working-context-calendar-${row.id}`} value={String(row.id)}>
              {toOptionLabel(row, "CAL")}
            </option>
          ))}
        </select>
      </label>

      <label className="min-w-[14rem] flex-[1.2] text-[11px] font-semibold uppercase tracking-wide text-slate-600">
        {t("workingContext.fiscalPeriod", "Fiscal period")}
        <select
          className="mt-1 w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-sm font-normal text-slate-700"
          value={workingContext.fiscalPeriodId}
          onChange={(event) =>
            setWorkingContext({
              fiscalPeriodId: event.target.value,
            })
          }
          disabled={!workingContext.fiscalCalendarId || (loading && fiscalPeriods.length === 0)}
        >
          <option value="">
            {t("workingContext.selectFiscalPeriod", "Select fiscal period")}
          </option>
          {fiscalPeriods.map((row) => (
            <option key={`working-context-period-${row.id}`} value={String(row.id)}>
              {renderPeriodLabel(row)}
            </option>
          ))}
        </select>
      </label>

      <div className="flex items-center gap-2 pb-1">
        {workingContext.dateFrom && workingContext.dateTo ? (
          <span className="rounded border border-slate-200 bg-slate-50 px-2 py-1 text-xs text-slate-600">
            {workingContext.dateFrom} - {workingContext.dateTo}
          </span>
        ) : null}
        <button
          type="button"
          onClick={refreshLookups}
          className="rounded border border-slate-300 bg-white px-2 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-50"
          disabled={loading}
        >
          {loading ? t("workingContext.loading", "Loading...") : t("workingContext.refresh", "Refresh")}
        </button>
      </div>

      {error ? (
        <p className="w-full text-xs text-rose-600">
          {error}
        </p>
      ) : null}
    </div>
  );
}

