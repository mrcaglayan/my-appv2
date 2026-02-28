import { useEffect, useMemo, useState } from "react";
import {
  listCashExceptions,
  listCashRegisters,
} from "../../api/cashAdmin.js";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";
import CashControlModeBanner from "./CashControlModeBanner.jsx";

const EMPTY_SECTIONS = {
  highVariance: { total: 0, rows: [] },
  forcedClose: { total: 0, rows: [] },
  overrideUsage: { total: 0, rows: [] },
  unposted: { total: 0, rows: [] },
  glCashControlEvents: { total: 0, rows: [] },
};

const INITIAL_FILTERS = {
  registerId: "",
  legalEntityId: "",
  operatingUnitId: "",
  fromDate: "",
  toDate: "",
  minAbsVariance: "0",
};

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function toOptionalNumber(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : Number.NaN;
}

function toUpper(value) {
  return String(value || "").trim().toUpperCase();
}

function normalizeErrorMessage(value) {
  return String(value || "").trim();
}

function extractRequestId(err) {
  return (
    err?.response?.data?.requestId ||
    err?.response?.headers?.["x-request-id"] ||
    null
  );
}

function formatAmount(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) {
    return "-";
  }
  return amount.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 6,
  });
}

function formatDate(value) {
  if (!value) {
    return "-";
  }
  const date = new Date(`${String(value).slice(0, 10)}T00:00:00`);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleDateString();
}

function formatDateTime(value) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return `${date.toLocaleDateString()} ${date.toLocaleTimeString()}`;
}

function toRegisterLabel(row) {
  return `${row?.cash_register_code || row?.cash_register_id || "-"} - ${
    row?.cash_register_name || "-"
  }`;
}

function sectionOrEmpty(section) {
  return {
    total: Number(section?.total || 0),
    rows: Array.isArray(section?.rows) ? section.rows : [],
  };
}

function stringifyPayload(value) {
  if (value === undefined || value === null || value === "") {
    return "-";
  }
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

export default function CashExceptionsPage() {
  const { hasPermission } = useAuth();
  const { t } = useI18n();

  const canRead = hasPermission("cash.report.read");

  const [filters, setFilters] = useState(INITIAL_FILTERS);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [errorRequestId, setErrorRequestId] = useState(null);
  const [warning, setWarning] = useState("");

  const [registerRows, setRegisterRows] = useState([]);
  const [sections, setSections] = useState(EMPTY_SECTIONS);
  const [notes, setNotes] = useState([]);

  const selectedLegalEntityId = toPositiveInt(filters.legalEntityId);
  const selectedOperatingUnitId = toPositiveInt(filters.operatingUnitId);

  const legalEntityOptions = useMemo(() => {
    const map = new Map();
    for (const row of registerRows) {
      const id = toPositiveInt(row?.legal_entity_id);
      if (!id || map.has(id)) {
        continue;
      }
      map.set(id, {
        id,
        code: row?.legal_entity_code || String(id),
        name: row?.legal_entity_name || "-",
      });
    }
    return [...map.values()].sort((a, b) =>
      String(a.code || "").localeCompare(String(b.code || ""))
    );
  }, [registerRows]);

  const operatingUnitOptions = useMemo(() => {
    const map = new Map();
    for (const row of registerRows) {
      const id = toPositiveInt(row?.operating_unit_id);
      if (!id || map.has(id)) {
        continue;
      }
      if (
        selectedLegalEntityId &&
        toPositiveInt(row?.legal_entity_id) !== selectedLegalEntityId
      ) {
        continue;
      }
      map.set(id, {
        id,
        code: row?.operating_unit_code || String(id),
        name: row?.operating_unit_name || "-",
      });
    }
    return [...map.values()].sort((a, b) =>
      String(a.code || "").localeCompare(String(b.code || ""))
    );
  }, [registerRows, selectedLegalEntityId]);

  const registerOptions = useMemo(() => {
    const filtered = registerRows.filter((row) => {
      if (
        selectedLegalEntityId &&
        toPositiveInt(row?.legal_entity_id) !== selectedLegalEntityId
      ) {
        return false;
      }
      if (
        selectedOperatingUnitId &&
        toPositiveInt(row?.operating_unit_id) !== selectedOperatingUnitId
      ) {
        return false;
      }
      return true;
    });

    return [...filtered].sort((a, b) =>
      String(a?.code || "").localeCompare(String(b?.code || ""))
    );
  }, [registerRows, selectedLegalEntityId, selectedOperatingUnitId]);

  function resetMessages() {
    setError("");
    setErrorRequestId(null);
    setWarning("");
  }

  function setSectionsFromApi(payload) {
    const nextSections = payload?.sections || {};
    setSections({
      highVariance: sectionOrEmpty(nextSections.highVariance),
      forcedClose: sectionOrEmpty(nextSections.forcedClose),
      overrideUsage: sectionOrEmpty(nextSections.overrideUsage),
      unposted: sectionOrEmpty(nextSections.unposted),
      glCashControlEvents: sectionOrEmpty(nextSections.glCashControlEvents),
    });
    setNotes(Array.isArray(payload?.notes) ? payload.notes : []);
  }

  async function loadData(nextFilters = filters) {
    if (!canRead) {
      setRegisterRows([]);
      setSections(EMPTY_SECTIONS);
      setNotes([]);
      return;
    }

    const threshold = toOptionalNumber(nextFilters.minAbsVariance);
    if (threshold !== null && (Number.isNaN(threshold) || threshold < 0)) {
      setError(t("cashExceptions.errors.invalidVarianceThreshold"));
      setErrorRequestId(null);
      return;
    }

    setLoading(true);
    resetMessages();

    const requestQuery = {
      legalEntityId: toPositiveInt(nextFilters.legalEntityId) || undefined,
      operatingUnitId: toPositiveInt(nextFilters.operatingUnitId) || undefined,
      registerId: toPositiveInt(nextFilters.registerId) || undefined,
      fromDate: nextFilters.fromDate || undefined,
      toDate: nextFilters.toDate || undefined,
      minAbsVariance:
        threshold === null ? 0 : Number(threshold.toFixed(6)),
      limit: 200,
      offset: 0,
      includeRows: true,
    };

    const [registerResult, exceptionResult] = await Promise.allSettled([
      listCashRegisters({ limit: 500, offset: 0 }),
      listCashExceptions(requestQuery),
    ]);

    const warnings = [];

    if (registerResult.status === "fulfilled") {
      setRegisterRows(registerResult.value?.rows || []);
    } else {
      setRegisterRows([]);
      warnings.push(
        registerResult.reason?.response?.data?.message ||
          t("cashExceptions.warnings.registerLookupUnavailable")
      );
    }

    if (exceptionResult.status === "fulfilled") {
      setSectionsFromApi(exceptionResult.value || {});
      const noteText = Array.isArray(exceptionResult.value?.notes)
        ? exceptionResult.value.notes.join(" ")
        : "";
      const warningText = [warnings.join(" "), noteText].filter(Boolean).join(" ");
      setWarning(warningText);
      setError("");
      setErrorRequestId(null);
    } else {
      setSections(EMPTY_SECTIONS);
      setNotes([]);
      const err = exceptionResult.reason;
      const message = normalizeErrorMessage(
        err?.response?.data?.message || err?.message
      );
      setError(message || t("cashExceptions.errors.load"));
      setErrorRequestId(extractRequestId(err));
      setWarning(warnings.join(" "));
    }

    setLoading(false);
  }

  useEffect(() => {
    loadData(INITIAL_FILTERS);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead]);

  function handleApplyFilters(event) {
    event.preventDefault();

    if (filters.fromDate && filters.toDate && filters.fromDate > filters.toDate) {
      setError(t("cashExceptions.errors.invalidDateRange"));
      setErrorRequestId(null);
      return;
    }

    loadData(filters);
  }

  function handleClearFilters() {
    setFilters(INITIAL_FILTERS);
    loadData(INITIAL_FILTERS);
  }

  if (!canRead) {
    return (
      <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
        {t("cashExceptions.errors.missingReadPermission")}
      </div>
    );
  }

  const highVarianceRows = sections.highVariance.rows;
  const forcedCloseRows = sections.forcedClose.rows;
  const overrideRows = sections.overrideUsage.rows;
  const unpostedRows = sections.unposted.rows;
  const glRows = sections.glCashControlEvents.rows;

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">{t("cashExceptions.title")}</h1>
        <p className="mt-1 text-sm text-slate-600">{t("cashExceptions.subtitle")}</p>
      </div>

      <CashControlModeBanner />

      <div className="rounded-lg border border-blue-200 bg-blue-50 px-3 py-2 text-sm text-blue-800">
        {t("cashExceptions.glWarningNote")}
      </div>

      {error ? (
        <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
          <p>{error}</p>
          {errorRequestId ? (
            <p className="mt-1 text-xs font-medium text-rose-700">
              {t("cashExceptions.requestId", { requestId: errorRequestId })}
            </p>
          ) : null}
        </div>
      ) : null}

      {warning ? (
        <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          {warning}
        </div>
      ) : null}

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("cashExceptions.sections.filters")}
        </h2>

        <form onSubmit={handleApplyFilters} className="grid gap-2 md:grid-cols-3">
          <select
            value={filters.legalEntityId}
            onChange={(event) =>
              setFilters((prev) => ({
                ...prev,
                legalEntityId: event.target.value,
                operatingUnitId: "",
                registerId: "",
              }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            <option value="">{t("cashExceptions.filters.allLegalEntities")}</option>
            {legalEntityOptions.map((option) => (
              <option key={`cash-exc-le-${option.id}`} value={option.id}>
                {option.code} - {option.name}
              </option>
            ))}
          </select>

          <select
            value={filters.operatingUnitId}
            onChange={(event) =>
              setFilters((prev) => ({
                ...prev,
                operatingUnitId: event.target.value,
                registerId: "",
              }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            <option value="">{t("cashExceptions.filters.allOperatingUnits")}</option>
            {operatingUnitOptions.map((option) => (
              <option key={`cash-exc-ou-${option.id}`} value={option.id}>
                {option.code} - {option.name}
              </option>
            ))}
          </select>

          <select
            value={filters.registerId}
            onChange={(event) =>
              setFilters((prev) => ({ ...prev, registerId: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            <option value="">{t("cashExceptions.filters.allRegisters")}</option>
            {registerOptions.map((row) => (
              <option key={`cash-exc-register-${row.id}`} value={row.id}>
                {`${row.code || row.id} - ${row.name || "-"}`}
              </option>
            ))}
          </select>

          <label className="flex flex-col gap-1 text-xs font-semibold text-slate-600">
            <span>{t("cashExceptions.filters.fromDate")}</span>
            <input
              type="date"
              value={filters.fromDate}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, fromDate: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-normal"
            />
          </label>

          <label className="flex flex-col gap-1 text-xs font-semibold text-slate-600">
            <span>{t("cashExceptions.filters.toDate")}</span>
            <input
              type="date"
              value={filters.toDate}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, toDate: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-normal"
            />
          </label>

          <label className="flex flex-col gap-1 text-xs font-semibold text-slate-600">
            <span>{t("cashExceptions.filters.minAbsVariance")}</span>
            <input
              type="number"
              min="0"
              step="0.000001"
              value={filters.minAbsVariance}
              onChange={(event) =>
                setFilters((prev) => ({
                  ...prev,
                  minAbsVariance: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-normal"
            />
          </label>

          <div className="md:col-span-3 flex flex-wrap gap-2">
            <button
              type="submit"
              disabled={loading}
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {loading ? t("cashExceptions.actions.loading") : t("cashExceptions.actions.apply")}
            </button>
            <button
              type="button"
              disabled={loading}
              onClick={handleClearFilters}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 disabled:opacity-60"
            >
              {t("cashExceptions.actions.clear")}
            </button>
            <button
              type="button"
              disabled={loading}
              onClick={() => loadData(filters)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 disabled:opacity-60"
            >
              {t("cashExceptions.actions.refresh")}
            </button>
          </div>
        </form>
      </section>

      <section className="grid gap-3 md:grid-cols-5">
        <article className="rounded-xl border border-slate-200 bg-white p-4">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            {t("cashExceptions.cards.highVariance")}
          </p>
          <p className="mt-2 text-2xl font-semibold text-slate-900">{sections.highVariance.total}</p>
        </article>
        <article className="rounded-xl border border-slate-200 bg-white p-4">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            {t("cashExceptions.cards.forcedClose")}
          </p>
          <p className="mt-2 text-2xl font-semibold text-slate-900">{sections.forcedClose.total}</p>
        </article>
        <article className="rounded-xl border border-slate-200 bg-white p-4">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            {t("cashExceptions.cards.overrideUsage")}
          </p>
          <p className="mt-2 text-2xl font-semibold text-slate-900">{sections.overrideUsage.total}</p>
        </article>
        <article className="rounded-xl border border-slate-200 bg-white p-4">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            {t("cashExceptions.cards.unposted")}
          </p>
          <p className="mt-2 text-2xl font-semibold text-slate-900">{sections.unposted.total}</p>
        </article>
        <article className="rounded-xl border border-slate-200 bg-white p-4">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            {t("cashExceptions.cards.glCashControlEvents")}
          </p>
          <p className="mt-2 text-2xl font-semibold text-slate-900">{sections.glCashControlEvents.total}</p>
        </article>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("cashExceptions.sections.highVariance")}
        </h2>
        <div className="overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">ID</th>
                <th className="px-3 py-2">{t("cashExceptions.table.register")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.legalEntity")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.operatingUnit")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.status")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.expected")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.counted")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.variance")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.closedAt")}</th>
              </tr>
            </thead>
            <tbody>
              {highVarianceRows.map((row) => (
                <tr key={`cash-exc-hv-${row.id}`} className="border-t border-slate-100">
                  <td className="px-3 py-2">{row.id}</td>
                  <td className="px-3 py-2">{toRegisterLabel(row)}</td>
                  <td className="px-3 py-2">
                    {(row.legal_entity_code || row.legal_entity_id || "-") +
                      " - " +
                      (row.legal_entity_name || "-")}
                  </td>
                  <td className="px-3 py-2">
                    {row.operating_unit_id
                      ? `${row.operating_unit_code || row.operating_unit_id} - ${
                          row.operating_unit_name || "-"
                        }`
                      : "-"}
                  </td>
                  <td className="px-3 py-2">{row.status || "-"}</td>
                  <td className="px-3 py-2">{formatAmount(row.expected_closing_amount)}</td>
                  <td className="px-3 py-2">{formatAmount(row.counted_closing_amount)}</td>
                  <td className="px-3 py-2">{formatAmount(row.variance_amount)}</td>
                  <td className="px-3 py-2">{formatDateTime(row.closed_at)}</td>
                </tr>
              ))}
              {!loading && highVarianceRows.length === 0 ? (
                <tr>
                  <td colSpan={9} className="px-3 py-3 text-slate-500">
                    {t("cashExceptions.empty.highVariance")}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("cashExceptions.sections.forcedClose")}
        </h2>
        <div className="overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">ID</th>
                <th className="px-3 py-2">{t("cashExceptions.table.register")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.status")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.closedReason")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.closeNote")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.closedBy")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.closedAt")}</th>
              </tr>
            </thead>
            <tbody>
              {forcedCloseRows.map((row) => (
                <tr key={`cash-exc-fc-${row.id}`} className="border-t border-slate-100">
                  <td className="px-3 py-2">{row.id}</td>
                  <td className="px-3 py-2">{toRegisterLabel(row)}</td>
                  <td className="px-3 py-2">{row.status || "-"}</td>
                  <td className="px-3 py-2">{row.closed_reason || row.closedReason || "-"}</td>
                  <td className="px-3 py-2">{row.close_note || row.closeNote || "-"}</td>
                  <td className="px-3 py-2">{row.closed_by_email || row.closed_by_user_id || "-"}</td>
                  <td className="px-3 py-2">{formatDateTime(row.closed_at)}</td>
                </tr>
              ))}
              {!loading && forcedCloseRows.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-3 py-3 text-slate-500">
                    {t("cashExceptions.empty.forcedClose")}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("cashExceptions.sections.overrideUsage")}
        </h2>
        <div className="overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">ID</th>
                <th className="px-3 py-2">{t("cashExceptions.table.txnNo")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.txnType")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.status")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.register")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.bookDate")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.amount")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.overrideReason")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.postedJournal")}</th>
              </tr>
            </thead>
            <tbody>
              {overrideRows.map((row) => (
                <tr key={`cash-exc-ov-${row.id}`} className="border-t border-slate-100">
                  <td className="px-3 py-2">{row.id}</td>
                  <td className="px-3 py-2">{row.txn_no || "-"}</td>
                  <td className="px-3 py-2">{row.txn_type || "-"}</td>
                  <td className="px-3 py-2">{row.status || "-"}</td>
                  <td className="px-3 py-2">{toRegisterLabel(row)}</td>
                  <td className="px-3 py-2">{formatDate(row.book_date)}</td>
                  <td className="px-3 py-2">{formatAmount(row.amount)}</td>
                  <td className="px-3 py-2">{row.override_reason || row.overrideReason || "-"}</td>
                  <td className="px-3 py-2">
                    {row.posted_journal_entry_id || row.postedJournalEntryId || "-"}
                  </td>
                </tr>
              ))}
              {!loading && overrideRows.length === 0 ? (
                <tr>
                  <td colSpan={9} className="px-3 py-3 text-slate-500">
                    {t("cashExceptions.empty.overrideUsage")}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("cashExceptions.sections.unposted")}
        </h2>
        <div className="overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">ID</th>
                <th className="px-3 py-2">{t("cashExceptions.table.txnNo")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.txnType")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.status")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.register")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.bookDate")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.amount")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.createdAt")}</th>
              </tr>
            </thead>
            <tbody>
              {unpostedRows.map((row) => (
                <tr key={`cash-exc-up-${row.id}`} className="border-t border-slate-100">
                  <td className="px-3 py-2">{row.id}</td>
                  <td className="px-3 py-2">{row.txn_no || "-"}</td>
                  <td className="px-3 py-2">{row.txn_type || "-"}</td>
                  <td className="px-3 py-2">{row.status || "-"}</td>
                  <td className="px-3 py-2">{toRegisterLabel(row)}</td>
                  <td className="px-3 py-2">{formatDate(row.book_date)}</td>
                  <td className="px-3 py-2">{formatAmount(row.amount)}</td>
                  <td className="px-3 py-2">{formatDateTime(row.created_at)}</td>
                </tr>
              ))}
              {!loading && unpostedRows.length === 0 ? (
                <tr>
                  <td colSpan={8} className="px-3 py-3 text-slate-500">
                    {t("cashExceptions.empty.unposted")}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("cashExceptions.sections.glCashControlEvents")}
        </h2>
        <div className="overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">ID</th>
                <th className="px-3 py-2">{t("cashExceptions.table.action")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.legalEntity")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.journalNo")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.resource")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.scope")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.requestId")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.payload")}</th>
                <th className="px-3 py-2">{t("cashExceptions.table.createdAt")}</th>
              </tr>
            </thead>
            <tbody>
              {glRows.map((row) => (
                <tr key={`cash-exc-gl-${row.id}`} className="border-t border-slate-100">
                  <td className="px-3 py-2">{row.id}</td>
                  <td className="px-3 py-2">
                    {toUpper(row.action) === "GL.CASH_CONTROL.OVERRIDE"
                      ? t("cashExceptions.values.glActionOverride")
                      : t("cashExceptions.values.glActionWarn")}
                  </td>
                  <td className="px-3 py-2">
                    {row.legal_entity_code || row.journal_legal_entity_id
                      ? `${row.legal_entity_code || row.journal_legal_entity_id} - ${
                          row.legal_entity_name || "-"
                        }`
                      : "-"}
                  </td>
                  <td className="px-3 py-2">{row.journal_no || "-"}</td>
                  <td className="px-3 py-2">{`${row.resource_type || "-"}:${row.resource_id || "-"}`}</td>
                  <td className="px-3 py-2">{`${row.scope_type || "-"}:${row.scope_id || "-"}`}</td>
                  <td className="px-3 py-2">{row.request_id || "-"}</td>
                  <td className="px-3 py-2">{stringifyPayload(row.payload_json)}</td>
                  <td className="px-3 py-2">{formatDateTime(row.created_at)}</td>
                </tr>
              ))}
              {!loading && glRows.length === 0 ? (
                <tr>
                  <td colSpan={9} className="px-3 py-3 text-slate-500">
                    {t("cashExceptions.empty.glCashControlEvents")}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      {notes.length > 0 ? (
        <section className="rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="mb-2 text-sm font-semibold text-slate-700">
            {t("cashExceptions.sections.notes")}
          </h2>
          <ul className="list-disc pl-5 text-sm text-slate-700">
            {notes.map((note, index) => (
              <li key={`cash-exception-note-${index}`}>{note}</li>
            ))}
          </ul>
        </section>
      ) : null}
    </div>
  );
}
