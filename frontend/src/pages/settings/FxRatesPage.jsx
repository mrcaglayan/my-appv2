import { useEffect, useMemo, useState } from "react";
import { bulkUpsertFxRates, listFxRates } from "../../api/fxAdmin.js";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";
import TenantReadinessChecklist from "../../readiness/TenantReadinessChecklist.jsx";

function createDraftRow(rateDate = "") {
  return {
    id: `fx-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    rateDate,
    fromCurrencyCode: "USD",
    toCurrencyCode: "TRY",
    rateType: "CLOSING",
    value: "",
    source: "",
  };
}

export default function FxRatesPage() {
  const { hasPermission } = useAuth();
  const { language } = useI18n();
  const isTr = language === "tr";
  const l = (en, tr) => (isTr ? tr : en);
  const canReadRates = hasPermission("fx.rate.read");
  const canUpsertRates = hasPermission("fx.rate.bulk_upsert");

  const today = new Date().toISOString().slice(0, 10);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [rows, setRows] = useState([]);
  const [queryForm, setQueryForm] = useState({
    dateFrom: today.slice(0, 8) + "01",
    dateTo: today,
    fromCurrencyCode: "",
    toCurrencyCode: "",
    rateType: "",
  });
  const [draftRows, setDraftRows] = useState([createDraftRow(today)]);

  const sortedRows = useMemo(() => {
    return [...rows].sort((a, b) => {
      const dateCompare = String(b.rate_date || "").localeCompare(
        String(a.rate_date || "")
      );
      if (dateCompare !== 0) return dateCompare;
      const fromCompare = String(a.from_currency_code || "").localeCompare(
        String(b.from_currency_code || "")
      );
      if (fromCompare !== 0) return fromCompare;
      return String(a.to_currency_code || "").localeCompare(
        String(b.to_currency_code || "")
      );
    });
  }, [rows]);

  async function loadRates(nextFilters = queryForm) {
    if (!canReadRates) {
      setRows([]);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const response = await listFxRates({
        dateFrom: nextFilters.dateFrom || undefined,
        dateTo: nextFilters.dateTo || undefined,
        fromCurrencyCode: nextFilters.fromCurrencyCode || undefined,
        toCurrencyCode: nextFilters.toCurrencyCode || undefined,
        rateType: nextFilters.rateType || undefined,
      });
      setRows(response?.rows || []);
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l("Failed to load FX rates.", "Kur verileri yuklenemedi.")
      );
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadRates();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadRates]);

  function updateDraftRow(rowId, field, value) {
    setDraftRows((prev) =>
      prev.map((row) => (row.id === rowId ? { ...row, [field]: value } : row))
    );
  }

  function addDraftRow() {
    setDraftRows((prev) => [...prev, createDraftRow(today)]);
  }

  function removeDraftRow(rowId) {
    setDraftRows((prev) => {
      if (prev.length <= 1) {
        return prev;
      }
      return prev.filter((row) => row.id !== rowId);
    });
  }

  async function handleBulkUpsert(event) {
    event.preventDefault();
    if (!canUpsertRates) {
      setError(l("Missing permission: fx.rate.bulk_upsert", "Eksik yetki: fx.rate.bulk_upsert"));
      return;
    }

    const payload = [];
    for (let index = 0; index < draftRows.length; index += 1) {
      const row = draftRows[index];
      const numericRate = Number(row.value);
      if (!row.rateDate || !row.fromCurrencyCode || !row.toCurrencyCode || !row.rateType) {
        setError(
          l(
            `Row ${index + 1}: date/currency/type are required.`,
            `Satir ${index + 1}: tarih/para birimi/tip zorunludur.`
          )
        );
        return;
      }
      if (!Number.isFinite(numericRate) || numericRate <= 0) {
        setError(
          l(
            `Row ${index + 1}: rate value must be positive.`,
            `Satir ${index + 1}: kur degeri pozitif olmalidir.`
          )
        );
        return;
      }

      payload.push({
        rateDate: row.rateDate,
        fromCurrencyCode: String(row.fromCurrencyCode).toUpperCase(),
        toCurrencyCode: String(row.toCurrencyCode).toUpperCase(),
        rateType: String(row.rateType).toUpperCase(),
        value: numericRate,
        source: row.source ? String(row.source).trim() : undefined,
      });
    }

    setSaving(true);
    setError("");
    setMessage("");
    try {
      const response = await bulkUpsertFxRates(payload);
      setMessage(
        l(
          `Rates upserted: ${Number(response?.upserted || payload.length)}.`,
          `Kur kayitlari guncellendi: ${Number(response?.upserted || payload.length)}.`
        )
      );
      setDraftRows([createDraftRow(today)]);
      await loadRates();
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l("Failed to upsert rates.", "Kur guncelleme islemi basarisiz.")
      );
    } finally {
      setSaving(false);
    }
  }

  async function handleQuerySubmit(event) {
    event.preventDefault();
    setError("");
    setMessage("");
    await loadRates(queryForm);
  }

  if (!canReadRates && !canUpsertRates) {
    return (
      <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
        {l(
          "You need fx.rate.read and/or fx.rate.bulk_upsert to use this page.",
          "Bu sayfayi kullanmak icin fx.rate.read ve/veya fx.rate.bulk_upsert yetkisi gerekir."
        )}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <TenantReadinessChecklist />

      <div>
        <h1 className="text-xl font-semibold text-slate-900">
          {l("FX Rate Management", "Kur Yonetimi")}
        </h1>
        <p className="mt-1 text-sm text-slate-600">
          {l(
            "Query and bulk upsert foreign exchange rates used by consolidation and valuation flows.",
            "Konsolidasyon ve degerleme akislarinda kullanilan doviz kurlarini sorgulayin ve toplu guncelleyin."
          )}
        </p>
      </div>

      {error && (
        <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
          {error}
        </div>
      )}
      {message && (
        <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
          {message}
        </div>
      )}

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {l("Rate Query", "Kur Sorgulama")}
        </h2>
        <form onSubmit={handleQuerySubmit} className="grid gap-2 md:grid-cols-6">
          <input
            type="date"
            value={queryForm.dateFrom}
            onChange={(event) =>
              setQueryForm((prev) => ({ ...prev, dateFrom: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          />
          <input
            type="date"
            value={queryForm.dateTo}
            onChange={(event) =>
              setQueryForm((prev) => ({ ...prev, dateTo: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          />
          <input
            value={queryForm.fromCurrencyCode}
            onChange={(event) =>
              setQueryForm((prev) => ({
                ...prev,
                fromCurrencyCode: event.target.value.toUpperCase(),
              }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            maxLength={3}
            placeholder={l("From (USD)", "Kaynak (USD)")}
          />
          <input
            value={queryForm.toCurrencyCode}
            onChange={(event) =>
              setQueryForm((prev) => ({
                ...prev,
                toCurrencyCode: event.target.value.toUpperCase(),
              }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            maxLength={3}
            placeholder={l("To (TRY)", "Hedef (TRY)")}
          />
          <select
            value={queryForm.rateType}
            onChange={(event) =>
              setQueryForm((prev) => ({ ...prev, rateType: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            <option value="">{l("All rate types", "Tum kur tipleri")}</option>
            {["SPOT", "AVERAGE", "CLOSING"].map((rateType) => (
              <option key={rateType} value={rateType}>
                {rateType}
              </option>
            ))}
          </select>
          <button
            type="submit"
            disabled={!canReadRates || loading}
            className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
          >
            {loading ? l("Loading...", "Yukleniyor...") : l("Query", "Sorgula")}
          </button>
        </form>

        <div className="mt-3 overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">{l("Date", "Tarih")}</th>
                <th className="px-3 py-2">{l("Pair", "Parite")}</th>
                <th className="px-3 py-2">{l("Type", "Tip")}</th>
                <th className="px-3 py-2">{l("Rate", "Kur")}</th>
                <th className="px-3 py-2">{l("Source", "Kaynak")}</th>
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((row) => (
                <tr
                  key={`${row.id}-${row.rate_date}-${row.from_currency_code}-${row.to_currency_code}-${row.rate_type}`}
                  className="border-t border-slate-100"
                >
                  <td className="px-3 py-2">{row.rate_date}</td>
                  <td className="px-3 py-2">
                    {row.from_currency_code}/{row.to_currency_code}
                  </td>
                  <td className="px-3 py-2">{row.rate_type}</td>
                  <td className="px-3 py-2">{Number(row.rate || 0).toFixed(6)}</td>
                  <td className="px-3 py-2">{row.source || "-"}</td>
                </tr>
              ))}
              {sortedRows.length === 0 && !loading && (
                <tr>
                  <td colSpan={5} className="px-3 py-3 text-slate-500">
                    {l("No FX rates found.", "Kur verisi bulunamadi.")}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-3 flex items-center justify-between gap-2">
          <h2 className="text-sm font-semibold text-slate-700">
            {l("Bulk Upsert", "Toplu Kur Guncelleme")}
          </h2>
          <button
            type="button"
            onClick={addDraftRow}
            className="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-50"
          >
            {l("Add row", "Satir ekle")}
          </button>
        </div>

        <form onSubmit={handleBulkUpsert} className="space-y-2">
          {draftRows.map((row) => (
            <div key={row.id} className="grid gap-2 md:grid-cols-7">
              <input
                type="date"
                value={row.rateDate}
                onChange={(event) =>
                  updateDraftRow(row.id, "rateDate", event.target.value)
                }
                className="rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
                required
              />
              <input
                value={row.fromCurrencyCode}
                onChange={(event) =>
                  updateDraftRow(
                    row.id,
                    "fromCurrencyCode",
                    event.target.value.toUpperCase()
                  )
                }
                className="rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
                maxLength={3}
                placeholder="USD"
                required
              />
              <input
                value={row.toCurrencyCode}
                onChange={(event) =>
                  updateDraftRow(
                    row.id,
                    "toCurrencyCode",
                    event.target.value.toUpperCase()
                  )
                }
                className="rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
                maxLength={3}
                placeholder="TRY"
                required
              />
              <select
                value={row.rateType}
                onChange={(event) =>
                  updateDraftRow(row.id, "rateType", event.target.value)
                }
                className="rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
                required
              >
                {["SPOT", "AVERAGE", "CLOSING"].map((rateType) => (
                  <option key={rateType} value={rateType}>
                    {rateType}
                  </option>
                ))}
              </select>
              <input
                type="number"
                min="0.0000000001"
                step="0.000001"
                value={row.value}
                onChange={(event) => updateDraftRow(row.id, "value", event.target.value)}
                className="rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
                placeholder={l("Rate value", "Kur degeri")}
                required
              />
              <input
                value={row.source}
                onChange={(event) => updateDraftRow(row.id, "source", event.target.value)}
                className="rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
                placeholder={l("Source (optional)", "Kaynak (opsiyonel)")}
              />
              <button
                type="button"
                onClick={() => removeDraftRow(row.id)}
                disabled={draftRows.length <= 1}
                className="rounded-lg border border-rose-200 px-2 py-1.5 text-xs font-semibold text-rose-700 hover:bg-rose-50 disabled:opacity-50"
              >
                {l("Remove", "Kaldir")}
              </button>
            </div>
          ))}

          <div className="flex justify-end">
            <button
              type="submit"
              disabled={!canUpsertRates || saving}
              className="rounded-lg bg-cyan-700 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving ? l("Saving...", "Kaydediliyor...") : l("Upsert rates", "Kurlari guncelle")}
            </button>
          </div>
        </form>
      </section>
    </div>
  );
}
