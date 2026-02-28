import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { listPayrollRuns } from "../../api/payrollRuns.js";
import { useAuth } from "../../auth/useAuth.js";

function formatAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return "-";
  }
  return parsed.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 6,
  });
}

function formatDate(value) {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }
  return parsed.toISOString().slice(0, 10);
}

function buildCloseControlsHref(row) {
  const params = new URLSearchParams();
  if (row?.legal_entity_id) params.set("legalEntityId", String(row.legal_entity_id));
  const payrollPeriod = formatDate(row?.payroll_period);
  if (payrollPeriod && payrollPeriod !== "-") {
    params.set("payrollPeriod", payrollPeriod);
  }
  const query = params.toString();
  return `/app/payroll-close-controls${query ? `?${query}` : ""}`;
}

function sourceLabel(row) {
  const sourceType = String(row?.source_type || "").toUpperCase();
  if (!sourceType || sourceType === "MANUAL") return null;
  if (sourceType === "PROVIDER_IMPORT") {
    const jobId = row?.source_provider_import_job_id ? ` #${row.source_provider_import_job_id}` : "";
    const provider = row?.source_provider_code ? ` (${row.source_provider_code})` : "";
    return `PROVIDER_IMPORT${jobId}${provider}`;
  }
  return sourceType;
}

export default function PayrollRunsPage() {
  const { hasPermission } = useAuth();
  const canRead = hasPermission("payroll.runs.read");
  const canImport = hasPermission("payroll.runs.import");

  const [filters, setFilters] = useState({
    providerCode: "",
    payrollPeriod: "",
    status: "",
    q: "",
  });
  const [rows, setRows] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function loadRuns() {
    if (!canRead) {
      setRows([]);
      setTotal(0);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const res = await listPayrollRuns({
        limit: 200,
        offset: 0,
        providerCode: filters.providerCode || undefined,
        payrollPeriod: filters.payrollPeriod || undefined,
        status: filters.status || undefined,
        q: filters.q || undefined,
      });
      setRows(res?.rows || []);
      setTotal(Number(res?.total || 0));
    } catch (err) {
      setRows([]);
      setTotal(0);
      setError(err?.response?.data?.message || "Payroll run listesi yuklenemedi");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadRuns();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead]);

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold text-slate-900">Bordro Runlari</h1>
          <p className="mt-1 text-sm text-slate-600">
            Payroll subledger import kayıtları (PR-P01).
          </p>
        </div>
        {canImport ? (
          <Link
            to="/app/payroll-runs/import"
            className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700"
          >
            Bordro Import
          </Link>
        ) : null}
      </div>

      {!canRead ? (
        <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          Missing permission: <code>payroll.runs.read</code>
        </div>
      ) : null}
      {error ? (
        <div className="rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
          {error}
        </div>
      ) : null}

      <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="grid gap-3 md:grid-cols-4">
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-700">Provider</label>
            <input
              value={filters.providerCode}
              onChange={(e) => setFilters((prev) => ({ ...prev, providerCode: e.target.value }))}
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
              placeholder="OUTSOURCED_PAYROLL_X"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-700">Payroll Period</label>
            <input
              type="date"
              value={filters.payrollPeriod}
              onChange={(e) => setFilters((prev) => ({ ...prev, payrollPeriod: e.target.value }))}
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-700">Status</label>
            <select
              value={filters.status}
              onChange={(e) => setFilters((prev) => ({ ...prev, status: e.target.value }))}
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
            >
              <option value="">Tum</option>
              <option value="IMPORTED">IMPORTED</option>
              <option value="REVIEWED">REVIEWED</option>
              <option value="FINALIZED">FINALIZED</option>
              <option value="DRAFT">DRAFT</option>
            </select>
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-700">Arama</label>
            <div className="flex gap-2">
              <input
                value={filters.q}
                onChange={(e) => setFilters((prev) => ({ ...prev, q: e.target.value }))}
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="run no / provider / file"
              />
              <button
                type="button"
                className="rounded border border-slate-300 px-3 py-1.5 text-sm"
                onClick={loadRuns}
                disabled={loading || !canRead}
              >
                Ara
              </button>
            </div>
          </div>
        </div>

        <div className="mt-3 text-xs text-slate-500">Toplam kayıt: {total}</div>

        <div className="mt-4 overflow-auto">
          <table className="min-w-full border-collapse text-sm">
            <thead>
              <tr className="border-b">
                <th className="p-2 text-left">Run No</th>
                <th className="p-2 text-left">Entity</th>
                <th className="p-2 text-left">Provider</th>
                <th className="p-2 text-left">Period</th>
                <th className="p-2 text-left">Pay Date</th>
                <th className="p-2 text-left">Employees</th>
                <th className="p-2 text-left">Gross</th>
                <th className="p-2 text-left">Net</th>
                <th className="p-2 text-left">Status</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.id} className="border-b">
                  <td className="p-2">
                    <Link className="underline" to={`/app/payroll-runs/${row.id}`}>
                      {row.run_no}
                    </Link>
                    {row.legal_entity_id ? (
                      <div className="mt-1 text-xs">
                        <Link className="underline text-slate-600" to={buildCloseControlsHref(row)}>
                          Close Controls
                        </Link>
                      </div>
                    ) : null}
                  </td>
                  <td className="p-2">
                    {row.entity_code}
                    <div className="text-xs text-slate-500">{row.legal_entity_name || ""}</div>
                  </td>
                  <td className="p-2">
                    <div>{row.provider_code}</div>
                    {sourceLabel(row) ? (
                      <div className="mt-1 text-xs text-indigo-700">{sourceLabel(row)}</div>
                    ) : null}
                  </td>
                  <td className="p-2">{formatDate(row.payroll_period)}</td>
                  <td className="p-2">{formatDate(row.pay_date)}</td>
                  <td className="p-2">{row.employee_count}</td>
                  <td className="p-2">{formatAmount(row.total_gross_pay)}</td>
                  <td className="p-2">{formatAmount(row.total_net_pay)}</td>
                  <td className="p-2">
                    <div>{row.status}</div>
                    {row.run_type && row.run_type !== "REGULAR" ? (
                      <div className="text-xs text-slate-500">{row.run_type}</div>
                    ) : null}
                    {Number(row.is_reversed || 0) === 1 ? (
                      <div className="text-xs text-rose-600">REVERSED</div>
                    ) : null}
                  </td>
                </tr>
              ))}
              {rows.length === 0 ? (
                <tr>
                  <td className="p-3 text-slate-500" colSpan={9}>
                    Kayit yok.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
