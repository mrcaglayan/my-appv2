import { useEffect, useState } from "react";
import { listSensitiveDataAudit } from "../../api/sensitiveDataAudit.js";
import { useAuth } from "../../auth/useAuth.js";

function formatDateTime(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleString();
}

export default function SensitiveDataAuditPage() {
  const { hasPermission } = useAuth();
  const canRead = hasPermission("security.sensitive_data.audit.read");

  const [filters, setFilters] = useState({
    moduleCode: "",
    objectType: "",
    action: "",
    legalEntityId: "",
    dateFrom: "",
    dateTo: "",
    q: "",
  });
  const [rows, setRows] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function loadRows() {
    if (!canRead) {
      setRows([]);
      setTotal(0);
      return;
    }
    setLoading(true);
    setError("");
    try {
      const res = await listSensitiveDataAudit({
        limit: 100,
        offset: 0,
        moduleCode: filters.moduleCode || undefined,
        objectType: filters.objectType || undefined,
        action: filters.action || undefined,
        legalEntityId: filters.legalEntityId || undefined,
        dateFrom: filters.dateFrom || undefined,
        dateTo: filters.dateTo || undefined,
        q: filters.q || undefined,
      });
      setRows(res?.rows || []);
      setTotal(Number(res?.total || 0));
    } catch (err) {
      setRows([]);
      setTotal(0);
      setError(err?.response?.data?.message || "Sensitive data audit loglari yuklenemedi");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadRows();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead]);

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">Sensitive Data Audit</h1>
        <p className="mt-1 text-sm text-slate-600">
          H01 kapsaminda sifreleme / mask / purge islemlerinin audit izleri.
        </p>
      </div>

      {!canRead ? (
        <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          Missing permission: <code>security.sensitive_data.audit.read</code>
        </div>
      ) : null}

      {error ? (
        <div className="rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
          {error}
        </div>
      ) : null}

      <div className="grid gap-2 rounded-xl border border-slate-200 bg-white p-4 md:grid-cols-4 xl:grid-cols-7">
        <input
          value={filters.moduleCode}
          onChange={(e) => setFilters((prev) => ({ ...prev, moduleCode: e.target.value }))}
          className="rounded border border-slate-300 px-3 py-2 text-sm"
          placeholder="MODULE (PAYROLL/BANK)"
        />
        <input
          value={filters.objectType}
          onChange={(e) => setFilters((prev) => ({ ...prev, objectType: e.target.value }))}
          className="rounded border border-slate-300 px-3 py-2 text-sm"
          placeholder="OBJECT_TYPE"
        />
        <input
          value={filters.action}
          onChange={(e) => setFilters((prev) => ({ ...prev, action: e.target.value }))}
          className="rounded border border-slate-300 px-3 py-2 text-sm"
          placeholder="ACTION"
        />
        <input
          type="number"
          min={1}
          value={filters.legalEntityId}
          onChange={(e) => setFilters((prev) => ({ ...prev, legalEntityId: e.target.value }))}
          className="rounded border border-slate-300 px-3 py-2 text-sm"
          placeholder="Legal Entity ID"
        />
        <input
          type="date"
          value={filters.dateFrom}
          onChange={(e) => setFilters((prev) => ({ ...prev, dateFrom: e.target.value }))}
          className="rounded border border-slate-300 px-3 py-2 text-sm"
        />
        <input
          type="date"
          value={filters.dateTo}
          onChange={(e) => setFilters((prev) => ({ ...prev, dateTo: e.target.value }))}
          className="rounded border border-slate-300 px-3 py-2 text-sm"
        />
        <div className="flex gap-2">
          <input
            value={filters.q}
            onChange={(e) => setFilters((prev) => ({ ...prev, q: e.target.value }))}
            className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
            placeholder="Ara"
          />
          <button
            type="button"
            onClick={loadRows}
            disabled={loading || !canRead}
            className="rounded bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
          >
            Ara
          </button>
        </div>
      </div>

      <div className="text-xs text-slate-500">Toplam kayit: {total}</div>

      <section className="overflow-hidden rounded-xl border border-slate-200 bg-white">
        {loading ? (
          <div className="px-4 py-3 text-sm text-slate-500">Yukleniyor...</div>
        ) : rows.length === 0 ? (
          <div className="px-4 py-3 text-sm text-slate-500">Kayit yok.</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-3 py-2">Time</th>
                  <th className="px-3 py-2">Module</th>
                  <th className="px-3 py-2">Object</th>
                  <th className="px-3 py-2">Action</th>
                  <th className="px-3 py-2">Actor</th>
                  <th className="px-3 py-2">LE</th>
                  <th className="px-3 py-2">Note</th>
                  <th className="px-3 py-2">Payload</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr key={row.id} className="border-t border-slate-100 align-top">
                    <td className="px-3 py-2 whitespace-nowrap">{formatDateTime(row.acted_at)}</td>
                    <td className="px-3 py-2">{row.module_code}</td>
                    <td className="px-3 py-2">
                      <div>{row.object_type}</div>
                      <div className="text-xs text-slate-500">#{row.object_id}</div>
                    </td>
                    <td className="px-3 py-2">{row.action}</td>
                    <td className="px-3 py-2 text-xs">
                      <div>{row.acted_by_user_name || "-"}</div>
                      <div className="text-slate-500">{row.acted_by_user_email || ""}</div>
                    </td>
                    <td className="px-3 py-2 text-xs">
                      <div>{row.legal_entity_id || "-"}</div>
                      <div className="text-slate-500">{row.legal_entity_code || ""}</div>
                    </td>
                    <td className="px-3 py-2 max-w-[280px] whitespace-pre-wrap">{row.note || "-"}</td>
                    <td className="px-3 py-2">
                      <pre className="max-w-[420px] overflow-auto whitespace-pre-wrap rounded bg-slate-50 p-2 text-xs text-slate-700">
                        {row.payload_json ? JSON.stringify(row.payload_json, null, 2) : "-"}
                      </pre>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}

