import { useEffect, useState } from "react";
import { listAuditLogs } from "../../api/rbacAdmin.js";
import { useI18n } from "../../i18n/useI18n.js";

const SCOPE_TYPES = ["", "TENANT", "GROUP", "COUNTRY", "LEGAL_ENTITY", "OPERATING_UNIT"];

export default function RbacAuditLogsPage() {
  const { t } = useI18n();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [rows, setRows] = useState([]);
  const [pagination, setPagination] = useState({
    page: 1,
    pageSize: 50,
    total: 0,
    totalPages: 0,
  });
  const [filters, setFilters] = useState({
    scopeType: "",
    scopeId: "",
    action: "",
    resourceType: "",
  });

  async function loadLogs(nextPage = pagination.page) {
    setLoading(true);
    setError("");
    try {
      const result = await listAuditLogs({
        page: nextPage,
        pageSize: pagination.pageSize,
        scopeType: filters.scopeType || undefined,
        scopeId: filters.scopeId || undefined,
        action: filters.action || undefined,
        resourceType: filters.resourceType || undefined,
      });
      setRows(result?.rows || []);
      setPagination((prev) => ({
        ...prev,
        ...(result?.pagination || {}),
      }));
    } catch (err) {
      setError(err?.response?.data?.message || t("rbacAuditLogs.errors.loadFailed"));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadLogs(1);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function changePage(delta) {
    const nextPage = pagination.page + delta;
    if (nextPage < 1 || (pagination.totalPages && nextPage > pagination.totalPages)) {
      return;
    }
    loadLogs(nextPage);
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">{t("rbacAuditLogs.title")}</h1>
        <p className="mt-1 text-sm text-slate-600">
          {t("rbacAuditLogs.subtitle")}
        </p>
      </div>

      {error && (
        <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
          {error}
        </div>
      )}

      <div className="grid gap-2 rounded-xl border border-slate-200 bg-white p-4 md:grid-cols-5">
        <select
          value={filters.scopeType}
          onChange={(event) =>
            setFilters((prev) => ({ ...prev, scopeType: event.target.value }))
          }
          className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
        >
          {SCOPE_TYPES.map((scopeType) => (
            <option key={scopeType || "ALL"} value={scopeType}>
              {scopeType || t("rbacAuditLogs.filters.allScopeTypes")}
            </option>
          ))}
        </select>
        <input
          type="number"
          min={1}
          value={filters.scopeId}
          onChange={(event) =>
            setFilters((prev) => ({ ...prev, scopeId: event.target.value }))
          }
          className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          placeholder={t("rbacAuditLogs.filters.scopeId")}
        />
        <input
          value={filters.action}
          onChange={(event) =>
            setFilters((prev) => ({ ...prev, action: event.target.value }))
          }
          className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          placeholder={t("rbacAuditLogs.filters.action")}
        />
        <input
          value={filters.resourceType}
          onChange={(event) =>
            setFilters((prev) => ({ ...prev, resourceType: event.target.value }))
          }
          className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          placeholder={t("rbacAuditLogs.filters.resourceType")}
        />
        <button
          type="button"
          onClick={() => loadLogs(1)}
          className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white"
        >
          {t("rbacAuditLogs.filters.apply")}
        </button>
      </div>

      <section className="overflow-hidden rounded-xl border border-slate-200 bg-white">
        <div className="border-b border-slate-200 px-4 py-3 text-sm font-semibold text-slate-700">
          {t("rbacAuditLogs.recordsTitle")}
        </div>
        {loading ? (
          <p className="px-4 py-3 text-sm text-slate-500">{t("rbacAuditLogs.loading")}</p>
        ) : rows.length === 0 ? (
          <p className="px-4 py-3 text-sm text-slate-500">{t("rbacAuditLogs.empty")}</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-4 py-2">{t("rbacAuditLogs.columns.time")}</th>
                  <th className="px-4 py-2">{t("rbacAuditLogs.columns.action")}</th>
                  <th className="px-4 py-2">{t("rbacAuditLogs.columns.resource")}</th>
                  <th className="px-4 py-2">{t("rbacAuditLogs.columns.actor")}</th>
                  <th className="px-4 py-2">{t("rbacAuditLogs.columns.target")}</th>
                  <th className="px-4 py-2">{t("rbacAuditLogs.columns.scope")}</th>
                  <th className="px-4 py-2">{t("rbacAuditLogs.columns.payload")}</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr key={row.id} className="border-t border-slate-100 align-top">
                    <td className="px-4 py-2 whitespace-nowrap">
                      {row.created_at ? new Date(row.created_at).toLocaleString() : "-"}
                    </td>
                    <td className="px-4 py-2">{row.action}</td>
                    <td className="px-4 py-2">
                      {row.resource_type}
                      {row.resource_id ? `:${row.resource_id}` : ""}
                    </td>
                    <td className="px-4 py-2 text-xs">
                      <div>{row.actor_user_name || "-"}</div>
                      <div className="text-slate-500">{row.actor_user_email || ""}</div>
                    </td>
                    <td className="px-4 py-2 text-xs">
                      <div>{row.target_user_name || "-"}</div>
                      <div className="text-slate-500">{row.target_user_email || ""}</div>
                    </td>
                    <td className="px-4 py-2">
                      {row.scope_type ? `${row.scope_type}:${row.scope_id}` : "-"}
                    </td>
                    <td className="px-4 py-2">
                      <pre className="max-w-[420px] overflow-auto whitespace-pre-wrap rounded bg-slate-50 p-2 text-xs text-slate-700">
                        {row.payload_json
                          ? JSON.stringify(row.payload_json, null, 2)
                          : "-"}
                      </pre>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <div className="flex items-center justify-between text-sm text-slate-600">
        <div>
          {t("rbacAuditLogs.pagination.summary", {
            page: pagination.page,
            totalPages: pagination.totalPages || 1,
            total: pagination.total,
          })}
        </div>
        <div className="flex gap-2">
          <button
            type="button"
            onClick={() => changePage(-1)}
            disabled={pagination.page <= 1 || loading}
            className="rounded-lg border border-slate-300 px-3 py-1.5 disabled:opacity-50"
          >
            {t("rbacAuditLogs.pagination.previous")}
          </button>
          <button
            type="button"
            onClick={() => changePage(1)}
            disabled={
              loading ||
              (pagination.totalPages > 0 && pagination.page >= pagination.totalPages)
            }
            className="rounded-lg border border-slate-300 px-3 py-1.5 disabled:opacity-50"
          >
            {t("rbacAuditLogs.pagination.next")}
          </button>
        </div>
      </div>
    </div>
  );
}
