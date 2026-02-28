import { useEffect, useMemo, useState } from "react";
import { listCariAudit } from "../../api/cariAudit.js";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";
import { toDayBoundsForAuditFilters } from "./cariAuditUtils.js";

const DEFAULT_FILTERS = {
  legalEntityId: "",
  action: "",
  resourceType: "",
  resourceId: "",
  actorUserId: "",
  requestId: "",
  createdFrom: "",
  createdTo: "",
  includePayload: false,
  limit: 100,
  offset: 0,
};

function toPositiveInt(value, fallback) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function toNonNegativeInt(value, fallback) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : fallback;
}

function normalizeApiError(error, fallback = "Request failed") {
  const message = String(error?.message || error?.response?.data?.message || fallback).trim();
  const requestId = String(error?.requestId || error?.response?.data?.requestId || "").trim();
  return requestId ? `${message} (requestId: ${requestId})` : message;
}

function formatDateTime(value) {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }
  return parsed.toLocaleString();
}

function stringifyPayload(payload) {
  if (payload === undefined) {
    return "";
  }
  try {
    return JSON.stringify(payload, null, 2);
  } catch {
    return String(payload);
  }
}

function getResourceCell(row) {
  const type = String(row?.resourceType || "").trim();
  const id = String(row?.resourceId || "").trim();
  if (!type && !id) {
    return "-";
  }
  if (!id) {
    return type;
  }
  return `${type}:${id}`;
}

async function copyText(value) {
  if (!value) {
    return false;
  }
  if (typeof navigator !== "undefined" && navigator?.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return true;
  }

  if (typeof document !== "undefined") {
    const textarea = document.createElement("textarea");
    textarea.value = value;
    textarea.style.position = "fixed";
    textarea.style.left = "-9999px";
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(textarea);
    return Boolean(ok);
  }

  return false;
}

function buildAuditQuery(filters) {
  const dayBounds = toDayBoundsForAuditFilters(filters);
  const limit = toPositiveInt(filters.limit, 100);
  const offset = toNonNegativeInt(filters.offset, 0);

  return {
    legalEntityId: filters.legalEntityId || undefined,
    action: String(filters.action || "").trim() || undefined,
    resourceType: String(filters.resourceType || "").trim() || undefined,
    resourceId: String(filters.resourceId || "").trim() || undefined,
    actorUserId: filters.actorUserId || undefined,
    requestId: String(filters.requestId || "").trim() || undefined,
    createdFrom: dayBounds.createdFrom || undefined,
    createdTo: dayBounds.createdTo || undefined,
    includePayload: Boolean(filters.includePayload),
    limit,
    offset,
  };
}

export default function CariAuditPage() {
  const { hasPermission } = useAuth();
  const { t, language } = useI18n();
  const l = (en, tr) => (language === "tr" ? tr : en);
  const canReadAudit = hasPermission("cari.audit.read");

  const [filters, setFilters] = useState(DEFAULT_FILTERS);
  const [rows, setRows] = useState([]);
  const [byAction, setByAction] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [info, setInfo] = useState("");
  const [expandedPayloadIds, setExpandedPayloadIds] = useState(() => new Set());

  const limit = toPositiveInt(filters.limit, 100);
  const offset = toNonNegativeInt(filters.offset, 0);
  const hasPrev = offset > 0;
  const hasNext = offset + limit < total;
  const currentPage = useMemo(() => Math.floor(offset / limit) + 1, [offset, limit]);
  const totalPages = useMemo(() => Math.max(1, Math.ceil(total / limit) || 1), [total, limit]);

  async function loadAudit(nextFilters = filters) {
    if (!canReadAudit) {
      setRows([]);
      setByAction([]);
      setTotal(0);
      setError(l("Missing permission: cari.audit.read", "Eksik yetki: cari.audit.read"));
      return;
    }

    setLoading(true);
    setError("");
    setInfo("");
    try {
      const payload = await listCariAudit(buildAuditQuery(nextFilters));
      setRows(Array.isArray(payload?.rows) ? payload.rows : []);
      setByAction(Array.isArray(payload?.byAction) ? payload.byAction : []);
      setTotal(Number(payload?.total || 0));
      setExpandedPayloadIds(new Set());
    } catch (err) {
      setRows([]);
      setByAction([]);
      setTotal(0);
      setError(normalizeApiError(err, l("Failed to load audit logs.", "Denetim kayitlari yuklenemedi.")));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!canReadAudit) {
      return;
    }
    void loadAudit(DEFAULT_FILTERS);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadAudit]);

  function togglePayload(auditLogId) {
    setExpandedPayloadIds((prev) => {
      const next = new Set(prev);
      if (next.has(auditLogId)) {
        next.delete(auditLogId);
      } else {
        next.add(auditLogId);
      }
      return next;
    });
  }

  async function handleCopyRequestId(requestId) {
    if (!requestId) {
      return;
    }
    try {
      const copied = await copyText(String(requestId));
      if (copied) {
        setInfo(l(`requestId copied: ${requestId}`, `requestId kopyalandi: ${requestId}`));
      } else {
        setInfo(
          l(
            "Could not copy requestId on this browser.",
            "Bu tarayicida requestId kopyalanamadi."
          )
        );
      }
    } catch (err) {
      setInfo(normalizeApiError(err, l("Could not copy requestId.", "requestId kopyalanamadi.")));
    }
  }

  function applyFilters(event) {
    event.preventDefault();
    const nextFilters = {
      ...filters,
      limit: toPositiveInt(filters.limit, 100),
      offset: 0,
    };
    setFilters(nextFilters);
    void loadAudit(nextFilters);
  }

  function resetFilters() {
    setFilters(DEFAULT_FILTERS);
    void loadAudit(DEFAULT_FILTERS);
  }

  function goPrevPage() {
    if (!hasPrev || loading) {
      return;
    }
    const next = {
      ...filters,
      offset: Math.max(0, offset - limit),
    };
    setFilters(next);
    void loadAudit(next);
  }

  function goNextPage() {
    if (!hasNext || loading) {
      return;
    }
    const next = {
      ...filters,
      offset: offset + limit,
    };
    setFilters(next);
    void loadAudit(next);
  }

  if (!canReadAudit) {
    return (
      <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
        {l("Missing permission: `cari.audit.read`", "Eksik yetki: `cari.audit.read`")}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h1 className="text-xl font-semibold text-slate-900">{t("cariAudit.title")}</h1>
        <p className="mt-1 text-sm text-slate-600">{t("cariAudit.subtitle")}</p>
        {error ? (
          <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
            {error}
          </div>
        ) : null}
        {info ? (
          <div className="mt-3 rounded-md border border-cyan-200 bg-cyan-50 px-3 py-2 text-sm text-cyan-800">
            {info}
          </div>
        ) : null}
        <form className="mt-4 grid gap-3 md:grid-cols-4" onSubmit={applyFilters}>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            legalEntityId
            <input
              type="number"
              min="1"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={filters.legalEntityId}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, legalEntityId: event.target.value }))
              }
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            action
            <input
              type="text"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={filters.action}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, action: event.target.value }))
              }
              placeholder="cari.settlement.*"
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            resourceType
            <input
              type="text"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={filters.resourceType}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, resourceType: event.target.value }))
              }
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            resourceId
            <input
              type="text"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={filters.resourceId}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, resourceId: event.target.value }))
              }
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            actorUserId
            <input
              type="number"
              min="1"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={filters.actorUserId}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, actorUserId: event.target.value }))
              }
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            requestId
            <input
              type="text"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={filters.requestId}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, requestId: event.target.value }))
              }
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            createdFrom
            <input
              type="date"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={filters.createdFrom}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, createdFrom: event.target.value }))
              }
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            createdTo
            <input
              type="date"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={filters.createdTo}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, createdTo: event.target.value }))
              }
            />
          </label>
          <label className="flex items-center gap-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={Boolean(filters.includePayload)}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, includePayload: event.target.checked }))
              }
            />
            {l("includePayload", "includePayload")}
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            limit
            <input
              type="number"
              min="1"
              max="500"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={filters.limit}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, limit: event.target.value }))
              }
            />
          </label>
          <div className="md:col-span-2 flex items-end gap-2">
            <button
              type="submit"
              className="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
              disabled={loading}
            >
              {loading ? l("Loading...", "Yukleniyor...") : l("Apply Filters", "Filtrele")}
            </button>
            <button
              type="button"
              className="rounded-md border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700"
              onClick={resetFilters}
              disabled={loading}
            >
              {l("Reset", "Sifirla")}
            </button>
          </div>
        </form>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-900">{t("cariAudit.byActionTitle")}</h2>
        <div className="mt-3 grid gap-3 md:grid-cols-4">
          {byAction.map((entry) => (
            <article
              key={`audit-action-${entry.action || "UNKNOWN"}`}
              className="rounded-lg border border-slate-200 px-3 py-2"
            >
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">
                {entry.action || "-"}
              </p>
              <p className="mt-1 text-lg font-semibold text-slate-900">{Number(entry.count || 0)}</p>
            </article>
          ))}
          {byAction.length === 0 ? (
            <p className="text-sm text-slate-500">
              {l("No action summary rows.", "Aksiyon ozeti satiri yok.")}
            </p>
          ) : null}
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white shadow-sm">
        <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-slate-700">
            {l("Audit rows", "Denetim kayitlari")}
          </h2>
          <p className="text-xs text-slate-500">
            {l("Total", "Toplam")}: {total} | {l("Page", "Sayfa")} {currentPage}/{totalPages}
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">{l("createdAt", "olusturmaZamani")}</th>
                <th className="px-3 py-2">{l("action", "aksiyon")}</th>
                <th className="px-3 py-2">{l("resource", "kaynak")}</th>
                <th className="px-3 py-2">{l("actor", "kullanici")}</th>
                <th className="px-3 py-2">{l("requestId", "requestId")}</th>
                <th className="px-3 py-2">{l("payload", "icerik")}</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => {
                const payloadExpanded = expandedPayloadIds.has(row.auditLogId);
                return (
                  <tr key={`audit-row-${row.auditLogId}`} className="border-t border-slate-100 align-top">
                    <td className="px-3 py-2 whitespace-nowrap">{formatDateTime(row.createdAt)}</td>
                    <td className="px-3 py-2">{row.action || "-"}</td>
                    <td className="px-3 py-2">{getResourceCell(row)}</td>
                    <td className="px-3 py-2 text-xs">
                      <div>{row.actorName || "-"}</div>
                      <div className="text-slate-500">{row.actorEmail || ""}</div>
                      <div className="text-slate-500">{l("userId", "kullaniciId")}: {row.actorUserId || "-"}</div>
                    </td>
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-2">
                        <code className="rounded bg-slate-100 px-1.5 py-0.5 text-xs">
                          {row.requestId || "-"}
                        </code>
                        <button
                          type="button"
                          className="rounded border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700 disabled:opacity-60"
                          onClick={() => handleCopyRequestId(row.requestId)}
                          disabled={!row.requestId}
                        >
                          {l("Copy", "Kopyala")}
                        </button>
                      </div>
                    </td>
                    <td className="px-3 py-2">
                      {!filters.includePayload ? (
                        <span className="text-xs text-slate-500">
                          {l(
                            "Enable includePayload to fetch payload.",
                            "Icerigi almak icin includePayload secenegini acin."
                          )}
                        </span>
                      ) : (
                        <div className="space-y-2">
                          <button
                            type="button"
                            className="rounded border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700"
                            onClick={() => togglePayload(row.auditLogId)}
                          >
                            {payloadExpanded
                              ? l("Collapse payload", "Icerigi daralt")
                              : l("Expand payload", "Icerigi genislet")}
                          </button>
                          {payloadExpanded ? (
                            <pre className="max-w-[520px] overflow-auto whitespace-pre-wrap rounded bg-slate-900 p-2 text-xs text-slate-100">
{stringifyPayload(row.payload)}
                            </pre>
                          ) : null}
                        </div>
                      )}
                    </td>
                  </tr>
                );
              })}
              {rows.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-3 py-4 text-slate-500">
                    {loading
                      ? l("Loading audit rows...", "Denetim kayitlari yukleniyor...")
                      : l(
                          "No audit rows found for current filters.",
                          "Secili filtreler icin denetim kaydi bulunamadi."
                        )}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
        <div className="flex items-center justify-between border-t border-slate-200 px-4 py-3">
          <p className="text-xs text-slate-500">
            offset={offset} limit={limit}
          </p>
          <div className="flex gap-2">
            <button
              type="button"
              className="rounded border border-slate-300 px-3 py-1.5 text-sm disabled:opacity-50"
              onClick={goPrevPage}
              disabled={!hasPrev || loading}
            >
              {l("Previous", "Onceki")}
            </button>
            <button
              type="button"
              className="rounded border border-slate-300 px-3 py-1.5 text-sm disabled:opacity-50"
              onClick={goNextPage}
              disabled={!hasNext || loading}
            >
              {l("Next", "Sonraki")}
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
