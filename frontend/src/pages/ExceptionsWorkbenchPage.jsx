import { useCallback, useEffect, useMemo, useState } from "react";
import {
  bulkActionExceptionWorkbench,
  claimExceptionWorkbench,
  getExceptionWorkbenchById,
  ignoreExceptionWorkbench,
  listExceptionWorkbench,
  refreshExceptionWorkbench,
  reopenExceptionWorkbench,
  resolveExceptionWorkbench,
} from "../api/exceptionsWorkbench.js";
import { useAuth } from "../auth/useAuth.js";
import { useWorkingContextDefaults } from "../context/useWorkingContextDefaults.js";
import { usePersistedFilters } from "../hooks/usePersistedFilters.js";
import { useToastMessage } from "../hooks/useToastMessage.js";
import { useI18n } from "../i18n/useI18n.js";

function formatDateTime(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleString();
}

function toInt(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
}

function formatCount(value) {
  return toInt(value, 0).toLocaleString();
}

const SLA_SOON_THRESHOLD_MS = 24 * 60 * 60 * 1000;

function parseDateTime(value) {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function formatDurationHours(valueMs) {
  const totalHours = Math.max(1, Math.ceil(Number(valueMs || 0) / (60 * 60 * 1000)));
  if (totalHours < 24) {
    return `${totalHours}h`;
  }
  const days = Math.floor(totalHours / 24);
  const hours = totalHours % 24;
  return hours > 0 ? `${days}d ${hours}h` : `${days}d`;
}

function resolveSlaBadge(row) {
  const status = String(row?.status || "").trim().toUpperCase();
  if (status === "RESOLVED" || status === "IGNORED") {
    return {
      className: "border-slate-200 bg-slate-100 text-slate-700",
      text: "SLA closed",
    };
  }

  const dueAt = parseDateTime(row?.sla_due_at);
  if (!dueAt) {
    return {
      className: "border-slate-200 bg-slate-100 text-slate-700",
      text: "SLA not set",
    };
  }

  const deltaMs = dueAt.getTime() - Date.now();
  if (deltaMs <= 0) {
    return {
      className: "border-red-200 bg-red-50 text-red-700",
      text: `Overdue ${formatDurationHours(Math.abs(deltaMs))}`,
    };
  }
  if (deltaMs <= SLA_SOON_THRESHOLD_MS) {
    return {
      className: "border-amber-200 bg-amber-50 text-amber-700",
      text: `Due soon (${formatDateTime(row?.sla_due_at)})`,
    };
  }
  return {
    className: "border-emerald-200 bg-emerald-50 text-emerald-700",
    text: `Due ${formatDateTime(row?.sla_due_at)}`,
  };
}

function normalizeText(value, fallback = "") {
  const text = String(value || "").trim();
  return text || fallback;
}

function toExceptionId(value) {
  return toInt(value, 0);
}

function uniqPositiveIds(values) {
  const ids = [];
  for (const value of values || []) {
    const parsed = toExceptionId(value);
    if (parsed > 0 && !ids.includes(parsed)) {
      ids.push(parsed);
    }
  }
  return ids;
}

function buildQueueBaseParams(filters) {
  const params = {
    limit: 1,
    offset: 0,
    refresh: 0,
  };
  if (normalizeText(filters.moduleCode)) params.moduleCode = normalizeText(filters.moduleCode).toUpperCase();
  if (normalizeText(filters.legalEntityId)) params.legalEntityId = Number(filters.legalEntityId);
  if (normalizeText(filters.q)) params.q = normalizeText(filters.q);
  if (normalizeText(filters.days)) params.days = Number(filters.days);
  return params;
}

const EXCEPTIONS_CONTEXT_MAPPINGS = [{ stateKey: "legalEntityId" }];
const EXCEPTIONS_FILTERS_STORAGE_SCOPE = "exceptions-workbench.filters";
const EXCEPTIONS_QUEUE_KEYS = {
  ALL: "ALL",
  NEEDS_REVIEW: "NEEDS_REVIEW",
  APPROVAL: "APPROVAL",
  STUCK: "STUCK",
  MINE: "MINE",
  RESOLVED: "RESOLVED",
  CUSTOM: "CUSTOM",
};
const EXCEPTIONS_DEFAULT_FILTERS = {
  queue: EXCEPTIONS_QUEUE_KEYS.NEEDS_REVIEW,
  moduleCode: "",
  status: "OPEN",
  severity: "",
  sortBy: "URGENCY",
  ownerUserId: "",
  legalEntityId: "",
  q: "",
  refresh: true,
  days: "180",
};

export default function ExceptionsWorkbenchPage() {
  const { hasPermission, user } = useAuth();
  const { t } = useI18n();
  const canRead = hasPermission("ops.exceptions.read");
  const canManage = hasPermission("ops.exceptions.manage");
  const currentUserId = toInt(user?.id || user?.userId, 0);

  const [filters, setFilters, resetFilters] = usePersistedFilters(
    EXCEPTIONS_FILTERS_STORAGE_SCOPE,
    () => ({ ...EXCEPTIONS_DEFAULT_FILTERS })
  );
  useWorkingContextDefaults(setFilters, EXCEPTIONS_CONTEXT_MAPPINGS, [
    filters.legalEntityId,
  ]);
  const [rows, setRows] = useState([]);
  const [summary, setSummary] = useState({ by_status: {}, by_module: {}, by_severity: {} });
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [message, setMessage] = useToastMessage("", { toastType: "success" });
  const [queueCounts, setQueueCounts] = useState({
    total: 0,
    byStatus: {},
    stuck: 0,
    mine: 0,
  });
  const [selectedIds, setSelectedIds] = useState([]);
  const [selected, setSelected] = useState(null);
  const [selectedAudit, setSelectedAudit] = useState([]);
  const [resolutionNote, setResolutionNote] = useState("");
  const [bulkBusy, setBulkBusy] = useState("");

  const queryParams = useMemo(() => {
    const params = {
      limit: 100,
      offset: 0,
      refresh: filters.refresh ? 1 : 0,
      sortBy: normalizeText(filters.sortBy, "URGENCY").toUpperCase(),
    };
    if (normalizeText(filters.moduleCode)) params.moduleCode = normalizeText(filters.moduleCode).toUpperCase();
    if (normalizeText(filters.status)) params.status = normalizeText(filters.status).toUpperCase();
    if (normalizeText(filters.severity)) params.severity = normalizeText(filters.severity).toUpperCase();
    if (normalizeText(filters.ownerUserId)) params.ownerUserId = Number(filters.ownerUserId);
    if (normalizeText(filters.legalEntityId)) params.legalEntityId = Number(filters.legalEntityId);
    if (normalizeText(filters.q)) params.q = normalizeText(filters.q);
    if (normalizeText(filters.days)) params.days = Number(filters.days);
    return params;
  }, [filters]);
  const queueBaseParams = useMemo(() => buildQueueBaseParams(filters), [filters]);

  const load = useCallback(async () => {
    if (!canRead) {
      setRows([]);
      setSummary({ by_status: {}, by_module: {}, by_severity: {} });
      setTotal(0);
      setQueueCounts({ total: 0, byStatus: {}, stuck: 0, mine: 0 });
      return;
    }
    setLoading(true);
    setError("");
    try {
      const res = await listExceptionWorkbench(queryParams);
      const [queueSummaryRes, stuckQueueRes, mineQueueRes] = await Promise.all([
        listExceptionWorkbench(queueBaseParams),
        listExceptionWorkbench({
          ...queueBaseParams,
          status: "OPEN",
          severity: "CRITICAL",
        }),
        currentUserId > 0
          ? listExceptionWorkbench({
              ...queueBaseParams,
              status: "IN_REVIEW",
              ownerUserId: currentUserId,
            })
          : Promise.resolve({ total: 0 }),
      ]);
      const nextRows = Array.isArray(res?.rows) ? res.rows : [];
      setRows(nextRows);
      setSelectedIds((prev) => {
        const visibleIdSet = new Set(nextRows.map((row) => toExceptionId(row?.id)).filter((id) => id > 0));
        return uniqPositiveIds(prev).filter((id) => visibleIdSet.has(id));
      });
      setSummary(res?.summary || { by_status: {}, by_module: {}, by_severity: {} });
      setTotal(Number(res?.total || 0));
      setQueueCounts({
        total: toInt(queueSummaryRes?.total, 0),
        byStatus: queueSummaryRes?.summary?.by_status || {},
        stuck: toInt(stuckQueueRes?.total, 0),
        mine: toInt(mineQueueRes?.total, 0),
      });
      if (selected?.id) {
        const exists = nextRows.some((r) => Number(r.id) === Number(selected.id));
        if (!exists) {
          setSelected(null);
          setSelectedAudit([]);
        }
      }
    } catch (err) {
      setError(
        err?.response?.data?.message || t("exceptionsWorkbench.messages.loadFailed", "Exception workbench could not be loaded")
      );
      setRows([]);
      setSelectedIds([]);
      setSummary({ by_status: {}, by_module: {}, by_severity: {} });
      setTotal(0);
      setQueueCounts({ total: 0, byStatus: {}, stuck: 0, mine: 0 });
    } finally {
      setLoading(false);
    }
  }, [canRead, currentUserId, queryParams, queueBaseParams, selected?.id, t]);

  useEffect(() => {
    load();
  }, [load]);

  const queueTabs = useMemo(
    () => [
      {
        key: EXCEPTIONS_QUEUE_KEYS.ALL,
        label: t("exceptionsWorkbench.queues.all", "All"),
        count: toInt(queueCounts.total, 0),
      },
      {
        key: EXCEPTIONS_QUEUE_KEYS.NEEDS_REVIEW,
        label: t("exceptionsWorkbench.queues.needsReview", "Needs Review"),
        count: toInt(queueCounts.byStatus?.OPEN, 0),
      },
      {
        key: EXCEPTIONS_QUEUE_KEYS.APPROVAL,
        label: t("exceptionsWorkbench.queues.approval", "Approval"),
        count: toInt(queueCounts.byStatus?.IN_REVIEW, 0),
      },
      {
        key: EXCEPTIONS_QUEUE_KEYS.STUCK,
        label: t("exceptionsWorkbench.queues.stuck", "Stuck"),
        count: toInt(queueCounts.stuck, 0),
      },
      {
        key: EXCEPTIONS_QUEUE_KEYS.MINE,
        label: t("exceptionsWorkbench.queues.mine", "Mine"),
        count: toInt(queueCounts.mine, 0),
      },
      {
        key: EXCEPTIONS_QUEUE_KEYS.RESOLVED,
        label: t("exceptionsWorkbench.queues.resolved", "Resolved"),
        count: toInt(queueCounts.byStatus?.RESOLVED, 0),
      },
    ],
    [queueCounts, t]
  );
  const visibleExceptionIds = useMemo(
    () => uniqPositiveIds(rows.map((row) => row?.id)),
    [rows]
  );
  const selectedIdSet = useMemo(() => new Set(selectedIds), [selectedIds]);
  const allVisibleSelected =
    visibleExceptionIds.length > 0 &&
    visibleExceptionIds.every((id) => selectedIdSet.has(id));
  const selectedCount = selectedIds.length;

  function applyQueue(queueKey) {
    setFilters((state) => {
      if (queueKey === EXCEPTIONS_QUEUE_KEYS.ALL) {
        return {
          ...state,
          queue: queueKey,
          status: "",
          severity: "",
          ownerUserId: "",
        };
      }
      if (queueKey === EXCEPTIONS_QUEUE_KEYS.NEEDS_REVIEW) {
        return {
          ...state,
          queue: queueKey,
          status: "OPEN",
          severity: "",
          ownerUserId: "",
        };
      }
      if (queueKey === EXCEPTIONS_QUEUE_KEYS.APPROVAL) {
        return {
          ...state,
          queue: queueKey,
          status: "IN_REVIEW",
          severity: "",
          ownerUserId: "",
        };
      }
      if (queueKey === EXCEPTIONS_QUEUE_KEYS.STUCK) {
        return {
          ...state,
          queue: queueKey,
          status: "OPEN",
          severity: "CRITICAL",
          ownerUserId: "",
        };
      }
      if (queueKey === EXCEPTIONS_QUEUE_KEYS.MINE) {
        return {
          ...state,
          queue: queueKey,
          status: "IN_REVIEW",
          severity: "",
          ownerUserId: currentUserId > 0 ? String(currentUserId) : "",
        };
      }
      if (queueKey === EXCEPTIONS_QUEUE_KEYS.RESOLVED) {
        return {
          ...state,
          queue: queueKey,
          status: "RESOLVED",
          severity: "",
          ownerUserId: "",
        };
      }
      return {
        ...state,
        queue: EXCEPTIONS_QUEUE_KEYS.CUSTOM,
      };
    });
  }

  function toggleRowSelection(exceptionId) {
    const id = toExceptionId(exceptionId);
    if (!id) return;
    setSelectedIds((prev) => {
      const normalized = uniqPositiveIds(prev);
      if (normalized.includes(id)) {
        return normalized.filter((value) => value !== id);
      }
      return [...normalized, id];
    });
  }

  function toggleSelectVisible() {
    setSelectedIds((prev) => {
      const normalized = uniqPositiveIds(prev);
      if (allVisibleSelected) {
        return normalized.filter((id) => !visibleExceptionIds.includes(id));
      }
      return uniqPositiveIds([...normalized, ...visibleExceptionIds]);
    });
  }

  function clearSelection() {
    setSelectedIds([]);
  }

  async function loadDetail(exceptionId) {
    if (!canRead || !exceptionId) return;
    setBusy(`detail-${exceptionId}`);
    setError("");
    try {
      const res = await getExceptionWorkbenchById(exceptionId);
      setSelected(res?.row || null);
      setSelectedAudit(Array.isArray(res?.audit) ? res.audit : []);
    } catch (err) {
      setError(
        err?.response?.data?.message || t("exceptionsWorkbench.messages.detailLoadFailed", "Exception detail could not be loaded")
      );
    } finally {
      setBusy("");
    }
  }

  async function handleManualRefresh() {
    if (!canRead) return;
    setBusy("manual-refresh");
    setError("");
    setMessage("");
    try {
      const payload = {};
      if (normalizeText(filters.legalEntityId)) payload.legalEntityId = Number(filters.legalEntityId);
      if (normalizeText(filters.days)) payload.days = Number(filters.days);
      await refreshExceptionWorkbench(payload);
      setMessage(t("exceptionsWorkbench.messages.workbenchRefreshed", "Workbench refreshed."));
      await load();
    } catch (err) {
      setError(err?.response?.data?.message || t("exceptionsWorkbench.messages.refreshFailed", "Refresh failed"));
    } finally {
      setBusy("");
    }
  }

  async function runAction(action, exceptionId) {
    if (!canManage || !exceptionId) return;
    setBusy(`${action}-${exceptionId}`);
    setError("");
    setMessage("");
    try {
      if (action === "claim") {
        await claimExceptionWorkbench(exceptionId, {});
      } else if (action === "resolve") {
        await resolveExceptionWorkbench(exceptionId, {
          resolutionAction: "MANUAL_RESOLVE",
          resolutionNote: normalizeText(resolutionNote) || null,
        });
      } else if (action === "ignore") {
        await ignoreExceptionWorkbench(exceptionId, {
          resolutionAction: "MANUAL_IGNORE",
          resolutionNote: normalizeText(resolutionNote) || null,
        });
      } else if (action === "reopen") {
        await reopenExceptionWorkbench(exceptionId, {
          resolutionNote: normalizeText(resolutionNote) || null,
        });
      }
      setResolutionNote("");
      await load();
      if (selected?.id && Number(selected.id) === Number(exceptionId)) {
        await loadDetail(exceptionId);
      }
      setMessage(t("exceptionsWorkbench.messages.actionApplied", "Action {{action}} applied.", { action: action.toUpperCase() }));
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          t("exceptionsWorkbench.messages.actionFailed", "Action {{action}} failed", { action })
      );
    } finally {
      setBusy("");
    }
  }

  async function runBulkAction(action) {
    const ids = uniqPositiveIds(selectedIds);
    if (!canManage || ids.length === 0) return;
    setBulkBusy(action);
    setError("");
    setMessage("");
    try {
      const payload = {
        action,
        exceptionIds: ids,
        continueOnError: true,
      };
      if (action === "claim") {
        const note = normalizeText(resolutionNote);
        if (note) payload.note = note;
      } else if (action === "resolve") {
        payload.resolutionAction = "MANUAL_RESOLVE";
      } else if (action === "ignore") {
        payload.resolutionAction = "MANUAL_IGNORE";
      }
      const note = normalizeText(resolutionNote);
      if (note && action !== "claim") {
        payload.resolutionNote = note;
      }

      const result = await bulkActionExceptionWorkbench(payload);
      const requested = toInt(result?.requested, ids.length);
      const succeeded = toInt(result?.succeeded, 0);
      const failed = toInt(result?.failed, 0);
      const failedIds = uniqPositiveIds(
        Array.isArray(result?.results)
          ? result.results
              .filter((entry) => !entry?.ok)
              .map((entry) => entry?.exception_id)
          : []
      );
      const firstFailure = Array.isArray(result?.results)
        ? result.results.find((entry) => !entry?.ok)
        : null;

      await load();
      if (selected?.id && ids.includes(toExceptionId(selected.id))) {
        await loadDetail(selected.id);
      }
      setResolutionNote("");
      if (failed > 0) {
        const fallbackMessage = t(
          "exceptionsWorkbench.messages.bulkActionPartial",
          "Bulk action {{action}} finished with partial success ({{succeeded}}/{{total}} succeeded, {{failed}} failed).",
          {
            action: action.toUpperCase(),
            succeeded,
            failed,
            total: requested,
          }
        );
        const failureDetail = normalizeText(firstFailure?.message);
        setError(failureDetail ? `${fallbackMessage} ${failureDetail}` : fallbackMessage);
        setSelectedIds((prev) => uniqPositiveIds(prev).filter((id) => failedIds.includes(id)));
      } else {
        setSelectedIds([]);
        setMessage(
          t("exceptionsWorkbench.messages.bulkActionApplied", "Bulk action {{action}} applied to {{count}} exceptions.", {
            action: action.toUpperCase(),
            count: succeeded,
          })
        );
      }
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          t("exceptionsWorkbench.messages.bulkActionFailed", "Bulk action {{action}} failed.", {
            action: action.toUpperCase(),
          })
      );
    } finally {
      setBulkBusy("");
    }
  }

  return (
    <div className="space-y-4">
      <section className="rounded border bg-white p-4">
        <div className="mb-2 flex items-center gap-2">
          <h1 className="text-lg font-semibold">{t("exceptionsWorkbench.title", "Unified Exception Workbench (H06)")}</h1>
          <span className="rounded border px-2 py-0.5 text-xs text-slate-600">
            {t("exceptionsWorkbench.total", "Total: {{total}}", { total })}
          </span>
        </div>
        {!canRead ? (
          <div className="text-sm text-slate-500">
            {t("exceptionsWorkbench.messages.missingReadPermission", "Missing permission:")} <code>ops.exceptions.read</code>
          </div>
        ) : (
          <>
            <div className="mb-3 flex flex-wrap gap-2">
              {queueTabs.map((tab) => {
                const isActive = (filters.queue || EXCEPTIONS_QUEUE_KEYS.CUSTOM) === tab.key;
                return (
                  <button
                    key={tab.key}
                    type="button"
                    onClick={() => applyQueue(tab.key)}
                    className={`rounded border px-3 py-1.5 text-sm ${
                      isActive
                        ? "border-cyan-300 bg-cyan-50 text-cyan-900"
                        : "border-slate-300 bg-white text-slate-700"
                    }`}
                  >
                    <span className="font-medium">{tab.label}</span>
                    <span className="ml-2 rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-600">
                      {formatCount(tab.count)}
                    </span>
                  </button>
                );
              })}
            </div>
            <div className="grid gap-2 md:grid-cols-4">
              <label className="text-sm">
                <div className="mb-1 text-slate-600">{t("exceptionsWorkbench.filters.module", "Module")}</div>
                <select
                  className="w-full rounded border px-2 py-1"
                  value={filters.moduleCode}
                  onChange={(e) => setFilters((s) => ({ ...s, moduleCode: e.target.value }))}
                >
                  <option value="">{t("exceptionsWorkbench.filters.all", "All")}</option>
                  <option value="BANK">BANK</option>
                  <option value="PAYROLL">PAYROLL</option>
                </select>
              </label>
              <label className="text-sm">
                <div className="mb-1 text-slate-600">{t("exceptionsWorkbench.filters.status", "Status")}</div>
                <select
                  className="w-full rounded border px-2 py-1"
                  value={filters.status}
                  onChange={(e) =>
                    setFilters((s) => ({
                      ...s,
                      queue: EXCEPTIONS_QUEUE_KEYS.CUSTOM,
                      status: e.target.value,
                    }))
                  }
                >
                  <option value="">{t("exceptionsWorkbench.filters.all", "All")}</option>
                  <option value="OPEN">OPEN</option>
                  <option value="IN_REVIEW">IN_REVIEW</option>
                  <option value="RESOLVED">RESOLVED</option>
                  <option value="IGNORED">IGNORED</option>
                </select>
              </label>
              <label className="text-sm">
                <div className="mb-1 text-slate-600">{t("exceptionsWorkbench.filters.severity", "Severity")}</div>
                <select
                  className="w-full rounded border px-2 py-1"
                  value={filters.severity}
                  onChange={(e) =>
                    setFilters((s) => ({
                      ...s,
                      queue: EXCEPTIONS_QUEUE_KEYS.CUSTOM,
                      severity: e.target.value,
                    }))
                  }
                >
                  <option value="">{t("exceptionsWorkbench.filters.all", "All")}</option>
                  <option value="CRITICAL">CRITICAL</option>
                  <option value="HIGH">HIGH</option>
                  <option value="MEDIUM">MEDIUM</option>
                  <option value="LOW">LOW</option>
                </select>
              </label>
              <label className="text-sm">
                <div className="mb-1 text-slate-600">{t("exceptionsWorkbench.filters.sort", "Sort")}</div>
                <select
                  className="w-full rounded border px-2 py-1"
                  value={normalizeText(filters.sortBy, "URGENCY").toUpperCase()}
                  onChange={(e) => setFilters((s) => ({ ...s, sortBy: e.target.value }))}
                >
                  <option value="URGENCY">{t("exceptionsWorkbench.sort.urgency", "Urgency (SLA)")}</option>
                  <option value="UPDATED_DESC">{t("exceptionsWorkbench.sort.updatedDesc", "Last updated (newest)")}</option>
                  <option value="LAST_SEEN_ASC">{t("exceptionsWorkbench.sort.lastSeenAsc", "Last seen (oldest)")}</option>
                  <option value="PRIORITY">{t("exceptionsWorkbench.sort.priority", "Priority (status + severity)")}</option>
                </select>
              </label>
              <label className="text-sm">
                <div className="mb-1 text-slate-600">{t("exceptionsWorkbench.filters.legalEntityId", "Legal entity ID")}</div>
                <input
                  className="w-full rounded border px-2 py-1"
                  value={filters.legalEntityId}
                  onChange={(e) => setFilters((s) => ({ ...s, legalEntityId: e.target.value }))}
                  placeholder={t("exceptionsWorkbench.placeholders.optional", "optional")}
                />
              </label>
              <label className="text-sm">
                <div className="mb-1 text-slate-600">{t("exceptionsWorkbench.filters.search", "Search")}</div>
                <input
                  className="w-full rounded border px-2 py-1"
                  value={filters.q}
                  onChange={(e) => setFilters((s) => ({ ...s, q: e.target.value }))}
                  placeholder={t("exceptionsWorkbench.placeholders.search", "title/source/note")}
                />
              </label>
              <label className="text-sm">
                <div className="mb-1 text-slate-600">{t("exceptionsWorkbench.filters.days", "Days")}</div>
                <input
                  className="w-full rounded border px-2 py-1"
                  value={filters.days}
                  onChange={(e) => setFilters((s) => ({ ...s, days: e.target.value }))}
                  placeholder={t("exceptionsWorkbench.placeholders.days", "180")}
                />
              </label>
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={Boolean(filters.refresh)}
                  onChange={(e) => setFilters((s) => ({ ...s, refresh: e.target.checked }))}
                />
                {t("exceptionsWorkbench.filters.autoRefresh", "Auto-refresh sources on list")}
              </label>
              <div className="flex items-end gap-2">
                <button type="button" className="rounded border px-3 py-1 text-sm" onClick={load} disabled={loading}>
                  {loading ? t("exceptionsWorkbench.actions.loading", "Loading...") : t("exceptionsWorkbench.actions.applyFilters", "Apply Filters")}
                </button>
                <button
                  type="button"
                  className="rounded border px-3 py-1 text-sm"
                  onClick={resetFilters}
                  disabled={loading}
                >
                  {t("exceptionsWorkbench.actions.resetFilters", "Reset")}
                </button>
                <button
                  type="button"
                  className="rounded border px-3 py-1 text-sm"
                  onClick={handleManualRefresh}
                  disabled={busy === "manual-refresh"}
                >
                  {busy === "manual-refresh"
                    ? t("exceptionsWorkbench.actions.refreshing", "Refreshing...")
                    : t("exceptionsWorkbench.actions.manualRefresh", "Manual Refresh")}
                </button>
              </div>
            </div>
            <div className="mt-3 grid gap-2 text-xs md:grid-cols-3">
              <div className="rounded border bg-slate-50 p-2">
                <div className="font-medium">{t("exceptionsWorkbench.summary.byStatus", "By Status")}</div>
                <pre className="mt-1 overflow-auto">{JSON.stringify(summary.by_status || {}, null, 2)}</pre>
              </div>
              <div className="rounded border bg-slate-50 p-2">
                <div className="font-medium">{t("exceptionsWorkbench.summary.byModule", "By Module")}</div>
                <pre className="mt-1 overflow-auto">{JSON.stringify(summary.by_module || {}, null, 2)}</pre>
              </div>
              <div className="rounded border bg-slate-50 p-2">
                <div className="font-medium">{t("exceptionsWorkbench.summary.bySeverity", "By Severity")}</div>
                <pre className="mt-1 overflow-auto">{JSON.stringify(summary.by_severity || {}, null, 2)}</pre>
              </div>
            </div>
          </>
        )}
      </section>

      {error ? <div className="rounded border border-red-200 bg-red-50 p-3 text-sm text-red-700">{error}</div> : null}
      {message ? <div className="rounded border border-emerald-200 bg-emerald-50 p-3 text-sm text-emerald-700">{message}</div> : null}

      <section className="rounded border bg-white p-4">
        <h2 className="mb-2 font-medium">{t("exceptionsWorkbench.sections.exceptions", "Exceptions")}</h2>
        {!canRead ? null : loading ? (
          <div className="text-sm text-slate-500">{t("exceptionsWorkbench.actions.loading", "Loading...")}</div>
        ) : rows.length === 0 ? (
          <div className="text-sm text-slate-500">{t("exceptionsWorkbench.messages.empty", "No exceptions found for current filters.")}</div>
        ) : (
          <div className="space-y-2">
            {canManage ? (
              <div className="flex flex-wrap items-center gap-2 rounded border border-slate-200 bg-slate-50 p-2 text-xs">
                <label className="flex items-center gap-2 text-slate-700">
                  <input type="checkbox" checked={allVisibleSelected} onChange={toggleSelectVisible} />
                  {t("exceptionsWorkbench.bulk.selectVisible", "Select visible")}
                </label>
                <span className="rounded border border-slate-300 bg-white px-2 py-1 text-slate-700">
                  {t("exceptionsWorkbench.bulk.selectedCount", "Selected: {{count}}", {
                    count: formatCount(selectedCount),
                  })}
                </span>
                <button
                  type="button"
                  className="rounded border border-slate-300 bg-white px-2 py-1"
                  onClick={clearSelection}
                  disabled={selectedCount === 0 || Boolean(bulkBusy)}
                >
                  {t("exceptionsWorkbench.bulk.clearSelection", "Clear")}
                </button>
                <button
                  type="button"
                  className="rounded border border-slate-300 bg-white px-2 py-1"
                  onClick={() => runBulkAction("claim")}
                  disabled={selectedCount === 0 || Boolean(bulkBusy)}
                >
                  {bulkBusy === "claim"
                    ? t("exceptionsWorkbench.actions.loading", "Loading...")
                    : t("exceptionsWorkbench.bulk.claimSelected", "Claim Selected")}
                </button>
                <button
                  type="button"
                  className="rounded border border-slate-300 bg-white px-2 py-1"
                  onClick={() => runBulkAction("resolve")}
                  disabled={selectedCount === 0 || Boolean(bulkBusy)}
                >
                  {bulkBusy === "resolve"
                    ? t("exceptionsWorkbench.actions.loading", "Loading...")
                    : t("exceptionsWorkbench.bulk.resolveSelected", "Resolve Selected")}
                </button>
                <button
                  type="button"
                  className="rounded border border-slate-300 bg-white px-2 py-1"
                  onClick={() => runBulkAction("ignore")}
                  disabled={selectedCount === 0 || Boolean(bulkBusy)}
                >
                  {bulkBusy === "ignore"
                    ? t("exceptionsWorkbench.actions.loading", "Loading...")
                    : t("exceptionsWorkbench.bulk.ignoreSelected", "Ignore Selected")}
                </button>
                <button
                  type="button"
                  className="rounded border border-slate-300 bg-white px-2 py-1"
                  onClick={() => runBulkAction("reopen")}
                  disabled={selectedCount === 0 || Boolean(bulkBusy)}
                >
                  {bulkBusy === "reopen"
                    ? t("exceptionsWorkbench.actions.loading", "Loading...")
                    : t("exceptionsWorkbench.bulk.reopenSelected", "Reopen Selected")}
                </button>
              </div>
            ) : null}
            {rows.map((row) => {
              const slaBadge = resolveSlaBadge(row);
              const rowId = toExceptionId(row.id);
              const isSelected = selectedIdSet.has(rowId);
              return (
                <div key={row.id} className="rounded border p-3 text-sm">
                  <div className="flex flex-wrap items-center gap-2">
                    {canManage ? (
                      <label className="flex items-center gap-1 text-xs text-slate-600">
                        <input
                          type="checkbox"
                          checked={isSelected}
                          onChange={() => toggleRowSelection(rowId)}
                          disabled={Boolean(bulkBusy)}
                          aria-label={t("exceptionsWorkbench.bulk.selectRow", "Select exception row")}
                        />
                        {t("exceptionsWorkbench.bulk.select", "Select")}
                      </label>
                    ) : null}
                    <span className="rounded border px-1 text-xs">{row.module_code}</span>
                    <span className="rounded border px-1 text-xs">{row.severity}</span>
                    <span className="rounded border px-1 text-xs">{row.status}</span>
                    <span className={`rounded border px-1 text-xs ${slaBadge.className}`}>{slaBadge.text}</span>
                    <span className="rounded border px-1 text-xs">{row.exception_type}</span>
                    <div className="ml-auto text-xs text-slate-500">
                      {t("exceptionsWorkbench.labels.lastSeen", "last seen:")} {formatDateTime(row.last_seen_at)}
                    </div>
                  </div>
                  <div className="mt-1 font-medium">{row.title}</div>
                  <div className="text-xs text-slate-600">
                    {t("exceptionsWorkbench.labels.source", "source:")} {row.source_type} / {row.source_key}
                  </div>
                  {row.description ? <div className="mt-1 text-xs text-slate-600">{row.description}</div> : null}
                  <div className="mt-2 flex flex-wrap gap-2">
                    <button
                      type="button"
                      className="rounded border px-2 py-1 text-xs"
                      onClick={() => loadDetail(row.id)}
                      disabled={busy === `detail-${row.id}`}
                    >
                      {busy === `detail-${row.id}` ? t("exceptionsWorkbench.actions.loading", "Loading...") : t("exceptionsWorkbench.actions.details", "Details")}
                    </button>
                    {canManage ? (
                      <>
                        <button
                          type="button"
                          className="rounded border px-2 py-1 text-xs"
                          onClick={() => runAction("claim", row.id)}
                          disabled={busy === `claim-${row.id}` || Boolean(bulkBusy)}
                        >
                          {busy === `claim-${row.id}` ? "..." : t("exceptionsWorkbench.actions.claim", "Claim")}
                        </button>
                        <button
                          type="button"
                          className="rounded border px-2 py-1 text-xs"
                          onClick={() => runAction("resolve", row.id)}
                          disabled={busy === `resolve-${row.id}` || Boolean(bulkBusy)}
                        >
                          {busy === `resolve-${row.id}` ? "..." : t("exceptionsWorkbench.actions.resolve", "Resolve")}
                        </button>
                        <button
                          type="button"
                          className="rounded border px-2 py-1 text-xs"
                          onClick={() => runAction("ignore", row.id)}
                          disabled={busy === `ignore-${row.id}` || Boolean(bulkBusy)}
                        >
                          {busy === `ignore-${row.id}` ? "..." : t("exceptionsWorkbench.actions.ignore", "Ignore")}
                        </button>
                        <button
                          type="button"
                          className="rounded border px-2 py-1 text-xs"
                          onClick={() => runAction("reopen", row.id)}
                          disabled={busy === `reopen-${row.id}` || Boolean(bulkBusy)}
                        >
                          {busy === `reopen-${row.id}` ? "..." : t("exceptionsWorkbench.actions.reopen", "Reopen")}
                        </button>
                      </>
                    ) : null}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </section>

      <section className="rounded border bg-white p-4">
        <h2 className="mb-2 font-medium">{t("exceptionsWorkbench.sections.resolutionNote", "Resolution Note")}</h2>
        <textarea
          className="min-h-[80px] w-full rounded border px-2 py-1 text-sm"
          value={resolutionNote}
          onChange={(e) => setResolutionNote(e.target.value)}
          placeholder={t("exceptionsWorkbench.placeholders.resolutionNote", "Used by resolve/ignore/reopen actions")}
        />
      </section>

      <section className="rounded border bg-white p-4">
        <h2 className="mb-2 font-medium">{t("exceptionsWorkbench.sections.selectedException", "Selected Exception")}</h2>
        {!selected ? (
          <div className="text-sm text-slate-500">{t("exceptionsWorkbench.messages.selectRow", "Select an exception row and click Details.")}</div>
        ) : (
          <pre className="overflow-auto rounded bg-slate-50 p-3 text-xs">{JSON.stringify(selected, null, 2)}</pre>
        )}
        <h3 className="mt-3 font-medium">{t("exceptionsWorkbench.sections.auditTrail", "Audit Trail")}</h3>
        {selectedAudit.length === 0 ? (
          <div className="text-sm text-slate-500">{t("exceptionsWorkbench.messages.noAudit", "No audit entries.")}</div>
        ) : (
          <pre className="overflow-auto rounded bg-slate-50 p-3 text-xs">{JSON.stringify(selectedAudit, null, 2)}</pre>
        )}
      </section>
    </div>
  );
}
