import { useEffect, useMemo, useState } from "react";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";
import {
  createExportSnapshot,
  createRetentionPolicy,
  getExportSnapshot,
  getRetentionRun,
  listExportSnapshots,
  listRetentionPolicies,
  listRetentionRuns,
  runRetentionPolicy,
  updateRetentionPolicy,
} from "../../api/retentionAdmin.js";

const DATASET_OPTIONS = [
  { code: "PAYROLL_PROVIDER_IMPORT_RAW", actions: ["MASK", "PURGE"] },
  { code: "BANK_FEED_RAW_PAYLOAD", actions: ["MASK", "PURGE"] },
  { code: "BANK_WEBHOOK_RAW_PAYLOAD", actions: ["MASK", "PURGE"] },
  { code: "JOB_EXECUTION_LOG", actions: ["MASK", "PURGE"] },
  { code: "SENSITIVE_DATA_AUDIT", actions: ["ARCHIVE"] },
];

function pretty(value) {
  return JSON.stringify(value ?? null, null, 2);
}

function asIntOrEmpty(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) return "";
  return parsed;
}

function statusBadge(status) {
  const s = String(status || "").toUpperCase();
  if (s === "ACTIVE" || s === "COMPLETED" || s === "READY") return "text-emerald-700";
  if (s === "PAUSED" || s === "PARTIAL") return "text-amber-700";
  if (s === "FAILED" || s === "DISABLED") return "text-rose-700";
  return "text-slate-700";
}

export default function RetentionAdminPage() {
  const { hasPermission } = useAuth();
  const { t } = useI18n();
  const canReadRetention = hasPermission("ops.retention.read");
  const canManageRetention = hasPermission("ops.retention.manage");
  const canReadSnapshots = hasPermission("ops.export_snapshot.read");
  const canCreateSnapshots = hasPermission("ops.export_snapshot.create");

  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const [policyFilters, setPolicyFilters] = useState({
    legalEntityId: "",
    datasetCode: "",
    status: "",
    q: "",
  });
  const [policies, setPolicies] = useState([]);
  const [policiesTotal, setPoliciesTotal] = useState(0);
  const [policiesLoading, setPoliciesLoading] = useState(false);

  const [policyForm, setPolicyForm] = useState({
    policyCode: "",
    policyName: "",
    datasetCode: "PAYROLL_PROVIDER_IMPORT_RAW",
    actionCode: "MASK",
    retentionDays: "90",
    legalEntityId: "",
    status: "ACTIVE",
  });
  const [creatingPolicy, setCreatingPolicy] = useState(false);
  const [updatingPolicyId, setUpdatingPolicyId] = useState(null);
  const [runningPolicyKey, setRunningPolicyKey] = useState("");

  const [runFilters, setRunFilters] = useState({
    legalEntityId: "",
    status: "",
    policyId: "",
    dateFrom: "",
    dateTo: "",
  });
  const [runs, setRuns] = useState([]);
  const [runsTotal, setRunsTotal] = useState(0);
  const [runsLoading, setRunsLoading] = useState(false);
  const [selectedRun, setSelectedRun] = useState(null);

  const [snapshotFilters, setSnapshotFilters] = useState({
    legalEntityId: "",
    payrollPeriodCloseId: "",
  });
  const [snapshots, setSnapshots] = useState([]);
  const [snapshotsTotal, setSnapshotsTotal] = useState(0);
  const [snapshotsLoading, setSnapshotsLoading] = useState(false);
  const [selectedSnapshot, setSelectedSnapshot] = useState(null);

  const [snapshotForm, setSnapshotForm] = useState({
    payrollPeriodCloseId: "",
    idempotencyKey: "",
  });
  const [creatingSnapshot, setCreatingSnapshot] = useState(false);

  const currentDataset = useMemo(
    () => DATASET_OPTIONS.find((item) => item.code === policyForm.datasetCode) || DATASET_OPTIONS[0],
    [policyForm.datasetCode]
  );

  useEffect(() => {
    if (!currentDataset.actions.includes(policyForm.actionCode)) {
      setPolicyForm((prev) => ({ ...prev, actionCode: currentDataset.actions[0] }));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentDataset.code]);

  async function loadPolicies() {
    if (!canReadRetention) return;
    setPoliciesLoading(true);
    setError("");
    try {
      const res = await listRetentionPolicies({
        limit: 100,
        offset: 0,
        legalEntityId: policyFilters.legalEntityId || undefined,
        datasetCode: policyFilters.datasetCode || undefined,
        status: policyFilters.status || undefined,
        q: policyFilters.q || undefined,
      });
      setPolicies(res?.rows || []);
      setPoliciesTotal(Number(res?.total || 0));
    } catch (err) {
      setPolicies([]);
      setPoliciesTotal(0);
      setError(err?.response?.data?.message || t("retentionAdmin.messages.policiesLoadFailed", "Retention policies could not be loaded"));
    } finally {
      setPoliciesLoading(false);
    }
  }

  async function loadRuns() {
    if (!canReadRetention) return;
    setRunsLoading(true);
    setError("");
    try {
      const res = await listRetentionRuns({
        limit: 100,
        offset: 0,
        legalEntityId: runFilters.legalEntityId || undefined,
        status: runFilters.status || undefined,
        policyId: runFilters.policyId || undefined,
        dateFrom: runFilters.dateFrom || undefined,
        dateTo: runFilters.dateTo || undefined,
      });
      setRuns(res?.rows || []);
      setRunsTotal(Number(res?.total || 0));
    } catch (err) {
      setRuns([]);
      setRunsTotal(0);
      setError(err?.response?.data?.message || t("retentionAdmin.messages.runsLoadFailed", "Retention runs could not be loaded"));
    } finally {
      setRunsLoading(false);
    }
  }

  async function loadSnapshots() {
    if (!canReadSnapshots) return;
    setSnapshotsLoading(true);
    setError("");
    try {
      const res = await listExportSnapshots({
        limit: 100,
        offset: 0,
        legalEntityId: snapshotFilters.legalEntityId || undefined,
        payrollPeriodCloseId: snapshotFilters.payrollPeriodCloseId || undefined,
      });
      setSnapshots(res?.rows || []);
      setSnapshotsTotal(Number(res?.total || 0));
    } catch (err) {
      setSnapshots([]);
      setSnapshotsTotal(0);
      setError(err?.response?.data?.message || t("retentionAdmin.messages.snapshotsLoadFailed", "Export snapshots could not be loaded"));
    } finally {
      setSnapshotsLoading(false);
    }
  }

  useEffect(() => {
    loadPolicies();
    loadRuns();
    loadSnapshots();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadRetention, canReadSnapshots]);

  async function handleCreatePolicy(e) {
    e.preventDefault();
    if (!canManageRetention) return;
    setCreatingPolicy(true);
    setError("");
    setMessage("");

    try {
      await createRetentionPolicy({
        policyCode: policyForm.policyCode,
        policyName: policyForm.policyName,
        datasetCode: policyForm.datasetCode,
        actionCode: policyForm.actionCode,
        retentionDays: Number(policyForm.retentionDays),
        legalEntityId: asIntOrEmpty(policyForm.legalEntityId) || undefined,
        status: policyForm.status,
      });
      setMessage(t("retentionAdmin.messages.policyCreated", "Retention policy created."));
      setPolicyForm((prev) => ({
        ...prev,
        policyCode: "",
        policyName: "",
      }));
      await loadPolicies();
    } catch (err) {
      setError(err?.response?.data?.message || t("retentionAdmin.messages.policyCreateFailed", "Retention policy could not be created"));
    } finally {
      setCreatingPolicy(false);
    }
  }

  async function handleTogglePolicyStatus(policy) {
    if (!canManageRetention || !policy?.id) return;
    setUpdatingPolicyId(policy.id);
    setError("");
    setMessage("");
    const nextStatus = String(policy.status || "").toUpperCase() === "ACTIVE" ? "PAUSED" : "ACTIVE";

    try {
      await updateRetentionPolicy(policy.id, { status: nextStatus });
      setMessage(
        t("retentionAdmin.messages.policyStatusUpdated", "Policy {{code}} status updated to {{status}}.", {
          code: policy.policy_code,
          status: nextStatus,
        })
      );
      await loadPolicies();
    } catch (err) {
      setError(err?.response?.data?.message || t("retentionAdmin.messages.policyStatusUpdateFailed", "Policy status could not be updated"));
    } finally {
      setUpdatingPolicyId(null);
    }
  }

  async function handleRunPolicy(policy, asyncMode) {
    if (!canManageRetention || !policy?.id) return;
    const runKey = `${policy.id}:${asyncMode ? "async" : "sync"}`;
    setRunningPolicyKey(runKey);
    setError("");
    setMessage("");

    try {
      const res = await runRetentionPolicy(
        policy.id,
        {
          runIdempotencyKey: `${policy.policy_code}-${Date.now()}`,
          triggerMode: asyncMode ? "JOB" : "MANUAL",
        },
        { asyncMode }
      );

      if (asyncMode) {
        setMessage(
          t("retentionAdmin.messages.runQueued", "Retention run queued as job #{{id}}.", {
            id: res?.job?.id || "?",
          })
        );
      } else {
        setMessage(
          t("retentionAdmin.messages.runCompleted", "Retention run completed (#{{id}}).", {
            id: res?.row?.id || "?",
          })
        );
      }
      await loadRuns();
      await loadPolicies();
    } catch (err) {
      setError(err?.response?.data?.message || t("retentionAdmin.messages.runFailed", "Retention run failed"));
    } finally {
      setRunningPolicyKey("");
    }
  }

  async function handleSelectRun(runId) {
    if (!runId) return;
    setError("");
    try {
      const res = await getRetentionRun(runId);
      setSelectedRun(res?.row || null);
    } catch (err) {
      setSelectedRun(null);
      setError(err?.response?.data?.message || t("retentionAdmin.messages.runDetailLoadFailed", "Retention run detail could not be loaded"));
    }
  }

  async function handleCreateSnapshot(e) {
    e.preventDefault();
    if (!canCreateSnapshots) return;
    setCreatingSnapshot(true);
    setError("");
    setMessage("");

    try {
      const res = await createExportSnapshot({
        payrollPeriodCloseId: Number(snapshotForm.payrollPeriodCloseId),
        idempotencyKey: snapshotForm.idempotencyKey || undefined,
      });
      setMessage(
        res?.idempotent
          ? t("retentionAdmin.messages.snapshotExists", "Snapshot already exists (#{{id}}).", {
              id: res?.snapshot?.id || "?",
            })
          : t("retentionAdmin.messages.snapshotCreated", "Snapshot created (#{{id}}).", {
              id: res?.snapshot?.id || "?",
            })
      );
      await loadSnapshots();
      setSnapshotForm((prev) => ({ ...prev, idempotencyKey: "" }));
    } catch (err) {
      setError(err?.response?.data?.message || t("retentionAdmin.messages.snapshotCreateFailed", "Export snapshot could not be created"));
    } finally {
      setCreatingSnapshot(false);
    }
  }

  async function handleSelectSnapshot(snapshotId) {
    if (!snapshotId) return;
    setError("");
    try {
      const res = await getExportSnapshot(snapshotId);
      setSelectedSnapshot(res || null);
    } catch (err) {
      setSelectedSnapshot(null);
      setError(err?.response?.data?.message || t("retentionAdmin.messages.snapshotDetailLoadFailed", "Snapshot detail could not be loaded"));
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">{t("retentionAdmin.title", "Retention and Export Snapshots")}</h1>
        <p className="mt-1 text-sm text-slate-600">
          {t(
            "retentionAdmin.subtitle",
            "PR-H07: policy-driven retention runs and immutable closed-period snapshot hashes."
          )}
        </p>
      </div>

      {error ? (
        <div className="rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{error}</div>
      ) : null}
      {message ? (
        <div className="rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
          {message}
        </div>
      ) : null}

      {!canReadRetention && !canReadSnapshots ? (
        <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          {t("retentionAdmin.messages.missingPermissions", "Missing permissions:")} <code>ops.retention.read</code>{" "}
          {t("retentionAdmin.messages.andOr", "and/or")} <code>ops.export_snapshot.read</code>
        </div>
      ) : null}

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <h2 className="text-sm font-semibold text-slate-900">{t("retentionAdmin.sections.policies", "Retention Policies")}</h2>

        <form className="mt-3 grid gap-2 md:grid-cols-4" onSubmit={handleCreatePolicy}>
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            placeholder={t("retentionAdmin.placeholders.policyCode", "Policy Code")}
            value={policyForm.policyCode}
            onChange={(e) => setPolicyForm((s) => ({ ...s, policyCode: e.target.value.toUpperCase() }))}
            required
          />
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            placeholder={t("retentionAdmin.placeholders.policyName", "Policy Name")}
            value={policyForm.policyName}
            onChange={(e) => setPolicyForm((s) => ({ ...s, policyName: e.target.value }))}
            required
          />
          <select
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            value={policyForm.datasetCode}
            onChange={(e) => setPolicyForm((s) => ({ ...s, datasetCode: e.target.value }))}
          >
            {DATASET_OPTIONS.map((opt) => (
              <option key={opt.code} value={opt.code}>
                {opt.code}
              </option>
            ))}
          </select>
          <select
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            value={policyForm.actionCode}
            onChange={(e) => setPolicyForm((s) => ({ ...s, actionCode: e.target.value }))}
          >
            {currentDataset.actions.map((action) => (
              <option key={action} value={action}>
                {action}
              </option>
            ))}
          </select>
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            type="number"
            min={1}
            max={36500}
            placeholder={t("retentionAdmin.placeholders.retentionDays", "Retention Days")}
            value={policyForm.retentionDays}
            onChange={(e) => setPolicyForm((s) => ({ ...s, retentionDays: e.target.value }))}
            required
          />
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            type="number"
            min={1}
            placeholder={t("retentionAdmin.placeholders.legalEntityOptional", "Legal Entity ID (optional)")}
            value={policyForm.legalEntityId}
            onChange={(e) => setPolicyForm((s) => ({ ...s, legalEntityId: e.target.value }))}
          />
          <select
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            value={policyForm.status}
            onChange={(e) => setPolicyForm((s) => ({ ...s, status: e.target.value }))}
          >
            <option value="ACTIVE">ACTIVE</option>
            <option value="PAUSED">PAUSED</option>
            <option value="DISABLED">DISABLED</option>
          </select>
          <div>
            <button
              type="submit"
              disabled={!canManageRetention || creatingPolicy}
              className="rounded border border-slate-300 px-3 py-1.5 text-sm"
            >
              {creatingPolicy
                ? t("retentionAdmin.actions.creating", "Creating...")
                : t("retentionAdmin.actions.createPolicy", "Create Policy")}
            </button>
          </div>
        </form>

        <div className="mt-3 grid gap-2 md:grid-cols-5">
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            placeholder={t("retentionAdmin.placeholders.leId", "LE ID")}
            value={policyFilters.legalEntityId}
            onChange={(e) => setPolicyFilters((s) => ({ ...s, legalEntityId: e.target.value }))}
          />
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            placeholder={t("retentionAdmin.placeholders.dataset", "Dataset")}
            value={policyFilters.datasetCode}
            onChange={(e) => setPolicyFilters((s) => ({ ...s, datasetCode: e.target.value.toUpperCase() }))}
          />
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            placeholder={t("retentionAdmin.placeholders.status", "Status")}
            value={policyFilters.status}
            onChange={(e) => setPolicyFilters((s) => ({ ...s, status: e.target.value.toUpperCase() }))}
          />
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            placeholder={t("retentionAdmin.placeholders.search", "Search")}
            value={policyFilters.q}
            onChange={(e) => setPolicyFilters((s) => ({ ...s, q: e.target.value }))}
          />
          <button type="button" className="rounded border border-slate-300 px-3 py-1.5 text-sm" onClick={loadPolicies}>
            {policiesLoading
              ? t("retentionAdmin.actions.loading", "Loading...")
              : t("retentionAdmin.actions.refreshPolicies", "Refresh Policies")}
          </button>
        </div>

        <div className="mt-3 text-xs text-slate-500">
          {t("retentionAdmin.totals.policies", "Total policies: {{total}}", { total: policiesTotal })}
        </div>
        <div className="mt-2 overflow-x-auto rounded border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">{t("retentionAdmin.table.code", "Code")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.datasetAction", "Dataset/Action")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.le", "LE")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.days", "Days")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.status", "Status")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.lastRun", "Last Run")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.actions", "Actions")}</th>
              </tr>
            </thead>
            <tbody>
              {policies.map((row) => (
                <tr key={row.id} className="border-t border-slate-100">
                  <td className="px-3 py-2">
                    <div className="font-medium">{row.policy_code}</div>
                    <div className="text-xs text-slate-500">{row.policy_name}</div>
                  </td>
                  <td className="px-3 py-2 text-xs">
                    <div>{row.dataset_code}</div>
                    <div className="text-slate-500">{row.action_code}</div>
                  </td>
                  <td className="px-3 py-2 text-xs">{row.legal_entity_id || t("retentionAdmin.labels.tenant", "TENANT")}</td>
                  <td className="px-3 py-2">{row.retention_days}</td>
                  <td className={`px-3 py-2 font-medium ${statusBadge(row.status)}`}>{row.status}</td>
                  <td className="px-3 py-2 text-xs">
                    <div>{row.last_run_at || "-"}</div>
                    <div className="text-slate-500">{row.last_run_status || "-"}</div>
                  </td>
                  <td className="px-3 py-2">
                    <div className="flex flex-wrap gap-2">
                      <button
                        type="button"
                        className="rounded border border-slate-300 px-2 py-1 text-xs"
                        disabled={!canManageRetention || updatingPolicyId === row.id}
                        onClick={() => handleTogglePolicyStatus(row)}
                      >
                        {updatingPolicyId === row.id
                          ? t("retentionAdmin.actions.updating", "Updating...")
                          : t("retentionAdmin.actions.toggleStatus", "Toggle Status")}
                      </button>
                      <button
                        type="button"
                        className="rounded border border-slate-300 px-2 py-1 text-xs"
                        disabled={!canManageRetention || runningPolicyKey === `${row.id}:sync`}
                        onClick={() => handleRunPolicy(row, false)}
                      >
                        {t("retentionAdmin.actions.runSync", "Run Sync")}
                      </button>
                      <button
                        type="button"
                        className="rounded border border-slate-300 px-2 py-1 text-xs"
                        disabled={!canManageRetention || runningPolicyKey === `${row.id}:async`}
                        onClick={() => handleRunPolicy(row, true)}
                      >
                        {t("retentionAdmin.actions.queueAsync", "Queue Async")}
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <h2 className="text-sm font-semibold text-slate-900">{t("retentionAdmin.sections.runs", "Retention Runs")}</h2>

        <div className="mt-3 grid gap-2 md:grid-cols-6">
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            placeholder={t("retentionAdmin.placeholders.leId", "LE ID")}
            value={runFilters.legalEntityId}
            onChange={(e) => setRunFilters((s) => ({ ...s, legalEntityId: e.target.value }))}
          />
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            placeholder={t("retentionAdmin.placeholders.policyId", "Policy ID")}
            value={runFilters.policyId}
            onChange={(e) => setRunFilters((s) => ({ ...s, policyId: e.target.value }))}
          />
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            placeholder={t("retentionAdmin.placeholders.status", "Status")}
            value={runFilters.status}
            onChange={(e) => setRunFilters((s) => ({ ...s, status: e.target.value.toUpperCase() }))}
          />
          <input
            type="date"
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            value={runFilters.dateFrom}
            onChange={(e) => setRunFilters((s) => ({ ...s, dateFrom: e.target.value }))}
          />
          <input
            type="date"
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            value={runFilters.dateTo}
            onChange={(e) => setRunFilters((s) => ({ ...s, dateTo: e.target.value }))}
          />
          <button type="button" className="rounded border border-slate-300 px-3 py-1.5 text-sm" onClick={loadRuns}>
            {runsLoading ? t("retentionAdmin.actions.loading", "Loading...") : t("retentionAdmin.actions.refreshRuns", "Refresh Runs")}
          </button>
        </div>

        <div className="mt-3 text-xs text-slate-500">
          {t("retentionAdmin.totals.runs", "Total runs: {{total}}", { total: runsTotal })}
        </div>
        <div className="mt-2 overflow-x-auto rounded border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">{t("retentionAdmin.table.run", "Run")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.policy", "Policy")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.status", "Status")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.counts", "Counts")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.startedFinished", "Started/Finished")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.detail", "Detail")}</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((row) => (
                <tr key={row.id} className="border-t border-slate-100">
                  <td className="px-3 py-2">#{row.id}</td>
                  <td className="px-3 py-2 text-xs">
                    <div>{row.policy_code}</div>
                    <div className="text-slate-500">{row.dataset_code}</div>
                  </td>
                  <td className={`px-3 py-2 font-medium ${statusBadge(row.status)}`}>{row.status}</td>
                  <td className="px-3 py-2 text-xs">
                    <div>{t("retentionAdmin.labels.scanned", "scanned:")} {row.scanned_rows}</div>
                    <div>{t("retentionAdmin.labels.affected", "affected:")} {row.affected_rows}</div>
                    <div>
                      {t("retentionAdmin.labels.maskedPurgedArchived", "masked/purged/archived:")}{" "}
                      {row.masked_rows}/{row.purged_rows}/{row.archived_rows}
                    </div>
                  </td>
                  <td className="px-3 py-2 text-xs">
                    <div>{row.started_at || "-"}</div>
                    <div>{row.finished_at || "-"}</div>
                  </td>
                  <td className="px-3 py-2">
                    <button
                      type="button"
                      className="rounded border border-slate-300 px-2 py-1 text-xs"
                      onClick={() => handleSelectRun(row.id)}
                    >
                      {t("retentionAdmin.actions.view", "View")}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {selectedRun ? (
          <pre className="mt-3 overflow-auto rounded bg-slate-50 p-3 text-xs">{pretty(selectedRun)}</pre>
        ) : null}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <h2 className="text-sm font-semibold text-slate-900">{t("retentionAdmin.sections.snapshots", "Period Export Snapshots")}</h2>

        <form className="mt-3 grid gap-2 md:grid-cols-4" onSubmit={handleCreateSnapshot}>
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            type="number"
            min={1}
            placeholder={t("retentionAdmin.placeholders.payrollCloseId", "Payroll Close ID")}
            value={snapshotForm.payrollPeriodCloseId}
            onChange={(e) => setSnapshotForm((s) => ({ ...s, payrollPeriodCloseId: e.target.value }))}
            required
          />
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            placeholder={t("retentionAdmin.placeholders.idempotencyKeyOptional", "Idempotency key (optional)")}
            value={snapshotForm.idempotencyKey}
            onChange={(e) => setSnapshotForm((s) => ({ ...s, idempotencyKey: e.target.value }))}
          />
          <button
            type="submit"
            disabled={!canCreateSnapshots || creatingSnapshot}
            className="rounded border border-slate-300 px-3 py-1.5 text-sm"
          >
            {creatingSnapshot
              ? t("retentionAdmin.actions.creating", "Creating...")
              : t("retentionAdmin.actions.createSnapshot", "Create Snapshot")}
          </button>
        </form>

        <div className="mt-3 grid gap-2 md:grid-cols-3">
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            placeholder={t("retentionAdmin.placeholders.leId", "LE ID")}
            value={snapshotFilters.legalEntityId}
            onChange={(e) => setSnapshotFilters((s) => ({ ...s, legalEntityId: e.target.value }))}
          />
          <input
            className="rounded border border-slate-300 px-2 py-1.5 text-sm"
            placeholder={t("retentionAdmin.placeholders.payrollCloseId", "Payroll Close ID")}
            value={snapshotFilters.payrollPeriodCloseId}
            onChange={(e) => setSnapshotFilters((s) => ({ ...s, payrollPeriodCloseId: e.target.value }))}
          />
          <button
            type="button"
            className="rounded border border-slate-300 px-3 py-1.5 text-sm"
            onClick={loadSnapshots}
          >
            {snapshotsLoading
              ? t("retentionAdmin.actions.loading", "Loading...")
              : t("retentionAdmin.actions.refreshSnapshots", "Refresh Snapshots")}
          </button>
        </div>

        <div className="mt-3 text-xs text-slate-500">
          {t("retentionAdmin.totals.snapshots", "Total snapshots: {{total}}", { total: snapshotsTotal })}
        </div>
        <div className="mt-2 overflow-x-auto rounded border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">{t("retentionAdmin.table.snapshot", "Snapshot")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.lePeriod", "LE / Period")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.closeId", "Close ID")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.hash", "Hash")}</th>
                <th className="px-3 py-2">{t("retentionAdmin.table.detail", "Detail")}</th>
              </tr>
            </thead>
            <tbody>
              {snapshots.map((row) => (
                <tr key={row.id} className="border-t border-slate-100">
                  <td className={`px-3 py-2 font-medium ${statusBadge(row.status)}`}>#{row.id}</td>
                  <td className="px-3 py-2 text-xs">
                    <div>{t("retentionAdmin.labels.le", "LE:")} {row.legal_entity_id}</div>
                    <div>{row.period_start} - {row.period_end}</div>
                  </td>
                  <td className="px-3 py-2">{row.payroll_period_close_id || "-"}</td>
                  <td className="px-3 py-2 text-xs break-all max-w-[280px]">{row.snapshot_hash}</td>
                  <td className="px-3 py-2">
                    <button
                      type="button"
                      className="rounded border border-slate-300 px-2 py-1 text-xs"
                      onClick={() => handleSelectSnapshot(row.id)}
                    >
                      {t("retentionAdmin.actions.view", "View")}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {selectedSnapshot ? (
          <pre className="mt-3 overflow-auto rounded bg-slate-50 p-3 text-xs">{pretty(selectedSnapshot)}</pre>
        ) : null}
      </section>
    </div>
  );
}
