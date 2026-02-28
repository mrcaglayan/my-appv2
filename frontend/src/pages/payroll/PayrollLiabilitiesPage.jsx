import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  buildPayrollRunLiabilities,
  createPayrollRunPaymentBatch,
  getPayrollRunLiabilities,
  getPayrollRunPaymentBatchPreview,
  listPayrollLiabilities,
} from "../../api/payrollLiabilities.js";
import {
  applyPayrollPaymentSync,
  getPayrollPaymentSyncPreview,
} from "../../api/payrollPaymentSync.js";
import {
  approveApplyPayrollManualSettlementRequest,
  createPayrollManualSettlementRequest,
  listPayrollManualSettlementRequests,
  rejectPayrollManualSettlementRequest,
} from "../../api/payrollSettlementOverrides.js";
import { getPayrollLiabilityBeneficiarySnapshot } from "../../api/payrollBeneficiaries.js";
import { useAuth } from "../../auth/useAuth.js";

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function formatAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "-";
  return parsed.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 6,
  });
}

function formatDateTime(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleString();
}

export default function PayrollLiabilitiesPage() {
  const params = useParams();
  const routeRunId = toPositiveInt(params.runId ?? params.id);
  const { hasPermission } = useAuth();
  const canRead = hasPermission("payroll.liabilities.read");
  const canBuild = hasPermission("payroll.liabilities.build");
  const canPrepare = hasPermission("payroll.payment.prepare");
  const canSyncRead = hasPermission("payroll.payment.sync.read");
  const canSyncApply = hasPermission("payroll.payment.sync.apply");
  const canOverrideRead = hasPermission("payroll.settlement.override.read");
  const canOverrideRequest = hasPermission("payroll.settlement.override.request");
  const canOverrideApprove = hasPermission("payroll.settlement.override.approve");
  const canBeneficiarySnapshotRead = hasPermission("payroll.beneficiary.snapshot.read");

  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const [globalFilters, setGlobalFilters] = useState({
    runId: "",
    status: "",
    scope: "",
    q: "",
  });

  const [runDetail, setRunDetail] = useState(null);
  const [items, setItems] = useState([]);
  const [summary, setSummary] = useState(null);
  const [audit, setAudit] = useState([]);
  const [preview, setPreview] = useState(null);
  const [syncPreview, setSyncPreview] = useState(null);
  const [syncError, setSyncError] = useState("");
  const [selectedLiabilityId, setSelectedLiabilityId] = useState(null);
  const [selectedLiability, setSelectedLiability] = useState(null);
  const [overrideRequests, setOverrideRequests] = useState([]);
  const [overrideError, setOverrideError] = useState("");
  const [overrideBusy, setOverrideBusy] = useState(false);
  const [beneficiarySnapshotBusy, setBeneficiarySnapshotBusy] = useState(false);
  const [beneficiarySnapshotError, setBeneficiarySnapshotError] = useState("");
  const [beneficiarySnapshotItem, setBeneficiarySnapshotItem] = useState(null);

  const [scope, setScope] = useState("NET_PAY");
  const [syncScope, setSyncScope] = useState("ALL");
  const [allowB04OnlySync, setAllowB04OnlySync] = useState(false);
  const [batchForm, setBatchForm] = useState({
    bankAccountId: "",
    idempotencyKey: "",
    notes: "",
  });
  const [createdBatch, setCreatedBatch] = useState(null);

  const totalAmount = useMemo(
    () => toAmount(items.reduce((sum, row) => sum + Number(row?.amount || 0), 0)),
    [items]
  );

  function toAmount(value) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return 0;
    return Number(parsed.toFixed(6));
  }

  function findLiabilityInItems(liabilityId) {
    return (items || []).find((row) => Number(row?.id) === Number(liabilityId)) || null;
  }

  async function loadOverrideRequests(liabilityId, { silent = false } = {}) {
    if (!canOverrideRead || !liabilityId) {
      setSelectedLiabilityId(null);
      setSelectedLiability(null);
      setOverrideRequests([]);
      setOverrideError("");
      return;
    }
    if (!silent) {
      setOverrideBusy(true);
    }
    setOverrideError("");
    setSelectedLiabilityId(Number(liabilityId));
    try {
      const res = await listPayrollManualSettlementRequests(liabilityId);
      setOverrideRequests(res?.items || []);
      setSelectedLiability(res?.liability || findLiabilityInItems(liabilityId));
    } catch (err) {
      setOverrideRequests([]);
      setSelectedLiability(findLiabilityInItems(liabilityId));
      setOverrideError(
        err?.response?.data?.message || "Manual settlement override request listesi yuklenemedi"
      );
    } finally {
      if (!silent) {
        setOverrideBusy(false);
      }
    }
  }

  async function handleCreateManualOverrideRequest(liability) {
    if (!liability) return;
    if (!canOverrideRequest) {
      setOverrideError("Missing permission: payroll.settlement.override.request");
      return;
    }
    const amountRaw = window.prompt(
      `Manual settlement amount (remaining ${formatAmount(liability.outstanding_amount ?? liability.amount)})`,
      ""
    );
    if (amountRaw === null) return;
    const amount = Number(amountRaw);
    if (!Number.isFinite(amount) || amount <= 0) {
      setOverrideError("Gecerli bir amount girin");
      return;
    }
    const reason =
      window.prompt(
        "Manual settlement reason",
        "Bank evidence unavailable, treasury confirmation received"
      ) || "";
    if (!String(reason).trim()) {
      setOverrideError("Reason gerekli");
      return;
    }
    const externalRef = window.prompt("External reference (optional)", "") || "";

    setOverrideBusy(true);
    setOverrideError("");
    setMessage("");
    try {
      await createPayrollManualSettlementRequest(liability.id, {
        amount,
        settled_at: new Date().toISOString(),
        reason: String(reason).trim(),
        external_ref: String(externalRef || "").trim() || undefined,
      });
      setMessage(`Manual settlement override request olusturuldu (Liability #${liability.id})`);
      await load();
      await loadOverrideRequests(liability.id, { silent: true });
    } catch (err) {
      setOverrideError(err?.response?.data?.message || "Manual settlement request basarisiz");
    } finally {
      setOverrideBusy(false);
    }
  }

  async function handleApproveApplyOverride(reqRow) {
    if (!reqRow) return;
    if (!canOverrideApprove) {
      setOverrideError("Missing permission: payroll.settlement.override.approve");
      return;
    }
    const decisionNote =
      window.prompt("Approve/apply decision note (optional)", "Approved after treasury confirmation") || "";

    setOverrideBusy(true);
    setOverrideError("");
    setMessage("");
    try {
      await approveApplyPayrollManualSettlementRequest(reqRow.id, {
        decision_note: String(decisionNote || "").trim() || undefined,
      });
      setMessage(`Manual settlement request #${reqRow.id} approve+apply tamamlandi`);
      await load();
      await loadOverrideRequests(reqRow.payroll_liability_id, { silent: true });
    } catch (err) {
      setOverrideError(err?.response?.data?.message || "Approve/apply basarisiz");
    } finally {
      setOverrideBusy(false);
    }
  }

  async function handleRejectOverride(reqRow) {
    if (!reqRow) return;
    if (!canOverrideApprove) {
      setOverrideError("Missing permission: payroll.settlement.override.approve");
      return;
    }
    const decisionNote = window.prompt("Reject reason", "Insufficient evidence");
    if (decisionNote === null) return;

    setOverrideBusy(true);
    setOverrideError("");
    setMessage("");
    try {
      await rejectPayrollManualSettlementRequest(reqRow.id, {
        decision_note: String(decisionNote || "").trim() || "Rejected",
      });
      setMessage(`Manual settlement request #${reqRow.id} rejected`);
      await loadOverrideRequests(reqRow.payroll_liability_id, { silent: true });
    } catch (err) {
      setOverrideError(err?.response?.data?.message || "Reject basarisiz");
    } finally {
      setOverrideBusy(false);
    }
  }

  async function handleLoadBeneficiarySnapshot(liabilityId) {
    if (!liabilityId) return;
    if (!canBeneficiarySnapshotRead) {
      setBeneficiarySnapshotError("Missing permission: payroll.beneficiary.snapshot.read");
      return;
    }
    setBeneficiarySnapshotBusy(true);
    setBeneficiarySnapshotError("");
    try {
      const res = await getPayrollLiabilityBeneficiarySnapshot(liabilityId);
      setBeneficiarySnapshotItem(res?.item || null);
      setMessage(`Beneficiary snapshot yuklendi (Liability #${liabilityId})`);
    } catch (err) {
      setBeneficiarySnapshotItem(null);
      setBeneficiarySnapshotError(err?.response?.data?.message || "Beneficiary snapshot yuklenemedi");
    } finally {
      setBeneficiarySnapshotBusy(false);
    }
  }

  async function loadGlobal() {
    const res = await listPayrollLiabilities({
      limit: 300,
      offset: 0,
      runId: globalFilters.runId || undefined,
      status: globalFilters.status || undefined,
      scope: globalFilters.scope || undefined,
      q: globalFilters.q || undefined,
    });
    setRunDetail(null);
    setItems(res?.rows || []);
    setSummary(null);
    setAudit([]);
    setPreview(null);
    setSyncPreview(null);
    setSyncError("");
    setBeneficiarySnapshotItem(null);
    setBeneficiarySnapshotError("");
  }

  async function loadRun() {
    const [liabRes, previewRes, syncRes] = await Promise.all([
      getPayrollRunLiabilities(routeRunId),
      getPayrollRunPaymentBatchPreview(routeRunId, { scope }).catch((err) => {
        const messageText = err?.response?.data?.message || err?.message || "";
        if (messageText) {
          setPreview((prev) =>
            prev && prev.run?.id === routeRunId
              ? prev
              : null
          );
        }
        return null;
      }),
      canSyncRead
        ? getPayrollPaymentSyncPreview(routeRunId, {
            scope: syncScope,
            allow_b04_only_settlement: allowB04OnlySync,
          }).catch((err) => {
            setSyncError(
              err?.response?.data?.message || err?.message || "Payment settlement sync preview yuklenemedi"
            );
            return null;
          })
        : Promise.resolve(null),
    ]);

    setRunDetail(liabRes?.run || null);
    setItems(liabRes?.items || []);
    setSummary(liabRes?.summary || null);
    setAudit(liabRes?.audit || []);
    setPreview(previewRes?.preview || null);
    setSyncPreview(syncRes?.preview || null);
    if (!canSyncRead) {
      setSyncError("");
    }
    if (beneficiarySnapshotItem) {
      const refreshed =
        (liabRes?.items || []).find((row) => Number(row?.id) === Number(beneficiarySnapshotItem?.payroll_liability_id)) ||
        null;
      if (!refreshed) {
        setBeneficiarySnapshotItem(null);
      }
    }
  }

  async function load() {
    if (!canRead) {
      setRunDetail(null);
      setItems([]);
      setSummary(null);
      setAudit([]);
      setPreview(null);
      return;
    }
    setLoading(true);
    setError("");
    setSyncError("");
    try {
      if (routeRunId) {
        await loadRun();
      } else {
        await loadGlobal();
      }
    } catch (err) {
      setError(err?.response?.data?.message || "Payroll liabilities yuklenemedi");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead, routeRunId, scope, syncScope, allowB04OnlySync, canSyncRead]);

  useEffect(() => {
    if (!selectedLiabilityId || !canOverrideRead) {
      return;
    }
    const exists = (items || []).some((row) => Number(row?.id) === Number(selectedLiabilityId));
    if (!exists) {
      setSelectedLiability(null);
      setOverrideRequests([]);
      return;
    }
    loadOverrideRequests(selectedLiabilityId, { silent: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [items, selectedLiabilityId, canOverrideRead]);

  async function handleBuildLiabilities() {
    if (!routeRunId) return;
    if (!canBuild) {
      setError("Missing permission: payroll.liabilities.build");
      return;
    }
    setBusy(true);
    setError("");
    setMessage("");
    try {
      const res = await buildPayrollRunLiabilities(routeRunId, {});
      setMessage(res?.alreadyBuilt ? "Liabilities zaten olusturulmus" : "Liabilities olusturuldu");
      await load();
    } catch (err) {
      setError(err?.response?.data?.message || "Liability build basarisiz");
    } finally {
      setBusy(false);
    }
  }

  async function handlePrepareBatch() {
    if (!routeRunId) return;
    if (!canPrepare) {
      setError("Missing permission: payroll.payment.prepare");
      return;
    }
    const bankAccountId = toPositiveInt(batchForm.bankAccountId);
    if (!bankAccountId) {
      setError("bankAccountId gerekli");
      return;
    }
    setBusy(true);
    setError("");
    setMessage("");
    try {
      const res = await createPayrollRunPaymentBatch(routeRunId, {
        scope,
        bankAccountId,
        idempotencyKey: String(batchForm.idempotencyKey || "").trim() || undefined,
        notes: String(batchForm.notes || "").trim() || undefined,
      });
      setCreatedBatch(res?.batch || null);
      setMessage("Payroll payment batch hazirlandi");
      if (!batchForm.idempotencyKey && res?.linkSummary?.idempotencyKey) {
        setBatchForm((prev) => ({ ...prev, idempotencyKey: res.linkSummary.idempotencyKey }));
      }
      setRunDetail(res?.run || runDetail);
      setItems(res?.liabilities?.items || items);
      setSummary(res?.liabilities?.summary || summary);
      setAudit(res?.liabilities?.audit || audit);
      setPreview(res?.preview_after_prepare || preview);
    } catch (err) {
      setError(err?.response?.data?.message || "Payment batch hazirlama basarisiz");
    } finally {
      setBusy(false);
    }
  }

  async function handleRefreshSyncPreview() {
    if (!routeRunId || !canSyncRead) {
      return;
    }
    setBusy(true);
    setSyncError("");
    setError("");
    try {
      const res = await getPayrollPaymentSyncPreview(routeRunId, {
        scope: syncScope,
        allow_b04_only_settlement: allowB04OnlySync,
      });
      setSyncPreview(res?.preview || null);
      setMessage("Payment settlement sync preview yenilendi");
    } catch (err) {
      setSyncError(err?.response?.data?.message || "Payment settlement sync preview yenilenemedi");
    } finally {
      setBusy(false);
    }
  }

  async function handleApplySync() {
    if (!routeRunId) return;
    if (!canSyncApply) {
      setError("Missing permission: payroll.payment.sync.apply");
      return;
    }
    setBusy(true);
    setError("");
    setSyncError("");
    setMessage("");
    try {
      const res = await applyPayrollPaymentSync(routeRunId, {
        scope: syncScope,
        allow_b04_only_settlement: allowB04OnlySync,
      });
      setMessage(
        `Payment sync uygulandi: PARTIAL ${res?.applied?.mark_partial_count || 0}, PAID ${res?.applied?.mark_paid_count || 0}, RELEASE ${res?.applied?.release_count || 0}`
      );
      await load();
    } catch (err) {
      setSyncError(err?.response?.data?.message || "Payment settlement sync apply basarisiz");
    } finally {
      setBusy(false);
    }
  }

  const currentPreview = preview;
  const currentSyncPreview = syncPreview;

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2">
            {routeRunId ? (
              <Link to={`/app/payroll-runs/${routeRunId}`} className="text-sm underline">
                Bordro Run Detayi
              </Link>
            ) : null}
            <h1 className="text-xl font-semibold text-slate-900">
              {routeRunId ? `Bordro Liability & Payment Prep (Run #${routeRunId})` : "Bordro Liabilities"}
            </h1>
          </div>
          <p className="mt-1 text-sm text-slate-600">
            PR-P03 + PR-P04: liability subledger, payment batch hazirlama, settlement sync.
          </p>
        </div>
        <button
          type="button"
          className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700"
          onClick={load}
          disabled={loading || busy}
        >
          Yenile
        </button>
      </div>

      {!canRead ? (
        <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          Missing permission: <code>payroll.liabilities.read</code>
        </div>
      ) : null}
      {error ? (
        <div className="rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
          {error}
        </div>
      ) : null}
      {message ? (
        <div className="rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
          {message}
        </div>
      ) : null}

      {!routeRunId ? (
        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <h2 className="text-sm font-semibold text-slate-900">Global Liste Filtreleri</h2>
          <div className="mt-3 grid gap-3 md:grid-cols-4">
            <input
              className="rounded border border-slate-300 px-2 py-1.5 text-sm"
              placeholder="Run ID"
              value={globalFilters.runId}
              onChange={(e) => setGlobalFilters((s) => ({ ...s, runId: e.target.value }))}
            />
            <select
              className="rounded border border-slate-300 px-2 py-1.5 text-sm"
              value={globalFilters.status}
              onChange={(e) => setGlobalFilters((s) => ({ ...s, status: e.target.value }))}
            >
              <option value="">Tum statusler</option>
              <option value="OPEN">OPEN</option>
              <option value="IN_BATCH">IN_BATCH</option>
              <option value="PARTIALLY_PAID">PARTIALLY_PAID</option>
              <option value="PAID">PAID</option>
              <option value="CANCELLED">CANCELLED</option>
            </select>
            <select
              className="rounded border border-slate-300 px-2 py-1.5 text-sm"
              value={globalFilters.scope}
              onChange={(e) => setGlobalFilters((s) => ({ ...s, scope: e.target.value }))}
            >
              <option value="">Tum scopelar</option>
              <option value="NET_PAY">NET_PAY</option>
              <option value="STATUTORY">STATUTORY</option>
              <option value="ALL">ALL</option>
            </select>
            <div className="flex gap-2">
              <input
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="q"
                value={globalFilters.q}
                onChange={(e) => setGlobalFilters((s) => ({ ...s, q: e.target.value }))}
              />
              <button
                type="button"
                className="rounded border border-slate-300 px-3 py-1.5 text-sm"
                onClick={load}
                disabled={loading || busy}
              >
                Listele
              </button>
            </div>
          </div>
        </section>
      ) : (
        <>
          <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <div className="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-4">
              <div>
                <div className="text-xs text-slate-500">Run</div>
                <div className="font-medium">{runDetail?.run_no || "-"}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Entity</div>
                <div className="font-medium">{runDetail?.legal_entity_code || runDetail?.entity_code || "-"}</div>
                <div className="text-xs text-slate-500">{runDetail?.legal_entity_name || ""}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Status</div>
                <div className="font-medium">{runDetail?.status || "-"}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Liabilities Built</div>
                <div className="font-medium">{runDetail?.liabilities_built_at ? formatDateTime(runDetail.liabilities_built_at) : "-"}</div>
              </div>
            </div>

            <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-6">
              <div>
                <div className="text-xs text-slate-500">Toplam Liability</div>
                <div className="font-medium">{formatAmount(summary?.total_amount)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Open</div>
                <div className="font-medium">{formatAmount(summary?.total_open)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">In Batch</div>
                <div className="font-medium">{formatAmount(summary?.total_in_batch)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Partially Paid</div>
                <div className="font-medium">{formatAmount(summary?.total_partially_paid)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Paid</div>
                <div className="font-medium">{formatAmount(summary?.total_paid)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Outstanding</div>
                <div className="font-medium">{formatAmount(summary?.total_outstanding)}</div>
              </div>
            </div>

            <div className="mt-4 flex flex-wrap gap-2">
              {canBuild ? (
                <button
                  type="button"
                  className="rounded border border-slate-300 px-3 py-1.5 text-sm"
                  onClick={handleBuildLiabilities}
                  disabled={busy || loading}
                >
                  Liabilities Build
                </button>
              ) : null}
              <select
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                value={scope}
                onChange={(e) => setScope(e.target.value)}
                disabled={busy}
              >
                <option value="NET_PAY">NET_PAY</option>
                <option value="STATUTORY">STATUTORY</option>
                <option value="ALL">ALL</option>
              </select>
              <input
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="Bank Account ID"
                value={batchForm.bankAccountId}
                onChange={(e) => setBatchForm((s) => ({ ...s, bankAccountId: e.target.value }))}
              />
              <input
                className="min-w-[220px] rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="Idempotency Key (optional)"
                value={batchForm.idempotencyKey}
                onChange={(e) => setBatchForm((s) => ({ ...s, idempotencyKey: e.target.value }))}
              />
              <input
                className="min-w-[220px] rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="Notes (optional)"
                value={batchForm.notes}
                onChange={(e) => setBatchForm((s) => ({ ...s, notes: e.target.value }))}
              />
              {canPrepare ? (
                <button
                  type="button"
                  className="rounded bg-slate-900 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-60"
                  onClick={handlePrepareBatch}
                  disabled={busy || loading || !currentPreview?.can_prepare_payment_batch}
                >
                  Payment Batch Hazirla
                </button>
              ) : null}
            </div>

            {createdBatch?.id ? (
              <div className="mt-3 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-900">
                Batch olustu:
                {" "}
                <Link className="underline" to={`/app/odeme-batchleri/${createdBatch.id}`}>
                  {createdBatch.batch_no || `#${createdBatch.id}`}
                </Link>
              </div>
            ) : null}
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <h2 className="text-sm font-semibold text-slate-900">Payment Batch Preview ({scope})</h2>
            {!currentPreview ? (
              <div className="mt-3 text-sm text-slate-600">
                Preview yok. Once liabilities build edip tekrar deneyin.
              </div>
            ) : (
              <div className="mt-3 space-y-3 text-sm">
                <div className="grid gap-3 md:grid-cols-3">
                  <div>
                    <div className="text-xs text-slate-500">Eligible Liabilities</div>
                    <div className="font-medium">{currentPreview.eligible_liability_count || 0}</div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-500">Preview Total</div>
                    <div className="font-medium">{formatAmount(currentPreview.total_amount)}</div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-500">Can Prepare</div>
                    <div className="font-medium">{currentPreview.can_prepare_payment_batch ? "YES" : "NO"}</div>
                  </div>
                </div>
                <div className="text-xs text-slate-500">
                  Default idempotency key: {currentPreview.default_idempotency_key || "-"}
                </div>
                <div className="overflow-auto">
                  <table className="min-w-full border-collapse text-sm">
                    <thead>
                      <tr className="border-b">
                        <th className="p-2 text-left">Liability ID</th>
                        <th className="p-2 text-left">Type</th>
                        <th className="p-2 text-left">Beneficiary</th>
                        <th className="p-2 text-left">GL</th>
                        <th className="p-2 text-left">Amount</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(currentPreview.eligible_liabilities || []).map((row) => (
                        <tr key={row.id} className="border-b">
                          <td className="p-2">{row.id}</td>
                          <td className="p-2">{row.liability_type}</td>
                          <td className="p-2">
                            {row.employee_code ? `${row.employee_code} - ${row.employee_name || ""}` : row.beneficiary_name}
                          </td>
                          <td className="p-2">{row.payable_gl_account_id}</td>
                          <td className="p-2">{formatAmount(row.amount)}</td>
                        </tr>
                      ))}
                      {(currentPreview.eligible_liabilities || []).length === 0 ? (
                        <tr>
                          <td className="p-3 text-slate-500" colSpan={5}>
                            Uygun liability yok.
                          </td>
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <h2 className="text-sm font-semibold text-slate-900">Payment Settlement Sync (PR-P04)</h2>
                <p className="mt-1 text-xs text-slate-600">
                  B04 payment status + B03 bank reconciliation evidence ile liabilities sync preview/apply.
                </p>
              </div>
              {!canSyncRead ? (
                <div className="text-xs text-amber-700">
                  Missing permission: <code>payroll.payment.sync.read</code>
                </div>
              ) : null}
            </div>

            <div className="mt-4 flex flex-wrap items-center gap-2">
              <select
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                value={syncScope}
                onChange={(e) => setSyncScope(e.target.value)}
                disabled={busy || !canSyncRead}
              >
                <option value="ALL">ALL</option>
                <option value="NET_PAY">NET_PAY</option>
                <option value="STATUTORY">STATUTORY</option>
              </select>
              <label className="flex items-center gap-2 rounded border border-amber-200 bg-amber-50 px-2 py-1 text-xs text-amber-900">
                <input
                  type="checkbox"
                  checked={allowB04OnlySync}
                  onChange={(e) => setAllowB04OnlySync(e.target.checked)}
                  disabled={busy || !canSyncRead}
                />
                B04-only settlement kabul et (reconciliation olmadan)
              </label>
              {canSyncRead ? (
                <button
                  type="button"
                  className="rounded border border-slate-300 px-3 py-1.5 text-sm"
                  onClick={handleRefreshSyncPreview}
                  disabled={busy || loading}
                >
                  Sync Preview Yenile
                </button>
              ) : null}
              {canSyncApply ? (
                <button
                  type="button"
                  className="rounded bg-slate-900 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-60"
                  onClick={handleApplySync}
                  disabled={busy || loading || !currentSyncPreview}
                >
                  Payment Sync Uygula
                </button>
              ) : null}
            </div>

            {syncError ? (
              <div className="mt-3 rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
                {syncError}
              </div>
            ) : null}

            {!canSyncRead ? null : !currentSyncPreview ? (
              <div className="mt-3 text-sm text-slate-600">Sync preview yok.</div>
            ) : (
              <div className="mt-4 space-y-4">
                <div className="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-6">
                  <div>
                    <div className="text-xs text-slate-500">Candidates</div>
                    <div className="font-medium">{currentSyncPreview.summary?.total_candidates || 0}</div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-500">Mark PARTIAL</div>
                    <div className="font-medium">
                      {currentSyncPreview.summary?.mark_partial_count || 0} /{" "}
                      {formatAmount(currentSyncPreview.summary?.mark_partial_amount)}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-500">Mark PAID</div>
                    <div className="font-medium">
                      {currentSyncPreview.summary?.mark_paid_count || 0} /{" "}
                      {formatAmount(currentSyncPreview.summary?.mark_paid_amount)}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-500">Release to OPEN</div>
                    <div className="font-medium">
                      {currentSyncPreview.summary?.release_count || 0} /{" "}
                      {formatAmount(currentSyncPreview.summary?.release_amount)}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-500">No-op</div>
                    <div className="font-medium">
                      {currentSyncPreview.summary?.noop_count || 0} /{" "}
                      {formatAmount(currentSyncPreview.summary?.noop_amount)}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-500">Exceptions</div>
                    <div className="font-medium">
                      {currentSyncPreview.summary?.exception_count || 0}
                    </div>
                  </div>
                </div>

                <div className="text-xs text-slate-500">
                  Scope: {currentSyncPreview.scope || syncScope}
                  {" | "}B04-only: {currentSyncPreview.allow_b04_only_settlement ? "YES" : "NO"}
                </div>

                <div className="overflow-auto">
                  <table className="min-w-full border-collapse text-sm">
                    <thead>
                      <tr className="border-b">
                        <th className="p-2 text-left">Liability</th>
                        <th className="p-2 text-left">Beneficiary</th>
                        <th className="p-2 text-left">Batch/Line</th>
                        <th className="p-2 text-left">Bank Evidence</th>
                        <th className="p-2 text-left">Verdict</th>
                        <th className="p-2 text-left">Amount</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(currentSyncPreview.items || []).slice(0, 20).map((row) => (
                        <tr key={`${row.payroll_liability_id}-${row.link_id}`} className="border-b">
                          <td className="p-2">
                            {row.payroll_liability_id}
                            <div className="text-xs text-slate-500">{row.liability_type}</div>
                          </td>
                          <td className="p-2">
                            {row.employee_code
                              ? `${row.employee_code} - ${row.employee_name || ""}`
                              : row.beneficiary_name}
                          </td>
                          <td className="p-2">
                            B{row.payment_batch_id}
                            {row.payment_batch_line_id ? ` / L${row.payment_batch_line_id}` : ""}
                            <div className="text-xs text-slate-500">
                              {row.payment_batch_status} / {row.payment_batch_line_status || "-"}
                            </div>
                          </td>
                          <td className="p-2">
                            {Number(row.batch_bank_match_count || 0) > 0 ? (
                              <>
                                matches={row.batch_bank_match_count} total={formatAmount(row.batch_bank_matched_total)}
                                {row.bank_statement_line_id ? (
                                  <div className="text-xs text-slate-500">
                                    stmtLine #{row.bank_statement_line_id}
                                  </div>
                                ) : null}
                              </>
                            ) : (
                              <span className="text-slate-500">-</span>
                            )}
                          </td>
                          <td className="p-2">
                            <div className="font-medium">{row.verdict?.action || "-"}</div>
                            <div className="text-xs text-slate-500">{row.verdict?.reason || "-"}</div>
                          </td>
                          <td className="p-2">
                            {formatAmount(row.verdict?.amount ?? row.allocated_amount)}
                            {(row.verdict?.targetSettledAmount ?? row.verdict?.target_settled_amount) ? (
                              <div className="text-xs text-slate-500">
                                target:{" "}
                                {formatAmount(
                                  row.verdict?.targetSettledAmount ?? row.verdict?.target_settled_amount
                                )}
                              </div>
                            ) : null}
                          </td>
                        </tr>
                      ))}
                      {(currentSyncPreview.items || []).length === 0 ? (
                        <tr>
                          <td className="p-3 text-slate-500" colSpan={6}>
                            Sync candidate yok.
                          </td>
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                </div>

                {(currentSyncPreview.items || []).length > 20 ? (
                  <div className="text-xs text-slate-500">
                    Ilk 20 aday gosteriliyor ({currentSyncPreview.items.length} toplam).
                  </div>
                ) : null}
              </div>
            )}
          </section>
        </>
      )}

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="flex items-center justify-between gap-2">
          <h2 className="text-sm font-semibold text-slate-900">Liabilities</h2>
          <div className="text-xs text-slate-500">Toplam: {formatAmount(totalAmount)}</div>
        </div>
        <div className="mt-3 overflow-auto">
          <table className="min-w-full border-collapse text-sm">
            <thead>
              <tr className="border-b">
                <th className="p-2 text-left">ID</th>
                <th className="p-2 text-left">Run</th>
                <th className="p-2 text-left">Type</th>
                <th className="p-2 text-left">Employee</th>
                <th className="p-2 text-left">Beneficiary</th>
                <th className="p-2 text-left">Amount</th>
                <th className="p-2 text-left">Settled</th>
                <th className="p-2 text-left">Outstanding</th>
                <th className="p-2 text-left">Status</th>
                <th className="p-2 text-left">Benef Snapshot</th>
                <th className="p-2 text-left">Batch</th>
                <th className="p-2 text-left">Actions</th>
              </tr>
            </thead>
            <tbody>
              {items.map((row) => (
                <tr key={row.id} className="border-b">
                  <td className="p-2">{row.id}</td>
                  <td className="p-2">
                    {routeRunId ? row.run_id : (
                      <Link className="underline" to={`/app/payroll-runs/${row.run_id}/liabilities`}>
                        {row.run_id}
                      </Link>
                    )}
                  </td>
                  <td className="p-2">{row.liability_type}</td>
                  <td className="p-2">
                    {row.employee_code ? `${row.employee_code} - ${row.employee_name || ""}` : "-"}
                  </td>
                  <td className="p-2">{row.beneficiary_name}</td>
                  <td className="p-2">{formatAmount(row.amount)}</td>
                  <td className="p-2">{formatAmount(row.settled_amount)}</td>
                  <td className="p-2">{formatAmount(row.outstanding_amount)}</td>
                  <td className="p-2">{row.status}</td>
                  <td className="p-2">
                    <div>{row.beneficiary_snapshot_status || "-"}</div>
                    {row.beneficiary_bank_snapshot_id ? (
                      <div className="text-xs text-slate-500">#{row.beneficiary_bank_snapshot_id}</div>
                    ) : null}
                    {row.beneficiary_type === "EMPLOYEE" && !row.beneficiary_ready_for_payment ? (
                      <div className="text-xs text-amber-700">Missing</div>
                    ) : null}
                  </td>
                  <td className="p-2">
                    {row.reserved_payment_batch_id ? (
                      <Link className="underline" to={`/app/odeme-batchleri/${row.reserved_payment_batch_id}`}>
                        {row.reserved_payment_batch_id}
                      </Link>
                    ) : "-"}
                  </td>
                  <td className="p-2">
                    <div className="flex flex-wrap gap-1">
                      {canOverrideRead ? (
                        <button
                          type="button"
                          className="rounded border border-slate-300 px-2 py-0.5 text-xs"
                          onClick={() => loadOverrideRequests(row.id)}
                          disabled={overrideBusy}
                        >
                          Overrides
                        </button>
                      ) : null}
                      {canBeneficiarySnapshotRead ? (
                        <button
                          type="button"
                          className="rounded border border-slate-300 px-2 py-0.5 text-xs"
                          onClick={() => handleLoadBeneficiarySnapshot(row.id)}
                          disabled={beneficiarySnapshotBusy}
                          title="Load latest beneficiary snapshot attached to this liability link"
                        >
                          Snapshot
                        </button>
                      ) : null}
                      {canOverrideRequest ? (
                        <button
                          type="button"
                          className="rounded border border-slate-300 px-2 py-0.5 text-xs"
                          onClick={() => handleCreateManualOverrideRequest(row)}
                          disabled={
                            overrideBusy ||
                            !["IN_BATCH", "PARTIALLY_PAID"].includes(String(row.status || "").toUpperCase())
                          }
                          title="Manual settlement override request (maker)"
                        >
                          Request
                        </button>
                      ) : null}
                    </div>
                  </td>
                </tr>
              ))}
              {items.length === 0 ? (
                <tr>
                  <td className="p-3 text-slate-500" colSpan={12}>
                    Liability kaydi yok.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      {(canBeneficiarySnapshotRead || beneficiarySnapshotError || beneficiarySnapshotItem) ? (
        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div>
              <h2 className="text-sm font-semibold text-slate-900">Beneficiary Snapshot (PR-P07)</h2>
              <p className="mt-1 text-xs text-slate-600">
                Payroll payment export should use immutable snapshot data, not live beneficiary master data.
              </p>
            </div>
            {!canBeneficiarySnapshotRead ? (
              <div className="text-xs text-amber-700">
                Missing permission: <code>payroll.beneficiary.snapshot.read</code>
              </div>
            ) : null}
          </div>

          {beneficiarySnapshotError ? (
            <div className="mt-3 rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
              {beneficiarySnapshotError}
            </div>
          ) : null}

          {!beneficiarySnapshotItem ? (
            <div className="mt-3 text-sm text-slate-600">
              Liability tablosundan <strong>Snapshot</strong> butonuyla bir kayit secin.
            </div>
          ) : (
            <div className="mt-3 space-y-3 text-sm">
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
                <div>
                  <div className="text-xs text-slate-500">Liability</div>
                  <div className="font-medium">#{beneficiarySnapshotItem.payroll_liability_id}</div>
                </div>
                <div>
                  <div className="text-xs text-slate-500">Snapshot Status</div>
                  <div className="font-medium">{beneficiarySnapshotItem.beneficiary_snapshot_status || "-"}</div>
                </div>
                <div>
                  <div className="text-xs text-slate-500">Snapshot ID</div>
                  <div className="font-medium">{beneficiarySnapshotItem.beneficiary_bank_snapshot_id || "-"}</div>
                </div>
                <div>
                  <div className="text-xs text-slate-500">Employee</div>
                  <div className="font-medium">
                    {beneficiarySnapshotItem.employee_code || "-"}
                    {beneficiarySnapshotItem.employee_name ? ` - ${beneficiarySnapshotItem.employee_name}` : ""}
                  </div>
                </div>
                <div>
                  <div className="text-xs text-slate-500">Batch/Line</div>
                  <div className="font-medium">
                    {beneficiarySnapshotItem.payment_batch_id || "-"} /{" "}
                    {beneficiarySnapshotItem.payment_batch_line_id || "-"}
                  </div>
                </div>
                <div>
                  <div className="text-xs text-slate-500">Busy</div>
                  <div className="font-medium">{beneficiarySnapshotBusy ? "YES" : "NO"}</div>
                </div>
              </div>

              {beneficiarySnapshotItem.snapshot ? (
                <pre className="overflow-auto whitespace-pre-wrap rounded bg-slate-50 p-3 text-xs">
                  {JSON.stringify(beneficiarySnapshotItem.snapshot, null, 2)}
                </pre>
              ) : (
                <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
                  Snapshot yok. Liability employee ise payroll payment prep (PR-P03) sirasinda beneficiary setup eksik
                  olabilir.
                </div>
              )}
            </div>
          )}
        </section>
      ) : null}

      {(canOverrideRead || canOverrideRequest || canOverrideApprove) ? (
        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div>
              <h2 className="text-sm font-semibold text-slate-900">
                Manual Settlement Overrides (PR-P06)
              </h2>
              <p className="mt-1 text-xs text-slate-600">
                Maker-checker manual settlement requests for liabilities already in payment flow.
              </p>
            </div>
            {!canOverrideRead ? (
              <div className="text-xs text-amber-700">
                Read permission missing: <code>payroll.settlement.override.read</code>
              </div>
            ) : null}
          </div>

          {overrideError ? (
            <div className="mt-3 rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
              {overrideError}
            </div>
          ) : null}

          {!selectedLiabilityId ? (
            <div className="mt-3 text-sm text-slate-600">
              Liability tablosundan <strong>Overrides</strong> veya <strong>Request</strong> ile bir liability secin.
            </div>
          ) : (
            <div className="mt-3 space-y-4">
              <div className="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-6">
                <div>
                  <div className="text-xs text-slate-500">Liability</div>
                  <div className="font-medium">#{selectedLiability?.id || selectedLiabilityId}</div>
                </div>
                <div>
                  <div className="text-xs text-slate-500">Status</div>
                  <div className="font-medium">{selectedLiability?.status || "-"}</div>
                </div>
                <div>
                  <div className="text-xs text-slate-500">Amount</div>
                  <div className="font-medium">{formatAmount(selectedLiability?.amount)}</div>
                </div>
                <div>
                  <div className="text-xs text-slate-500">Settled</div>
                  <div className="font-medium">{formatAmount(selectedLiability?.settled_amount)}</div>
                </div>
                <div>
                  <div className="text-xs text-slate-500">Outstanding</div>
                  <div className="font-medium">{formatAmount(selectedLiability?.outstanding_amount)}</div>
                </div>
                <div>
                  <div className="text-xs text-slate-500">Payment Link</div>
                  <div className="font-medium">{selectedLiability?.payment_link_id || "-"}</div>
                </div>
              </div>

              <div className="flex flex-wrap gap-2">
                {canOverrideRead ? (
                  <button
                    type="button"
                    className="rounded border border-slate-300 px-3 py-1.5 text-sm"
                    onClick={() => loadOverrideRequests(selectedLiabilityId)}
                    disabled={overrideBusy}
                  >
                    Requests Yenile
                  </button>
                ) : null}
                {canOverrideRequest ? (
                  <button
                    type="button"
                    className="rounded border border-slate-300 px-3 py-1.5 text-sm disabled:opacity-60"
                    onClick={() => handleCreateManualOverrideRequest(selectedLiability)}
                    disabled={
                      overrideBusy ||
                      !selectedLiability ||
                      !["IN_BATCH", "PARTIALLY_PAID"].includes(
                        String(selectedLiability?.status || "").toUpperCase()
                      )
                    }
                  >
                    Manual Settlement Request (Maker)
                  </button>
                ) : null}
              </div>

              <div className="space-y-2">
                {overrideBusy && (overrideRequests || []).length === 0 ? (
                  <div className="text-sm text-slate-500">Override requestleri yukleniyor...</div>
                ) : null}
                {(overrideRequests || []).map((reqRow) => (
                  <div key={reqRow.id} className="rounded border border-slate-200 p-3 text-sm">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="font-medium">Req #{reqRow.id}</span>
                      <span className="rounded border px-2 py-0.5 text-xs">{reqRow.status}</span>
                      <span className="text-xs text-slate-500">
                        amount {formatAmount(reqRow.requested_amount)} {reqRow.currency_code}
                      </span>
                      <span className="ml-auto text-xs text-slate-500">
                        {formatDateTime(reqRow.requested_at)}
                      </span>
                    </div>
                    <div className="mt-1 text-xs text-slate-600">
                      settled_at: {formatDateTime(reqRow.settled_at)} | requested_by: #
                      {reqRow.requested_by_user_id || "-"}
                    </div>
                    <div className="mt-1 text-xs text-slate-700">
                      reason: {reqRow.reason || "-"}
                      {reqRow.external_ref ? ` | external_ref: ${reqRow.external_ref}` : ""}
                    </div>
                    {reqRow.decision_note ? (
                      <div className="mt-1 text-xs text-slate-600">decision: {reqRow.decision_note}</div>
                    ) : null}
                    {reqRow.status === "REQUESTED" && canOverrideApprove ? (
                      <div className="mt-2 flex flex-wrap gap-2">
                        <button
                          type="button"
                          className="rounded border border-slate-300 px-2 py-1 text-xs"
                          onClick={() => handleApproveApplyOverride(reqRow)}
                          disabled={overrideBusy}
                        >
                          Approve & Apply (Checker)
                        </button>
                        <button
                          type="button"
                          className="rounded border border-rose-300 bg-rose-50 px-2 py-1 text-xs text-rose-700"
                          onClick={() => handleRejectOverride(reqRow)}
                          disabled={overrideBusy}
                        >
                          Reject
                        </button>
                      </div>
                    ) : null}
                  </div>
                ))}
                {(overrideRequests || []).length === 0 && !overrideBusy ? (
                  <div className="text-sm text-slate-500">Manual override request kaydi yok.</div>
                ) : null}
              </div>
            </div>
          )}
        </section>
      ) : null}

      {routeRunId ? (
        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <h2 className="text-sm font-semibold text-slate-900">Liability Audit</h2>
          <div className="mt-3 space-y-2 text-sm">
            {audit.map((row) => (
              <div key={row.id} className="rounded border border-slate-200 p-2">
                <div className="flex items-center gap-2">
                  <span className="font-medium">{row.action}</span>
                  <span className="text-xs text-slate-500">#{row.acted_by_user_id || "-"}</span>
                  <span className="ml-auto text-xs text-slate-500">{formatDateTime(row.acted_at)}</span>
                </div>
                {row.payload_json ? (
                  <pre className="mt-2 whitespace-pre-wrap rounded bg-slate-50 p-2 text-xs">
                    {typeof row.payload_json === "string"
                      ? row.payload_json
                      : JSON.stringify(row.payload_json, null, 2)}
                  </pre>
                ) : null}
              </div>
            ))}
            {audit.length === 0 ? <div className="text-slate-500">Audit kaydi yok.</div> : null}
          </div>
        </section>
      ) : null}
    </div>
  );
}
