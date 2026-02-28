import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useAuth } from "../../auth/useAuth.js";
import {
  approveClosePayrollCloseControl,
  getPayrollCloseControl,
  listPayrollCloseControls,
  preparePayrollCloseControl,
  reopenPayrollCloseControl,
  requestPayrollCloseControl,
} from "../../api/payrollClose.js";

function formatDate(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toISOString().slice(0, 10);
}

function formatDateTime(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toISOString().replace("T", " ").slice(0, 19);
}

function statusColor(status) {
  const s = String(status || "").toUpperCase();
  if (s === "CLOSED") return "text-emerald-700";
  if (s === "REQUESTED") return "text-amber-700";
  if (s === "READY") return "text-blue-700";
  if (s === "REOPENED") return "text-violet-700";
  return "text-slate-700";
}

function normalizeDateQueryParam(value) {
  if (!value) return "";
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toISOString().slice(0, 10);
}

function buildFilterPrefillFromQuery(searchParams) {
  const legalEntityId = String(searchParams.get("legalEntityId") || "").trim();
  const periodStartRaw = searchParams.get("periodStart");
  const periodEndRaw = searchParams.get("periodEnd");
  const payrollPeriodRaw = searchParams.get("payrollPeriod");
  const payrollPeriod = normalizeDateQueryParam(payrollPeriodRaw);
  const periodStart = normalizeDateQueryParam(periodStartRaw) || payrollPeriod || "";
  const periodEnd = normalizeDateQueryParam(periodEndRaw) || payrollPeriod || "";
  const status = String(searchParams.get("status") || "").trim().toUpperCase();
  const hasUsefulPrefill = Boolean(legalEntityId || periodStart || periodEnd || status);
  return {
    hasUsefulPrefill,
    filters: {
      legalEntityId,
      status,
      periodStart,
      periodEnd,
    },
  };
}

export default function PayrollCloseControlsPage() {
  const [searchParams] = useSearchParams();
  const { hasPermission } = useAuth();
  const canRead = hasPermission("payroll.close.read");
  const canPrepare = hasPermission("payroll.close.prepare");
  const canRequest = hasPermission("payroll.close.request");
  const canApprove = hasPermission("payroll.close.approve");
  const canReopen = hasPermission("payroll.close.reopen");

  const [filters, setFilters] = useState({
    legalEntityId: "",
    status: "",
    periodStart: "",
    periodEnd: "",
  });
  const [rows, setRows] = useState([]);
  const [total, setTotal] = useState(0);
  const [listLoading, setListLoading] = useState(false);
  const [listError, setListError] = useState("");

  const [selectedCloseId, setSelectedCloseId] = useState(null);
  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState("");

  const [prepareForm, setPrepareForm] = useState({
    legalEntityId: "",
    periodStart: "",
    periodEnd: "",
    lockRunChanges: true,
    lockManualSettlements: true,
    lockPaymentPrep: false,
    note: "",
  });
  const [requestForm, setRequestForm] = useState({
    note: "",
    requestIdempotencyKey: "",
  });
  const [approveForm, setApproveForm] = useState({
    note: "",
    closeIdempotencyKey: "",
  });
  const [reopenForm, setReopenForm] = useState({
    reason: "",
  });

  const selectedClose = detail?.close || null;
  const failingErrorChecks = useMemo(
    () =>
      (detail?.checks || []).filter(
        (check) =>
          String(check?.severity || "").toUpperCase() === "ERROR" &&
          String(check?.status || "").toUpperCase() === "FAIL"
      ),
    [detail]
  );

  async function loadList(overrideFilters = null) {
    const activeFilters = overrideFilters || filters;
    if (!canRead) return;
    setListLoading(true);
    setListError("");
    setMessage("");
    try {
      const res = await listPayrollCloseControls({
        limit: 200,
        offset: 0,
        legalEntityId: activeFilters.legalEntityId || undefined,
        status: activeFilters.status || undefined,
        periodStart: activeFilters.periodStart || undefined,
        periodEnd: activeFilters.periodEnd || undefined,
      });
      const nextRows = res?.rows || [];
      setRows(nextRows);
      setTotal(Number(res?.total || 0));
      if (!selectedCloseId && nextRows.length > 0) {
        setSelectedCloseId(nextRows[0].id);
      }
    } catch (err) {
      setRows([]);
      setTotal(0);
      setListError(err?.response?.data?.message || "Payroll close listesi yuklenemedi");
    } finally {
      setListLoading(false);
    }
  }

  async function loadDetail(closeId) {
    if (!canRead || !closeId) {
      setDetail(null);
      return;
    }
    setDetailLoading(true);
    setDetailError("");
    try {
      const res = await getPayrollCloseControl(closeId);
      setDetail(res);
    } catch (err) {
      setDetail(null);
      setDetailError(err?.response?.data?.message || "Payroll close detayi yuklenemedi");
    } finally {
      setDetailLoading(false);
    }
  }

  useEffect(() => {
    loadList();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead]);

  useEffect(() => {
    if (!canRead) return;
    const prefill = buildFilterPrefillFromQuery(searchParams);
    if (!prefill.hasUsefulPrefill) return;

    setFilters((prev) => ({ ...prev, ...prefill.filters }));
    setPrepareForm((prev) => ({
      ...prev,
      legalEntityId: prefill.filters.legalEntityId || prev.legalEntityId,
      periodStart: prefill.filters.periodStart || prev.periodStart,
      periodEnd: prefill.filters.periodEnd || prev.periodEnd,
    }));
    setSelectedCloseId(null);
    loadList(prefill.filters);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams, canRead]);

  useEffect(() => {
    if (selectedCloseId) {
      loadDetail(selectedCloseId);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedCloseId, canRead]);

  async function handlePrepare(e) {
    e.preventDefault();
    if (!canPrepare) return;
    setBusy("prepare");
    setMessage("");
    setDetailError("");
    try {
      const res = await preparePayrollCloseControl({
        ...prepareForm,
        legalEntityId: Number(prepareForm.legalEntityId),
      });
      setDetail(res);
      if (res?.close?.id) setSelectedCloseId(res.close.id);
      setMessage("Checklist prepared.");
      await loadList();
    } catch (err) {
      setDetailError(err?.response?.data?.message || "Prepare islemi basarisiz");
    } finally {
      setBusy("");
    }
  }

  async function handleRequest() {
    if (!canRequest || !selectedClose?.id) return;
    setBusy("request");
    setMessage("");
    setDetailError("");
    try {
      const res = await requestPayrollCloseControl(selectedClose.id, requestForm);
      setDetail(res);
      setMessage("Close request created.");
      await loadList();
    } catch (err) {
      setDetailError(err?.response?.data?.message || "Request-close basarisiz");
    } finally {
      setBusy("");
    }
  }

  async function handleApprove() {
    if (!canApprove || !selectedClose?.id) return;
    setBusy("approve");
    setMessage("");
    setDetailError("");
    try {
      const res = await approveClosePayrollCloseControl(selectedClose.id, approveForm);
      setDetail(res);
      setMessage("Payroll period closed.");
      await loadList();
    } catch (err) {
      setDetailError(err?.response?.data?.message || "Approve-close basarisiz");
    } finally {
      setBusy("");
    }
  }

  async function handleReopen() {
    if (!canReopen || !selectedClose?.id) return;
    setBusy("reopen");
    setMessage("");
    setDetailError("");
    try {
      const res = await reopenPayrollCloseControl(selectedClose.id, reopenForm);
      setDetail(res);
      setMessage("Payroll period reopened.");
      await loadList();
    } catch (err) {
      setDetailError(err?.response?.data?.message || "Reopen basarisiz");
    } finally {
      setBusy("");
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">Payroll Close Controls</h1>
        <p className="mt-1 text-sm text-slate-600">
          PR-P08 checklist, maker-checker close flow, and payroll-period locks.
        </p>
      </div>

      {!canRead ? (
        <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          Missing permission: <code>payroll.close.read</code>
        </div>
      ) : null}
      {listError ? (
        <div className="rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
          {listError}
        </div>
      ) : null}
      {detailError ? (
        <div className="rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
          {detailError}
        </div>
      ) : null}
      {message ? (
        <div className="rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
          {message}
        </div>
      ) : null}

      <div className="grid gap-6 xl:grid-cols-[1.05fr_1.4fr]">
        <div className="space-y-6">
          <form onSubmit={handlePrepare} className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <h2 className="text-sm font-semibold text-slate-900">Prepare Checklist</h2>
            {!canPrepare ? (
              <div className="mt-2 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
                Missing permission: <code>payroll.close.prepare</code>
              </div>
            ) : null}
            <div className="mt-3 grid gap-3 md:grid-cols-2">
              <input
                value={prepareForm.legalEntityId}
                onChange={(e) => setPrepareForm((p) => ({ ...p, legalEntityId: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="legalEntityId *"
                required
              />
              <input
                type="date"
                value={prepareForm.periodStart}
                onChange={(e) => setPrepareForm((p) => ({ ...p, periodStart: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                required
              />
              <input
                type="date"
                value={prepareForm.periodEnd}
                onChange={(e) => setPrepareForm((p) => ({ ...p, periodEnd: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                required
              />
              <input
                value={prepareForm.note}
                onChange={(e) => setPrepareForm((p) => ({ ...p, note: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="prepare note"
              />
              <label className="flex items-center gap-2 text-xs">
                <input
                  type="checkbox"
                  checked={Boolean(prepareForm.lockRunChanges)}
                  onChange={(e) => setPrepareForm((p) => ({ ...p, lockRunChanges: e.target.checked }))}
                />
                <span>lock_run_changes</span>
              </label>
              <label className="flex items-center gap-2 text-xs">
                <input
                  type="checkbox"
                  checked={Boolean(prepareForm.lockManualSettlements)}
                  onChange={(e) =>
                    setPrepareForm((p) => ({ ...p, lockManualSettlements: e.target.checked }))
                  }
                />
                <span>lock_manual_settlements</span>
              </label>
              <label className="flex items-center gap-2 text-xs md:col-span-2">
                <input
                  type="checkbox"
                  checked={Boolean(prepareForm.lockPaymentPrep)}
                  onChange={(e) => setPrepareForm((p) => ({ ...p, lockPaymentPrep: e.target.checked }))}
                />
                <span>lock_payment_prep</span>
              </label>
            </div>
            <div className="mt-3 flex gap-2">
              <button
                type="submit"
                disabled={!canPrepare || busy === "prepare"}
                className="rounded border border-slate-300 px-3 py-1.5 text-sm"
              >
                {busy === "prepare" ? "Preparing..." : "Prepare"}
              </button>
              <button
                type="button"
                onClick={() =>
                  setFilters((prev) => ({
                    ...prev,
                    legalEntityId: prepareForm.legalEntityId || prev.legalEntityId,
                    periodStart: prepareForm.periodStart || prev.periodStart,
                    periodEnd: prepareForm.periodEnd || prev.periodEnd,
                  }))
                }
                className="rounded border border-slate-300 px-3 py-1.5 text-sm"
              >
                Copy To Filters
              </button>
            </div>
          </form>

          <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <h2 className="text-sm font-semibold text-slate-900">Close List</h2>
            <div className="mt-3 grid gap-3 md:grid-cols-2">
              <input
                value={filters.legalEntityId}
                onChange={(e) => setFilters((p) => ({ ...p, legalEntityId: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="legalEntityId"
              />
              <select
                value={filters.status}
                onChange={(e) => setFilters((p) => ({ ...p, status: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
              >
                <option value="">ALL</option>
                <option value="DRAFT">DRAFT</option>
                <option value="READY">READY</option>
                <option value="REQUESTED">REQUESTED</option>
                <option value="CLOSED">CLOSED</option>
                <option value="REOPENED">REOPENED</option>
              </select>
              <input
                type="date"
                value={filters.periodStart}
                onChange={(e) => setFilters((p) => ({ ...p, periodStart: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
              />
              <input
                type="date"
                value={filters.periodEnd}
                onChange={(e) => setFilters((p) => ({ ...p, periodEnd: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
              />
            </div>
            <div className="mt-3 flex items-center gap-2">
              <button
                type="button"
                onClick={loadList}
                disabled={!canRead || listLoading}
                className="rounded border border-slate-300 px-3 py-1.5 text-sm"
              >
                {listLoading ? "Loading..." : "Load"}
              </button>
              <span className="text-xs text-slate-500">total={total}</span>
            </div>
            <div className="mt-4 max-h-[440px] overflow-auto">
              <table className="min-w-full border-collapse text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="p-2 text-left">ID</th>
                    <th className="p-2 text-left">Entity</th>
                    <th className="p-2 text-left">Period</th>
                    <th className="p-2 text-left">Status</th>
                    <th className="p-2 text-left">Checks</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row) => (
                    <tr
                      key={row.id}
                      className={`border-b ${Number(row.id) === Number(selectedCloseId) ? "bg-slate-50" : ""}`}
                    >
                      <td className="p-2">
                        <button
                          type="button"
                          className="underline"
                          onClick={() => setSelectedCloseId(row.id)}
                        >
                          {row.id}
                        </button>
                      </td>
                      <td className="p-2">
                        <div>{row.legal_entity_code || `LE#${row.legal_entity_id}`}</div>
                        <div className="text-xs text-slate-500">{row.legal_entity_name || ""}</div>
                      </td>
                      <td className="p-2">
                        {formatDate(row.period_start)} - {formatDate(row.period_end)}
                      </td>
                      <td className={`p-2 font-medium ${statusColor(row.status)}`}>{row.status}</td>
                      <td className="p-2 text-xs text-slate-600">
                        {row.passed_checks}/{row.total_checks}
                        <div>fail={row.failed_checks} warn={row.warning_checks}</div>
                      </td>
                    </tr>
                  ))}
                  {rows.length === 0 ? (
                    <tr>
                      <td className="p-3 text-slate-500" colSpan={5}>
                        No records.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h2 className="text-sm font-semibold text-slate-900">Selected Detail</h2>
              <p className="mt-1 text-xs text-slate-500">
                {selectedClose
                  ? `Close #${selectedClose.id} | LE#${selectedClose.legal_entity_id}`
                  : "Select a row from the list."}
              </p>
            </div>
            {detailLoading ? <div className="text-xs text-slate-500">Loading...</div> : null}
          </div>

          {!selectedClose ? (
            <div className="mt-4 rounded border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-600">
              No close selected.
            </div>
          ) : (
            <>
              <div className="mt-3 grid gap-3 md:grid-cols-2">
                <div className="rounded border border-slate-200 p-3 text-sm">
                  <div className="text-xs text-slate-500">Status</div>
                  <div className={`font-semibold ${statusColor(selectedClose.status)}`}>
                    {selectedClose.status}
                  </div>
                  <div className="mt-1 text-xs text-slate-500">
                    {formatDate(selectedClose.period_start)} - {formatDate(selectedClose.period_end)}
                  </div>
                </div>
                <div className="rounded border border-slate-200 p-3 text-sm">
                  <div className="text-xs text-slate-500">Checklist</div>
                  <div className="font-medium text-slate-900">
                    total={selectedClose.total_checks} pass={selectedClose.passed_checks}
                  </div>
                  <div className="text-xs text-slate-500">
                    fail={selectedClose.failed_checks} warn={selectedClose.warning_checks}
                  </div>
                </div>
                <div className="rounded border border-slate-200 p-3 text-xs text-slate-600 md:col-span-2">
                  Locks: run={Number(selectedClose.lock_run_changes) ? "on" : "off"} | manual=
                  {Number(selectedClose.lock_manual_settlements) ? "on" : "off"} | paymentPrep=
                  {Number(selectedClose.lock_payment_prep) ? "on" : "off"}
                  {failingErrorChecks.length > 0 ? (
                    <div className="mt-1 text-rose-700">
                      ERROR-level failing checks present: {failingErrorChecks.length}
                    </div>
                  ) : null}
                </div>
              </div>

              <div className="mt-4 grid gap-4 xl:grid-cols-3">
                <div className="rounded border border-slate-200 p-3">
                  <div className="mb-2 text-xs font-medium text-slate-700">Request Close</div>
                  <input
                    value={requestForm.note}
                    onChange={(e) => setRequestForm((p) => ({ ...p, note: e.target.value }))}
                    className="mb-2 w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    placeholder="note"
                  />
                  <input
                    value={requestForm.requestIdempotencyKey}
                    onChange={(e) =>
                      setRequestForm((p) => ({ ...p, requestIdempotencyKey: e.target.value }))
                    }
                    className="mb-2 w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    placeholder="requestIdempotencyKey"
                  />
                  <button
                    type="button"
                    onClick={handleRequest}
                    disabled={!canRequest || busy === "request"}
                    className="rounded border border-slate-300 px-3 py-1.5 text-sm"
                  >
                    {busy === "request" ? "Working..." : "Request"}
                  </button>
                </div>

                <div className="rounded border border-slate-200 p-3">
                  <div className="mb-2 text-xs font-medium text-slate-700">Approve + Close</div>
                  <input
                    value={approveForm.note}
                    onChange={(e) => setApproveForm((p) => ({ ...p, note: e.target.value }))}
                    className="mb-2 w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    placeholder="note"
                  />
                  <input
                    value={approveForm.closeIdempotencyKey}
                    onChange={(e) =>
                      setApproveForm((p) => ({ ...p, closeIdempotencyKey: e.target.value }))
                    }
                    className="mb-2 w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    placeholder="closeIdempotencyKey"
                  />
                  <button
                    type="button"
                    onClick={handleApprove}
                    disabled={!canApprove || busy === "approve"}
                    className="rounded border border-slate-300 px-3 py-1.5 text-sm"
                  >
                    {busy === "approve" ? "Working..." : "Approve + Close"}
                  </button>
                </div>

                <div className="rounded border border-slate-200 p-3">
                  <div className="mb-2 text-xs font-medium text-slate-700">Reopen</div>
                  <input
                    value={reopenForm.reason}
                    onChange={(e) => setReopenForm((p) => ({ ...p, reason: e.target.value }))}
                    className="mb-2 w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    placeholder="reason (required)"
                  />
                  <button
                    type="button"
                    onClick={handleReopen}
                    disabled={!canReopen || busy === "reopen"}
                    className="rounded border border-slate-300 px-3 py-1.5 text-sm"
                  >
                    {busy === "reopen" ? "Working..." : "Reopen"}
                  </button>
                </div>
              </div>

              <div className="mt-4 grid gap-4 xl:grid-cols-2">
                <div>
                  <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-600">
                    Checklist Rows
                  </h3>
                  <div className="max-h-[260px] overflow-auto rounded border border-slate-200">
                    <table className="min-w-full border-collapse text-xs">
                      <thead>
                        <tr className="border-b bg-slate-50">
                          <th className="p-2 text-left">Code</th>
                          <th className="p-2 text-left">Sev</th>
                          <th className="p-2 text-left">Status</th>
                          <th className="p-2 text-left">Metric</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(detail?.checks || []).map((check) => (
                          <tr key={check.id || check.check_code} className="border-b">
                            <td className="p-2">
                              <div className="font-medium">{check.check_code}</div>
                              <div className="text-slate-500">{check.check_name}</div>
                            </td>
                            <td className="p-2">{check.severity}</td>
                            <td className="p-2">{check.status}</td>
                            <td className="p-2">
                              {check.metric_value ?? "-"}
                              <div className="text-slate-500">{check.metric_text || ""}</div>
                            </td>
                          </tr>
                        ))}
                        {(detail?.checks || []).length === 0 ? (
                          <tr>
                            <td className="p-2 text-slate-500" colSpan={4}>
                              No checks.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div>
                  <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-600">
                    Audit
                  </h3>
                  <div className="max-h-[260px] overflow-auto rounded border border-slate-200">
                    <table className="min-w-full border-collapse text-xs">
                      <thead>
                        <tr className="border-b bg-slate-50">
                          <th className="p-2 text-left">Time</th>
                          <th className="p-2 text-left">Action</th>
                          <th className="p-2 text-left">User</th>
                          <th className="p-2 text-left">Note</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(detail?.audit || []).map((row) => (
                          <tr key={row.id} className="border-b">
                            <td className="p-2">{formatDateTime(row.acted_at)}</td>
                            <td className="p-2">{row.action}</td>
                            <td className="p-2">{row.acted_by_user_id || "-"}</td>
                            <td className="p-2">{row.note || "-"}</td>
                          </tr>
                        ))}
                        {(detail?.audit || []).length === 0 ? (
                          <tr>
                            <td className="p-2 text-slate-500" colSpan={4}>
                              No audit rows.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
