import { useEffect, useState } from "react";
import {
  createBankApprovalPolicy,
  listBankApprovalPolicies,
  updateBankApprovalPolicy,
} from "../../api/bankApprovalPolicies.js";
import {
  approveBankApprovalRequest,
  listBankApprovalRequests,
  rejectBankApprovalRequest,
} from "../../api/bankApprovalRequests.js";
import { useAuth } from "../../auth/useAuth.js";

function formatDateTime(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleString();
}

function formatAmount(value) {
  if (value === null || value === undefined || value === "") return "-";
  const n = Number(value);
  if (!Number.isFinite(n)) return String(value);
  return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 6 });
}

function boolFromInput(value) {
  return value === true || String(value).trim().toLowerCase() === "true";
}

export default function BankGovernancePage() {
  const { hasPermission } = useAuth();
  const canPoliciesRead = hasPermission("bank.approvals.policies.read");
  const canPoliciesCreate = hasPermission("bank.approvals.policies.create");
  const canPoliciesUpdate = hasPermission("bank.approvals.policies.update");
  const canRequestsRead = hasPermission("bank.approvals.requests.read");
  const canRequestsApprove = hasPermission("bank.approvals.requests.approve");
  const canRequestsReject = hasPermission("bank.approvals.requests.reject");

  const [policyFilters, setPolicyFilters] = useState({
    targetType: "",
    actionType: "",
    status: "",
    q: "",
  });
  const [policies, setPolicies] = useState([]);
  const [policiesTotal, setPoliciesTotal] = useState(0);
  const [requests, setRequests] = useState([]);
  const [requestsTotal, setRequestsTotal] = useState(0);
  const [loadingPolicies, setLoadingPolicies] = useState(false);
  const [loadingRequests, setLoadingRequests] = useState(false);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [newPolicy, setNewPolicy] = useState({
    policyCode: "",
    policyName: "",
    targetType: "PAYMENT_BATCH",
    actionType: "SUBMIT_EXPORT",
    scopeType: "GLOBAL",
    legalEntityId: "",
    bankAccountId: "",
    currencyCode: "",
    minAmount: "",
    maxAmount: "",
    requiredApprovals: "1",
    makerCheckerRequired: true,
    approverPermissionCode: "bank.approvals.requests.approve.payment",
    autoExecuteOnFinalApproval: true,
  });

  async function loadPolicies() {
    if (!canPoliciesRead) {
      setPolicies([]);
      setPoliciesTotal(0);
      return;
    }
    setLoadingPolicies(true);
    try {
      const res = await listBankApprovalPolicies({
        limit: 100,
        offset: 0,
        targetType: policyFilters.targetType || undefined,
        actionType: policyFilters.actionType || undefined,
        status: policyFilters.status || undefined,
        q: policyFilters.q || undefined,
      });
      setPolicies(Array.isArray(res?.rows) ? res.rows : []);
      setPoliciesTotal(Number(res?.total || 0));
    } catch (err) {
      setPolicies([]);
      setPoliciesTotal(0);
      setError(err?.response?.data?.message || "B09 policy listesi yuklenemedi");
    } finally {
      setLoadingPolicies(false);
    }
  }

  async function loadRequests() {
    if (!canRequestsRead) {
      setRequests([]);
      setRequestsTotal(0);
      return;
    }
    setLoadingRequests(true);
    try {
      const res = await listBankApprovalRequests({
        limit: 100,
        offset: 0,
        requestStatus: "PENDING",
      });
      setRequests(Array.isArray(res?.rows) ? res.rows : []);
      setRequestsTotal(Number(res?.total || 0));
    } catch (err) {
      setRequests([]);
      setRequestsTotal(0);
      setError(err?.response?.data?.message || "B09 onay kuyrugu yuklenemedi");
    } finally {
      setLoadingRequests(false);
    }
  }

  async function reloadAll() {
    setError("");
    await Promise.all([loadPolicies(), loadRequests()]);
  }

  useEffect(() => {
    reloadAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canPoliciesRead, canRequestsRead]);

  async function handleCreatePolicy() {
    if (!canPoliciesCreate) return;
    setBusy("create-policy");
    setError("");
    setMessage("");
    try {
      const payload = {
        ...newPolicy,
        legalEntityId: newPolicy.legalEntityId ? Number(newPolicy.legalEntityId) : undefined,
        bankAccountId: newPolicy.bankAccountId ? Number(newPolicy.bankAccountId) : undefined,
        currencyCode: newPolicy.currencyCode || undefined,
        minAmount: newPolicy.minAmount === "" ? undefined : Number(newPolicy.minAmount),
        maxAmount: newPolicy.maxAmount === "" ? undefined : Number(newPolicy.maxAmount),
        requiredApprovals: Number(newPolicy.requiredApprovals || 1),
        makerCheckerRequired: Boolean(newPolicy.makerCheckerRequired),
        autoExecuteOnFinalApproval: Boolean(newPolicy.autoExecuteOnFinalApproval),
      };
      await createBankApprovalPolicy(payload);
      setMessage("B09 policy olusturuldu");
      await loadPolicies();
    } catch (err) {
      setError(err?.response?.data?.message || "B09 policy olusturulamadi");
    } finally {
      setBusy("");
    }
  }

  async function handleTogglePolicyStatus(policy) {
    if (!canPoliciesUpdate) return;
    const nextStatus = String(policy?.status || "").toUpperCase() === "ACTIVE" ? "PAUSED" : "ACTIVE";
    setBusy(`policy-status-${policy.id}`);
    setError("");
    setMessage("");
    try {
      await updateBankApprovalPolicy(policy.id, { status: nextStatus });
      setMessage(`Policy #${policy.id} -> ${nextStatus}`);
      await loadPolicies();
    } catch (err) {
      setError(err?.response?.data?.message || "Policy guncellenemedi");
    } finally {
      setBusy("");
    }
  }

  async function handleApproveRequest(requestId) {
    if (!canRequestsApprove) return;
    const decisionComment = window.prompt("Onay notu (opsiyonel)", "") || "";
    setBusy(`approve-${requestId}`);
    setError("");
    setMessage("");
    try {
      const res = await approveBankApprovalRequest(requestId, { decisionComment });
      const status = res?.item?.request_status || "APPROVED";
      setMessage(`Talep #${requestId} onaylandi (${status})`);
      await reloadAll();
    } catch (err) {
      setError(err?.response?.data?.message || "Onay basarisiz");
    } finally {
      setBusy("");
    }
  }

  async function handleRejectRequest(requestId) {
    if (!canRequestsReject) return;
    const decisionComment = window.prompt("Red nedeni", "") || "";
    setBusy(`reject-${requestId}`);
    setError("");
    setMessage("");
    try {
      await rejectBankApprovalRequest(requestId, { decisionComment });
      setMessage(`Talep #${requestId} reddedildi`);
      await reloadAll();
    } catch (err) {
      setError(err?.response?.data?.message || "Red islemi basarisiz");
    } finally {
      setBusy("");
    }
  }

  return (
    <div className="space-y-4 p-4">
      <div className="rounded border bg-white p-4">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-lg font-semibold">Banka Onaylari (B09)</h1>
          <button
            type="button"
            className="ml-auto rounded border px-3 py-1 text-sm"
            onClick={reloadAll}
            disabled={loadingPolicies || loadingRequests || busy !== ""}
          >
            Yenile
          </button>
        </div>
        <p className="mt-1 text-sm text-slate-600">
          Banka islemleri icin governance katmani: policy (threshold + SoD) ve approval queue.
        </p>

        {error ? (
          <div className="mt-3 rounded border border-red-200 bg-red-50 p-3 text-sm text-red-700">
            {error}
          </div>
        ) : null}
        {message ? (
          <div className="mt-3 rounded border border-emerald-200 bg-emerald-50 p-3 text-sm text-emerald-700">
            {message}
          </div>
        ) : null}

        {!canPoliciesRead && !canRequestsRead ? (
          <div className="mt-3 rounded border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
            Missing permissions: <code>bank.approvals.policies.read</code> /{" "}
            <code>bank.approvals.requests.read</code>
          </div>
        ) : null}
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <section className="rounded border bg-white p-4">
          <div className="mb-2 flex items-center gap-2">
            <h2 className="font-medium">Policy Listesi</h2>
            <span className="text-xs text-slate-500">Toplam: {policiesTotal}</span>
          </div>

          <div className="grid gap-2 md:grid-cols-4">
            <input
              className="rounded border px-2 py-1 text-sm"
              placeholder="targetType"
              value={policyFilters.targetType}
              onChange={(e) => setPolicyFilters((p) => ({ ...p, targetType: e.target.value }))}
            />
            <input
              className="rounded border px-2 py-1 text-sm"
              placeholder="actionType"
              value={policyFilters.actionType}
              onChange={(e) => setPolicyFilters((p) => ({ ...p, actionType: e.target.value }))}
            />
            <input
              className="rounded border px-2 py-1 text-sm"
              placeholder="status"
              value={policyFilters.status}
              onChange={(e) => setPolicyFilters((p) => ({ ...p, status: e.target.value }))}
            />
            <div className="flex gap-2">
              <input
                className="w-full rounded border px-2 py-1 text-sm"
                placeholder="Ara"
                value={policyFilters.q}
                onChange={(e) => setPolicyFilters((p) => ({ ...p, q: e.target.value }))}
              />
              <button
                type="button"
                className="rounded border px-3 py-1 text-sm"
                disabled={loadingPolicies || !canPoliciesRead}
                onClick={loadPolicies}
              >
                Ara
              </button>
            </div>
          </div>

          <div className="mt-3 max-h-[360px] overflow-auto rounded border">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left">
                <tr>
                  <th className="px-2 py-1">Code</th>
                  <th className="px-2 py-1">Target/Action</th>
                  <th className="px-2 py-1">Scope</th>
                  <th className="px-2 py-1">Threshold</th>
                  <th className="px-2 py-1">SoD</th>
                  <th className="px-2 py-1">Status</th>
                  <th className="px-2 py-1">Action</th>
                </tr>
              </thead>
              <tbody>
                {loadingPolicies ? (
                  <tr>
                    <td className="px-2 py-2 text-slate-500" colSpan={7}>
                      Yukleniyor...
                    </td>
                  </tr>
                ) : policies.length === 0 ? (
                  <tr>
                    <td className="px-2 py-2 text-slate-500" colSpan={7}>
                      Policy kaydi yok.
                    </td>
                  </tr>
                ) : (
                  policies.map((row) => (
                    <tr key={row.id} className="border-t align-top">
                      <td className="px-2 py-1">
                        <div className="font-medium">{row.policy_code}</div>
                        <div className="text-xs text-slate-500">#{row.id}</div>
                      </td>
                      <td className="px-2 py-1">
                        <div>{row.target_type}</div>
                        <div className="text-xs text-slate-500">{row.action_type}</div>
                      </td>
                      <td className="px-2 py-1">
                        <div>{row.scope_type}</div>
                        <div className="text-xs text-slate-500">
                          LE:{row.legal_entity_id || "-"} BA:{row.bank_account_id || "-"}
                        </div>
                      </td>
                      <td className="px-2 py-1">
                        <div className="text-xs">
                          {row.currency_code || "*"} {formatAmount(row.min_amount)} -{" "}
                          {formatAmount(row.max_amount)}
                        </div>
                        <div className="text-xs text-slate-500">
                          approvals={row.required_approvals}
                        </div>
                      </td>
                      <td className="px-2 py-1 text-xs">
                        <div>{boolFromInput(row.maker_checker_required) ? "Yes" : "No"}</div>
                        <div className="text-slate-500">
                          {row.approver_permission_code || "-"}
                        </div>
                      </td>
                      <td className="px-2 py-1">
                        <span className="rounded border px-1 text-xs">{row.status}</span>
                      </td>
                      <td className="px-2 py-1">
                        <button
                          type="button"
                          className="rounded border px-2 py-0.5 text-xs"
                          disabled={!canPoliciesUpdate || busy === `policy-status-${row.id}`}
                          onClick={() => handleTogglePolicyStatus(row)}
                        >
                          {busy === `policy-status-${row.id}` ? "..." : row.status === "ACTIVE" ? "Pause" : "Activate"}
                        </button>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="rounded border bg-white p-4">
          <h2 className="mb-2 font-medium">Policy Olustur (B09)</h2>
          {canPoliciesCreate ? (
            <div className="grid gap-2 md:grid-cols-2">
              <input
                className="rounded border px-2 py-1 text-sm"
                placeholder="policyCode"
                value={newPolicy.policyCode}
                onChange={(e) => setNewPolicy((p) => ({ ...p, policyCode: e.target.value }))}
              />
              <input
                className="rounded border px-2 py-1 text-sm"
                placeholder="policyName"
                value={newPolicy.policyName}
                onChange={(e) => setNewPolicy((p) => ({ ...p, policyName: e.target.value }))}
              />
              <input
                className="rounded border px-2 py-1 text-sm"
                placeholder="targetType (PAYMENT_BATCH / RECON_RULE ...)"
                value={newPolicy.targetType}
                onChange={(e) => setNewPolicy((p) => ({ ...p, targetType: e.target.value }))}
              />
              <input
                className="rounded border px-2 py-1 text-sm"
                placeholder="actionType (SUBMIT_EXPORT / CREATE / UPDATE ...)"
                value={newPolicy.actionType}
                onChange={(e) => setNewPolicy((p) => ({ ...p, actionType: e.target.value }))}
              />
              <select
                className="rounded border px-2 py-1 text-sm"
                value={newPolicy.scopeType}
                onChange={(e) => setNewPolicy((p) => ({ ...p, scopeType: e.target.value }))}
              >
                <option value="GLOBAL">GLOBAL</option>
                <option value="LEGAL_ENTITY">LEGAL_ENTITY</option>
                <option value="BANK_ACCOUNT">BANK_ACCOUNT</option>
              </select>
              <input
                className="rounded border px-2 py-1 text-sm"
                type="number"
                min={1}
                placeholder="legalEntityId (opsiyonel)"
                value={newPolicy.legalEntityId}
                onChange={(e) => setNewPolicy((p) => ({ ...p, legalEntityId: e.target.value }))}
              />
              <input
                className="rounded border px-2 py-1 text-sm"
                type="number"
                min={1}
                placeholder="bankAccountId (opsiyonel)"
                value={newPolicy.bankAccountId}
                onChange={(e) => setNewPolicy((p) => ({ ...p, bankAccountId: e.target.value }))}
              />
              <input
                className="rounded border px-2 py-1 text-sm"
                placeholder="currencyCode (opsiyonel)"
                value={newPolicy.currencyCode}
                onChange={(e) => setNewPolicy((p) => ({ ...p, currencyCode: e.target.value }))}
              />
              <input
                className="rounded border px-2 py-1 text-sm"
                type="number"
                placeholder="minAmount (opsiyonel)"
                value={newPolicy.minAmount}
                onChange={(e) => setNewPolicy((p) => ({ ...p, minAmount: e.target.value }))}
              />
              <input
                className="rounded border px-2 py-1 text-sm"
                type="number"
                placeholder="maxAmount (opsiyonel)"
                value={newPolicy.maxAmount}
                onChange={(e) => setNewPolicy((p) => ({ ...p, maxAmount: e.target.value }))}
              />
              <input
                className="rounded border px-2 py-1 text-sm"
                type="number"
                min={1}
                placeholder="requiredApprovals"
                value={newPolicy.requiredApprovals}
                onChange={(e) => setNewPolicy((p) => ({ ...p, requiredApprovals: e.target.value }))}
              />
              <input
                className="rounded border px-2 py-1 text-sm"
                placeholder="approverPermissionCode"
                value={newPolicy.approverPermissionCode}
                onChange={(e) =>
                  setNewPolicy((p) => ({ ...p, approverPermissionCode: e.target.value }))
                }
              />
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={Boolean(newPolicy.makerCheckerRequired)}
                  onChange={(e) =>
                    setNewPolicy((p) => ({ ...p, makerCheckerRequired: e.target.checked }))
                  }
                />
                Maker-checker zorunlu
              </label>
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={Boolean(newPolicy.autoExecuteOnFinalApproval)}
                  onChange={(e) =>
                    setNewPolicy((p) => ({ ...p, autoExecuteOnFinalApproval: e.target.checked }))
                  }
                />
                Final onayda auto execute
              </label>
              <div className="md:col-span-2">
                <button
                  type="button"
                  className="rounded border px-3 py-1 text-sm"
                  disabled={busy === "create-policy"}
                  onClick={handleCreatePolicy}
                >
                  {busy === "create-policy" ? "Olusturuluyor..." : "Policy Olustur"}
                </button>
              </div>
            </div>
          ) : (
            <div className="text-sm text-slate-500">
              Missing permission: <code>bank.approvals.policies.create</code>
            </div>
          )}
        </section>
      </div>

      <section className="rounded border bg-white p-4">
        <div className="mb-2 flex items-center gap-2">
          <h2 className="font-medium">Onay Kuyrugu (Pending)</h2>
          <span className="text-xs text-slate-500">Toplam: {requestsTotal}</span>
        </div>
        {!canRequestsRead ? (
          <div className="text-sm text-slate-500">
            Missing permission: <code>bank.approvals.requests.read</code>
          </div>
        ) : loadingRequests ? (
          <div className="text-sm text-slate-500">Yukleniyor...</div>
        ) : requests.length === 0 ? (
          <div className="text-sm text-slate-500">Pending talep yok.</div>
        ) : (
          <div className="space-y-2">
            {requests.map((row) => (
              <div key={row.id} className="rounded border p-3 text-sm">
                <div className="flex flex-wrap items-center gap-2">
                  <div className="font-medium">{row.request_code}</div>
                  <span className="rounded border px-1 text-xs">{row.request_status}</span>
                  <span className="rounded border px-1 text-xs">{row.execution_status}</span>
                  <span className="rounded border px-1 text-xs">
                    {row.target_type}/{row.action_type}
                  </span>
                  <span className="ml-auto text-xs text-slate-500">
                    {formatDateTime(row.submitted_at)}
                  </span>
                </div>
                <div className="mt-1 grid gap-1 text-xs text-slate-600 md:grid-cols-3">
                  <div>
                    Target: #{row.target_id || "-"} | LE:{row.legal_entity_id || "-"} | BA:
                    {row.bank_account_id || "-"}
                  </div>
                  <div>
                    Threshold: {row.currency_code || "*"} {formatAmount(row.threshold_amount)}
                  </div>
                  <div>
                    Votes: approve {row.approve_count || 0} / reject {row.reject_count || 0}
                  </div>
                </div>
                {(canRequestsApprove || canRequestsReject) && row.request_status === "PENDING" ? (
                  <div className="mt-2 flex flex-wrap gap-2">
                    <button
                      type="button"
                      className="rounded border px-2 py-1 text-xs"
                      disabled={!canRequestsApprove || busy === `approve-${row.id}`}
                      onClick={() => handleApproveRequest(row.id)}
                    >
                      {busy === `approve-${row.id}` ? "Onay..." : "Approve"}
                    </button>
                    <button
                      type="button"
                      className="rounded border px-2 py-1 text-xs"
                      disabled={!canRequestsReject || busy === `reject-${row.id}`}
                      onClick={() => handleRejectRequest(row.id)}
                    >
                      {busy === `reject-${row.id}` ? "Red..." : "Reject"}
                    </button>
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
