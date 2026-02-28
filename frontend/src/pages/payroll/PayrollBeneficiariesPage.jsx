import { useMemo, useState } from "react";
import { useAuth } from "../../auth/useAuth.js";
import {
  createPayrollBeneficiaryAccount,
  listPayrollBeneficiaryAccounts,
  setPrimaryPayrollBeneficiaryAccount,
  updatePayrollBeneficiaryAccount,
} from "../../api/payrollBeneficiaries.js";

function formatDate(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toISOString().slice(0, 10);
}

function toPatchForm(row) {
  if (!row) {
    return {
      accountHolderName: "",
      bankName: "",
      iban: "",
      accountNumber: "",
      status: "ACTIVE",
      verificationStatus: "UNVERIFIED",
      reason: "Updated via payroll-beneficiaries page",
    };
  }
  return {
    accountHolderName: row.account_holder_name || "",
    bankName: row.bank_name || "",
    iban: row.iban || "",
    accountNumber: row.account_number || "",
    status: row.status || "ACTIVE",
    verificationStatus: row.verification_status || "UNVERIFIED",
    reason: "Updated via payroll-beneficiaries page",
  };
}

export default function PayrollBeneficiariesPage() {
  const { hasPermission } = useAuth();
  const canRead = hasPermission("payroll.beneficiary.read");
  const canWrite = hasPermission("payroll.beneficiary.write");
  const canSetPrimary = hasPermission("payroll.beneficiary.set_primary");

  const [filters, setFilters] = useState({
    legalEntityId: "",
    employeeCode: "",
    currencyCode: "",
    status: "",
  });
  const [createForm, setCreateForm] = useState({
    legalEntityId: "",
    employeeCode: "",
    employeeName: "",
    accountHolderName: "",
    bankName: "",
    currencyCode: "TRY",
    iban: "",
    accountNumber: "",
    isPrimary: true,
    verificationStatus: "UNVERIFIED",
  });
  const [rows, setRows] = useState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [patchForm, setPatchForm] = useState(toPatchForm(null));
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [rowBusyId, setRowBusyId] = useState(null);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const selectedRow = useMemo(
    () => rows.find((row) => Number(row.id) === Number(selectedId)) || null,
    [rows, selectedId]
  );

  async function loadRows(overrideFilters = null) {
    const activeFilters = overrideFilters || filters;
    if (!canRead) return;
    if (!activeFilters.legalEntityId || !activeFilters.employeeCode) {
      setError("legalEntityId ve employeeCode gerekli.");
      setRows([]);
      return;
    }
    setLoading(true);
    setError("");
    setMessage("");
    try {
      const res = await listPayrollBeneficiaryAccounts({
        legalEntityId: activeFilters.legalEntityId,
        employeeCode: activeFilters.employeeCode,
        currencyCode: activeFilters.currencyCode || undefined,
        status: activeFilters.status || undefined,
      });
      const items = res?.items || [];
      setRows(items);
      if (items.length > 0) {
        const nextSelected =
          items.find((row) => Number(row.id) === Number(selectedId))?.id || items[0].id;
        setSelectedId(nextSelected);
        const row = items.find((x) => Number(x.id) === Number(nextSelected));
        setPatchForm(toPatchForm(row));
      } else {
        setSelectedId(null);
        setPatchForm(toPatchForm(null));
      }
    } catch (err) {
      setRows([]);
      setError(err?.response?.data?.message || "Payroll beneficiary listesi yuklenemedi");
    } finally {
      setLoading(false);
    }
  }

  function selectRow(row) {
    setSelectedId(row.id);
    setPatchForm(toPatchForm(row));
  }

  async function handleCreate(e) {
    e.preventDefault();
    if (!canWrite) return;
    setSaving(true);
    setError("");
    setMessage("");
    try {
      const nextLookupFilters = {
        legalEntityId: createForm.legalEntityId || filters.legalEntityId,
        employeeCode: (createForm.employeeCode || "").toUpperCase() || filters.employeeCode,
        currencyCode: createForm.currencyCode || filters.currencyCode,
        status: filters.status,
      };
      await createPayrollBeneficiaryAccount({
        ...createForm,
        legalEntityId: Number(createForm.legalEntityId),
      });
      setMessage("Beneficiary hesap olusturuldu.");
      setFilters((prev) => ({ ...prev, ...nextLookupFilters }));
      await loadRows(nextLookupFilters);
    } catch (err) {
      setError(err?.response?.data?.message || "Beneficiary hesap olusturulamadi");
    } finally {
      setSaving(false);
    }
  }

  async function handleUpdate(e) {
    e.preventDefault();
    if (!canWrite || !selectedRow) return;
    setSaving(true);
    setError("");
    setMessage("");
    try {
      await updatePayrollBeneficiaryAccount(selectedRow.id, patchForm);
      setMessage(`Hesap #${selectedRow.id} guncellendi.`);
      await loadRows();
    } catch (err) {
      setError(err?.response?.data?.message || "Beneficiary hesap guncellenemedi");
    } finally {
      setSaving(false);
    }
  }

  async function handleSetPrimary(row) {
    if (!canSetPrimary) return;
    const reason = window.prompt("Reason (optional)", "Set primary via UI");
    if (reason === null) return;
    setRowBusyId(Number(row.id));
    setError("");
    setMessage("");
    try {
      await setPrimaryPayrollBeneficiaryAccount(row.id, { reason });
      setMessage(`Hesap #${row.id} primary yapildi.`);
      await loadRows();
    } catch (err) {
      setError(err?.response?.data?.message || "Primary atama basarisiz");
    } finally {
      setRowBusyId(null);
    }
  }

  async function handleToggleStatus(row) {
    if (!canWrite) return;
    const nextStatus = String(row.status || "").toUpperCase() === "ACTIVE" ? "INACTIVE" : "ACTIVE";
    setRowBusyId(Number(row.id));
    setError("");
    setMessage("");
    try {
      await updatePayrollBeneficiaryAccount(row.id, {
        status: nextStatus,
        reason: `${nextStatus} via UI`,
      });
      setMessage(`Hesap #${row.id} status=${nextStatus}.`);
      await loadRows();
    } catch (err) {
      setError(err?.response?.data?.message || "Status guncellenemedi");
    } finally {
      setRowBusyId(null);
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">Payroll Beneficiaries</h1>
        <p className="mt-1 text-sm text-slate-600">
          PR-P07 employee_code bazli beneficiary bank master CRUD + set-primary.
        </p>
      </div>

      {!canRead ? (
        <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          Missing permission: <code>payroll.beneficiary.read</code>
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

      <div className="grid gap-6 xl:grid-cols-[1.2fr_1.2fr]">
        <div className="space-y-6">
          <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <h2 className="text-sm font-semibold text-slate-900">Lookup</h2>
            <div className="mt-3 grid gap-3 md:grid-cols-2">
              <label className="text-xs">
                <span className="mb-1 block font-medium text-slate-700">Legal Entity ID *</span>
                <input
                  value={filters.legalEntityId}
                  onChange={(e) => setFilters((p) => ({ ...p, legalEntityId: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                />
              </label>
              <label className="text-xs">
                <span className="mb-1 block font-medium text-slate-700">Employee Code *</span>
                <input
                  value={filters.employeeCode}
                  onChange={(e) => setFilters((p) => ({ ...p, employeeCode: e.target.value.toUpperCase() }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                />
              </label>
              <label className="text-xs">
                <span className="mb-1 block font-medium text-slate-700">Currency</span>
                <input
                  value={filters.currencyCode}
                  onChange={(e) => setFilters((p) => ({ ...p, currencyCode: e.target.value.toUpperCase() }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                />
              </label>
              <label className="text-xs">
                <span className="mb-1 block font-medium text-slate-700">Status</span>
                <select
                  value={filters.status}
                  onChange={(e) => setFilters((p) => ({ ...p, status: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                >
                  <option value="">ALL</option>
                  <option value="ACTIVE">ACTIVE</option>
                  <option value="INACTIVE">INACTIVE</option>
                </select>
              </label>
            </div>
            <div className="mt-3 flex gap-2">
              <button
                type="button"
                onClick={loadRows}
                disabled={!canRead || loading}
                className="rounded border border-slate-300 px-3 py-1.5 text-sm"
              >
                {loading ? "Loading..." : "Load"}
              </button>
              <button
                type="button"
                onClick={() =>
                  setCreateForm((p) => ({
                    ...p,
                    legalEntityId: filters.legalEntityId || p.legalEntityId,
                    employeeCode: filters.employeeCode || p.employeeCode,
                    currencyCode: filters.currencyCode || p.currencyCode,
                  }))
                }
                className="rounded border border-slate-300 px-3 py-1.5 text-sm"
              >
                Copy To Create
              </button>
            </div>

            <div className="mt-4 overflow-auto">
              <table className="min-w-full border-collapse text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="p-2 text-left">ID</th>
                    <th className="p-2 text-left">Bank</th>
                    <th className="p-2 text-left">Account</th>
                    <th className="p-2 text-left">Status</th>
                    <th className="p-2 text-left">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row) => (
                    <tr
                      key={row.id}
                      className={`border-b ${Number(row.id) === Number(selectedId) ? "bg-slate-50" : ""}`}
                    >
                      <td className="p-2">
                        <button type="button" className="underline" onClick={() => selectRow(row)}>
                          {row.id}
                        </button>
                      </td>
                      <td className="p-2">
                        <div>{row.bank_name}</div>
                        <div className="text-xs text-slate-500">{row.account_holder_name}</div>
                      </td>
                      <td className="p-2">
                        <div>{row.currency_code}</div>
                        <div className="text-xs text-slate-500">
                          {row.iban ? `IBAN ****${String(row.iban).slice(-4)}` : `ACCT ****${row.account_last4 || "?"}`}
                        </div>
                      </td>
                      <td className="p-2">
                        <div>{row.status}</div>
                        <div className="text-xs text-slate-500">
                          {row.is_primary ? "PRIMARY" : "secondary"} / {row.verification_status}
                        </div>
                        <div className="text-xs text-slate-500">
                          {formatDate(row.effective_from)} - {formatDate(row.effective_to)}
                        </div>
                      </td>
                      <td className="p-2">
                        <div className="flex flex-wrap gap-2">
                          <button
                            type="button"
                            onClick={() => selectRow(row)}
                            className="rounded border border-slate-300 px-2 py-1 text-xs"
                          >
                            Edit
                          </button>
                          {canSetPrimary ? (
                            <button
                              type="button"
                              onClick={() => handleSetPrimary(row)}
                              disabled={rowBusyId === Number(row.id) || row.is_primary}
                              className="rounded border border-slate-300 px-2 py-1 text-xs"
                            >
                              Set Primary
                            </button>
                          ) : null}
                          {canWrite ? (
                            <button
                              type="button"
                              onClick={() => handleToggleStatus(row)}
                              disabled={rowBusyId === Number(row.id)}
                              className="rounded border border-slate-300 px-2 py-1 text-xs"
                            >
                              {String(row.status).toUpperCase() === "ACTIVE" ? "Deactivate" : "Activate"}
                            </button>
                          ) : null}
                        </div>
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

          <form onSubmit={handleCreate} className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <h2 className="text-sm font-semibold text-slate-900">Create Account</h2>
            <div className="mt-3 grid gap-3 md:grid-cols-2">
              <input
                value={createForm.legalEntityId}
                onChange={(e) => setCreateForm((p) => ({ ...p, legalEntityId: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="legalEntityId"
                required
              />
              <input
                value={createForm.employeeCode}
                onChange={(e) => setCreateForm((p) => ({ ...p, employeeCode: e.target.value.toUpperCase() }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="employeeCode"
                required
              />
              <input
                value={createForm.employeeName}
                onChange={(e) => setCreateForm((p) => ({ ...p, employeeName: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="employeeName"
              />
              <input
                value={createForm.currencyCode}
                onChange={(e) => setCreateForm((p) => ({ ...p, currencyCode: e.target.value.toUpperCase() }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="currencyCode"
                required
              />
              <input
                value={createForm.accountHolderName}
                onChange={(e) => setCreateForm((p) => ({ ...p, accountHolderName: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="accountHolderName *"
                required
              />
              <input
                value={createForm.bankName}
                onChange={(e) => setCreateForm((p) => ({ ...p, bankName: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="bankName *"
                required
              />
              <input
                value={createForm.iban}
                onChange={(e) => setCreateForm((p) => ({ ...p, iban: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm md:col-span-2"
                placeholder="IBAN (or accountNumber)"
              />
              <input
                value={createForm.accountNumber}
                onChange={(e) => setCreateForm((p) => ({ ...p, accountNumber: e.target.value }))}
                className="rounded border border-slate-300 px-2 py-1.5 text-sm md:col-span-2"
                placeholder="accountNumber (or IBAN)"
              />
              <label className="flex items-center gap-2 text-xs md:col-span-2">
                <input
                  type="checkbox"
                  checked={Boolean(createForm.isPrimary)}
                  onChange={(e) => setCreateForm((p) => ({ ...p, isPrimary: e.target.checked }))}
                />
                <span>isPrimary</span>
              </label>
            </div>
            <div className="mt-3">
              <button
                type="submit"
                disabled={!canWrite || saving}
                className="rounded border border-slate-300 px-3 py-1.5 text-sm"
              >
                {saving ? "Saving..." : "Create"}
              </button>
            </div>
          </form>
        </div>

        <form onSubmit={handleUpdate} className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <h2 className="text-sm font-semibold text-slate-900">Update Selected</h2>
          <p className="mt-1 text-xs text-slate-500">
            {selectedRow
              ? `#${selectedRow.id} | ${selectedRow.employee_code} | ${selectedRow.currency_code}`
              : "Select a row from the list."}
          </p>
          {!selectedRow ? (
            <div className="mt-4 rounded border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-600">
              No row selected.
            </div>
          ) : (
            <>
              <div className="mt-3 grid gap-3 md:grid-cols-2">
                <input
                  value={patchForm.accountHolderName}
                  onChange={(e) => setPatchForm((p) => ({ ...p, accountHolderName: e.target.value }))}
                  className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="accountHolderName"
                />
                <input
                  value={patchForm.bankName}
                  onChange={(e) => setPatchForm((p) => ({ ...p, bankName: e.target.value }))}
                  className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="bankName"
                />
                <input
                  value={patchForm.iban}
                  onChange={(e) => setPatchForm((p) => ({ ...p, iban: e.target.value }))}
                  className="rounded border border-slate-300 px-2 py-1.5 text-sm md:col-span-2"
                  placeholder="IBAN"
                />
                <input
                  value={patchForm.accountNumber}
                  onChange={(e) => setPatchForm((p) => ({ ...p, accountNumber: e.target.value }))}
                  className="rounded border border-slate-300 px-2 py-1.5 text-sm md:col-span-2"
                  placeholder="accountNumber"
                />
                <select
                  value={patchForm.status}
                  onChange={(e) => setPatchForm((p) => ({ ...p, status: e.target.value }))}
                  className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                >
                  <option value="ACTIVE">ACTIVE</option>
                  <option value="INACTIVE">INACTIVE</option>
                </select>
                <select
                  value={patchForm.verificationStatus}
                  onChange={(e) =>
                    setPatchForm((p) => ({ ...p, verificationStatus: e.target.value }))
                  }
                  className="rounded border border-slate-300 px-2 py-1.5 text-sm"
                >
                  <option value="UNVERIFIED">UNVERIFIED</option>
                  <option value="VERIFIED">VERIFIED</option>
                </select>
                <input
                  value={patchForm.reason}
                  onChange={(e) => setPatchForm((p) => ({ ...p, reason: e.target.value }))}
                  className="rounded border border-slate-300 px-2 py-1.5 text-sm md:col-span-2"
                  placeholder="reason"
                />
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                <button
                  type="submit"
                  disabled={!canWrite || saving}
                  className="rounded border border-slate-300 px-3 py-1.5 text-sm"
                >
                  {saving ? "Saving..." : "Update"}
                </button>
                {canSetPrimary ? (
                  <button
                    type="button"
                    onClick={() => handleSetPrimary(selectedRow)}
                    disabled={rowBusyId === Number(selectedRow.id)}
                    className="rounded border border-slate-300 px-3 py-1.5 text-sm"
                  >
                    Set Primary
                  </button>
                ) : null}
                {canWrite ? (
                  <button
                    type="button"
                    onClick={() => handleToggleStatus(selectedRow)}
                    disabled={rowBusyId === Number(selectedRow.id)}
                    className="rounded border border-slate-300 px-3 py-1.5 text-sm"
                  >
                    Toggle Active
                  </button>
                ) : null}
              </div>
              <div className="mt-4 rounded border border-slate-200 bg-slate-50 p-3 text-xs text-slate-600">
                Delete endpoint is not available yet; use <code>INACTIVE</code> as soft-delete.
              </div>
            </>
          )}
        </form>
      </div>
    </div>
  );
}
