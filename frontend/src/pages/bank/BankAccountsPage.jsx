import { useEffect, useMemo, useState } from "react";
import {
  activateBankAccount,
  createBankAccount,
  deactivateBankAccount,
  listBankAccounts,
  updateBankAccount,
} from "../../api/bankAccounts.js";
import {
  listBankConnectors,
  syncBankConnectorStatements,
  testBankConnectorConnection,
} from "../../api/bankConnectors.js";
import { listAccounts } from "../../api/glAdmin.js";
import { listCurrencies, listLegalEntities } from "../../api/orgAdmin.js";
import { useAuth } from "../../auth/useAuth.js";

const EMPTY_FORM = {
  id: "",
  legalEntityId: "",
  code: "",
  name: "",
  currencyCode: "",
  glAccountId: "",
  bankName: "",
  branchName: "",
  iban: "",
  accountNo: "",
  isActive: true,
};

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function mapRowToForm(row) {
  return {
    id: String(row?.id || ""),
    legalEntityId: String(row?.legal_entity_id || ""),
    code: String(row?.code || ""),
    name: String(row?.name || ""),
    currencyCode: String(row?.currency_code || "").toUpperCase(),
    glAccountId: String(row?.gl_account_id || ""),
    bankName: String(row?.bank_name || ""),
    branchName: String(row?.branch_name || ""),
    iban: String(row?.iban || ""),
    accountNo: String(row?.account_no || ""),
    isActive: parseDbBoolean(row?.is_active),
  };
}

export default function BankAccountsPage() {
  const { hasPermission } = useAuth();
  const canRead = hasPermission("bank.accounts.read");
  const canWrite = hasPermission("bank.accounts.write");
  const canReadConnectors = hasPermission("bank.connectors.read");
  const canSyncConnectors = hasPermission("bank.connectors.sync");
  const canReadOrgTree = hasPermission("org.tree.read");
  const canReadAccounts = hasPermission("gl.account.read");

  const [rows, setRows] = useState([]);
  const [connectorRows, setConnectorRows] = useState([]);
  const [legalEntities, setLegalEntities] = useState([]);
  const [currencies, setCurrencies] = useState([]);
  const [accounts, setAccounts] = useState([]);

  const [form, setForm] = useState(EMPTY_FORM);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [statusBusyId, setStatusBusyId] = useState(null);
  const [connectorLoading, setConnectorLoading] = useState(false);
  const [connectorActionBusy, setConnectorActionBusy] = useState({ testId: null, syncId: null });
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [connectorError, setConnectorError] = useState("");
  const [connectorMessage, setConnectorMessage] = useState("");
  const [lookupWarning, setLookupWarning] = useState("");

  const selectedLegalEntityId = toPositiveInt(form.legalEntityId);

  const legalEntityOptions = useMemo(
    () =>
      [...legalEntities].sort((a, b) =>
        String(a?.code || "").localeCompare(String(b?.code || ""))
      ),
    [legalEntities]
  );

  const currencyOptions = useMemo(
    () =>
      [...currencies].sort((a, b) =>
        String(a?.code || "").localeCompare(String(b?.code || ""))
      ),
    [currencies]
  );

  const glAccountOptions = useMemo(() => {
    const filtered = accounts.filter((row) => {
      if (!parseDbBoolean(row?.is_active) || !parseDbBoolean(row?.allow_posting)) {
        return false;
      }
      if (String(row?.scope || "").toUpperCase() !== "LEGAL_ENTITY") {
        return false;
      }
      if (String(row?.account_type || "").toUpperCase() !== "ASSET") {
        return false;
      }
      if (!selectedLegalEntityId) {
        return true;
      }
      return toPositiveInt(row?.legal_entity_id) === selectedLegalEntityId;
    });
    return [...filtered].sort((a, b) =>
      String(a?.code || "").localeCompare(String(b?.code || ""))
    );
  }, [accounts, selectedLegalEntityId]);

  useEffect(() => {
    if (form.id) {
      return;
    }
    if (!form.legalEntityId && legalEntityOptions.length > 0) {
      setForm((prev) => ({
        ...prev,
        legalEntityId: String(legalEntityOptions[0]?.id || ""),
      }));
    }
  }, [form.id, form.legalEntityId, legalEntityOptions]);

  useEffect(() => {
    if (form.id) {
      return;
    }
    if (!form.currencyCode && currencyOptions.length > 0) {
      setForm((prev) => ({
        ...prev,
        currencyCode: String(currencyOptions[0]?.code || "").toUpperCase(),
      }));
    }
  }, [currencyOptions, form.currencyCode, form.id]);

  async function loadRows() {
    if (!canRead) {
      setRows([]);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const response = await listBankAccounts({ limit: 200, offset: 0 });
      setRows(response?.rows || []);
    } catch (err) {
      setError(err?.response?.data?.message || "Failed to load bank accounts");
    } finally {
      setLoading(false);
    }
  }

  async function loadConnectors() {
    if (!canReadConnectors) {
      setConnectorRows([]);
      return;
    }

    setConnectorLoading(true);
    setConnectorError("");
    try {
      const response = await listBankConnectors({ limit: 100, offset: 0 });
      setConnectorRows(response?.rows || []);
    } catch (err) {
      setConnectorError(err?.response?.data?.message || "Failed to load bank connectors");
    } finally {
      setConnectorLoading(false);
    }
  }

  async function loadLookups() {
    if (!canWrite) {
      setLookupWarning("");
      return;
    }

    const warnings = [];

    if (canReadOrgTree) {
      try {
        const [leRes, curRes] = await Promise.all([listLegalEntities(), listCurrencies()]);
        setLegalEntities(leRes?.rows || []);
        setCurrencies(curRes?.rows || []);
      } catch (err) {
        warnings.push(err?.response?.data?.message || "Org/currency lookups could not be loaded");
        setLegalEntities([]);
        setCurrencies([]);
      }
    } else {
      warnings.push("Missing permission: org.tree.read (legal entity/currency lookups)");
      setLegalEntities([]);
      setCurrencies([]);
    }

    if (canReadAccounts) {
      try {
        const accountsRes = await listAccounts({ includeInactive: true });
        setAccounts(accountsRes?.rows || []);
      } catch (err) {
        warnings.push(err?.response?.data?.message || "GL account lookup could not be loaded");
        setAccounts([]);
      }
    } else {
      warnings.push("Missing permission: gl.account.read (GL account lookup)");
      setAccounts([]);
    }

    setLookupWarning(warnings.join(" | "));
  }

  useEffect(() => {
    loadRows();
    loadConnectors();
    loadLookups();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead, canWrite, canReadConnectors, canReadOrgTree, canReadAccounts]);

  function resetForm() {
    setForm((prev) => ({
      ...EMPTY_FORM,
      legalEntityId: prev.legalEntityId && !form.id ? prev.legalEntityId : "",
      currencyCode: prev.currencyCode && !form.id ? prev.currencyCode : "",
    }));
  }

  function startEdit(row) {
    setMessage("");
    setError("");
    setForm(mapRowToForm(row));
  }

  function buildPayloadFromForm() {
    const legalEntityId = toPositiveInt(form.legalEntityId);
    const glAccountId = toPositiveInt(form.glAccountId);
    if (!legalEntityId || !glAccountId) {
      throw new Error("legalEntityId and glAccountId are required");
    }
    return {
      legalEntityId,
      code: String(form.code || "").trim(),
      name: String(form.name || "").trim(),
      currencyCode: String(form.currencyCode || "").trim().toUpperCase(),
      glAccountId,
      bankName: String(form.bankName || "").trim() || null,
      branchName: String(form.branchName || "").trim() || null,
      iban: String(form.iban || "").trim() || null,
      accountNo: String(form.accountNo || "").trim() || null,
      isActive: Boolean(form.isActive),
    };
  }

  async function handleSubmit(event) {
    event.preventDefault();
    if (!canWrite) {
      setError("Missing permission: bank.accounts.write");
      return;
    }

    setSaving(true);
    setError("");
    setMessage("");
    try {
      const payload = buildPayloadFromForm();
      if (form.id) {
        await updateBankAccount(form.id, payload);
        setMessage("Bank account updated");
      } else {
        await createBankAccount(payload);
        setMessage("Bank account created");
      }
      resetForm();
      await loadRows();
    } catch (err) {
      setError(err?.response?.data?.message || err?.message || "Save failed");
    } finally {
      setSaving(false);
    }
  }

  async function handleStatusChange(row, nextActive) {
    if (!canWrite) {
      setError("Missing permission: bank.accounts.write");
      return;
    }

    const bankAccountId = toPositiveInt(row?.id);
    if (!bankAccountId) {
      setError("Invalid bankAccountId");
      return;
    }

    setStatusBusyId(bankAccountId);
    setError("");
    setMessage("");
    try {
      if (nextActive) {
        await activateBankAccount(bankAccountId);
        setMessage("Bank account activated");
      } else {
        await deactivateBankAccount(bankAccountId);
        setMessage("Bank account deactivated");
      }
      await loadRows();
      if (String(form.id || "") === String(bankAccountId)) {
        setForm((prev) => ({ ...prev, isActive: Boolean(nextActive) }));
      }
    } catch (err) {
      setError(err?.response?.data?.message || "Status update failed");
    } finally {
      setStatusBusyId(null);
    }
  }

  async function handleConnectorTest(row) {
    if (!canSyncConnectors) {
      setConnectorError("Missing permission: bank.connectors.sync");
      return;
    }
    const connectorId = toPositiveInt(row?.id);
    if (!connectorId) {
      setConnectorError("Invalid connectorId");
      return;
    }
    setConnectorActionBusy((prev) => ({ ...prev, testId: connectorId }));
    setConnectorError("");
    setConnectorMessage("");
    try {
      const response = await testBankConnectorConnection(connectorId);
      setConnectorMessage(
        response?.result?.ok
          ? `Connector test ok (${row?.connector_code || connectorId})`
          : `Connector test completed (${row?.connector_code || connectorId})`
      );
      await loadConnectors();
    } catch (err) {
      setConnectorError(err?.response?.data?.message || "Connector test failed");
    } finally {
      setConnectorActionBusy((prev) => ({ ...prev, testId: null }));
    }
  }

  async function handleConnectorSync(row) {
    if (!canSyncConnectors) {
      setConnectorError("Missing permission: bank.connectors.sync");
      return;
    }
    const connectorId = toPositiveInt(row?.id);
    if (!connectorId) {
      setConnectorError("Invalid connectorId");
      return;
    }
    setConnectorActionBusy((prev) => ({ ...prev, syncId: connectorId }));
    setConnectorError("");
    setConnectorMessage("");
    try {
      const response = await syncBankConnectorStatements(connectorId, {});
      const syncStatus = response?.sync_run?.status || "DONE";
      setConnectorMessage(
        `Connector sync completed (${row?.connector_code || connectorId}) - ${syncStatus}`
      );
      await loadConnectors();
    } catch (err) {
      setConnectorError(err?.response?.data?.message || "Connector sync failed");
    } finally {
      setConnectorActionBusy((prev) => ({ ...prev, syncId: null }));
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">Banka Tanimla</h1>
        <p className="mt-1 text-sm text-slate-600">
          Banka hesap ana verisi ve GL hesap baglantisi (PR-B01).
        </p>
      </div>

      {!canRead ? (
        <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          Missing permission: <code>bank.accounts.read</code>
        </div>
      ) : null}

      {error ? (
        <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
          {error}
        </div>
      ) : null}

      {message ? (
        <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
          {message}
        </div>
      ) : null}

      {lookupWarning ? (
        <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          {lookupWarning}
        </div>
      ) : null}

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,1.4fr)]">
        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-sm font-semibold text-slate-900">
              {form.id ? "Banka Hesabi Duzenle" : "Yeni Banka Hesabi"}
            </h2>
            <button
              type="button"
              onClick={() => {
                setForm(EMPTY_FORM);
                setError("");
                setMessage("");
              }}
              className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700"
              disabled={saving}
            >
              Temizle
            </button>
          </div>

          <form className="space-y-3" onSubmit={handleSubmit}>
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">Legal Entity</label>
              <select
                value={form.legalEntityId}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    legalEntityId: event.target.value,
                    glAccountId: "",
                  }))
                }
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                disabled={!canWrite || saving || !canReadOrgTree}
                required
              >
                <option value="">Secin</option>
                {legalEntityOptions.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.code} - {row.name}
                  </option>
                ))}
              </select>
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Code</label>
                <input
                  value={form.code}
                  onChange={(event) => setForm((prev) => ({ ...prev, code: event.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="BANK_TRY_MAIN"
                  disabled={!canWrite || saving}
                  required
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Currency</label>
                <select
                  value={form.currencyCode}
                  onChange={(event) =>
                    setForm((prev) => ({ ...prev, currencyCode: event.target.value }))
                  }
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canWrite || saving || !canReadOrgTree}
                  required
                >
                  <option value="">Secin</option>
                  {currencyOptions.map((row) => (
                    <option key={row.code} value={row.code}>
                      {row.code} - {row.name}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">Name</label>
              <input
                value={form.name}
                onChange={(event) => setForm((prev) => ({ ...prev, name: event.target.value }))}
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="Ana Banka Hesabi"
                disabled={!canWrite || saving}
                required
              />
            </div>

            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">GL Account</label>
              <select
                value={form.glAccountId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, glAccountId: event.target.value }))
                }
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                disabled={!canWrite || saving || !canReadAccounts}
                required
              >
                <option value="">Secin</option>
                {glAccountOptions.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.code} - {row.name}
                  </option>
                ))}
              </select>
              <p className="mt-1 text-xs text-slate-500">
                Only ACTIVE, postable, LEGAL_ENTITY-scoped ASSET accounts are listed.
              </p>
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Bank Name</label>
                <input
                  value={form.bankName}
                  onChange={(event) =>
                    setForm((prev) => ({ ...prev, bankName: event.target.value }))
                  }
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canWrite || saving}
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Branch Name</label>
                <input
                  value={form.branchName}
                  onChange={(event) =>
                    setForm((prev) => ({ ...prev, branchName: event.target.value }))
                  }
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canWrite || saving}
                />
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">IBAN</label>
                <input
                  value={form.iban}
                  onChange={(event) => setForm((prev) => ({ ...prev, iban: event.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canWrite || saving}
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Account No</label>
                <input
                  value={form.accountNo}
                  onChange={(event) =>
                    setForm((prev) => ({ ...prev, accountNo: event.target.value }))
                  }
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canWrite || saving}
                />
              </div>
            </div>

            <label className="flex items-center gap-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={Boolean(form.isActive)}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, isActive: event.target.checked }))
                }
                disabled={!canWrite || saving}
              />
              Active
            </label>

            <button
              type="submit"
              disabled={!canWrite || saving}
              className="w-full rounded bg-slate-900 px-3 py-2 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-60"
            >
              {saving ? "Kaydediliyor..." : form.id ? "Guncelle" : "Olustur"}
            </button>
          </form>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-sm font-semibold text-slate-900">Banka Hesaplari</h2>
            <button
              type="button"
              onClick={loadRows}
              disabled={loading || !canRead}
              className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 disabled:opacity-60"
            >
              {loading ? "Yukleniyor..." : "Yenile"}
            </button>
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full text-left text-sm">
              <thead className="text-xs uppercase tracking-wide text-slate-500">
                <tr>
                  <th className="px-2 py-2">Code</th>
                  <th className="px-2 py-2">Name</th>
                  <th className="px-2 py-2">LE</th>
                  <th className="px-2 py-2">Currency</th>
                  <th className="px-2 py-2">GL</th>
                  <th className="px-2 py-2">Status</th>
                  <th className="px-2 py-2 text-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => {
                  const rowId = toPositiveInt(row?.id);
                  const active = parseDbBoolean(row?.is_active);
                  return (
                    <tr key={rowId || row.code} className="border-t border-slate-100 align-top">
                      <td className="px-2 py-2 font-medium text-slate-800">{row.code}</td>
                      <td className="px-2 py-2 text-slate-700">
                        <div>{row.name}</div>
                        {(row.bank_name || row.branch_name) && (
                          <div className="text-xs text-slate-500">
                            {[row.bank_name, row.branch_name].filter(Boolean).join(" / ")}
                          </div>
                        )}
                        {row.iban ? (
                          <div className="font-mono text-xs text-slate-500">{row.iban}</div>
                        ) : null}
                      </td>
                      <td className="px-2 py-2 text-slate-700">
                        {row.legal_entity_code || "-"}
                        {row.legal_entity_name ? (
                          <div className="text-xs text-slate-500">{row.legal_entity_name}</div>
                        ) : null}
                      </td>
                      <td className="px-2 py-2 text-slate-700">{row.currency_code}</td>
                      <td className="px-2 py-2 text-slate-700">
                        {row.gl_account_code || "-"}
                        {row.gl_account_name ? (
                          <div className="text-xs text-slate-500">{row.gl_account_name}</div>
                        ) : null}
                      </td>
                      <td className="px-2 py-2">
                        <span
                          className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${
                            active
                              ? "bg-emerald-100 text-emerald-800"
                              : "bg-slate-200 text-slate-700"
                          }`}
                        >
                          {active ? "ACTIVE" : "INACTIVE"}
                        </span>
                      </td>
                      <td className="px-2 py-2">
                        <div className="flex justify-end gap-2">
                          <button
                            type="button"
                            onClick={() => startEdit(row)}
                            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700"
                            disabled={!canWrite || saving}
                          >
                            Edit
                          </button>
                          <button
                            type="button"
                            onClick={() => handleStatusChange(row, !active)}
                            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700"
                            disabled={!canWrite || statusBusyId === rowId}
                          >
                            {statusBusyId === rowId
                              ? "..."
                              : active
                                ? "Deactivate"
                                : "Activate"}
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
                {rows.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="px-2 py-6 text-center text-sm text-slate-500">
                      {loading ? "Yukleniyor..." : "Banka hesabi bulunamadi."}
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>
      </div>

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="mb-3 flex items-start justify-between gap-4">
          <div>
            <h2 className="text-sm font-semibold text-slate-900">Bank Connectors (B05)</h2>
            <p className="mt-1 text-xs text-slate-600">
              Live bank connectivity adapter list + test/sync actions. Connector CRUD/mapping APIs are
              ready under <code>/api/v1/bank/connectors</code>.
            </p>
          </div>
          <button
            type="button"
            onClick={loadConnectors}
            disabled={!canReadConnectors || connectorLoading}
            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 disabled:opacity-60"
          >
            {connectorLoading ? "Yukleniyor..." : "Yenile"}
          </button>
        </div>

        {!canReadConnectors ? (
          <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
            Missing permission: <code>bank.connectors.read</code>
          </div>
        ) : null}

        {connectorError ? (
          <div className="mb-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
            {connectorError}
          </div>
        ) : null}

        {connectorMessage ? (
          <div className="mb-3 rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
            {connectorMessage}
          </div>
        ) : null}

        <div className="overflow-x-auto">
          <table className="min-w-full text-left text-sm">
            <thead className="text-xs uppercase tracking-wide text-slate-500">
              <tr>
                <th className="px-2 py-2">Code</th>
                <th className="px-2 py-2">Provider</th>
                <th className="px-2 py-2">LE</th>
                <th className="px-2 py-2">Status</th>
                <th className="px-2 py-2">Sync</th>
                <th className="px-2 py-2 text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {connectorRows.map((row) => {
                const connectorId = toPositiveInt(row?.id);
                const testBusy = connectorActionBusy.testId === connectorId;
                const syncBusy = connectorActionBusy.syncId === connectorId;
                return (
                  <tr key={connectorId || row.connector_code} className="border-t border-slate-100">
                    <td className="px-2 py-2">
                      <div className="font-medium text-slate-800">{row.connector_code}</div>
                      <div className="text-xs text-slate-500">{row.connector_name}</div>
                    </td>
                    <td className="px-2 py-2 text-slate-700">
                      <div>{row.provider_code}</div>
                      <div className="text-xs text-slate-500">{row.connector_type}</div>
                    </td>
                    <td className="px-2 py-2 text-slate-700">
                      {row.legal_entity_code || "-"}
                      {row.legal_entity_name ? (
                        <div className="text-xs text-slate-500">{row.legal_entity_name}</div>
                      ) : null}
                    </td>
                    <td className="px-2 py-2">
                      <span className="inline-flex rounded-full bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-800">
                        {row.status || "-"}
                      </span>
                      <div className="mt-1 text-xs text-slate-500">
                        Links: {Number(row.account_link_count || 0)}
                      </div>
                    </td>
                    <td className="px-2 py-2 text-xs text-slate-600">
                      <div>{row.sync_mode || "-"}</div>
                      {row.last_sync_at ? <div>Last: {String(row.last_sync_at)}</div> : null}
                      {row.next_sync_at ? <div>Next: {String(row.next_sync_at)}</div> : null}
                    </td>
                    <td className="px-2 py-2">
                      <div className="flex justify-end gap-2">
                        <button
                          type="button"
                          onClick={() => handleConnectorTest(row)}
                          disabled={!canSyncConnectors || testBusy || syncBusy}
                          className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 disabled:opacity-60"
                        >
                          {testBusy ? "..." : "Test"}
                        </button>
                        <button
                          type="button"
                          onClick={() => handleConnectorSync(row)}
                          disabled={!canSyncConnectors || testBusy || syncBusy}
                          className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 disabled:opacity-60"
                        >
                          {syncBusy ? "..." : "Sync Now"}
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
              {connectorRows.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-2 py-6 text-center text-sm text-slate-500">
                    {connectorLoading ? "Yukleniyor..." : "Connector bulunamadi (B05)."}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
