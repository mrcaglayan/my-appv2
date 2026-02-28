import { useEffect, useMemo, useState } from "react";
import { listBankAccounts } from "../../api/bankAccounts.js";
import { listBankStatementImports, listBankStatementLines } from "../../api/bankStatements.js";
import { useAuth } from "../../auth/useAuth.js";

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function formatDate(value) {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }
  return parsed.toLocaleDateString();
}

function formatAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return "-";
  }
  return parsed.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 6,
  });
}

export default function BankStatementQueuePage() {
  const { hasPermission } = useAuth();
  const canRead = hasPermission("bank.statements.read");
  const canReadBanks = hasPermission("bank.accounts.read");

  const [bankAccounts, setBankAccounts] = useState([]);
  const [imports, setImports] = useState([]);
  const [lines, setLines] = useState([]);
  const [selectedImportId, setSelectedImportId] = useState("");
  const [loadingImports, setLoadingImports] = useState(false);
  const [loadingLines, setLoadingLines] = useState(false);
  const [error, setError] = useState("");
  const [filters, setFilters] = useState({
    bankAccountId: "",
    status: "",
    reconStatus: "UNMATCHED",
  });

  const bankOptions = useMemo(
    () =>
      [...(bankAccounts || [])].sort((a, b) =>
        String(a?.code || "").localeCompare(String(b?.code || ""))
      ),
    [bankAccounts]
  );

  async function loadImportsAndPickFirst() {
    if (!canRead) {
      setImports([]);
      setLines([]);
      return;
    }
    setLoadingImports(true);
    setError("");
    try {
      const res = await listBankStatementImports({
        limit: 100,
        offset: 0,
        bankAccountId: toPositiveInt(filters.bankAccountId) || undefined,
        status: filters.status || undefined,
      });
      const rows = res?.rows || [];
      setImports(rows);
      const nextId = rows[0]?.id ? String(rows[0].id) : "";
      setSelectedImportId(nextId);
      await loadLines(nextId);
    } catch (err) {
      setError(err?.response?.data?.message || "Failed to load statement imports");
      setImports([]);
    } finally {
      setLoadingImports(false);
    }
  }

  async function loadLines(importIdToUse = selectedImportId) {
    if (!canRead) {
      setLines([]);
      return;
    }
    setLoadingLines(true);
    try {
      const res = await listBankStatementLines({
        limit: 200,
        offset: 0,
        importId: toPositiveInt(importIdToUse) || undefined,
        bankAccountId:
          toPositiveInt(importIdToUse) ? undefined : toPositiveInt(filters.bankAccountId) || undefined,
        reconStatus: filters.reconStatus || undefined,
      });
      setLines(res?.rows || []);
    } catch (err) {
      setError(err?.response?.data?.message || "Failed to load statement lines");
      setLines([]);
    } finally {
      setLoadingLines(false);
    }
  }

  useEffect(() => {
    let cancelled = false;
    if (!canReadBanks) {
      return undefined;
    }
    (async () => {
      try {
        const res = await listBankAccounts({ limit: 300, offset: 0 });
        if (!cancelled) {
          setBankAccounts(res?.rows || []);
        }
      } catch {
        if (!cancelled) {
          setBankAccounts([]);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [canReadBanks]);

  useEffect(() => {
    loadImportsAndPickFirst();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead]);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">Banka Ekstre Kuyrugu</h1>
        <p className="mt-1 text-sm text-slate-600">
          PR-B02 statement importlar ve normalized line queue.
        </p>
      </div>

      {!canRead ? (
        <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          Missing permission: <code>bank.statements.read</code>
        </div>
      ) : null}
      {error ? (
        <div className="rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
          {error}
        </div>
      ) : null}

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <form
          onSubmit={(e) => {
            e.preventDefault();
            loadImportsAndPickFirst();
          }}
          className="grid gap-3 lg:grid-cols-[2fr_1fr_1fr_auto]"
        >
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-700">Bank Account</label>
            {canReadBanks ? (
              <select
                value={filters.bankAccountId}
                onChange={(e) => setFilters((p) => ({ ...p, bankAccountId: e.target.value }))}
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
              >
                <option value="">Tum banka hesaplari</option>
                {bankOptions.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.code} - {row.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                value={filters.bankAccountId}
                onChange={(e) => setFilters((p) => ({ ...p, bankAccountId: e.target.value }))}
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="bankAccountId"
              />
            )}
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-700">Import Status</label>
            <select
              value={filters.status}
              onChange={(e) => setFilters((p) => ({ ...p, status: e.target.value }))}
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
            >
              <option value="">Tum</option>
              <option value="IMPORTED">IMPORTED</option>
              <option value="FAILED">FAILED</option>
            </select>
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-700">Recon Status</label>
            <select
              value={filters.reconStatus}
              onChange={(e) => setFilters((p) => ({ ...p, reconStatus: e.target.value }))}
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
            >
              <option value="">Tum</option>
              <option value="UNMATCHED">UNMATCHED</option>
              <option value="PARTIAL">PARTIAL</option>
              <option value="MATCHED">MATCHED</option>
              <option value="IGNORED">IGNORED</option>
            </select>
          </div>
          <div className="flex items-end">
            <button
              type="submit"
              className="rounded bg-slate-900 px-3 py-2 text-sm font-medium text-white"
              disabled={!canRead || loadingImports || loadingLines}
            >
              {loadingImports || loadingLines ? "Yukleniyor..." : "Filtrele"}
            </button>
          </div>
        </form>
      </section>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,1.4fr)]">
        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <h2 className="mb-3 text-sm font-semibold text-slate-900">Importlar</h2>
          <div className="max-h-[560px] overflow-auto">
            <table className="min-w-full text-left text-xs">
              <thead className="sticky top-0 bg-slate-50 text-slate-600">
                <tr>
                  <th className="px-2 py-2">ID</th>
                  <th className="px-2 py-2">Banka</th>
                  <th className="px-2 py-2">Durum</th>
                  <th className="px-2 py-2">Top</th>
                  <th className="px-2 py-2">New</th>
                  <th className="px-2 py-2">Dup</th>
                </tr>
              </thead>
              <tbody>
                {imports.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="px-2 py-3 text-slate-500">
                      Import kaydi yok.
                    </td>
                  </tr>
                ) : (
                  imports.map((row) => {
                    const active = String(row?.id || "") === String(selectedImportId || "");
                    return (
                      <tr
                        key={row.id}
                        className={`cursor-pointer border-t ${
                          active ? "bg-slate-100" : "hover:bg-slate-50"
                        }`}
                        onClick={() => {
                          const nextId = String(row.id);
                          setSelectedImportId(nextId);
                          loadLines(nextId);
                        }}
                      >
                        <td className="px-2 py-2 font-medium text-slate-900">#{row.id}</td>
                        <td className="px-2 py-2">
                          <div>{row.bank_account_code}</div>
                          <div className="text-[11px] text-slate-500">{row.original_filename}</div>
                        </td>
                        <td className="px-2 py-2">{row.status}</td>
                        <td className="px-2 py-2">{row.line_count_total ?? 0}</td>
                        <td className="px-2 py-2">{row.line_count_inserted ?? 0}</td>
                        <td className="px-2 py-2">{row.line_count_duplicates ?? 0}</td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <h2 className="mb-3 text-sm font-semibold text-slate-900">Statement Lines</h2>
          <div className="mb-3 text-xs text-slate-500">
            {selectedImportId ? `Import #${selectedImportId}` : "Import secin"}
          </div>
          <div className="max-h-[560px] overflow-auto">
            <table className="min-w-full text-left text-xs">
              <thead className="sticky top-0 bg-slate-50 text-slate-600">
                <tr>
                  <th className="px-2 py-2">ID</th>
                  <th className="px-2 py-2">Txn</th>
                  <th className="px-2 py-2">Aciklama</th>
                  <th className="px-2 py-2">Ref</th>
                  <th className="px-2 py-2">Tutar</th>
                  <th className="px-2 py-2">Bakiye</th>
                  <th className="px-2 py-2">Durum</th>
                </tr>
              </thead>
              <tbody>
                {lines.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="px-2 py-3 text-slate-500">
                      Statement line yok.
                    </td>
                  </tr>
                ) : (
                  lines.map((row) => (
                    <tr key={row.id} className="border-t hover:bg-slate-50">
                      <td className="px-2 py-2 font-medium text-slate-900">#{row.id}</td>
                      <td className="px-2 py-2">
                        <div>{formatDate(row.txn_date)}</div>
                        <div className="text-[11px] text-slate-500">val: {formatDate(row.value_date)}</div>
                      </td>
                      <td className="px-2 py-2">
                        <div className="max-w-[240px] truncate" title={row.description}>
                          {row.description}
                        </div>
                        <div className="text-[11px] text-slate-500">{row.bank_account_code}</div>
                      </td>
                      <td className="px-2 py-2">{row.reference_no || "-"}</td>
                      <td className="px-2 py-2">
                        {formatAmount(row.amount)} {row.currency_code}
                      </td>
                      <td className="px-2 py-2">
                        {row.balance_after == null ? "-" : formatAmount(row.balance_after)}
                      </td>
                      <td className="px-2 py-2">{row.recon_status}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
  );
}
