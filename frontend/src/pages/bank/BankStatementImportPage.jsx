import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { listBankAccounts } from "../../api/bankAccounts.js";
import { importBankStatementCsv } from "../../api/bankStatements.js";
import { useAuth } from "../../auth/useAuth.js";

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
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

const SAMPLE_CSV = [
  "txn_date,value_date,description,reference_no,amount,currency_code,balance_after",
  "2026-02-01,2026-02-01,Havale gelisi,REF-001,15000.00,TRY,15000.00",
  "2026-02-02,2026-02-02,POS tahsilat,REF-002,2500.50,TRY,17500.50",
].join("\n");

export default function BankStatementImportPage() {
  const { hasPermission } = useAuth();
  const canImport = hasPermission("bank.statements.import");
  const canReadBanks = hasPermission("bank.accounts.read");

  const [bankAccounts, setBankAccounts] = useState([]);
  const [loadingBanks, setLoadingBanks] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [resultRow, setResultRow] = useState(null);
  const [form, setForm] = useState({
    bankAccountId: "",
    originalFilename: "",
    csvText: "",
  });

  const bankOptions = useMemo(
    () =>
      (bankAccounts || [])
        .filter((row) => parseDbBoolean(row?.is_active))
        .sort((a, b) => String(a?.code || "").localeCompare(String(b?.code || ""))),
    [bankAccounts]
  );

  useEffect(() => {
    let cancelled = false;
    if (!canReadBanks) {
      return undefined;
    }
    (async () => {
      setLoadingBanks(true);
      try {
        const res = await listBankAccounts({ limit: 300, offset: 0, isActive: true });
        if (!cancelled) {
          setBankAccounts(res?.rows || []);
        }
      } catch {
        if (!cancelled) {
          setBankAccounts([]);
        }
      } finally {
        if (!cancelled) {
          setLoadingBanks(false);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [canReadBanks]);

  useEffect(() => {
    if (form.bankAccountId || bankOptions.length === 0) {
      return;
    }
    setForm((prev) => ({ ...prev, bankAccountId: String(bankOptions[0]?.id || "") }));
  }, [bankOptions, form.bankAccountId]);

  async function handleFileChange(event) {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }
    try {
      const text = await file.text();
      setForm((prev) => ({
        ...prev,
        originalFilename: file.name || prev.originalFilename,
        csvText: text,
      }));
    } catch {
      setError("CSV file could not be read");
    }
  }

  async function handleSubmit(event) {
    event.preventDefault();
    if (!canImport) {
      setError("Missing permission: bank.statements.import");
      return;
    }
    const bankAccountId = toPositiveInt(form.bankAccountId);
    if (!bankAccountId) {
      setError("bankAccountId is required");
      return;
    }

    setSaving(true);
    setError("");
    setMessage("");
    setResultRow(null);
    try {
      const res = await importBankStatementCsv({
        bankAccountId,
        importSource: "CSV",
        originalFilename: String(form.originalFilename || "").trim() || "statement.csv",
        csvText: String(form.csvText || ""),
      });
      setResultRow(res?.row || null);
      setMessage("Statement imported successfully");
    } catch (err) {
      setError(err?.response?.data?.message || "Statement import failed");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold text-slate-900">Banka Ekstre Ice Aktar</h1>
          <p className="mt-1 text-sm text-slate-600">
            PR-B02 CSV import (checksum idempotency + line dedupe).
          </p>
        </div>
        <Link
          to="/app/banka-ekstre-kuyrugu"
          className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700"
        >
          Kuyruk
        </Link>
      </div>

      {!canImport ? (
        <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          Missing permission: <code>bank.statements.import</code>
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

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.2fr)_minmax(0,1fr)]">
        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-sm font-semibold text-slate-900">Import Formu</h2>
              <button
                type="button"
                className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700"
                onClick={() =>
                  setForm((prev) => ({
                    ...prev,
                    originalFilename: prev.originalFilename || "sample-statement.csv",
                    csvText: SAMPLE_CSV,
                  }))
                }
              >
                Ornek CSV
              </button>
            </div>

            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">Bank Account</label>
              {canReadBanks ? (
                <select
                  value={form.bankAccountId}
                  onChange={(e) => setForm((prev) => ({ ...prev, bankAccountId: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canImport || saving || loadingBanks}
                >
                  <option value="">Secin</option>
                  {bankOptions.map((row) => (
                    <option key={row.id} value={row.id}>
                      {row.code} - {row.name} ({row.currency_code})
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  value={form.bankAccountId}
                  onChange={(e) => setForm((prev) => ({ ...prev, bankAccountId: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="bankAccountId"
                />
              )}
            </div>

            <div className="grid gap-3 md:grid-cols-2">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">CSV File</label>
                <input
                  type="file"
                  accept=".csv,text/csv"
                  onChange={handleFileChange}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canImport || saving}
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">
                  Original Filename
                </label>
                <input
                  value={form.originalFilename}
                  onChange={(e) =>
                    setForm((prev) => ({ ...prev, originalFilename: e.target.value }))
                  }
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="statement.csv"
                />
              </div>
            </div>

            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">CSV Icerik</label>
              <textarea
                rows={12}
                value={form.csvText}
                onChange={(e) => setForm((prev) => ({ ...prev, csvText: e.target.value }))}
                className="w-full rounded border border-slate-300 px-2 py-1.5 font-mono text-xs"
                placeholder={SAMPLE_CSV}
              />
            </div>

            <button
              type="submit"
              disabled={!canImport || saving}
              className="rounded bg-slate-900 px-3 py-2 text-sm font-medium text-white disabled:opacity-60"
            >
              {saving ? "Iceri aktariliyor..." : "Ekstreyi Ice Aktar"}
            </button>
          </form>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <h2 className="text-sm font-semibold text-slate-900">Son Import</h2>
          {!resultRow ? (
            <div className="mt-3 text-sm text-slate-600">Heniz import yapilmadi.</div>
          ) : (
            <div className="mt-3 space-y-1 text-sm text-slate-700">
              <div>
                <strong>ID:</strong> #{resultRow.id}
              </div>
              <div>
                <strong>Banka:</strong> {resultRow.bank_account_code || "-"}
              </div>
              <div>
                <strong>Durum:</strong> {resultRow.status || "-"}
              </div>
              <div>
                <strong>Toplam:</strong> {resultRow.line_count_total ?? 0}
              </div>
              <div>
                <strong>Eklenen:</strong> {resultRow.line_count_inserted ?? 0}
              </div>
              <div>
                <strong>Duplicate:</strong> {resultRow.line_count_duplicates ?? 0}
              </div>
              <div>
                <strong>Zaman:</strong> {formatDateTime(resultRow.imported_at)}
              </div>
            </div>
          )}
          <div className="mt-4 rounded bg-slate-50 px-3 py-2 text-xs text-slate-600">
            PR-B02 kapsami: import + normalized queue. Eslestirme/mutabakat PR-B03.
          </div>
        </section>
      </div>
    </div>
  );
}
