import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { listBankAccounts } from "../../api/bankAccounts.js";
import { createPaymentBatch, listPaymentBatches } from "../../api/payments.js";
import { useAuth } from "../../auth/useAuth.js";

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

function buildSampleCreateJson(bankAccount) {
  return JSON.stringify(
    {
      sourceType: "MANUAL",
      sourceId: null,
      bankAccountId: Number(bankAccount?.id || 1),
      currencyCode: String(bankAccount?.currency_code || "TRY"),
      idempotencyKey: `manual-${new Date().toISOString().slice(0, 10)}-batch-001`,
      notes: "Manual payment batch sample (update GL account + IDs before save)",
      lines: [
        {
          beneficiaryType: "EMPLOYEE",
          beneficiaryId: 101,
          beneficiaryName: "Alice Doe",
          beneficiaryBankRef: "TR00 0000 0000 0000 0000 0000 00",
          payableEntityType: "MANUAL",
          payableEntityId: 5001,
          payableGlAccountId: 320001,
          payableRef: "ADV-EMP-101",
          amount: 500,
        },
        {
          beneficiaryType: "EMPLOYEE",
          beneficiaryId: 102,
          beneficiaryName: "Bob Doe",
          beneficiaryBankRef: "TR00 0000 0000 0000 0000 0000 01",
          payableEntityType: "MANUAL",
          payableEntityId: 5002,
          payableGlAccountId: 320001,
          payableRef: "ADV-EMP-102",
          amount: 450,
        },
      ],
    },
    null,
    2
  );
}

export default function PaymentBatchListPage() {
  const navigate = useNavigate();
  const { hasPermission } = useAuth();
  const canRead = hasPermission("payments.batch.read");
  const canCreate = hasPermission("payments.batch.create");
  const canReadBanks = hasPermission("bank.accounts.read");

  const [filters, setFilters] = useState({
    status: "",
    sourceType: "",
    q: "",
  });
  const [rows, setRows] = useState([]);
  const [total, setTotal] = useState(0);
  const [bankAccounts, setBankAccounts] = useState([]);
  const [createJson, setCreateJson] = useState("");
  const [loading, setLoading] = useState(false);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [lookupWarning, setLookupWarning] = useState("");

  const sortedBankAccounts = useMemo(
    () =>
      [...(bankAccounts || [])].sort((a, b) =>
        String(a?.code || "").localeCompare(String(b?.code || ""))
      ),
    [bankAccounts]
  );

  async function loadRows() {
    if (!canRead) {
      setRows([]);
      setTotal(0);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const res = await listPaymentBatches({
        limit: 200,
        offset: 0,
        status: filters.status || undefined,
        sourceType: filters.sourceType || undefined,
        q: filters.q || undefined,
      });
      setRows(res?.rows || []);
      setTotal(Number(res?.total || 0));
    } catch (err) {
      setRows([]);
      setTotal(0);
      setError(err?.response?.data?.message || "Odeme batch listesi yuklenemedi");
    } finally {
      setLoading(false);
    }
  }

  async function loadBankLookups() {
    if (!canCreate || !canReadBanks) {
      if (canCreate && !canReadBanks) {
        setLookupWarning("bank.accounts.read yok: ornek payload bank hesabini otomatik dolduramaz.");
      } else {
        setLookupWarning("");
      }
      return;
    }
    try {
      const res = await listBankAccounts({ limit: 300, offset: 0, isActive: true });
      const nextRows = res?.rows || [];
      setBankAccounts(nextRows);
      setLookupWarning("");
    } catch (err) {
      setBankAccounts([]);
      setLookupWarning(err?.response?.data?.message || "Banka hesaplari yuklenemedi");
    }
  }

  useEffect(() => {
    loadRows();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead]);

  useEffect(() => {
    loadBankLookups();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canCreate, canReadBanks]);

  useEffect(() => {
    if (!canCreate) {
      return;
    }
    if (createJson.trim()) {
      return;
    }
    setCreateJson(buildSampleCreateJson(sortedBankAccounts[0] || null));
  }, [canCreate, createJson, sortedBankAccounts]);

  async function handleRefresh() {
    await loadRows();
  }

  async function handleCreateFromJson() {
    if (!canCreate || creating) {
      return;
    }
    setCreating(true);
    setError("");
    setMessage("");
    try {
      const payload = JSON.parse(createJson);
      const res = await createPaymentBatch(payload);
      const batchId = Number(res?.row?.id || 0);
      setMessage("Odeme batch olusturuldu");
      await loadRows();
      if (batchId > 0) {
        navigate(`/app/odeme-batchleri/${batchId}`);
      }
    } catch (err) {
      if (err instanceof SyntaxError) {
        setError(`JSON parse hatasi: ${err.message}`);
      } else {
        setError(err?.response?.data?.message || err?.message || "Odeme batch olusturulamadi");
      }
    } finally {
      setCreating(false);
    }
  }

  return (
    <div className="p-4 space-y-4">
      <div className="rounded border bg-white p-4">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-lg font-semibold">Odeme Batchleri</h1>
          <span className="text-xs text-slate-500">Toplam: {total}</span>
          <button
            type="button"
            className="ml-auto rounded border px-2 py-1 text-sm"
            onClick={handleRefresh}
            disabled={loading || !canRead}
          >
            {loading ? "Yukleniyor..." : "Yenile"}
          </button>
        </div>

        {!canRead ? (
          <div className="mt-3 rounded border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
            Missing permission: <code>payments.batch.read</code>
          </div>
        ) : null}

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

        <div className="mt-3 grid gap-2 md:grid-cols-4">
          <label className="text-sm">
            <div className="mb-1 font-medium">Durum</div>
            <select
              className="w-full rounded border px-2 py-1"
              value={filters.status}
              onChange={(e) => setFilters((prev) => ({ ...prev, status: e.target.value }))}
            >
              <option value="">Tum</option>
              <option value="DRAFT">DRAFT</option>
              <option value="APPROVED">APPROVED</option>
              <option value="EXPORTED">EXPORTED</option>
              <option value="POSTED">POSTED</option>
              <option value="CANCELLED">CANCELLED</option>
            </select>
          </label>
          <label className="text-sm">
            <div className="mb-1 font-medium">Kaynak</div>
            <select
              className="w-full rounded border px-2 py-1"
              value={filters.sourceType}
              onChange={(e) => setFilters((prev) => ({ ...prev, sourceType: e.target.value }))}
            >
              <option value="">Tum</option>
              <option value="MANUAL">MANUAL</option>
              <option value="PAYROLL">PAYROLL</option>
              <option value="AP">AP</option>
              <option value="TAX">TAX</option>
            </select>
          </label>
          <label className="text-sm md:col-span-2">
            <div className="mb-1 font-medium">Arama</div>
            <div className="flex gap-2">
              <input
                className="w-full rounded border px-2 py-1"
                value={filters.q}
                onChange={(e) => setFilters((prev) => ({ ...prev, q: e.target.value }))}
                placeholder="Batch no / banka / not"
              />
              <button
                type="button"
                className="rounded border px-3 py-1 text-sm"
                onClick={handleRefresh}
                disabled={!canRead || loading}
              >
                Ara
              </button>
            </div>
          </label>
        </div>
      </div>

      {canCreate ? (
        <div className="rounded border bg-white p-4">
          <div className="flex items-center gap-2">
            <h2 className="font-medium">Hizli Olustur (JSON)</h2>
            <button
              type="button"
              className="ml-auto rounded border px-2 py-1 text-xs"
              onClick={() => setCreateJson(buildSampleCreateJson(sortedBankAccounts[0] || null))}
            >
              Ornek Doldur
            </button>
          </div>
          {lookupWarning ? <div className="mt-2 text-xs text-amber-700">{lookupWarning}</div> : null}
          <p className="mt-2 text-xs text-slate-600">
            PR-B04 icin minimal UI: JSON payloadi duzenleyip taslak batch olusturur.
          </p>
          <textarea
            className="mt-2 min-h-[220px] w-full rounded border p-2 font-mono text-xs"
            value={createJson}
            onChange={(e) => setCreateJson(e.target.value)}
            spellCheck={false}
          />
          <div className="mt-2 flex items-center gap-2">
            <button
              type="button"
              className="rounded border px-3 py-1 text-sm"
              onClick={handleCreateFromJson}
              disabled={creating}
            >
              {creating ? "Olusturuluyor..." : "Batch Olustur"}
            </button>
            <span className="text-xs text-slate-500">
              Not: `payableGlAccountId` mevcut ve ayni legal entity icinde postable/leaf olmalidir.
            </span>
          </div>
        </div>
      ) : null}

      <div className="rounded border bg-white p-4">
        <h2 className="mb-2 font-medium">Liste</h2>
        <div className="overflow-auto">
          <table className="min-w-full border-collapse text-sm">
            <thead>
              <tr className="border-b">
                <th className="p-2 text-left">Batch No</th>
                <th className="p-2 text-left">Kaynak</th>
                <th className="p-2 text-left">Banka</th>
                <th className="p-2 text-left">Para Birimi</th>
                <th className="p-2 text-left">Toplam</th>
                <th className="p-2 text-left">Durum</th>
                <th className="p-2 text-left">Satir</th>
                <th className="p-2 text-left">Olusma</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.id} className="border-b">
                  <td className="p-2">
                    <Link className="underline" to={`/app/odeme-batchleri/${row.id}`}>
                      {row.batch_no}
                    </Link>
                  </td>
                  <td className="p-2">{row.source_type}</td>
                  <td className="p-2">
                    <div>{row.bank_account_code}</div>
                    <div className="text-xs text-slate-500">{row.bank_account_name}</div>
                  </td>
                  <td className="p-2">{row.currency_code}</td>
                  <td className="p-2">{formatAmount(row.total_amount)}</td>
                  <td className="p-2">{row.status}</td>
                  <td className="p-2">
                    {row.line_count}{" "}
                    <span className="text-xs text-slate-500">
                      (P:{row.pending_line_count || 0} / O:{row.paid_line_count || 0})
                    </span>
                  </td>
                  <td className="p-2">{formatDateTime(row.created_at)}</td>
                </tr>
              ))}
              {rows.length === 0 ? (
                <tr>
                  <td className="p-3 text-slate-500" colSpan={8}>
                    Kayit yok.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

