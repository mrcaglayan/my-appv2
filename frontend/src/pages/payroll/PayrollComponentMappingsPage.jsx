import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { listLegalEntities } from "../../api/orgAdmin.js";
import { listPayrollMappings, upsertPayrollMapping } from "../../api/payrollMappings.js";
import { useAuth } from "../../auth/useAuth.js";

const COMPONENT_OPTIONS = [
  {
    code: "BASE_SALARY_EXPENSE",
    side: "DEBIT",
    label: "Base Salary Expense",
    help: "Gross salary base expense.",
  },
  {
    code: "OVERTIME_EXPENSE",
    side: "DEBIT",
    label: "Overtime Expense",
    help: "Overtime payroll expense.",
  },
  {
    code: "BONUS_EXPENSE",
    side: "DEBIT",
    label: "Bonus Expense",
    help: "Bonus and premium expense.",
  },
  {
    code: "ALLOWANCES_EXPENSE",
    side: "DEBIT",
    label: "Allowances Expense",
    help: "Allowance and benefit expense portion.",
  },
  {
    code: "EMPLOYER_TAX_EXPENSE",
    side: "DEBIT",
    label: "Employer Tax Expense",
    help: "Employer tax expense accrual.",
  },
  {
    code: "EMPLOYER_SOCIAL_SECURITY_EXPENSE",
    side: "DEBIT",
    label: "Employer Social Security Expense",
    help: "Employer SGK/SS expense accrual.",
  },
  {
    code: "PAYROLL_NET_PAYABLE",
    side: "CREDIT",
    label: "Payroll Net Payable",
    help: "Net salary liability to employees.",
  },
  {
    code: "EMPLOYEE_TAX_PAYABLE",
    side: "CREDIT",
    label: "Employee Tax Payable",
    help: "Withheld employee tax liability.",
  },
  {
    code: "EMPLOYEE_SOCIAL_SECURITY_PAYABLE",
    side: "CREDIT",
    label: "Employee Social Security Payable",
    help: "Withheld employee SGK/SS liability.",
  },
  {
    code: "EMPLOYER_TAX_PAYABLE",
    side: "CREDIT",
    label: "Employer Tax Payable",
    help: "Employer tax liability.",
  },
  {
    code: "EMPLOYER_SOCIAL_SECURITY_PAYABLE",
    side: "CREDIT",
    label: "Employer Social Security Payable",
    help: "Employer SGK/SS liability.",
  },
  {
    code: "OTHER_DEDUCTIONS_PAYABLE",
    side: "CREDIT",
    label: "Other Deductions Payable",
    help: "Other payroll deductions to third parties.",
  },
];

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
  return parsed.toISOString().slice(0, 10);
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

export default function PayrollComponentMappingsPage() {
  const { hasPermission } = useAuth();
  const canRead = hasPermission("payroll.mappings.read");
  const canWrite = hasPermission("payroll.mappings.write");
  const canReadOrg = hasPermission("org.tree.read");

  const [legalEntities, setLegalEntities] = useState([]);
  const [lookupWarning, setLookupWarning] = useState("");
  const [loadingLookups, setLoadingLookups] = useState(false);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [rows, setRows] = useState([]);
  const [total, setTotal] = useState(0);
  const [filters, setFilters] = useState({
    legalEntityId: "",
    providerCode: "",
    currencyCode: "TRY",
    componentCode: "",
    asOfDate: "",
    activeOnly: true,
  });
  const [form, setForm] = useState({
    legalEntityId: "",
    providerCode: "",
    currencyCode: "TRY",
    componentCode: COMPONENT_OPTIONS[0].code,
    entrySide: COMPONENT_OPTIONS[0].side,
    glAccountId: "",
    effectiveFrom: "",
    effectiveTo: "",
    closePreviousOpenMapping: true,
    notes: "",
  });

  const legalEntityOptions = useMemo(
    () =>
      [...(legalEntities || [])].sort((a, b) =>
        String(a?.code || "").localeCompare(String(b?.code || ""))
      ),
    [legalEntities]
  );

  const selectedComponentMeta = useMemo(
    () => COMPONENT_OPTIONS.find((item) => item.code === form.componentCode) || COMPONENT_OPTIONS[0],
    [form.componentCode]
  );

  useEffect(() => {
    let cancelled = false;
    if (!canReadOrg) {
      setLegalEntities([]);
      setLookupWarning((canRead || canWrite) ? "org.tree.read yok: legalEntityId manuel girin." : "");
      return undefined;
    }

    (async () => {
      setLoadingLookups(true);
      try {
        const res = await listLegalEntities({ limit: 500, offset: 0 });
        if (!cancelled) {
          setLegalEntities(res?.rows || []);
          setLookupWarning("");
        }
      } catch (err) {
        if (!cancelled) {
          setLegalEntities([]);
          setLookupWarning(err?.response?.data?.message || "Legal entity listesi yuklenemedi");
        }
      } finally {
        if (!cancelled) {
          setLoadingLookups(false);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [canRead, canReadOrg, canWrite]);

  useEffect(() => {
    if (!legalEntityOptions.length) {
      return;
    }
    setFilters((prev) =>
      prev.legalEntityId ? prev : { ...prev, legalEntityId: String(legalEntityOptions[0].id) }
    );
    setForm((prev) =>
      prev.legalEntityId ? prev : { ...prev, legalEntityId: String(legalEntityOptions[0].id) }
    );
  }, [legalEntityOptions]);

  useEffect(() => {
    if (form.effectiveFrom) {
      return;
    }
    const today = new Date();
    const firstDay = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), 1))
      .toISOString()
      .slice(0, 10);
    setForm((prev) => ({ ...prev, effectiveFrom: firstDay }));
  }, [form.effectiveFrom]);

  async function loadMappings() {
    if (!canRead) {
      setRows([]);
      setTotal(0);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const res = await listPayrollMappings({
        limit: 300,
        offset: 0,
        legalEntityId: filters.legalEntityId || undefined,
        providerCode: filters.providerCode || undefined,
        currencyCode: filters.currencyCode || undefined,
        componentCode: filters.componentCode || undefined,
        asOfDate: filters.asOfDate || undefined,
        activeOnly: filters.activeOnly,
      });
      setRows(res?.rows || []);
      setTotal(Number(res?.total || 0));
    } catch (err) {
      setRows([]);
      setTotal(0);
      setError(err?.response?.data?.message || "Payroll mapping listesi yuklenemedi");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadMappings();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead]);

  async function handleSubmit(event) {
    event.preventDefault();
    if (!canWrite) {
      setError("Missing permission: payroll.mappings.write");
      return;
    }

    const legalEntityId = toPositiveInt(form.legalEntityId);
    const glAccountId = toPositiveInt(form.glAccountId);
    if (!legalEntityId) {
      setError("legalEntityId gerekli");
      return;
    }
    if (!glAccountId) {
      setError("glAccountId gerekli");
      return;
    }
    if (!form.effectiveFrom) {
      setError("effectiveFrom gerekli");
      return;
    }

    setSaving(true);
    setError("");
    setMessage("");
    try {
      await upsertPayrollMapping({
        legalEntityId,
        providerCode: String(form.providerCode || "").trim().toUpperCase() || null,
        currencyCode: String(form.currencyCode || "").trim().toUpperCase(),
        componentCode: form.componentCode,
        entrySide: form.entrySide,
        glAccountId,
        effectiveFrom: form.effectiveFrom,
        effectiveTo: form.effectiveTo || null,
        closePreviousOpenMapping: Boolean(form.closePreviousOpenMapping),
        notes: String(form.notes || "").trim() || null,
      });
      setMessage("Payroll mapping kaydedildi");
      setFilters((prev) => ({
        ...prev,
        legalEntityId: String(legalEntityId),
        providerCode: String(form.providerCode || "").trim().toUpperCase(),
        currencyCode: String(form.currencyCode || "").trim().toUpperCase(),
      }));
      await loadMappings();
    } catch (err) {
      setError(err?.response?.data?.message || "Payroll mapping kaydi basarisiz");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold text-slate-900">Bordro Mappingleri</h1>
          <p className="mt-1 text-sm text-slate-600">
            PR-P02: Payroll component to GL hesap eslemeleri (effective-dated). Tahakkuk preview ve
            finalize bu kayitlari kullanir.
          </p>
        </div>
        <Link
          to="/app/payroll-runs"
          className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700"
        >
          Bordro Runlari
        </Link>
      </div>

      {!canRead ? (
        <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          Missing permission: <code>payroll.mappings.read</code>
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
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-sm font-semibold text-slate-900">Mapping Listesi</h2>
            <button
              type="button"
              className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700"
              onClick={loadMappings}
              disabled={loading || !canRead}
            >
              Yenile
            </button>
          </div>

          <div className="grid gap-3 md:grid-cols-3">
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">Legal Entity</label>
              {canReadOrg ? (
                <select
                  value={filters.legalEntityId}
                  onChange={(e) => setFilters((prev) => ({ ...prev, legalEntityId: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={loadingLookups}
                >
                  <option value="">Tum yetkili</option>
                  {legalEntityOptions.map((le) => (
                    <option key={le.id} value={le.id}>
                      {le.code} - {le.name}
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  value={filters.legalEntityId}
                  onChange={(e) => setFilters((prev) => ({ ...prev, legalEntityId: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="legalEntityId"
                />
              )}
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">Provider</label>
              <input
                value={filters.providerCode}
                onChange={(e) => setFilters((prev) => ({ ...prev, providerCode: e.target.value }))}
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="Optional"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">Currency</label>
              <input
                value={filters.currencyCode}
                onChange={(e) => setFilters((prev) => ({ ...prev, currencyCode: e.target.value }))}
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="TRY"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">Component</label>
              <select
                value={filters.componentCode}
                onChange={(e) => setFilters((prev) => ({ ...prev, componentCode: e.target.value }))}
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
              >
                <option value="">Tum componentler</option>
                {COMPONENT_OPTIONS.map((item) => (
                  <option key={item.code} value={item.code}>
                    {item.code}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">As Of Date</label>
              <input
                type="date"
                value={filters.asOfDate}
                onChange={(e) => setFilters((prev) => ({ ...prev, asOfDate: e.target.value }))}
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
              />
            </div>
            <div className="flex items-end gap-2">
              <label className="inline-flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={Boolean(filters.activeOnly)}
                  onChange={(e) =>
                    setFilters((prev) => ({ ...prev, activeOnly: Boolean(e.target.checked) }))
                  }
                />
                Sadece aktif
              </label>
              <button
                type="button"
                className="rounded border border-slate-300 px-3 py-1.5 text-sm"
                onClick={loadMappings}
                disabled={loading || !canRead}
              >
                Listele
              </button>
            </div>
          </div>

          {lookupWarning ? (
            <div className="mt-3 rounded border border-amber-200 bg-amber-50 px-2 py-1 text-xs text-amber-800">
              {lookupWarning}
            </div>
          ) : null}

          <div className="mt-3 text-xs text-slate-500">Toplam kayit: {total}</div>

          <div className="mt-4 overflow-auto">
            <table className="min-w-full border-collapse text-sm">
              <thead>
                <tr className="border-b">
                  <th className="p-2 text-left">Component</th>
                  <th className="p-2 text-left">Side</th>
                  <th className="p-2 text-left">GL</th>
                  <th className="p-2 text-left">Entity</th>
                  <th className="p-2 text-left">Provider</th>
                  <th className="p-2 text-left">Effective</th>
                  <th className="p-2 text-left">Active</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr key={row.id} className="border-b align-top">
                    <td className="p-2">
                      <div className="font-medium">{row.component_code}</div>
                      {row.notes ? (
                        <div className="mt-1 text-xs text-slate-500">{row.notes}</div>
                      ) : null}
                    </td>
                    <td className="p-2">{row.entry_side}</td>
                    <td className="p-2">
                      <div>{row.gl_account_code || row.gl_account_id}</div>
                      <div className="text-xs text-slate-500">{row.gl_account_name || "-"}</div>
                    </td>
                    <td className="p-2">
                      <div>{row.entity_code}</div>
                      <div className="text-xs text-slate-500">{row.legal_entity_name || ""}</div>
                    </td>
                    <td className="p-2">{row.provider_code || "*"}</td>
                    <td className="p-2">
                      <div>
                        {formatDate(row.effective_from)} {"->"}{" "}
                        {row.effective_to ? formatDate(row.effective_to) : "open"}
                      </div>
                      <div className="text-xs text-slate-500">{formatDateTime(row.created_at)}</div>
                    </td>
                    <td className="p-2">{row.is_active ? "ACTIVE" : "INACTIVE"}</td>
                  </tr>
                ))}
                {rows.length === 0 ? (
                  <tr>
                    <td className="p-3 text-slate-500" colSpan={7}>
                      Kayit yok.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <h2 className="text-sm font-semibold text-slate-900">Yeni Mapping</h2>
          <p className="mt-1 text-xs text-slate-600">
            Bir payroll componentini belirli tarih araliginda bir GL hesaba baglar. Provider bos ise
            fallback mapping olarak kullanilir.
          </p>

          {!canWrite ? (
            <div className="mt-3 rounded border border-amber-200 bg-amber-50 px-2 py-1 text-xs text-amber-800">
              Missing permission: payroll.mappings.write
            </div>
          ) : null}

          <form onSubmit={handleSubmit} className="mt-4 space-y-3">
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">Legal Entity</label>
              {canReadOrg ? (
                <select
                  value={form.legalEntityId}
                  onChange={(e) => setForm((prev) => ({ ...prev, legalEntityId: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={loadingLookups || saving}
                >
                  <option value="">Secin</option>
                  {legalEntityOptions.map((le) => (
                    <option key={le.id} value={le.id}>
                      {le.code} - {le.name}
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  value={form.legalEntityId}
                  onChange={(e) => setForm((prev) => ({ ...prev, legalEntityId: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="legalEntityId"
                />
              )}
            </div>

            <div className="grid gap-3 md:grid-cols-2">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Provider (optional)</label>
                <input
                  value={form.providerCode}
                  onChange={(e) => setForm((prev) => ({ ...prev, providerCode: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="OUTSOURCED_PAYROLL_X"
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Currency</label>
                <input
                  value={form.currencyCode}
                  onChange={(e) => setForm((prev) => ({ ...prev, currencyCode: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="TRY"
                />
              </div>
            </div>

            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">Component</label>
              <select
                value={form.componentCode}
                onChange={(e) => {
                  const nextCode = e.target.value;
                  const meta = COMPONENT_OPTIONS.find((item) => item.code === nextCode);
                  setForm((prev) => ({
                    ...prev,
                    componentCode: nextCode,
                    entrySide: meta?.side || prev.entrySide,
                  }));
                }}
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
              >
                {COMPONENT_OPTIONS.map((item) => (
                  <option key={item.code} value={item.code}>
                    {item.code}
                  </option>
                ))}
              </select>
              <div className="mt-1 rounded bg-slate-50 px-2 py-1 text-xs text-slate-600">
                Expected side: <b>{selectedComponentMeta.side}</b>. {selectedComponentMeta.help}
              </div>
            </div>

            <div className="grid gap-3 md:grid-cols-2">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Entry Side</label>
                <select
                  value={form.entrySide}
                  onChange={(e) => setForm((prev) => ({ ...prev, entrySide: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                >
                  <option value="DEBIT">DEBIT</option>
                  <option value="CREDIT">CREDIT</option>
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">GL Account ID</label>
                <input
                  value={form.glAccountId}
                  onChange={(e) => setForm((prev) => ({ ...prev, glAccountId: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="12345"
                />
              </div>
            </div>

            <div className="grid gap-3 md:grid-cols-2">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Effective From</label>
                <input
                  type="date"
                  value={form.effectiveFrom}
                  onChange={(e) => setForm((prev) => ({ ...prev, effectiveFrom: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Effective To</label>
                <input
                  type="date"
                  value={form.effectiveTo}
                  onChange={(e) => setForm((prev) => ({ ...prev, effectiveTo: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                />
              </div>
            </div>

            <div>
              <label className="mb-1 block text-xs font-medium text-slate-700">Notes</label>
              <textarea
                rows={3}
                value={form.notes}
                onChange={(e) => setForm((prev) => ({ ...prev, notes: e.target.value }))}
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                placeholder="Optional rationale"
              />
            </div>

            <label className="inline-flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={Boolean(form.closePreviousOpenMapping)}
                onChange={(e) =>
                  setForm((prev) => ({
                    ...prev,
                    closePreviousOpenMapping: Boolean(e.target.checked),
                  }))
                }
              />
              Ayni key icin onceki acik mappingi kapat
            </label>

            <button
              type="submit"
              disabled={!canWrite || saving}
              className="w-full rounded bg-slate-900 px-3 py-2 text-sm font-medium text-white disabled:opacity-60"
            >
              {saving ? "Kaydediliyor..." : "Mapping Kaydet"}
            </button>
          </form>
        </section>
      </div>
    </div>
  );
}
