import { useCallback, useEffect, useMemo, useState } from "react";
import {
  createBalanceSplitReclassification,
  createTransactionLineReclassification,
  getTrialBalance,
  listAccounts,
  listBooks,
  listReclassificationRuns,
  listReclassificationSourceLines,
} from "../../api/glAdmin.js";
import { listFiscalPeriods, listLegalEntities } from "../../api/orgAdmin.js";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";
import TenantReadinessChecklist from "../../readiness/TenantReadinessChecklist.jsx";

function toInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function toAmount(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function roundAmount(value) {
  return Math.round(toAmount(value) * 1_000_000) / 1_000_000;
}

function formatAmount(value) {
  return Number(value || 0).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function createTarget(mode = "PERCENT") {
  return {
    id: `t-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    accountId: "",
    percentage: mode === "PERCENT" ? "100" : "",
    amount: mode === "AMOUNT" ? "0" : "",
  };
}

export default function GlReclassificationPage() {
  const { hasPermission } = useAuth();
  const { language } = useI18n();
  const isTr = language === "tr";
  const l = useCallback((en, tr) => (isTr ? tr : en), [isTr]);
  const today = new Date().toISOString().slice(0, 10);

  const canReadOrgTree = hasPermission("org.tree.read");
  const canReadBooks = hasPermission("gl.book.read");
  const canReadAccounts = hasPermission("gl.account.read");
  const canReadPeriods = hasPermission("org.fiscal_period.read");
  const canReadTrialBalance = hasPermission("gl.trial_balance.read");
  const canCreate = hasPermission("gl.journal.create");
  const canReadRuns = hasPermission("gl.journal.read");

  const [loading, setLoading] = useState(false);
  const [loadingPeriods, setLoadingPeriods] = useState(false);
  const [loadingBalance, setLoadingBalance] = useState(false);
  const [loadingSourceLines, setLoadingSourceLines] = useState(false);
  const [loadingRuns, setLoadingRuns] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const [entities, setEntities] = useState([]);
  const [books, setBooks] = useState([]);
  const [accounts, setAccounts] = useState([]);
  const [periods, setPeriods] = useState([]);
  const [trialRows, setTrialRows] = useState([]);
  const [runs, setRuns] = useState([]);
  const [sourceLines, setSourceLines] = useState([]);
  const [sourceLineStates, setSourceLineStates] = useState({});

  const [form, setForm] = useState({
    legalEntityId: "",
    bookId: "",
    fiscalPeriodId: "",
    sourceAccountId: "",
    allocationMode: "PERCENT",
    entryDate: today,
    documentDate: today,
    currencyCode: "USD",
    description: "",
    referenceNo: "",
    note: "",
  });
  const [targets, setTargets] = useState([createTarget("PERCENT")]);
  const [txFilter, setTxFilter] = useState({
    dateFrom: today,
    dateTo: today,
    limit: "300",
  });

  const selectedLegalEntityId = toInt(form.legalEntityId);
  const selectedBookId = toInt(form.bookId);
  const selectedPeriodId = toInt(form.fiscalPeriodId);
  const selectedSourceAccountId = toInt(form.sourceAccountId);

  const sourceBalanceById = useMemo(() => {
    const map = new Map();
    for (const row of trialRows) {
      const accountId = toInt(row.account_id);
      if (accountId) {
        map.set(accountId, roundAmount(row.direct_balance));
      }
    }
    return map;
  }, [trialRows]);

  const postableLeafAccounts = useMemo(() => {
    const parentIds = new Set(accounts.map((row) => toInt(row.parent_account_id)).filter(Boolean));
    return accounts.filter((row) => {
      const id = toInt(row.id);
      if (!id || parentIds.has(id) || !row.is_active) return false;
      return !(
        row.allow_posting === false ||
        row.allow_posting === 0 ||
        row.allow_posting === "0"
      );
    });
  }, [accounts]);

  const sourceOptions = useMemo(
    () =>
      accounts
        .map((row) => ({ ...row, directBalance: roundAmount(sourceBalanceById.get(toInt(row.id))) }))
        .filter((row) => Math.abs(toAmount(row.directBalance)) > 0.000001)
        .sort((a, b) => String(a.code || "").localeCompare(String(b.code || ""))),
    [accounts, sourceBalanceById]
  );

  const targetOptions = useMemo(
    () =>
      postableLeafAccounts
        .filter((row) => toInt(row.id) !== selectedSourceAccountId)
        .sort((a, b) => String(a.code || "").localeCompare(String(b.code || ""))),
    [postableLeafAccounts, selectedSourceAccountId]
  );

  const sourceBalance = roundAmount(sourceBalanceById.get(selectedSourceAccountId));
  const reclassAmount = roundAmount(Math.abs(sourceBalance));
  const totalPct = roundAmount(targets.reduce((sum, row) => sum + toAmount(row.percentage), 0));
  const totalAmount = roundAmount(targets.reduce((sum, row) => sum + toAmount(row.amount), 0));
  const selectedSourceLines = useMemo(
    () =>
      sourceLines.filter((row) => Boolean(sourceLineStates[row.journalLineId]?.selected)),
    [sourceLines, sourceLineStates]
  );
  const selectedSourceLinesTotal = useMemo(
    () =>
      roundAmount(
        selectedSourceLines.reduce(
          (sum, row) => sum + Math.abs(toAmount(row.debitBase || row.creditBase)),
          0
        )
      ),
    [selectedSourceLines]
  );
  const selectedSourceLinesMappedCount = useMemo(
    () =>
      selectedSourceLines.filter((row) =>
        Boolean(toInt(sourceLineStates[row.journalLineId]?.targetAccountId))
      ).length,
    [selectedSourceLines, sourceLineStates]
  );

  async function loadRuns() {
    if (!canReadRuns || !selectedLegalEntityId) {
      setRuns([]);
      return;
    }
    setLoadingRuns(true);
    try {
      const res = await listReclassificationRuns({
        legalEntityId: selectedLegalEntityId,
        bookId: selectedBookId || undefined,
        fiscalPeriodId: selectedPeriodId || undefined,
        limit: 50,
      });
      setRuns(res?.rows || []);
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to load runs.", "Run listesi yuklenemedi."));
    } finally {
      setLoadingRuns(false);
    }
  }

  async function loadSourceLines() {
    if (!selectedLegalEntityId || !selectedBookId || !selectedSourceAccountId) {
      setSourceLines([]);
      setSourceLineStates({});
      return;
    }
    setLoadingSourceLines(true);
    try {
      const res = await listReclassificationSourceLines({
        legalEntityId: selectedLegalEntityId,
        bookId: selectedBookId,
        sourceAccountId: selectedSourceAccountId,
        dateFrom: txFilter.dateFrom || undefined,
        dateTo: txFilter.dateTo || undefined,
        limit: toInt(txFilter.limit) || 300,
      });
      const rows = res?.rows || [];
      setSourceLines(rows);
      setSourceLineStates((prev) => {
        const next = {};
        for (const row of rows) {
          const rowId = toInt(row.journalLineId);
          if (!rowId) continue;
          const oldState = prev[rowId] || {};
          next[rowId] = {
            selected: Boolean(oldState.selected),
            targetAccountId: oldState.targetAccountId || "",
          };
        }
        return next;
      });
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l("Failed to load source lines.", "Kaynak satirlar yuklenemedi.")
      );
    } finally {
      setLoadingSourceLines(false);
    }
  }

  async function handleCreateTransactionReclass() {
    setError("");
    setMessage("");
    if (selectedSourceLines.length === 0) {
      setError(
        l(
          "Select at least one source line.",
          "En az bir kaynak satir secin."
        )
      );
      return;
    }

    const lineMappings = selectedSourceLines.map((row, index) => {
      const journalLineId = toInt(row.journalLineId);
      const targetAccountId = toInt(sourceLineStates[row.journalLineId]?.targetAccountId);
      if (!journalLineId || !targetAccountId) {
        throw new Error(
          l(
            `Line ${index + 1}: target account is required.`,
            `Satir ${index + 1}: hedef hesap zorunlu.`
          )
        );
      }
      return { journalLineId, targetAccountId };
    });

    try {
      setSaving(true);
      const response = await createTransactionLineReclassification({
        legalEntityId: selectedLegalEntityId,
        bookId: selectedBookId,
        fiscalPeriodId: selectedPeriodId,
        sourceAccountId: selectedSourceAccountId,
        entryDate: form.entryDate,
        documentDate: form.documentDate,
        currencyCode: form.currencyCode.trim().toUpperCase(),
        description: form.description.trim() || undefined,
        referenceNo: form.referenceNo.trim() || undefined,
        note: form.note.trim() || undefined,
        dateFrom: txFilter.dateFrom || undefined,
        dateTo: txFilter.dateTo || undefined,
        lineMappings,
      });
      setMessage(
        l(
          `Transaction reclass draft created: ${response?.journalNo || "-"}.`,
          `Islem bazli taslak fis olustu: ${response?.journalNo || "-"}.`
        )
      );
      await Promise.all([loadRuns(), loadSourceLines()]);
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          err?.message ||
          l(
            "Failed to create transaction reclassification draft.",
            "Islem bazli yeniden siniflandirma taslagi olusturulamadi."
          )
      );
    } finally {
      setSaving(false);
    }
  }

  useEffect(() => {
    let cancelled = false;
    async function loadRefs() {
      if (!canReadOrgTree && !canReadBooks && !canReadAccounts) return;
      setLoading(true);
      try {
        const [entityRes, bookRes, accountRes] = await Promise.all([
          canReadOrgTree ? listLegalEntities() : Promise.resolve({ rows: [] }),
          canReadBooks
            ? listBooks(selectedLegalEntityId ? { legalEntityId: selectedLegalEntityId } : {})
            : Promise.resolve({ rows: [] }),
          canReadAccounts
            ? listAccounts(selectedLegalEntityId ? { legalEntityId: selectedLegalEntityId } : {})
            : Promise.resolve({ rows: [] }),
        ]);
        if (cancelled) return;
        const nextEntities = entityRes?.rows || [];
        const nextBooks = bookRes?.rows || [];
        setEntities(nextEntities);
        setBooks(nextBooks);
        setAccounts(accountRes?.rows || []);
        setForm((prev) => ({
          ...prev,
          legalEntityId: prev.legalEntityId || String(nextEntities[0]?.id || ""),
          bookId: prev.bookId || String(nextBooks[0]?.id || ""),
          currencyCode: String(nextBooks[0]?.base_currency_code || prev.currencyCode || "USD").toUpperCase(),
        }));
      } catch (err) {
        if (!cancelled) {
          setError(err?.response?.data?.message || l("Failed to load references.", "Referanslar yuklenemedi."));
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    loadRefs();
    return () => {
      cancelled = true;
    };
  }, [canReadOrgTree, canReadBooks, canReadAccounts, selectedLegalEntityId, l]);

  useEffect(() => {
    if (!canReadPeriods || !selectedBookId) {
      setPeriods([]);
      return;
    }
    let cancelled = false;
    async function loadBookPeriods() {
      const selectedBook = books.find((row) => Number(row.id) === Number(selectedBookId));
      const calendarId = toInt(selectedBook?.calendar_id);
      if (!calendarId) {
        setPeriods([]);
        return;
      }
      setLoadingPeriods(true);
      try {
        const res = await listFiscalPeriods(calendarId);
        if (cancelled) return;
        const nextPeriods = res?.rows || [];
        setPeriods(nextPeriods);
        setForm((prev) => ({
          ...prev,
          fiscalPeriodId: prev.fiscalPeriodId || String(nextPeriods[0]?.id || ""),
        }));
      } catch (err) {
        if (!cancelled) {
          setError(err?.response?.data?.message || l("Failed to load periods.", "Donemler yuklenemedi."));
        }
      } finally {
        if (!cancelled) setLoadingPeriods(false);
      }
    }
    loadBookPeriods();
    return () => {
      cancelled = true;
    };
  }, [books, canReadPeriods, selectedBookId, l]);

  useEffect(() => {
    if (!canReadTrialBalance || !selectedBookId || !selectedPeriodId) {
      setTrialRows([]);
      return;
    }
    let cancelled = false;
    async function loadBalance() {
      setLoadingBalance(true);
      try {
        const res = await getTrialBalance({
          bookId: selectedBookId,
          fiscalPeriodId: selectedPeriodId,
          includeRollup: true,
        });
        if (!cancelled) {
          setTrialRows(res?.rows || []);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err?.response?.data?.message || l("Failed to load trial balance.", "Mizan yuklenemedi."));
        }
      } finally {
        if (!cancelled) setLoadingBalance(false);
      }
    }
    loadBalance();
    return () => {
      cancelled = true;
    };
  }, [canReadTrialBalance, selectedBookId, selectedPeriodId, l]);

  useEffect(() => {
    setForm((prev) => ({
      ...prev,
      sourceAccountId:
        prev.sourceAccountId && sourceOptions.some((row) => String(row.id) === String(prev.sourceAccountId))
          ? prev.sourceAccountId
          : String(sourceOptions[0]?.id || ""),
    }));
  }, [sourceOptions]);

  useEffect(() => {
    setSourceLines([]);
    setSourceLineStates({});
  }, [selectedLegalEntityId, selectedBookId, selectedSourceAccountId]);

  useEffect(() => {
    loadRuns();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadRuns, selectedLegalEntityId, selectedBookId, selectedPeriodId]);

  function setTargetValue(targetId, key, value) {
    setTargets((prev) => prev.map((row) => (row.id === targetId ? { ...row, [key]: value } : row)));
  }

  function switchMode(nextMode) {
    setForm((prev) => ({ ...prev, allocationMode: nextMode }));
    setTargets((prev) =>
      prev.map((row) => ({
        ...row,
        percentage: nextMode === "PERCENT" ? row.percentage || "0" : "",
        amount: nextMode === "AMOUNT" ? row.amount || "0" : "",
      }))
    );
  }

  async function handleCreate() {
    setError("");
    setMessage("");
    try {
      const normalizedTargets = targets.map((row, index) => {
        const accountId = toInt(row.accountId);
        if (!accountId) throw new Error(l(`Target ${index + 1}: account required.`, `Hedef ${index + 1}: hesap secin.`));
        if (form.allocationMode === "PERCENT") {
          const percentage = roundAmount(row.percentage);
          if (percentage <= 0) throw new Error(l(`Target ${index + 1}: percentage > 0.`, `Hedef ${index + 1}: yuzde > 0.`));
          return { accountId, percentage };
        }
        const amount = roundAmount(row.amount);
        if (amount <= 0) throw new Error(l(`Target ${index + 1}: amount > 0.`, `Hedef ${index + 1}: tutar > 0.`));
        return { accountId, amount };
      });

      setSaving(true);
      const response = await createBalanceSplitReclassification({
        legalEntityId: selectedLegalEntityId,
        bookId: selectedBookId,
        fiscalPeriodId: selectedPeriodId,
        sourceAccountId: selectedSourceAccountId,
        allocationMode: form.allocationMode,
        entryDate: form.entryDate,
        documentDate: form.documentDate,
        currencyCode: form.currencyCode.trim().toUpperCase(),
        description: form.description.trim() || undefined,
        referenceNo: form.referenceNo.trim() || undefined,
        note: form.note.trim() || undefined,
        targets: normalizedTargets,
      });
      setMessage(l(`Draft created: ${response?.journalNo || "-"}.`, `Taslak olustu: ${response?.journalNo || "-"}.`));
      await loadRuns();
    } catch (err) {
      setError(err?.response?.data?.message || err?.message || l("Create failed.", "Olusturma basarisiz."));
    } finally {
      setSaving(false);
    }
  }

  if (!canReadOrgTree || !canReadBooks || !canReadAccounts || !canReadPeriods || !canReadTrialBalance) {
    return (
      <div className="space-y-4">
        <TenantReadinessChecklist />
        <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          {l(
            "Required: org.tree.read, gl.book.read, gl.account.read, org.fiscal_period.read, gl.trial_balance.read",
            "Gerekli: org.tree.read, gl.book.read, gl.account.read, org.fiscal_period.read, gl.trial_balance.read"
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <TenantReadinessChecklist />
      <div>
        <h1 className="text-xl font-semibold text-slate-900">
          {l("GL Reclassification Workbench", "GL Yeniden Siniflandirma Workbench")}
        </h1>
        <p className="mt-1 text-sm text-slate-600">
          {l("Create one draft journal to split source balance into sub-accounts.", "Kaynak bakiyeyi alt hesaplara dagitmak icin tek taslak fis olusturun.")}
        </p>
      </div>
      {error ? <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{error}</div> : null}
      {message ? <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">{message}</div> : null}

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {l("Create Balance Split", "Bakiye Dagitimi Olustur")}
        </h2>

        <div className="grid gap-2 md:grid-cols-4">
          <select
            value={form.legalEntityId}
            onChange={(event) =>
              setForm((prev) => ({
                ...prev,
                legalEntityId: event.target.value,
                bookId: "",
                fiscalPeriodId: "",
                sourceAccountId: "",
              }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            <option value="">{l("Select legal entity", "Istirak secin")}</option>
            {entities.map((row) => (
              <option key={row.id} value={row.id}>
                {row.code} - {row.name}
              </option>
            ))}
          </select>

          <select
            value={form.bookId}
            onChange={(event) =>
              setForm((prev) => ({ ...prev, bookId: event.target.value, fiscalPeriodId: "" }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            <option value="">{l("Select book", "Defter secin")}</option>
            {books.map((row) => (
              <option key={row.id} value={row.id}>
                {row.code} - {row.name}
              </option>
            ))}
          </select>

          <select
            value={form.fiscalPeriodId}
            onChange={(event) =>
              setForm((prev) => ({ ...prev, fiscalPeriodId: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            <option value="">
              {loadingPeriods
                ? l("Loading periods...", "Donemler yukleniyor...")
                : l("Select fiscal period", "Mali donem secin")}
            </option>
            {periods.map((row) => (
              <option key={row.id} value={row.id}>
                {row.fiscal_year}-P{row.period_no} {row.period_name}
              </option>
            ))}
          </select>

          <select
            value={form.sourceAccountId}
            onChange={(event) =>
              setForm((prev) => ({ ...prev, sourceAccountId: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            <option value="">
              {loadingBalance
                ? l("Loading source balances...", "Kaynak bakiyeler yukleniyor...")
                : l("Source account (direct != 0)", "Kaynak hesap (direkt != 0)")}
            </option>
            {sourceOptions.map((row) => (
              <option key={row.id} value={row.id}>
                {row.code} - {row.name} ({formatAmount(row.directBalance)})
              </option>
            ))}
          </select>

          <select
            value={form.allocationMode}
            onChange={(event) => switchMode(event.target.value)}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            <option value="PERCENT">{l("By percentage", "Yuzdeye gore")}</option>
            <option value="AMOUNT">{l("By amount", "Tutara gore")}</option>
          </select>

          <input
            type="date"
            value={form.entryDate}
            onChange={(event) => setForm((prev) => ({ ...prev, entryDate: event.target.value }))}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          />
          <input
            type="date"
            value={form.documentDate}
            onChange={(event) => setForm((prev) => ({ ...prev, documentDate: event.target.value }))}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          />
          <input
            value={form.currencyCode}
            onChange={(event) => setForm((prev) => ({ ...prev, currencyCode: event.target.value }))}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            placeholder={l("Currency", "Para birimi")}
          />
        </div>

        <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
          {l("Source balance", "Kaynak bakiye")}:{" "}
          <span className="font-semibold">{formatAmount(sourceBalance)}</span> |{" "}
          {l("Reclass amount", "Dagitilacak tutar")}:{" "}
          <span className="font-semibold">{formatAmount(reclassAmount)}</span>
        </div>

        <div className="mt-3 overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">{l("Target account", "Hedef hesap")}</th>
                <th className="px-3 py-2">
                  {form.allocationMode === "PERCENT" ? l("Percentage", "Yuzde") : l("Amount", "Tutar")}
                </th>
                <th className="px-3 py-2">{l("Action", "Islem")}</th>
              </tr>
            </thead>
            <tbody>
              {targets.map((target) => (
                <tr key={target.id} className="border-t border-slate-100">
                  <td className="px-3 py-2">
                    <select
                      value={target.accountId}
                      onChange={(event) => setTargetValue(target.id, "accountId", event.target.value)}
                      className="w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
                    >
                      <option value="">{l("Select target", "Hedef secin")}</option>
                      {targetOptions.map((account) => (
                        <option key={account.id} value={account.id}>
                          {account.code} - {account.name}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td className="px-3 py-2">
                    {form.allocationMode === "PERCENT" ? (
                      <input
                        type="number"
                        min={0}
                        step="0.000001"
                        value={target.percentage}
                        onChange={(event) => setTargetValue(target.id, "percentage", event.target.value)}
                        className="w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
                      />
                    ) : (
                      <input
                        type="number"
                        min={0}
                        step="0.000001"
                        value={target.amount}
                        onChange={(event) => setTargetValue(target.id, "amount", event.target.value)}
                        className="w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
                      />
                    )}
                  </td>
                  <td className="px-3 py-2">
                    <button
                      type="button"
                      onClick={() => setTargets((prev) => prev.filter((row) => row.id !== target.id))}
                      disabled={targets.length <= 1}
                      className="rounded border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-semibold text-slate-700 disabled:opacity-50"
                    >
                      {l("Remove", "Sil")}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
          <button
            type="button"
            onClick={() => setTargets((prev) => [...prev, createTarget(form.allocationMode)])}
            className="rounded border border-slate-300 bg-white px-2.5 py-1.5 font-semibold text-slate-700"
          >
            {l("Add target line", "Hedef satiri ekle")}
          </button>
          {form.allocationMode === "PERCENT" ? (
            <span className="font-semibold text-slate-700">
              {l("Total %", "Toplam %")}: {Number(totalPct).toFixed(6)}
            </span>
          ) : (
            <span className="font-semibold text-slate-700">
              {l("Total amount", "Toplam tutar")}: {formatAmount(totalAmount)} |{" "}
              {l("Difference", "Fark")}: {formatAmount(roundAmount(reclassAmount - totalAmount))}
            </span>
          )}
        </div>

        <div className="mt-3 grid gap-2 md:grid-cols-3">
          <input
            value={form.description}
            onChange={(event) => setForm((prev) => ({ ...prev, description: event.target.value }))}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            placeholder={l("Description (optional)", "Aciklama (opsiyonel)")}
          />
          <input
            value={form.referenceNo}
            onChange={(event) => setForm((prev) => ({ ...prev, referenceNo: event.target.value }))}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            placeholder={l("Reference no (optional)", "Referans no (opsiyonel)")}
          />
          <input
            value={form.note}
            onChange={(event) => setForm((prev) => ({ ...prev, note: event.target.value }))}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            placeholder={l("Run note (optional)", "Run notu (opsiyonel)")}
          />
        </div>

        <div className="mt-3">
          <button
            type="button"
            onClick={() => handleCreate()}
            disabled={
              !canCreate ||
              saving ||
              !selectedLegalEntityId ||
              !selectedBookId ||
              !selectedPeriodId ||
              !selectedSourceAccountId ||
              reclassAmount <= 0
            }
            className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
          >
            {saving
              ? l("Creating draft...", "Taslak olusturuluyor...")
              : l("Create reclassification draft", "Yeniden siniflandirma taslagi olustur")}
          </button>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {l("Transaction Reclass (line mapping)", "Islem Bazli Yeniden Siniflandirma")}
        </h2>

        <div className="grid gap-2 md:grid-cols-4">
          <input
            type="date"
            value={txFilter.dateFrom}
            onChange={(event) =>
              setTxFilter((prev) => ({ ...prev, dateFrom: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          />
          <input
            type="date"
            value={txFilter.dateTo}
            onChange={(event) =>
              setTxFilter((prev) => ({ ...prev, dateTo: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          />
          <input
            type="number"
            min={1}
            max={1000}
            value={txFilter.limit}
            onChange={(event) =>
              setTxFilter((prev) => ({ ...prev, limit: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            placeholder={l("Limit", "Limit")}
          />
          <button
            type="button"
            onClick={() => loadSourceLines()}
            disabled={
              loadingSourceLines ||
              !selectedLegalEntityId ||
              !selectedBookId ||
              !selectedSourceAccountId
            }
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm font-semibold text-slate-700 disabled:opacity-60"
          >
            {loadingSourceLines
              ? l("Loading lines...", "Satirlar yukleniyor...")
              : l("Load source lines", "Kaynak satirlari yukle")}
          </button>
        </div>

        <div className="mt-2 text-xs text-slate-600">
          {l(
            "Select lines and map each selected line to a target sub-account. The system will reverse source line and recreate on target.",
            "Satirlari secip her secili satiri hedef alt hesaba esleyin. Sistem kaynak satiri terse cevirip hedefte yeniden olusturur."
          )}
        </div>

        <div className="mt-3 overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">{l("Select", "Sec")}</th>
                <th className="px-3 py-2">{l("Date", "Tarih")}</th>
                <th className="px-3 py-2">{l("Journal", "Yevmiye")}</th>
                <th className="px-3 py-2">{l("Description", "Aciklama")}</th>
                <th className="px-3 py-2">{l("Amount", "Tutar")}</th>
                <th className="px-3 py-2">{l("Target account", "Hedef hesap")}</th>
              </tr>
            </thead>
            <tbody>
              {sourceLines.map((row) => {
                const rowId = toInt(row.journalLineId);
                const state = sourceLineStates[rowId] || {
                  selected: false,
                  targetAccountId: "",
                };
                const amount = Math.abs(toAmount(row.debitBase || row.creditBase));
                return (
                  <tr key={row.journalLineId} className="border-t border-slate-100">
                    <td className="px-3 py-2">
                      <input
                        type="checkbox"
                        checked={Boolean(state.selected)}
                        onChange={(event) =>
                          setSourceLineStates((prev) => ({
                            ...prev,
                            [rowId]: {
                              selected: event.target.checked,
                              targetAccountId: prev[rowId]?.targetAccountId || "",
                            },
                          }))
                        }
                      />
                    </td>
                    <td className="px-3 py-2">{row.entryDate || "-"}</td>
                    <td className="px-3 py-2">{row.journalNo || "-"}</td>
                    <td className="px-3 py-2">{row.description || "-"}</td>
                    <td className="px-3 py-2">{formatAmount(amount)}</td>
                    <td className="px-3 py-2">
                      <select
                        value={state.targetAccountId}
                        onChange={(event) =>
                          setSourceLineStates((prev) => ({
                            ...prev,
                            [rowId]: {
                              selected: prev[rowId]?.selected || false,
                              targetAccountId: event.target.value,
                            },
                          }))
                        }
                        className="w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm"
                        disabled={!state.selected}
                      >
                        <option value="">
                          {l("Select target", "Hedef secin")}
                        </option>
                        {targetOptions.map((account) => (
                          <option key={account.id} value={account.id}>
                            {account.code} - {account.name}
                          </option>
                        ))}
                      </select>
                    </td>
                  </tr>
                );
              })}
              {sourceLines.length === 0 && !loadingSourceLines ? (
                <tr>
                  <td colSpan={6} className="px-3 py-3 text-slate-500">
                    {l(
                      "No source lines loaded. Set date range and click load.",
                      "Kaynak satir yok. Tarih araligi secip yukleyin."
                    )}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>

        <div className="mt-2 text-xs font-semibold text-slate-700">
          {l("Selected lines", "Secili satir")}: {selectedSourceLines.length} |{" "}
          {l("Mapped", "Eslenen")}: {selectedSourceLinesMappedCount} |{" "}
          {l("Total amount", "Toplam tutar")}: {formatAmount(selectedSourceLinesTotal)}
        </div>

        <div className="mt-3">
          <button
            type="button"
            onClick={() => handleCreateTransactionReclass()}
            disabled={
              !canCreate ||
              saving ||
              !selectedLegalEntityId ||
              !selectedBookId ||
              !selectedPeriodId ||
              !selectedSourceAccountId ||
              selectedSourceLines.length === 0 ||
              selectedSourceLines.length !== selectedSourceLinesMappedCount
            }
            className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
          >
            {saving
              ? l("Creating draft...", "Taslak olusturuluyor...")
              : l(
                  "Create transaction reclass draft",
                  "Islem bazli yeniden siniflandirma taslagi olustur"
                )}
          </button>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-3 flex items-center justify-between gap-2">
          <h2 className="text-sm font-semibold text-slate-700">
            {l("Recent Reclassification Runs", "Son Yeniden Siniflandirma Runlari")}
          </h2>
          <button
            type="button"
            onClick={() => loadRuns()}
            disabled={loadingRuns || !canReadRuns}
            className="rounded border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-semibold text-slate-700 disabled:opacity-60"
          >
            {loadingRuns ? l("Loading...", "Yukleniyor...") : l("Refresh", "Yenile")}
          </button>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">ID</th>
                <th className="px-3 py-2">{l("Created", "Olusma")}</th>
                <th className="px-3 py-2">{l("Source", "Kaynak")}</th>
                <th className="px-3 py-2">{l("Amount", "Tutar")}</th>
                <th className="px-3 py-2">{l("Journal", "Yevmiye")}</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((row) => (
                <tr key={row.id} className="border-t border-slate-100">
                  <td className="px-3 py-2">{row.id}</td>
                  <td className="px-3 py-2">{row.createdAt || "-"}</td>
                  <td className="px-3 py-2">{row.sourceAccountCode || "-"} - {row.sourceAccountName || "-"}</td>
                  <td className="px-3 py-2">{formatAmount(row.reclassAmount)} {row.currencyCode || ""}</td>
                  <td className="px-3 py-2">
                    {row.journalNo ? `${row.journalNo} (#${row.journalEntryId})` : "-"}
                  </td>
                </tr>
              ))}
              {runs.length === 0 && !loadingRuns ? (
                <tr>
                  <td colSpan={5} className="px-3 py-3 text-slate-500">
                    {l("No reclassification runs found.", "Yeniden siniflandirma run kaydi yok.")}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      {loading ? (
        <div className="text-xs text-slate-500">
          {l("Loading references...", "Referanslar yukleniyor...")}
        </div>
      ) : null}
    </div>
  );
}
