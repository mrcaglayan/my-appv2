import { useEffect, useMemo, useState } from "react";
import { listLegalEntities } from "../../api/orgAdmin.js";
import { listCariCounterparties } from "../../api/cariCounterparty.js";
import {
  getCariApAgingReport,
  getCariArAgingReport,
  getCariCounterpartyStatementReport,
  getCariOpenItemsReport,
} from "../../api/cariReports.js";
import { useAuth } from "../../auth/useAuth.js";
import {
  buildCariReportQuery,
  reconcileOpenItemsSummary,
  reconcileStatementSummary,
  REPORT_TABS,
  ROLE_FILTER_OPTIONS,
  STATUS_FILTER_OPTIONS,
} from "./cariReportsUtils.js";

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatAmount(value) {
  const amount = toNumber(value);
  return amount.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 6,
  });
}

function formatDate(value) {
  const normalized = String(value || "").slice(0, 10);
  if (!normalized) {
    return "-";
  }
  const date = new Date(`${normalized}T00:00:00`);
  if (Number.isNaN(date.getTime())) {
    return normalized;
  }
  return date.toLocaleDateString();
}

function normalizeError(err, fallback) {
  return String(err?.response?.data?.message || err?.message || fallback);
}

const TAB_CONFIG = [
  { id: REPORT_TABS.AR_AGING, label: "AR Aging" },
  { id: REPORT_TABS.AP_AGING, label: "AP Aging" },
  { id: REPORT_TABS.OPEN_ITEMS, label: "Open Items" },
  { id: REPORT_TABS.STATEMENT, label: "Counterparty Statement" },
];

const DEFAULT_FILTERS = {
  asOfDate: todayIsoDate(),
  legalEntityId: "",
  counterpartyId: "",
  role: "",
  status: "OPEN",
  limit: 200,
  offset: 0,
};

function renderSummaryCards(summary) {
  if (!summary) {
    return null;
  }

  const cards = [
    ["Count", summary.count],
    ["Open", summary.openCount ?? summary.postedCount],
    ["Partial", summary.partiallySettledCount ?? summary.partiallyAppliedCount],
    ["Settled", summary.settledCount ?? summary.fullyAppliedCount],
    ["Residual Txn", formatAmount(summary.residualAmountTxnTotal)],
    ["Residual Base", formatAmount(summary.residualAmountBaseTotal)],
  ];

  return (
    <section className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
      {cards.map(([label, value]) => (
        <article key={`summary-${label}`} className="rounded-xl border border-slate-200 bg-white p-3">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</p>
          <p className="mt-1 text-lg font-semibold text-slate-900">{value ?? "-"}</p>
        </article>
      ))}
    </section>
  );
}

export default function CariReportsPage() {
  const { hasPermission } = useAuth();
  const canReadReports = hasPermission("cari.report.read");
  const canReadCards = hasPermission("cari.card.read");
  const canReadOrg = hasPermission("org.tree.read");

  const [activeTab, setActiveTab] = useState(REPORT_TABS.AR_AGING);
  const [filters, setFilters] = useState(DEFAULT_FILTERS);
  const [reportData, setReportData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [legalEntities, setLegalEntities] = useState([]);
  const [counterparties, setCounterparties] = useState([]);
  const [lookupWarning, setLookupWarning] = useState("");

  const openItemsReconcile = useMemo(
    () => reconcileOpenItemsSummary(reportData),
    [reportData]
  );
  const statementReconcile = useMemo(
    () => reconcileStatementSummary(reportData),
    [reportData]
  );

  async function loadLookups() {
    const warnings = [];
    if (canReadOrg) {
      try {
        const leResponse = await listLegalEntities({ limit: 500, includeInactive: true });
        setLegalEntities(Array.isArray(leResponse?.rows) ? leResponse.rows : []);
      } catch (err) {
        setLegalEntities([]);
        warnings.push(normalizeError(err, "Legal entity lookup failed"));
      }
    } else {
      setLegalEntities([]);
    }

    if (canReadCards) {
      try {
        const cpResponse = await listCariCounterparties({ limit: 500, offset: 0 });
        setCounterparties(Array.isArray(cpResponse?.rows) ? cpResponse.rows : []);
      } catch (err) {
        setCounterparties([]);
        warnings.push(normalizeError(err, "Counterparty lookup failed"));
      }
    } else {
      setCounterparties([]);
    }

    setLookupWarning(warnings.join(" "));
  }

  async function loadReport(nextTab = activeTab, nextFilters = filters) {
    if (!canReadReports) {
      setReportData(null);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const queryParams = buildCariReportQuery(nextFilters, nextTab);
      let payload = null;
      if (nextTab === REPORT_TABS.AR_AGING) {
        payload = await getCariArAgingReport(queryParams);
      } else if (nextTab === REPORT_TABS.AP_AGING) {
        payload = await getCariApAgingReport(queryParams);
      } else if (nextTab === REPORT_TABS.OPEN_ITEMS) {
        payload = await getCariOpenItemsReport(queryParams);
      } else {
        payload = await getCariCounterpartyStatementReport(queryParams);
      }
      setReportData(payload || null);
    } catch (err) {
      setReportData(null);
      setError(normalizeError(err, "Failed to load report."));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadLookups();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadCards, canReadOrg]);

  useEffect(() => {
    loadReport(activeTab, filters);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, canReadReports]);

  if (!canReadReports) {
    return (
      <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
        Missing permission: `cari.report.read`
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h1 className="text-xl font-semibold text-slate-900">Cari Reports</h1>
        <p className="mt-1 text-sm text-slate-600">
          AR/AP aging, open-items, and counterparty statement with as-of logic.
        </p>

        {lookupWarning ? (
          <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
            {lookupWarning}
          </div>
        ) : null}
        {error ? (
          <div className="mt-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
            {error}
          </div>
        ) : null}

        <div className="mt-4 grid gap-2 md:grid-cols-6">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            As-Of Date
            <input
              type="date"
              className="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm font-normal"
              value={filters.asOfDate}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, asOfDate: event.target.value }))
              }
            />
          </label>

          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            Legal Entity
            {legalEntities.length > 0 ? (
              <select
                className="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm font-normal"
                value={filters.legalEntityId}
                onChange={(event) =>
                  setFilters((prev) => ({ ...prev, legalEntityId: event.target.value }))
                }
              >
                <option value="">All in scope</option>
                {legalEntities.map((row) => (
                  <option key={`cari-reports-le-${row.id}`} value={row.id}>
                    {row.code} - {row.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min="1"
                className="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm font-normal"
                value={filters.legalEntityId}
                onChange={(event) =>
                  setFilters((prev) => ({ ...prev, legalEntityId: event.target.value }))
                }
                placeholder="legalEntityId"
              />
            )}
          </label>

          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            Counterparty
            {counterparties.length > 0 ? (
              <select
                className="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm font-normal"
                value={filters.counterpartyId}
                onChange={(event) =>
                  setFilters((prev) => ({ ...prev, counterpartyId: event.target.value }))
                }
              >
                <option value="">All</option>
                {counterparties.map((row) => (
                  <option key={`cari-reports-cp-${row.id}`} value={row.id}>
                    {row.code} - {row.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min="1"
                className="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm font-normal"
                value={filters.counterpartyId}
                onChange={(event) =>
                  setFilters((prev) => ({ ...prev, counterpartyId: event.target.value }))
                }
                placeholder="counterpartyId"
              />
            )}
          </label>

          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            Customer / Vendor
            <select
              className="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm font-normal"
              value={filters.role}
              onChange={(event) => setFilters((prev) => ({ ...prev, role: event.target.value }))}
            >
              {ROLE_FILTER_OPTIONS.map((option) => (
                <option key={`cari-reports-role-${option || "ALL"}`} value={option}>
                  {option || "ALL"}
                </option>
              ))}
            </select>
          </label>

          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            Status
            <select
              className="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm font-normal"
              value={filters.status}
              onChange={(event) => setFilters((prev) => ({ ...prev, status: event.target.value }))}
            >
              {STATUS_FILTER_OPTIONS.map((option) => (
                <option key={`cari-reports-status-${option}`} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>

          <div className="flex flex-col justify-end gap-2">
            <button
              type="button"
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
              onClick={() => loadReport(activeTab, filters)}
              disabled={loading}
            >
              {loading ? "Loading..." : "Apply Filters"}
            </button>
            <button
              type="button"
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-semibold text-slate-700"
              onClick={() => {
                setFilters(DEFAULT_FILTERS);
                loadReport(activeTab, DEFAULT_FILTERS);
              }}
              disabled={loading}
            >
              Reset
            </button>
          </div>
        </div>

        <div className="mt-4 flex flex-wrap gap-2">
          {TAB_CONFIG.map((tab) => (
            <button
              key={tab.id}
              type="button"
              className={`rounded-lg px-3 py-2 text-sm font-semibold ${
                activeTab === tab.id
                  ? "bg-cyan-700 text-white"
                  : "border border-slate-300 text-slate-700"
              }`}
              onClick={() => setActiveTab(tab.id)}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </section>

      {activeTab === REPORT_TABS.OPEN_ITEMS && reportData ? (
        <div
          className={`rounded-lg border px-3 py-2 text-sm ${
            openItemsReconcile.matches
              ? "border-emerald-200 bg-emerald-50 text-emerald-700"
              : "border-amber-200 bg-amber-50 text-amber-800"
          }`}
        >
          API vs row-total reconcile (open items): txn diff={openItemsReconcile.txnDiff}, base diff=
          {openItemsReconcile.baseDiff}
        </div>
      ) : null}

      {activeTab === REPORT_TABS.STATEMENT && reportData ? (
        <div
          className={`rounded-lg border px-3 py-2 text-sm ${
            statementReconcile.matches
              ? "border-emerald-200 bg-emerald-50 text-emerald-700"
              : "border-amber-200 bg-amber-50 text-amber-800"
          }`}
        >
          Statement reconcile (document vs open-item residual): txn diff={statementReconcile.txnDiff},
          base diff={statementReconcile.baseDiff}
        </div>
      ) : null}

      {renderSummaryCards(reportData?.summary)}

      {(activeTab === REPORT_TABS.AR_AGING || activeTab === REPORT_TABS.AP_AGING) && reportData ? (
        <section className="rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="text-sm font-semibold text-slate-700">Aging Buckets</h2>
          <div className="mt-3 overflow-x-auto rounded-lg border border-slate-200">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-3 py-2">Bucket</th>
                  <th className="px-3 py-2">Count</th>
                  <th className="px-3 py-2">Residual Txn</th>
                  <th className="px-3 py-2">Residual Base</th>
                </tr>
              </thead>
              <tbody>
                {(reportData.buckets || []).map((row) => (
                  <tr key={`bucket-${row.bucketCode}`} className="border-t border-slate-100">
                    <td className="px-3 py-2">{row.bucketLabel}</td>
                    <td className="px-3 py-2">{row.count}</td>
                    <td className="px-3 py-2">{formatAmount(row.residualAmountTxnTotal)}</td>
                    <td className="px-3 py-2">{formatAmount(row.residualAmountBaseTotal)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {activeTab === REPORT_TABS.OPEN_ITEMS && reportData ? (
        <>
          <section className="rounded-xl border border-slate-200 bg-white p-4">
            <h2 className="text-sm font-semibold text-slate-700">Open-Item Rows</h2>
            <div className="mt-3 overflow-x-auto rounded-lg border border-slate-200">
              <table className="min-w-full text-sm">
                <thead className="bg-slate-50 text-left text-slate-600">
                  <tr>
                    <th className="px-3 py-2">Doc</th>
                    <th className="px-3 py-2">Due</th>
                    <th className="px-3 py-2">Status</th>
                    <th className="px-3 py-2">Original</th>
                    <th className="px-3 py-2">Residual</th>
                    <th className="px-3 py-2">Bucket</th>
                    <th className="px-3 py-2">Settlements</th>
                    <th className="px-3 py-2">Bank Linked</th>
                  </tr>
                </thead>
                <tbody>
                  {(reportData.rows || []).map((row) => (
                    <tr key={`open-item-${row.openItemId}`} className="border-t border-slate-100">
                      <td className="px-3 py-2">{row.documentNo || row.documentId}</td>
                      <td className="px-3 py-2">{formatDate(row.dueDate)}</td>
                      <td className="px-3 py-2">{row.asOfStatus}</td>
                      <td className="px-3 py-2">{formatAmount(row.originalAmountTxn)}</td>
                      <td className="px-3 py-2">{formatAmount(row.residualAmountTxnAsOf)}</td>
                      <td className="px-3 py-2">{row.agingBucket?.label || "-"}</td>
                      <td className="px-3 py-2">{row.settlementContext?.allocationCountAsOf || 0}</td>
                      <td className="px-3 py-2">
                        {row.settlementContext?.bankLinkedAllocationCountAsOf || 0}
                      </td>
                    </tr>
                  ))}
                  {(reportData.rows || []).length === 0 ? (
                    <tr>
                      <td colSpan={8} className="px-3 py-3 text-slate-500">
                        No rows.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4">
            <h2 className="text-sm font-semibold text-slate-700">Unapplied Balances</h2>
            <div className="mt-3 overflow-x-auto rounded-lg border border-slate-200">
              <table className="min-w-full text-sm">
                <thead className="bg-slate-50 text-left text-slate-600">
                  <tr>
                    <th className="px-3 py-2">Receipt No</th>
                    <th className="px-3 py-2">Date</th>
                    <th className="px-3 py-2">As-Of Status</th>
                    <th className="px-3 py-2">Amount</th>
                    <th className="px-3 py-2">Residual</th>
                    <th className="px-3 py-2">Bank Ref</th>
                  </tr>
                </thead>
                <tbody>
                  {(reportData.unapplied?.rows || []).map((row) => (
                    <tr key={`unapplied-${row.unappliedCashId}`} className="border-t border-slate-100">
                      <td className="px-3 py-2">{row.cashReceiptNo || row.unappliedCashId}</td>
                      <td className="px-3 py-2">{formatDate(row.receiptDate)}</td>
                      <td className="px-3 py-2">{row.asOfStatus}</td>
                      <td className="px-3 py-2">{formatAmount(row.amountTxn)}</td>
                      <td className="px-3 py-2">{formatAmount(row.residualAmountTxnAsOf)}</td>
                      <td className="px-3 py-2">
                        {row.bankTransactionRef || row.bankStatementLineId || "-"}
                      </td>
                    </tr>
                  ))}
                  {(reportData.unapplied?.rows || []).length === 0 ? (
                    <tr>
                      <td colSpan={6} className="px-3 py-3 text-slate-500">
                        No unapplied rows.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}

      {activeTab === REPORT_TABS.STATEMENT && reportData ? (
        <>
          <section className="rounded-xl border border-slate-200 bg-white p-4">
            <h2 className="text-sm font-semibold text-slate-700">Documents</h2>
            <div className="mt-3 overflow-x-auto rounded-lg border border-slate-200">
              <table className="min-w-full text-sm">
                <thead className="bg-slate-50 text-left text-slate-600">
                  <tr>
                    <th className="px-3 py-2">Doc</th>
                    <th className="px-3 py-2">Date</th>
                    <th className="px-3 py-2">Status</th>
                    <th className="px-3 py-2">Amount</th>
                    <th className="px-3 py-2">Open As-Of</th>
                    <th className="px-3 py-2">Reversal Link</th>
                  </tr>
                </thead>
                <tbody>
                  {(reportData.documents?.rows || []).map((row) => (
                    <tr key={`stmt-doc-${row.documentId}`} className="border-t border-slate-100">
                      <td className="px-3 py-2">{row.documentNo || row.documentId}</td>
                      <td className="px-3 py-2">{formatDate(row.documentDate)}</td>
                      <td className="px-3 py-2">{row.asOfStatus}</td>
                      <td className="px-3 py-2">{formatAmount(row.amountTxn)}</td>
                      <td className="px-3 py-2">{formatAmount(row.asOfOpenAmountTxn)}</td>
                      <td className="px-3 py-2">
                        {row.reversedByDocumentNo || row.reversalOfDocumentId || "-"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4">
            <h2 className="text-sm font-semibold text-slate-700">Settlements & Reversals</h2>
            <div className="mt-3 overflow-x-auto rounded-lg border border-slate-200">
              <table className="min-w-full text-sm">
                <thead className="bg-slate-50 text-left text-slate-600">
                  <tr>
                    <th className="px-3 py-2">Settlement</th>
                    <th className="px-3 py-2">Date</th>
                    <th className="px-3 py-2">Status</th>
                    <th className="px-3 py-2">Total</th>
                    <th className="px-3 py-2">Cash Txn</th>
                    <th className="px-3 py-2">Reversal Of</th>
                    <th className="px-3 py-2">Reversed By</th>
                    <th className="px-3 py-2">Bank Ref</th>
                  </tr>
                </thead>
                <tbody>
                  {(reportData.settlements?.rows || []).map((row) => (
                    <tr key={`stmt-settle-${row.settlementBatchId}`} className="border-t border-slate-100">
                      <td className="px-3 py-2">{row.settlementNo || row.settlementBatchId}</td>
                      <td className="px-3 py-2">{formatDate(row.settlementDate)}</td>
                      <td className="px-3 py-2">{row.statusCurrent}</td>
                      <td className="px-3 py-2">{formatAmount(row.totalAllocatedTxn)}</td>
                      <td className="px-3 py-2">{row.cashTransactionId || "-"}</td>
                      <td className="px-3 py-2">{row.reversalOfSettlementNo || "-"}</td>
                      <td className="px-3 py-2">{row.reversedBySettlementNo || "-"}</td>
                      <td className="px-3 py-2">
                        {row.bankTransactionRef || row.bankStatementLineId || "-"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}
    </div>
  );
}
