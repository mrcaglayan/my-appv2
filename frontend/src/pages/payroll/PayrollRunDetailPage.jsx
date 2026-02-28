import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  finalizePayrollRun,
  getPayrollRun,
  getPayrollRunAccrualPreview,
  reviewPayrollRun,
} from "../../api/payrollRuns.js";
import {
  createPayrollCorrectionShell,
  listPayrollRunCorrections,
  reversePayrollRunWithCorrection,
} from "../../api/payrollCorrections.js";
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

function buildCloseControlsHrefFromRun(row) {
  const params = new URLSearchParams();
  if (row?.legal_entity_id) params.set("legalEntityId", String(row.legal_entity_id));
  const payrollPeriod = formatDate(row?.payroll_period);
  if (payrollPeriod && payrollPeriod !== "-") {
    params.set("payrollPeriod", payrollPeriod);
  }
  const query = params.toString();
  return `/app/payroll-close-controls${query ? `?${query}` : ""}`;
}

function sourceLabel(row) {
  const sourceType = String(row?.source_type || "").toUpperCase();
  if (!sourceType || sourceType === "MANUAL") return null;
  if (sourceType === "PROVIDER_IMPORT") {
    const jobId = row?.source_provider_import_job_id ? ` #${row.source_provider_import_job_id}` : "";
    const provider = row?.source_provider_code ? ` (${row.source_provider_code})` : "";
    return `PROVIDER_IMPORT${jobId}${provider}`;
  }
  return sourceType;
}

export default function PayrollRunDetailPage() {
  const { runId } = useParams();
  const { hasPermission } = useAuth();
  const canRead = hasPermission("payroll.runs.read");
  const canReview = hasPermission("payroll.runs.review");
  const canFinalize = hasPermission("payroll.runs.finalize");
  const canReadLiabilities = hasPermission("payroll.liabilities.read");
  const canCorrectionsRead = hasPermission("payroll.corrections.read");
  const canCorrectionsCreate = hasPermission("payroll.corrections.create");
  const canCorrectionsReverse = hasPermission("payroll.corrections.reverse");
  const canImportRuns = hasPermission("payroll.runs.import");

  const [row, setRow] = useState(null);
  const [preview, setPreview] = useState(null);
  const [corrections, setCorrections] = useState([]);
  const [loading, setLoading] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [correctionsLoading, setCorrectionsLoading] = useState(false);
  const [actionBusy, setActionBusy] = useState(false);
  const [correctionsActionBusy, setCorrectionsActionBusy] = useState(false);
  const [error, setError] = useState("");
  const [previewError, setPreviewError] = useState("");
  const [correctionsError, setCorrectionsError] = useState("");

  async function loadRow() {
    if (!canRead) {
      setRow(null);
      setPreview(null);
      return;
    }
    setLoading(true);
    setPreviewLoading(true);
    setError("");
    setPreviewError("");
    try {
      const [runRes, previewRes] = await Promise.all([
        getPayrollRun(runId),
        getPayrollRunAccrualPreview(runId).catch((err) => {
          setPreviewError(
            err?.response?.data?.message || err?.message || "Tahakkuk preview yuklenemedi"
          );
          return null;
        }),
      ]);
      setRow(runRes?.row || null);
      setPreview(previewRes?.preview || null);
    } catch (err) {
      setRow(null);
      setPreview(null);
      setError(err?.response?.data?.message || "Payroll run detayi yuklenemedi");
    } finally {
      setLoading(false);
      setPreviewLoading(false);
    }
  }

  async function loadCorrections() {
    if (!canCorrectionsRead) {
      setCorrections([]);
      setCorrectionsError("");
      return;
    }
    setCorrectionsLoading(true);
    setCorrectionsError("");
    try {
      const res = await listPayrollRunCorrections(runId);
      setCorrections(res?.items || []);
    } catch (err) {
      setCorrections([]);
      setCorrectionsError(
        err?.response?.data?.message || "Correction / reversal kayitlari yuklenemedi"
      );
    } finally {
      setCorrectionsLoading(false);
    }
  }

  async function handleReview() {
    if (!canReview) {
      setError("Missing permission: payroll.runs.review");
      return;
    }
    setActionBusy(true);
    setError("");
    try {
      await reviewPayrollRun(runId, {});
      await loadRow();
    } catch (err) {
      setError(err?.response?.data?.message || "Review islemi basarisiz");
    } finally {
      setActionBusy(false);
    }
  }

  async function handleFinalize() {
    if (!canFinalize) {
      setError("Missing permission: payroll.runs.finalize");
      return;
    }
    setActionBusy(true);
    setError("");
    try {
      await finalizePayrollRun(runId, {});
      await loadRow();
    } catch (err) {
      setError(err?.response?.data?.message || "Finalize islemi basarisiz");
    } finally {
      setActionBusy(false);
    }
  }

  async function handleCreateCorrectionShell(correctionType) {
    if (!canCorrectionsCreate) {
      setCorrectionsError("Missing permission: payroll.corrections.create");
      return;
    }
    if (!row) return;
    const reason =
      window.prompt(
        `${correctionType} correction shell reason (opsiyonel)`,
        correctionType === "RETRO" ? "Retro payroll correction" : "Off-cycle payroll payment"
      ) || "";

    setCorrectionsActionBusy(true);
    setCorrectionsError("");
    try {
      const res = await createPayrollCorrectionShell({
        correctionType,
        originalRunId: Number(runId),
        entityCode: row.entity_code,
        providerCode: row.provider_code,
        payrollPeriod: formatDate(row.payroll_period),
        payDate: formatDate(row.pay_date),
        currencyCode: row.currency_code,
        reason: reason || null,
      });
      const createdRunId = res?.correction_run?.id;
      await loadCorrections();
      if (createdRunId) {
        setError("");
      }
    } catch (err) {
      setCorrectionsError(err?.response?.data?.message || "Correction shell olusturulamadi");
    } finally {
      setCorrectionsActionBusy(false);
    }
  }

  async function handleReverseRun() {
    if (!canCorrectionsReverse) {
      setCorrectionsError("Missing permission: payroll.corrections.reverse");
      return;
    }
    const reason = (window.prompt("Reversal reason", "Payroll run reversal") || "").trim();
    if (!reason) {
      setCorrectionsError("Reversal reason gerekli");
      return;
    }

    setCorrectionsActionBusy(true);
    setCorrectionsError("");
    setError("");
    try {
      await reversePayrollRunWithCorrection(runId, { reason });
      await Promise.all([loadRow(), loadCorrections()]);
    } catch (err) {
      setCorrectionsError(err?.response?.data?.message || "Payroll reversal basarisiz");
    } finally {
      setCorrectionsActionBusy(false);
    }
  }

  useEffect(() => {
    loadRow();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId, canRead]);

  useEffect(() => {
    loadCorrections();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId, canCorrectionsRead]);

  if (!canRead) {
    return (
      <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
        Missing permission: <code>payroll.runs.read</code>
      </div>
    );
  }

  if (loading && !row) {
    return <div className="p-4">Yukleniyor...</div>;
  }

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2">
            <Link to="/app/payroll-runs" className="text-sm underline">
              ← Bordro Runlari
            </Link>
            <h1 className="text-xl font-semibold text-slate-900">
              {row?.run_no || `Run #${runId}`}
            </h1>
            {row?.status ? (
              <span className="rounded border px-2 py-0.5 text-xs">{row.status}</span>
            ) : null}
            {sourceLabel(row) ? (
              <span className="rounded border border-indigo-200 bg-indigo-50 px-2 py-0.5 text-xs text-indigo-700">
                {sourceLabel(row)}
              </span>
            ) : null}
          </div>
          <p className="mt-1 text-sm text-slate-600">
            Employee-level payroll import detail + audit (PR-P01).
          </p>
        </div>
        <div className="flex items-center gap-2">
          {row?.legal_entity_id ? (
            <Link
              to={buildCloseControlsHrefFromRun(row)}
              className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700"
            >
              Close Controls
            </Link>
          ) : null}
          {canReadLiabilities ? (
            <Link
              to={`/app/payroll-runs/${runId}/liabilities`}
              className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700"
            >
              Liabilities & Payment Prep
            </Link>
          ) : null}
          <button
            type="button"
            className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700"
            onClick={() => {
              loadRow();
              loadCorrections();
            }}
            disabled={loading || correctionsLoading}
          >
            Yenile
          </button>
        </div>
      </div>

      {error ? (
        <div className="rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
          {error}
        </div>
      ) : null}

      {row ? (
        <>
          <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <div className="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-4">
              <div>
                <div className="text-xs text-slate-500">Entity</div>
                <div className="font-medium">{row.entity_code}</div>
                <div className="text-xs text-slate-500">{row.legal_entity_name || ""}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Provider</div>
                <div className="font-medium">{row.provider_code}</div>
                {row.source_provider_code ? (
                  <div className="text-xs text-slate-500">Source Provider: {row.source_provider_code}</div>
                ) : null}
              </div>
              <div>
                <div className="text-xs text-slate-500">Payroll Period</div>
                <div className="font-medium">{formatDate(row.payroll_period)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Pay Date</div>
                <div className="font-medium">{formatDate(row.pay_date)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Currency</div>
                <div className="font-medium">{row.currency_code}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Employees</div>
                <div className="font-medium">{row.employee_count}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Rows</div>
                <div className="font-medium">
                  {row.line_count_total} / inserted {row.line_count_inserted} / dup {row.line_count_duplicates}
                </div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Imported At</div>
                <div className="font-medium">{formatDateTime(row.imported_at)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Source Type</div>
                <div className="font-medium">{row.source_type || "MANUAL"}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Provider Import Job</div>
                <div className="font-medium">{row.source_provider_import_job_id || "-"}</div>
              </div>
            </div>

            <div className="mt-4 grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-4">
              <div>
                <div className="text-xs text-slate-500">Total Gross</div>
                <div className="font-medium">{formatAmount(row.total_gross_pay)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Total Net</div>
                <div className="font-medium">{formatAmount(row.total_net_pay)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Emp Tax</div>
                <div className="font-medium">{formatAmount(row.total_employee_tax)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Emp SS</div>
                <div className="font-medium">{formatAmount(row.total_employee_social_security)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Employer Tax</div>
                <div className="font-medium">{formatAmount(row.total_employer_tax)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Employer SS</div>
                <div className="font-medium">{formatAmount(row.total_employer_social_security)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Allowances</div>
                <div className="font-medium">{formatAmount(row.total_allowances)}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Filename</div>
                <div className="font-medium break-all">{row.original_filename}</div>
              </div>
            </div>
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <h2 className="text-sm font-semibold text-slate-900">Corrections / Reversals (PR-P05)</h2>
                <p className="mt-1 text-xs text-slate-600">
                  Reverse finalized payroll runs safely or create OFF_CYCLE / RETRO correction shells.
                </p>
              </div>
              <div className="flex flex-wrap gap-2">
                {canCorrectionsCreate ? (
                  <>
                    <button
                      type="button"
                      className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 disabled:opacity-60"
                      onClick={() => handleCreateCorrectionShell("OFF_CYCLE")}
                      disabled={correctionsActionBusy || !row}
                    >
                      Create OFF_CYCLE Shell
                    </button>
                    <button
                      type="button"
                      className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 disabled:opacity-60"
                      onClick={() => handleCreateCorrectionShell("RETRO")}
                      disabled={correctionsActionBusy || !row}
                    >
                      Create RETRO Shell
                    </button>
                  </>
                ) : null}
                {canCorrectionsReverse ? (
                  <button
                    type="button"
                    className="rounded border border-rose-300 bg-rose-50 px-2 py-1 text-xs text-rose-700 disabled:opacity-60"
                    onClick={handleReverseRun}
                    disabled={
                      correctionsActionBusy ||
                      !row ||
                      row.status !== "FINALIZED" ||
                      row.run_type === "REVERSAL" ||
                      Number(row.is_reversed || 0) === 1
                    }
                    title={
                      row?.status === "FINALIZED"
                        ? "Finalize edilmis bordro run'ini ters kayda cevir"
                        : "Yalnizca FINALIZED run reversal alabilir"
                    }
                  >
                    Reverse Run
                  </button>
                ) : null}
              </div>
            </div>

            <div className="mt-3 grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-4">
              <div>
                <div className="text-xs text-slate-500">Run Type</div>
                <div className="font-medium">{row.run_type || "REGULAR"}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Correction Of</div>
                <div className="font-medium">{row.correction_of_run_id || "-"}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Is Reversed</div>
                <div className="font-medium">{Number(row.is_reversed || 0) === 1 ? "YES" : "NO"}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Reversed By Run</div>
                <div className="font-medium">{row.reversed_by_run_id || "-"}</div>
              </div>
            </div>

            {correctionsError ? (
              <div className="mt-3 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
                {correctionsError}
              </div>
            ) : null}

            {correctionsLoading ? (
              <div className="mt-3 text-sm text-slate-500">Correction kayitlari yukleniyor...</div>
            ) : null}

            {!correctionsLoading ? (
              <div className="mt-3 overflow-auto">
                <table className="min-w-full border-collapse text-sm">
                  <thead>
                    <tr className="border-b">
                      <th className="p-2 text-left">Type</th>
                      <th className="p-2 text-left">Original</th>
                      <th className="p-2 text-left">Correction</th>
                      <th className="p-2 text-left">Link Status</th>
                      <th className="p-2 text-left">Created</th>
                      <th className="p-2 text-left">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {corrections.map((item) => {
                      const correctionRunId = item.correction_run_id;
                      const canImportIntoShell =
                        canImportRuns &&
                        item.correction_run_status === "DRAFT" &&
                        ["OFF_CYCLE", "RETRO"].includes(String(item.correction_type || "").toUpperCase());
                      return (
                        <tr key={item.id} className="border-b">
                          <td className="p-2">{item.correction_type}</td>
                          <td className="p-2">
                            {item.original_run_id ? (
                              <>
                                <Link className="underline" to={`/app/payroll-runs/${item.original_run_id}`}>
                                  {item.original_run_no || `#${item.original_run_id}`}
                                </Link>
                                <div className="text-xs text-slate-500">
                                  {item.original_run_status || "-"}
                                </div>
                              </>
                            ) : (
                              "-"
                            )}
                          </td>
                          <td className="p-2">
                            <Link className="underline" to={`/app/payroll-runs/${correctionRunId}`}>
                              {item.correction_run_no || `#${correctionRunId}`}
                            </Link>
                            <div className="text-xs text-slate-500">
                              {item.correction_run_status || "-"}
                            </div>
                          </td>
                          <td className="p-2">{item.status}</td>
                          <td className="p-2">{formatDateTime(item.created_at)}</td>
                          <td className="p-2">
                            {canImportIntoShell ? (
                              <Link
                                className="underline"
                                to={`/app/payroll-runs/import?targetRunId=${correctionRunId}&legalEntityId=${row.legal_entity_id}`}
                              >
                                Import CSV into Shell
                              </Link>
                            ) : (
                              "-"
                            )}
                          </td>
                        </tr>
                      );
                    })}
                    {corrections.length === 0 ? (
                      <tr>
                        <td className="p-3 text-slate-500" colSpan={6}>
                          Correction / reversal kaydi yok.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            ) : null}
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <h2 className="text-sm font-semibold text-slate-900">Tahakkuk Preview (PR-P02)</h2>
                <p className="mt-1 text-xs text-slate-600">
                  Payroll component mapping kontrolu + finalize edilecek GL tahakkuk satirlari.
                </p>
              </div>
              <div className="flex items-center gap-2">
                {preview ? (
                  <span className="rounded border px-2 py-0.5 text-xs">
                    {preview.is_balanced ? "Balanced" : "Not Balanced"}
                  </span>
                ) : null}
                <button
                  type="button"
                  className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700"
                  onClick={loadRow}
                  disabled={loading || previewLoading || actionBusy}
                >
                  Preview Yenile
                </button>
              </div>
            </div>

            {previewError ? (
              <div className="mt-3 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
                {previewError}
              </div>
            ) : null}

            {previewLoading && !preview ? (
              <div className="mt-3 text-sm text-slate-500">Preview yukleniyor...</div>
            ) : null}

            {!previewLoading && !preview ? (
              <div className="mt-3 text-sm text-slate-500">Preview yok.</div>
            ) : null}

            {preview ? (
              <div className="mt-4 space-y-4">
                <div className="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-4">
                  <div>
                    <div className="text-xs text-slate-500">Debit Total</div>
                    <div className="font-medium">{formatAmount(preview.debit_total)}</div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-500">Credit Total</div>
                    <div className="font-medium">{formatAmount(preview.credit_total)}</div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-500">Missing Mappings</div>
                    <div className="font-medium">{preview.missing_mappings?.length || 0}</div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-500">Accrual JE</div>
                    <div className="font-medium">
                      {preview?.run?.accrual_journal_entry_id || row?.accrual_journal_entry_id || "-"}
                    </div>
                  </div>
                </div>

                {(preview.missing_mappings || []).length > 0 ? (
                  <div className="rounded border border-rose-200 bg-rose-50 p-3">
                    <div className="text-sm font-medium text-rose-900">Eksik / Hatali Mappingler</div>
                    <ul className="mt-2 list-disc pl-5 text-sm text-rose-800">
                      {preview.missing_mappings.map((item, idx) => (
                        <li key={`${item.component_code}-${idx}`}>
                          {item.component_code} ({item.entry_side}) amount={formatAmount(item.amount)}
                          {item.issue ? ` - ${item.issue}` : ""}
                        </li>
                      ))}
                    </ul>
                    <div className="mt-2 text-xs text-rose-800">
                      Once <Link className="underline" to="/app/payroll-mappings">Bordro Mappingleri</Link>{" "}
                      sayfasinda gerekli eslemeleri tamamlayin.
                    </div>
                  </div>
                ) : null}

                <div className="overflow-auto">
                  <table className="min-w-full border-collapse text-sm">
                    <thead>
                      <tr className="border-b">
                        <th className="p-2 text-left">Component</th>
                        <th className="p-2 text-left">Side</th>
                        <th className="p-2 text-left">GL</th>
                        <th className="p-2 text-left">Amount</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(preview.posting_lines || []).map((line, idx) => (
                        <tr key={`${line.component_code}-${idx}`} className="border-b">
                          <td className="p-2">{line.component_code}</td>
                          <td className="p-2">{line.entry_side}</td>
                          <td className="p-2">
                            {line.gl_account_code || line.gl_account_id}
                            {line.gl_account_name ? (
                              <div className="text-xs text-slate-500">{line.gl_account_name}</div>
                            ) : null}
                          </td>
                          <td className="p-2">{formatAmount(line.amount)}</td>
                        </tr>
                      ))}
                      {(preview.posting_lines || []).length === 0 ? (
                        <tr>
                          <td className="p-3 text-slate-500" colSpan={4}>
                            Tahakkuk satiri yok.
                          </td>
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                </div>

                {row?.status !== "FINALIZED" ? (
                  <div className="flex flex-wrap gap-2">
                    {canReview ? (
                      <button
                        type="button"
                        className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700 disabled:opacity-60"
                        onClick={handleReview}
                        disabled={actionBusy || loading}
                      >
                        {actionBusy ? "Isleniyor..." : "Mark Reviewed"}
                      </button>
                    ) : null}
                    {canFinalize ? (
                      <button
                        type="button"
                        className="rounded bg-slate-900 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-60"
                        onClick={handleFinalize}
                        disabled={actionBusy || loading || !preview.can_finalize}
                        title={
                          preview.can_finalize
                            ? "Tahakkuk jurnalini olustur ve run'i finalize et"
                            : "Preview dengeli ve mappingler tam olmadan finalize edilemez"
                        }
                      >
                        {actionBusy ? "Isleniyor..." : "Finalize + Post Accrual"}
                      </button>
                    ) : null}
                    {!canReview && !canFinalize ? (
                      <div className="text-xs text-slate-500">
                        Review/finalize icin yetki gerekli.
                      </div>
                    ) : null}
                  </div>
                ) : (
                  <div className="space-y-2">
                    <div className="rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-900">
                      Run FINALIZED. Tahakkuk jurnal ID: {row?.accrual_journal_entry_id || "-"}
                    </div>
                    {canReadLiabilities ? (
                      <div className="text-xs text-slate-600">
                        Sonraki adim (PR-P03):{" "}
                        <Link className="underline" to={`/app/payroll-runs/${runId}/liabilities`}>
                          payroll liabilities ve payment batch hazirlama
                        </Link>
                      </div>
                    ) : null}
                  </div>
                )}
              </div>
            ) : null}
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <h2 className="text-sm font-semibold text-slate-900">Employee Lines</h2>
            <div className="mt-3 overflow-auto">
              <table className="min-w-full border-collapse text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="p-2 text-left">#</th>
                    <th className="p-2 text-left">Employee</th>
                    <th className="p-2 text-left">Cost Center</th>
                    <th className="p-2 text-left">Gross</th>
                    <th className="p-2 text-left">Net</th>
                    <th className="p-2 text-left">Emp Tax</th>
                    <th className="p-2 text-left">Emp SS</th>
                    <th className="p-2 text-left">Employer Tax</th>
                    <th className="p-2 text-left">Employer SS</th>
                  </tr>
                </thead>
                <tbody>
                  {(row.lines || []).map((line) => (
                    <tr key={line.id} className="border-b">
                      <td className="p-2">{line.line_no}</td>
                      <td className="p-2">
                        {line.employee_code} - {line.employee_name}
                      </td>
                      <td className="p-2">{line.cost_center_code || "-"}</td>
                      <td className="p-2">{formatAmount(line.gross_pay)}</td>
                      <td className="p-2">{formatAmount(line.net_pay)}</td>
                      <td className="p-2">{formatAmount(line.employee_tax)}</td>
                      <td className="p-2">{formatAmount(line.employee_social_security)}</td>
                      <td className="p-2">{formatAmount(line.employer_tax)}</td>
                      <td className="p-2">{formatAmount(line.employer_social_security)}</td>
                    </tr>
                  ))}
                  {(row.lines || []).length === 0 ? (
                    <tr>
                      <td className="p-3 text-slate-500" colSpan={9}>
                        Satir yok.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <h2 className="text-sm font-semibold text-slate-900">Audit</h2>
            <div className="mt-3 space-y-2 text-sm">
              {(row.audit || []).map((audit) => (
                <div key={audit.id} className="rounded border p-2">
                  <div className="flex items-center gap-2">
                    <span className="font-medium">{audit.action}</span>
                    <span className="text-xs text-slate-500">#{audit.acted_by_user_id || "-"}</span>
                    <span className="ml-auto text-xs text-slate-500">
                      {formatDateTime(audit.acted_at)}
                    </span>
                  </div>
                  {audit.payload_json ? (
                    <pre className="mt-2 whitespace-pre-wrap rounded bg-slate-50 p-2 text-xs">
                      {typeof audit.payload_json === "string"
                        ? audit.payload_json
                        : JSON.stringify(audit.payload_json, null, 2)}
                    </pre>
                  ) : null}
                </div>
              ))}
              {(row.audit || []).length === 0 ? (
                <div className="text-slate-500">Audit kaydi yok.</div>
              ) : null}
            </div>
          </section>
        </>
      ) : null}
    </div>
  );
}
