import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  approvePaymentBatch,
  cancelPaymentBatch,
  exportPaymentBatch,
  getPaymentBatch,
  postPaymentBatch,
} from "../../api/payments.js";
import {
  exportPaymentBatchFile,
  listPaymentBatchAckImports,
  importPaymentBatchAck,
} from "../../api/bankPaymentFiles.js";
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

function canApproveStatus(status) {
  return status === "DRAFT";
}

function canExportStatus(status) {
  return status === "APPROVED" || status === "EXPORTED";
}

function canPostStatus(status) {
  return status === "APPROVED" || status === "EXPORTED" || status === "POSTED";
}

function canCancelStatus(status) {
  return status === "DRAFT" || status === "APPROVED" || status === "EXPORTED";
}

export default function PaymentBatchDetailPage() {
  const { batchId } = useParams();
  const { hasPermission } = useAuth();
  const canRead = hasPermission("payments.batch.read");
  const canApprove = hasPermission("payments.batch.approve");
  const canExport = hasPermission("payments.batch.export");
  const canPost = hasPermission("payments.batch.post");
  const canCancel = hasPermission("payments.batch.cancel");
  const canBankExportB06 = hasPermission("bank.payments.export.create");
  const canBankAckImport = hasPermission("bank.payments.ack.import");
  const canBankAckRead = hasPermission("bank.payments.ack.read");

  const [row, setRow] = useState(null);
  const [loading, setLoading] = useState(false);
  const [busyAction, setBusyAction] = useState("");
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [exportPreview, setExportPreview] = useState("");
  const [ackText, setAckText] = useState("");
  const [ackRequestId, setAckRequestId] = useState("");
  const [ackFileName, setAckFileName] = useState("ack.csv");
  const [ackImports, setAckImports] = useState([]);

  const latestExport = useMemo(() => row?.exports?.[0] || null, [row]);

  async function loadAckImports(targetBatchId = batchId) {
    if (!canBankAckRead) {
      setAckImports([]);
      return;
    }
    try {
      const res = await listPaymentBatchAckImports(targetBatchId);
      setAckImports(Array.isArray(res?.items) ? res.items : []);
    } catch {
      setAckImports([]);
    }
  }

  async function loadRow() {
    if (!canRead) {
      setRow(null);
      return;
    }
    setLoading(true);
    setError("");
    try {
      const res = await getPaymentBatch(batchId);
      setRow(res?.row || null);
      await loadAckImports(batchId);
    } catch (err) {
      setRow(null);
      setAckImports([]);
      setError(err?.response?.data?.message || "Odeme batch detayi yuklenemedi");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadRow();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [batchId, canRead]);

  async function runAction(actionKey, fn) {
    if (!row) {
      return;
    }
    setBusyAction(actionKey);
    setError("");
    setMessage("");
    try {
      const res = await fn();
      if (res?.row) {
        setRow(res.row);
        await loadAckImports(res?.row?.id || batchId);
      } else {
        await loadRow();
      }
      return res;
    } catch (err) {
      setError(err?.response?.data?.message || err?.message || "Islem basarisiz");
      return null;
    } finally {
      setBusyAction("");
    }
  }

  async function handleApprove() {
    if (!row) {
      return;
    }
    const note = window.prompt("Onay notu (opsiyonel)", "") || "";
    const res = await runAction("approve", () => approvePaymentBatch(row.id, { note }));
    if (res) {
      setMessage("Batch onaylandi");
    }
  }

  async function handleExport() {
    if (!row) {
      return;
    }
    const exportRequestId = window.prompt("B06 export request id (opsiyonel)", "") || "";
    const markSent =
      (window.prompt("Bankaya gonderildi mi? (true/false)", "false") || "false")
        .trim()
        .toLowerCase() === "true";
    const res = await runAction("export", () =>
      canBankExportB06
        ? exportPaymentBatchFile(row.id, {
            fileFormatCode: "GENERIC_CSV_V1",
            exportRequestId: exportRequestId || undefined,
            markSent,
          })
        : exportPaymentBatch(row.id, { format: "CSV" })
    );
    if (res) {
      if (res?.approval_required) {
        const approvalId = res?.approval_request?.id;
        setExportPreview("");
        setMessage(
          approvalId
            ? `B09 onay talebi olusturuldu (#${approvalId}). Export onaydan sonra calisacak.`
            : "B09 onay talebi olusturuldu. Export onaydan sonra calisacak."
        );
      } else {
        setExportPreview(String(res?.export?.csv || ""));
        setMessage(canBankExportB06 ? "B06 banka export olusturuldu" : "CSV export olusturuldu");
      }
    }
  }

  async function handleImportAck() {
    if (!row) {
      return;
    }
    if (!ackText.trim()) {
      setError("Ack CSV metni gerekli");
      return;
    }
    const res = await runAction("ack-import", () =>
      importPaymentBatchAck(row.id, {
        fileFormatCode: "GENERIC_CSV_V1",
        ackRequestId: ackRequestId || undefined,
        fileName: ackFileName || undefined,
        ackText,
      })
    );
    if (res) {
      setMessage("Banka ack dosyasi ice aktarildi");
    }
  }

  async function handlePost() {
    if (!row) {
      return;
    }
    const prefix = window.prompt("External payment ref prefix (opsiyonel)", "") || "";
    const note = window.prompt("Posting notu (opsiyonel)", "") || "";
    const res = await runAction("post", () =>
      postPaymentBatch(row.id, {
        externalPaymentRefPrefix: prefix || undefined,
        note: note || undefined,
      })
    );
    if (res) {
      setMessage("Batch post edildi");
    }
  }

  async function handleCancel() {
    if (!row) {
      return;
    }
    const reason = window.prompt("Iptal nedeni (opsiyonel)", "") || "";
    const res = await runAction("cancel", () => cancelPaymentBatch(row.id, { reason }));
    if (res) {
      setMessage("Batch iptal edildi");
    }
  }

  if (!canRead) {
    return (
      <div className="p-4">
        <div className="rounded border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900">
          Missing permission: <code>payments.batch.read</code>
        </div>
      </div>
    );
  }

  if (loading && !row) {
    return <div className="p-4">Yukleniyor...</div>;
  }

  return (
    <div className="p-4 space-y-4">
      <div className="rounded border bg-white p-4">
        <div className="flex flex-wrap items-center gap-2">
          <Link className="text-sm underline" to="/app/odeme-batchleri">
            ← Listeye don
          </Link>
          <h1 className="text-lg font-semibold">
            {row?.batch_no || `Batch #${batchId}`}
          </h1>
          {row?.status ? (
            <span className="rounded border px-2 py-0.5 text-xs">{row.status}</span>
          ) : null}
          <button
            type="button"
            className="ml-auto rounded border px-2 py-1 text-sm"
            onClick={loadRow}
            disabled={loading}
          >
            Yenile
          </button>
        </div>

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

        {row ? (
          <>
            <div className="mt-3 grid gap-2 text-sm md:grid-cols-2 lg:grid-cols-4">
              <div>
                <div className="text-slate-500">Kaynak</div>
                <div className="font-medium">
                  {row.source_type}
                  {row.source_id ? ` #${row.source_id}` : ""}
                </div>
              </div>
              <div>
                <div className="text-slate-500">Banka</div>
                <div className="font-medium">{row.bank_account_code}</div>
                <div className="text-xs text-slate-500">{row.bank_account_name}</div>
              </div>
              <div>
                <div className="text-slate-500">Para Birimi</div>
                <div className="font-medium">{row.currency_code}</div>
              </div>
              <div>
                <div className="text-slate-500">Toplam</div>
                <div className="font-medium">{formatAmount(row.total_amount)}</div>
              </div>
              <div>
                <div className="text-slate-500">Olusturan</div>
                <div className="font-medium">#{row.created_by_user_id || "-"}</div>
              </div>
              <div>
                <div className="text-slate-500">Olusma</div>
                <div className="font-medium">{formatDateTime(row.created_at)}</div>
              </div>
              <div>
                <div className="text-slate-500">Son Export</div>
                <div className="font-medium">{row.last_export_file_name || "-"}</div>
              </div>
              <div>
                <div className="text-slate-500">Bank Export Durumu</div>
                <div className="font-medium">{row.bank_export_status || "-"}</div>
              </div>
              <div>
                <div className="text-slate-500">B09 Onay Durumu</div>
                <div className="font-medium">{row.governance_approval_status || "-"}</div>
                <div className="text-xs text-slate-500">
                  Req #{row.governance_approval_request_id || "-"}
                </div>
              </div>
              <div>
                <div className="text-slate-500">Bank Ack Durumu</div>
                <div className="font-medium">{row.bank_ack_status || "-"}</div>
                <div className="text-xs text-slate-500">{formatDateTime(row.last_ack_imported_at)}</div>
              </div>
              <div>
                <div className="text-slate-500">Posted Journal</div>
                <div className="font-medium">{row.posted_journal_entry_id || "-"}</div>
              </div>
              <div>
                <div className="text-slate-500">B09 Onaylayan</div>
                <div className="font-medium">#{row.governance_approved_by_user_id || "-"}</div>
                <div className="text-xs text-slate-500">{formatDateTime(row.governance_approved_at)}</div>
              </div>
            </div>

            {row.notes ? (
              <div className="mt-3 rounded border bg-slate-50 p-2 text-sm">
                <div className="text-xs text-slate-500">Not</div>
                <div>{row.notes}</div>
              </div>
            ) : null}

            <div className="mt-3 flex flex-wrap gap-2">
              <button
                type="button"
                className="rounded border px-3 py-1 text-sm"
                onClick={handleApprove}
                disabled={
                  busyAction !== "" || !canApprove || !canApproveStatus(String(row.status || ""))
                }
              >
                {busyAction === "approve" ? "Onaylaniyor..." : "Onayla"}
              </button>
              <button
                type="button"
                className="rounded border px-3 py-1 text-sm"
                onClick={handleExport}
                disabled={
                  busyAction !== "" ||
                  !(canExport || canBankExportB06) ||
                  !canExportStatus(String(row.status || ""))
                }
              >
                {busyAction === "export"
                  ? "Export..."
                  : canBankExportB06
                    ? "Bank Export (B06)"
                    : "CSV Export"}
              </button>
              <button
                type="button"
                className="rounded border px-3 py-1 text-sm"
                onClick={handlePost}
                disabled={busyAction !== "" || !canPost || !canPostStatus(String(row.status || ""))}
              >
                {busyAction === "post" ? "Posting..." : "Post"}
              </button>
              <button
                type="button"
                className="rounded border px-3 py-1 text-sm"
                onClick={handleCancel}
                disabled={busyAction !== "" || !canCancel || !canCancelStatus(String(row.status || ""))}
              >
                {busyAction === "cancel" ? "Iptal..." : "Iptal"}
              </button>
            </div>

            <div className="mt-2 flex flex-wrap gap-3 text-xs text-slate-500">
              {!canApprove ? <span>approve yetkisi yok</span> : null}
              {!canExport ? <span>export yetkisi yok</span> : null}
              {!canBankExportB06 ? <span>B06 bank export yetkisi yok</span> : null}
              {!canBankAckImport ? <span>B06 ack import yetkisi yok</span> : null}
              {!canPost ? <span>post yetkisi yok</span> : null}
              {!canCancel ? <span>cancel yetkisi yok</span> : null}
            </div>
          </>
        ) : null}
      </div>

      <div className="rounded border bg-white p-4">
        <h2 className="mb-2 font-medium">Satirlar</h2>
        <div className="overflow-auto">
          <table className="min-w-full border-collapse text-sm">
            <thead>
              <tr className="border-b">
                <th className="p-2 text-left">#</th>
                <th className="p-2 text-left">Lehdar</th>
                <th className="p-2 text-left">Payable</th>
                <th className="p-2 text-left">GL</th>
                <th className="p-2 text-left">Tutar</th>
                <th className="p-2 text-left">Exec Tutar</th>
                <th className="p-2 text-left">Durum</th>
                <th className="p-2 text-left">Bank Exec</th>
                <th className="p-2 text-left">Ack</th>
                <th className="p-2 text-left">External Ref</th>
                <th className="p-2 text-left">Settlement Ref</th>
              </tr>
            </thead>
            <tbody>
              {(row?.lines || []).map((line) => (
                <tr key={line.id} className="border-b">
                  <td className="p-2">{line.line_no}</td>
                  <td className="p-2">
                    <div>{line.beneficiary_name}</div>
                    <div className="text-xs text-slate-500">{line.beneficiary_bank_ref || "-"}</div>
                  </td>
                  <td className="p-2">
                    <div>{line.payable_entity_type}</div>
                    <div className="text-xs text-slate-500">
                      {line.payable_entity_id ? `#${line.payable_entity_id}` : "-"}{" "}
                      {line.payable_ref ? `(${line.payable_ref})` : ""}
                    </div>
                  </td>
                  <td className="p-2">
                    {line.payable_gl_account_code || line.payable_gl_account_id}
                    <div className="text-xs text-slate-500">{line.payable_gl_account_name || ""}</div>
                  </td>
                  <td className="p-2">{formatAmount(line.amount)}</td>
                  <td className="p-2">{formatAmount(line.executed_amount)}</td>
                  <td className="p-2">{line.status}</td>
                  <td className="p-2">
                    <div>{line.bank_execution_status || "-"}</div>
                    <div className="text-xs text-slate-500">{formatDateTime(line.acknowledged_at)}</div>
                  </td>
                  <td className="p-2">
                    <div>{line.ack_status || "-"}</div>
                    <div className="text-xs text-slate-500">{line.ack_code || "-"}</div>
                  </td>
                  <td className="p-2">{line.external_payment_ref || "-"}</td>
                  <td className="p-2">{line.settlement_journal_line_ref || "-"}</td>
                </tr>
              ))}
              {(row?.lines || []).length === 0 ? (
                <tr>
                  <td className="p-3 text-slate-500" colSpan={11}>
                    Satir yok.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <div className="rounded border bg-white p-4">
          <h2 className="mb-2 font-medium">Export Geçmisi</h2>
          <div className="space-y-2 text-sm">
            {(row?.exports || []).map((exp) => (
              <div key={exp.id} className="rounded border p-2">
                <div className="flex items-center gap-2">
                  <span className="font-medium">{exp.file_name}</span>
                  <span className="rounded border px-1 text-xs">
                    {exp.bank_file_format_code || exp.export_format}
                  </span>
                  {exp.export_status ? (
                    <span className="rounded border px-1 text-xs">{exp.export_status}</span>
                  ) : null}
                  <span className="ml-auto text-xs text-slate-500">{formatDateTime(exp.exported_at)}</span>
                </div>
                {exp.export_request_id ? (
                  <div className="mt-1 text-xs text-slate-600">ReqId: {exp.export_request_id}</div>
                ) : null}
                <div className="mt-1 text-xs text-slate-600 break-all">{exp.file_checksum}</div>
              </div>
            ))}
            {(row?.exports || []).length === 0 ? (
              <div className="text-slate-500">Export kaydi yok.</div>
            ) : null}
          </div>
        </div>

        <div className="rounded border bg-white p-4">
          <h2 className="mb-2 font-medium">Denetim Izleri</h2>
          <div className="max-h-[320px] space-y-2 overflow-auto text-sm">
            {(row?.audit || []).map((audit) => (
              <div key={audit.id} className="rounded border p-2">
                <div className="flex items-center gap-2">
                  <span className="font-medium">{audit.action}</span>
                  <span className="text-xs text-slate-500">#{audit.acted_by_user_id || "-"}</span>
                  <span className="ml-auto text-xs text-slate-500">{formatDateTime(audit.acted_at)}</span>
                </div>
                {audit.payload_json ? (
                  <pre className="mt-1 whitespace-pre-wrap break-all rounded bg-slate-50 p-2 text-xs">
                    {typeof audit.payload_json === "string"
                      ? audit.payload_json
                      : JSON.stringify(audit.payload_json, null, 2)}
                  </pre>
                ) : null}
              </div>
            ))}
            {(row?.audit || []).length === 0 ? (
              <div className="text-slate-500">Audit kaydi yok.</div>
            ) : null}
          </div>
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <div className="rounded border bg-white p-4">
          <h2 className="mb-2 font-medium">Banka Ack Import (B06)</h2>
          {canBankAckImport ? (
            <div className="space-y-2 text-sm">
              <div className="grid gap-2 md:grid-cols-2">
                <label className="block">
                  <div className="mb-1 text-xs text-slate-500">Ack Request Id (opsiyonel)</div>
                  <input
                    className="w-full rounded border px-2 py-1"
                    value={ackRequestId}
                    onChange={(e) => setAckRequestId(e.target.value)}
                    placeholder="ACK-PB-..."
                  />
                </label>
                <label className="block">
                  <div className="mb-1 text-xs text-slate-500">Dosya Adi</div>
                  <input
                    className="w-full rounded border px-2 py-1"
                    value={ackFileName}
                    onChange={(e) => setAckFileName(e.target.value)}
                    placeholder="ack.csv"
                  />
                </label>
              </div>
              <label className="block">
                <div className="mb-1 text-xs text-slate-500">
                  Ack CSV (line_ref veya batch_no+line_no + ack_status)
                </div>
                <textarea
                  className="min-h-[160px] w-full rounded border p-2 font-mono text-xs"
                  value={ackText}
                  onChange={(e) => setAckText(e.target.value)}
                  placeholder={"line_ref,ack_status,ack_amount,bank_reference,ack_code,ack_message,executed_at\nPB1-L1,PAID,100.00,BR-123,,,2026-02-26T10:00:00Z"}
                />
              </label>
              <button
                type="button"
                className="rounded border px-3 py-1 text-sm"
                onClick={handleImportAck}
                disabled={busyAction !== "" || !ackText.trim()}
              >
                {busyAction === "ack-import" ? "Ack import..." : "Ack Import"}
              </button>
            </div>
          ) : (
            <div className="text-sm text-slate-500">
              Missing permission: <code>bank.payments.ack.import</code>
            </div>
          )}
        </div>

        <div className="rounded border bg-white p-4">
          <h2 className="mb-2 font-medium">Ack Gecmisi</h2>
          {canBankAckRead ? (
            <div className="max-h-[320px] space-y-2 overflow-auto text-sm">
              {ackImports.map((ack) => (
                <div key={ack.id} className="rounded border p-2">
                  <div className="flex items-center gap-2">
                    <span className="font-medium">#{ack.id}</span>
                    <span className="rounded border px-1 text-xs">{ack.file_format_code}</span>
                    <span className="rounded border px-1 text-xs">{ack.status}</span>
                    <span className="ml-auto text-xs text-slate-500">
                      {formatDateTime(ack.created_at)}
                    </span>
                  </div>
                  <div className="mt-1 text-xs text-slate-600">
                    rows={ack.total_rows} applied={ack.applied_rows} dup={ack.duplicate_rows} err=
                    {ack.error_rows}
                  </div>
                  {ack.ack_request_id ? (
                    <div className="mt-1 text-xs text-slate-600">ReqId: {ack.ack_request_id}</div>
                  ) : null}
                </div>
              ))}
              {ackImports.length === 0 ? (
                <div className="text-slate-500">Ack kaydi yok.</div>
              ) : null}
            </div>
          ) : (
            <div className="text-sm text-slate-500">
              Missing permission: <code>bank.payments.ack.read</code>
            </div>
          )}
        </div>
      </div>

      {exportPreview || latestExport?.export_payload_text ? (
        <div className="rounded border bg-white p-4">
          <h2 className="mb-2 font-medium">CSV Preview</h2>
          <pre className="max-h-[420px] overflow-auto whitespace-pre-wrap rounded bg-slate-50 p-2 text-xs">
            {exportPreview || latestExport?.export_payload_text || ""}
          </pre>
        </div>
      ) : null}
    </div>
  );
}
