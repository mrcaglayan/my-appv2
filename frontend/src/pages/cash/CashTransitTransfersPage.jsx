import { useEffect, useMemo, useState } from "react";
import {
  cancelCashTransitTransfer,
  listCashRegisters,
  listCashSessions,
  listCashTransitTransfers,
  receiveCashTransitTransfer,
} from "../../api/cashAdmin.js";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";
import CashControlModeBanner from "./CashControlModeBanner.jsx";

const TRANSIT_STATUSES = ["INITIATED", "IN_TRANSIT", "RECEIVED", "CANCELED", "REVERSED"];

const INITIAL_FILTERS = {
  legalEntityId: "",
  sourceRegisterId: "",
  targetRegisterId: "",
  status: "",
  initiatedDateFrom: "",
  initiatedDateTo: "",
};

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function toUpper(value) {
  return String(value || "").trim().toUpperCase();
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function toDateTimeLocalInput(date = new Date()) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hour = String(date.getHours()).padStart(2, "0");
  const minute = String(date.getMinutes()).padStart(2, "0");
  return `${year}-${month}-${day}T${hour}:${minute}`;
}

function buildTransitReceiveIdempotencyKey() {
  if (globalThis.crypto && typeof globalThis.crypto.randomUUID === "function") {
    return `cash-transit-receive-${globalThis.crypto.randomUUID()}`;
  }
  return `cash-transit-receive-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function formatAmount(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) {
    return "-";
  }
  return amount.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 6,
  });
}

function formatDateTime(value) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return `${date.toLocaleDateString()} ${date.toLocaleTimeString()}`;
}

function normalizeErrorMessage(value) {
  return String(value || "").trim();
}

function extractRequestId(err) {
  return (
    err?.response?.data?.requestId ||
    err?.response?.headers?.["x-request-id"] ||
    null
  );
}

function mapTransitErrorMessage(rawMessage, l) {
  const message = normalizeErrorMessage(rawMessage);
  if (!message) {
    return "";
  }
  const lower = message.toLowerCase();

  if (lower.includes("sourceregisterid not found for tenant")) {
    return l("Source register not found.", "Kaynak kasa bulunamadi.");
  }
  if (lower.includes("targetregisterid not found for tenant")) {
    return l("Target register not found.", "Hedef kasa bulunamadi.");
  }
  if (lower.includes("must be in_transit before receive")) {
    return l(
      "Transit transfer must be IN_TRANSIT before receive.",
      "Transit transfer teslim alinmadan once IN_TRANSIT olmali."
    );
  }
  if (lower.includes("must be posted before receive")) {
    return l(
      "Transfer-out must be POSTED before receive.",
      "Teslim almadan once transfer-out kaydi POSTED olmali."
    );
  }
  if (lower.includes("already received or not in transit")) {
    return l("Transit transfer is already received.", "Transit transfer zaten teslim alinmis.");
  }
  if (lower.includes("cashsessionid must be open")) {
    return l("Selected cash session must be OPEN.", "Secilen kasa oturumu OPEN olmali.");
  }
  if (lower.includes("cash transit transfer not found")) {
    return l("Transit transfer not found.", "Transit transfer bulunamadi.");
  }
  if (lower.includes("cancelreason is required")) {
    return l("Cancel reason is required.", "Iptal nedeni zorunludur.");
  }
  if (lower.includes("only initiated transfer can be cancelled")) {
    return l(
      "Only INITIATED transit transfers can be cancelled.",
      "Sadece INITIATED durumundaki transferler iptal edilebilir."
    );
  }

  return "";
}

function statusClassName(status) {
  const normalized = toUpper(status);
  if (normalized === "RECEIVED") {
    return "bg-emerald-100 text-emerald-700";
  }
  if (normalized === "IN_TRANSIT") {
    return "bg-amber-100 text-amber-800";
  }
  if (normalized === "INITIATED") {
    return "bg-sky-100 text-sky-700";
  }
  if (normalized === "CANCELED") {
    return "bg-rose-100 text-rose-700";
  }
  if (normalized === "REVERSED") {
    return "bg-violet-100 text-violet-700";
  }
  return "bg-slate-200 text-slate-700";
}

function toRegisterLabel(row) {
  return `${row?.code || row?.id || "-"} - ${row?.name || "-"}`;
}

function toLegalEntityLabel(row) {
  return `${row?.code || row?.id || "-"} - ${row?.name || "-"}`;
}

export default function CashTransitTransfersPage() {
  const { language } = useI18n();
  const { hasPermission } = useAuth();
  const l = (en, tr) => (language === "tr" ? tr : en);

  const canRead = hasPermission("cash.txn.read");
  const canCreate = hasPermission("cash.txn.create");
  const canCancel = hasPermission("cash.txn.cancel");

  const [filters, setFilters] = useState(INITIAL_FILTERS);
  const [loading, setLoading] = useState(false);
  const [actionSaving, setActionSaving] = useState(false);
  const [error, setError] = useState("");
  const [errorRequestId, setErrorRequestId] = useState(null);
  const [warning, setWarning] = useState("");
  const [message, setMessage] = useState("");

  const [rows, setRows] = useState([]);
  const [registerRows, setRegisterRows] = useState([]);
  const [openSessions, setOpenSessions] = useState([]);
  const [actionForm, setActionForm] = useState(null);

  const selectedLegalEntityId = toPositiveInt(filters.legalEntityId);

  const legalEntityOptions = useMemo(() => {
    const map = new Map();
    for (const row of registerRows) {
      const legalEntityId = toPositiveInt(row?.legal_entity_id);
      if (!legalEntityId || map.has(legalEntityId)) {
        continue;
      }
      map.set(legalEntityId, {
        id: legalEntityId,
        code: row?.legal_entity_code || String(legalEntityId),
        name: row?.legal_entity_name || "-",
      });
    }
    return [...map.values()].sort((a, b) =>
      String(a.code || "").localeCompare(String(b.code || ""))
    );
  }, [registerRows]);

  const sourceRegisterOptions = useMemo(() => {
    return registerRows
      .filter((row) => {
        if (!selectedLegalEntityId) {
          return true;
        }
        return toPositiveInt(row?.legal_entity_id) === selectedLegalEntityId;
      })
      .sort((a, b) => String(a?.code || "").localeCompare(String(b?.code || "")));
  }, [registerRows, selectedLegalEntityId]);

  const targetRegisterOptions = sourceRegisterOptions;

  const selectedActionRow = useMemo(() => {
    const transferId = toPositiveInt(actionForm?.transferId);
    if (!transferId) {
      return null;
    }
    return rows.find((row) => toPositiveInt(row?.id) === transferId) || null;
  }, [actionForm?.transferId, rows]);

  const selectedTargetOpenSessions = useMemo(() => {
    if (actionForm?.type !== "receive") {
      return [];
    }
    const targetRegisterId = toPositiveInt(selectedActionRow?.target_cash_register_id);
    if (!targetRegisterId) {
      return [];
    }
    return openSessions.filter(
      (row) => toPositiveInt(row?.cash_register_id) === targetRegisterId
    );
  }, [actionForm?.type, openSessions, selectedActionRow]);

  const transferRows = useMemo(
    () => [...rows].sort((a, b) => Number(b?.id || 0) - Number(a?.id || 0)),
    [rows]
  );

  function resetMessages() {
    setError("");
    setErrorRequestId(null);
    setWarning("");
    setMessage("");
  }

  function toListQuery(nextFilters) {
    return {
      legalEntityId: toPositiveInt(nextFilters.legalEntityId) || undefined,
      sourceRegisterId: toPositiveInt(nextFilters.sourceRegisterId) || undefined,
      targetRegisterId: toPositiveInt(nextFilters.targetRegisterId) || undefined,
      status: nextFilters.status || undefined,
      initiatedDateFrom: nextFilters.initiatedDateFrom || undefined,
      initiatedDateTo: nextFilters.initiatedDateTo || undefined,
      limit: 200,
      offset: 0,
    };
  }

  async function loadData(nextFilters = filters) {
    if (!canRead) {
      setRows([]);
      setRegisterRows([]);
      setOpenSessions([]);
      return;
    }

    setLoading(true);
    setError("");
    setErrorRequestId(null);
    setWarning("");

    const [transitResult, registerResult, sessionResult] = await Promise.allSettled([
      listCashTransitTransfers(toListQuery(nextFilters)),
      listCashRegisters({ limit: 400, offset: 0 }),
      listCashSessions({ status: "OPEN", limit: 400, offset: 0 }),
    ]);

    try {
      if (transitResult.status !== "fulfilled") {
        throw transitResult.reason;
      }

      const warnings = [];
      if (registerResult.status === "fulfilled") {
        setRegisterRows(registerResult.value?.rows || []);
      } else {
        setRegisterRows([]);
        warnings.push(
          registerResult.reason?.response?.data?.message ||
            l(
              "Register lookup is unavailable.",
              "Kasa listesi su anda alinmiyor."
            )
        );
      }

      if (sessionResult.status === "fulfilled") {
        setOpenSessions(sessionResult.value?.rows || []);
      } else {
        setOpenSessions([]);
        warnings.push(
          sessionResult.reason?.response?.data?.message ||
            l(
              "Open session lookup is unavailable.",
              "Acik kasa oturum listesi su anda alinmiyor."
            )
        );
      }

      const nextRows = Array.isArray(transitResult.value?.rows)
        ? transitResult.value.rows
        : [];
      setRows(nextRows);
      if (warnings.length > 0) {
        setWarning(warnings.join(" "));
      }
    } catch (err) {
      const rawMessage = normalizeErrorMessage(
        err?.response?.data?.message || err?.message
      );
      const mappedMessage = mapTransitErrorMessage(rawMessage, l);
      setError(
        mappedMessage ||
          (rawMessage
            ? `${l("Transit transfers could not be loaded.", "Transit transfer kayitlari yuklenemedi.")} (${rawMessage})`
            : l("Transit transfers could not be loaded.", "Transit transfer kayitlari yuklenemedi."))
      );
      setErrorRequestId(extractRequestId(err));
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  function canReceiveRow(row) {
    if (!canCreate) {
      return false;
    }
    if (toUpper(row?.status) !== "IN_TRANSIT") {
      return false;
    }
    if (toPositiveInt(row?.transfer_in_cash_transaction_id)) {
      return false;
    }
    return true;
  }

  function canCancelRow(row) {
    if (!canCancel) {
      return false;
    }
    return toUpper(row?.status) === "INITIATED";
  }

  function openReceiveForm(row) {
    if (!canReceiveRow(row)) {
      return;
    }

    const targetRegisterId = toPositiveInt(row?.target_cash_register_id);
    const defaultSession = openSessions.find(
      (entry) => toPositiveInt(entry?.cash_register_id) === targetRegisterId
    );

    setActionForm({
      type: "receive",
      transferId: toPositiveInt(row?.id) || "",
      cashSessionId: toPositiveInt(defaultSession?.id) ? String(defaultSession.id) : "",
      txnDatetime: toDateTimeLocalInput(),
      bookDate: todayIsoDate(),
      referenceNo: "",
      description: "",
    });
    setError("");
    setErrorRequestId(null);
    setMessage("");
  }

  function openCancelForm(row) {
    if (!canCancelRow(row)) {
      return;
    }
    setActionForm({
      type: "cancel",
      transferId: toPositiveInt(row?.id) || "",
      cancelReason: "",
    });
    setError("");
    setErrorRequestId(null);
    setMessage("");
  }

  function closeActionForm() {
    setActionForm(null);
    setError("");
    setErrorRequestId(null);
  }

  async function handleActionSubmit(event) {
    event.preventDefault();
    if (!actionForm || actionSaving) {
      return;
    }

    const transferId = toPositiveInt(actionForm.transferId);
    if (!transferId) {
      setError(l("transferId is required.", "transferId zorunludur."));
      setErrorRequestId(null);
      return;
    }

    setActionSaving(true);
    setError("");
    setErrorRequestId(null);
    setMessage("");

    try {
      if (actionForm.type === "receive") {
        const response = await receiveCashTransitTransfer(transferId, {
          cashSessionId: toPositiveInt(actionForm.cashSessionId) || undefined,
          txnDatetime: actionForm.txnDatetime
            ? new Date(actionForm.txnDatetime).toISOString()
            : undefined,
          bookDate: actionForm.bookDate || undefined,
          referenceNo: actionForm.referenceNo || undefined,
          description: actionForm.description || undefined,
          idempotencyKey: buildTransitReceiveIdempotencyKey(),
        });
        const transferInTxnId =
          toPositiveInt(response?.transferInTransaction?.id) ||
          toPositiveInt(response?.transfer?.transfer_in_cash_transaction_id);

        if (response?.idempotentReplay) {
          setMessage(
            l(
              `Transit receive replayed. transferInTxnId=${transferInTxnId || "-"}`,
              `Transit teslim alma istegi tekrar oynatildi. transferInTxnId=${transferInTxnId || "-"}`
            )
          );
        } else {
          setMessage(
            l(
              `Transit received. transferInTxnId=${transferInTxnId || "-"}`,
              `Transit teslim alindi. transferInTxnId=${transferInTxnId || "-"}`
            )
          );
        }
      } else if (actionForm.type === "cancel") {
        if (!String(actionForm.cancelReason || "").trim()) {
          throw new Error(l("cancelReason is required.", "cancelReason zorunludur."));
        }
        await cancelCashTransitTransfer(transferId, {
          cancelReason: String(actionForm.cancelReason || "").trim(),
        });
        setMessage(
          l(
            `Transit transfer #${transferId} cancelled.`,
            `Transit transfer #${transferId} iptal edildi.`
          )
        );
      }

      setActionForm(null);
      await loadData(filters);
    } catch (err) {
      const rawMessage = normalizeErrorMessage(
        err?.response?.data?.message || err?.message
      );
      const mappedMessage = mapTransitErrorMessage(rawMessage, l);
      setError(
        mappedMessage ||
          (rawMessage
            ? `${l("Transit action failed.", "Transit islemi basarisiz oldu.")} (${rawMessage})`
            : l("Transit action failed.", "Transit islemi basarisiz oldu."))
      );
      setErrorRequestId(extractRequestId(err));
    } finally {
      setActionSaving(false);
    }
  }

  useEffect(() => {
    loadData(filters);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead]);

  function handleFilterSubmit(event) {
    event.preventDefault();
    resetMessages();
    loadData(filters);
  }

  function handleFilterReset() {
    const nextFilters = { ...INITIAL_FILTERS };
    setFilters(nextFilters);
    resetMessages();
    loadData(nextFilters);
  }

  return (
    <div className="space-y-6">
      <CashControlModeBanner />

      <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h1 className="text-xl font-semibold text-slate-900">
              {l("Cash Transit Transfers", "Kasa Transit Transferleri")}
            </h1>
            <p className="mt-1 text-sm text-slate-600">
              {l(
                "Monitor cross-OU transfer lifecycle and complete receive/cancel actions safely.",
                "OU'lar arasi transfer yasam dongusunu izleyin, teslim alma/iptal islemlerini guvenle tamamlayin."
              )}
            </p>
          </div>
          <div className="text-xs text-slate-500">
            {canRead
              ? l("Permission: cash.txn.read", "Yetki: cash.txn.read")
              : l("Read permission missing", "Okuma yetkisi eksik")}
          </div>
        </div>
      </section>

      {actionForm ? (
        <section className="rounded-2xl border border-indigo-200 bg-indigo-50 p-4 shadow-sm">
          <div className="mb-3 flex items-center justify-between gap-3">
            <h2 className="text-base font-semibold text-indigo-900">
              {actionForm.type === "receive"
                ? l(
                    `Transit Receive #${actionForm.transferId}`,
                    `Transit Teslim Alma #${actionForm.transferId}`
                  )
                : l(
                    `Transit Cancel #${actionForm.transferId}`,
                    `Transit Iptal #${actionForm.transferId}`
                  )}
            </h2>
            <button
              type="button"
              onClick={closeActionForm}
              className="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-600 hover:bg-white"
              disabled={actionSaving}
            >
              {l("Close", "Kapat")}
            </button>
          </div>

          <form className="space-y-4" onSubmit={handleActionSubmit}>
            {actionForm.type === "receive" ? (
              <div className="grid gap-3 md:grid-cols-2">
                <label className="flex flex-col gap-1 text-sm text-slate-700">
                  {l("Target register session (optional)", "Hedef kasa oturumu (opsiyonel)")}
                  <select
                    value={actionForm.cashSessionId || ""}
                    onChange={(event) =>
                      setActionForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              cashSessionId: event.target.value,
                            }
                          : prev
                      )
                    }
                    className="rounded-xl border border-slate-300 px-3 py-2"
                  >
                    <option value="">{l("No session", "Oturum yok")}</option>
                    {selectedTargetOpenSessions.map((session) => (
                      <option key={`transit-receive-session-${session.id}`} value={session.id}>
                        {l(
                          `Session #${session.id} (${session.cash_register_code || session.cash_register_id})`,
                          `Oturum #${session.id} (${session.cash_register_code || session.cash_register_id})`
                        )}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="flex flex-col gap-1 text-sm text-slate-700">
                  {l("Book date", "Kayit tarihi")}
                  <input
                    type="date"
                    value={actionForm.bookDate || ""}
                    onChange={(event) =>
                      setActionForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              bookDate: event.target.value,
                            }
                          : prev
                      )
                    }
                    className="rounded-xl border border-slate-300 px-3 py-2"
                  />
                </label>

                <label className="flex flex-col gap-1 text-sm text-slate-700">
                  {l("Transaction datetime", "Islem tarihi-saati")}
                  <input
                    type="datetime-local"
                    value={actionForm.txnDatetime || ""}
                    onChange={(event) =>
                      setActionForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              txnDatetime: event.target.value,
                            }
                          : prev
                      )
                    }
                    className="rounded-xl border border-slate-300 px-3 py-2"
                  />
                </label>

                <label className="flex flex-col gap-1 text-sm text-slate-700">
                  {l("Reference no", "Referans no")}
                  <input
                    value={actionForm.referenceNo || ""}
                    onChange={(event) =>
                      setActionForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              referenceNo: event.target.value,
                            }
                          : prev
                      )
                    }
                    className="rounded-xl border border-slate-300 px-3 py-2"
                    maxLength={100}
                    placeholder={l("Optional", "Opsiyonel")}
                  />
                </label>

                <label className="md:col-span-2 flex flex-col gap-1 text-sm text-slate-700">
                  {l("Description", "Aciklama")}
                  <input
                    value={actionForm.description || ""}
                    onChange={(event) =>
                      setActionForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              description: event.target.value,
                            }
                          : prev
                      )
                    }
                    className="rounded-xl border border-slate-300 px-3 py-2"
                    maxLength={500}
                    placeholder={l("Optional", "Opsiyonel")}
                  />
                </label>
              </div>
            ) : (
              <label className="flex flex-col gap-1 text-sm text-slate-700">
                {l("Cancel reason", "Iptal nedeni")}
                <textarea
                  rows={3}
                  value={actionForm.cancelReason || ""}
                  onChange={(event) =>
                    setActionForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            cancelReason: event.target.value,
                          }
                        : prev
                    )
                  }
                  className="rounded-xl border border-slate-300 px-3 py-2"
                  maxLength={255}
                  placeholder={l("Required", "Zorunlu")}
                />
              </label>
            )}

            <div className="flex gap-2">
              <button
                type="submit"
                disabled={actionSaving}
                className="rounded-xl bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-500 disabled:cursor-not-allowed disabled:bg-indigo-300"
              >
                {actionSaving
                  ? l("Saving...", "Kaydediliyor...")
                  : actionForm.type === "receive"
                    ? l("Receive Transit", "Transit Teslim Al")
                    : l("Cancel Transit", "Transit Iptal Et")}
              </button>
            </div>
          </form>
        </section>
      ) : null}

      <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
        <h2 className="text-base font-semibold text-slate-900">{l("Filters", "Filtreler")}</h2>
        <form className="mt-4 grid gap-3 md:grid-cols-6" onSubmit={handleFilterSubmit}>
          <label className="flex flex-col gap-1 text-sm text-slate-700">
            {l("Legal entity", "Hukuki birim")}
            <select
              value={filters.legalEntityId}
              onChange={(event) =>
                setFilters((prev) => ({
                  ...prev,
                  legalEntityId: event.target.value,
                  sourceRegisterId: "",
                  targetRegisterId: "",
                }))
              }
              className="rounded-xl border border-slate-300 px-3 py-2"
            >
              <option value="">{l("All", "Tum")}</option>
              {legalEntityOptions.map((entity) => (
                <option key={`transit-entity-${entity.id}`} value={entity.id}>
                  {toLegalEntityLabel(entity)}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-1 text-sm text-slate-700">
            {l("Source register", "Kaynak kasa")}
            <select
              value={filters.sourceRegisterId}
              onChange={(event) =>
                setFilters((prev) => ({
                  ...prev,
                  sourceRegisterId: event.target.value,
                }))
              }
              className="rounded-xl border border-slate-300 px-3 py-2"
            >
              <option value="">{l("All", "Tum")}</option>
              {sourceRegisterOptions.map((register) => (
                <option key={`transit-source-register-${register.id}`} value={register.id}>
                  {toRegisterLabel(register)}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-1 text-sm text-slate-700">
            {l("Target register", "Hedef kasa")}
            <select
              value={filters.targetRegisterId}
              onChange={(event) =>
                setFilters((prev) => ({
                  ...prev,
                  targetRegisterId: event.target.value,
                }))
              }
              className="rounded-xl border border-slate-300 px-3 py-2"
            >
              <option value="">{l("All", "Tum")}</option>
              {targetRegisterOptions.map((register) => (
                <option key={`transit-target-register-${register.id}`} value={register.id}>
                  {toRegisterLabel(register)}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-1 text-sm text-slate-700">
            {l("Status", "Durum")}
            <select
              value={filters.status}
              onChange={(event) =>
                setFilters((prev) => ({
                  ...prev,
                  status: event.target.value,
                }))
              }
              className="rounded-xl border border-slate-300 px-3 py-2"
            >
              <option value="">{l("All", "Tum")}</option>
              {TRANSIT_STATUSES.map((status) => (
                <option key={`transit-status-${status}`} value={status}>
                  {status}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-1 text-sm text-slate-700">
            {l("Initiated from", "Baslangic tarihi")}
            <input
              type="date"
              value={filters.initiatedDateFrom}
              onChange={(event) =>
                setFilters((prev) => ({
                  ...prev,
                  initiatedDateFrom: event.target.value,
                }))
              }
              className="rounded-xl border border-slate-300 px-3 py-2"
            />
          </label>

          <label className="flex flex-col gap-1 text-sm text-slate-700">
            {l("Initiated to", "Bitis tarihi")}
            <input
              type="date"
              value={filters.initiatedDateTo}
              onChange={(event) =>
                setFilters((prev) => ({
                  ...prev,
                  initiatedDateTo: event.target.value,
                }))
              }
              className="rounded-xl border border-slate-300 px-3 py-2"
            />
          </label>

          <div className="md:col-span-6 flex flex-wrap gap-2">
            <button
              type="submit"
              disabled={loading}
              className="rounded-xl bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-700 disabled:cursor-not-allowed disabled:bg-slate-400"
            >
              {loading ? l("Loading...", "Yukleniyor...") : l("Apply filters", "Filtrele")}
            </button>
            <button
              type="button"
              onClick={handleFilterReset}
              className="rounded-xl border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-600 hover:bg-slate-100"
              disabled={loading}
            >
              {l("Reset", "Sifirla")}
            </button>
          </div>
        </form>

        {error ? (
          <div className="mt-4 rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
            <p>{error}</p>
            {errorRequestId ? (
              <p className="mt-1 text-xs">{l("requestId", "talepId")}: {errorRequestId}</p>
            ) : null}
          </div>
        ) : null}

        {warning ? (
          <div className="mt-4 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700">
            {warning}
          </div>
        ) : null}

        {message ? (
          <div className="mt-4 rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
            {message}
          </div>
        ) : null}
      </section>

      <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
        <div className="mb-3 flex items-center justify-between gap-3">
          <h2 className="text-base font-semibold text-slate-900">
            {l("Transit transfers", "Transit transferler")}
          </h2>
          <span className="text-xs text-slate-500">
            {l(`${transferRows.length} row(s)`, `${transferRows.length} satir`)}
          </span>
        </div>

        {!canRead ? (
          <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700">
            {l(
              "You do not have permission to view transit transfers.",
              "Transit transferleri goruntuleme yetkiniz yok."
            )}
          </div>
        ) : transferRows.length === 0 ? (
          <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-8 text-center text-sm text-slate-500">
            {l("No transit transfers found.", "Transit transfer kaydi bulunamadi.")}
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-slate-200 text-sm">
              <thead className="bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-500">
                <tr>
                  <th className="px-3 py-2">{l("Transfer", "Transfer")}</th>
                  <th className="px-3 py-2">{l("Route", "Rota")}</th>
                  <th className="px-3 py-2">{l("Amount", "Tutar")}</th>
                  <th className="px-3 py-2">{l("Linked txns", "Bagli islemler")}</th>
                  <th className="px-3 py-2">{l("Timeline", "Zaman cizelgesi")}</th>
                  <th className="px-3 py-2">{l("Notes", "Notlar")}</th>
                  <th className="px-3 py-2">{l("Actions", "Aksiyonlar")}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 text-slate-700">
                {transferRows.map((row, index) => {
                  const transferId = toPositiveInt(row?.id);
                  const status = toUpper(row?.status);
                  const canReceive = canReceiveRow(row);
                  const canCancelTransit = canCancelRow(row);

                  return (
                    <tr key={`transit-row-${transferId || index}`}>
                      <td className="px-3 py-2 align-top">
                        <div className="font-medium text-slate-900">#{transferId || "-"}</div>
                        <div
                          className={`mt-1 inline-flex rounded-full px-2 py-0.5 text-xs font-semibold ${statusClassName(
                            status
                          )}`}
                        >
                          {status || "-"}
                        </div>
                        <div className="mt-1 text-xs text-slate-500">
                          {l("LE", "HB")}: {row?.legal_entity_code || row?.legal_entity_id || "-"}
                        </div>
                      </td>

                      <td className="px-3 py-2 align-top">
                        <div>
                          <span className="font-medium">
                            {row?.source_cash_register_code || row?.source_cash_register_id || "-"}
                          </span>
                          <span className="mx-1 text-slate-400">-&gt;</span>
                          <span className="font-medium">
                            {row?.target_cash_register_code || row?.target_cash_register_id || "-"}
                          </span>
                        </div>
                        <div className="text-xs text-slate-500">
                          {l("OU", "OU")}:{" "}
                          {row?.source_operating_unit_code || row?.source_operating_unit_id || "-"}
                          <span className="mx-1">-&gt;</span>
                          {row?.target_operating_unit_code || row?.target_operating_unit_id || "-"}
                        </div>
                      </td>

                      <td className="px-3 py-2 align-top">
                        <div className="font-medium">
                          {formatAmount(row?.amount)} {row?.currency_code || ""}
                        </div>
                        <div className="text-xs text-slate-500">
                          {l("Transit account", "Transit hesap")}:{" "}
                          {row?.transit_account_code || row?.transit_account_id || "-"}
                        </div>
                      </td>

                      <td className="px-3 py-2 align-top">
                        <div>
                          {l("OUT", "CIKIS")}: #{row?.transfer_out_cash_transaction_id || "-"}
                          <span className="ml-1 text-xs text-slate-500">
                            ({row?.transfer_out_txn_status || "-"})
                          </span>
                        </div>
                        <div>
                          {l("IN", "GIRIS")}: #{row?.transfer_in_cash_transaction_id || "-"}
                          <span className="ml-1 text-xs text-slate-500">
                            ({row?.transfer_in_txn_status || "-"})
                          </span>
                        </div>
                      </td>

                      <td className="px-3 py-2 align-top text-xs text-slate-600">
                        <div>{l("Initiated", "Baslatildi")}: {formatDateTime(row?.initiated_at)}</div>
                        <div>{l("In transit", "Yolda")}: {formatDateTime(row?.in_transit_at)}</div>
                        <div>{l("Received", "Teslim alindi")}: {formatDateTime(row?.received_at)}</div>
                      </td>

                      <td className="px-3 py-2 align-top text-xs text-slate-600">
                        <div>{row?.note || "-"}</div>
                        {row?.cancel_reason ? (
                          <div>{l("Cancel", "Iptal")}: {row.cancel_reason}</div>
                        ) : null}
                        {row?.reverse_reason ? (
                          <div>{l("Reverse", "Ters kayit")}: {row.reverse_reason}</div>
                        ) : null}
                      </td>

                      <td className="px-3 py-2 align-top">
                        <div className="flex flex-col gap-2">
                          <button
                            type="button"
                            onClick={() => openReceiveForm(row)}
                            disabled={!canReceive || actionSaving}
                            className="rounded-lg border border-indigo-200 px-3 py-1.5 text-xs font-medium text-indigo-700 hover:bg-indigo-50 disabled:cursor-not-allowed disabled:border-slate-200 disabled:text-slate-400"
                          >
                            {l("Receive", "Teslim al")}
                          </button>
                          <button
                            type="button"
                            onClick={() => openCancelForm(row)}
                            disabled={!canCancelTransit || actionSaving}
                            className="rounded-lg border border-rose-200 px-3 py-1.5 text-xs font-medium text-rose-700 hover:bg-rose-50 disabled:cursor-not-allowed disabled:border-slate-200 disabled:text-slate-400"
                          >
                            {l("Cancel", "Iptal et")}
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
