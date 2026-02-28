import { useEffect, useMemo, useState } from "react";
import {
  closeCashSession,
  listCashRegisters,
  listCashSessions,
  openCashSession,
} from "../../api/cashAdmin.js";
import { useAuth } from "../../auth/useAuth.js";
import StatusTimeline from "../../components/StatusTimeline.jsx";
import { useToastMessage } from "../../hooks/useToastMessage.js";
import { useI18n } from "../../i18n/useI18n.js";
import {
  buildLifecycleTimelineSteps,
  getLifecycleAllowedActions,
  getLifecycleStatusMeta,
} from "../../lifecycle/lifecycleRules.js";
import CashControlModeBanner from "./CashControlModeBanner.jsx";

const CLOSE_REASONS = ["END_SHIFT", "FORCED_CLOSE", "COUNT_CORRECTION"];

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function toOptionalNumber(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : Number.NaN;
}

function toUpper(value) {
  return String(value || "").trim().toUpperCase();
}

function formatAmount(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return "-";
  }
  return num.toLocaleString(undefined, {
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

function buildCashSessionLifecycleEvents(row) {
  if (!row) {
    return [];
  }

  const status = toUpper(row?.status);
  const openedAt = row?.opened_at || row?.openedAt || null;
  const closedAt = row?.closed_at || row?.closedAt || null;
  const updatedAt = row?.updated_at || row?.updatedAt || null;
  const openedBy =
    String(row?.opened_by_email || "").trim() ||
    (toPositiveInt(row?.opened_by_user_id) ? `User #${row.opened_by_user_id}` : null);
  const closedBy =
    String(row?.closed_by_email || "").trim() ||
    String(row?.approved_by_email || "").trim() ||
    (toPositiveInt(row?.closed_by_user_id)
      ? `User #${row.closed_by_user_id}`
      : toPositiveInt(row?.approved_by_user_id)
        ? `User #${row.approved_by_user_id}`
        : null);
  const closeReason = String(row?.closed_reason || row?.closedReason || "").trim();
  const closeNote = String(row?.close_note || row?.closeNote || "").trim();

  const events = [];
  if (openedAt) {
    events.push({
      statusCode: "OPEN",
      at: openedAt,
      actorName: openedBy,
    });
  }
  if (status === "CLOSED") {
    events.push({
      statusCode: "CLOSED",
      at: closedAt || updatedAt || openedAt,
      actorName: closedBy,
      note: [closeReason, closeNote].filter(Boolean).join(" | ") || null,
    });
  }

  return events;
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

function mapSessionErrorMessage(rawMessage, t) {
  const message = normalizeErrorMessage(rawMessage);
  if (!message) {
    return "";
  }

  const lower = message.toLowerCase();
  if (lower.includes("registerid not found for tenant")) {
    return t("cashSessions.errorsMapped.registerNotFound");
  }
  if (lower.includes("an open session already exists for this register")) {
    return t("cashSessions.errorsMapped.sessionAlreadyOpen");
  }
  if (lower.includes("cash register session_mode is none")) {
    return t("cashSessions.errorsMapped.sessionModeNone");
  }
  if (lower.includes("cash register is not active")) {
    return t("cashSessions.errorsMapped.registerInactive");
  }
  if (lower.includes("cash session not found")) {
    return t("cashSessions.errorsMapped.sessionNotFound");
  }
  if (lower.includes("only open sessions can be closed")) {
    return t("cashSessions.errorsMapped.onlyOpenClose");
  }
  if (lower.includes("cannot close session while draft/submitted/approved transactions exist")) {
    return t("cashSessions.errorsMapped.unpostedTransactionsExist");
  }
  if (lower.includes("closenote is required when closedreason is forced_close")) {
    return t("cashSessions.errors.closeNoteForced");
  }
  if (lower.includes("closenote is required when variance exceeds approval threshold")) {
    return t("cashSessions.errorsMapped.closeNoteThreshold");
  }
  if (lower.includes("variance exceeds configured threshold")) {
    return t("cashSessions.errorsMapped.varianceApprovalRequired");
  }
  if (lower.includes("variancegainaccountid must be configured")) {
    return t("cashSessions.errorsMapped.varianceGainMissing");
  }
  if (lower.includes("variancelossaccountid must be configured")) {
    return t("cashSessions.errorsMapped.varianceLossMissing");
  }

  return "";
}

function toSessionErrorState(err, t, fallbackKey) {
  const requestId = extractRequestId(err);
  const rawMessage = normalizeErrorMessage(
    err?.response?.data?.message || err?.message
  );
  const mappedMessage = mapSessionErrorMessage(rawMessage, t);
  const fallbackMessage = t(fallbackKey);
  return {
    message: mappedMessage || (rawMessage ? `${fallbackMessage} (${rawMessage})` : fallbackMessage),
    requestId,
  };
}

export default function CashSessionsPage() {
  const { hasPermission } = useAuth();
  const { t } = useI18n();
  const localizeSessionStatus = (status) => {
    const normalized = toUpper(status);
    if (!normalized) {
      return "-";
    }
    if (normalized === "OPEN") {
      return t("cashSessions.values.statusOpen");
    }
    if (normalized === "CLOSED") {
      return t("cashSessions.values.statusClosed");
    }
    return normalized;
  };

  const canRead = hasPermission("cash.register.read");
  const canOpen = hasPermission("cash.session.open");
  const canClose = hasPermission("cash.session.close");
  const canApproveVariance = hasPermission("cash.variance.approve");

  const [loading, setLoading] = useState(false);
  const [opening, setOpening] = useState(false);
  const [closing, setClosing] = useState(false);
  const [error, setError] = useState("");
  const [errorRequestId, setErrorRequestId] = useState(null);
  const [message, setMessage] = useToastMessage("", { toastType: "success" });

  const [registers, setRegisters] = useState([]);
  const [sessionRows, setSessionRows] = useState([]);
  const [openSessionRows, setOpenSessionRows] = useState([]);

  const [openForm, setOpenForm] = useState({
    registerId: "",
    openingAmount: "",
  });
  const [closeForm, setCloseForm] = useState({
    sessionId: "",
    countedClosingAmount: "",
    closedReason: "END_SHIFT",
    closeNote: "",
    approveVariance: false,
  });
  const [selectedLifecycleSessionId, setSelectedLifecycleSessionId] = useState(null);

  const openableRegisters = useMemo(() => {
    return [...registers]
      .filter((row) => {
        const status = toUpper(row?.status);
        const sessionMode = toUpper(row?.session_mode);
        return status === "ACTIVE" && sessionMode !== "NONE";
      })
      .sort((a, b) => Number(a?.id || 0) - Number(b?.id || 0));
  }, [registers]);

  const openSessions = useMemo(() => {
    return [...openSessionRows].sort((a, b) => Number(b?.id || 0) - Number(a?.id || 0));
  }, [openSessionRows]);

  const historyRows = useMemo(() => {
    return [...sessionRows].sort((a, b) => Number(b?.id || 0) - Number(a?.id || 0));
  }, [sessionRows]);

  const openRegisterIds = useMemo(() => {
    return new Set(
      openSessions
        .map((row) => toPositiveInt(row?.cash_register_id))
        .filter(Boolean)
    );
  }, [openSessions]);

  const requiredModeWithoutOpen = useMemo(() => {
    return registers.filter((row) => {
      const status = toUpper(row?.status);
      const sessionMode = toUpper(row?.session_mode);
      const registerId = toPositiveInt(row?.id);
      return (
        status === "ACTIVE" &&
        sessionMode === "REQUIRED" &&
        registerId &&
        !openRegisterIds.has(registerId)
      );
    });
  }, [registers, openRegisterIds]);

  const selectedCloseSession = useMemo(() => {
    const sessionId = toPositiveInt(closeForm.sessionId);
    if (!sessionId) {
      return null;
    }
    return openSessions.find((row) => toPositiveInt(row?.id) === sessionId) || null;
  }, [closeForm.sessionId, openSessions]);
  const selectedLifecycleSession = useMemo(() => {
    const sessionId = toPositiveInt(selectedLifecycleSessionId || closeForm.sessionId);
    if (!sessionId) {
      return null;
    }
    return (
      [...openSessions, ...historyRows].find((row) => toPositiveInt(row?.id) === sessionId) ||
      null
    );
  }, [closeForm.sessionId, historyRows, openSessions, selectedLifecycleSessionId]);
  const selectedSessionLifecycleMeta = useMemo(
    () => getLifecycleStatusMeta("cashSession", selectedLifecycleSession?.status),
    [selectedLifecycleSession?.status]
  );
  const selectedSessionLifecycleActions = useMemo(
    () => getLifecycleAllowedActions("cashSession", selectedLifecycleSession?.status),
    [selectedLifecycleSession?.status]
  );
  const selectedSessionLifecycleActionLabels = useMemo(() => {
    const labelsByAction = {
      close: t("cashSessions.lifecycle.actionLabels.close"),
    };
    return selectedSessionLifecycleActions.map(
      (row) => labelsByAction[row.action] || row.label
    );
  }, [selectedSessionLifecycleActions, t]);
  const selectedSessionLifecycleTimeline = useMemo(
    () =>
      buildLifecycleTimelineSteps(
        "cashSession",
        selectedLifecycleSession?.status,
        buildCashSessionLifecycleEvents(selectedLifecycleSession)
      ),
    [selectedLifecycleSession]
  );

  async function loadData() {
    if (!canRead) {
      setRegisters([]);
      setSessionRows([]);
      setOpenSessionRows([]);
      return;
    }

    setLoading(true);
    setError("");
    setErrorRequestId(null);
    try {
      const [registerRes, sessionsRes, openSessionsRes] = await Promise.all([
        listCashRegisters({ status: "ACTIVE", limit: 300, offset: 0 }),
        listCashSessions({ limit: 300, offset: 0 }),
        listCashSessions({ status: "OPEN", limit: 300, offset: 0 }),
      ]);

      const registerRows = registerRes?.rows || [];
      const allSessions = sessionsRes?.rows || [];
      const openedSessions = openSessionsRes?.rows || [];

      setRegisters(registerRows);
      setSessionRows(allSessions);
      setOpenSessionRows(openedSessions);

      setOpenForm((prev) => {
        const currentId = toPositiveInt(prev.registerId);
        if (currentId && registerRows.some((row) => toPositiveInt(row?.id) === currentId)) {
          return prev;
        }
        const defaultRegister = registerRows.find((row) => {
          const status = toUpper(row?.status);
          const sessionMode = toUpper(row?.session_mode);
          return status === "ACTIVE" && sessionMode !== "NONE";
        });
        return {
          ...prev,
          registerId: defaultRegister ? String(defaultRegister.id) : "",
        };
      });

      setCloseForm((prev) => {
        const currentSessionId = toPositiveInt(prev.sessionId);
        if (
          currentSessionId &&
          openedSessions.some((row) => toPositiveInt(row?.id) === currentSessionId)
        ) {
          return prev;
        }
        return {
          ...prev,
          sessionId: openedSessions[0]?.id ? String(openedSessions[0].id) : "",
        };
      });
    } catch (err) {
      const errorState = toSessionErrorState(err, t, "cashSessions.errors.load");
      setError(errorState.message);
      setErrorRequestId(errorState.requestId);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead]);

  useEffect(() => {
    const closeSessionId = toPositiveInt(closeForm.sessionId);
    if (closeSessionId) {
      setSelectedLifecycleSessionId(String(closeSessionId));
    }
  }, [closeForm.sessionId]);

  useEffect(() => {
    const selectedId = toPositiveInt(selectedLifecycleSessionId);
    if (!selectedId) {
      return;
    }
    const found = [...openSessions, ...historyRows].some(
      (row) => toPositiveInt(row?.id) === selectedId
    );
    if (!found) {
      setSelectedLifecycleSessionId(null);
    }
  }, [historyRows, openSessions, selectedLifecycleSessionId]);

  async function handleOpenSession(event) {
    event.preventDefault();
    if (!canOpen) {
      setError(t("cashSessions.errors.missingOpenPermission"));
      setErrorRequestId(null);
      return;
    }

    const registerId = toPositiveInt(openForm.registerId);
    const openingAmount = toOptionalNumber(openForm.openingAmount);

    if (!registerId) {
      setError(t("cashSessions.errors.registerRequired"));
      setErrorRequestId(null);
      return;
    }
    if (Number.isNaN(openingAmount)) {
      setError(t("cashSessions.errors.invalidOpeningAmount"));
      setErrorRequestId(null);
      return;
    }

    setOpening(true);
    setError("");
    setErrorRequestId(null);
    setMessage("");

    try {
      await openCashSession({
        registerId,
        openingAmount: openingAmount === null ? undefined : openingAmount,
      });
      setMessage(t("cashSessions.messages.opened"));
      setOpenForm((prev) => ({ ...prev, openingAmount: "" }));
      await loadData();
    } catch (err) {
      const errorState = toSessionErrorState(err, t, "cashSessions.errors.open");
      setError(errorState.message);
      setErrorRequestId(errorState.requestId);
    } finally {
      setOpening(false);
    }
  }

  async function handleCloseSession(event) {
    event.preventDefault();
    if (!canClose) {
      setError(t("cashSessions.errors.missingClosePermission"));
      setErrorRequestId(null);
      return;
    }

    const sessionId = toPositiveInt(closeForm.sessionId);
    const countedClosingAmount = toOptionalNumber(closeForm.countedClosingAmount);
    const closeNote = String(closeForm.closeNote || "").trim();

    if (!sessionId) {
      setError(t("cashSessions.errors.sessionRequired"));
      setErrorRequestId(null);
      return;
    }
    if (countedClosingAmount === null || Number.isNaN(countedClosingAmount)) {
      setError(t("cashSessions.errors.countedRequired"));
      setErrorRequestId(null);
      return;
    }
    if (toUpper(closeForm.closedReason) === "FORCED_CLOSE" && !closeNote) {
      setError(t("cashSessions.errors.closeNoteForced"));
      setErrorRequestId(null);
      return;
    }
    if (closeForm.approveVariance && !canApproveVariance) {
      setError(t("cashSessions.errors.missingVarianceApprovePermission"));
      setErrorRequestId(null);
      return;
    }
    if (closeForm.approveVariance && !closeNote) {
      setError(t("cashSessions.errors.closeNoteApproval"));
      setErrorRequestId(null);
      return;
    }

    const payload = {
      countedClosingAmount,
      closedReason: closeForm.closedReason,
      closeNote: closeNote || undefined,
      approveVariance: Boolean(closeForm.approveVariance),
    };

    setClosing(true);
    setError("");
    setErrorRequestId(null);
    setMessage("");

    try {
      await closeCashSession(sessionId, payload);
      setMessage(t("cashSessions.messages.closed"));
      setCloseForm((prev) => ({
        ...prev,
        countedClosingAmount: "",
        closeNote: "",
        approveVariance: false,
      }));
      await loadData();
    } catch (err) {
      const errorState = toSessionErrorState(err, t, "cashSessions.errors.close");
      setError(errorState.message);
      setErrorRequestId(errorState.requestId);
    } finally {
      setClosing(false);
    }
  }

  function selectSessionForClose(row) {
    const sessionId = toPositiveInt(row?.id);
    if (!sessionId) {
      return;
    }
    setSelectedLifecycleSessionId(String(sessionId));
    setCloseForm((prev) => ({
      ...prev,
      sessionId: String(sessionId),
    }));
  }

  if (!canRead) {
    return (
      <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
        {t("cashSessions.errors.missingReadPermission")}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">{t("cashSessions.title")}</h1>
        <p className="mt-1 text-sm text-slate-600">{t("cashSessions.subtitle")}</p>
      </div>

      <CashControlModeBanner />

      {error ? (
        <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
          <p>{error}</p>
          {errorRequestId ? (
            <p className="mt-1 text-xs font-medium text-rose-700">
              {t("cashSessions.errors.requestId", { requestId: errorRequestId })}
            </p>
          ) : null}
        </div>
      ) : null}

      {message ? (
        <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
          {message}
        </div>
      ) : null}

      {requiredModeWithoutOpen.length > 0 ? (
        <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          <p className="font-semibold">{t("cashSessions.requiredWarning.title")}</p>
          <p className="mt-1">{t("cashSessions.requiredWarning.description")}</p>
          <ul className="mt-2 list-disc pl-5">
            {requiredModeWithoutOpen.map((row) => (
              <li key={`required-warning-${row.id}`}>
                {(row.code || row.id) + " - " + (row.name || "-")}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("cashSessions.sections.open")}
        </h2>

        {canOpen ? (
          <form onSubmit={handleOpenSession} className="grid gap-2 md:grid-cols-3">
            <select
              value={openForm.registerId}
              onChange={(event) =>
                setOpenForm((prev) => ({ ...prev, registerId: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              <option value="">{t("cashSessions.placeholders.register")}</option>
              {openableRegisters.map((row) => (
                <option key={`open-register-${row.id}`} value={row.id}>
                  {(row.code || row.id) + " - " + (row.name || "-")}
                </option>
              ))}
            </select>
            <input
              type="number"
              min="0"
              step="0.000001"
              value={openForm.openingAmount}
              onChange={(event) =>
                setOpenForm((prev) => ({ ...prev, openingAmount: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={t("cashSessions.form.openingAmountOptional")}
            />
            <button
              type="submit"
              disabled={opening}
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {opening
                ? t("cashSessions.actions.saving")
                : t("cashSessions.actions.open")}
            </button>
          </form>
        ) : (
          <p className="text-sm text-slate-500">{t("cashSessions.readOnlyOpenNotice")}</p>
        )}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("cashSessions.sections.close")}
        </h2>

        {openSessions.length === 0 ? (
          <p className="text-sm text-slate-500">{t("cashSessions.emptyOpen")}</p>
        ) : canClose ? (
          <form onSubmit={handleCloseSession} className="grid gap-2 md:grid-cols-2">
            <select
              value={closeForm.sessionId}
              onChange={(event) =>
                setCloseForm((prev) => ({ ...prev, sessionId: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              <option value="">{t("cashSessions.placeholders.openSession")}</option>
              {openSessions.map((row) => (
                <option key={`close-session-${row.id}`} value={row.id}>
                  {`#${row.id} - ${row.cash_register_code || row.cash_register_id} (${row.cash_register_name || "-"})`}
                </option>
              ))}
            </select>

            <input
              type="number"
              min="0"
              step="0.000001"
              value={closeForm.countedClosingAmount}
              onChange={(event) =>
                setCloseForm((prev) => ({
                  ...prev,
                  countedClosingAmount: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={t("cashSessions.form.countedClosingAmount")}
              required
            />

            <select
              value={closeForm.closedReason}
              onChange={(event) =>
                setCloseForm((prev) => ({ ...prev, closedReason: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            >
              {CLOSE_REASONS.map((reason) => (
                <option key={reason} value={reason}>
                  {reason}
                </option>
              ))}
            </select>

            <textarea
              value={closeForm.closeNote}
              onChange={(event) =>
                setCloseForm((prev) => ({ ...prev, closeNote: event.target.value }))
              }
              className="min-h-24 rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={t("cashSessions.form.closeNote")}
              maxLength={500}
            />

            {canApproveVariance ? (
              <label className="inline-flex items-center gap-2 rounded-lg border border-slate-300 px-3 py-2 text-sm text-slate-700 md:col-span-2">
                <input
                  type="checkbox"
                  checked={Boolean(closeForm.approveVariance)}
                  onChange={(event) =>
                    setCloseForm((prev) => ({
                      ...prev,
                      approveVariance: event.target.checked,
                    }))
                  }
                />
                {t("cashSessions.form.approveVariance")}
              </label>
            ) : (
              <p className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800 md:col-span-2">
                {t("cashSessions.approvalNotice")}
              </p>
            )}

            {toUpper(closeForm.closedReason) === "FORCED_CLOSE" ? (
              <p className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800 md:col-span-2">
                {t("cashSessions.forcedCloseNotice")}
              </p>
            ) : null}

            <div className="md:col-span-2">
              <button
                type="submit"
                disabled={closing}
                className="rounded-lg bg-cyan-700 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
              >
                {closing
                  ? t("cashSessions.actions.saving")
                  : t("cashSessions.actions.close")}
              </button>
            </div>
          </form>
        ) : (
          <p className="text-sm text-slate-500">{t("cashSessions.readOnlyCloseNotice")}</p>
        )}

        {selectedCloseSession ? (
          <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700">
            {t("cashSessions.selectedSessionSummary", {
              id: selectedCloseSession.id,
              registerCode:
                selectedCloseSession.cash_register_code ||
                selectedCloseSession.cash_register_id,
              opening: formatAmount(selectedCloseSession.opening_amount),
              expected:
                selectedCloseSession.expected_closing_amount === null ||
                selectedCloseSession.expected_closing_amount === undefined
                  ? "-"
                  : formatAmount(selectedCloseSession.expected_closing_amount),
            })}
          </div>
        ) : null}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("cashSessions.sections.lifecycle")}
        </h2>
        {selectedLifecycleSession ? (
          <div className="grid gap-3 md:grid-cols-2">
            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-700">
                {t("cashSessions.lifecycle.snapshotTitle")}
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {t("cashSessions.lifecycle.selectedSummary", {
                  id: selectedLifecycleSession.id || "-",
                  registerCode:
                    selectedLifecycleSession.cash_register_code ||
                    selectedLifecycleSession.cash_register_id ||
                    "-",
                  status: localizeSessionStatus(selectedLifecycleSession.status),
                })}
              </p>
              {selectedSessionLifecycleMeta?.description ? (
                <p className="mt-1 text-sm text-slate-700">
                  {selectedSessionLifecycleMeta.description}
                </p>
              ) : null}
              {selectedSessionLifecycleActionLabels.length > 0 ? (
                <p className="mt-1 text-xs text-slate-600">
                  {t("cashSessions.lifecycle.nextTransitions", {
                    actions: selectedSessionLifecycleActionLabels.join(", "),
                  })}
                </p>
              ) : (
                <p className="mt-1 text-xs text-slate-500">
                  {t("cashSessions.lifecycle.noTransitions")}
                </p>
              )}
            </div>
            <StatusTimeline
              title={t("cashSessions.lifecycle.timelineTitle")}
              steps={selectedSessionLifecycleTimeline}
              emptyText={t("cashSessions.lifecycle.timelineEmpty")}
            />
          </div>
        ) : (
          <p className="text-sm text-slate-500">{t("cashSessions.lifecycle.noSelection")}</p>
        )}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-3 flex items-center justify-between gap-2">
          <h2 className="text-sm font-semibold text-slate-700">
            {t("cashSessions.sections.openSessions")}
          </h2>
          <button
            type="button"
            onClick={loadData}
            disabled={loading}
            className="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-50 disabled:opacity-60"
          >
            {loading ? t("cashSessions.actions.loading") : t("cashSessions.actions.refresh")}
          </button>
        </div>

        <div className="overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">ID</th>
                <th className="px-3 py-2">{t("cashSessions.table.register")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.status")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.openedAt")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.opening")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.expected")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.counted")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.variance")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.actions")}</th>
              </tr>
            </thead>
            <tbody>
              {openSessions.map((row) => (
                <tr
                  key={`open-session-row-${row.id}`}
                  className={`border-t border-slate-100 ${
                    toPositiveInt(row.id) === toPositiveInt(selectedLifecycleSessionId)
                      ? "bg-cyan-50"
                      : ""
                  }`}
                >
                  <td className="px-3 py-2">{row.id}</td>
                  <td className="px-3 py-2">
                    {(row.cash_register_code || row.cash_register_id) + " - " +
                      (row.cash_register_name || "-")}
                  </td>
                  <td className="px-3 py-2">{localizeSessionStatus(row.status)}</td>
                  <td className="px-3 py-2">{formatDateTime(row.opened_at)}</td>
                  <td className="px-3 py-2">{formatAmount(row.opening_amount)}</td>
                  <td className="px-3 py-2">
                    {row.expected_closing_amount === null ||
                    row.expected_closing_amount === undefined
                      ? "-"
                      : formatAmount(row.expected_closing_amount)}
                  </td>
                  <td className="px-3 py-2">
                    {row.counted_closing_amount === null ||
                    row.counted_closing_amount === undefined
                      ? "-"
                      : formatAmount(row.counted_closing_amount)}
                  </td>
                  <td className="px-3 py-2">
                    {row.variance_amount === null ||
                    row.variance_amount === undefined
                      ? row.varianceAmount === null || row.varianceAmount === undefined
                        ? "-"
                        : formatAmount(row.varianceAmount)
                      : formatAmount(row.variance_amount)}
                  </td>
                  <td className="px-3 py-2">
                    <div className="flex flex-wrap gap-1">
                      {canClose ? (
                        <button
                          type="button"
                          onClick={() => selectSessionForClose(row)}
                          className="rounded-md border border-cyan-300 px-2 py-1 text-xs font-semibold text-cyan-700 hover:bg-cyan-50"
                        >
                          {t("cashSessions.actions.useForClose")}
                        </button>
                      ) : null}
                      <button
                        type="button"
                        onClick={() => setSelectedLifecycleSessionId(String(row.id))}
                        className="rounded-md border border-slate-300 px-2 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-50"
                      >
                        {t("cashSessions.actions.inspectLifecycle")}
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
              {!loading && openSessions.length === 0 ? (
                <tr>
                  <td colSpan={9} className="px-3 py-3 text-slate-500">
                    {t("cashSessions.emptyOpen")}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("cashSessions.sections.history")}
        </h2>

        <div className="overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">ID</th>
                <th className="px-3 py-2">{t("cashSessions.table.register")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.status")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.openedAt")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.closedAt")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.opening")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.expected")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.counted")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.variance")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.closedReason")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.approvedBy")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.approvedAt")}</th>
                <th className="px-3 py-2">{t("cashSessions.table.actions")}</th>
              </tr>
            </thead>
            <tbody>
              {historyRows.map((row) => (
                <tr
                  key={`session-history-row-${row.id}`}
                  className={`border-t border-slate-100 ${
                    toPositiveInt(row.id) === toPositiveInt(selectedLifecycleSessionId)
                      ? "bg-cyan-50"
                      : ""
                  }`}
                >
                  <td className="px-3 py-2">{row.id}</td>
                  <td className="px-3 py-2">
                    {(row.cash_register_code || row.cash_register_id) + " - " +
                      (row.cash_register_name || "-")}
                  </td>
                  <td className="px-3 py-2">{localizeSessionStatus(row.status)}</td>
                  <td className="px-3 py-2">{formatDateTime(row.opened_at)}</td>
                  <td className="px-3 py-2">{formatDateTime(row.closed_at)}</td>
                  <td className="px-3 py-2">{formatAmount(row.opening_amount)}</td>
                  <td className="px-3 py-2">
                    {row.expected_closing_amount === null ||
                    row.expected_closing_amount === undefined
                      ? "-"
                      : formatAmount(row.expected_closing_amount)}
                  </td>
                  <td className="px-3 py-2">
                    {row.counted_closing_amount === null ||
                    row.counted_closing_amount === undefined
                      ? "-"
                      : formatAmount(row.counted_closing_amount)}
                  </td>
                  <td className="px-3 py-2">
                    {row.variance_amount === null || row.variance_amount === undefined
                      ? row.varianceAmount === null || row.varianceAmount === undefined
                        ? "-"
                        : formatAmount(row.varianceAmount)
                      : formatAmount(row.variance_amount)}
                  </td>
                  <td className="px-3 py-2">{row.closed_reason || row.closedReason || "-"}</td>
                  <td className="px-3 py-2">
                    {row.approved_by_user_id ||
                      row.approvedByUserId ||
                      row.approved_by_email ||
                      "-"}
                  </td>
                  <td className="px-3 py-2">
                    {formatDateTime(row.approved_at || row.approvedAt)}
                  </td>
                  <td className="px-3 py-2">
                    <button
                      type="button"
                      onClick={() => setSelectedLifecycleSessionId(String(row.id))}
                      className="rounded-md border border-slate-300 px-2 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-50"
                    >
                      {t("cashSessions.actions.inspectLifecycle")}
                    </button>
                  </td>
                </tr>
              ))}
              {!loading && historyRows.length === 0 ? (
                <tr>
                  <td colSpan={13} className="px-3 py-3 text-slate-500">
                    {t("cashSessions.emptyHistory")}
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
