import { useEffect, useMemo, useState } from "react";
import { listBankAccounts } from "../../api/bankAccounts.js";
import {
  getReconciliationSuggestions,
  ignoreReconciliationLine,
  listReconciliationAudit,
  listReconciliationQueue,
  matchReconciliationLine,
  unmatchReconciliationLine,
} from "../../api/bankReconciliation.js";
import {
  applyReconciliationAutoRun,
  assignReconciliationException,
  ignoreReconciliationExceptionItem,
  listReconciliationExceptions,
  previewReconciliationAutoRun,
  resolveReconciliationException,
  retryReconciliationException,
} from "../../api/bankReconciliationAutomation.js";
import {
  createReconciliationPostingTemplate,
  listReconciliationPostingTemplates,
  updateReconciliationPostingTemplate,
} from "../../api/bankReconciliationPostingTemplates.js";
import {
  createReconciliationDifferenceProfile,
  listReconciliationDifferenceProfiles,
  updateReconciliationDifferenceProfile,
} from "../../api/bankReconciliationDifferenceProfiles.js";
import {
  createBankPaymentReturn,
  ignoreBankPaymentReturn,
  listBankPaymentReturns,
} from "../../api/bankPaymentReturns.js";
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

const EMPTY_B08_TEMPLATE_FORM = {
  id: "",
  scopeType: "LEGAL_ENTITY",
  legalEntityId: "",
  bankAccountId: "",
  templateCode: "",
  templateName: "",
  status: "ACTIVE",
  entryKind: "BANK_MISC",
  directionPolicy: "BOTH",
  counterAccountId: "",
  descriptionMode: "USE_STATEMENT_TEXT",
  fixedDescription: "",
  descriptionPrefix: "",
};

const EMPTY_B08B_DIFF_PROFILE_FORM = {
  id: "",
  scopeType: "LEGAL_ENTITY",
  legalEntityId: "",
  bankAccountId: "",
  profileCode: "",
  profileName: "",
  status: "ACTIVE",
  differenceType: "FEE",
  directionPolicy: "BOTH",
  maxAbsDifference: "0",
  expenseAccountId: "",
  fxGainAccountId: "",
  fxLossAccountId: "",
  currencyCode: "",
  descriptionPrefix: "",
};

const EMPTY_B08B_RETURN_FORM = {
  paymentBatchLineId: "",
  bankStatementLineId: "",
  eventType: "PAYMENT_RETURNED",
  amount: "",
  currencyCode: "",
  reasonCode: "",
  reasonMessage: "",
};

function mapB08TemplateRowToForm(row) {
  return {
    id: String(row?.id || ""),
    scopeType: String(row?.scope_type || "LEGAL_ENTITY"),
    legalEntityId: String(row?.legal_entity_id || ""),
    bankAccountId: String(row?.bank_account_id || ""),
    templateCode: String(row?.template_code || ""),
    templateName: String(row?.template_name || ""),
    status: String(row?.status || "ACTIVE"),
    entryKind: String(row?.entry_kind || "BANK_MISC"),
    directionPolicy: String(row?.direction_policy || "BOTH"),
    counterAccountId: String(row?.counter_account_id || ""),
    descriptionMode: String(row?.description_mode || "USE_STATEMENT_TEXT"),
    fixedDescription: String(row?.fixed_description || ""),
    descriptionPrefix: String(row?.description_prefix || ""),
  };
}

function mapB08BDiffProfileRowToForm(row) {
  return {
    id: String(row?.id || ""),
    scopeType: String(row?.scope_type || "LEGAL_ENTITY"),
    legalEntityId: String(row?.legal_entity_id || ""),
    bankAccountId: String(row?.bank_account_id || ""),
    profileCode: String(row?.profile_code || ""),
    profileName: String(row?.profile_name || ""),
    status: String(row?.status || "ACTIVE"),
    differenceType: String(row?.difference_type || "FEE"),
    directionPolicy: String(row?.direction_policy || "BOTH"),
    maxAbsDifference:
      row?.max_abs_difference === null || row?.max_abs_difference === undefined
        ? "0"
        : String(row.max_abs_difference),
    expenseAccountId: String(row?.expense_account_id || ""),
    fxGainAccountId: String(row?.fx_gain_account_id || ""),
    fxLossAccountId: String(row?.fx_loss_account_id || ""),
    currencyCode: String(row?.currency_code || ""),
    descriptionPrefix: String(row?.description_prefix || ""),
  };
}

export default function BankReconciliationPage() {
  const { hasPermission } = useAuth();
  const canRead = hasPermission("bank.reconcile.read");
  const canWrite = hasPermission("bank.reconcile.write");
  const canReadBanks = hasPermission("bank.accounts.read");
  const canAutoRun = hasPermission("bank.reconcile.auto.run");
  const canReadTemplates = hasPermission("bank.reconcile.templates.read");
  const canWriteTemplates = hasPermission("bank.reconcile.templates.write");
  const canReadDiffProfiles = hasPermission("bank.reconcile.diffprofiles.read");
  const canWriteDiffProfiles = hasPermission("bank.reconcile.diffprofiles.write");
  const canReadReturnEvents = hasPermission("bank.payments.returns.read");
  const canWriteReturnEvents = hasPermission("bank.payments.returns.write");
  const canReadExceptions = hasPermission("bank.reconcile.exceptions.read");
  const canWriteExceptions = hasPermission("bank.reconcile.exceptions.write");

  const [bankAccounts, setBankAccounts] = useState([]);
  const [filters, setFilters] = useState({
    bankAccountId: "",
    reconStatus: "",
    q: "",
  });
  const [autoFilters, setAutoFilters] = useState({
    dateFrom: "",
    dateTo: "",
  });
  const [queueRows, setQueueRows] = useState([]);
  const [queueTotal, setQueueTotal] = useState(0);
  const [selectedLineId, setSelectedLineId] = useState("");
  const [selectedLine, setSelectedLine] = useState(null);
  const [selectedMatches, setSelectedMatches] = useState([]);
  const [suggestions, setSuggestions] = useState([]);
  const [auditRows, setAuditRows] = useState([]);
  const [autoPreviewRows, setAutoPreviewRows] = useState([]);
  const [autoPreviewSummary, setAutoPreviewSummary] = useState(null);
  const [autoRunBusy, setAutoRunBusy] = useState(false);
  const [exceptions, setExceptions] = useState([]);
  const [exceptionsTotal, setExceptionsTotal] = useState(0);
  const [postingTemplates, setPostingTemplates] = useState([]);
  const [templateTotal, setTemplateTotal] = useState(0);
  const [loadingTemplates, setLoadingTemplates] = useState(false);
  const [templateSaving, setTemplateSaving] = useState(false);
  const [templateForm, setTemplateForm] = useState(EMPTY_B08_TEMPLATE_FORM);
  const [differenceProfiles, setDifferenceProfiles] = useState([]);
  const [differenceProfileTotal, setDifferenceProfileTotal] = useState(0);
  const [loadingDifferenceProfiles, setLoadingDifferenceProfiles] = useState(false);
  const [differenceProfileSaving, setDifferenceProfileSaving] = useState(false);
  const [differenceProfileForm, setDifferenceProfileForm] = useState(EMPTY_B08B_DIFF_PROFILE_FORM);
  const [returnEvents, setReturnEvents] = useState([]);
  const [returnEventsTotal, setReturnEventsTotal] = useState(0);
  const [loadingReturnEvents, setLoadingReturnEvents] = useState(false);
  const [returnSaving, setReturnSaving] = useState(false);
  const [returnForm, setReturnForm] = useState(EMPTY_B08B_RETURN_FORM);
  const [loadingExceptions, setLoadingExceptions] = useState(false);
  const [exceptionStatusFilter, setExceptionStatusFilter] = useState("OPEN");
  const [loadingQueue, setLoadingQueue] = useState(false);
  const [loadingDetails, setLoadingDetails] = useState(false);
  const [actionBusy, setActionBusy] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [templateError, setTemplateError] = useState("");
  const [templateMessage, setTemplateMessage] = useState("");
  const [b08bError, setB08bError] = useState("");
  const [b08bMessage, setB08bMessage] = useState("");
  const [lookupWarning, setLookupWarning] = useState("");

  const bankOptions = useMemo(
    () =>
      [...(bankAccounts || [])].sort((a, b) =>
        String(a?.code || "").localeCompare(String(b?.code || ""))
      ),
    [bankAccounts]
  );

  async function loadBankLookups() {
    if (!canReadBanks) {
      setBankAccounts([]);
      setLookupWarning("Missing permission: bank.accounts.read (bank filter optional)");
      return;
    }
    setLookupWarning("");
    try {
      const res = await listBankAccounts({ limit: 300, offset: 0 });
      setBankAccounts(res?.rows || []);
    } catch (err) {
      setBankAccounts([]);
      setLookupWarning(err?.response?.data?.message || "Bank account list could not be loaded");
    }
  }

  async function loadQueue({ preserveSelection = true } = {}) {
    if (!canRead) {
      setQueueRows([]);
      setQueueTotal(0);
      return [];
    }
    setLoadingQueue(true);
    setError("");
    try {
      const res = await listReconciliationQueue({
        limit: 200,
        offset: 0,
        bankAccountId: toPositiveInt(filters.bankAccountId) || undefined,
        reconStatus: filters.reconStatus || undefined,
        q: filters.q || undefined,
      });
      const rows = res?.rows || [];
      setQueueRows(rows);
      setQueueTotal(Number(res?.total || 0));
      if (!preserveSelection) {
        const nextId = rows[0]?.id ? String(rows[0].id) : "";
        setSelectedLineId(nextId);
      } else if (
        selectedLineId &&
        !rows.some((row) => String(row?.id || "") === String(selectedLineId))
      ) {
        setSelectedLineId(rows[0]?.id ? String(rows[0].id) : "");
      }
      return rows;
    } catch (err) {
      setError(err?.response?.data?.message || "Failed to load reconciliation queue");
      setQueueRows([]);
      setQueueTotal(0);
      return [];
    } finally {
      setLoadingQueue(false);
    }
  }

  async function loadExceptions() {
    if (!canReadExceptions) {
      setExceptions([]);
      setExceptionsTotal(0);
      return [];
    }
    setLoadingExceptions(true);
    try {
      const res = await listReconciliationExceptions({
        limit: 100,
        offset: 0,
        bankAccountId: toPositiveInt(filters.bankAccountId) || undefined,
        status: exceptionStatusFilter || undefined,
      });
      const rows = res?.rows || [];
      setExceptions(rows);
      setExceptionsTotal(Number(res?.total || 0));
      return rows;
    } catch (err) {
      setExceptions([]);
      setExceptionsTotal(0);
      setError(err?.response?.data?.message || "Failed to load B07 exception queue");
      return [];
    } finally {
      setLoadingExceptions(false);
    }
  }

  async function loadPostingTemplates() {
    if (!canReadTemplates) {
      setPostingTemplates([]);
      setTemplateTotal(0);
      return [];
    }
    setLoadingTemplates(true);
    setTemplateError("");
    try {
      const res = await listReconciliationPostingTemplates({
        limit: 100,
        offset: 0,
        bankAccountId: toPositiveInt(filters.bankAccountId) || undefined,
      });
      const rows = res?.rows || [];
      setPostingTemplates(rows);
      setTemplateTotal(Number(res?.total || 0));
      return rows;
    } catch (err) {
      setPostingTemplates([]);
      setTemplateTotal(0);
      setTemplateError(err?.response?.data?.message || "Failed to load B08 templates");
      return [];
    } finally {
      setLoadingTemplates(false);
    }
  }

  function resetTemplateForm({ preserveScope = true } = {}) {
    setTemplateForm((prev) => ({
      ...EMPTY_B08_TEMPLATE_FORM,
      legalEntityId: preserveScope ? prev.legalEntityId : "",
      bankAccountId: preserveScope ? prev.bankAccountId : "",
    }));
  }

  function startEditTemplate(row) {
    setTemplateError("");
    setTemplateMessage("");
    setTemplateForm(mapB08TemplateRowToForm(row));
  }

  function buildTemplatePayloadFromForm() {
    const scopeType = String(templateForm.scopeType || "LEGAL_ENTITY").toUpperCase();
    const bankAccountId = toPositiveInt(templateForm.bankAccountId);
    let legalEntityId = toPositiveInt(templateForm.legalEntityId);

    if (bankAccountId) {
      const selectedBank = bankAccounts.find((row) => toPositiveInt(row?.id) === bankAccountId);
      if (selectedBank?.legal_entity_id) {
        legalEntityId = toPositiveInt(selectedBank.legal_entity_id);
      }
    }

    const payload = {
      scopeType,
      legalEntityId: legalEntityId || undefined,
      bankAccountId: scopeType === "BANK_ACCOUNT" ? bankAccountId || undefined : undefined,
      templateCode: String(templateForm.templateCode || "").trim().toUpperCase(),
      templateName: String(templateForm.templateName || "").trim(),
      status: String(templateForm.status || "ACTIVE").trim().toUpperCase(),
      entryKind: String(templateForm.entryKind || "BANK_MISC").trim().toUpperCase(),
      directionPolicy: String(templateForm.directionPolicy || "BOTH").trim().toUpperCase(),
      counterAccountId: toPositiveInt(templateForm.counterAccountId) || undefined,
      descriptionMode: String(templateForm.descriptionMode || "USE_STATEMENT_TEXT").trim().toUpperCase(),
      fixedDescription: String(templateForm.fixedDescription || "").trim() || undefined,
      descriptionPrefix: String(templateForm.descriptionPrefix || "").trim() || undefined,
    };

    if (!payload.templateCode) throw new Error("templateCode is required");
    if (!payload.templateName) throw new Error("templateName is required");
    if (!payload.counterAccountId) throw new Error("counterAccountId is required");
    if (scopeType === "BANK_ACCOUNT" && !payload.bankAccountId) {
      throw new Error("bankAccountId is required for BANK_ACCOUNT scope");
    }
    if (!payload.legalEntityId) {
      throw new Error("legalEntityId is required (select a bank account or enter legalEntityId)");
    }

    return payload;
  }

  async function handleSaveTemplate(event) {
    event.preventDefault();
    if (!canWriteTemplates || templateSaving) return;
    setTemplateSaving(true);
    setTemplateError("");
    setTemplateMessage("");
    try {
      const payload = buildTemplatePayloadFromForm();
      if (templateForm.id) {
        await updateReconciliationPostingTemplate(templateForm.id, payload);
        setTemplateMessage("B08 template updated");
      } else {
        await createReconciliationPostingTemplate(payload);
        setTemplateMessage("B08 template created");
      }
      resetTemplateForm({ preserveScope: true });
      await loadPostingTemplates();
    } catch (err) {
      setTemplateError(err?.response?.data?.message || err?.message || "Template save failed");
    } finally {
      setTemplateSaving(false);
    }
  }

  async function loadDifferenceProfiles() {
    if (!canReadDiffProfiles) {
      setDifferenceProfiles([]);
      setDifferenceProfileTotal(0);
      return [];
    }
    setLoadingDifferenceProfiles(true);
    setB08bError("");
    try {
      const res = await listReconciliationDifferenceProfiles({
        limit: 100,
        offset: 0,
        bankAccountId: toPositiveInt(filters.bankAccountId) || undefined,
      });
      const rows = res?.rows || [];
      setDifferenceProfiles(rows);
      setDifferenceProfileTotal(Number(res?.total || 0));
      return rows;
    } catch (err) {
      setDifferenceProfiles([]);
      setDifferenceProfileTotal(0);
      setB08bError(err?.response?.data?.message || "Failed to load B08-B difference profiles");
      return [];
    } finally {
      setLoadingDifferenceProfiles(false);
    }
  }

  function resetDifferenceProfileForm({ preserveScope = true } = {}) {
    setDifferenceProfileForm((prev) => ({
      ...EMPTY_B08B_DIFF_PROFILE_FORM,
      legalEntityId: preserveScope ? prev.legalEntityId : "",
      bankAccountId: preserveScope ? prev.bankAccountId : "",
      differenceType: prev.differenceType || "FEE",
    }));
  }

  function startEditDifferenceProfile(row) {
    setB08bError("");
    setB08bMessage("");
    setDifferenceProfileForm(mapB08BDiffProfileRowToForm(row));
  }

  function buildDifferenceProfilePayloadFromForm() {
    const scopeType = String(differenceProfileForm.scopeType || "LEGAL_ENTITY").trim().toUpperCase();
    const bankAccountId = toPositiveInt(differenceProfileForm.bankAccountId) || undefined;
    let legalEntityId = toPositiveInt(differenceProfileForm.legalEntityId) || undefined;
    if (bankAccountId) {
      const selectedBank = bankAccounts.find((row) => toPositiveInt(row?.id) === bankAccountId);
      if (selectedBank?.legal_entity_id) {
        legalEntityId = toPositiveInt(selectedBank.legal_entity_id) || legalEntityId;
      }
    }
    const differenceType = String(differenceProfileForm.differenceType || "FEE").trim().toUpperCase();
    const payload = {
      scopeType,
      legalEntityId,
      bankAccountId: scopeType === "BANK_ACCOUNT" ? bankAccountId : undefined,
      profileCode: String(differenceProfileForm.profileCode || "").trim().toUpperCase(),
      profileName: String(differenceProfileForm.profileName || "").trim(),
      status: String(differenceProfileForm.status || "ACTIVE").trim().toUpperCase(),
      differenceType,
      directionPolicy: String(differenceProfileForm.directionPolicy || "BOTH").trim().toUpperCase(),
      maxAbsDifference:
        differenceProfileForm.maxAbsDifference === ""
          ? 0
          : Number(differenceProfileForm.maxAbsDifference),
      expenseAccountId: toPositiveInt(differenceProfileForm.expenseAccountId) || undefined,
      fxGainAccountId: toPositiveInt(differenceProfileForm.fxGainAccountId) || undefined,
      fxLossAccountId: toPositiveInt(differenceProfileForm.fxLossAccountId) || undefined,
      currencyCode: String(differenceProfileForm.currencyCode || "").trim().toUpperCase() || undefined,
      descriptionPrefix: String(differenceProfileForm.descriptionPrefix || "").trim() || undefined,
    };

    if (!payload.profileCode && !differenceProfileForm.id) {
      throw new Error("profileCode is required");
    }
    if (!payload.profileName) throw new Error("profileName is required");
    if (!(Number.isFinite(payload.maxAbsDifference) && payload.maxAbsDifference >= 0)) {
      throw new Error("maxAbsDifference must be >= 0");
    }
    if (!payload.legalEntityId) {
      throw new Error("legalEntityId is required (or select bank account)");
    }
    if (differenceType === "FEE" && !payload.expenseAccountId) {
      throw new Error("expenseAccountId is required for FEE profile");
    }
    if (differenceType === "FX" && (!payload.fxGainAccountId || !payload.fxLossAccountId)) {
      throw new Error("fxGainAccountId and fxLossAccountId are required for FX profile");
    }
    return payload;
  }

  async function handleSaveDifferenceProfile(event) {
    event.preventDefault();
    if (!canWriteDiffProfiles || differenceProfileSaving) return;
    setDifferenceProfileSaving(true);
    setB08bError("");
    setB08bMessage("");
    try {
      const payload = buildDifferenceProfilePayloadFromForm();
      if (differenceProfileForm.id) {
        await updateReconciliationDifferenceProfile(differenceProfileForm.id, payload);
        setB08bMessage("B08-B difference profile updated");
      } else {
        await createReconciliationDifferenceProfile(payload);
        setB08bMessage("B08-B difference profile created");
      }
      resetDifferenceProfileForm({ preserveScope: true });
      await loadDifferenceProfiles();
    } catch (err) {
      setB08bError(err?.response?.data?.message || err?.message || "Difference profile save failed");
    } finally {
      setDifferenceProfileSaving(false);
    }
  }

  async function loadReturnEvents() {
    if (!canReadReturnEvents) {
      setReturnEvents([]);
      setReturnEventsTotal(0);
      return [];
    }
    setLoadingReturnEvents(true);
    setB08bError("");
    try {
      const res = await listBankPaymentReturns({
        limit: 50,
        offset: 0,
        bankAccountId: toPositiveInt(filters.bankAccountId) || undefined,
        bankStatementLineId: toPositiveInt(selectedLineId) || undefined,
      });
      const rows = res?.rows || [];
      setReturnEvents(rows);
      setReturnEventsTotal(Number(res?.total || 0));
      return rows;
    } catch (err) {
      setReturnEvents([]);
      setReturnEventsTotal(0);
      setB08bError(err?.response?.data?.message || "Failed to load B08-B return events");
      return [];
    } finally {
      setLoadingReturnEvents(false);
    }
  }

  async function handleCreateManualReturnEvent(event) {
    event.preventDefault();
    if (!canWriteReturnEvents || returnSaving) return;
    setReturnSaving(true);
    setB08bError("");
    setB08bMessage("");
    try {
      const payload = {
        paymentBatchLineId: toPositiveInt(returnForm.paymentBatchLineId),
        bankStatementLineId: toPositiveInt(returnForm.bankStatementLineId) || undefined,
        eventType: String(returnForm.eventType || "PAYMENT_RETURNED").trim().toUpperCase(),
        amount: Number(returnForm.amount),
        currencyCode: String(returnForm.currencyCode || "").trim().toUpperCase(),
        reasonCode: String(returnForm.reasonCode || "").trim().toUpperCase() || undefined,
        reasonMessage: String(returnForm.reasonMessage || "").trim() || undefined,
      };
      if (!payload.paymentBatchLineId) throw new Error("paymentBatchLineId is required");
      if (!(Number.isFinite(payload.amount) && payload.amount > 0)) throw new Error("amount must be > 0");
      if (!payload.currencyCode) throw new Error("currencyCode is required");

      await createBankPaymentReturn(payload);
      setB08bMessage("B08-B return event created");
      await Promise.all([
        loadReturnEvents(),
        returnForm.bankStatementLineId ? refreshAfterAction(returnForm.bankStatementLineId) : Promise.resolve(),
      ]);
    } catch (err) {
      setB08bError(err?.response?.data?.message || err?.message || "Return event create failed");
    } finally {
      setReturnSaving(false);
    }
  }

  async function handleIgnoreReturnEventItem(eventId) {
    if (!canWriteReturnEvents || actionBusy) return;
    const note = window.prompt("Ignore reason (optional):", "") || "";
    setActionBusy(true);
    setB08bError("");
    try {
      await ignoreBankPaymentReturn(eventId, { reasonMessage: note || undefined });
      setB08bMessage("B08-B return event ignored");
      await loadReturnEvents();
    } catch (err) {
      setB08bError(err?.response?.data?.message || "Return event ignore failed");
    } finally {
      setActionBusy(false);
    }
  }

  async function loadLineDetails(lineId, queueLineFallback = null) {
    const parsedLineId = toPositiveInt(lineId);
    if (!parsedLineId || !canRead) {
      setSelectedLine(null);
      setSelectedMatches([]);
      setSuggestions([]);
      setAuditRows([]);
      return;
    }

    setLoadingDetails(true);
    setError("");
    try {
      const [suggestRes, auditRes] = await Promise.all([
        getReconciliationSuggestions(parsedLineId),
        listReconciliationAudit({ statementLineId: parsedLineId, limit: 100, offset: 0 }),
      ]);

      setSelectedLine(suggestRes?.line || queueLineFallback || null);
      setSelectedMatches(suggestRes?.matches || []);
      setSuggestions(suggestRes?.suggestions || []);
      setAuditRows(auditRes?.rows || []);
    } catch (err) {
      setSelectedLine(queueLineFallback || null);
      setSelectedMatches([]);
      setSuggestions([]);
      setAuditRows([]);
      setError(err?.response?.data?.message || "Failed to load reconciliation details");
    } finally {
      setLoadingDetails(false);
    }
  }

  async function refreshAfterAction(lineId) {
    const rows = await loadQueue({ preserveSelection: true });
    const updatedQueueLine =
      rows.find((row) => String(row?.id || "") === String(lineId || "")) || null;
    await loadLineDetails(lineId, updatedQueueLine);
    await loadExceptions();
  }

  async function handleAutoPreview() {
    if (!canAutoRun || autoRunBusy) return;
    setAutoRunBusy(true);
    setError("");
    setMessage("");
    try {
      const res = await previewReconciliationAutoRun({
        bankAccountId: toPositiveInt(filters.bankAccountId) || undefined,
        dateFrom: autoFilters.dateFrom || undefined,
        dateTo: autoFilters.dateTo || undefined,
        limit: 100,
      });
      setAutoPreviewRows(res?.rows || []);
      setAutoPreviewSummary(res?.summary || null);
      setMessage("B07 automation preview generated");
    } catch (err) {
      setError(err?.response?.data?.message || "B07 auto-preview failed");
    } finally {
      setAutoRunBusy(false);
    }
  }

  async function handleAutoApply() {
    if (!canAutoRun || autoRunBusy) return;
    if (!window.confirm("Run B07 auto-apply on current filters?")) return;
    setAutoRunBusy(true);
    setError("");
    setMessage("");
    try {
      const res = await applyReconciliationAutoRun({
        bankAccountId: toPositiveInt(filters.bankAccountId) || undefined,
        dateFrom: autoFilters.dateFrom || undefined,
        dateTo: autoFilters.dateTo || undefined,
        limit: 100,
        runRequestId: `b07-${Date.now()}`,
      });
      setAutoPreviewRows(res?.rows || []);
      setAutoPreviewSummary(res?.summary || null);
        const refreshedQueue = await loadQueue({ preserveSelection: true });
        await loadExceptions();
        if (selectedLineId) {
          const queueLine =
            refreshedQueue.find((row) => String(row?.id || "") === String(selectedLineId || "")) ||
            null;
          await loadLineDetails(selectedLineId, queueLine);
        }
      setMessage(
        `B07 auto-apply completed${res?.replay ? " (replay)" : ""}: reconciled ${
          res?.summary?.reconciledCount ?? 0
        }, exceptions ${res?.summary?.exceptionCount ?? 0}`
      );
    } catch (err) {
      setError(err?.response?.data?.message || "B07 auto-apply failed");
    } finally {
      setAutoRunBusy(false);
    }
  }

  async function handleAssignExceptionToMe(exceptionId) {
    if (!canWriteExceptions) return;
    setActionBusy(true);
    setError("");
    try {
      await assignReconciliationException(exceptionId, {});
      await loadExceptions();
      setMessage("Exception assigned");
    } catch (err) {
      setError(err?.response?.data?.message || "Exception assign failed");
    } finally {
      setActionBusy(false);
    }
  }

  async function handleResolveException(exceptionId) {
    if (!canWriteExceptions) return;
    const resolutionNote = window.prompt("Resolution note (optional):", "") || "";
    setActionBusy(true);
    setError("");
    try {
      await resolveReconciliationException(exceptionId, {
        resolutionCode: "RESOLVED_MANUALLY",
        resolutionNote,
      });
      await loadExceptions();
      setMessage("Exception resolved");
    } catch (err) {
      setError(err?.response?.data?.message || "Exception resolve failed");
    } finally {
      setActionBusy(false);
    }
  }

  async function handleIgnoreExceptionItem(exceptionId) {
    if (!canWriteExceptions) return;
    const resolutionNote = window.prompt("Ignore note (optional):", "") || "";
    setActionBusy(true);
    setError("");
    try {
      await ignoreReconciliationExceptionItem(exceptionId, { resolutionNote });
      await loadExceptions();
      setMessage("Exception ignored");
    } catch (err) {
      setError(err?.response?.data?.message || "Exception ignore failed");
    } finally {
      setActionBusy(false);
    }
  }

  async function handleRetryExceptionItem(exceptionId) {
    if (!canWriteExceptions) return;
    setActionBusy(true);
    setError("");
    try {
      await retryReconciliationException(exceptionId, {});
      await loadExceptions();
      setMessage("Exception reopened for retry");
    } catch (err) {
      setError(err?.response?.data?.message || "Exception retry failed");
    } finally {
      setActionBusy(false);
    }
  }

  async function handleMatchSuggestion(suggestion) {
    if (!selectedLine || !canWrite || actionBusy) {
      return;
    }
    const matchedAmount = Number(suggestion?.suggestedAmount || 0);
    if (!(matchedAmount > 0)) {
      setError("No remaining amount to match");
      return;
    }

    setActionBusy(true);
    setError("");
    setMessage("");
    try {
      await matchReconciliationLine(selectedLine.id, {
        matchType: "MANUAL",
        matchedEntityType: suggestion.matchedEntityType,
        matchedEntityId: suggestion.matchedEntityId,
        matchedAmount,
        notes: "Matched from suggestion",
      });
      setMessage("Reconciliation match created");
      await refreshAfterAction(selectedLine.id);
    } catch (err) {
      setError(err?.response?.data?.message || "Match failed");
    } finally {
      setActionBusy(false);
    }
  }

  async function handleUnmatchAll() {
    if (!selectedLine || !canWrite || actionBusy) {
      return;
    }
    setActionBusy(true);
    setError("");
    setMessage("");
    try {
      await unmatchReconciliationLine(selectedLine.id, {});
      setMessage("Active match(es) reversed");
      await refreshAfterAction(selectedLine.id);
    } catch (err) {
      setError(err?.response?.data?.message || "Unmatch failed");
    } finally {
      setActionBusy(false);
    }
  }

  async function handleIgnore() {
    if (!selectedLine || !canWrite || actionBusy) {
      return;
    }
    const reason = window.prompt("Ignore reason (optional):", "") || "";
    setActionBusy(true);
    setError("");
    setMessage("");
    try {
      await ignoreReconciliationLine(selectedLine.id, { reason });
      setMessage("Line marked as IGNORED");
      await refreshAfterAction(selectedLine.id);
    } catch (err) {
      setError(err?.response?.data?.message || "Ignore failed");
    } finally {
      setActionBusy(false);
    }
  }

  useEffect(() => {
    if (!canRead) {
      return;
    }
    loadQueue({ preserveSelection: false });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead]);

  useEffect(() => {
    loadExceptions();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadExceptions, exceptionStatusFilter, filters.bankAccountId]);

  useEffect(() => {
    loadPostingTemplates();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadTemplates, filters.bankAccountId]);

  useEffect(() => {
    loadDifferenceProfiles();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadDiffProfiles, filters.bankAccountId]);

  useEffect(() => {
    loadReturnEvents();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadReturnEvents, filters.bankAccountId, selectedLineId]);

  useEffect(() => {
    loadBankLookups();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadBanks]);

  useEffect(() => {
    const queueLine =
      queueRows.find((row) => String(row?.id || "") === String(selectedLineId || "")) || null;
    if (!selectedLineId) {
      setSelectedLine(null);
      setSelectedMatches([]);
      setSuggestions([]);
      setAuditRows([]);
      return;
    }
    loadLineDetails(selectedLineId, queueLine);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedLineId]);

  useEffect(() => {
    if (!selectedLine) return;
    setReturnForm((prev) => ({
      ...prev,
      bankStatementLineId: String(selectedLine.id || ""),
      amount:
        prev.amount && String(prev.bankStatementLineId || "") === String(selectedLine.id || "")
          ? prev.amount
          : String(Math.abs(Number(selectedLine.amount || 0)) || ""),
      currencyCode: prev.currencyCode || String(selectedLine.currency_code || ""),
    }));
  }, [selectedLine]);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">Banka Mutabakat</h1>
        <p className="mt-1 text-sm text-slate-600">
          PR-B03 manual reconciliation queue (suggestions, match/unmatch/ignore, audit).
        </p>
      </div>

      {!canRead ? (
        <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          Missing permission: <code>bank.reconcile.read</code>
        </div>
      ) : null}
      {!canWrite ? (
        <div className="rounded border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
          Read-only mode: <code>bank.reconcile.write</code> missing.
        </div>
      ) : null}
      {lookupWarning ? (
        <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          {lookupWarning}
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

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <form
          onSubmit={(event) => {
            event.preventDefault();
            loadQueue({ preserveSelection: false });
            loadExceptions();
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
            <label className="mb-1 block text-xs font-medium text-slate-700">Status</label>
            <select
              value={filters.reconStatus}
              onChange={(e) => setFilters((p) => ({ ...p, reconStatus: e.target.value }))}
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
            >
              <option value="">Queue default</option>
              <option value="UNMATCHED">UNMATCHED</option>
              <option value="PARTIAL">PARTIAL</option>
              <option value="MATCHED">MATCHED</option>
              <option value="IGNORED">IGNORED</option>
            </select>
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-700">Search</label>
            <input
              value={filters.q}
              onChange={(e) => setFilters((p) => ({ ...p, q: e.target.value }))}
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
              placeholder="Description / ref"
            />
          </div>
          <div className="flex items-end gap-2">
            <button
              type="submit"
              className="rounded bg-slate-900 px-3 py-2 text-sm font-medium text-white"
              disabled={!canRead || loadingQueue}
            >
              {loadingQueue ? "Yukleniyor..." : "Filtrele"}
            </button>
            <button
              type="button"
              onClick={() => loadQueue({ preserveSelection: true })}
              className="rounded border border-slate-300 px-3 py-2 text-sm text-slate-700"
              disabled={!canRead || loadingQueue}
            >
              Yenile
            </button>
          </div>
        </form>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="mb-3 flex items-center justify-between">
          <div>
            <h2 className="text-sm font-semibold text-slate-900">B07 Automation</h2>
            <p className="text-xs text-slate-500">
              Deterministic rule preview/apply on top of B03 reconciliation core.
            </p>
          </div>
          {!canAutoRun ? (
            <span className="text-xs text-slate-500">Missing: bank.reconcile.auto.run</span>
          ) : null}
        </div>
        <div className="grid gap-3 lg:grid-cols-[1fr_1fr_auto]">
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-700">Date From</label>
            <input
              type="date"
              value={autoFilters.dateFrom}
              onChange={(e) => setAutoFilters((p) => ({ ...p, dateFrom: e.target.value }))}
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-700">Date To</label>
            <input
              type="date"
              value={autoFilters.dateTo}
              onChange={(e) => setAutoFilters((p) => ({ ...p, dateTo: e.target.value }))}
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
            />
          </div>
          <div className="flex items-end gap-2">
            <button
              type="button"
              onClick={handleAutoPreview}
              disabled={!canAutoRun || autoRunBusy}
              className="rounded border border-slate-300 px-3 py-2 text-sm text-slate-700 disabled:opacity-50"
            >
              {autoRunBusy ? "..." : "Preview"}
            </button>
            <button
              type="button"
              onClick={handleAutoApply}
              disabled={!canAutoRun || autoRunBusy}
              className="rounded bg-slate-900 px-3 py-2 text-sm font-medium text-white disabled:opacity-50"
            >
              {autoRunBusy ? "..." : "Apply"}
            </button>
          </div>
        </div>
        {autoPreviewSummary ? (
          <div className="mt-3 rounded border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
            scanned {autoPreviewSummary.scannedCount || 0} | matched{" "}
            {autoPreviewSummary.matchedCount || 0} | reconciled{" "}
            {autoPreviewSummary.reconciledCount || 0} | exceptions{" "}
            {autoPreviewSummary.exceptionCount || 0} | rules{" "}
            {autoPreviewSummary.rulesEvaluated || 0}
          </div>
        ) : null}
        <div className="mt-3 max-h-48 overflow-auto">
          <table className="min-w-full text-left text-xs">
            <thead className="bg-slate-50 text-slate-600">
              <tr>
                <th className="px-2 py-2">Line</th>
                <th className="px-2 py-2">Outcome</th>
                <th className="px-2 py-2">Rule</th>
                <th className="px-2 py-2">Target</th>
              </tr>
            </thead>
            <tbody>
              {autoPreviewRows.length === 0 ? (
                <tr>
                  <td className="px-2 py-2 text-slate-500" colSpan={4}>
                    No B07 preview rows yet.
                  </td>
                </tr>
              ) : (
                autoPreviewRows.slice(0, 50).map((row) => (
                  <tr key={`b07-preview-${row.statementLineId}`} className="border-t">
                    <td className="px-2 py-2">
                      #{row.statementLineId} {formatDate(row.txnDate)}
                    </td>
                    <td className="px-2 py-2">{row.outcome}</td>
                    <td className="px-2 py-2">{row.rule?.ruleCode || "-"}</td>
                    <td className="px-2 py-2">
                      {row.target?.entityType ? `${row.target.entityType}/${row.target.entityId}` : "-"}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="mb-3 flex items-start justify-between gap-3">
          <div>
            <h2 className="text-sm font-semibold text-slate-900">B08 Auto-Posting Templates</h2>
            <p className="text-xs text-slate-500">
              Bank fees/charges/interest templates used by B07 rule action{" "}
              <code>AUTO_POST_TEMPLATE</code>. Rule <code>actionPayload</code> should include{" "}
              <code>postingTemplateId</code>.
            </p>
          </div>
          <button
            type="button"
            onClick={() => loadPostingTemplates()}
            className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 disabled:opacity-50"
            disabled={!canReadTemplates || loadingTemplates}
          >
            {loadingTemplates ? "..." : "Refresh"}
          </button>
        </div>

        {!canReadTemplates ? (
          <div className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
            Missing permission: <code>bank.reconcile.templates.read</code>
          </div>
        ) : null}

        {templateError ? (
          <div className="mb-3 rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
            {templateError}
          </div>
        ) : null}
        {templateMessage ? (
          <div className="mb-3 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
            {templateMessage}
          </div>
        ) : null}

        <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,1.35fr)]">
          <form onSubmit={handleSaveTemplate} className="space-y-3 rounded border border-slate-200 p-3">
            <div className="flex items-center justify-between">
              <div className="text-sm font-medium text-slate-900">
                {templateForm.id ? `Edit Template #${templateForm.id}` : "New Template"}
              </div>
              <button
                type="button"
                onClick={() => {
                  resetTemplateForm({ preserveScope: true });
                  setTemplateError("");
                  setTemplateMessage("");
                }}
                className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700"
                disabled={templateSaving}
              >
                Clear
              </button>
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Scope</label>
                <select
                  value={templateForm.scopeType}
                  onChange={(e) =>
                    setTemplateForm((p) => ({
                      ...p,
                      scopeType: e.target.value,
                      bankAccountId: e.target.value === "BANK_ACCOUNT" ? p.bankAccountId : "",
                    }))
                  }
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canWriteTemplates || templateSaving}
                >
                  <option value="LEGAL_ENTITY">LEGAL_ENTITY</option>
                  <option value="BANK_ACCOUNT">BANK_ACCOUNT</option>
                  <option value="GLOBAL">GLOBAL (LE-anchored)</option>
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Status</label>
                <select
                  value={templateForm.status}
                  onChange={(e) => setTemplateForm((p) => ({ ...p, status: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canWriteTemplates || templateSaving}
                >
                  <option value="ACTIVE">ACTIVE</option>
                  <option value="PAUSED">PAUSED</option>
                  <option value="DISABLED">DISABLED</option>
                </select>
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Bank Account (optional / required for BANK_ACCOUNT scope)</label>
                <select
                  value={templateForm.bankAccountId}
                  onChange={(e) => {
                    const nextBankAccountId = e.target.value;
                    const selected = bankAccounts.find(
                      (row) => String(row?.id || "") === String(nextBankAccountId || "")
                    );
                    setTemplateForm((p) => ({
                      ...p,
                      bankAccountId: nextBankAccountId,
                      legalEntityId: selected?.legal_entity_id ? String(selected.legal_entity_id) : p.legalEntityId,
                    }));
                  }}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canWriteTemplates || templateSaving || !canReadBanks}
                >
                  <option value="">(none)</option>
                  {bankOptions.map((row) => (
                    <option key={`b08-tpl-bank-${row.id}`} value={row.id}>
                      {row.code} - {row.name}
                    </option>
                  ))}
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Legal Entity Id</label>
                <input
                  type="number"
                  min="1"
                  value={templateForm.legalEntityId}
                  onChange={(e) => setTemplateForm((p) => ({ ...p, legalEntityId: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="Required in this repo (v1)"
                  disabled={!canWriteTemplates || templateSaving}
                />
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Template Code</label>
                <input
                  value={templateForm.templateCode}
                  onChange={(e) => setTemplateForm((p) => ({ ...p, templateCode: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="BANK_FEE_TRY"
                  disabled={!canWriteTemplates || templateSaving || Boolean(templateForm.id)}
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Template Name</label>
                <input
                  value={templateForm.templateName}
                  onChange={(e) => setTemplateForm((p) => ({ ...p, templateName: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="Bank Fee Expense"
                  disabled={!canWriteTemplates || templateSaving}
                />
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-3">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Entry Kind</label>
                <input
                  value={templateForm.entryKind}
                  onChange={(e) => setTemplateForm((p) => ({ ...p, entryKind: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="BANK_MISC"
                  disabled={!canWriteTemplates || templateSaving}
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Direction</label>
                <select
                  value={templateForm.directionPolicy}
                  onChange={(e) => setTemplateForm((p) => ({ ...p, directionPolicy: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canWriteTemplates || templateSaving}
                >
                  <option value="BOTH">BOTH</option>
                  <option value="OUTFLOW_ONLY">OUTFLOW_ONLY</option>
                  <option value="INFLOW_ONLY">INFLOW_ONLY</option>
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Counter GL Account Id</label>
                <input
                  type="number"
                  min="1"
                  value={templateForm.counterAccountId}
                  onChange={(e) => setTemplateForm((p) => ({ ...p, counterAccountId: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder="e.g. 770 leaf account id"
                  disabled={!canWriteTemplates || templateSaving}
                />
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-[1fr_2fr]">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Description Mode</label>
                <select
                  value={templateForm.descriptionMode}
                  onChange={(e) => setTemplateForm((p) => ({ ...p, descriptionMode: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canWriteTemplates || templateSaving}
                >
                  <option value="USE_STATEMENT_TEXT">USE_STATEMENT_TEXT</option>
                  <option value="FIXED_TEXT">FIXED_TEXT</option>
                  <option value="PREFIXED">PREFIXED</option>
                </select>
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">
                  {templateForm.descriptionMode === "FIXED_TEXT"
                    ? "Fixed Description"
                    : templateForm.descriptionMode === "PREFIXED"
                      ? "Description Prefix"
                      : "Optional Text Override"}
                </label>
                <input
                  value={
                    templateForm.descriptionMode === "PREFIXED"
                      ? templateForm.descriptionPrefix
                      : templateForm.fixedDescription
                  }
                  onChange={(e) =>
                    setTemplateForm((p) =>
                      p.descriptionMode === "PREFIXED"
                        ? { ...p, descriptionPrefix: e.target.value }
                        : { ...p, fixedDescription: e.target.value }
                    )
                  }
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  placeholder={
                    templateForm.descriptionMode === "USE_STATEMENT_TEXT"
                      ? "Leave empty"
                      : templateForm.descriptionMode === "FIXED_TEXT"
                        ? "Bank service fee"
                        : "Bank Fee:"
                  }
                  disabled={!canWriteTemplates || templateSaving}
                />
              </div>
            </div>

            {!canWriteTemplates ? (
              <div className="rounded border border-slate-200 bg-slate-50 px-2 py-2 text-xs text-slate-600">
                Read-only: <code>bank.reconcile.templates.write</code> missing.
              </div>
            ) : null}

            <button
              type="submit"
              disabled={!canWriteTemplates || templateSaving}
              className="w-full rounded bg-slate-900 px-3 py-2 text-sm font-medium text-white disabled:opacity-50"
            >
              {templateSaving ? "Saving..." : templateForm.id ? "Update Template" : "Create Template"}
            </button>
          </form>

          <div className="rounded border border-slate-200 p-3">
            <div className="mb-2 flex items-center justify-between">
              <div className="text-sm font-medium text-slate-900">
                Templates ({loadingTemplates ? "..." : `${postingTemplates.length} / ${templateTotal}`})
              </div>
              <div className="text-xs text-slate-500">
                Filtered by selected bank account if set
              </div>
            </div>
            <div className="max-h-80 overflow-auto">
              <table className="min-w-full text-left text-xs">
                <thead className="sticky top-0 bg-slate-50 text-slate-600">
                  <tr>
                    <th className="px-2 py-2">Code</th>
                    <th className="px-2 py-2">Scope</th>
                    <th className="px-2 py-2">Counter</th>
                    <th className="px-2 py-2">Dir</th>
                    <th className="px-2 py-2">Status</th>
                    <th className="px-2 py-2">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {postingTemplates.length === 0 ? (
                    <tr>
                      <td className="px-2 py-2 text-slate-500" colSpan={6}>
                        {loadingTemplates ? "Loading..." : "No B08 templates."}
                      </td>
                    </tr>
                  ) : (
                    postingTemplates.map((row) => (
                      <tr key={`b08-template-${row.id}`} className="border-t">
                        <td className="px-2 py-2">
                          <div className="font-medium text-slate-900">{row.template_code}</div>
                          <div className="max-w-[220px] truncate text-slate-500" title={row.template_name}>
                            {row.template_name}
                          </div>
                        </td>
                        <td className="px-2 py-2">
                          <div>{row.scope_type}</div>
                          <div className="text-slate-500">
                            {row.bank_account_code || row.legal_entity_code || "-"}
                          </div>
                        </td>
                        <td className="px-2 py-2">
                          {row.counter_account_code || row.counter_account_id || "-"}
                          {row.counter_account_name ? (
                            <div className="max-w-[180px] truncate text-slate-500" title={row.counter_account_name}>
                              {row.counter_account_name}
                            </div>
                          ) : null}
                        </td>
                        <td className="px-2 py-2">{row.direction_policy}</td>
                        <td className="px-2 py-2">{row.status}</td>
                        <td className="px-2 py-2">
                          <button
                            type="button"
                            onClick={() => startEditTemplate(row)}
                            disabled={!canReadTemplates}
                            className="rounded border border-slate-300 px-2 py-1 text-[11px] text-slate-700 disabled:opacity-50"
                          >
                            Edit
                          </button>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="mb-3 flex items-start justify-between gap-3">
          <div>
            <h2 className="text-sm font-semibold text-slate-900">B08-B Returns / Differences</h2>
            <p className="text-xs text-slate-500">
              Difference profiles for B07 action <code>AUTO_MATCH_PAYMENT_LINE_WITH_DIFFERENCE</code> and
              manual bank return/rejection events.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => {
                loadDifferenceProfiles();
                loadReturnEvents();
              }}
              className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 disabled:opacity-50"
              disabled={loadingDifferenceProfiles || loadingReturnEvents}
            >
              {loadingDifferenceProfiles || loadingReturnEvents ? "..." : "Refresh"}
            </button>
          </div>
        </div>

        {b08bError ? (
          <div className="mb-3 rounded border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
            {b08bError}
          </div>
        ) : null}
        {b08bMessage ? (
          <div className="mb-3 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
            {b08bMessage}
          </div>
        ) : null}

        <div className="grid gap-4 xl:grid-cols-2">
          <div className="rounded border border-slate-200 p-3">
            <div className="mb-2 flex items-center justify-between">
              <div className="text-sm font-medium text-slate-900">
                Difference Profiles ({loadingDifferenceProfiles ? "..." : `${differenceProfiles.length} / ${differenceProfileTotal}`})
              </div>
              {!canReadDiffProfiles ? (
                <span className="text-xs text-slate-500">Missing: bank.reconcile.diffprofiles.read</span>
              ) : null}
            </div>

            {canReadDiffProfiles ? (
              <div className="max-h-48 overflow-auto">
                <table className="min-w-full text-left text-xs">
                  <thead className="sticky top-0 bg-slate-50 text-slate-600">
                    <tr>
                      <th className="px-2 py-2">Code</th>
                      <th className="px-2 py-2">Type</th>
                      <th className="px-2 py-2">Max Diff</th>
                      <th className="px-2 py-2">Scope</th>
                      <th className="px-2 py-2">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {differenceProfiles.length === 0 ? (
                      <tr>
                        <td className="px-2 py-2 text-slate-500" colSpan={5}>
                          {loadingDifferenceProfiles ? "Loading..." : "No B08-B difference profiles."}
                        </td>
                      </tr>
                    ) : (
                      differenceProfiles.map((row) => (
                        <tr key={`b08b-dp-${row.id}`} className="border-t">
                          <td className="px-2 py-2">
                            <div className="font-medium text-slate-900">{row.profile_code}</div>
                            <div className="max-w-[180px] truncate text-slate-500" title={row.profile_name}>
                              {row.profile_name}
                            </div>
                          </td>
                          <td className="px-2 py-2">
                            {row.difference_type}
                            <div className="text-slate-500">{row.status}</div>
                          </td>
                          <td className="px-2 py-2">{formatAmount(row.max_abs_difference)}</td>
                          <td className="px-2 py-2">
                            <div>{row.scope_type}</div>
                            <div className="text-slate-500">{row.bank_account_code || row.legal_entity_code || "-"}</div>
                          </td>
                          <td className="px-2 py-2">
                            <button
                              type="button"
                              onClick={() => startEditDifferenceProfile(row)}
                              className="rounded border border-slate-300 px-2 py-1 text-[11px] text-slate-700"
                              disabled={!canReadDiffProfiles}
                            >
                              Edit
                            </button>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            ) : null}

            <form onSubmit={handleSaveDifferenceProfile} className="mt-3 space-y-3 rounded border border-slate-200 p-3">
              <div className="flex items-center justify-between">
                <div className="text-sm font-medium text-slate-900">
                  {differenceProfileForm.id ? `Edit Profile #${differenceProfileForm.id}` : "New Difference Profile"}
                </div>
                <button
                  type="button"
                  onClick={() => resetDifferenceProfileForm({ preserveScope: true })}
                  className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700"
                >
                  Clear
                </button>
              </div>

              <div className="grid gap-3 sm:grid-cols-2">
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Scope</label>
                  <select
                    value={differenceProfileForm.scopeType}
                    onChange={(e) =>
                      setDifferenceProfileForm((p) => ({
                        ...p,
                        scopeType: e.target.value,
                        bankAccountId: e.target.value === "BANK_ACCOUNT" ? p.bankAccountId : "",
                      }))
                    }
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteDiffProfiles || differenceProfileSaving}
                  >
                    <option value="LEGAL_ENTITY">LEGAL_ENTITY</option>
                    <option value="BANK_ACCOUNT">BANK_ACCOUNT</option>
                    <option value="GLOBAL">GLOBAL (LE-anchored)</option>
                  </select>
                </div>
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Status</label>
                  <select
                    value={differenceProfileForm.status}
                    onChange={(e) => setDifferenceProfileForm((p) => ({ ...p, status: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteDiffProfiles || differenceProfileSaving}
                  >
                    <option value="ACTIVE">ACTIVE</option>
                    <option value="PAUSED">PAUSED</option>
                    <option value="DISABLED">DISABLED</option>
                  </select>
                </div>
              </div>

              <div className="grid gap-3 sm:grid-cols-2">
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Bank Account</label>
                  <select
                    value={differenceProfileForm.bankAccountId}
                    onChange={(e) => {
                      const nextBankAccountId = e.target.value;
                      const selected = bankAccounts.find((row) => String(row?.id || "") === String(nextBankAccountId || ""));
                      setDifferenceProfileForm((p) => ({
                        ...p,
                        bankAccountId: nextBankAccountId,
                        legalEntityId: selected?.legal_entity_id ? String(selected.legal_entity_id) : p.legalEntityId,
                      }));
                    }}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteDiffProfiles || differenceProfileSaving || !canReadBanks}
                  >
                    <option value="">(none)</option>
                    {bankOptions.map((row) => (
                      <option key={`b08b-dp-bank-${row.id}`} value={row.id}>
                        {row.code} - {row.name}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Legal Entity Id</label>
                  <input
                    type="number"
                    min="1"
                    value={differenceProfileForm.legalEntityId}
                    onChange={(e) => setDifferenceProfileForm((p) => ({ ...p, legalEntityId: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteDiffProfiles || differenceProfileSaving}
                  />
                </div>
              </div>

              <div className="grid gap-3 sm:grid-cols-2">
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Profile Code</label>
                  <input
                    value={differenceProfileForm.profileCode}
                    onChange={(e) => setDifferenceProfileForm((p) => ({ ...p, profileCode: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteDiffProfiles || differenceProfileSaving || Boolean(differenceProfileForm.id)}
                    placeholder="FX_DIFF_TRY"
                  />
                </div>
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Profile Name</label>
                  <input
                    value={differenceProfileForm.profileName}
                    onChange={(e) => setDifferenceProfileForm((p) => ({ ...p, profileName: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteDiffProfiles || differenceProfileSaving}
                  />
                </div>
              </div>

              <div className="grid gap-3 sm:grid-cols-4">
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Type</label>
                  <select
                    value={differenceProfileForm.differenceType}
                    onChange={(e) => setDifferenceProfileForm((p) => ({ ...p, differenceType: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteDiffProfiles || differenceProfileSaving}
                  >
                    <option value="FEE">FEE</option>
                    <option value="FX">FX</option>
                  </select>
                </div>
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Direction</label>
                  <select
                    value={differenceProfileForm.directionPolicy}
                    onChange={(e) => setDifferenceProfileForm((p) => ({ ...p, directionPolicy: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteDiffProfiles || differenceProfileSaving}
                  >
                    <option value="BOTH">BOTH</option>
                    <option value="INCREASE_ONLY">INCREASE_ONLY</option>
                    <option value="DECREASE_ONLY">DECREASE_ONLY</option>
                  </select>
                </div>
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Max Diff</label>
                  <input
                    value={differenceProfileForm.maxAbsDifference}
                    onChange={(e) => setDifferenceProfileForm((p) => ({ ...p, maxAbsDifference: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteDiffProfiles || differenceProfileSaving}
                  />
                </div>
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Currency (opt.)</label>
                  <input
                    value={differenceProfileForm.currencyCode}
                    onChange={(e) => setDifferenceProfileForm((p) => ({ ...p, currencyCode: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteDiffProfiles || differenceProfileSaving}
                    placeholder="TRY"
                  />
                </div>
              </div>

              {differenceProfileForm.differenceType === "FEE" ? (
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Expense GL Account Id</label>
                  <input
                    type="number"
                    min="1"
                    value={differenceProfileForm.expenseAccountId}
                    onChange={(e) => setDifferenceProfileForm((p) => ({ ...p, expenseAccountId: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteDiffProfiles || differenceProfileSaving}
                  />
                </div>
              ) : (
                <div className="grid gap-3 sm:grid-cols-2">
                  <div>
                    <label className="mb-1 block text-xs font-medium text-slate-700">FX Gain GL Account Id</label>
                    <input
                      type="number"
                      min="1"
                      value={differenceProfileForm.fxGainAccountId}
                      onChange={(e) => setDifferenceProfileForm((p) => ({ ...p, fxGainAccountId: e.target.value }))}
                      className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                      disabled={!canWriteDiffProfiles || differenceProfileSaving}
                    />
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-slate-700">FX Loss GL Account Id</label>
                    <input
                      type="number"
                      min="1"
                      value={differenceProfileForm.fxLossAccountId}
                      onChange={(e) => setDifferenceProfileForm((p) => ({ ...p, fxLossAccountId: e.target.value }))}
                      className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                      disabled={!canWriteDiffProfiles || differenceProfileSaving}
                    />
                  </div>
                </div>
              )}

              <div>
                <label className="mb-1 block text-xs font-medium text-slate-700">Description Prefix (opt.)</label>
                <input
                  value={differenceProfileForm.descriptionPrefix}
                  onChange={(e) => setDifferenceProfileForm((p) => ({ ...p, descriptionPrefix: e.target.value }))}
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                  disabled={!canWriteDiffProfiles || differenceProfileSaving}
                  placeholder="Bank FX diff"
                />
              </div>

              {!canWriteDiffProfiles ? (
                <div className="rounded border border-slate-200 bg-slate-50 px-2 py-2 text-xs text-slate-600">
                  Read-only: <code>bank.reconcile.diffprofiles.write</code> missing.
                </div>
              ) : null}

              <button
                type="submit"
                disabled={!canWriteDiffProfiles || differenceProfileSaving}
                className="w-full rounded bg-slate-900 px-3 py-2 text-sm font-medium text-white disabled:opacity-50"
              >
                {differenceProfileSaving
                  ? "Saving..."
                  : differenceProfileForm.id
                    ? "Update Difference Profile"
                    : "Create Difference Profile"}
              </button>
            </form>
          </div>

          <div className="rounded border border-slate-200 p-3">
            <div className="mb-2 flex items-center justify-between">
              <div className="text-sm font-medium text-slate-900">
                Return Events ({loadingReturnEvents ? "..." : `${returnEvents.length} / ${returnEventsTotal}`})
              </div>
              {!canReadReturnEvents ? (
                <span className="text-xs text-slate-500">Missing: bank.payments.returns.read</span>
              ) : (
                <span className="text-xs text-slate-500">Filtered by selected bank / line</span>
              )}
            </div>

            {canReadReturnEvents ? (
              <div className="max-h-52 overflow-auto">
                <table className="min-w-full text-left text-xs">
                  <thead className="sticky top-0 bg-slate-50 text-slate-600">
                    <tr>
                      <th className="px-2 py-2">Event</th>
                      <th className="px-2 py-2">Payment Line</th>
                      <th className="px-2 py-2">Amount</th>
                      <th className="px-2 py-2">Status</th>
                      <th className="px-2 py-2">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {returnEvents.length === 0 ? (
                      <tr>
                        <td className="px-2 py-2 text-slate-500" colSpan={5}>
                          {loadingReturnEvents ? "Loading..." : "No B08-B return events."}
                        </td>
                      </tr>
                    ) : (
                      returnEvents.map((row) => (
                        <tr key={`b08b-ret-${row.id}`} className="border-t">
                          <td className="px-2 py-2">
                            <div className="font-medium text-slate-900">{row.event_type}</div>
                            <div className="text-slate-500">{row.source_type} / #{row.id}</div>
                            <div className="max-w-[180px] truncate text-slate-500" title={row.reason_message || ""}>
                              {row.reason_code || row.reason_message || "-"}
                            </div>
                          </td>
                          <td className="px-2 py-2">
                            <div>Batch {row.batch_no || row.payment_batch_id}</div>
                            <div className="text-slate-500">Line {row.payment_batch_line_no || row.payment_batch_line_id}</div>
                          </td>
                          <td className="px-2 py-2">
                            {formatAmount(row.amount)} {row.currency_code}
                          </td>
                          <td className="px-2 py-2">
                            <div>{row.event_status}</div>
                            <div className="text-slate-500">{row.return_status || "-"}</div>
                          </td>
                          <td className="px-2 py-2">
                            <button
                              type="button"
                              onClick={() => handleIgnoreReturnEventItem(row.id)}
                              disabled={!canWriteReturnEvents || actionBusy || row.event_status === "IGNORED"}
                              className="rounded border border-slate-300 px-2 py-1 text-[11px] text-slate-700 disabled:opacity-50"
                            >
                              Ignore
                            </button>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            ) : null}

            <form onSubmit={handleCreateManualReturnEvent} className="mt-3 space-y-3 rounded border border-slate-200 p-3">
              <div className="text-sm font-medium text-slate-900">Manual Return / Rejection Event</div>
              <div className="grid gap-3 sm:grid-cols-2">
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Payment Batch Line Id</label>
                  <input
                    type="number"
                    min="1"
                    value={returnForm.paymentBatchLineId}
                    onChange={(e) => setReturnForm((p) => ({ ...p, paymentBatchLineId: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteReturnEvents || returnSaving}
                    placeholder="Required"
                  />
                </div>
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Statement Line Id (opt.)</label>
                  <input
                    type="number"
                    min="1"
                    value={returnForm.bankStatementLineId}
                    onChange={(e) => setReturnForm((p) => ({ ...p, bankStatementLineId: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteReturnEvents || returnSaving}
                    placeholder={selectedLine ? `Selected: ${selectedLine.id}` : ""}
                  />
                </div>
              </div>
              <div className="grid gap-3 sm:grid-cols-3">
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Event Type</label>
                  <select
                    value={returnForm.eventType}
                    onChange={(e) => setReturnForm((p) => ({ ...p, eventType: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteReturnEvents || returnSaving}
                  >
                    <option value="PAYMENT_RETURNED">PAYMENT_RETURNED</option>
                    <option value="PAYMENT_REJECTED">PAYMENT_REJECTED</option>
                    <option value="PAYMENT_REVERSAL">PAYMENT_REVERSAL</option>
                  </select>
                </div>
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Amount</label>
                  <input
                    value={returnForm.amount}
                    onChange={(e) => setReturnForm((p) => ({ ...p, amount: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteReturnEvents || returnSaving}
                    placeholder="e.g. 100.00"
                  />
                </div>
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Currency</label>
                  <input
                    value={returnForm.currencyCode}
                    onChange={(e) => setReturnForm((p) => ({ ...p, currencyCode: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteReturnEvents || returnSaving}
                    placeholder="TRY"
                  />
                </div>
              </div>
              <div className="grid gap-3 sm:grid-cols-2">
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Reason Code (opt.)</label>
                  <input
                    value={returnForm.reasonCode}
                    onChange={(e) => setReturnForm((p) => ({ ...p, reasonCode: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteReturnEvents || returnSaving}
                  />
                </div>
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-700">Reason Message (opt.)</label>
                  <input
                    value={returnForm.reasonMessage}
                    onChange={(e) => setReturnForm((p) => ({ ...p, reasonMessage: e.target.value }))}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
                    disabled={!canWriteReturnEvents || returnSaving}
                  />
                </div>
              </div>
              {!canWriteReturnEvents ? (
                <div className="rounded border border-slate-200 bg-slate-50 px-2 py-2 text-xs text-slate-600">
                  Read-only: <code>bank.payments.returns.write</code> missing.
                </div>
              ) : null}
              <button
                type="submit"
                disabled={!canWriteReturnEvents || returnSaving}
                className="w-full rounded bg-slate-900 px-3 py-2 text-sm font-medium text-white disabled:opacity-50"
              >
                {returnSaving ? "Saving..." : "Create Return Event"}
              </button>
            </form>
          </div>
        </div>
      </section>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,1.4fr)]">
        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-sm font-semibold text-slate-900">Queue</h2>
            <span className="text-xs text-slate-500">
              {loadingQueue ? "Yukleniyor..." : `${queueRows.length} / ${queueTotal}`}
            </span>
          </div>
          <div className="max-h-[620px] overflow-auto">
            <table className="min-w-full text-left text-xs">
              <thead className="sticky top-0 bg-slate-50 text-slate-600">
                <tr>
                  <th className="px-2 py-2">Date</th>
                  <th className="px-2 py-2">Desc</th>
                  <th className="px-2 py-2">Amount</th>
                  <th className="px-2 py-2">Status</th>
                  <th className="px-2 py-2">Matched</th>
                </tr>
              </thead>
              <tbody>
                {queueRows.length === 0 ? (
                  <tr>
                    <td className="px-2 py-3 text-slate-500" colSpan={5}>
                      No reconciliation items.
                    </td>
                  </tr>
                ) : (
                  queueRows.map((row) => {
                    const active = String(row?.id || "") === String(selectedLineId || "");
                    return (
                      <tr
                        key={row.id}
                        className={`cursor-pointer border-t ${
                          active ? "bg-slate-100" : "hover:bg-slate-50"
                        }`}
                        onClick={() => setSelectedLineId(String(row.id))}
                      >
                        <td className="px-2 py-2">
                          <div>{formatDate(row.txn_date)}</div>
                          <div className="text-[11px] text-slate-500">{row.bank_account_code}</div>
                        </td>
                        <td className="px-2 py-2">
                          <div className="max-w-[240px] truncate" title={row.description}>
                            {row.description}
                          </div>
                          <div className="text-[11px] text-slate-500">{row.reference_no || "-"}</div>
                        </td>
                        <td className="px-2 py-2">
                          {formatAmount(row.amount)} {row.currency_code}
                        </td>
                        <td className="px-2 py-2">{row.recon_status}</td>
                        <td className="px-2 py-2">
                          {formatAmount(row.active_matched_total)} ({row.active_match_count || 0})
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-sm font-semibold text-slate-900">Line Details</h2>
            {loadingDetails ? <span className="text-xs text-slate-500">Yukleniyor...</span> : null}
          </div>

          {!selectedLine ? (
            <div className="text-sm text-slate-600">Queue satiri secin.</div>
          ) : (
            <div className="space-y-4 text-sm">
              <div className="rounded border border-slate-200 bg-slate-50 p-3">
                <div>
                  <strong>Date:</strong> {formatDate(selectedLine.txn_date)}
                </div>
                <div>
                  <strong>Description:</strong> {selectedLine.description}
                </div>
                <div>
                  <strong>Reference:</strong> {selectedLine.reference_no || "-"}
                </div>
                <div>
                  <strong>Amount:</strong> {formatAmount(selectedLine.amount)}{" "}
                  {selectedLine.currency_code}
                </div>
                <div>
                  <strong>Status:</strong> {selectedLine.recon_status}
                </div>
                <div>
                  <strong>B07 Meta:</strong>{" "}
                  {selectedLine.reconciliation_method || selectedLine.reconciliation_rule_id
                    ? `${selectedLine.reconciliation_method || "-"} / rule ${
                        selectedLine.reconciliation_rule_id || "-"
                      } / conf ${selectedLine.reconciliation_confidence ?? "-"}`
                    : "-"}
                </div>
                <div>
                  <strong>B08 Auto-Post:</strong>{" "}
                  {selectedLine.auto_post_template_id || selectedLine.auto_post_journal_entry_id
                    ? `tpl ${selectedLine.auto_post_template_id || "-"} / JE ${
                        selectedLine.auto_post_journal_entry_id || "-"
                      }`
                    : "-"}
                </div>
                <div>
                  <strong>B08-B Diff:</strong>{" "}
                  {selectedLine.reconciliation_difference_type ||
                  selectedLine.reconciliation_difference_journal_entry_id
                    ? `${selectedLine.reconciliation_difference_type || "-"} / amt ${
                        selectedLine.reconciliation_difference_amount ?? "-"
                      } / profile ${selectedLine.reconciliation_difference_profile_id || "-"} / JE ${
                        selectedLine.reconciliation_difference_journal_entry_id || "-"
                      }`
                    : "-"}
                </div>
                <div>
                  <strong>Bank:</strong> {selectedLine.bank_account_code || "-"}
                </div>
              </div>

              <div>
                <div className="mb-2 flex items-center justify-between">
                  <div className="font-medium text-slate-900">Active Matches</div>
                  <button
                    type="button"
                    onClick={handleUnmatchAll}
                    disabled={!canWrite || actionBusy || selectedMatches.length === 0}
                    className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 disabled:opacity-50"
                  >
                    {actionBusy ? "..." : "Unmatch all"}
                  </button>
                </div>
                {selectedMatches.length === 0 ? (
                  <div className="text-xs text-slate-500">No active matches.</div>
                ) : (
                  <div className="space-y-2">
                    {selectedMatches.map((row) => (
                      <div key={row.id} className="rounded border border-slate-200 p-2 text-xs">
                        <div>
                          <strong>#{row.id}</strong> {row.matched_entity_type} / {row.matched_entity_id}
                        </div>
                        <div>
                          Amount: {formatAmount(row.matched_amount)} | Type: {row.match_type}
                        </div>
                        {(row.reconciliation_rule_id ||
                          (row.reconciliation_confidence !== undefined &&
                            row.reconciliation_confidence !== null)) && (
                          <div className="text-slate-500">
                            Rule: {row.reconciliation_rule_id || "-"} | Confidence:{" "}
                            {row.reconciliation_confidence ?? "-"}
                          </div>
                        )}
                        <div className="text-slate-500">
                          {row.notes || "-"} | {formatDateTime(row.matched_at)}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div>
                <div className="mb-2 flex items-center justify-between">
                  <div className="font-medium text-slate-900">Suggestions (Journal v1)</div>
                  <button
                    type="button"
                    onClick={handleIgnore}
                    disabled={!canWrite || actionBusy || selectedLine.recon_status === "IGNORED"}
                    className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 disabled:opacity-50"
                  >
                    {actionBusy ? "..." : "Ignore line"}
                  </button>
                </div>
                {suggestions.length === 0 ? (
                  <div className="text-xs text-slate-500">No suggestions.</div>
                ) : (
                  <div className="space-y-2">
                    {suggestions.map((s) => (
                      <div
                        key={`${s.matchedEntityType}-${s.matchedEntityId}`}
                        className="rounded border border-slate-200 p-2 text-xs"
                      >
                        <div className="font-medium text-slate-900">
                          {s.displayRef || `${s.matchedEntityType}#${s.matchedEntityId}`}
                        </div>
                        <div className="text-slate-600">{s.displayText || "-"}</div>
                        <div className="mt-1 text-slate-600">
                          Score {s.score} | JE amount {formatAmount(s.bankGlAmount)} | Suggest{" "}
                          {formatAmount(s.suggestedAmount)}
                        </div>
                        <button
                          type="button"
                          onClick={() => handleMatchSuggestion(s)}
                          disabled={!canWrite || actionBusy || !(Number(s.suggestedAmount) > 0)}
                          className="mt-2 rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 disabled:opacity-50"
                        >
                          Match suggested amount
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div>
                <div className="mb-2 font-medium text-slate-900">Audit</div>
                <div className="max-h-64 space-y-2 overflow-auto">
                  {auditRows.length === 0 ? (
                    <div className="text-xs text-slate-500">No audit rows yet.</div>
                  ) : (
                    auditRows.map((row) => (
                      <div key={row.id} className="rounded border border-slate-200 p-2 text-xs">
                        <div className="font-medium text-slate-900">
                          {row.action} - {formatDateTime(row.acted_at)}
                        </div>
                        <div className="text-slate-500">
                          {row.bank_account_code || "-"} | {row.statement_recon_status || "-"}
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>
            </div>
          )}
        </section>
      </div>

      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="mb-3 flex items-center justify-between">
          <div>
            <h2 className="text-sm font-semibold text-slate-900">B07 Exception Queue</h2>
            <p className="text-xs text-slate-500">
              Unmatched / ambiguous / policy-blocked automation results.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <select
              value={exceptionStatusFilter}
              onChange={(e) => setExceptionStatusFilter(e.target.value)}
              className="rounded border border-slate-300 px-2 py-1 text-xs"
              disabled={!canReadExceptions}
            >
              <option value="">ALL</option>
              <option value="OPEN">OPEN</option>
              <option value="ASSIGNED">ASSIGNED</option>
              <option value="RESOLVED">RESOLVED</option>
              <option value="IGNORED">IGNORED</option>
            </select>
            <button
              type="button"
              onClick={() => loadExceptions()}
              className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700"
              disabled={!canReadExceptions || loadingExceptions}
            >
              {loadingExceptions ? "..." : "Refresh"}
            </button>
          </div>
        </div>
        {!canReadExceptions ? (
          <div className="text-xs text-slate-500">Missing permission: bank.reconcile.exceptions.read</div>
        ) : (
          <div className="max-h-72 overflow-auto">
            <table className="min-w-full text-left text-xs">
              <thead className="sticky top-0 bg-slate-50 text-slate-600">
                <tr>
                  <th className="px-2 py-2">ID</th>
                  <th className="px-2 py-2">Line</th>
                  <th className="px-2 py-2">Reason</th>
                  <th className="px-2 py-2">Status</th>
                  <th className="px-2 py-2">Rule</th>
                  <th className="px-2 py-2">Actions</th>
                </tr>
              </thead>
              <tbody>
                {exceptions.length === 0 ? (
                  <tr>
                    <td className="px-2 py-2 text-slate-500" colSpan={6}>
                      {loadingExceptions
                        ? "Loading exceptions..."
                        : `No exception rows (${exceptionsTotal} total).`}
                    </td>
                  </tr>
                ) : (
                  exceptions.map((row) => (
                    <tr key={row.id} className="border-t">
                      <td className="px-2 py-2">#{row.id}</td>
                      <td className="px-2 py-2">
                        <button
                          type="button"
                          onClick={() => setSelectedLineId(String(row.statement_line_id))}
                          className="text-left text-slate-700 underline-offset-2 hover:underline"
                        >
                          #{row.statement_line_id}
                        </button>
                        <div className="text-[11px] text-slate-500">
                          {formatDate(row.txn_date)} | {formatAmount(row.statement_amount)}
                        </div>
                      </td>
                      <td className="px-2 py-2">
                        <div>{row.reason_code}</div>
                        <div className="max-w-[260px] truncate text-[11px] text-slate-500" title={row.reason_message}>
                          {row.reason_message || "-"}
                        </div>
                      </td>
                      <td className="px-2 py-2">
                        <div>{row.status}</div>
                        <div className="text-[11px] text-slate-500">{row.severity}</div>
                      </td>
                      <td className="px-2 py-2">{row.matched_rule_id || "-"}</td>
                      <td className="px-2 py-2">
                        <div className="flex flex-wrap gap-1">
                          <button
                            type="button"
                            onClick={() => handleAssignExceptionToMe(row.id)}
                            disabled={!canWriteExceptions || actionBusy}
                            className="rounded border border-slate-300 px-2 py-1 text-[11px] text-slate-700 disabled:opacity-50"
                          >
                            Assign
                          </button>
                          <button
                            type="button"
                            onClick={() => handleResolveException(row.id)}
                            disabled={!canWriteExceptions || actionBusy}
                            className="rounded border border-slate-300 px-2 py-1 text-[11px] text-slate-700 disabled:opacity-50"
                          >
                            Resolve
                          </button>
                          <button
                            type="button"
                            onClick={() => handleIgnoreExceptionItem(row.id)}
                            disabled={!canWriteExceptions || actionBusy}
                            className="rounded border border-slate-300 px-2 py-1 text-[11px] text-slate-700 disabled:opacity-50"
                          >
                            Ignore
                          </button>
                          <button
                            type="button"
                            onClick={() => handleRetryExceptionItem(row.id)}
                            disabled={!canWriteExceptions || actionBusy}
                            className="rounded border border-slate-300 px-2 py-1 text-[11px] text-slate-700 disabled:opacity-50"
                          >
                            Retry
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
