import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  applyCariSettlement,
  attachCariBankReference,
  applyCariBankSettlement,
  reverseCariSettlement,
} from "../../api/cariSettlements.js";
import {
  listCashRegisters,
  listCashSessions,
} from "../../api/cashAdmin.js";
import { listAccounts } from "../../api/glAdmin.js";
import { listCariCounterparties } from "../../api/cariCounterparty.js";
import { listLegalEntities } from "../../api/orgAdmin.js";
import { getCariOpenItemsReport } from "../../api/cariReports.js";
import { extractCariReplayAndRisks } from "../../api/cariCommon.js";
import { useAuth } from "../../auth/useAuth.js";
import { useWorkingContextDefaults } from "../../context/useWorkingContextDefaults.js";
import { usePersistedFilters } from "../../hooks/usePersistedFilters.js";
import { useModuleReadiness } from "../../readiness/useModuleReadiness.js";
import {
  buildAutoAllocatePreview,
  buildSettlementApplyPayload,
} from "./cariSettlementsUtils.js";
import {
  buildSettlementIntentFingerprint,
  buildSettlementIntentScope,
  clearPendingIdempotencyKey,
  createEphemeralIdempotencyKey,
  createPendingIdempotencyKey,
  loadPendingIdempotencyKey,
  shouldClearPendingKeyAfterError,
} from "./cariIdempotency.js";

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

function toUpper(value) {
  return String(value || "").trim().toUpperCase();
}

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function toPositiveDecimal(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function toOptionalPositiveDecimal(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  return toPositiveDecimal(value);
}

function normalizeUiError(error, fallback) {
  const message = String(error?.message || error?.response?.data?.message || fallback || "Request failed");
  const requestId = String(error?.requestId || error?.response?.data?.requestId || "").trim();
  return requestId ? `${message} (requestId: ${requestId})` : message;
}

function formatReadinessReason(reason) {
  switch (String(reason || "").trim().toUpperCase()) {
    case "ACCOUNT_NOT_FOUND":
      return "Mapped account no longer exists.";
    case "ACCOUNT_INACTIVE":
      return "Mapped account is inactive.";
    case "ACCOUNT_NOT_POSTABLE":
      return "Mapped account is not postable.";
    case "ACCOUNT_SCOPE_NOT_LEGAL_ENTITY":
      return "Mapped account is not in a legal-entity chart.";
    case "ACCOUNT_LEGAL_ENTITY_MISMATCH":
      return "Mapped account belongs to a different legal entity.";
    case "PURPOSES_MUST_MAP_TO_DIFFERENT_ACCOUNTS":
      return "Control and offset must map to different accounts.";
    case "MAPPED_ACCOUNT_ID_INVALID":
      return "Mapped account id is invalid.";
    case "ACCOUNT_TENANT_MISMATCH":
      return "Mapped account belongs to a different tenant.";
    default:
      return String(reason || "Invalid mapping.");
  }
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

function parseManualAllocations(openItems, draftMap) {
  const allocations = [];
  for (const item of openItems || []) {
    const openItemId = Number(item?.openItemId || 0);
    if (!openItemId) {
      continue;
    }
    const key = String(openItemId);
    const amount = toOptionalPositiveDecimal(draftMap?.[key]);
    if (!amount) {
      continue;
    }
    allocations.push({ openItemId, amountTxn: amount });
  }
  return allocations;
}

function parseAllocationsJson(raw) {
  const text = String(raw || "").trim();
  if (!text) {
    return [];
  }
  const parsed = JSON.parse(text);
  if (!Array.isArray(parsed)) {
    throw new Error("allocations must be a JSON array.");
  }
  return parsed.map((entry, index) => {
    const openItemId = toPositiveInt(entry?.openItemId);
    const amountTxn = toPositiveDecimal(entry?.amountTxn);
    if (!openItemId) {
      throw new Error(`allocations[${index}].openItemId must be a positive integer.`);
    }
    if (!amountTxn) {
      throw new Error(`allocations[${index}].amountTxn must be > 0.`);
    }
    return { openItemId, amountTxn };
  });
}

function hasMixedDirections(openItems = []) {
  const directions = new Set(
    openItems
      .map((row) => String(row?.direction || "").trim().toUpperCase())
      .filter(Boolean)
  );
  return directions.size > 1;
}

function buildApplyDefaultForm() {
  return {
    legalEntityId: "",
    counterpartyId: "",
    direction: "",
    settlementDate: todayIsoDate(),
    currencyCode: "USD",
    incomingAmountTxn: "",
    idempotencyKey: "",
    autoAllocate: true,
    useUnappliedCash: false,
    allocations: [],
    fxRate: "",
    note: "",
  };
}

function buildLinkedCashDefaultForm() {
  return {
    paymentChannel: "MANUAL",
    createLinkedCashTransaction: false,
    registerId: "",
    cashSessionId: "",
    counterAccountId: "",
    txnDatetime: toDateTimeLocalInput(),
    bookDate: todayIsoDate(),
    referenceNo: "",
    description: "",
  };
}

function buildBankAttachDefaultForm() {
  return {
    legalEntityId: "",
    targetType: "SETTLEMENT",
    settlementBatchId: "",
    unappliedCashId: "",
    bankStatementLineId: "",
    bankTransactionRef: "",
    idempotencyKey: "",
    note: "",
  };
}

function buildBankApplyDefaultForm() {
  return {
    legalEntityId: "",
    counterpartyId: "",
    direction: "",
    settlementDate: todayIsoDate(),
    currencyCode: "USD",
    incomingAmountTxn: "",
    useUnappliedCash: false,
    autoAllocate: true,
    allocationsJson: "",
    bankStatementLineId: "",
    bankTransactionRef: "",
    bankApplyIdempotencyKey: "",
    note: "",
  };
}

function buildReverseDefaultForm() {
  return {
    settlementBatchId: "",
    reversalDate: todayIsoDate(),
    reason: "Manual settlement reversal",
  };
}

function buildPreviewDefaultFilters() {
  return {
    legalEntityId: "",
    counterpartyId: "",
    asOfDate: todayIsoDate(),
    direction: "",
  };
}

const SETTLEMENT_PREVIEW_CONTEXT_MAPPINGS = [{ stateKey: "legalEntityId" }];
const SETTLEMENT_APPLY_CONTEXT_MAPPINGS = [{ stateKey: "legalEntityId" }];
const SETTLEMENT_BANK_ATTACH_CONTEXT_MAPPINGS = [{ stateKey: "legalEntityId" }];
const SETTLEMENT_BANK_APPLY_CONTEXT_MAPPINGS = [{ stateKey: "legalEntityId" }];
const SETTLEMENT_PREVIEW_FILTERS_STORAGE_SCOPE = "cari-settlements.preview";

export default function CariSettlementsPage() {
  const { hasPermission } = useAuth();
  const { getModuleRow } = useModuleReadiness();
  const canApply = hasPermission("cari.settlement.apply");
  const canReverse = hasPermission("cari.settlement.reverse");
  const canBankAttach = hasPermission("cari.bank.attach");
  const canBankApply = hasPermission("cari.bank.apply");
  const canReadReports = hasPermission("cari.report.read");
  const canReadCards = hasPermission("cari.card.read");
  const canReadOrg = hasPermission("org.tree.read");
  const canCreateCashTxn = hasPermission("cash.txn.create");
  const canReadCashRegisters = hasPermission("cash.register.read");
  const canReadCashSessions = hasPermission("cash.session.read");
  const canReadGlAccounts = hasPermission("gl.account.read");

  const [previewFilters, setPreviewFilters] = usePersistedFilters(
    SETTLEMENT_PREVIEW_FILTERS_STORAGE_SCOPE,
    () => buildPreviewDefaultFilters()
  );
  const [openItems, setOpenItems] = useState([]);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState("");

  const [applyForm, setApplyForm] = useState(() => buildApplyDefaultForm());
  const [manualAllocationDraft, setManualAllocationDraft] = useState({});
  const [applySubmitting, setApplySubmitting] = useState(false);
  const [applyError, setApplyError] = useState("");
  const [applyMessage, setApplyMessage] = useState("");
  const [applyReplayMessage, setApplyReplayMessage] = useState("");
  const [applyResult, setApplyResult] = useState(null);
  const [applyFollowUpRisks, setApplyFollowUpRisks] = useState([]);
  const [linkedCashForm, setLinkedCashForm] = useState(() => buildLinkedCashDefaultForm());
  const [linkedCashError, setLinkedCashError] = useState("");
  const [linkedCashMessage, setLinkedCashMessage] = useState("");
  const [linkedCashResult, setLinkedCashResult] = useState(null);
  const [legalEntities, setLegalEntities] = useState([]);
  const [counterpartyOptions, setCounterpartyOptions] = useState([]);
  const [lookupWarning, setLookupWarning] = useState("");
  const [cashRegisterOptions, setCashRegisterOptions] = useState([]);
  const [openCashSessions, setOpenCashSessions] = useState([]);
  const [cashAccountOptions, setCashAccountOptions] = useState([]);

  const [reverseForm, setReverseForm] = useState(() => buildReverseDefaultForm());
  const [reverseSubmitting, setReverseSubmitting] = useState(false);
  const [reverseError, setReverseError] = useState("");
  const [reverseMessage, setReverseMessage] = useState("");
  const [reverseResult, setReverseResult] = useState(null);

  const [bankAttachForm, setBankAttachForm] = useState(() => buildBankAttachDefaultForm());
  const [bankAttachSubmitting, setBankAttachSubmitting] = useState(false);
  const [bankAttachError, setBankAttachError] = useState("");
  const [bankAttachMessage, setBankAttachMessage] = useState("");
  const [bankAttachResult, setBankAttachResult] = useState(null);

  const [bankApplyForm, setBankApplyForm] = useState(() => buildBankApplyDefaultForm());
  const [bankApplySubmitting, setBankApplySubmitting] = useState(false);
  const [bankApplyError, setBankApplyError] = useState("");
  const [bankApplyMessage, setBankApplyMessage] = useState("");
  const [bankApplyResult, setBankApplyResult] = useState(null);
  const [bankApplyFollowUpRisks, setBankApplyFollowUpRisks] = useState([]);

  useWorkingContextDefaults(setPreviewFilters, SETTLEMENT_PREVIEW_CONTEXT_MAPPINGS, [
    previewFilters.legalEntityId,
  ]);
  useWorkingContextDefaults(setApplyForm, SETTLEMENT_APPLY_CONTEXT_MAPPINGS, [
    applyForm.legalEntityId,
  ]);
  useWorkingContextDefaults(setBankAttachForm, SETTLEMENT_BANK_ATTACH_CONTEXT_MAPPINGS, [
    bankAttachForm.legalEntityId,
  ]);
  useWorkingContextDefaults(setBankApplyForm, SETTLEMENT_BANK_APPLY_CONTEXT_MAPPINGS, [
    bankApplyForm.legalEntityId,
  ]);

  const applyLegalEntityId = toPositiveInt(applyForm.legalEntityId);
  const applyCariReadiness = getModuleRow("cariPosting", applyLegalEntityId);
  const applyCariNotReady = Boolean(applyCariReadiness && !applyCariReadiness.ready);
  const bankApplyLegalEntityId = toPositiveInt(bankApplyForm.legalEntityId);
  const bankApplyCariReadiness = getModuleRow(
    "cariPosting",
    bankApplyLegalEntityId
  );
  const bankApplyCariNotReady = Boolean(
    bankApplyCariReadiness && !bankApplyCariReadiness.ready
  );

  const previewRows = useMemo(
    () => buildAutoAllocatePreview(openItems, Number(applyForm.incomingAmountTxn || 0)),
    [openItems, applyForm.incomingAmountTxn]
  );
  const mixedDirectionRisk = useMemo(() => hasMixedDirections(openItems), [openItems]);
  const linkedRegisterOptions = useMemo(() => {
    const legalEntityId = toPositiveInt(applyForm.legalEntityId);
    if (!legalEntityId) {
      return cashRegisterOptions;
    }
    return cashRegisterOptions.filter(
      (row) => toPositiveInt(row?.legal_entity_id) === legalEntityId
    );
  }, [applyForm.legalEntityId, cashRegisterOptions]);
  const selectedLinkedRegister = useMemo(() => {
    const registerId = toPositiveInt(linkedCashForm.registerId);
    if (!registerId) {
      return null;
    }
    return linkedRegisterOptions.find((row) => toPositiveInt(row?.id) === registerId) || null;
  }, [linkedCashForm.registerId, linkedRegisterOptions]);
  const linkedRegisterOpenSessions = useMemo(() => {
    const registerId = toPositiveInt(linkedCashForm.registerId);
    if (!registerId) {
      return [];
    }
    return openCashSessions.filter(
      (row) => toPositiveInt(row?.cash_register_id) === registerId
    );
  }, [linkedCashForm.registerId, openCashSessions]);
  const postingAccountOptions = useMemo(
    () =>
      (cashAccountOptions || []).filter((row) => {
        if (!row) {
          return false;
        }
        const legalEntityId = toPositiveInt(applyForm.legalEntityId);
        if (
          legalEntityId &&
          toPositiveInt(row.legal_entity_id || row.legalEntityId) !== legalEntityId
        ) {
          return false;
        }
        const allowPosting =
          row.allow_posting === 1 || row.allowPosting === true || row.allow_posting === true;
        const isActive = row.is_active === 1 || row.isActive === true || row.is_active === true;
        return allowPosting && isActive;
      }),
    [applyForm.legalEntityId, cashAccountOptions]
  );

  const applyIntentScope = useMemo(
    () => buildSettlementIntentScope(applyForm),
    [applyForm]
  );
  const applyIntentFingerprint = useMemo(
    () => buildSettlementIntentFingerprint(applyForm),
    [applyForm]
  );
  const previewLegalEntityId = previewFilters.legalEntityId;
  const previewCounterpartyId = previewFilters.counterpartyId;
  const previewAsOfDate = previewFilters.asOfDate;
  const previewDirection = previewFilters.direction;

  useEffect(() => {
    const pendingKey = loadPendingIdempotencyKey(applyIntentScope, applyIntentFingerprint);
    setApplyForm((prev) =>
      prev.idempotencyKey === pendingKey ? prev : { ...prev, idempotencyKey: pendingKey }
    );
  }, [applyIntentScope, applyIntentFingerprint]);

  useEffect(() => {
    if (!canReadReports) {
      setOpenItems([]);
      setPreviewError("");
      return;
    }

    if (!previewLegalEntityId || !previewCounterpartyId || !previewAsOfDate) {
      setOpenItems([]);
      setPreviewError("");
      return;
    }

    let active = true;
    async function loadPreviewOpenItems() {
      setPreviewLoading(true);
      setPreviewError("");
      try {
        const payload = await getCariOpenItemsReport({
          legalEntityId: previewLegalEntityId,
          counterpartyId: previewCounterpartyId,
          asOfDate: previewAsOfDate,
          direction: previewDirection || undefined,
          status: "OPEN",
          limit: 500,
          offset: 0,
        });
        if (!active) {
          return;
        }
        const rows = Array.isArray(payload?.rows) ? payload.rows : [];
        setOpenItems(rows);
      } catch (error) {
        if (!active) {
          return;
        }
        setOpenItems([]);
        setPreviewError(normalizeUiError(error, "Failed to load open-items preview."));
      } finally {
        if (active) {
          setPreviewLoading(false);
        }
      }
    }

    loadPreviewOpenItems();
    return () => {
      active = false;
    };
  }, [
    canReadReports,
    previewLegalEntityId,
    previewCounterpartyId,
    previewAsOfDate,
    previewDirection,
  ]);

  useEffect(() => {
    let active = true;
    async function loadLookups() {
      const warnings = [];
      const [legalEntitiesResult, registersResult, sessionsResult, accountsResult] =
        await Promise.allSettled([
          canReadOrg ? listLegalEntities({ limit: 500, includeInactive: true }) : Promise.resolve({ rows: [] }),
          canReadCashRegisters ? listCashRegisters({ limit: 300, offset: 0 }) : Promise.resolve({ rows: [] }),
          canReadCashSessions ? listCashSessions({ status: "OPEN", limit: 300, offset: 0 }) : Promise.resolve({ rows: [] }),
          canReadGlAccounts ? listAccounts({ includeInactive: true, limit: 800 }) : Promise.resolve({ rows: [] }),
        ]);

      if (!active) {
        return;
      }

      if (legalEntitiesResult.status === "fulfilled") {
        setLegalEntities(Array.isArray(legalEntitiesResult.value?.rows) ? legalEntitiesResult.value.rows : []);
      } else {
        setLegalEntities([]);
        warnings.push("Legal entity lookup unavailable.");
      }

      if (registersResult.status === "fulfilled") {
        setCashRegisterOptions(Array.isArray(registersResult.value?.rows) ? registersResult.value.rows : []);
      } else {
        setCashRegisterOptions([]);
        warnings.push("Cash register lookup unavailable.");
      }

      if (sessionsResult.status === "fulfilled") {
        setOpenCashSessions(Array.isArray(sessionsResult.value?.rows) ? sessionsResult.value.rows : []);
      } else {
        setOpenCashSessions([]);
        warnings.push("Cash session lookup unavailable.");
      }

      if (accountsResult.status === "fulfilled") {
        setCashAccountOptions(Array.isArray(accountsResult.value?.rows) ? accountsResult.value.rows : []);
      } else {
        setCashAccountOptions([]);
        warnings.push("GL account lookup unavailable.");
      }

      setLookupWarning(warnings.join(" "));
    }

    loadLookups();
    return () => {
      active = false;
    };
  }, [canReadCashRegisters, canReadCashSessions, canReadGlAccounts, canReadOrg]);

  useEffect(() => {
    if (!canReadCards) {
      setCounterpartyOptions([]);
      return;
    }
    const legalEntityId = toPositiveInt(applyForm.legalEntityId);
    if (!legalEntityId) {
      setCounterpartyOptions([]);
      return;
    }

    const role =
      toUpper(applyForm.direction) === "AR"
        ? "CUSTOMER"
        : toUpper(applyForm.direction) === "AP"
          ? "VENDOR"
          : undefined;

    let active = true;
    async function loadCounterpartyRows() {
      try {
        const response = await listCariCounterparties({
          legalEntityId,
          role,
          status: "ACTIVE",
          sortBy: "NAME",
          sortDir: "ASC",
          limit: 300,
          offset: 0,
        });
        if (!active) {
          return;
        }
        setCounterpartyOptions(Array.isArray(response?.rows) ? response.rows : []);
      } catch {
        if (!active) {
          return;
        }
        setCounterpartyOptions([]);
      }
    }

    loadCounterpartyRows();
    return () => {
      active = false;
    };
  }, [applyForm.direction, applyForm.legalEntityId, canReadCards]);

  useEffect(() => {
    if (!linkedCashForm.createLinkedCashTransaction) {
      return;
    }
    if (toPositiveInt(linkedCashForm.registerId)) {
      return;
    }
    if (!linkedRegisterOptions.length) {
      return;
    }
    const preferred = linkedRegisterOptions.find((row) => toUpper(row?.status) === "ACTIVE");
    setLinkedCashForm((prev) => ({
      ...prev,
      registerId: String(preferred?.id || linkedRegisterOptions[0]?.id || ""),
    }));
  }, [
    linkedCashForm.createLinkedCashTransaction,
    linkedCashForm.registerId,
    linkedRegisterOptions,
  ]);

  useEffect(() => {
    const registerId = toPositiveInt(linkedCashForm.registerId);
    if (!registerId) {
      return;
    }
    const exists = linkedRegisterOptions.some(
      (row) => toPositiveInt(row?.id) === registerId
    );
    if (exists) {
      return;
    }
    setLinkedCashForm((prev) => ({
      ...prev,
      registerId: "",
      cashSessionId: "",
    }));
  }, [linkedCashForm.registerId, linkedRegisterOptions]);

  function updateApplyForm(field, value) {
    setApplyForm((prev) => ({ ...prev, [field]: value }));
    if (field === "legalEntityId" || field === "counterpartyId" || field === "direction") {
      setPreviewFilters((prev) => ({ ...prev, [field]: value }));
    }
    if (field === "settlementDate") {
      setLinkedCashForm((prev) => ({
        ...prev,
        bookDate: String(value || "").trim() || prev.bookDate,
      }));
    }
  }

  function validateLinkedCashFormBeforeApply(formSnapshot) {
    if (
      !linkedCashForm.createLinkedCashTransaction ||
      toUpper(linkedCashForm.paymentChannel) !== "CASH"
    ) {
      return "";
    }
    if (!canCreateCashTxn) {
      return "Missing permission: cash.txn.create";
    }
    if (!toPositiveInt(linkedCashForm.registerId)) {
      return "registerId is required for linked cash transaction.";
    }
    if (!toPositiveInt(linkedCashForm.counterAccountId)) {
      return "counterAccountId is required for linked cash transaction.";
    }
    const direction = toUpper(formSnapshot.direction);
    if (direction !== "AR" && direction !== "AP") {
      return "direction must be AR or AP when linked cash creation is enabled.";
    }
    if (!toPositiveInt(formSnapshot.counterpartyId)) {
      return "counterpartyId is required when linked cash creation is enabled.";
    }
    if (!toPositiveDecimal(formSnapshot.incomingAmountTxn)) {
      return "incomingAmountTxn must be > 0 when linked cash creation is enabled.";
    }
    return "";
  }

  function buildLinkedCashPayloadForApply(formSnapshot, settlementIdempotencyKey) {
    const wantsCashLink =
      linkedCashForm.createLinkedCashTransaction &&
      toUpper(linkedCashForm.paymentChannel) === "CASH";
    if (!wantsCashLink) {
      return {
        paymentChannel: "MANUAL",
        linkedCashTransaction: undefined,
      };
    }

    const registerId = toPositiveInt(linkedCashForm.registerId);
    const counterAccountId = toPositiveInt(linkedCashForm.counterAccountId);
    const deterministicCashKey = `CARI-CASH-${settlementIdempotencyKey}`.slice(0, 100);
    const deterministicCashEvent = `CARI-CASH-EVT-${settlementIdempotencyKey}`.slice(0, 100);

    return {
      paymentChannel: "CASH",
      linkedCashTransaction: {
        registerId,
        cashSessionId: toPositiveInt(linkedCashForm.cashSessionId) || undefined,
        counterAccountId,
        txnDatetime: String(linkedCashForm.txnDatetime || "").trim() || toDateTimeLocalInput(),
        bookDate:
          String(linkedCashForm.bookDate || "").trim() ||
          String(formSnapshot.settlementDate || "").trim() ||
          todayIsoDate(),
        referenceNo: String(linkedCashForm.referenceNo || "").trim() || undefined,
        description: String(linkedCashForm.description || "").trim() || undefined,
        idempotencyKey: deterministicCashKey,
        integrationEventUid: deterministicCashEvent,
      },
    };
  }

  async function onApply(form = applyForm) {
    setApplyError("");
    setApplyMessage("");
    setApplyReplayMessage("");
    setApplyResult(null);
    setApplyFollowUpRisks([]);
    setLinkedCashError("");
    setLinkedCashMessage("");
    setLinkedCashResult(null);

    if (!canApply) {
      setApplyError("Missing permission: cari.settlement.apply");
      return;
    }
    if (applyCariNotReady) {
      setApplyError(
        "Setup incomplete for selected legal entity. Configure CARI purpose mappings in GL Setup first."
      );
      return;
    }

    if (form.autoAllocate && !form.direction) {
      setApplyError("Direction is required for auto-allocation.");
      return;
    }
    if (form.autoAllocate && mixedDirectionRisk) {
      setApplyError(
        "Open-items preview contains mixed AR/AP rows. Select one direction and retry."
      );
      return;
    }

    const linkedCashValidationError = validateLinkedCashFormBeforeApply(form);
    if (linkedCashValidationError) {
      setApplyError(linkedCashValidationError);
      return;
    }

    const intentScope = buildSettlementIntentScope(form);
    const intentFingerprint = buildSettlementIntentFingerprint(form);
    const idempotencyKey =
      form.idempotencyKey || createPendingIdempotencyKey(intentScope, intentFingerprint);
    setApplyForm((prev) => ({ ...prev, idempotencyKey }));

    const manualAllocations = parseManualAllocations(openItems, manualAllocationDraft);
    if (!form.autoAllocate && manualAllocations.length === 0) {
      setApplyError("allocations are required when autoAllocate=false.");
      return;
    }

    for (const allocation of manualAllocations) {
      const openItem = openItems.find(
        (row) => Number(row?.openItemId || 0) === Number(allocation.openItemId || 0)
      );
      const maxOpen = Number(openItem?.residualAmountTxnAsOf || 0);
      if (allocation.amountTxn > maxOpen + 0.000001) {
        setApplyError(`Allocation exceeds open amount for openItemId=${allocation.openItemId}.`);
        return;
      }
    }

    setApplySubmitting(true);
    try {
      const payload = buildSettlementApplyPayload({
        ...form,
        idempotencyKey,
        allocations: form.autoAllocate ? [] : manualAllocations,
        ...buildLinkedCashPayloadForApply(form, idempotencyKey),
      });
      const response = await applyCariSettlement(payload);
      const replayState = extractCariReplayAndRisks(response);
      clearPendingIdempotencyKey(intentScope);

      setApplyResult(response);
      setApplyFollowUpRisks(replayState.followUpRisks);
      if (replayState.idempotentReplay) {
        setApplyReplayMessage("Bu istek daha once uygulanmis; mevcut sonuc gosteriliyor.");
      }
      setApplyMessage(
        `Settlement apply completed. settlementBatchId=${response?.row?.id || "-"}`
      );
      const wantsCashLink =
        linkedCashForm.createLinkedCashTransaction &&
        toUpper(linkedCashForm.paymentChannel) === "CASH";
      if (wantsCashLink) {
        const linkedCashId =
          toPositiveInt(response?.cashTransaction?.id) ||
          toPositiveInt(response?.row?.cashTransactionId);
        if (!linkedCashId) {
          setLinkedCashError(
            "Settlement applied, but linked cash transaction details were not returned."
          );
        } else {
          setLinkedCashResult(response?.cashTransaction || { id: linkedCashId });
          setLinkedCashMessage(
            replayState.idempotentReplay
              ? `Linked cash transaction already exists. cashTransactionId=${linkedCashId}`
              : `Linked cash transaction created. cashTransactionId=${linkedCashId}`
          );
        }
      }
    } catch (error) {
      if (shouldClearPendingKeyAfterError(error)) {
        clearPendingIdempotencyKey(intentScope);
      }
      setApplyError(normalizeUiError(error, "Settlement apply failed."));
    } finally {
      setApplySubmitting(false);
    }
  }

  async function onReverse(event) {
    event.preventDefault();
    setReverseError("");
    setReverseMessage("");
    setReverseResult(null);
    if (!canReverse) {
      setReverseError("Missing permission: cari.settlement.reverse");
      return;
    }

    const settlementBatchId = toPositiveInt(reverseForm.settlementBatchId);
    if (!settlementBatchId) {
      setReverseError("settlementBatchId must be a positive integer.");
      return;
    }

    setReverseSubmitting(true);
    try {
      const response = await reverseCariSettlement(settlementBatchId, {
        reason: String(reverseForm.reason || "").trim() || "Manual settlement reversal",
        reversalDate: String(reverseForm.reversalDate || "").trim() || undefined,
      });
      setReverseResult(response);
      setReverseMessage(
        `Settlement reversed. reversalSettlementBatchId=${response?.row?.id || "-"}`
      );
    } catch (error) {
      setReverseError(normalizeUiError(error, "Settlement reverse failed."));
    } finally {
      setReverseSubmitting(false);
    }
  }

  async function onBankAttach(event) {
    event.preventDefault();
    setBankAttachError("");
    setBankAttachMessage("");
    setBankAttachResult(null);
    if (!canBankAttach) {
      setBankAttachError("Missing permission: cari.bank.attach");
      return;
    }

    const legalEntityId = toPositiveInt(bankAttachForm.legalEntityId);
    if (!legalEntityId) {
      setBankAttachError("legalEntityId is required.");
      return;
    }
    if (!bankAttachForm.bankStatementLineId && !bankAttachForm.bankTransactionRef) {
      setBankAttachError(
        "bankStatementLineId or bankTransactionRef is required for bank attach."
      );
      return;
    }

    const targetType = String(bankAttachForm.targetType || "").toUpperCase();
    const settlementBatchId = toPositiveInt(bankAttachForm.settlementBatchId);
    const unappliedCashId = toPositiveInt(bankAttachForm.unappliedCashId);
    if (targetType === "SETTLEMENT") {
      if (!settlementBatchId) {
        setBankAttachError("settlementBatchId is required when targetType=SETTLEMENT.");
        return;
      }
      if (unappliedCashId) {
        setBankAttachError("unappliedCashId must be empty when targetType=SETTLEMENT.");
        return;
      }
    } else if (targetType === "UNAPPLIED_CASH") {
      if (!unappliedCashId) {
        setBankAttachError("unappliedCashId is required when targetType=UNAPPLIED_CASH.");
        return;
      }
      if (settlementBatchId) {
        setBankAttachError("settlementBatchId must be empty when targetType=UNAPPLIED_CASH.");
        return;
      }
    } else {
      setBankAttachError("targetType must be SETTLEMENT or UNAPPLIED_CASH.");
      return;
    }

    const idempotencyKey =
      bankAttachForm.idempotencyKey ||
      createEphemeralIdempotencyKey("CARI-BANK-ATTACH");
    setBankAttachForm((prev) => ({ ...prev, idempotencyKey }));

    setBankAttachSubmitting(true);
    try {
      const response = await attachCariBankReference({
        legalEntityId,
        targetType,
        settlementBatchId: settlementBatchId || null,
        unappliedCashId: unappliedCashId || null,
        bankStatementLineId: toPositiveInt(bankAttachForm.bankStatementLineId),
        bankTransactionRef: String(bankAttachForm.bankTransactionRef || "").trim() || null,
        idempotencyKey,
        note: String(bankAttachForm.note || "").trim() || undefined,
      });
      setBankAttachResult(response);
      if (response?.idempotentReplay) {
        setBankAttachMessage("Bu istek daha once uygulanmis; mevcut sonuc gosteriliyor.");
      } else {
        setBankAttachMessage("Bank attach completed.");
      }
    } catch (error) {
      setBankAttachError(normalizeUiError(error, "Bank attach failed."));
    } finally {
      setBankAttachSubmitting(false);
    }
  }

  async function onBankApply(event) {
    event.preventDefault();
    setBankApplyError("");
    setBankApplyMessage("");
    setBankApplyResult(null);
    setBankApplyFollowUpRisks([]);
    if (!canBankApply) {
      setBankApplyError("Missing permission: cari.bank.apply");
      return;
    }
    if (bankApplyCariNotReady) {
      setBankApplyError(
        "Setup incomplete for selected legal entity. Configure CARI purpose mappings in GL Setup first."
      );
      return;
    }

    const legalEntityId = toPositiveInt(bankApplyForm.legalEntityId);
    const counterpartyId = toPositiveInt(bankApplyForm.counterpartyId);
    if (!legalEntityId) {
      setBankApplyError("legalEntityId is required.");
      return;
    }
    if (!counterpartyId) {
      setBankApplyError("counterpartyId is required.");
      return;
    }
    if (!bankApplyForm.bankStatementLineId && !bankApplyForm.bankTransactionRef) {
      setBankApplyError(
        "bankStatementLineId or bankTransactionRef is required for bank apply."
      );
      return;
    }
    if (bankApplyForm.autoAllocate && !String(bankApplyForm.direction || "").trim()) {
      setBankApplyError("Direction is required for auto-allocation.");
      return;
    }

    let allocations = [];
    if (!bankApplyForm.autoAllocate) {
      try {
        allocations = parseAllocationsJson(bankApplyForm.allocationsJson);
      } catch (error) {
        setBankApplyError(error?.message || "allocations JSON is invalid.");
        return;
      }
      if (allocations.length === 0) {
        setBankApplyError("allocations are required when autoAllocate=false.");
        return;
      }
    }

    const bankApplyIdempotencyKey =
      bankApplyForm.bankApplyIdempotencyKey ||
      createEphemeralIdempotencyKey("CARI-BANK-APPLY");
    setBankApplyForm((prev) => ({ ...prev, bankApplyIdempotencyKey }));

    setBankApplySubmitting(true);
    try {
      const payload = buildSettlementApplyPayload({
        ...bankApplyForm,
        legalEntityId,
        counterpartyId,
        idempotencyKey: bankApplyIdempotencyKey,
        allocations: bankApplyForm.autoAllocate ? [] : allocations,
      });
      const response = await applyCariBankSettlement({
        ...payload,
        bankApplyIdempotencyKey,
        bankStatementLineId: toPositiveInt(bankApplyForm.bankStatementLineId),
        bankTransactionRef: String(bankApplyForm.bankTransactionRef || "").trim() || null,
      });
      const replayState = extractCariReplayAndRisks(response);
      setBankApplyResult(response);
      setBankApplyFollowUpRisks(replayState.followUpRisks);
      if (replayState.idempotentReplay) {
        setBankApplyMessage("Bu istek daha once uygulanmis; mevcut sonuc gosteriliyor.");
      } else {
        setBankApplyMessage(
          `Bank apply completed. settlementBatchId=${response?.row?.id || "-"}`
        );
      }
    } catch (error) {
      setBankApplyError(normalizeUiError(error, "Bank apply failed."));
    } finally {
      setBankApplySubmitting(false);
    }
  }

  const manualAllocations = useMemo(
    () => parseManualAllocations(openItems, manualAllocationDraft),
    [openItems, manualAllocationDraft]
  );
  const autoAllocateDirectionMissing =
    Boolean(applyForm.autoAllocate) && !String(applyForm.direction || "").trim();
  const autoAllocateBlocked =
    autoAllocateDirectionMissing || (Boolean(applyForm.autoAllocate) && mixedDirectionRisk);

  return (
    <div className="space-y-5">
      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h1 className="text-xl font-semibold text-slate-900">Cari Settlements</h1>
        <p className="mt-1 text-sm text-slate-600">
          Settlement apply/reverse and bank attach/apply workflows are separated on this page.
        </p>
        {lookupWarning ? (
          <div className="mt-3 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
            {lookupWarning}
          </div>
        ) : null}
        <div className="mt-4 grid gap-3 md:grid-cols-4">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            Legal Entity ID
            {legalEntities.length > 0 ? (
              <select
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                value={applyForm.legalEntityId}
                onChange={(event) => updateApplyForm("legalEntityId", event.target.value)}
                disabled={!canApply}
              >
                <option value="">Select legal entity</option>
                {legalEntities.map((row) => (
                  <option key={`settlement-le-${row.id}`} value={row.id}>
                    {`${row.code || row.id} - ${row.name || "-"}`}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min="1"
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                value={applyForm.legalEntityId}
                onChange={(event) => updateApplyForm("legalEntityId", event.target.value)}
                disabled={!canApply}
              />
            )}
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            Counterparty ID
            {counterpartyOptions.length > 0 ? (
              <select
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                value={applyForm.counterpartyId}
                onChange={(event) => updateApplyForm("counterpartyId", event.target.value)}
                disabled={!canApply}
              >
                <option value="">Select counterparty</option>
                {counterpartyOptions.map((row) => (
                  <option key={`settlement-cp-${row.id}`} value={row.id}>
                    {`${row.code || row.id} - ${row.name || "-"} (${row.counterpartyType || "OTHER"})`}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min="1"
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                value={applyForm.counterpartyId}
                onChange={(event) => updateApplyForm("counterpartyId", event.target.value)}
                disabled={!canApply}
              />
            )}
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            Direction
            <select
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={applyForm.direction}
              onChange={(event) => updateApplyForm("direction", event.target.value)}
              disabled={!canApply}
            >
              <option value="">Select</option>
              <option value="AR">AR</option>
              <option value="AP">AP</option>
            </select>
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            As-Of Date (Preview)
            <input
              type="date"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={previewFilters.asOfDate}
              onChange={(event) =>
                setPreviewFilters((prev) => ({ ...prev, asOfDate: event.target.value }))
              }
              disabled={!canApply}
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            Payment Channel
            <select
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={linkedCashForm.paymentChannel}
              onChange={(event) =>
                setLinkedCashForm((prev) => ({
                  ...prev,
                  paymentChannel: event.target.value,
                  createLinkedCashTransaction:
                    event.target.value === "CASH" ? prev.createLinkedCashTransaction : false,
                }))
              }
              disabled={!canApply}
            >
              <option value="MANUAL">MANUAL</option>
              <option value="CASH">CASH</option>
            </select>
          </label>
          <label className="flex items-center gap-2 rounded-md border border-slate-200 px-3 py-2 text-sm text-slate-700 md:col-span-2">
            <input
              type="checkbox"
              checked={Boolean(linkedCashForm.createLinkedCashTransaction)}
              onChange={(event) =>
                setLinkedCashForm((prev) => ({
                  ...prev,
                  createLinkedCashTransaction:
                    toUpper(prev.paymentChannel) === "CASH" ? event.target.checked : false,
                }))
              }
              disabled={!canApply || toUpper(linkedCashForm.paymentChannel) !== "CASH"}
            />
            Create linked cash transaction after settlement apply
          </label>
          {toUpper(linkedCashForm.paymentChannel) !== "CASH" ? (
            <p className="text-xs text-slate-500 md:col-span-2">
              Select payment channel CASH to enable linked cash transaction creation.
            </p>
          ) : null}
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-900">Settlement Apply</h2>
        {!canApply ? (
          <div className="mt-3 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
            Missing permission: `cari.settlement.apply`
          </div>
        ) : null}
        {applyCariNotReady ? (
          <div className="mt-3 rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-900">
            <p className="font-semibold">Setup incomplete (CARI posting)</p>
            <p className="mt-1">
              Settlement apply is disabled for legalEntityId={applyLegalEntityId || "-"}.
            </p>
            {Array.isArray(applyCariReadiness?.missingPurposeCodes) &&
            applyCariReadiness.missingPurposeCodes.length > 0 ? (
              <p className="mt-1">
                Missing purpose codes: {applyCariReadiness.missingPurposeCodes.join(", ")}
              </p>
            ) : null}
            {Array.isArray(applyCariReadiness?.invalidMappings) &&
            applyCariReadiness.invalidMappings.length > 0 ? (
              <ul className="mt-2 list-disc pl-5">
                {applyCariReadiness.invalidMappings.map((row, index) => (
                  <li key={`apply-cari-invalid-${index}`}>
                    {String(row?.purposeCode || "-")}: {formatReadinessReason(row?.reason)}
                  </li>
                ))}
              </ul>
            ) : null}
            <div className="mt-2 flex flex-wrap gap-2">
              <Link
                to="/app/ayarlar/hesap-plani-ayarlari#manual-purpose-mappings"
                className="rounded border border-amber-300 bg-white px-2.5 py-1 text-xs font-semibold text-amber-900"
              >
                Fix manually
              </Link>
              <Link
                to="/app/ayarlar/hesap-plani-ayarlari#template-wizard"
                className="rounded border border-amber-300 bg-white px-2.5 py-1 text-xs font-semibold text-amber-900"
              >
                Use template
              </Link>
            </div>
          </div>
        ) : null}
        {applyError ? (
          <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
            {applyError}
          </div>
        ) : null}
        {applyMessage ? (
          <div className="mt-3 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
            {applyMessage}
          </div>
        ) : null}
        {applyReplayMessage ? (
          <div className="mt-3 rounded-md border border-cyan-200 bg-cyan-50 px-3 py-2 text-sm text-cyan-800">
            {applyReplayMessage}
          </div>
        ) : null}
        {applyFollowUpRisks.length > 0 ? (
          <div className="mt-3 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
            <p className="font-semibold">Follow-up risks</p>
            <ul className="mt-1 list-disc pl-5">
              {applyFollowUpRisks.map((risk, index) => (
                <li key={`apply-risk-${index}`}>{risk}</li>
              ))}
            </ul>
          </div>
        ) : null}
        {linkedCashError ? (
          <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
            {linkedCashError}
          </div>
        ) : null}
        {linkedCashMessage ? (
          <div className="mt-3 rounded-md border border-cyan-200 bg-cyan-50 px-3 py-2 text-sm text-cyan-800">
            {linkedCashMessage}
          </div>
        ) : null}

        <form
          className="mt-4 grid gap-3 md:grid-cols-4"
          onSubmit={(event) => {
            event.preventDefault();
            void onApply(applyForm);
          }}
        >
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            Settlement Date
            <input
              type="date"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={applyForm.settlementDate}
              onChange={(event) => updateApplyForm("settlementDate", event.target.value)}
              disabled={!canApply || applySubmitting}
              required
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            Currency
            <input
              type="text"
              maxLength={3}
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal uppercase"
              value={applyForm.currencyCode}
              onChange={(event) => updateApplyForm("currencyCode", event.target.value)}
              disabled={!canApply || applySubmitting}
              required
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            Incoming Amount Txn
            <input
              type="number"
              min="0"
              step="0.000001"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={applyForm.incomingAmountTxn}
              onChange={(event) => updateApplyForm("incomingAmountTxn", event.target.value)}
              disabled={!canApply || applySubmitting}
              required
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            FX Rate (optional)
            <input
              type="number"
              min="0.0000000001"
              step="0.0000000001"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={applyForm.fxRate}
              onChange={(event) => updateApplyForm("fxRate", event.target.value)}
              disabled={!canApply || applySubmitting}
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600 md:col-span-2">
            Note (optional)
            <input
              type="text"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={applyForm.note}
              onChange={(event) => updateApplyForm("note", event.target.value)}
              disabled={!canApply || applySubmitting}
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600 md:col-span-2">
            Idempotency Key (auto-generated if empty)
            <input
              type="text"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={applyForm.idempotencyKey}
              onChange={(event) => updateApplyForm("idempotencyKey", event.target.value)}
              disabled={!canApply || applySubmitting}
            />
          </label>
          <label className="flex items-center gap-2 text-sm text-slate-700 md:col-span-2">
            <input
              type="checkbox"
              checked={Boolean(applyForm.autoAllocate)}
              onChange={(event) => updateApplyForm("autoAllocate", event.target.checked)}
              disabled={!canApply || applySubmitting}
            />
            autoAllocate
          </label>
          <label className="flex items-center gap-2 text-sm text-slate-700 md:col-span-2">
            <input
              type="checkbox"
              checked={Boolean(applyForm.useUnappliedCash)}
              onChange={(event) => updateApplyForm("useUnappliedCash", event.target.checked)}
              disabled={!canApply || applySubmitting}
            />
            useUnappliedCash
          </label>

          {linkedCashForm.createLinkedCashTransaction &&
          toUpper(linkedCashForm.paymentChannel) === "CASH" ? (
            <>
              {!canCreateCashTxn ? (
                <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800 md:col-span-4">
                  Missing permission: `cash.txn.create`
                </div>
              ) : null}
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
                cash register
                {linkedRegisterOptions.length > 0 ? (
                  <select
                    className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                    value={linkedCashForm.registerId}
                    onChange={(event) =>
                      setLinkedCashForm((prev) => ({
                        ...prev,
                        registerId: event.target.value,
                        cashSessionId: "",
                      }))
                    }
                    disabled={applySubmitting}
                    required
                  >
                    <option value="">Select register</option>
                    {linkedRegisterOptions.map((row) => (
                      <option key={`linked-register-${row.id}`} value={row.id}>
                        {`${row.code || row.id} - ${row.name || "-"} (${row.currency_code || "-"})`}
                      </option>
                    ))}
                  </select>
                ) : (
                  <input
                    type="number"
                    min="1"
                    className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                    value={linkedCashForm.registerId}
                    onChange={(event) =>
                      setLinkedCashForm((prev) => ({
                        ...prev,
                        registerId: event.target.value,
                        cashSessionId: "",
                      }))
                    }
                    disabled={applySubmitting}
                    required
                  />
                )}
              </label>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
                cash session (optional)
                {linkedRegisterOpenSessions.length > 0 ? (
                  <select
                    className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                    value={linkedCashForm.cashSessionId}
                    onChange={(event) =>
                      setLinkedCashForm((prev) => ({ ...prev, cashSessionId: event.target.value }))
                    }
                    disabled={applySubmitting}
                  >
                    <option value="">Select open session</option>
                    {linkedRegisterOpenSessions.map((row) => (
                      <option key={`linked-session-${row.id}`} value={row.id}>
                        {`#${row.id} - ${row.cash_register_code || row.cash_register_id}`}
                      </option>
                    ))}
                  </select>
                ) : (
                  <input
                    type="number"
                    min="1"
                    className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                    value={linkedCashForm.cashSessionId}
                    onChange={(event) =>
                      setLinkedCashForm((prev) => ({ ...prev, cashSessionId: event.target.value }))
                    }
                    disabled={applySubmitting}
                  />
                )}
              </label>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
                counterAccount
                {postingAccountOptions.length > 0 ? (
                  <select
                    className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                    value={linkedCashForm.counterAccountId}
                    onChange={(event) =>
                      setLinkedCashForm((prev) => ({
                        ...prev,
                        counterAccountId: event.target.value,
                      }))
                    }
                    disabled={applySubmitting}
                    required
                  >
                    <option value="">Select account</option>
                    {postingAccountOptions.map((row) => (
                      <option key={`linked-account-${row.id}`} value={row.id}>
                        {`${row.code || row.id} - ${row.name || "-"}`}
                      </option>
                    ))}
                  </select>
                ) : (
                  <input
                    type="number"
                    min="1"
                    className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                    value={linkedCashForm.counterAccountId}
                    onChange={(event) =>
                      setLinkedCashForm((prev) => ({
                        ...prev,
                        counterAccountId: event.target.value,
                      }))
                    }
                    disabled={applySubmitting}
                    required
                  />
                )}
              </label>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
                txnDatetime
                <input
                  type="datetime-local"
                  className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                  value={linkedCashForm.txnDatetime}
                  onChange={(event) =>
                    setLinkedCashForm((prev) => ({ ...prev, txnDatetime: event.target.value }))
                  }
                  disabled={applySubmitting}
                  required
                />
              </label>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
                bookDate
                <input
                  type="date"
                  className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                  value={linkedCashForm.bookDate}
                  onChange={(event) =>
                    setLinkedCashForm((prev) => ({ ...prev, bookDate: event.target.value }))
                  }
                  disabled={applySubmitting}
                  required
                />
              </label>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
                referenceNo (optional)
                <input
                  type="text"
                  className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                  value={linkedCashForm.referenceNo}
                  onChange={(event) =>
                    setLinkedCashForm((prev) => ({ ...prev, referenceNo: event.target.value }))
                  }
                  disabled={applySubmitting}
                />
              </label>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-600 md:col-span-2">
                description (optional)
                <input
                  type="text"
                  className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
                  value={linkedCashForm.description}
                  onChange={(event) =>
                    setLinkedCashForm((prev) => ({ ...prev, description: event.target.value }))
                  }
                  disabled={applySubmitting}
                />
              </label>
              {selectedLinkedRegister &&
              toUpper(selectedLinkedRegister.currency_code) !== toUpper(applyForm.currencyCode) ? (
                <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800 md:col-span-4">
                  Register currency ({selectedLinkedRegister.currency_code}) differs from settlement
                  currency ({toUpper(applyForm.currencyCode)}). Cash creation will fail unless they match.
                </div>
              ) : null}
            </>
          ) : null}

          <div className="md:col-span-4 flex flex-wrap gap-2">
            <button
              type="submit"
              className="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
              disabled={
                !canApply ||
                applySubmitting ||
                autoAllocateBlocked ||
                applyCariNotReady
              }
            >
              {applySubmitting ? "Applying..." : "Apply Settlement"}
            </button>
            <button
              type="button"
              className="rounded-md border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700"
              onClick={() => {
                setApplyForm(buildApplyDefaultForm());
                setManualAllocationDraft({});
                setApplyError("");
                setApplyMessage("");
                setApplyReplayMessage("");
                setApplyResult(null);
                setApplyFollowUpRisks([]);
                setLinkedCashForm(buildLinkedCashDefaultForm());
                setLinkedCashError("");
                setLinkedCashMessage("");
                setLinkedCashResult(null);
                setPreviewFilters((prev) => ({
                  ...prev,
                  legalEntityId: "",
                  counterpartyId: "",
                  direction: "",
                }));
              }}
              disabled={applySubmitting}
            >
              Reset Apply Form
            </button>
          </div>
        </form>

        {autoAllocateDirectionMissing ? (
          <p className="mt-3 text-sm text-amber-700">
            Direction is required for auto-allocation.
          </p>
        ) : null}
        {applyForm.autoAllocate && mixedDirectionRisk ? (
          <p className="mt-1 text-sm text-amber-700">
            Mixed-direction risk detected in preview rows. Select one direction before auto-allocate.
          </p>
        ) : null}

        <div className="mt-4 rounded-lg border border-slate-200 p-4">
          <h3 className="text-sm font-semibold uppercase tracking-wide text-slate-700">
            Auto-allocation preview (oldest due first)
          </h3>
          {!canReadReports ? (
            <p className="mt-2 text-sm text-amber-700">
              Preview needs permission: `cari.report.read`. Settlement apply/reverse and bank
              workflows can still be submitted with their own permissions.
            </p>
          ) : null}
          {previewError ? (
            <div className="mt-2 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
              {previewError}
            </div>
          ) : null}
          <div className="mt-3 overflow-x-auto rounded-lg border border-slate-200">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-3 py-2">openItemId</th>
                  <th className="px-3 py-2">documentNo</th>
                  <th className="px-3 py-2">direction</th>
                  <th className="px-3 py-2">dueDate</th>
                  <th className="px-3 py-2">openAmountTxn</th>
                  <th className="px-3 py-2">expectedApplyTxn</th>
                  <th className="px-3 py-2">expectedResidualTxn</th>
                  <th className="px-3 py-2">manual amount</th>
                </tr>
              </thead>
              <tbody>
                {previewRows.map((row) => (
                  <tr key={`preview-${row.openItemId}`} className="border-t border-slate-100">
                    <td className="px-3 py-2">{row.openItemId}</td>
                    <td className="px-3 py-2">{row.documentNo || "-"}</td>
                    <td className="px-3 py-2">{row.direction || "-"}</td>
                    <td className="px-3 py-2">{row.dueDate || "-"}</td>
                    <td className="px-3 py-2">{formatAmount(row.openAmountTxn)}</td>
                    <td className="px-3 py-2">{formatAmount(row.expectedApplyTxn)}</td>
                    <td className="px-3 py-2">{formatAmount(row.expectedResidualTxn)}</td>
                    <td className="px-3 py-2">
                      <input
                        type="number"
                        min="0"
                        step="0.000001"
                        className="w-36 rounded-md border border-slate-300 px-2 py-1 text-xs"
                        value={manualAllocationDraft[String(row.openItemId)] || ""}
                        onChange={(event) =>
                          setManualAllocationDraft((prev) => ({
                            ...prev,
                            [String(row.openItemId)]: event.target.value,
                          }))
                        }
                        disabled={!canApply || applySubmitting || applyForm.autoAllocate}
                      />
                    </td>
                  </tr>
                ))}
                {previewRows.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="px-3 py-3 text-slate-500">
                      {previewLoading
                        ? "Loading preview..."
                        : "No preview rows. Enter legalEntityId, counterpartyId and asOfDate."}
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
          {!applyForm.autoAllocate ? (
            <p className="mt-2 text-sm text-slate-600">
              Manual allocations selected: {manualAllocations.length}
            </p>
          ) : null}
        </div>

        {applyResult ? (
          <div className="mt-4 rounded-lg border border-slate-200 p-4">
            <h3 className="text-sm font-semibold uppercase tracking-wide text-slate-700">
              Apply response blocks
            </h3>
            <dl className="mt-2 grid grid-cols-2 gap-2 text-sm">
              <dt className="font-semibold text-slate-600">settlementBatchId</dt>
              <dd>{applyResult?.row?.id || "-"}</dd>
              <dt className="font-semibold text-slate-600">settlementNo</dt>
              <dd>{applyResult?.row?.settlementNo || "-"}</dd>
              <dt className="font-semibold text-slate-600">idempotentReplay</dt>
              <dd>{String(Boolean(applyResult?.idempotentReplay))}</dd>
              <dt className="font-semibold text-slate-600">allocationCount</dt>
              <dd>{Array.isArray(applyResult?.allocations) ? applyResult.allocations.length : 0}</dd>
              <dt className="font-semibold text-slate-600">linkedCashTransactionId</dt>
              <dd>
                {applyResult?.row?.cashTransactionId ||
                  linkedCashResult?.id ||
                  "-"}
              </dd>
            </dl>
            <pre className="mt-3 overflow-x-auto rounded-md bg-slate-900 p-3 text-xs text-slate-100">
{JSON.stringify(
  {
    allocations: applyResult?.allocations || [],
    fx: applyResult?.fx || null,
    unapplied: applyResult?.unapplied || null,
  },
  null,
  2
)}
            </pre>
          </div>
        ) : null}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-900">Settlement Reverse</h2>
        {!canReverse ? (
          <div className="mt-3 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
            Missing permission: `cari.settlement.reverse`
          </div>
        ) : null}
        {reverseError ? (
          <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
            {reverseError}
          </div>
        ) : null}
        {reverseMessage ? (
          <div className="mt-3 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
            {reverseMessage}
          </div>
        ) : null}
        <form className="mt-4 grid gap-3 md:grid-cols-3" onSubmit={onReverse}>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            settlementBatchId
            <input
              type="number"
              min="1"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={reverseForm.settlementBatchId}
              onChange={(event) =>
                setReverseForm((prev) => ({ ...prev, settlementBatchId: event.target.value }))
              }
              disabled={!canReverse || reverseSubmitting}
              required
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            reversalDate
            <input
              type="date"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={reverseForm.reversalDate}
              onChange={(event) =>
                setReverseForm((prev) => ({ ...prev, reversalDate: event.target.value }))
              }
              disabled={!canReverse || reverseSubmitting}
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            reason
            <input
              type="text"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={reverseForm.reason}
              onChange={(event) =>
                setReverseForm((prev) => ({ ...prev, reason: event.target.value }))
              }
              disabled={!canReverse || reverseSubmitting}
            />
          </label>
          <div className="md:col-span-3">
            <button
              type="submit"
              className="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
              disabled={!canReverse || reverseSubmitting}
            >
              {reverseSubmitting ? "Reversing..." : "Reverse Settlement"}
            </button>
          </div>
        </form>
        {reverseResult ? (
          <pre className="mt-3 overflow-x-auto rounded-md bg-slate-900 p-3 text-xs text-slate-100">
{JSON.stringify(reverseResult, null, 2)}
          </pre>
        ) : null}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-900">Bank Attach (explicit workflow)</h2>
        {!canBankAttach ? (
          <div className="mt-3 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
            Missing permission: `cari.bank.attach`
          </div>
        ) : null}
        {bankAttachError ? (
          <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
            {bankAttachError}
          </div>
        ) : null}
        {bankAttachMessage ? (
          <div className="mt-3 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
            {bankAttachMessage}
          </div>
        ) : null}
        <form className="mt-4 grid gap-3 md:grid-cols-4" onSubmit={onBankAttach}>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            legalEntityId
            <input
              type="number"
              min="1"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankAttachForm.legalEntityId}
              onChange={(event) =>
                setBankAttachForm((prev) => ({ ...prev, legalEntityId: event.target.value }))
              }
              disabled={!canBankAttach || bankAttachSubmitting}
              required
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            targetType
            <select
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankAttachForm.targetType}
              onChange={(event) =>
                setBankAttachForm((prev) => ({ ...prev, targetType: event.target.value }))
              }
              disabled={!canBankAttach || bankAttachSubmitting}
            >
              <option value="SETTLEMENT">SETTLEMENT</option>
              <option value="UNAPPLIED_CASH">UNAPPLIED_CASH</option>
            </select>
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            settlementBatchId
            <input
              type="number"
              min="1"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankAttachForm.settlementBatchId}
              onChange={(event) =>
                setBankAttachForm((prev) => ({ ...prev, settlementBatchId: event.target.value }))
              }
              disabled={
                !canBankAttach ||
                bankAttachSubmitting ||
                String(bankAttachForm.targetType || "") === "UNAPPLIED_CASH"
              }
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            unappliedCashId
            <input
              type="number"
              min="1"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankAttachForm.unappliedCashId}
              onChange={(event) =>
                setBankAttachForm((prev) => ({ ...prev, unappliedCashId: event.target.value }))
              }
              disabled={
                !canBankAttach ||
                bankAttachSubmitting ||
                String(bankAttachForm.targetType || "") === "SETTLEMENT"
              }
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            bankStatementLineId
            <input
              type="number"
              min="1"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankAttachForm.bankStatementLineId}
              onChange={(event) =>
                setBankAttachForm((prev) => ({ ...prev, bankStatementLineId: event.target.value }))
              }
              disabled={!canBankAttach || bankAttachSubmitting}
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            bankTransactionRef
            <input
              type="text"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankAttachForm.bankTransactionRef}
              onChange={(event) =>
                setBankAttachForm((prev) => ({ ...prev, bankTransactionRef: event.target.value }))
              }
              disabled={!canBankAttach || bankAttachSubmitting}
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600 md:col-span-2">
            note
            <input
              type="text"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankAttachForm.note}
              onChange={(event) =>
                setBankAttachForm((prev) => ({ ...prev, note: event.target.value }))
              }
              disabled={!canBankAttach || bankAttachSubmitting}
            />
          </label>
          <div className="md:col-span-4">
            <button
              type="submit"
              className="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
              disabled={!canBankAttach || bankAttachSubmitting}
            >
              {bankAttachSubmitting ? "Attaching..." : "Attach Bank Reference"}
            </button>
          </div>
        </form>
        {bankAttachResult ? (
          <pre className="mt-3 overflow-x-auto rounded-md bg-slate-900 p-3 text-xs text-slate-100">
{JSON.stringify(bankAttachResult, null, 2)}
          </pre>
        ) : null}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-900">Bank Apply (explicit workflow)</h2>
        {!canBankApply ? (
          <div className="mt-3 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
            Missing permission: `cari.bank.apply`
          </div>
        ) : null}
        {bankApplyCariNotReady ? (
          <div className="mt-3 rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-900">
            <p className="font-semibold">Setup incomplete (CARI posting)</p>
            <p className="mt-1">
              Bank apply is disabled for legalEntityId={bankApplyLegalEntityId || "-"}.
            </p>
            {Array.isArray(bankApplyCariReadiness?.missingPurposeCodes) &&
            bankApplyCariReadiness.missingPurposeCodes.length > 0 ? (
              <p className="mt-1">
                Missing purpose codes:{" "}
                {bankApplyCariReadiness.missingPurposeCodes.join(", ")}
              </p>
            ) : null}
            {Array.isArray(bankApplyCariReadiness?.invalidMappings) &&
            bankApplyCariReadiness.invalidMappings.length > 0 ? (
              <ul className="mt-2 list-disc pl-5">
                {bankApplyCariReadiness.invalidMappings.map((row, index) => (
                  <li key={`bank-apply-cari-invalid-${index}`}>
                    {String(row?.purposeCode || "-")}: {formatReadinessReason(row?.reason)}
                  </li>
                ))}
              </ul>
            ) : null}
            <div className="mt-2 flex flex-wrap gap-2">
              <Link
                to="/app/ayarlar/hesap-plani-ayarlari#manual-purpose-mappings"
                className="rounded border border-amber-300 bg-white px-2.5 py-1 text-xs font-semibold text-amber-900"
              >
                Fix manually
              </Link>
              <Link
                to="/app/ayarlar/hesap-plani-ayarlari#template-wizard"
                className="rounded border border-amber-300 bg-white px-2.5 py-1 text-xs font-semibold text-amber-900"
              >
                Use template
              </Link>
            </div>
          </div>
        ) : null}
        {bankApplyError ? (
          <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
            {bankApplyError}
          </div>
        ) : null}
        {bankApplyMessage ? (
          <div className="mt-3 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
            {bankApplyMessage}
          </div>
        ) : null}
        {bankApplyFollowUpRisks.length > 0 ? (
          <div className="mt-3 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
            <p className="font-semibold">Follow-up risks</p>
            <ul className="mt-1 list-disc pl-5">
              {bankApplyFollowUpRisks.map((risk, index) => (
                <li key={`bank-apply-risk-${index}`}>{risk}</li>
              ))}
            </ul>
          </div>
        ) : null}

        <form className="mt-4 grid gap-3 md:grid-cols-4" onSubmit={onBankApply}>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            legalEntityId
            <input
              type="number"
              min="1"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankApplyForm.legalEntityId}
              onChange={(event) =>
                setBankApplyForm((prev) => ({ ...prev, legalEntityId: event.target.value }))
              }
              disabled={!canBankApply || bankApplySubmitting}
              required
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            counterpartyId
            <input
              type="number"
              min="1"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankApplyForm.counterpartyId}
              onChange={(event) =>
                setBankApplyForm((prev) => ({ ...prev, counterpartyId: event.target.value }))
              }
              disabled={!canBankApply || bankApplySubmitting}
              required
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            direction
            <select
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankApplyForm.direction}
              onChange={(event) =>
                setBankApplyForm((prev) => ({ ...prev, direction: event.target.value }))
              }
              disabled={!canBankApply || bankApplySubmitting}
            >
              <option value="">Select</option>
              <option value="AR">AR</option>
              <option value="AP">AP</option>
            </select>
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            settlementDate
            <input
              type="date"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankApplyForm.settlementDate}
              onChange={(event) =>
                setBankApplyForm((prev) => ({ ...prev, settlementDate: event.target.value }))
              }
              disabled={!canBankApply || bankApplySubmitting}
              required
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            currencyCode
            <input
              type="text"
              maxLength={3}
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal uppercase"
              value={bankApplyForm.currencyCode}
              onChange={(event) =>
                setBankApplyForm((prev) => ({ ...prev, currencyCode: event.target.value }))
              }
              disabled={!canBankApply || bankApplySubmitting}
              required
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            incomingAmountTxn
            <input
              type="number"
              min="0"
              step="0.000001"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankApplyForm.incomingAmountTxn}
              onChange={(event) =>
                setBankApplyForm((prev) => ({ ...prev, incomingAmountTxn: event.target.value }))
              }
              disabled={!canBankApply || bankApplySubmitting}
              required
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            bankStatementLineId
            <input
              type="number"
              min="1"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankApplyForm.bankStatementLineId}
              onChange={(event) =>
                setBankApplyForm((prev) => ({ ...prev, bankStatementLineId: event.target.value }))
              }
              disabled={!canBankApply || bankApplySubmitting}
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            bankTransactionRef
            <input
              type="text"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankApplyForm.bankTransactionRef}
              onChange={(event) =>
                setBankApplyForm((prev) => ({ ...prev, bankTransactionRef: event.target.value }))
              }
              disabled={!canBankApply || bankApplySubmitting}
            />
          </label>
          <label className="flex items-center gap-2 text-sm text-slate-700 md:col-span-2">
            <input
              type="checkbox"
              checked={Boolean(bankApplyForm.autoAllocate)}
              onChange={(event) =>
                setBankApplyForm((prev) => ({ ...prev, autoAllocate: event.target.checked }))
              }
              disabled={!canBankApply || bankApplySubmitting}
            />
            autoAllocate
          </label>
          <label className="flex items-center gap-2 text-sm text-slate-700 md:col-span-2">
            <input
              type="checkbox"
              checked={Boolean(bankApplyForm.useUnappliedCash)}
              onChange={(event) =>
                setBankApplyForm((prev) => ({ ...prev, useUnappliedCash: event.target.checked }))
              }
              disabled={!canBankApply || bankApplySubmitting}
            />
            useUnappliedCash
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600 md:col-span-4">
            allocations JSON (required if autoAllocate=false)
            <textarea
              rows={4}
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal font-mono"
              value={bankApplyForm.allocationsJson}
              onChange={(event) =>
                setBankApplyForm((prev) => ({ ...prev, allocationsJson: event.target.value }))
              }
              disabled={!canBankApply || bankApplySubmitting || bankApplyForm.autoAllocate}
              placeholder='[{"openItemId":123,"amountTxn":100.5}]'
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600 md:col-span-2">
            bankApplyIdempotencyKey
            <input
              type="text"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankApplyForm.bankApplyIdempotencyKey}
              onChange={(event) =>
                setBankApplyForm((prev) => ({
                  ...prev,
                  bankApplyIdempotencyKey: event.target.value,
                }))
              }
              disabled={!canBankApply || bankApplySubmitting}
            />
          </label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600 md:col-span-2">
            note
            <input
              type="text"
              className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal"
              value={bankApplyForm.note}
              onChange={(event) =>
                setBankApplyForm((prev) => ({ ...prev, note: event.target.value }))
              }
              disabled={!canBankApply || bankApplySubmitting}
            />
          </label>
          <div className="md:col-span-4">
            <button
              type="submit"
              className="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
              disabled={!canBankApply || bankApplySubmitting || bankApplyCariNotReady}
            >
              {bankApplySubmitting ? "Applying..." : "Apply Bank Settlement"}
            </button>
          </div>
        </form>
        {bankApplyResult ? (
          <pre className="mt-3 overflow-x-auto rounded-md bg-slate-900 p-3 text-xs text-slate-100">
{JSON.stringify(bankApplyResult, null, 2)}
          </pre>
        ) : null}
      </section>
    </div>
  );
}
