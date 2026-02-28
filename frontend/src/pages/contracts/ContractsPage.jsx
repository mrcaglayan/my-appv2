import { useEffect, useMemo, useState } from "react";
import { listAccounts } from "../../api/glAdmin.js";
import {
  activateContract,
  amendContract,
  adjustContractDocumentLink,
  cancelContract,
  closeContract,
  createContract,
  generateContractBilling,
  generateContractRevrec,
  getContract,
  listContractLinkableDocuments,
  linkContractDocument,
  listContractDocuments,
  listContracts,
  patchContractLine,
  suspendContract,
  unlinkContractDocumentLink,
  updateContract,
} from "../../api/contracts.js";
import { listCariCounterparties } from "../../api/cariCounterparty.js";
import { useAuth } from "../../auth/useAuth.js";
import {
  CONTRACT_LINE_STATUSES,
  CONTRACT_STATUSES,
  CONTRACT_TYPES,
  LINK_TYPES,
  RECOGNITION_METHODS,
  buildContractLinkPayload,
  buildContractBillingPayload,
  buildContractLinkAdjustmentPayload,
  buildContractLinkUnlinkPayload,
  buildContractListQuery,
  canAdjustContractLink,
  canUnlinkContractLink,
  createEmptyContractLine,
  createInitialContractForm,
  createInitialBillingForm,
  createInitialRevrecForm,
  createInitialLinkAdjustmentForm,
  createInitialLinkForm,
  createInitialLinkUnlinkForm,
  filterAccountsForContractRole,
  formatAmount,
  getCounterpartyRoleForContractType,
  getLifecycleActionStates,
  mapContractDetailToForm,
  normalizeContractFinancialRollup,
  resolveContractsPermissionGates,
  toPositiveInt,
  validateContractForm,
  validateContractBillingForm,
  validateContractRevrecForm,
  validateContractLinePatchForm,
  validateContractLinkAdjustmentForm,
  validateContractLinkForm,
  validateContractLinkUnlinkForm,
  BILLING_DOC_TYPES,
  BILLING_AMOUNT_STRATEGIES,
  REVREC_GENERATION_MODES,
} from "./contractsUtils.js";

const DEFAULT_FILTERS = {
  legalEntityId: "",
  counterpartyId: "",
  contractType: "",
  status: "",
  q: "",
  limit: 100,
  offset: 0,
};

function toUpper(value) {
  return String(value || "").trim().toUpperCase();
}

function todayDateOnly() {
  return new Date().toISOString().slice(0, 10);
}

function clampPercent(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  if (parsed < 0) {
    return 0;
  }
  if (parsed > 100) {
    return 100;
  }
  return parsed;
}

function normalizeApiError(error, fallback = "Operation failed.") {
  const message = String(error?.message || error?.response?.data?.message || fallback).trim();
  const requestId = String(error?.requestId || error?.response?.data?.requestId || "").trim();
  return requestId ? `${message || fallback} (requestId: ${requestId})` : message || fallback;
}

export default function ContractsPage() {
  const { permissions } = useAuth();
  const gates = useMemo(() => resolveContractsPermissionGates(permissions), [permissions]);

  const [filters, setFilters] = useState(DEFAULT_FILTERS);
  const [rows, setRows] = useState([]);
  const [totalRows, setTotalRows] = useState(0);
  const [listLoading, setListLoading] = useState(false);
  const [listError, setListError] = useState("");

  const [selectedContractId, setSelectedContractId] = useState(null);
  const [selectedContractDetail, setSelectedContractDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");

  const [documentLinks, setDocumentLinks] = useState([]);
  const [linksError, setLinksError] = useState("");

  const [formMode, setFormMode] = useState("create");
  const [contractForm, setContractForm] = useState(() => createInitialContractForm());
  const [formSaving, setFormSaving] = useState(false);
  const [formError, setFormError] = useState("");
  const [formMessage, setFormMessage] = useState("");
  const [amendReason, setAmendReason] = useState("");
  const [linePatchReason, setLinePatchReason] = useState("");
  const [linePatchSavingId, setLinePatchSavingId] = useState(null);

  const [lifecycleLoading, setLifecycleLoading] = useState("");
  const [lifecycleError, setLifecycleError] = useState("");
  const [lifecycleMessage, setLifecycleMessage] = useState("");

  const [billingForm, setBillingForm] = useState(() => createInitialBillingForm());
  const [billingSaving, setBillingSaving] = useState(false);
  const [billingError, setBillingError] = useState("");
  const [billingMessage, setBillingMessage] = useState("");
  const [billingResult, setBillingResult] = useState(null);
  const [revrecForm, setRevrecForm] = useState(() => createInitialRevrecForm());
  const [revrecSaving, setRevrecSaving] = useState(false);
  const [revrecError, setRevrecError] = useState("");
  const [revrecMessage, setRevrecMessage] = useState("");
  const [revrecResult, setRevrecResult] = useState(null);

  const [linkForm, setLinkForm] = useState(() => createInitialLinkForm());
  const [linkSaving, setLinkSaving] = useState(false);
  const [linkError, setLinkError] = useState("");
  const [linkMessage, setLinkMessage] = useState("");
  const [linkAdjustmentForm, setLinkAdjustmentForm] = useState(() =>
    createInitialLinkAdjustmentForm()
  );
  const [linkUnlinkForm, setLinkUnlinkForm] = useState(() =>
    createInitialLinkUnlinkForm()
  );
  const [linkAdjustmentSaving, setLinkAdjustmentSaving] = useState(false);
  const [linkUnlinkSaving, setLinkUnlinkSaving] = useState(false);
  const [linkActionError, setLinkActionError] = useState("");
  const [linkActionMessage, setLinkActionMessage] = useState("");

  const [counterpartyOptions, setCounterpartyOptions] = useState([]);
  const [accountOptions, setAccountOptions] = useState([]);
  const [documentPickerRows, setDocumentPickerRows] = useState([]);

  const selectedContractRow = useMemo(
    () => rows.find((row) => Number(row?.id || 0) === Number(selectedContractId || 0)) || null,
    [rows, selectedContractId]
  );
  const selectedContract = selectedContractDetail || selectedContractRow;
  const financialRollup = useMemo(
    () =>
      normalizeContractFinancialRollup(
        selectedContract?.financialRollup,
        selectedContract?.contractType
      ),
    [selectedContract?.financialRollup, selectedContract?.contractType]
  );
  const collectionProgressPct = clampPercent(
    financialRollup?.collectedCoveragePct ||
      (financialRollup?.billedAmountBase
        ? (Number(financialRollup?.collectedAmountBase || 0) /
            Number(financialRollup?.billedAmountBase || 0)) *
          100
        : 0)
  );
  const recognitionProgressPct = clampPercent(
    financialRollup?.recognizedCoveragePct ||
      (financialRollup?.revrecScheduledAmountBase
        ? (Number(financialRollup?.recognizedToDateBase || 0) /
            Number(financialRollup?.revrecScheduledAmountBase || 0)) *
          100
        : 0)
  );
  const lifecycleStates = useMemo(
    () => getLifecycleActionStates(selectedContract?.status, gates),
    [selectedContract?.status, gates]
  );

  const selectedStatus = toUpper(selectedContract?.status);
  const canDraftEditSelected = gates.canUpsertContract && selectedStatus === "DRAFT";
  const canAmendSelected =
    gates.canUpsertContract && (selectedStatus === "ACTIVE" || selectedStatus === "SUSPENDED");

  const deferredAccountOptions = useMemo(
    () => filterAccountsForContractRole(accountOptions, contractForm.contractType, "deferred"),
    [accountOptions, contractForm.contractType]
  );
  const revenueAccountOptions = useMemo(
    () => filterAccountsForContractRole(accountOptions, contractForm.contractType, "revenue"),
    [accountOptions, contractForm.contractType]
  );

  async function loadContracts(nextFilters = filters) {
    if (!gates.canReadContractsRoute) {
      setRows([]);
      setTotalRows(0);
      setListError("Missing permission: contract.read");
      return;
    }
    setListLoading(true);
    setListError("");
    try {
      const response = await listContracts(buildContractListQuery(nextFilters));
      setRows(Array.isArray(response?.rows) ? response.rows : []);
      setTotalRows(Number(response?.total || 0));
    } catch (error) {
      setRows([]);
      setTotalRows(0);
      setListError(normalizeApiError(error, "Failed to load contracts."));
    } finally {
      setListLoading(false);
    }
  }

  async function loadContractDetail(contractId) {
    if (!gates.canReadContractsRoute || !toPositiveInt(contractId)) {
      setSelectedContractDetail(null);
      setDocumentLinks([]);
      return null;
    }
    setDetailLoading(true);
    setDetailError("");
    setLinksError("");
    try {
      const [detailResponse, linksResponse] = await Promise.all([
        getContract(contractId),
        listContractDocuments(contractId),
      ]);
      setSelectedContractDetail(detailResponse?.row || null);
      setDocumentLinks(Array.isArray(linksResponse?.rows) ? linksResponse.rows : []);
      return detailResponse?.row || null;
    } catch (error) {
      const message = normalizeApiError(error, "Failed to load contract detail.");
      setSelectedContractDetail(null);
      setDocumentLinks([]);
      setDetailError(message);
      setLinksError(message);
      return null;
    } finally {
      setDetailLoading(false);
    }
  }

  useEffect(() => {
    loadContracts(DEFAULT_FILTERS);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [gates.canReadContractsRoute]);

  useEffect(() => {
    if (!toPositiveInt(selectedContractId)) {
      setSelectedContractDetail(null);
      setDocumentLinks([]);
      setBillingForm(createInitialBillingForm());
      setBillingError("");
      setBillingMessage("");
      setBillingResult(null);
      setRevrecForm(createInitialRevrecForm());
      setRevrecError("");
      setRevrecMessage("");
      setRevrecResult(null);
      setLinkAdjustmentForm(createInitialLinkAdjustmentForm());
      setLinkUnlinkForm(createInitialLinkUnlinkForm());
      setAmendReason("");
      setLinePatchReason("");
      setLinePatchSavingId(null);
      return;
    }
    setBillingForm(createInitialBillingForm());
    setBillingError("");
    setBillingMessage("");
    setBillingResult(null);
    setRevrecForm(createInitialRevrecForm());
    setRevrecError("");
    setRevrecMessage("");
    setRevrecResult(null);
    setLinkAdjustmentForm(createInitialLinkAdjustmentForm());
    setLinkUnlinkForm(createInitialLinkUnlinkForm());
    setLinePatchSavingId(null);
    setLinkActionError("");
    setLinkActionMessage("");
    loadContractDetail(selectedContractId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedContractId, gates.canReadContractsRoute]);

  useEffect(() => {
    const legalEntityId = toPositiveInt(contractForm.legalEntityId);
    if (!gates.shouldFetchCounterparties || !legalEntityId) {
      setCounterpartyOptions([]);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const response = await listCariCounterparties({
          legalEntityId,
          role: getCounterpartyRoleForContractType(contractForm.contractType),
          status: "ACTIVE",
          limit: 100,
          offset: 0,
        });
        if (!cancelled) {
          setCounterpartyOptions(Array.isArray(response?.rows) ? response.rows : []);
        }
      } catch {
        if (!cancelled) {
          setCounterpartyOptions([]);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [contractForm.contractType, contractForm.legalEntityId, gates.shouldFetchCounterparties]);

  useEffect(() => {
    const legalEntityId = toPositiveInt(contractForm.legalEntityId);
    if (!gates.shouldFetchAccounts || !legalEntityId) {
      setAccountOptions([]);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const response = await listAccounts({ legalEntityId, includeInactive: false, limit: 600 });
        if (!cancelled) {
          setAccountOptions(Array.isArray(response?.rows) ? response.rows : []);
        }
      } catch {
        if (!cancelled) {
          setAccountOptions([]);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [contractForm.legalEntityId, gates.shouldFetchAccounts]);

  useEffect(() => {
    const contractId = toPositiveInt(selectedContractId);
    if (!gates.shouldFetchDocuments || !contractId) {
      setDocumentPickerRows([]);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const response = await listContractLinkableDocuments(contractId, {
          limit: 100,
          offset: 0,
        });
        if (!cancelled) {
          setDocumentPickerRows(Array.isArray(response?.rows) ? response.rows : []);
        }
      } catch {
        if (!cancelled) {
          setDocumentPickerRows([]);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [gates.shouldFetchDocuments, selectedContractId]);

  useEffect(() => {
    const lines = Array.isArray(selectedContract?.lines) ? selectedContract.lines : [];
    if (lines.length === 0) {
      return;
    }
    setBillingForm((prev) => {
      const hasSelected = Array.isArray(prev.selectedLineIds) && prev.selectedLineIds.length > 0;
      const nextSelected = hasSelected
        ? prev.selectedLineIds
        : lines
            .filter((line) => toUpper(line?.status) === "ACTIVE")
            .map((line) => String(line?.id || ""))
            .filter((entry) => toPositiveInt(entry));
      const nextBillingDate = prev.billingDate || todayDateOnly();
      if (hasSelected && prev.billingDate) {
        return prev;
      }
      return {
        ...prev,
        selectedLineIds: nextSelected,
        billingDate: nextBillingDate,
      };
    });
    setRevrecForm((prev) => {
      const hasSelected = Array.isArray(prev.contractLineIds) && prev.contractLineIds.length > 0;
      if (hasSelected) {
        return prev;
      }
      return {
        ...prev,
        contractLineIds: lines
          .filter((line) => toUpper(line?.status) === "ACTIVE")
          .map((line) => String(line?.id || ""))
          .filter((entry) => toPositiveInt(entry)),
      };
    });
  }, [selectedContract]);

  function handleStartCreate() {
    setFormMode("create");
    setContractForm(createInitialContractForm());
    setAmendReason("");
    setLinePatchReason("");
    setLinePatchSavingId(null);
    setBillingForm(createInitialBillingForm());
    setBillingError("");
    setBillingMessage("");
    setBillingResult(null);
    setRevrecForm(createInitialRevrecForm());
    setRevrecError("");
    setRevrecMessage("");
    setRevrecResult(null);
    setSelectedContractId(null);
    setSelectedContractDetail(null);
    setDocumentLinks([]);
    setLinkAdjustmentForm(createInitialLinkAdjustmentForm());
    setLinkUnlinkForm(createInitialLinkUnlinkForm());
    setFormError("");
    setFormMessage("");
    setLinkActionError("");
    setLinkActionMessage("");
  }

  function handleLoadSelectedForEdit() {
    if (!selectedContract) {
      setFormError("Select a contract first.");
      return;
    }
    if (!Array.isArray(selectedContract?.lines)) {
      setFormError("Load contract detail first, then retry edit/amend.");
      return;
    }
    const status = toUpper(selectedContract.status);
    if (status === "DRAFT") {
      setFormMode("edit");
    } else if (status === "ACTIVE" || status === "SUSPENDED") {
      setFormMode("amend");
    } else {
      setFormError("Only DRAFT, ACTIVE, or SUSPENDED contracts can be edited/amended.");
      return;
    }
    setContractForm(mapContractDetailToForm(selectedContract));
    setAmendReason("");
    setLinePatchReason("");
    setLinePatchSavingId(null);
    setFormError("");
    setFormMessage("");
  }

  function handleLineChange(index, field, value) {
    setContractForm((prev) => {
      const lines = Array.isArray(prev.lines) ? [...prev.lines] : [];
      if (!lines[index]) {
        return prev;
      }
      lines[index] = { ...lines[index], [field]: value };
      return { ...prev, lines };
    });
  }

  function addLine() {
    setContractForm((prev) => ({
      ...prev,
      lines: [...(Array.isArray(prev.lines) ? prev.lines : []), createEmptyContractLine()],
    }));
  }

  function removeLine(index) {
    setContractForm((prev) => {
      const lines = Array.isArray(prev.lines) ? [...prev.lines] : [];
      if (lines.length <= 1) {
        return prev;
      }
      lines.splice(index, 1);
      return { ...prev, lines };
    });
  }

  async function handlePatchLine(index) {
    const contractId = toPositiveInt(selectedContractId);
    if (!contractId) {
      setFormError("Select a contract first.");
      return;
    }
    if (!canAmendSelected) {
      setFormError("Line patch is allowed only for selected ACTIVE/SUSPENDED contracts.");
      return;
    }

    const lines = Array.isArray(contractForm.lines) ? contractForm.lines : [];
    const line = lines[index];
    const lineId = toPositiveInt(line?.id);
    if (!lineId) {
      setFormError("Selected line does not have a persisted id.");
      return;
    }

    const { payload, errors } = validateContractLinePatchForm(line, linePatchReason);
    if (errors.length > 0) {
      setFormError(errors.join(" "));
      return;
    }

    setLinePatchSavingId(lineId);
    setFormError("");
    setFormMessage("");
    try {
      await patchContractLine(contractId, lineId, payload);
      setFormMessage(`Line ${lineId} patched.`);
      setLinePatchReason("");
      const updatedDetail = await loadContractDetail(contractId);
      if (updatedDetail) {
        setContractForm(mapContractDetailToForm(updatedDetail));
      }
      await loadContracts(filters);
    } catch (error) {
      setFormError(normalizeApiError(error, "Failed to patch contract line."));
    } finally {
      setLinePatchSavingId(null);
    }
  }

  async function handleSubmitContract(event) {
    event.preventDefault();
    if (!gates.canUpsertContract) {
      setFormError("Missing permission: contract.upsert");
      return;
    }

    setFormSaving(true);
    setFormError("");
    setFormMessage("");
    try {
      const { payload, errors } = validateContractForm(contractForm);
      if (errors.length > 0) {
        setFormError(errors.join(" "));
        return;
      }

      if (formMode === "edit") {
        const contractId = toPositiveInt(selectedContractId);
        if (!contractId || !canDraftEditSelected) {
          setFormError("Only selected DRAFT contracts can be edited.");
          return;
        }
        await updateContract(contractId, payload);
        setFormMessage("Contract updated.");
        const updatedDetail = await loadContractDetail(contractId);
        if (updatedDetail) {
          setContractForm(mapContractDetailToForm(updatedDetail));
        }
        await loadContracts(filters);
      } else if (formMode === "amend") {
        const contractId = toPositiveInt(selectedContractId);
        if (!contractId || !canAmendSelected) {
          setFormError("Only selected ACTIVE/SUSPENDED contracts can be amended.");
          return;
        }
        const reason = String(amendReason || "").trim();
        if (!reason) {
          setFormError("reason is required for amendment.");
          return;
        }
        await amendContract(contractId, { ...payload, reason });
        setFormMessage("Contract amended.");
        const updatedDetail = await loadContractDetail(contractId);
        if (updatedDetail) {
          setContractForm(mapContractDetailToForm(updatedDetail));
        }
        await loadContracts(filters);
      } else {
        const response = await createContract(payload);
        const createdId = toPositiveInt(response?.row?.id);
        setFormMessage(`Contract created. id=${createdId || "-"}`);
        if (createdId) {
          setSelectedContractId(createdId);
        }
        await loadContracts(filters);
      }
    } catch (error) {
      setFormError(normalizeApiError(error, "Failed to save contract."));
    } finally {
      setFormSaving(false);
    }
  }

  async function handleLifecycleAction(action) {
    const contractId = toPositiveInt(selectedContractId);
    if (!contractId) {
      setLifecycleError("Select a contract first.");
      return;
    }

    const actionState = lifecycleStates?.[action];
    if (!actionState?.allowed) {
      setLifecycleError(actionState?.reason || `Action ${action} is not allowed.`);
      return;
    }

    setLifecycleLoading(action);
    setLifecycleError("");
    setLifecycleMessage("");
    try {
      if (action === "activate") {
        await activateContract(contractId);
      } else if (action === "suspend") {
        await suspendContract(contractId);
      } else if (action === "close") {
        await closeContract(contractId);
      } else if (action === "cancel") {
        await cancelContract(contractId);
      }
      setLifecycleMessage(`Contract ${action} action completed.`);
      await Promise.all([loadContracts(filters), loadContractDetail(contractId)]);
    } catch (error) {
      setLifecycleError(normalizeApiError(error, `Failed to ${action} contract.`));
    } finally {
      setLifecycleLoading("");
    }
  }

  function handleToggleBillingLine(lineId, checked) {
    const normalizedLineId = String(lineId || "").trim();
    if (!toPositiveInt(normalizedLineId)) {
      return;
    }
    setBillingForm((prev) => {
      const current = new Set(
        (Array.isArray(prev.selectedLineIds) ? prev.selectedLineIds : []).map((entry) =>
          String(entry || "").trim()
        )
      );
      if (checked) {
        current.add(normalizedLineId);
      } else {
        current.delete(normalizedLineId);
      }
      return {
        ...prev,
        selectedLineIds: Array.from(current),
      };
    });
  }

  function handleSelectAllBillingLines() {
    const lineIds = (Array.isArray(selectedContract?.lines) ? selectedContract.lines : [])
      .filter((line) => toUpper(line?.status) === "ACTIVE")
      .map((line) => String(line?.id || ""))
      .filter((value) => toPositiveInt(value));
    setBillingForm((prev) => ({
      ...prev,
      selectedLineIds: lineIds,
    }));
  }

  function handleClearBillingLines() {
    setBillingForm((prev) => ({
      ...prev,
      selectedLineIds: [],
    }));
  }

  async function handleGenerateBilling(event) {
    event.preventDefault();
    const contractId = toPositiveInt(selectedContractId);
    if (!contractId) {
      setBillingError("Select a contract first.");
      return;
    }
    if (!gates.canGenerateBilling) {
      setBillingError("Missing permission: contract.link_document");
      return;
    }

    setBillingSaving(true);
    setBillingError("");
    setBillingMessage("");
    setBillingResult(null);
    try {
      const { payload, errors } = validateContractBillingForm(billingForm);
      if (errors.length > 0) {
        setBillingError(errors.join(" "));
        return;
      }
      const requestPayload = buildContractBillingPayload(payload);
      const response = await generateContractBilling(contractId, requestPayload);
      const replay = Boolean(response?.idempotentReplay);
      const documentLabel = response?.document?.documentNo || response?.document?.id || "-";
      const linkLabel = response?.link?.linkId || "-";
      setBillingResult(response || null);
      setBillingMessage(
        `${replay ? "Idempotent replay returned" : "Billing generated"} (doc: ${documentLabel}, link: ${linkLabel}).`
      );
      setBillingForm((prev) => ({
        ...prev,
        idempotencyKey: createInitialBillingForm().idempotencyKey,
        integrationEventUid: "",
      }));
      await Promise.all([loadContracts(filters), loadContractDetail(contractId)]);
    } catch (error) {
      setBillingError(normalizeApiError(error, "Failed to generate billing."));
    } finally {
      setBillingSaving(false);
    }
  }

  function handleToggleRevrecLine(lineId, checked) {
    const normalizedLineId = String(lineId || "").trim();
    if (!toPositiveInt(normalizedLineId)) {
      return;
    }
    setRevrecForm((prev) => {
      const current = new Set(
        (Array.isArray(prev.contractLineIds) ? prev.contractLineIds : []).map((entry) =>
          String(entry || "").trim()
        )
      );
      if (checked) {
        current.add(normalizedLineId);
      } else {
        current.delete(normalizedLineId);
      }
      return {
        ...prev,
        contractLineIds: Array.from(current),
      };
    });
  }

  function handleSelectAllRevrecLines() {
    const lineIds = (Array.isArray(selectedContract?.lines) ? selectedContract.lines : [])
      .filter((line) => toUpper(line?.status) === "ACTIVE")
      .map((line) => String(line?.id || ""))
      .filter((value) => toPositiveInt(value));
    setRevrecForm((prev) => ({
      ...prev,
      contractLineIds: lineIds,
    }));
  }

  function handleClearRevrecLines() {
    setRevrecForm((prev) => ({
      ...prev,
      contractLineIds: [],
    }));
  }

  async function handleGenerateRevrec(event) {
    event.preventDefault();
    const contractId = toPositiveInt(selectedContractId);
    if (!contractId) {
      setRevrecError("Select a contract first.");
      return;
    }
    if (!gates.canGenerateRevrec) {
      setRevrecError("Missing permission: revenue.schedule.generate");
      return;
    }

    setRevrecSaving(true);
    setRevrecError("");
    setRevrecMessage("");
    setRevrecResult(null);
    try {
      const { payload, errors } = validateContractRevrecForm(revrecForm);
      if (errors.length > 0) {
        setRevrecError(errors.join(" "));
        return;
      }
      const response = await generateContractRevrec(contractId, payload);
      setRevrecResult(response || null);
      setRevrecMessage(
        `${response?.idempotentReplay ? "Idempotent replay returned" : "RevRec schedules generated"} ` +
          `(schedules: ${response?.generatedScheduleCount || 0}, lines: ${
            response?.generatedLineCount || 0
          }, skipped: ${response?.skippedLineCount || 0}).`
      );
    } catch (error) {
      setRevrecError(normalizeApiError(error, "Failed to generate RevRec schedules."));
    } finally {
      setRevrecSaving(false);
    }
  }

  async function handleLinkDocument(event) {
    event.preventDefault();
    const contractId = toPositiveInt(selectedContractId);
    if (!contractId) {
      setLinkError("Select a contract first.");
      return;
    }
    if (!gates.canLinkDocument) {
      setLinkError("Missing permission: contract.link_document");
      return;
    }

    setLinkSaving(true);
    setLinkError("");
    setLinkMessage("");
    try {
      const { payload, errors } = validateContractLinkForm(linkForm);
      if (errors.length > 0) {
        setLinkError(errors.join(" "));
        return;
      }
      await linkContractDocument(contractId, buildContractLinkPayload(payload));
      setLinkMessage("Document linked.");
      setLinkForm(createInitialLinkForm());
      await loadContractDetail(contractId);
    } catch (error) {
      setLinkError(normalizeApiError(error, "Failed to link document."));
    } finally {
      setLinkSaving(false);
    }
  }

  function handleSelectLinkForAdjustment(row) {
    const linkId = toPositiveInt(row?.linkId ?? row?.id);
    if (!linkId) {
      setLinkActionError("Selected link row is missing linkId.");
      return;
    }

    setLinkActionError("");
    setLinkActionMessage("");
    setLinkAdjustmentForm({
      linkId: String(linkId),
      nextLinkedAmountTxn:
        row?.linkedAmountTxn === undefined || row?.linkedAmountTxn === null
          ? ""
          : String(row.linkedAmountTxn),
      nextLinkedAmountBase:
        row?.linkedAmountBase === undefined || row?.linkedAmountBase === null
          ? ""
          : String(row.linkedAmountBase),
      reason: "",
    });
  }

  function handleSelectLinkForUnlink(row) {
    const linkId = toPositiveInt(row?.linkId ?? row?.id);
    if (!linkId) {
      setLinkActionError("Selected link row is missing linkId.");
      return;
    }

    setLinkActionError("");
    setLinkActionMessage("");
    setLinkUnlinkForm({
      linkId: String(linkId),
      reason: "",
    });
  }

  async function handleAdjustLink(event) {
    event.preventDefault();
    const contractId = toPositiveInt(selectedContractId);
    if (!contractId) {
      setLinkActionError("Select a contract first.");
      return;
    }
    if (!gates.canLinkDocument) {
      setLinkActionError("Missing permission: contract.link_document");
      return;
    }

    const row = documentLinks.find(
      (candidate) =>
        toPositiveInt(candidate?.linkId ?? candidate?.id) ===
        toPositiveInt(linkAdjustmentForm.linkId)
    );
    const state = canAdjustContractLink(row, gates);
    if (!state.allowed) {
      setLinkActionError(state.reason || "Selected link cannot be adjusted.");
      return;
    }

    setLinkAdjustmentSaving(true);
    setLinkActionError("");
    setLinkActionMessage("");
    try {
      const { payload, errors } = validateContractLinkAdjustmentForm(linkAdjustmentForm);
      if (errors.length > 0) {
        setLinkActionError(errors.join(" "));
        return;
      }
      const requestPayload = buildContractLinkAdjustmentPayload(payload);
      await adjustContractDocumentLink(contractId, payload.linkId, {
        nextLinkedAmountTxn: requestPayload.nextLinkedAmountTxn,
        nextLinkedAmountBase: requestPayload.nextLinkedAmountBase,
        reason: requestPayload.reason,
      });
      setLinkActionMessage(`Link ${payload.linkId} adjusted.`);
      setLinkAdjustmentForm(createInitialLinkAdjustmentForm());
      await loadContractDetail(contractId);
    } catch (error) {
      setLinkActionError(normalizeApiError(error, "Failed to adjust link."));
    } finally {
      setLinkAdjustmentSaving(false);
    }
  }

  async function handleUnlinkLink(event) {
    event.preventDefault();
    const contractId = toPositiveInt(selectedContractId);
    if (!contractId) {
      setLinkActionError("Select a contract first.");
      return;
    }
    if (!gates.canLinkDocument) {
      setLinkActionError("Missing permission: contract.link_document");
      return;
    }

    const row = documentLinks.find(
      (candidate) =>
        toPositiveInt(candidate?.linkId ?? candidate?.id) ===
        toPositiveInt(linkUnlinkForm.linkId)
    );
    const state = canUnlinkContractLink(row, gates);
    if (!state.allowed) {
      setLinkActionError(state.reason || "Selected link cannot be unlinked.");
      return;
    }

    setLinkUnlinkSaving(true);
    setLinkActionError("");
    setLinkActionMessage("");
    try {
      const { payload, errors } = validateContractLinkUnlinkForm(linkUnlinkForm);
      if (errors.length > 0) {
        setLinkActionError(errors.join(" "));
        return;
      }
      const requestPayload = buildContractLinkUnlinkPayload(payload);
      await unlinkContractDocumentLink(contractId, payload.linkId, {
        reason: requestPayload.reason,
      });
      setLinkActionMessage(`Link ${payload.linkId} unlinked.`);
      setLinkUnlinkForm(createInitialLinkUnlinkForm());
      setLinkAdjustmentForm(createInitialLinkAdjustmentForm());
      await loadContractDetail(contractId);
    } catch (error) {
      setLinkActionError(normalizeApiError(error, "Failed to unlink link."));
    } finally {
      setLinkUnlinkSaving(false);
    }
  }

  if (!gates.canReadContractsRoute) {
    return (
      <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
        Missing permission: <code>contract.read</code>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h1 className="text-xl font-semibold text-slate-900">Contracts</h1>
        {!gates.canReadCounterpartyPicker ? (
          <p className="mt-1 text-xs text-amber-700">Picker disabled: cari.card.read</p>
        ) : null}
        {!gates.canReadAccountPicker ? (
          <p className="mt-1 text-xs text-amber-700">Picker disabled: gl.account.read</p>
        ) : null}
        {!gates.canReadDocumentPicker ? (
          <p className="mt-1 text-xs text-amber-700">Picker disabled: contract.link_document</p>
        ) : null}

        {listError ? <div className="mt-2 text-sm text-rose-700">{listError}</div> : null}
        <div className="mt-3 grid gap-2 md:grid-cols-5">
          <input
            className="rounded border border-slate-300 px-2 py-1 text-sm"
            placeholder="legalEntityId"
            value={filters.legalEntityId}
            onChange={(event) => setFilters((prev) => ({ ...prev, legalEntityId: event.target.value }))}
          />
          <input
            className="rounded border border-slate-300 px-2 py-1 text-sm"
            placeholder="counterpartyId"
            value={filters.counterpartyId}
            onChange={(event) => setFilters((prev) => ({ ...prev, counterpartyId: event.target.value }))}
          />
          <select
            className="rounded border border-slate-300 px-2 py-1 text-sm"
            value={filters.contractType}
            onChange={(event) => setFilters((prev) => ({ ...prev, contractType: event.target.value }))}
          >
            <option value="">ALL TYPES</option>
            {CONTRACT_TYPES.map((type) => (
              <option key={type} value={type}>{type}</option>
            ))}
          </select>
          <select
            className="rounded border border-slate-300 px-2 py-1 text-sm"
            value={filters.status}
            onChange={(event) => setFilters((prev) => ({ ...prev, status: event.target.value }))}
          >
            <option value="">ALL STATUS</option>
            {CONTRACT_STATUSES.map((status) => (
              <option key={status} value={status}>{status}</option>
            ))}
          </select>
          <input
            className="rounded border border-slate-300 px-2 py-1 text-sm"
            placeholder="search"
            value={filters.q}
            onChange={(event) => setFilters((prev) => ({ ...prev, q: event.target.value }))}
          />
        </div>
        <div className="mt-2 flex gap-2">
          <button className="rounded bg-slate-900 px-3 py-1 text-sm text-white" onClick={() => loadContracts(filters)} disabled={listLoading}>
            {listLoading ? "Loading..." : "Refresh"}
          </button>
          <button className="rounded border border-slate-300 px-3 py-1 text-sm" onClick={handleStartCreate}>New</button>
          <button className="rounded border border-slate-300 px-3 py-1 text-sm" onClick={handleLoadSelectedForEdit} disabled={!selectedContract}>Edit Selected</button>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <div className="text-sm text-slate-600">Total contracts: {totalRows}</div>
        <div className="mt-2 overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="text-left text-slate-600">
              <tr>
                <th>ID</th><th>No</th><th>Type</th><th>Status</th><th>Total</th><th></th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.id} className="border-t border-slate-200">
                  <td>{row.id}</td>
                  <td>{row.contractNo}</td>
                  <td>{row.contractType}</td>
                  <td>{row.status}</td>
                  <td>{formatAmount(row.totalAmountBase)}</td>
                  <td>
                    <button className="rounded border border-slate-300 px-2 py-1 text-xs" onClick={() => setSelectedContractId(row.id)}>
                      Select
                    </button>
                  </td>
                </tr>
              ))}
              {rows.length === 0 ? (
                <tr><td colSpan={6} className="py-3 text-slate-500">No rows.</td></tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="font-semibold text-slate-900">
          {formMode === "edit"
            ? "Edit Draft"
            : formMode === "amend"
              ? "Amend Active/Suspended"
              : "Create Draft"}
        </h2>
        {formError ? <div className="mt-2 text-sm text-rose-700">{formError}</div> : null}
        {formMessage ? <div className="mt-2 text-sm text-emerald-700">{formMessage}</div> : null}

        <form className="mt-3 space-y-3" onSubmit={handleSubmitContract}>
          <div className="grid gap-2 md:grid-cols-4">
            <input className="rounded border border-slate-300 px-2 py-1 text-sm" placeholder="legalEntityId" value={contractForm.legalEntityId} onChange={(event) => setContractForm((prev) => ({ ...prev, legalEntityId: event.target.value }))} />
            {gates.canReadCounterpartyPicker ? (
              <select className="rounded border border-slate-300 px-2 py-1 text-sm" value={contractForm.counterpartyId} onChange={(event) => setContractForm((prev) => ({ ...prev, counterpartyId: event.target.value }))}>
                <option value="">counterparty</option>
                {counterpartyOptions.map((row) => <option key={row.id} value={row.id}>{row.code || row.id} - {row.name || "-"}</option>)}
              </select>
            ) : (
              <input className="rounded border border-slate-300 px-2 py-1 text-sm" placeholder="counterpartyId" value={contractForm.counterpartyId} onChange={(event) => setContractForm((prev) => ({ ...prev, counterpartyId: event.target.value }))} />
            )}
            <input className="rounded border border-slate-300 px-2 py-1 text-sm" placeholder="contractNo" value={contractForm.contractNo} onChange={(event) => setContractForm((prev) => ({ ...prev, contractNo: event.target.value }))} />
            <select className="rounded border border-slate-300 px-2 py-1 text-sm" value={contractForm.contractType} onChange={(event) => setContractForm((prev) => ({ ...prev, contractType: event.target.value }))}>
              {CONTRACT_TYPES.map((type) => <option key={type} value={type}>{type}</option>)}
            </select>
            <input className="rounded border border-slate-300 px-2 py-1 text-sm" placeholder="currencyCode" value={contractForm.currencyCode} onChange={(event) => setContractForm((prev) => ({ ...prev, currencyCode: event.target.value }))} />
            <input type="date" className="rounded border border-slate-300 px-2 py-1 text-sm" value={contractForm.startDate} onChange={(event) => setContractForm((prev) => ({ ...prev, startDate: event.target.value }))} />
            <input type="date" className="rounded border border-slate-300 px-2 py-1 text-sm" value={contractForm.endDate} onChange={(event) => setContractForm((prev) => ({ ...prev, endDate: event.target.value }))} />
            <input className="rounded border border-slate-300 px-2 py-1 text-sm md:col-span-4" placeholder="notes" value={contractForm.notes} onChange={(event) => setContractForm((prev) => ({ ...prev, notes: event.target.value }))} />
          </div>

          <div className="space-y-2 rounded border border-slate-200 p-3">
            {formMode === "amend" ? (
              <div className="grid gap-2 md:grid-cols-2">
                <input
                  className="rounded border border-slate-300 px-2 py-1 text-sm"
                  placeholder="amendment reason (required)"
                  value={amendReason}
                  onChange={(event) => setAmendReason(event.target.value)}
                />
                <input
                  className="rounded border border-slate-300 px-2 py-1 text-sm"
                  placeholder="line patch reason"
                  value={linePatchReason}
                  onChange={(event) => setLinePatchReason(event.target.value)}
                />
              </div>
            ) : null}
            <div className="flex items-center justify-between">
              <div className="text-sm font-semibold text-slate-700">Lines</div>
              <button type="button" className="rounded border border-slate-300 px-2 py-1 text-xs" onClick={addLine}>Add line</button>
            </div>
            {(Array.isArray(contractForm.lines) ? contractForm.lines : []).map((line, index) => (
              <div key={`line-${index}`} className="grid gap-2 md:grid-cols-5">
                <input className="rounded border border-slate-300 px-2 py-1 text-sm md:col-span-2" placeholder="description" value={line.description} onChange={(event) => handleLineChange(index, "description", event.target.value)} />
                <input className="rounded border border-slate-300 px-2 py-1 text-sm" placeholder="amountTxn" value={line.lineAmountTxn} onChange={(event) => handleLineChange(index, "lineAmountTxn", event.target.value)} />
                <input className="rounded border border-slate-300 px-2 py-1 text-sm" placeholder="amountBase" value={line.lineAmountBase} onChange={(event) => handleLineChange(index, "lineAmountBase", event.target.value)} />
                <button type="button" className="rounded border border-rose-300 px-2 py-1 text-xs text-rose-700" onClick={() => removeLine(index)} disabled={(contractForm.lines || []).length <= 1}>Remove</button>
                {formMode === "amend" ? (
                  <button
                    type="button"
                    className="rounded border border-sky-300 px-2 py-1 text-xs text-sky-700 disabled:opacity-60"
                    onClick={() => handlePatchLine(index)}
                    disabled={!toPositiveInt(line.id) || linePatchSavingId === toPositiveInt(line.id)}
                  >
                    {linePatchSavingId === toPositiveInt(line.id) ? "Patching..." : "Patch Line"}
                  </button>
                ) : null}

                <select className="rounded border border-slate-300 px-2 py-1 text-sm" value={line.recognitionMethod} onChange={(event) => handleLineChange(index, "recognitionMethod", event.target.value)}>
                  {RECOGNITION_METHODS.map((method) => <option key={`${index}-${method}`} value={method}>{method}</option>)}
                </select>
                <input type="date" className="rounded border border-slate-300 px-2 py-1 text-sm" value={line.recognitionStartDate} onChange={(event) => handleLineChange(index, "recognitionStartDate", event.target.value)} />
                <input type="date" className="rounded border border-slate-300 px-2 py-1 text-sm" value={line.recognitionEndDate} onChange={(event) => handleLineChange(index, "recognitionEndDate", event.target.value)} />
                <select className="rounded border border-slate-300 px-2 py-1 text-sm" value={line.status} onChange={(event) => handleLineChange(index, "status", event.target.value)}>
                  {CONTRACT_LINE_STATUSES.map((status) => <option key={`${index}-${status}`} value={status}>{status}</option>)}
                </select>

                {gates.canReadAccountPicker ? (
                  <select className="rounded border border-slate-300 px-2 py-1 text-sm" value={line.deferredAccountId} onChange={(event) => handleLineChange(index, "deferredAccountId", event.target.value)}>
                    <option value="">deferred account</option>
                    {deferredAccountOptions.map((row) => <option key={`d-${index}-${row.id}`} value={row.id}>{row.code} - {row.name}</option>)}
                  </select>
                ) : (
                  <input className="rounded border border-slate-300 px-2 py-1 text-sm" placeholder="deferredAccountId" value={line.deferredAccountId} onChange={(event) => handleLineChange(index, "deferredAccountId", event.target.value)} />
                )}

                {gates.canReadAccountPicker ? (
                  <select className="rounded border border-slate-300 px-2 py-1 text-sm" value={line.revenueAccountId} onChange={(event) => handleLineChange(index, "revenueAccountId", event.target.value)}>
                    <option value="">revenue/expense account</option>
                    {revenueAccountOptions.map((row) => <option key={`r-${index}-${row.id}`} value={row.id}>{row.code} - {row.name}</option>)}
                  </select>
                ) : (
                  <input className="rounded border border-slate-300 px-2 py-1 text-sm" placeholder="revenueAccountId" value={line.revenueAccountId} onChange={(event) => handleLineChange(index, "revenueAccountId", event.target.value)} />
                )}
              </div>
            ))}
          </div>

          <div className="flex gap-2">
            <button type="submit" className="rounded bg-slate-900 px-3 py-1 text-sm text-white" disabled={formSaving || !gates.canUpsertContract}>
              {formSaving
                ? "Saving..."
                : formMode === "edit"
                  ? "Update"
                  : formMode === "amend"
                    ? "Apply Amendment"
                    : "Create"}
            </button>
            <button type="button" className="rounded border border-slate-300 px-3 py-1 text-sm" onClick={handleStartCreate}>Reset</button>
          </div>
        </form>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="font-semibold text-slate-900">Lifecycle</h2>
        {detailError ? <div className="mt-2 text-sm text-rose-700">{detailError}</div> : null}
        {lifecycleError ? <div className="mt-2 text-sm text-rose-700">{lifecycleError}</div> : null}
        {lifecycleMessage ? <div className="mt-2 text-sm text-emerald-700">{lifecycleMessage}</div> : null}
        {selectedContract ? (
          <div className="mt-3 space-y-2">
            <div className="text-sm text-slate-700">
              #{selectedContract.id} | {selectedContract.contractNo} | {selectedContract.status}
            </div>
            <div className="flex flex-wrap gap-2">
              <button className="rounded bg-emerald-700 px-3 py-1 text-sm text-white disabled:opacity-60" onClick={() => handleLifecycleAction("activate")} disabled={!lifecycleStates.activate.allowed || lifecycleLoading === "activate"}>{lifecycleLoading === "activate" ? "..." : "Activate"}</button>
              <button className="rounded bg-amber-700 px-3 py-1 text-sm text-white disabled:opacity-60" onClick={() => handleLifecycleAction("suspend")} disabled={!lifecycleStates.suspend.allowed || lifecycleLoading === "suspend"}>{lifecycleLoading === "suspend" ? "..." : "Suspend"}</button>
              <button className="rounded bg-slate-700 px-3 py-1 text-sm text-white disabled:opacity-60" onClick={() => handleLifecycleAction("close")} disabled={!lifecycleStates.close.allowed || lifecycleLoading === "close"}>{lifecycleLoading === "close" ? "..." : "Close"}</button>
              <button className="rounded bg-rose-700 px-3 py-1 text-sm text-white disabled:opacity-60" onClick={() => handleLifecycleAction("cancel")} disabled={!lifecycleStates.cancel.allowed || lifecycleLoading === "cancel"}>{lifecycleLoading === "cancel" ? "..." : "Cancel"}</button>
            </div>
          </div>
        ) : (
          <div className="mt-2 text-sm text-slate-500">{detailLoading ? "Loading..." : "Select a contract."}</div>
        )}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="font-semibold text-slate-900">Financial Rollups</h2>
        {!selectedContract ? (
          <div className="mt-2 text-sm text-slate-500">Select a contract.</div>
        ) : (
          <div className="mt-3 space-y-3">
            <div className="grid gap-2 md:grid-cols-3">
              <div className="rounded border border-slate-200 bg-slate-50 p-3">
                <div className="text-xs uppercase tracking-wide text-slate-500">Billed</div>
                <div className="mt-1 text-lg font-semibold text-slate-900">
                  {formatAmount(financialRollup?.billedAmountBase)}
                </div>
                <div className="text-xs text-slate-600">Txn {formatAmount(financialRollup?.billedAmountTxn)}</div>
              </div>
              <div className="rounded border border-slate-200 bg-slate-50 p-3">
                <div className="text-xs uppercase tracking-wide text-slate-500">Collected</div>
                <div className="mt-1 text-lg font-semibold text-emerald-700">
                  {formatAmount(financialRollup?.collectedAmountBase)}
                </div>
                <div className="text-xs text-slate-600">Txn {formatAmount(financialRollup?.collectedAmountTxn)}</div>
              </div>
              <div className="rounded border border-slate-200 bg-slate-50 p-3">
                <div className="text-xs uppercase tracking-wide text-slate-500">Uncollected</div>
                <div className="mt-1 text-lg font-semibold text-amber-700">
                  {formatAmount(financialRollup?.uncollectedAmountBase)}
                </div>
                <div className="text-xs text-slate-600">
                  Txn {formatAmount(financialRollup?.uncollectedAmountTxn)}
                </div>
              </div>
              <div className="rounded border border-slate-200 bg-slate-50 p-3">
                <div className="text-xs uppercase tracking-wide text-slate-500">Recognized To Date</div>
                <div className="mt-1 text-lg font-semibold text-sky-700">
                  {formatAmount(financialRollup?.recognizedToDateBase)}
                </div>
                <div className="text-xs text-slate-600">
                  Txn {formatAmount(financialRollup?.recognizedToDateTxn)}
                </div>
              </div>
              <div className="rounded border border-slate-200 bg-slate-50 p-3">
                <div className="text-xs uppercase tracking-wide text-slate-500">Deferred Balance</div>
                <div className="mt-1 text-lg font-semibold text-indigo-700">
                  {formatAmount(financialRollup?.deferredBalanceBase)}
                </div>
                <div className="text-xs text-slate-600">
                  Txn {formatAmount(financialRollup?.deferredBalanceTxn)}
                </div>
              </div>
              <div className="rounded border border-slate-200 bg-slate-50 p-3">
                <div className="text-xs uppercase tracking-wide text-slate-500">
                  {toUpper(selectedContract?.contractType) === "VENDOR"
                    ? "Open Payable"
                    : "Open Receivable"}
                </div>
                <div className="mt-1 text-lg font-semibold text-rose-700">
                  {toUpper(selectedContract?.contractType) === "VENDOR"
                    ? formatAmount(financialRollup?.openPayableBase)
                    : formatAmount(financialRollup?.openReceivableBase)}
                </div>
                <div className="text-xs text-slate-600">
                  Txn{" "}
                  {toUpper(selectedContract?.contractType) === "VENDOR"
                    ? formatAmount(financialRollup?.openPayableTxn)
                    : formatAmount(financialRollup?.openReceivableTxn)}
                </div>
              </div>
            </div>

            <div className="space-y-2 rounded border border-slate-200 p-3">
              <div>
                <div className="flex items-center justify-between text-xs text-slate-600">
                  <span>Collection Progress (Collected / Billed)</span>
                  <span>{collectionProgressPct.toFixed(2)}%</span>
                </div>
                <div className="mt-1 h-2 overflow-hidden rounded bg-slate-200">
                  <div
                    className="h-2 rounded bg-emerald-500"
                    style={{ width: `${collectionProgressPct}%` }}
                  />
                </div>
              </div>
              <div>
                <div className="flex items-center justify-between text-xs text-slate-600">
                  <span>Recognition Progress (Recognized / Scheduled)</span>
                  <span>{recognitionProgressPct.toFixed(2)}%</span>
                </div>
                <div className="mt-1 h-2 overflow-hidden rounded bg-slate-200">
                  <div
                    className="h-2 rounded bg-sky-500"
                    style={{ width: `${recognitionProgressPct}%` }}
                  />
                </div>
              </div>
              <div className="text-xs text-slate-500">
                Linked docs: {financialRollup?.activeLinkedDocumentCount || 0} active /{" "}
                {financialRollup?.linkedDocumentCount || 0} total. RevRec lines:{" "}
                {financialRollup?.revrecRecognizedRunLineCount || 0} recognized /{" "}
                {financialRollup?.revrecScheduleLineCount || 0} scheduled.
              </div>
            </div>
          </div>
        )}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="font-semibold text-slate-900">Generate Billing</h2>
        {billingError ? <div className="mt-2 text-sm text-rose-700">{billingError}</div> : null}
        {billingMessage ? <div className="mt-2 text-sm text-emerald-700">{billingMessage}</div> : null}

        {!selectedContract ? (
          <p className="mt-2 text-sm text-slate-500">Select a contract first.</p>
        ) : (
          <form className="mt-3 space-y-3" onSubmit={handleGenerateBilling}>
            <div className="grid gap-2 md:grid-cols-3">
              <select
                className="rounded border border-slate-300 px-2 py-1 text-sm"
                value={billingForm.docType}
                onChange={(event) =>
                  setBillingForm((prev) => ({ ...prev, docType: event.target.value }))
                }
              >
                {BILLING_DOC_TYPES.map((type) => (
                  <option key={`doc-${type}`} value={type}>
                    {type}
                  </option>
                ))}
              </select>
              <select
                className="rounded border border-slate-300 px-2 py-1 text-sm"
                value={billingForm.amountStrategy}
                onChange={(event) =>
                  setBillingForm((prev) => ({
                    ...prev,
                    amountStrategy: event.target.value,
                    amountTxn: event.target.value === "FULL" ? "" : prev.amountTxn,
                    amountBase: event.target.value === "FULL" ? "" : prev.amountBase,
                  }))
                }
              >
                {BILLING_AMOUNT_STRATEGIES.map((strategy) => (
                  <option key={`strategy-${strategy}`} value={strategy}>
                    {strategy}
                  </option>
                ))}
              </select>
              <input
                type="date"
                className="rounded border border-slate-300 px-2 py-1 text-sm"
                value={billingForm.billingDate}
                onChange={(event) =>
                  setBillingForm((prev) => ({ ...prev, billingDate: event.target.value }))
                }
              />
              <input
                type="date"
                className="rounded border border-slate-300 px-2 py-1 text-sm"
                value={billingForm.dueDate}
                onChange={(event) =>
                  setBillingForm((prev) => ({ ...prev, dueDate: event.target.value }))
                }
              />
              {toUpper(billingForm.amountStrategy) !== "FULL" ? (
                <input
                  className="rounded border border-slate-300 px-2 py-1 text-sm"
                  placeholder="amountTxn"
                  value={billingForm.amountTxn}
                  onChange={(event) =>
                    setBillingForm((prev) => ({ ...prev, amountTxn: event.target.value }))
                  }
                />
              ) : null}
              {toUpper(billingForm.amountStrategy) !== "FULL" ? (
                <input
                  className="rounded border border-slate-300 px-2 py-1 text-sm"
                  placeholder="amountBase"
                  value={billingForm.amountBase}
                  onChange={(event) =>
                    setBillingForm((prev) => ({ ...prev, amountBase: event.target.value }))
                  }
                />
              ) : null}
              <input
                className="rounded border border-slate-300 px-2 py-1 text-sm md:col-span-2"
                placeholder="idempotencyKey"
                value={billingForm.idempotencyKey}
                onChange={(event) =>
                  setBillingForm((prev) => ({ ...prev, idempotencyKey: event.target.value }))
                }
              />
              <input
                className="rounded border border-slate-300 px-2 py-1 text-sm"
                placeholder="integrationEventUid (optional)"
                value={billingForm.integrationEventUid}
                onChange={(event) =>
                  setBillingForm((prev) => ({ ...prev, integrationEventUid: event.target.value }))
                }
              />
              <input
                className="rounded border border-slate-300 px-2 py-1 text-sm md:col-span-3"
                placeholder="note (optional)"
                value={billingForm.note}
                onChange={(event) =>
                  setBillingForm((prev) => ({ ...prev, note: event.target.value }))
                }
              />
            </div>

            <div className="rounded border border-slate-200 p-3">
              <div className="flex flex-wrap items-center gap-2">
                <div className="text-sm font-semibold text-slate-700">Billable Lines</div>
                <button
                  type="button"
                  className="rounded border border-slate-300 px-2 py-1 text-xs"
                  onClick={handleSelectAllBillingLines}
                >
                  Select Active
                </button>
                <button
                  type="button"
                  className="rounded border border-slate-300 px-2 py-1 text-xs"
                  onClick={handleClearBillingLines}
                >
                  Clear
                </button>
                <div className="text-xs text-slate-500">
                  Empty selection means: use all ACTIVE lines.
                </div>
              </div>
              <div className="mt-2 grid gap-2 md:grid-cols-2">
                {(Array.isArray(selectedContract?.lines) ? selectedContract.lines : []).map((line, index) => {
                  const lineId = String(line?.id || "");
                  const checked = (billingForm.selectedLineIds || []).some(
                    (entry) => String(entry) === lineId
                  );
                  return (
                    <label
                      key={`bill-line-${lineId || index}`}
                      className="flex items-center gap-2 rounded border border-slate-200 px-2 py-1 text-sm"
                    >
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={(event) => handleToggleBillingLine(lineId, event.target.checked)}
                      />
                      <span>
                        #{lineId || "-"} | {line?.description || "-"} | {line?.status} | txn{" "}
                        {formatAmount(line?.lineAmountTxn)}
                      </span>
                    </label>
                  );
                })}
              </div>
            </div>

            <button
              className="rounded bg-slate-900 px-3 py-1 text-sm text-white disabled:opacity-60"
              disabled={!gates.canGenerateBilling || billingSaving}
            >
              {billingSaving ? "Generating..." : "Generate Billing"}
            </button>

            {billingResult ? (
              <div className="rounded border border-emerald-200 bg-emerald-50 p-3 text-sm text-emerald-900">
                <div>
                  Batch: {billingResult?.billingBatch?.batchId || "-"} | Replay:{" "}
                  {billingResult?.idempotentReplay ? "YES" : "NO"}
                </div>
                <div>
                  Document: {billingResult?.document?.documentNo || billingResult?.document?.id || "-"}{" "}
                  ({billingResult?.document?.documentType || "-"})
                </div>
                <div>LinkId: {billingResult?.link?.linkId || "-"}</div>
              </div>
            ) : null}
          </form>
        )}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="font-semibold text-slate-900">Generate RevRec Schedule</h2>
        {revrecError ? <div className="mt-2 text-sm text-rose-700">{revrecError}</div> : null}
        {revrecMessage ? <div className="mt-2 text-sm text-emerald-700">{revrecMessage}</div> : null}

        {!selectedContract ? (
          <p className="mt-2 text-sm text-slate-500">Select a contract first.</p>
        ) : (
          <form className="mt-3 space-y-3" onSubmit={handleGenerateRevrec}>
            <div className="grid gap-2 md:grid-cols-3">
              <input
                className="rounded border border-slate-300 px-2 py-1 text-sm"
                placeholder="fiscalPeriodId"
                value={revrecForm.fiscalPeriodId}
                onChange={(event) =>
                  setRevrecForm((prev) => ({ ...prev, fiscalPeriodId: event.target.value }))
                }
              />
              <select
                className="rounded border border-slate-300 px-2 py-1 text-sm"
                value={revrecForm.generationMode}
                onChange={(event) =>
                  setRevrecForm((prev) => ({
                    ...prev,
                    generationMode: event.target.value,
                    sourceCariDocumentId:
                      event.target.value === "BY_LINKED_DOCUMENT" ? prev.sourceCariDocumentId : "",
                  }))
                }
              >
                {REVREC_GENERATION_MODES.map((mode) => (
                  <option key={`revrec-mode-${mode}`} value={mode}>
                    {mode}
                  </option>
                ))}
              </select>
              {toUpper(revrecForm.generationMode) === "BY_LINKED_DOCUMENT" ? (
                <select
                  className="rounded border border-slate-300 px-2 py-1 text-sm"
                  value={revrecForm.sourceCariDocumentId}
                  onChange={(event) =>
                    setRevrecForm((prev) => ({
                      ...prev,
                      sourceCariDocumentId: event.target.value,
                    }))
                  }
                >
                  <option value="">sourceCariDocumentId</option>
                  {documentLinks
                    .filter((row) => !row?.isUnlinked)
                    .map((row) => (
                      <option key={`revrec-doc-${row?.linkId || row?.cariDocumentId}`} value={row?.cariDocumentId}>
                        {row?.documentNo || row?.cariDocumentId} | {row?.linkType || "-"}
                      </option>
                    ))}
                </select>
              ) : null}
              <label className="flex items-center gap-2 text-sm text-slate-700 md:col-span-2">
                <input
                  type="checkbox"
                  checked={Boolean(revrecForm.regenerateMissingOnly)}
                  onChange={(event) =>
                    setRevrecForm((prev) => ({
                      ...prev,
                      regenerateMissingOnly: event.target.checked,
                    }))
                  }
                />
                Regenerate missing only (idempotent-safe)
              </label>
            </div>

            <div className="rounded border border-slate-200 p-3">
              <div className="flex flex-wrap items-center gap-2">
                <div className="text-sm font-semibold text-slate-700">RevRec Lines</div>
                <button
                  type="button"
                  className="rounded border border-slate-300 px-2 py-1 text-xs"
                  onClick={handleSelectAllRevrecLines}
                >
                  Select Active
                </button>
                <button
                  type="button"
                  className="rounded border border-slate-300 px-2 py-1 text-xs"
                  onClick={handleClearRevrecLines}
                >
                  Clear
                </button>
                <div className="text-xs text-slate-500">
                  Empty selection means: use all ACTIVE lines.
                </div>
              </div>
              <div className="mt-2 grid gap-2 md:grid-cols-2">
                {(Array.isArray(selectedContract?.lines) ? selectedContract.lines : []).map((line, index) => {
                  const lineId = String(line?.id || "");
                  const checked = (revrecForm.contractLineIds || []).some(
                    (entry) => String(entry) === lineId
                  );
                  return (
                    <label
                      key={`revrec-line-${lineId || index}`}
                      className="flex items-center gap-2 rounded border border-slate-200 px-2 py-1 text-sm"
                    >
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={(event) => handleToggleRevrecLine(lineId, event.target.checked)}
                      />
                      <span>
                        #{lineId || "-"} | {line?.description || "-"} | {line?.recognitionMethod} |{" "}
                        {line?.recognitionStartDate || "-"} .. {line?.recognitionEndDate || "-"}
                      </span>
                    </label>
                  );
                })}
              </div>
            </div>

            <button
              className="rounded bg-slate-900 px-3 py-1 text-sm text-white disabled:opacity-60"
              disabled={!gates.canGenerateRevrec || revrecSaving}
            >
              {revrecSaving ? "Generating..." : "Generate RevRec"}
            </button>

            {revrecResult ? (
              <div className="rounded border border-emerald-200 bg-emerald-50 p-3 text-sm text-emerald-900">
                <div>
                  Replay: {revrecResult?.idempotentReplay ? "YES" : "NO"} | Family:{" "}
                  {revrecResult?.accountFamily || "-"} | Mode: {revrecResult?.generationMode || "-"}
                </div>
                <div>
                  Generated schedules: {revrecResult?.generatedScheduleCount || 0} | Generated lines:{" "}
                  {revrecResult?.generatedLineCount || 0} | Skipped:{" "}
                  {revrecResult?.skippedLineCount || 0}
                </div>
                <div>
                  Schedule IDs:{" "}
                  {(Array.isArray(revrecResult?.rows) ? revrecResult.rows : [])
                    .map((row) => row?.id)
                    .filter((value) => value)
                    .join(", ") || "-"}
                </div>
              </div>
            ) : null}
          </form>
        )}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="font-semibold text-slate-900">Link Document</h2>
        {linksError ? <div className="mt-2 text-sm text-rose-700">{linksError}</div> : null}
        {linkError ? <div className="mt-2 text-sm text-rose-700">{linkError}</div> : null}
        {linkMessage ? <div className="mt-2 text-sm text-emerald-700">{linkMessage}</div> : null}
        {linkActionError ? <div className="mt-2 text-sm text-rose-700">{linkActionError}</div> : null}
        {linkActionMessage ? <div className="mt-2 text-sm text-emerald-700">{linkActionMessage}</div> : null}

        {!selectedContract ? (
          <p className="mt-2 text-sm text-slate-500">Select a contract first.</p>
        ) : (
          <div className="mt-3 space-y-3">
            {!gates.canReadDocumentPicker ? (
              <div className="text-xs text-amber-700">Picker hidden: contract.link_document missing.</div>
            ) : null}
            <form className="grid gap-2 md:grid-cols-5" onSubmit={handleLinkDocument}>
              <input className="rounded border border-slate-300 px-2 py-1 text-sm" placeholder="cariDocumentId" value={linkForm.cariDocumentId} onChange={(event) => setLinkForm((prev) => ({ ...prev, cariDocumentId: event.target.value }))} />
              {gates.canReadDocumentPicker ? (
                <select className="rounded border border-slate-300 px-2 py-1 text-sm md:col-span-4" value="" onChange={(event) => setLinkForm((prev) => ({ ...prev, cariDocumentId: event.target.value }))}>
                  <option value="">picker documents</option>
                  {documentPickerRows.map((row) => <option key={row.id} value={row.id}>{row.documentNo || row.id} | {row.direction} | {row.status}</option>)}
                </select>
              ) : null}
              <select className="rounded border border-slate-300 px-2 py-1 text-sm" value={linkForm.linkType} onChange={(event) => setLinkForm((prev) => ({ ...prev, linkType: event.target.value }))}>
                {LINK_TYPES.map((type) => <option key={type} value={type}>{type}</option>)}
              </select>
              <input className="rounded border border-slate-300 px-2 py-1 text-sm" placeholder="linkedAmountTxn" value={linkForm.linkedAmountTxn} onChange={(event) => setLinkForm((prev) => ({ ...prev, linkedAmountTxn: event.target.value }))} />
              <input className="rounded border border-slate-300 px-2 py-1 text-sm" placeholder="linkedAmountBase" value={linkForm.linkedAmountBase} onChange={(event) => setLinkForm((prev) => ({ ...prev, linkedAmountBase: event.target.value }))} />
              <input className="rounded border border-slate-300 px-2 py-1 text-sm" placeholder="linkFxRate (optional)" value={linkForm.linkFxRate} onChange={(event) => setLinkForm((prev) => ({ ...prev, linkFxRate: event.target.value }))} />
              <button className="rounded bg-slate-900 px-3 py-1 text-sm text-white disabled:opacity-60" disabled={!gates.canLinkDocument || linkSaving}>
                {linkSaving ? "Linking..." : "Link"}
              </button>
            </form>

            <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead className="text-left text-slate-600">
                  <tr>
                    <th>LinkId</th>
                    <th>Document</th>
                    <th>Type</th>
                    <th>Ccy Snapshot</th>
                    <th>FX Snapshot</th>
                    <th>Effective Txn</th>
                    <th>Effective Base</th>
                    <th>Status</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {documentLinks.map((row, index) => (
                    <tr key={`link-${index}`} className="border-t border-slate-200">
                      <td>{row.linkId || "-"}</td>
                      <td>{row.documentNo || row.cariDocumentId}</td>
                      <td>{row.linkType}</td>
                      <td>
                        {(row.contractCurrencyCodeSnapshot || "-")} {"->"}{" "}
                        {(row.documentCurrencyCodeSnapshot || "-")}
                      </td>
                      <td>{formatAmount(row.linkFxRateSnapshot)}</td>
                      <td>{formatAmount(row.linkedAmountTxn)}</td>
                      <td>{formatAmount(row.linkedAmountBase)}</td>
                      <td>{row.isUnlinked ? "UNLINKED" : "ACTIVE"}</td>
                      <td>
                        <div className="flex flex-wrap gap-2">
                          <button
                            type="button"
                            className="rounded border border-slate-300 px-2 py-1 text-xs disabled:opacity-60"
                            onClick={() => handleSelectLinkForAdjustment(row)}
                            disabled={!canAdjustContractLink(row, gates).allowed}
                            title={canAdjustContractLink(row, gates).reason || "Prepare adjustment"}
                          >
                            Adjust
                          </button>
                          <button
                            type="button"
                            className="rounded border border-rose-300 px-2 py-1 text-xs text-rose-700 disabled:opacity-60"
                            onClick={() => handleSelectLinkForUnlink(row)}
                            disabled={!canUnlinkContractLink(row, gates).allowed}
                            title={canUnlinkContractLink(row, gates).reason || "Prepare unlink"}
                          >
                            Unlink
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  {documentLinks.length === 0 ? (
                    <tr><td colSpan={9} className="py-2 text-slate-500">No links.</td></tr>
                  ) : null}
                </tbody>
              </table>
            </div>

            <div className="grid gap-3 md:grid-cols-2">
              <form className="rounded border border-slate-200 p-3 space-y-2" onSubmit={handleAdjustLink}>
                <h3 className="text-sm font-semibold text-slate-800">Adjust Link</h3>
                <input
                  className="w-full rounded border border-slate-300 px-2 py-1 text-sm"
                  placeholder="linkId"
                  value={linkAdjustmentForm.linkId}
                  onChange={(event) =>
                    setLinkAdjustmentForm((prev) => ({ ...prev, linkId: event.target.value }))
                  }
                />
                <input
                  className="w-full rounded border border-slate-300 px-2 py-1 text-sm"
                  placeholder="nextLinkedAmountTxn"
                  value={linkAdjustmentForm.nextLinkedAmountTxn}
                  onChange={(event) =>
                    setLinkAdjustmentForm((prev) => ({
                      ...prev,
                      nextLinkedAmountTxn: event.target.value,
                    }))
                  }
                />
                <input
                  className="w-full rounded border border-slate-300 px-2 py-1 text-sm"
                  placeholder="nextLinkedAmountBase"
                  value={linkAdjustmentForm.nextLinkedAmountBase}
                  onChange={(event) =>
                    setLinkAdjustmentForm((prev) => ({
                      ...prev,
                      nextLinkedAmountBase: event.target.value,
                    }))
                  }
                />
                <input
                  className="w-full rounded border border-slate-300 px-2 py-1 text-sm"
                  placeholder="reason"
                  value={linkAdjustmentForm.reason}
                  onChange={(event) =>
                    setLinkAdjustmentForm((prev) => ({ ...prev, reason: event.target.value }))
                  }
                />
                <button
                  className="rounded bg-slate-900 px-3 py-1 text-sm text-white disabled:opacity-60"
                  disabled={!gates.canLinkDocument || linkAdjustmentSaving}
                >
                  {linkAdjustmentSaving ? "Applying..." : "Apply Adjust"}
                </button>
              </form>

              <form className="rounded border border-slate-200 p-3 space-y-2" onSubmit={handleUnlinkLink}>
                <h3 className="text-sm font-semibold text-slate-800">Unlink Link</h3>
                <input
                  className="w-full rounded border border-slate-300 px-2 py-1 text-sm"
                  placeholder="linkId"
                  value={linkUnlinkForm.linkId}
                  onChange={(event) =>
                    setLinkUnlinkForm((prev) => ({ ...prev, linkId: event.target.value }))
                  }
                />
                <input
                  className="w-full rounded border border-slate-300 px-2 py-1 text-sm"
                  placeholder="reason"
                  value={linkUnlinkForm.reason}
                  onChange={(event) =>
                    setLinkUnlinkForm((prev) => ({ ...prev, reason: event.target.value }))
                  }
                />
                <button
                  className="rounded bg-rose-700 px-3 py-1 text-sm text-white disabled:opacity-60"
                  disabled={!gates.canLinkDocument || linkUnlinkSaving}
                >
                  {linkUnlinkSaving ? "Applying..." : "Apply Unlink"}
                </button>
              </form>
            </div>
          </div>
        )}
      </section>
    </div>
  );
}
