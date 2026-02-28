export const CONTRACT_TYPES = ["CUSTOMER", "VENDOR"];
export const CONTRACT_STATUSES = [
  "DRAFT",
  "ACTIVE",
  "SUSPENDED",
  "CLOSED",
  "CANCELLED",
];
export const CONTRACT_LINE_STATUSES = ["ACTIVE", "INACTIVE"];
export const RECOGNITION_METHODS = ["STRAIGHT_LINE", "MILESTONE", "MANUAL"];
export const LINK_TYPES = ["BILLING", "ADVANCE", "ADJUSTMENT"];
export const BILLING_DOC_TYPES = ["INVOICE", "ADVANCE", "ADJUSTMENT"];
export const BILLING_AMOUNT_STRATEGIES = ["FULL", "PARTIAL", "MILESTONE"];
export const REVREC_GENERATION_MODES = ["BY_CONTRACT_LINE", "BY_LINKED_DOCUMENT"];
const FINANCIAL_ROLLUP_NUMERIC_FIELDS = Object.freeze([
  "linkedDocumentCount",
  "activeLinkedDocumentCount",
  "revrecScheduleLineCount",
  "revrecRecognizedRunLineCount",
  "billedAmountTxn",
  "billedAmountBase",
  "collectedAmountTxn",
  "collectedAmountBase",
  "uncollectedAmountTxn",
  "uncollectedAmountBase",
  "revrecScheduledAmountTxn",
  "revrecScheduledAmountBase",
  "recognizedToDateTxn",
  "recognizedToDateBase",
  "deferredBalanceTxn",
  "deferredBalanceBase",
  "openReceivableTxn",
  "openReceivableBase",
  "openPayableTxn",
  "openPayableBase",
  "collectedCoveragePct",
  "recognizedCoveragePct",
]);

const LIFECYCLE_FROM_STATUS = Object.freeze({
  activate: new Set(["DRAFT", "SUSPENDED"]),
  suspend: new Set(["ACTIVE"]),
  close: new Set(["ACTIVE", "SUSPENDED"]),
  cancel: new Set(["DRAFT"]),
});

function toUpper(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function toPermissionSet(permissionCodes = []) {
  return new Set(
    Array.isArray(permissionCodes)
      ? permissionCodes.map((code) => String(code || "").trim()).filter(Boolean)
      : []
  );
}

export function resolveContractsPermissionGates(permissionCodes = []) {
  const permissionSet = toPermissionSet(permissionCodes);
  const canReadCounterpartyPicker = permissionSet.has("cari.card.read");
  const canReadAccountPicker = permissionSet.has("gl.account.read");
  const canReadDocumentPicker = permissionSet.has("contract.link_document");

  return {
    canReadContractsRoute: permissionSet.has("contract.read"),
    canUpsertContract: permissionSet.has("contract.upsert"),
    canActivateContract: permissionSet.has("contract.activate"),
    canSuspendContract: permissionSet.has("contract.suspend"),
    canCloseContract: permissionSet.has("contract.close"),
    canCancelContract: permissionSet.has("contract.cancel"),
    canLinkDocument: permissionSet.has("contract.link_document"),
    canGenerateBilling: permissionSet.has("contract.link_document"),
    canGenerateRevrec: permissionSet.has("revenue.schedule.generate"),
    canReadCounterpartyPicker,
    canReadAccountPicker,
    canReadDocumentPicker,
    shouldFetchCounterparties: canReadCounterpartyPicker,
    shouldFetchAccounts: canReadAccountPicker,
    shouldFetchDocuments: canReadDocumentPicker,
  };
}

export function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

export function toOptionalNumber(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function isFiniteNonZero(value) {
  return Number.isFinite(value) && value !== 0;
}

export function formatAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return "-";
  }
  return parsed.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 6,
  });
}

export function createEmptyContractLine() {
  return {
    id: "",
    description: "",
    lineAmountTxn: "",
    lineAmountBase: "",
    recognitionMethod: "STRAIGHT_LINE",
    recognitionStartDate: "",
    recognitionEndDate: "",
    deferredAccountId: "",
    revenueAccountId: "",
    status: "ACTIVE",
  };
}

export function createInitialContractForm() {
  return {
    legalEntityId: "",
    counterpartyId: "",
    contractNo: "",
    contractType: "CUSTOMER",
    currencyCode: "USD",
    startDate: "",
    endDate: "",
    notes: "",
    lines: [createEmptyContractLine()],
  };
}

export function createInitialLinkForm() {
  return {
    cariDocumentId: "",
    linkType: "BILLING",
    linkedAmountTxn: "",
    linkedAmountBase: "",
    linkFxRate: "",
  };
}

export function createInitialLinkAdjustmentForm() {
  return {
    linkId: "",
    nextLinkedAmountTxn: "",
    nextLinkedAmountBase: "",
    reason: "",
  };
}

export function createInitialLinkUnlinkForm() {
  return {
    linkId: "",
    reason: "",
  };
}

function createSuggestedBillingIdempotencyKey() {
  const stamp = Date.now().toString(36).toUpperCase();
  const random = Math.random().toString(36).slice(2, 8).toUpperCase();
  return `BILL-${stamp}-${random}`.slice(0, 100);
}

export function createInitialBillingForm() {
  return {
    docType: "INVOICE",
    amountStrategy: "FULL",
    billingDate: "",
    dueDate: "",
    amountTxn: "",
    amountBase: "",
    idempotencyKey: createSuggestedBillingIdempotencyKey(),
    integrationEventUid: "",
    note: "",
    selectedLineIds: [],
  };
}

export function createInitialRevrecForm() {
  return {
    fiscalPeriodId: "",
    generationMode: "BY_CONTRACT_LINE",
    sourceCariDocumentId: "",
    regenerateMissingOnly: true,
    contractLineIds: [],
  };
}

export function createEmptyContractFinancialRollup(contractType = "CUSTOMER") {
  const isVendorContract = toUpper(contractType) === "VENDOR";
  return {
    currencyCode: null,
    linkedDocumentCount: 0,
    activeLinkedDocumentCount: 0,
    revrecScheduleLineCount: 0,
    revrecRecognizedRunLineCount: 0,
    billedAmountTxn: 0,
    billedAmountBase: 0,
    collectedAmountTxn: 0,
    collectedAmountBase: 0,
    uncollectedAmountTxn: 0,
    uncollectedAmountBase: 0,
    revrecScheduledAmountTxn: 0,
    revrecScheduledAmountBase: 0,
    recognizedToDateTxn: 0,
    recognizedToDateBase: 0,
    deferredBalanceTxn: 0,
    deferredBalanceBase: 0,
    openReceivableTxn: isVendorContract ? 0 : 0,
    openReceivableBase: isVendorContract ? 0 : 0,
    openPayableTxn: isVendorContract ? 0 : 0,
    openPayableBase: isVendorContract ? 0 : 0,
    collectedCoveragePct: 0,
    recognizedCoveragePct: 0,
  };
}

export function normalizeContractFinancialRollup(rollup, contractType = "CUSTOMER") {
  const base = createEmptyContractFinancialRollup(contractType);
  const next = { ...base, ...(rollup && typeof rollup === "object" ? rollup : {}) };
  const normalized = { ...next };
  for (const field of FINANCIAL_ROLLUP_NUMERIC_FIELDS) {
    normalized[field] = toOptionalNumber(next[field]) ?? 0;
  }
  normalized.currencyCode = String(next.currencyCode || "").trim().toUpperCase() || null;
  return normalized;
}

export function getCounterpartyRoleForContractType(contractType) {
  return toUpper(contractType) === "VENDOR" ? "VENDOR" : "CUSTOMER";
}

export function getDocumentDirectionForContractType(contractType) {
  return toUpper(contractType) === "VENDOR" ? "AP" : "AR";
}

export function getExpectedAccountTypesForContractType(contractType) {
  if (toUpper(contractType) === "VENDOR") {
    return {
      deferredAccountType: "ASSET",
      revenueAccountType: "EXPENSE",
    };
  }
  return {
    deferredAccountType: "LIABILITY",
    revenueAccountType: "REVENUE",
  };
}

export function filterAccountsForContractRole(accounts, contractType, role = "deferred") {
  const expectedTypes = getExpectedAccountTypesForContractType(contractType);
  const expectedType =
    role === "revenue" ? expectedTypes.revenueAccountType : expectedTypes.deferredAccountType;

  return (Array.isArray(accounts) ? accounts : []).filter(
    (row) => toUpper(row?.account_type) === expectedType
  );
}

export function mapContractDetailToForm(row) {
  const lines = Array.isArray(row?.lines)
    ? row.lines.map((line) => ({
        id: String(line?.id || ""),
        description: String(line?.description || ""),
        lineAmountTxn:
          line?.lineAmountTxn === null || line?.lineAmountTxn === undefined
            ? ""
            : String(line.lineAmountTxn),
        lineAmountBase:
          line?.lineAmountBase === null || line?.lineAmountBase === undefined
            ? ""
            : String(line.lineAmountBase),
        recognitionMethod: toUpper(line?.recognitionMethod) || "STRAIGHT_LINE",
        recognitionStartDate: String(line?.recognitionStartDate || ""),
        recognitionEndDate: String(line?.recognitionEndDate || ""),
        deferredAccountId: String(line?.deferredAccountId || ""),
        revenueAccountId: String(line?.revenueAccountId || ""),
        status: toUpper(line?.status) || "ACTIVE",
      }))
    : [];

  return {
    legalEntityId: String(row?.legalEntityId || ""),
    counterpartyId: String(row?.counterpartyId || ""),
    contractNo: String(row?.contractNo || ""),
    contractType: toUpper(row?.contractType) || "CUSTOMER",
    currencyCode: String(row?.currencyCode || "USD")
      .trim()
      .toUpperCase(),
    startDate: String(row?.startDate || ""),
    endDate: String(row?.endDate || ""),
    notes: String(row?.notes || ""),
    lines: lines.length > 0 ? lines : [createEmptyContractLine()],
  };
}

export function buildContractListQuery(filters = {}) {
  return {
    legalEntityId: toPositiveInt(filters.legalEntityId) || undefined,
    counterpartyId: toPositiveInt(filters.counterpartyId) || undefined,
    contractType: toUpper(filters.contractType) || undefined,
    status: toUpper(filters.status) || undefined,
    q: String(filters.q || "").trim() || undefined,
    limit: toPositiveInt(filters.limit) || 100,
    offset: Number.isInteger(Number(filters.offset)) && Number(filters.offset) >= 0
      ? Number(filters.offset)
      : 0,
  };
}

function normalizeLinePayload(line) {
  return {
    description: String(line?.description || "").trim(),
    lineAmountTxn: toOptionalNumber(line?.lineAmountTxn),
    lineAmountBase: toOptionalNumber(line?.lineAmountBase),
    recognitionMethod: toUpper(line?.recognitionMethod) || "STRAIGHT_LINE",
    recognitionStartDate: String(line?.recognitionStartDate || "").trim(),
    recognitionEndDate: String(line?.recognitionEndDate || "").trim(),
    deferredAccountId: toPositiveInt(line?.deferredAccountId),
    revenueAccountId: toPositiveInt(line?.revenueAccountId),
    status: toUpper(line?.status) || "ACTIVE",
  };
}

export function buildContractMutationPayload(form) {
  const lines = (Array.isArray(form?.lines) ? form.lines : []).map((line) =>
    normalizeLinePayload(line)
  );

  return {
    legalEntityId: toPositiveInt(form?.legalEntityId),
    counterpartyId: toPositiveInt(form?.counterpartyId),
    contractNo: String(form?.contractNo || "").trim(),
    contractType: toUpper(form?.contractType),
    currencyCode: String(form?.currencyCode || "")
      .trim()
      .toUpperCase(),
    startDate: String(form?.startDate || "").trim(),
    endDate: String(form?.endDate || "").trim() || null,
    notes: String(form?.notes || "").trim() || null,
    lines: lines.map((line) => ({
      description: line.description,
      lineAmountTxn: line.lineAmountTxn,
      lineAmountBase: line.lineAmountBase,
      recognitionMethod: line.recognitionMethod,
      recognitionStartDate: line.recognitionStartDate || null,
      recognitionEndDate: line.recognitionEndDate || null,
      deferredAccountId: line.deferredAccountId,
      revenueAccountId: line.revenueAccountId,
      status: line.status,
    })),
  };
}

export function validateContractForm(form) {
  const payload = buildContractMutationPayload(form);
  const errors = [];

  if (!payload.legalEntityId) {
    errors.push("legalEntityId is required.");
  }
  if (!payload.counterpartyId) {
    errors.push("counterpartyId is required.");
  }
  if (!payload.contractNo) {
    errors.push("contractNo is required.");
  }
  if (!CONTRACT_TYPES.includes(payload.contractType)) {
    errors.push("contractType must be CUSTOMER or VENDOR.");
  }
  if (!/^[A-Z]{3}$/.test(payload.currencyCode)) {
    errors.push("currencyCode must be a 3-letter code.");
  }
  if (!payload.startDate) {
    errors.push("startDate is required.");
  }
  if (!Array.isArray(payload.lines) || payload.lines.length === 0) {
    errors.push("At least one contract line is required.");
  }

  payload.lines.forEach((line, index) => {
    const lineLabel = `lines[${index}]`;
    if (!line.description) {
      errors.push(`${lineLabel}.description is required.`);
    }
    if (!isFiniteNonZero(line.lineAmountTxn)) {
      errors.push(`${lineLabel}.lineAmountTxn must be non-zero.`);
    }
    if (!isFiniteNonZero(line.lineAmountBase)) {
      errors.push(`${lineLabel}.lineAmountBase must be non-zero.`);
    }
    if (!RECOGNITION_METHODS.includes(line.recognitionMethod)) {
      errors.push(`${lineLabel}.recognitionMethod is invalid.`);
    }
    if (!CONTRACT_LINE_STATUSES.includes(line.status)) {
      errors.push(`${lineLabel}.status is invalid.`);
    }
    if (line.recognitionMethod === "STRAIGHT_LINE") {
      if (!line.recognitionStartDate || !line.recognitionEndDate) {
        errors.push(
          `${lineLabel}.recognitionStartDate and ${lineLabel}.recognitionEndDate are required for STRAIGHT_LINE.`
        );
      }
    } else if (line.recognitionMethod === "MILESTONE") {
      if (!line.recognitionStartDate || !line.recognitionEndDate) {
        errors.push(
          `${lineLabel}.recognitionStartDate and ${lineLabel}.recognitionEndDate are required for MILESTONE.`
        );
      } else if (line.recognitionStartDate !== line.recognitionEndDate) {
        errors.push(
          `${lineLabel}.recognitionStartDate and ${lineLabel}.recognitionEndDate must match for MILESTONE.`
        );
      }
    } else if (line.recognitionMethod === "MANUAL") {
      if (line.recognitionStartDate || line.recognitionEndDate) {
        errors.push(
          `${lineLabel}.recognitionStartDate and ${lineLabel}.recognitionEndDate must be omitted for MANUAL.`
        );
      }
    }
    if (
      line.recognitionStartDate &&
      line.recognitionEndDate &&
      line.recognitionStartDate > line.recognitionEndDate
    ) {
      errors.push(
        `${lineLabel}.recognitionStartDate cannot be greater than ${lineLabel}.recognitionEndDate.`
      );
    }
  });

  return { payload, errors };
}

export function buildContractLinkPayload(form) {
  return {
    cariDocumentId: toPositiveInt(form?.cariDocumentId),
    linkType: toUpper(form?.linkType),
    linkedAmountTxn: toOptionalNumber(form?.linkedAmountTxn),
    linkedAmountBase: toOptionalNumber(form?.linkedAmountBase),
    linkFxRate: toOptionalNumber(form?.linkFxRate),
  };
}

export function buildContractBillingPayload(form) {
  const selectedLineIds = Array.isArray(form?.selectedLineIds)
    ? form.selectedLineIds.map((entry) => toPositiveInt(entry)).filter(Boolean)
    : [];

  return {
    docType: toUpper(form?.docType),
    amountStrategy: toUpper(form?.amountStrategy) || "FULL",
    billingDate: String(form?.billingDate || "").trim(),
    dueDate: String(form?.dueDate || "").trim() || null,
    amountTxn: toOptionalNumber(form?.amountTxn),
    amountBase: toOptionalNumber(form?.amountBase),
    idempotencyKey: String(form?.idempotencyKey || "").trim(),
    integrationEventUid: String(form?.integrationEventUid || "").trim() || null,
    note: String(form?.note || "").trim() || null,
    selectedLineIds,
  };
}

export function validateContractBillingForm(form) {
  const payload = buildContractBillingPayload(form);
  const errors = [];

  if (!BILLING_DOC_TYPES.includes(payload.docType)) {
    errors.push("docType is invalid.");
  }
  if (!BILLING_AMOUNT_STRATEGIES.includes(payload.amountStrategy)) {
    errors.push("amountStrategy is invalid.");
  }
  if (!payload.billingDate) {
    errors.push("billingDate is required.");
  }
  if (!payload.idempotencyKey) {
    errors.push("idempotencyKey is required.");
  }

  const hasAmountTxn = Number.isFinite(payload.amountTxn);
  const hasAmountBase = Number.isFinite(payload.amountBase);
  if (hasAmountTxn !== hasAmountBase) {
    errors.push("amountTxn and amountBase must be provided together.");
  }
  if (payload.amountStrategy === "FULL" && (hasAmountTxn || hasAmountBase)) {
    errors.push("amountTxn and amountBase must be empty for FULL strategy.");
  }
  if (payload.amountStrategy !== "FULL") {
    if (!hasAmountTxn || Number(payload.amountTxn) <= 0) {
      errors.push("amountTxn must be > 0 for PARTIAL/MILESTONE.");
    }
    if (!hasAmountBase || Number(payload.amountBase) <= 0) {
      errors.push("amountBase must be > 0 for PARTIAL/MILESTONE.");
    }
  }

  if (payload.dueDate && payload.billingDate && payload.dueDate < payload.billingDate) {
    errors.push("dueDate cannot be earlier than billingDate.");
  }

  return { payload, errors };
}

export function buildContractRevrecPayload(form) {
  const contractLineIds = Array.isArray(form?.contractLineIds)
    ? form.contractLineIds.map((entry) => toPositiveInt(entry)).filter(Boolean)
    : [];
  return {
    fiscalPeriodId: toPositiveInt(form?.fiscalPeriodId),
    generationMode: toUpper(form?.generationMode) || "BY_CONTRACT_LINE",
    sourceCariDocumentId: toPositiveInt(form?.sourceCariDocumentId),
    regenerateMissingOnly: Boolean(form?.regenerateMissingOnly ?? true),
    contractLineIds,
  };
}

export function validateContractRevrecForm(form) {
  const payload = buildContractRevrecPayload(form);
  const errors = [];

  if (!payload.fiscalPeriodId) {
    errors.push("fiscalPeriodId is required.");
  }
  if (!REVREC_GENERATION_MODES.includes(payload.generationMode)) {
    errors.push("generationMode is invalid.");
  }
  if (
    payload.generationMode === "BY_LINKED_DOCUMENT" &&
    !payload.sourceCariDocumentId
  ) {
    errors.push("sourceCariDocumentId is required for BY_LINKED_DOCUMENT mode.");
  }

  return { payload, errors };
}

export function validateContractLinkForm(form) {
  const payload = buildContractLinkPayload(form);
  const errors = [];

  if (!payload.cariDocumentId) {
    errors.push("cariDocumentId is required.");
  }
  if (!LINK_TYPES.includes(payload.linkType)) {
    errors.push("linkType is invalid.");
  }
  if (!Number.isFinite(payload.linkedAmountTxn) || payload.linkedAmountTxn <= 0) {
    errors.push("linkedAmountTxn must be > 0.");
  }
  if (!Number.isFinite(payload.linkedAmountBase) || payload.linkedAmountBase <= 0) {
    errors.push("linkedAmountBase must be > 0.");
  }
  if (payload.linkFxRate !== null && payload.linkFxRate !== undefined) {
    if (!Number.isFinite(payload.linkFxRate) || payload.linkFxRate <= 0) {
      errors.push("linkFxRate must be > 0 when provided.");
    }
  }

  return { payload, errors };
}

export function buildContractLinePatchPayload(lineForm, reason) {
  const normalized = normalizeLinePayload(lineForm || {});
  return {
    description: normalized.description,
    lineAmountTxn: normalized.lineAmountTxn,
    lineAmountBase: normalized.lineAmountBase,
    recognitionMethod: normalized.recognitionMethod,
    recognitionStartDate: normalized.recognitionStartDate || null,
    recognitionEndDate: normalized.recognitionEndDate || null,
    deferredAccountId: normalized.deferredAccountId,
    revenueAccountId: normalized.revenueAccountId,
    status: normalized.status,
    reason: String(reason || "").trim(),
  };
}

export function validateContractLinePatchForm(lineForm, reason) {
  const payload = buildContractLinePatchPayload(lineForm, reason);
  const errors = [];

  if (!payload.description) {
    errors.push("line.description is required.");
  }
  if (!isFiniteNonZero(payload.lineAmountTxn)) {
    errors.push("line.lineAmountTxn must be non-zero.");
  }
  if (!isFiniteNonZero(payload.lineAmountBase)) {
    errors.push("line.lineAmountBase must be non-zero.");
  }
  if (!RECOGNITION_METHODS.includes(payload.recognitionMethod)) {
    errors.push("line.recognitionMethod is invalid.");
  }
  if (!CONTRACT_LINE_STATUSES.includes(payload.status)) {
    errors.push("line.status is invalid.");
  }
  if (payload.recognitionMethod === "STRAIGHT_LINE") {
    if (!payload.recognitionStartDate || !payload.recognitionEndDate) {
      errors.push(
        "line.recognitionStartDate and line.recognitionEndDate are required for STRAIGHT_LINE."
      );
    }
  } else if (payload.recognitionMethod === "MILESTONE") {
    if (!payload.recognitionStartDate || !payload.recognitionEndDate) {
      errors.push("line.recognitionStartDate and line.recognitionEndDate are required for MILESTONE.");
    } else if (payload.recognitionStartDate !== payload.recognitionEndDate) {
      errors.push("line.recognitionStartDate and line.recognitionEndDate must match for MILESTONE.");
    }
  } else if (payload.recognitionMethod === "MANUAL") {
    if (payload.recognitionStartDate || payload.recognitionEndDate) {
      errors.push(
        "line.recognitionStartDate and line.recognitionEndDate must be omitted for MANUAL."
      );
    }
  }
  if (
    payload.recognitionStartDate &&
    payload.recognitionEndDate &&
    payload.recognitionStartDate > payload.recognitionEndDate
  ) {
    errors.push("line.recognitionStartDate cannot be greater than line.recognitionEndDate.");
  }
  if (!payload.reason) {
    errors.push("reason is required for line patch.");
  }

  return { payload, errors };
}

export function buildContractLinkAdjustmentPayload(form) {
  return {
    linkId: toPositiveInt(form?.linkId),
    nextLinkedAmountTxn: toOptionalNumber(form?.nextLinkedAmountTxn),
    nextLinkedAmountBase: toOptionalNumber(form?.nextLinkedAmountBase),
    reason: String(form?.reason || "").trim(),
  };
}

export function validateContractLinkAdjustmentForm(form) {
  const payload = buildContractLinkAdjustmentPayload(form);
  const errors = [];

  if (!payload.linkId) {
    errors.push("linkId is required.");
  }
  if (
    !Number.isFinite(payload.nextLinkedAmountTxn) ||
    Number(payload.nextLinkedAmountTxn) <= 0
  ) {
    errors.push("nextLinkedAmountTxn must be > 0.");
  }
  if (
    !Number.isFinite(payload.nextLinkedAmountBase) ||
    Number(payload.nextLinkedAmountBase) <= 0
  ) {
    errors.push("nextLinkedAmountBase must be > 0.");
  }
  if (!payload.reason) {
    errors.push("reason is required.");
  }

  return { payload, errors };
}

export function buildContractLinkUnlinkPayload(form) {
  return {
    linkId: toPositiveInt(form?.linkId),
    reason: String(form?.reason || "").trim(),
  };
}

export function validateContractLinkUnlinkForm(form) {
  const payload = buildContractLinkUnlinkPayload(form);
  const errors = [];

  if (!payload.linkId) {
    errors.push("linkId is required.");
  }
  if (!payload.reason) {
    errors.push("reason is required.");
  }

  return { payload, errors };
}

export function canAdjustContractLink(linkRow, gates = {}) {
  const canWrite = Boolean(gates?.canLinkDocument);
  if (!canWrite) {
    return {
      allowed: false,
      reason: "Missing permission: contract.link_document",
    };
  }

  const linkId = toPositiveInt(linkRow?.linkId ?? linkRow?.id);
  if (!linkId) {
    return {
      allowed: false,
      reason: "linkId is missing",
    };
  }

  if (linkRow?.isUnlinked) {
    return {
      allowed: false,
      reason: "Link is already unlinked",
    };
  }

  return { allowed: true, reason: null };
}

export function canUnlinkContractLink(linkRow, gates = {}) {
  const canWrite = Boolean(gates?.canLinkDocument);
  if (!canWrite) {
    return {
      allowed: false,
      reason: "Missing permission: contract.link_document",
    };
  }

  const linkId = toPositiveInt(linkRow?.linkId ?? linkRow?.id);
  if (!linkId) {
    return {
      allowed: false,
      reason: "linkId is missing",
    };
  }

  if (linkRow?.isUnlinked) {
    return {
      allowed: false,
      reason: "Link is already unlinked",
    };
  }

  return { allowed: true, reason: null };
}

export function canTransitionContractStatus(status, action) {
  const normalizedStatus = toUpper(status);
  const fromStatuses = LIFECYCLE_FROM_STATUS[action];
  if (!fromStatuses) {
    return false;
  }
  return fromStatuses.has(normalizedStatus);
}

export function getLifecycleActionStates(status, gates = {}) {
  const normalizedStatus = toUpper(status);

  const activatePermission = Boolean(gates?.canActivateContract);
  const suspendPermission = Boolean(gates?.canSuspendContract);
  const closePermission = Boolean(gates?.canCloseContract);
  const cancelPermission = Boolean(gates?.canCancelContract);

  const activateAllowed = activatePermission && canTransitionContractStatus(normalizedStatus, "activate");
  const suspendAllowed = suspendPermission && canTransitionContractStatus(normalizedStatus, "suspend");
  const closeAllowed = closePermission && canTransitionContractStatus(normalizedStatus, "close");
  const cancelAllowed = cancelPermission && canTransitionContractStatus(normalizedStatus, "cancel");

  return {
    activate: {
      allowed: activateAllowed,
      reason: activatePermission ? null : "Missing permission: contract.activate",
    },
    suspend: {
      allowed: suspendAllowed,
      reason: suspendPermission ? null : "Missing permission: contract.suspend",
    },
    close: {
      allowed: closeAllowed,
      reason: closePermission ? null : "Missing permission: contract.close",
    },
    cancel: {
      allowed: cancelAllowed,
      reason: cancelPermission ? null : "Missing permission: contract.cancel",
    },
  };
}

