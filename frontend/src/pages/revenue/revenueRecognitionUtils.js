export const REVENUE_ACCOUNT_FAMILIES = [
  "DEFREV",
  "PREPAID_EXPENSE",
  "ACCRUED_REVENUE",
  "ACCRUED_EXPENSE",
];
export const REVENUE_MATURITY_BUCKETS = ["SHORT_TERM", "LONG_TERM"];
export const REVENUE_RUN_STATUSES = ["DRAFT", "READY", "POSTED", "REVERSED"];
export const REVENUE_SCHEDULE_STATUSES = ["DRAFT", "READY", "POSTED", "REVERSED"];

export const REVENUE_FAMILY_LABELS = Object.freeze({
  DEFREV: "Gelecek Aylar/Yillar Gelirleri",
  PREPAID_EXPENSE: "Gelecek Aylara/Yillara Ait Giderler (180/280)",
  ACCRUED_REVENUE: "Gelir Tahakkuklari (181/281)",
  ACCRUED_EXPENSE: "Gider Tahakkuklari (381/481)",
});

function toUpper(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
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

export function familyLabel(accountFamily) {
  return REVENUE_FAMILY_LABELS[toUpper(accountFamily)] || toUpper(accountFamily) || "-";
}

export function createInitialScheduleForm() {
  return {
    legalEntityId: "",
    fiscalPeriodId: "",
    accountFamily: "DEFREV",
    maturityBucket: "LONG_TERM",
    maturityDate: "",
    reclassRequired: true,
    currencyCode: "USD",
    fxRate: "1",
    amountTxn: "",
    amountBase: "",
    sourceEventUid: "",
  };
}

export function createInitialRunForm() {
  return {
    legalEntityId: "",
    fiscalPeriodId: "",
    scheduleId: "",
    runNo: "",
    sourceRunUid: "",
    accountFamily: "DEFREV",
    maturityBucket: "LONG_TERM",
    maturityDate: "",
    reclassRequired: true,
    currencyCode: "USD",
    fxRate: "1",
    totalAmountTxn: "",
    totalAmountBase: "",
  };
}

export function createInitialRunActionForm() {
  return {
    settlementPeriodId: "",
    reversalPeriodId: "",
    reason: "Manual reversal",
  };
}

export function createInitialReportFilters() {
  return {
    legalEntityId: "",
    fiscalPeriodId: "",
    accountFamily: "",
    asOfDate: "",
    limit: 100,
    offset: 0,
  };
}

export function buildRevenueListQuery(filters = {}) {
  return {
    legalEntityId: toPositiveInt(filters.legalEntityId) || undefined,
    fiscalPeriodId: toPositiveInt(filters.fiscalPeriodId) || undefined,
    accountFamily: toUpper(filters.accountFamily) || undefined,
    status: toUpper(filters.status) || undefined,
    q: String(filters.q || "").trim() || undefined,
    limit: toPositiveInt(filters.limit) || 100,
    offset:
      Number.isInteger(Number(filters.offset)) && Number(filters.offset) >= 0
        ? Number(filters.offset)
        : 0,
  };
}

export function buildRevenueReportQuery(filters = {}) {
  return {
    legalEntityId: toPositiveInt(filters.legalEntityId) || undefined,
    fiscalPeriodId: toPositiveInt(filters.fiscalPeriodId) || undefined,
    accountFamily: toUpper(filters.accountFamily) || undefined,
    asOfDate: String(filters.asOfDate || "").trim() || undefined,
    limit: toPositiveInt(filters.limit) || 100,
    offset:
      Number.isInteger(Number(filters.offset)) && Number(filters.offset) >= 0
        ? Number(filters.offset)
        : 0,
  };
}

export function buildRevenueSchedulePayload(form = {}) {
  const fxRate = toOptionalNumber(form.fxRate);
  return {
    legalEntityId: toPositiveInt(form.legalEntityId),
    fiscalPeriodId: toPositiveInt(form.fiscalPeriodId),
    accountFamily: toUpper(form.accountFamily),
    maturityBucket: toUpper(form.maturityBucket),
    maturityDate: String(form.maturityDate || "").trim(),
    reclassRequired: Boolean(form.reclassRequired),
    currencyCode: toUpper(form.currencyCode),
    fxRate: fxRate === null ? null : fxRate,
    amountTxn: toOptionalNumber(form.amountTxn),
    amountBase: toOptionalNumber(form.amountBase),
    sourceEventUid: String(form.sourceEventUid || "").trim() || null,
  };
}

export function validateRevenueScheduleForm(form = {}) {
  const payload = buildRevenueSchedulePayload(form);
  const errors = [];

  if (!payload.legalEntityId) {
    errors.push("legalEntityId is required.");
  }
  if (!payload.fiscalPeriodId) {
    errors.push("fiscalPeriodId is required.");
  }
  if (!REVENUE_ACCOUNT_FAMILIES.includes(payload.accountFamily)) {
    errors.push("accountFamily is invalid.");
  }
  if (!REVENUE_MATURITY_BUCKETS.includes(payload.maturityBucket)) {
    errors.push("maturityBucket is invalid.");
  }
  if (!payload.maturityDate) {
    errors.push("maturityDate is required.");
  }
  if (!/^[A-Z]{3}$/.test(payload.currencyCode)) {
    errors.push("currencyCode must be a 3-letter code.");
  }
  if (!Number.isFinite(payload.amountTxn)) {
    errors.push("amountTxn is required.");
  }
  if (!Number.isFinite(payload.amountBase)) {
    errors.push("amountBase is required.");
  }

  return { payload, errors };
}

export function buildRevenueRunPayload(form = {}) {
  const fxRate = toOptionalNumber(form.fxRate);
  return {
    legalEntityId: toPositiveInt(form.legalEntityId),
    fiscalPeriodId: toPositiveInt(form.fiscalPeriodId),
    scheduleId: toPositiveInt(form.scheduleId),
    runNo: String(form.runNo || "").trim() || null,
    sourceRunUid: String(form.sourceRunUid || "").trim() || null,
    accountFamily: toUpper(form.accountFamily),
    maturityBucket: toUpper(form.maturityBucket),
    maturityDate: String(form.maturityDate || "").trim(),
    reclassRequired: Boolean(form.reclassRequired),
    currencyCode: toUpper(form.currencyCode),
    fxRate: fxRate === null ? null : fxRate,
    totalAmountTxn: toOptionalNumber(form.totalAmountTxn),
    totalAmountBase: toOptionalNumber(form.totalAmountBase),
  };
}

export function validateRevenueRunForm(form = {}) {
  const payload = buildRevenueRunPayload(form);
  const errors = [];

  if (!payload.legalEntityId) {
    errors.push("legalEntityId is required.");
  }
  if (!payload.fiscalPeriodId) {
    errors.push("fiscalPeriodId is required.");
  }
  if (!REVENUE_ACCOUNT_FAMILIES.includes(payload.accountFamily)) {
    errors.push("accountFamily is invalid.");
  }
  if (!REVENUE_MATURITY_BUCKETS.includes(payload.maturityBucket)) {
    errors.push("maturityBucket is invalid.");
  }
  if (!payload.maturityDate) {
    errors.push("maturityDate is required.");
  }
  if (!/^[A-Z]{3}$/.test(payload.currencyCode)) {
    errors.push("currencyCode must be a 3-letter code.");
  }
  if (!Number.isFinite(payload.totalAmountTxn)) {
    errors.push("totalAmountTxn is required.");
  }
  if (!Number.isFinite(payload.totalAmountBase)) {
    errors.push("totalAmountBase is required.");
  }

  return { payload, errors };
}

export function buildRevenueRunActionPayload(form = {}, action = "post") {
  if (action === "reverse") {
    return {
      reversalPeriodId: toPositiveInt(form.reversalPeriodId),
      reason: String(form.reason || "").trim() || null,
    };
  }
  return {
    settlementPeriodId: toPositiveInt(form.settlementPeriodId),
  };
}

export function estimateReclassToShortTerm(summary = {}) {
  const shortTerm = Number(summary?.shortTermAmountBase || summary?.closingShortTermAmountBase || 0);
  const longTerm = Number(summary?.longTermAmountBase || summary?.closingLongTermAmountBase || 0);
  const movement = Number(summary?.movementAmountBase || 0);

  if (!Number.isFinite(shortTerm) || !Number.isFinite(longTerm) || !Number.isFinite(movement)) {
    return 0;
  }
  return Number(Math.max(0, shortTerm - longTerm - movement).toFixed(6));
}

