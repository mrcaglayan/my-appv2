export const REPORT_TABS = Object.freeze({
  AR_AGING: "AR_AGING",
  AP_AGING: "AP_AGING",
  OPEN_ITEMS: "OPEN_ITEMS",
  STATEMENT: "STATEMENT",
});

export const STATUS_FILTER_OPTIONS = ["OPEN", "PARTIALLY_SETTLED", "SETTLED", "ALL"];
export const ROLE_FILTER_OPTIONS = ["", "CUSTOMER", "VENDOR", "BOTH"];

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function normalizeText(value) {
  const normalized = String(value || "").trim();
  return normalized || "";
}

function normalizeDate(value) {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return "";
  }
  return /^\d{4}-\d{2}-\d{2}$/.test(normalized) ? normalized : "";
}

export function buildCariReportQuery(filters, tab) {
  const params = {
    asOfDate: normalizeDate(filters.asOfDate),
    legalEntityId: toPositiveInt(filters.legalEntityId) || undefined,
    counterpartyId: toPositiveInt(filters.counterpartyId) || undefined,
    role: normalizeText(filters.role) || undefined,
    status: normalizeText(filters.status) || undefined,
    includeDetails: true,
    limit: toPositiveInt(filters.limit) || 200,
    offset: Number.isInteger(Number(filters.offset)) && Number(filters.offset) >= 0
      ? Number(filters.offset)
      : 0,
  };

  if (tab === REPORT_TABS.AR_AGING) {
    return {
      ...params,
      direction: "AR",
    };
  }
  if (tab === REPORT_TABS.AP_AGING) {
    return {
      ...params,
      direction: "AP",
    };
  }
  return params;
}

export function reconcileOpenItemsSummary(reportData) {
  const rows = Array.isArray(reportData?.rows) ? reportData.rows : [];
  const summary = reportData?.summary || {};
  const rowsResidualTxn = rows.reduce(
    (sum, row) => sum + toNumber(row?.residualAmountTxnAsOf),
    0
  );
  const rowsResidualBase = rows.reduce(
    (sum, row) => sum + toNumber(row?.residualAmountBaseAsOf),
    0
  );
  const summaryResidualTxn = toNumber(summary?.residualAmountTxnTotal);
  const summaryResidualBase = toNumber(summary?.residualAmountBaseTotal);

  return {
    rowsResidualTxn: Number(rowsResidualTxn.toFixed(6)),
    rowsResidualBase: Number(rowsResidualBase.toFixed(6)),
    summaryResidualTxn: Number(summaryResidualTxn.toFixed(6)),
    summaryResidualBase: Number(summaryResidualBase.toFixed(6)),
    txnDiff: Number((rowsResidualTxn - summaryResidualTxn).toFixed(6)),
    baseDiff: Number((rowsResidualBase - summaryResidualBase).toFixed(6)),
    matches:
      Math.abs(rowsResidualTxn - summaryResidualTxn) <= 0.000001 &&
      Math.abs(rowsResidualBase - summaryResidualBase) <= 0.000001,
  };
}

export function reconcileStatementSummary(reportData) {
  const reconcile = reportData?.summary?.reconcile || {};
  const txnDiff =
    toNumber(reconcile?.openResidualAmountTxnFromDocuments) -
    toNumber(reconcile?.openResidualAmountTxnFromOpenItems);
  const baseDiff =
    toNumber(reconcile?.openResidualAmountBaseFromDocuments) -
    toNumber(reconcile?.openResidualAmountBaseFromOpenItems);

  return {
    txnDiff: Number(txnDiff.toFixed(6)),
    baseDiff: Number(baseDiff.toFixed(6)),
    matches: Math.abs(txnDiff) <= 0.000001 && Math.abs(baseDiff) <= 0.000001,
  };
}
