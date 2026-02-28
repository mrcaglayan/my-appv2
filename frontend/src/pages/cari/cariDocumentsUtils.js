export const DOCUMENT_STATUSES = [
  "DRAFT",
  "POSTED",
  "PARTIALLY_SETTLED",
  "SETTLED",
  "CANCELLED",
  "REVERSED",
];

export const DOCUMENT_DIRECTIONS = ["AR", "AP"];

export const DOCUMENT_TYPES = [
  "INVOICE",
  "DEBIT_NOTE",
  "CREDIT_NOTE",
  "PAYMENT",
  "ADJUSTMENT",
];

export const DUE_DATE_REQUIRED_TYPES = new Set(["INVOICE", "DEBIT_NOTE"]);

export function buildDocumentListQuery(filters) {
  return {
    legalEntityId: filters.legalEntityId || undefined,
    counterpartyId: filters.counterpartyId || undefined,
    direction: filters.direction || undefined,
    documentType: filters.documentType || undefined,
    status: filters.status || undefined,
    dateFrom: filters.dateFrom || filters.documentDateFrom || undefined,
    dateTo: filters.dateTo || filters.documentDateTo || undefined,
    q: filters.q || undefined,
    limit: filters.limit || 100,
    offset: filters.offset || 0,
  };
}

export function requiresDueDate(documentType) {
  return DUE_DATE_REQUIRED_TYPES.has(String(documentType || "").toUpperCase());
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

export function mapDocumentRowToForm(row) {
  return {
    legalEntityId: String(row?.legalEntityId || ""),
    counterpartyId: String(row?.counterpartyId || ""),
    paymentTermId: String(row?.paymentTermId || ""),
    direction: String(row?.direction || "AR"),
    documentType: String(row?.documentType || "INVOICE"),
    documentDate: String(row?.documentDate || ""),
    dueDate: String(row?.dueDate || ""),
    amountTxn:
      row?.amountTxn === null || row?.amountTxn === undefined
        ? ""
        : String(row.amountTxn),
    amountBase:
      row?.amountBase === null || row?.amountBase === undefined
        ? ""
        : String(row.amountBase),
    currencyCode: String(row?.currencyCode || ""),
    fxRate:
      row?.fxRate === null || row?.fxRate === undefined
        ? ""
        : String(row.fxRate),
  };
}

export function buildDocumentMutationPayload(form) {
  const legalEntityId = toPositiveInt(form.legalEntityId);
  const counterpartyId = toPositiveInt(form.counterpartyId);
  const paymentTermId = toPositiveInt(form.paymentTermId);
  const amountTxn = toOptionalNumber(form.amountTxn);
  const amountBase = toOptionalNumber(form.amountBase);
  const fxRate = toOptionalNumber(form.fxRate);
  const direction = String(form.direction || "").trim().toUpperCase();
  const documentType = String(form.documentType || "").trim().toUpperCase();
  const documentDate = String(form.documentDate || "").trim();
  const dueDate = String(form.dueDate || "").trim();
  const currencyCode = String(form.currencyCode || "")
    .trim()
    .toUpperCase();

  return {
    legalEntityId,
    counterpartyId,
    paymentTermId,
    direction,
    documentType,
    documentDate,
    dueDate: dueDate || null,
    amountTxn,
    amountBase,
    currencyCode,
    fxRate,
  };
}

export function validateDocumentMutationForm(form) {
  const payload = buildDocumentMutationPayload(form);
  const errors = [];
  if (!payload.legalEntityId) {
    errors.push("legalEntityId is required.");
  }
  if (!payload.counterpartyId) {
    errors.push("counterpartyId is required.");
  }
  if (!DOCUMENT_DIRECTIONS.includes(payload.direction)) {
    errors.push("direction must be AR or AP.");
  }
  if (!DOCUMENT_TYPES.includes(payload.documentType)) {
    errors.push("documentType is invalid.");
  }
  if (!payload.documentDate) {
    errors.push("documentDate is required.");
  }
  if (requiresDueDate(payload.documentType) && !payload.dueDate) {
    errors.push(`dueDate is required for documentType=${payload.documentType}.`);
  }
  if (payload.amountTxn === null || payload.amountTxn <= 0) {
    errors.push("amountTxn must be > 0.");
  }
  if (payload.amountBase === null || payload.amountBase <= 0) {
    errors.push("amountBase must be > 0.");
  }
  if (!/^[A-Z]{3}$/.test(payload.currencyCode)) {
    errors.push("currencyCode must be a 3-letter code.");
  }
  if (String(form.fxRate || "").trim() && (payload.fxRate === null || payload.fxRate <= 0)) {
    errors.push("fxRate must be > 0 when provided.");
  }

  return {
    payload,
    errors,
  };
}
