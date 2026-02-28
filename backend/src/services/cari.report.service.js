import { query } from "../db.js";
import { parsePositiveInt } from "../routes/_utils.js";

const AMOUNT_SCALE = 6;
const AMOUNT_EPSILON = 0.000001;
const AGING_BUCKETS_IN_ORDER = [
  { code: "CURRENT", label: "Current / Not Due" },
  { code: "DUE_1_30", label: "1-30 Days Past Due" },
  { code: "DUE_31_60", label: "31-60 Days Past Due" },
  { code: "DUE_61_90", label: "61-90 Days Past Due" },
  { code: "DUE_91_PLUS", label: "91+ Days Past Due" },
];

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function deriveCounterpartyType({ isCustomer, isVendor }) {
  if (isCustomer && isVendor) {
    return "BOTH";
  }
  if (isCustomer) {
    return "CUSTOMER";
  }
  if (isVendor) {
    return "VENDOR";
  }
  return "OTHER";
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function roundAmount(value) {
  return Number(toNumber(value).toFixed(AMOUNT_SCALE));
}

function amountsAreEqual(left, right, epsilon = AMOUNT_EPSILON) {
  return Math.abs(toNumber(left) - toNumber(right)) <= epsilon;
}

function toDateOnlyString(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      return null;
    }
    const year = value.getFullYear();
    const month = String(value.getMonth() + 1).padStart(2, "0");
    const day = String(value.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }

  const raw = String(value).trim();
  if (!raw) {
    return null;
  }
  if (/^\d{4}-\d{2}-\d{2}(?:\b|T)/.test(raw)) {
    return raw.slice(0, 10);
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  const year = parsed.getFullYear();
  const month = String(parsed.getMonth() + 1).padStart(2, "0");
  const day = String(parsed.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function toUtcEpochDay(dateOnly) {
  const normalized = toDateOnlyString(dateOnly);
  if (!normalized || !/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    return null;
  }

  const [year, month, day] = normalized.split("-").map((part) => Number(part));
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return null;
  }

  return Date.UTC(year, month - 1, day);
}

function calcDaysPastDue(asOfDate, dueDate) {
  const asOfEpoch = toUtcEpochDay(asOfDate);
  const dueEpoch = toUtcEpochDay(dueDate);
  if (asOfEpoch === null || dueEpoch === null) {
    return null;
  }

  const dayMs = 24 * 60 * 60 * 1000;
  return Math.floor((asOfEpoch - dueEpoch) / dayMs);
}

function resolveAgingBucket(asOfDate, dueDate) {
  const daysPastDue = calcDaysPastDue(asOfDate, dueDate);
  if (daysPastDue === null || daysPastDue <= 0) {
    return {
      code: "CURRENT",
      label: "Current / Not Due",
      daysPastDue,
    };
  }
  if (daysPastDue <= 30) {
    return {
      code: "DUE_1_30",
      label: "1-30 Days Past Due",
      daysPastDue,
    };
  }
  if (daysPastDue <= 60) {
    return {
      code: "DUE_31_60",
      label: "31-60 Days Past Due",
      daysPastDue,
    };
  }
  if (daysPastDue <= 90) {
    return {
      code: "DUE_61_90",
      label: "61-90 Days Past Due",
      daysPastDue,
    };
  }

  return {
    code: "DUE_91_PLUS",
    label: "91+ Days Past Due",
    daysPastDue,
  };
}

function resolveOpenStatus({ originalAmountTxn, residualAmountTxn }) {
  const safeOriginal = roundAmount(originalAmountTxn);
  const safeResidual = roundAmount(residualAmountTxn);

  if (safeResidual <= AMOUNT_EPSILON) {
    return "SETTLED";
  }
  if (safeOriginal - safeResidual <= AMOUNT_EPSILON) {
    return "OPEN";
  }
  return "PARTIALLY_SETTLED";
}

function matchesOpenStatusFilter(status, filter) {
  const normalizedFilter = normalizeUpperText(filter || "ALL") || "ALL";
  const normalizedStatus = normalizeUpperText(status);

  if (normalizedFilter === "ALL") {
    return true;
  }
  if (normalizedFilter === "OPEN") {
    return ["OPEN", "PARTIALLY_SETTLED"].includes(normalizedStatus);
  }
  return normalizedStatus === normalizedFilter;
}

function matchesStatementStatusFilter(status, filter) {
  const normalizedFilter = normalizeUpperText(filter || "ALL") || "ALL";
  const normalizedStatus = normalizeUpperText(status);

  if (normalizedFilter === "ALL") {
    return true;
  }
  if (normalizedFilter === "OPEN") {
    return ["POSTED", "PARTIALLY_SETTLED"].includes(normalizedStatus);
  }
  if (normalizedFilter === "PARTIALLY_SETTLED") {
    return normalizedStatus === "PARTIALLY_SETTLED";
  }
  if (normalizedFilter === "SETTLED") {
    return normalizedStatus === "SETTLED";
  }
  return normalizedStatus === normalizedFilter;
}

function appendRoleCondition({
  conditions,
  params,
  role,
  customerFlagColumn,
  vendorFlagColumn,
}) {
  const normalizedRole = normalizeUpperText(role);
  if (!normalizedRole) {
    return;
  }

  if (normalizedRole === "CUSTOMER") {
    conditions.push(`${customerFlagColumn} = ?`);
    params.push(1);
    return;
  }
  if (normalizedRole === "VENDOR") {
    conditions.push(`${vendorFlagColumn} = ?`);
    params.push(1);
    return;
  }
  if (normalizedRole === "BOTH") {
    conditions.push(`${customerFlagColumn} = ?`);
    conditions.push(`${vendorFlagColumn} = ?`);
    params.push(1, 1);
  }
}

function appendCommonEntityFilters({
  req,
  filters,
  params,
  conditions,
  buildScopeFilter,
  assertScopeAccess,
  legalEntityColumn,
  counterpartyColumn,
  roleCustomerColumn,
  roleVendorColumn,
  directionColumn,
}) {
  if (typeof buildScopeFilter === "function") {
    conditions.push(buildScopeFilter(req, "legal_entity", legalEntityColumn, params));
  }

  if (filters.legalEntityId) {
    if (typeof assertScopeAccess === "function") {
      assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    }
    conditions.push(`${legalEntityColumn} = ?`);
    params.push(filters.legalEntityId);
  }

  if (filters.counterpartyId) {
    conditions.push(`${counterpartyColumn} = ?`);
    params.push(filters.counterpartyId);
  }

  appendRoleCondition({
    conditions,
    params,
    role: filters.role,
    customerFlagColumn: roleCustomerColumn,
    vendorFlagColumn: roleVendorColumn,
  });

  if (directionColumn && filters.direction) {
    conditions.push(`${directionColumn} = ?`);
    params.push(filters.direction);
  }
}

function addCurrencyAggregate(currencyMap, currencyCode, txnAmount, baseAmount) {
  const key = normalizeUpperText(currencyCode) || "-";
  if (!currencyMap.has(key)) {
    currencyMap.set(key, {
      currencyCode: key,
      amountTxnTotal: 0,
      amountBaseTotal: 0,
    });
  }

  const bucket = currencyMap.get(key);
  bucket.amountTxnTotal = roundAmount(bucket.amountTxnTotal + toNumber(txnAmount));
  bucket.amountBaseTotal = roundAmount(bucket.amountBaseTotal + toNumber(baseAmount));
}

function currencyMapToSortedArray(currencyMap) {
  return Array.from(currencyMap.values()).sort((left, right) =>
    String(left.currencyCode).localeCompare(String(right.currencyCode))
  );
}

function summarizeOpenItemRows(rows) {
  let originalTxnTotal = 0;
  let originalBaseTotal = 0;
  let residualTxnTotal = 0;
  let residualBaseTotal = 0;
  let settledTxnTotal = 0;
  let settledBaseTotal = 0;
  let openCount = 0;
  let partialCount = 0;
  let settledCount = 0;
  const byCurrency = new Map();

  for (const row of rows) {
    originalTxnTotal = roundAmount(originalTxnTotal + toNumber(row.originalAmountTxn));
    originalBaseTotal = roundAmount(originalBaseTotal + toNumber(row.originalAmountBase));
    residualTxnTotal = roundAmount(residualTxnTotal + toNumber(row.residualAmountTxnAsOf));
    residualBaseTotal = roundAmount(residualBaseTotal + toNumber(row.residualAmountBaseAsOf));
    settledTxnTotal = roundAmount(settledTxnTotal + toNumber(row.settledAmountTxnAsOf));
    settledBaseTotal = roundAmount(settledBaseTotal + toNumber(row.settledAmountBaseAsOf));

    const normalizedStatus = normalizeUpperText(row.asOfStatus);
    if (normalizedStatus === "OPEN") {
      openCount += 1;
    } else if (normalizedStatus === "PARTIALLY_SETTLED") {
      partialCount += 1;
    } else if (normalizedStatus === "SETTLED") {
      settledCount += 1;
    }

    addCurrencyAggregate(
      byCurrency,
      row.currencyCode,
      row.residualAmountTxnAsOf,
      row.residualAmountBaseAsOf
    );
  }

  return {
    count: rows.length,
    openCount,
    partiallySettledCount: partialCount,
    settledCount,
    originalAmountTxnTotal: originalTxnTotal,
    originalAmountBaseTotal: originalBaseTotal,
    residualAmountTxnTotal: residualTxnTotal,
    residualAmountBaseTotal: residualBaseTotal,
    settledAmountTxnTotal: settledTxnTotal,
    settledAmountBaseTotal: settledBaseTotal,
    residualByCurrency: currencyMapToSortedArray(byCurrency),
  };
}

function summarizeUnappliedRows(rows) {
  let amountTxnTotal = 0;
  let amountBaseTotal = 0;
  let residualTxnTotal = 0;
  let residualBaseTotal = 0;
  let unappliedCount = 0;
  let partialCount = 0;
  let fullCount = 0;
  let reversedCount = 0;
  const byCurrency = new Map();

  for (const row of rows) {
    amountTxnTotal = roundAmount(amountTxnTotal + toNumber(row.amountTxn));
    amountBaseTotal = roundAmount(amountBaseTotal + toNumber(row.amountBase));
    residualTxnTotal = roundAmount(residualTxnTotal + toNumber(row.residualAmountTxnAsOf));
    residualBaseTotal = roundAmount(residualBaseTotal + toNumber(row.residualAmountBaseAsOf));

    const status = normalizeUpperText(row.asOfStatus);
    if (status === "UNAPPLIED") {
      unappliedCount += 1;
    } else if (status === "PARTIALLY_APPLIED") {
      partialCount += 1;
    } else if (status === "FULLY_APPLIED") {
      fullCount += 1;
    } else if (status === "REVERSED") {
      reversedCount += 1;
    }

    addCurrencyAggregate(
      byCurrency,
      row.currencyCode,
      row.residualAmountTxnAsOf,
      row.residualAmountBaseAsOf
    );
  }

  return {
    count: rows.length,
    unappliedCount,
    partiallyAppliedCount: partialCount,
    fullyAppliedCount: fullCount,
    reversedCount,
    amountTxnTotal,
    amountBaseTotal,
    residualAmountTxnTotal: residualTxnTotal,
    residualAmountBaseTotal: residualBaseTotal,
    residualByCurrency: currencyMapToSortedArray(byCurrency),
  };
}

function buildAgingBucketSummary(rows) {
  const buckets = new Map(
    AGING_BUCKETS_IN_ORDER.map((bucket) => [
      bucket.code,
      {
        bucketCode: bucket.code,
        bucketLabel: bucket.label,
        count: 0,
        originalAmountTxnTotal: 0,
        originalAmountBaseTotal: 0,
        residualAmountTxnTotal: 0,
        residualAmountBaseTotal: 0,
      },
    ])
  );

  for (const row of rows) {
    const code = row.agingBucket?.code || "CURRENT";
    if (!buckets.has(code)) {
      buckets.set(code, {
        bucketCode: code,
        bucketLabel: row.agingBucket?.label || code,
        count: 0,
        originalAmountTxnTotal: 0,
        originalAmountBaseTotal: 0,
        residualAmountTxnTotal: 0,
        residualAmountBaseTotal: 0,
      });
    }

    const bucket = buckets.get(code);
    bucket.count += 1;
    bucket.originalAmountTxnTotal = roundAmount(
      bucket.originalAmountTxnTotal + toNumber(row.originalAmountTxn)
    );
    bucket.originalAmountBaseTotal = roundAmount(
      bucket.originalAmountBaseTotal + toNumber(row.originalAmountBase)
    );
    bucket.residualAmountTxnTotal = roundAmount(
      bucket.residualAmountTxnTotal + toNumber(row.residualAmountTxnAsOf)
    );
    bucket.residualAmountBaseTotal = roundAmount(
      bucket.residualAmountBaseTotal + toNumber(row.residualAmountBaseAsOf)
    );
  }

  return AGING_BUCKETS_IN_ORDER.map((bucket) => buckets.get(bucket.code));
}

function buildCounterpartySummary(rows) {
  const byCounterparty = new Map();

  for (const row of rows) {
    const counterpartyId = parsePositiveInt(row.counterpartyId) || 0;
    if (!byCounterparty.has(counterpartyId)) {
      byCounterparty.set(counterpartyId, {
        counterpartyId,
        counterpartyCode: row.counterpartyCodeSnapshot || row.counterpartyCodeCurrent || null,
        counterpartyName: row.counterpartyNameSnapshot || row.counterpartyNameCurrent || null,
        counterpartyType: row.counterpartyType || null,
        count: 0,
        residualAmountTxnTotal: 0,
        residualAmountBaseTotal: 0,
      });
    }

    const bucket = byCounterparty.get(counterpartyId);
    bucket.count += 1;
    bucket.residualAmountTxnTotal = roundAmount(
      bucket.residualAmountTxnTotal + toNumber(row.residualAmountTxnAsOf)
    );
    bucket.residualAmountBaseTotal = roundAmount(
      bucket.residualAmountBaseTotal + toNumber(row.residualAmountBaseAsOf)
    );
  }

  return Array.from(byCounterparty.values()).sort((left, right) =>
    String(left.counterpartyName || left.counterpartyCode || left.counterpartyId).localeCompare(
      String(right.counterpartyName || right.counterpartyCode || right.counterpartyId)
    )
  );
}

function paginateRows(rows, limit, offset) {
  const safeLimit = Number.isInteger(limit) && limit > 0 ? limit : 200;
  const safeOffset = Number.isInteger(offset) && offset >= 0 ? offset : 0;
  return {
    total: rows.length,
    limit: safeLimit,
    offset: safeOffset,
    rows: rows.slice(safeOffset, safeOffset + safeLimit),
  };
}

function parseJsonPayload(value) {
  if (!value) {
    return null;
  }
  if (typeof value === "object") {
    return value;
  }
  if (typeof value === "string") {
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }
  return null;
}

function mapOpenItemAsOfRow(row, asOfDate) {
  const originalAmountTxn = roundAmount(row.original_amount_txn);
  const originalAmountBase = roundAmount(row.original_amount_base);
  const allocatedAmountTxn = roundAmount(row.allocated_txn_as_of);
  const allocatedAmountBase = roundAmount(row.allocated_base_as_of);

  let residualAmountTxn = roundAmount(originalAmountTxn - allocatedAmountTxn);
  let residualAmountBase = roundAmount(originalAmountBase - allocatedAmountBase);
  if (residualAmountTxn < 0 && Math.abs(residualAmountTxn) <= AMOUNT_EPSILON) {
    residualAmountTxn = 0;
  }
  if (residualAmountBase < 0 && Math.abs(residualAmountBase) <= AMOUNT_EPSILON) {
    residualAmountBase = 0;
  }
  residualAmountTxn = Math.max(0, residualAmountTxn);
  residualAmountBase = Math.max(0, residualAmountBase);

  const settledAmountTxn = roundAmount(originalAmountTxn - residualAmountTxn);
  const settledAmountBase = roundAmount(originalAmountBase - residualAmountBase);
  const asOfStatus = resolveOpenStatus({
    originalAmountTxn,
    residualAmountTxn,
  });

  const dueDate = toDateOnlyString(row.open_item_due_date || row.due_date_snapshot || row.document_date);
  const agingBucket = resolveAgingBucket(asOfDate, dueDate);
  const isCustomer = parseDbBoolean(row.counterparty_is_customer);
  const isVendor = parseDbBoolean(row.counterparty_is_vendor);

  return {
    openItemId: parsePositiveInt(row.open_item_id),
    tenantId: parsePositiveInt(row.tenant_id),
    legalEntityId: parsePositiveInt(row.legal_entity_id),
    legalEntityCode: row.legal_entity_code || null,
    legalEntityName: row.legal_entity_name || null,
    counterpartyId: parsePositiveInt(row.counterparty_id),
    counterpartyCodeSnapshot: row.counterparty_code_snapshot || null,
    counterpartyNameSnapshot: row.counterparty_name_snapshot || null,
    counterpartyCodeCurrent: row.counterparty_code_current || null,
    counterpartyNameCurrent: row.counterparty_name_current || null,
    counterpartyType: deriveCounterpartyType({ isCustomer, isVendor }),
    isCustomer,
    isVendor,
    counterpartyStatus: row.counterparty_status || null,
    documentId: parsePositiveInt(row.document_id),
    documentNo: row.document_no || null,
    documentDate: toDateOnlyString(row.document_date),
    dueDate,
    direction: row.direction || null,
    documentType: row.document_type || null,
    documentStatusCurrent: row.document_status_current || null,
    paymentTermSnapshot: row.payment_term_snapshot || null,
    dueDateSnapshot: toDateOnlyString(row.due_date_snapshot),
    currencyCodeSnapshot: row.currency_code_snapshot || null,
    fxRateSnapshot: row.fx_rate_snapshot === null ? null : toNumber(row.fx_rate_snapshot),
    currencyCode: row.open_item_currency_code || row.currency_code_snapshot || null,
    originalAmountTxn,
    originalAmountBase,
    allocatedAmountTxnAsOf: allocatedAmountTxn,
    allocatedAmountBaseAsOf: allocatedAmountBase,
    residualAmountTxnAsOf: residualAmountTxn,
    residualAmountBaseAsOf: residualAmountBase,
    settledAmountTxnAsOf: settledAmountTxn,
    settledAmountBaseAsOf: settledAmountBase,
    asOfStatus,
    agingBucket,
    daysPastDue: agingBucket.daysPastDue,
    settlementContext: {
      allocationCountAsOf: Number(row.allocation_count_as_of || 0),
      lastSettlementDateAsOf: toDateOnlyString(row.last_settlement_date_as_of),
      bankLinkedAllocationCountAsOf: Number(row.bank_linked_allocation_count_as_of || 0),
    },
  };
}

async function loadOpenItemAsOfRows({
  req,
  filters,
  buildScopeFilter,
  assertScopeAccess,
  runQuery = query,
}) {
  const params = [filters.tenantId, filters.asOfDate, filters.asOfDate, filters.tenantId];
  const conditions = ["oi.tenant_id = ?"];

  appendCommonEntityFilters({
    req,
    filters,
    params,
    conditions,
    buildScopeFilter,
    assertScopeAccess,
    legalEntityColumn: "oi.legal_entity_id",
    counterpartyColumn: "oi.counterparty_id",
    roleCustomerColumn: "cp.is_customer",
    roleVendorColumn: "cp.is_vendor",
    directionColumn: "d.direction",
  });

  conditions.push("d.document_date <= ?");
  params.push(filters.asOfDate);
  conditions.push("d.status <> 'DRAFT'");
  conditions.push("d.status <> 'CANCELLED'");
  conditions.push("(reversal_doc.id IS NULL OR reversal_doc.document_date > ?)");
  params.push(filters.asOfDate);

  const rowsResult = await runQuery(
    `SELECT
       oi.id AS open_item_id,
       oi.tenant_id,
       oi.legal_entity_id,
       oi.counterparty_id,
       oi.document_id,
       oi.document_date AS open_item_document_date,
       oi.due_date AS open_item_due_date,
       oi.original_amount_txn,
       oi.original_amount_base,
       oi.currency_code AS open_item_currency_code,
       d.document_no,
       d.document_date,
       d.due_date,
       d.direction,
       d.document_type,
       d.status AS document_status_current,
       d.counterparty_code_snapshot,
       d.counterparty_name_snapshot,
       d.payment_term_snapshot,
       d.due_date_snapshot,
       d.currency_code_snapshot,
       d.fx_rate_snapshot,
       cp.code AS counterparty_code_current,
       cp.name AS counterparty_name_current,
       cp.is_customer AS counterparty_is_customer,
       cp.is_vendor AS counterparty_is_vendor,
       cp.status AS counterparty_status,
       le.code AS legal_entity_code,
       le.name AS legal_entity_name,
       COALESCE(alloc_stats.allocated_txn, 0.000000) AS allocated_txn_as_of,
       COALESCE(alloc_stats.allocated_base, 0.000000) AS allocated_base_as_of,
       COALESCE(alloc_stats.allocation_count, 0) AS allocation_count_as_of,
       alloc_stats.last_settlement_date AS last_settlement_date_as_of,
       COALESCE(alloc_stats.bank_linked_allocation_count, 0) AS bank_linked_allocation_count_as_of
     FROM cari_open_items oi
     JOIN cari_documents d
       ON d.tenant_id = oi.tenant_id
      AND d.legal_entity_id = oi.legal_entity_id
      AND d.id = oi.document_id
     LEFT JOIN cari_documents reversal_doc
       ON reversal_doc.tenant_id = d.tenant_id
      AND reversal_doc.reversal_of_document_id = d.id
     LEFT JOIN counterparties cp
       ON cp.tenant_id = oi.tenant_id
      AND cp.legal_entity_id = oi.legal_entity_id
      AND cp.id = oi.counterparty_id
     LEFT JOIN legal_entities le
       ON le.tenant_id = oi.tenant_id
      AND le.id = oi.legal_entity_id
     LEFT JOIN (
       SELECT
         a.tenant_id,
         a.legal_entity_id,
         a.open_item_id,
         SUM(a.allocation_amount_txn) AS allocated_txn,
         SUM(a.allocation_amount_base) AS allocated_base,
         COUNT(*) AS allocation_count,
         MAX(b.settlement_date) AS last_settlement_date,
         SUM(
           CASE
             WHEN b.bank_statement_line_id IS NOT NULL
               OR b.bank_transaction_ref IS NOT NULL
               OR a.bank_statement_line_id IS NOT NULL
             THEN 1
             ELSE 0
           END
         ) AS bank_linked_allocation_count
       FROM cari_settlement_allocations a
       JOIN cari_settlement_batches b
         ON b.tenant_id = a.tenant_id
        AND b.legal_entity_id = a.legal_entity_id
        AND b.id = a.settlement_batch_id
       LEFT JOIN cari_settlement_batches b_rev
         ON b_rev.tenant_id = b.tenant_id
        AND b_rev.legal_entity_id = b.legal_entity_id
        AND b_rev.reversal_of_settlement_batch_id = b.id
       WHERE a.tenant_id = ?
         AND b.settlement_date <= ?
         AND (b_rev.id IS NULL OR b_rev.settlement_date > ?)
       GROUP BY a.tenant_id, a.legal_entity_id, a.open_item_id
     ) alloc_stats
       ON alloc_stats.tenant_id = oi.tenant_id
      AND alloc_stats.legal_entity_id = oi.legal_entity_id
      AND alloc_stats.open_item_id = oi.id
     WHERE ${conditions.join(" AND ")}
     ORDER BY oi.due_date ASC, oi.document_date ASC, oi.id ASC`,
    params
  );

  return (rowsResult.rows || []).map((row) => mapOpenItemAsOfRow(row, filters.asOfDate));
}

async function loadSettlementReferenceMapForOpenItems({
  tenantId,
  asOfDate,
  openItemIds,
  runQuery = query,
}) {
  if (!Array.isArray(openItemIds) || openItemIds.length === 0) {
    return new Map();
  }

  const uniqueOpenItemIds = Array.from(
    new Set(openItemIds.map((value) => parsePositiveInt(value)).filter(Boolean))
  );
  if (uniqueOpenItemIds.length === 0) {
    return new Map();
  }

  const rowsResult = await runQuery(
    `SELECT
       a.open_item_id,
       b.id AS settlement_batch_id,
       b.settlement_no,
       b.settlement_date,
       b.status AS settlement_status,
       b.bank_statement_line_id,
       b.bank_transaction_ref,
       b.bank_apply_idempotency_key,
       b_rev.id AS reversal_settlement_batch_id,
       b_rev.settlement_no AS reversal_settlement_no,
       b_rev.settlement_date AS reversal_settlement_date
     FROM cari_settlement_allocations a
     JOIN cari_settlement_batches b
       ON b.tenant_id = a.tenant_id
      AND b.legal_entity_id = a.legal_entity_id
      AND b.id = a.settlement_batch_id
     LEFT JOIN cari_settlement_batches b_rev
       ON b_rev.tenant_id = b.tenant_id
      AND b_rev.legal_entity_id = b.legal_entity_id
      AND b_rev.reversal_of_settlement_batch_id = b.id
     WHERE a.tenant_id = ?
       AND b.settlement_date <= ?
       AND a.open_item_id IN (${uniqueOpenItemIds.map(() => "?").join(", ")})
     ORDER BY b.settlement_date ASC, b.id ASC, a.id ASC`,
    [tenantId, asOfDate, ...uniqueOpenItemIds]
  );

  const byOpenItemId = new Map();
  for (const row of rowsResult.rows || []) {
    const openItemId = parsePositiveInt(row.open_item_id);
    if (!openItemId) {
      continue;
    }

    if (!byOpenItemId.has(openItemId)) {
      byOpenItemId.set(openItemId, []);
    }

    const list = byOpenItemId.get(openItemId);
    const settlementBatchId = parsePositiveInt(row.settlement_batch_id);
    if (list.some((entry) => entry.settlementBatchId === settlementBatchId)) {
      continue;
    }

    const reversalDate = toDateOnlyString(row.reversal_settlement_date);
    const activeAsOf = !reversalDate || reversalDate > asOfDate;
    list.push({
      settlementBatchId,
      settlementNo: row.settlement_no || null,
      settlementDate: toDateOnlyString(row.settlement_date),
      settlementStatusCurrent: row.settlement_status || null,
      activeAsOf,
      reversalSettlementBatchId: parsePositiveInt(row.reversal_settlement_batch_id),
      reversalSettlementNo: row.reversal_settlement_no || null,
      reversalSettlementDate: reversalDate,
      bankStatementLineId: parsePositiveInt(row.bank_statement_line_id),
      bankTransactionRef: row.bank_transaction_ref || null,
      bankApplyIdempotencyKey: row.bank_apply_idempotency_key || null,
    });
  }

  return byOpenItemId;
}

async function loadStatementDocumentRows({
  req,
  filters,
  buildScopeFilter,
  assertScopeAccess,
  runQuery = query,
}) {
  const params = [filters.tenantId, filters.asOfDate];
  const conditions = [
    "d.tenant_id = ?",
    "d.document_date <= ?",
    "d.status <> 'DRAFT'",
    "d.status <> 'CANCELLED'",
  ];

  appendCommonEntityFilters({
    req,
    filters,
    params,
    conditions,
    buildScopeFilter,
    assertScopeAccess,
    legalEntityColumn: "d.legal_entity_id",
    counterpartyColumn: "d.counterparty_id",
    roleCustomerColumn: "cp.is_customer",
    roleVendorColumn: "cp.is_vendor",
    directionColumn: "d.direction",
  });

  const rowsResult = await runQuery(
    `SELECT
       d.id,
       d.tenant_id,
       d.legal_entity_id,
       d.counterparty_id,
       d.direction,
       d.document_type,
       d.document_no,
       d.status,
       d.document_date,
       d.due_date,
       d.amount_txn,
       d.amount_base,
       d.open_amount_txn,
       d.open_amount_base,
       d.currency_code,
       d.fx_rate,
       d.counterparty_code_snapshot,
       d.counterparty_name_snapshot,
       d.payment_term_snapshot,
       d.due_date_snapshot,
       d.currency_code_snapshot,
       d.fx_rate_snapshot,
       d.posted_journal_entry_id,
       d.reversal_of_document_id,
       d.posted_at,
       d.reversed_at,
       cp.code AS counterparty_code_current,
       cp.name AS counterparty_name_current,
       cp.is_customer AS counterparty_is_customer,
       cp.is_vendor AS counterparty_is_vendor,
       cp.status AS counterparty_status,
       le.code AS legal_entity_code,
       le.name AS legal_entity_name,
       reversed_by.id AS reversed_by_document_id,
       reversed_by.document_no AS reversed_by_document_no,
       reversed_by.document_date AS reversed_by_document_date
     FROM cari_documents d
     LEFT JOIN counterparties cp
       ON cp.tenant_id = d.tenant_id
      AND cp.legal_entity_id = d.legal_entity_id
      AND cp.id = d.counterparty_id
     LEFT JOIN legal_entities le
       ON le.tenant_id = d.tenant_id
      AND le.id = d.legal_entity_id
     LEFT JOIN cari_documents reversed_by
       ON reversed_by.tenant_id = d.tenant_id
      AND reversed_by.reversal_of_document_id = d.id
     WHERE ${conditions.join(" AND ")}
     ORDER BY d.document_date ASC, d.id ASC`,
    params
  );

  return (rowsResult.rows || []).map((row) => {
    const isCustomer = parseDbBoolean(row.counterparty_is_customer);
    const isVendor = parseDbBoolean(row.counterparty_is_vendor);
    return {
      documentId: parsePositiveInt(row.id),
      tenantId: parsePositiveInt(row.tenant_id),
      legalEntityId: parsePositiveInt(row.legal_entity_id),
      legalEntityCode: row.legal_entity_code || null,
      legalEntityName: row.legal_entity_name || null,
      counterpartyId: parsePositiveInt(row.counterparty_id),
      counterpartyCodeSnapshot: row.counterparty_code_snapshot || null,
      counterpartyNameSnapshot: row.counterparty_name_snapshot || null,
      counterpartyCodeCurrent: row.counterparty_code_current || null,
      counterpartyNameCurrent: row.counterparty_name_current || null,
      counterpartyType: deriveCounterpartyType({ isCustomer, isVendor }),
      isCustomer,
      isVendor,
      counterpartyStatus: row.counterparty_status || null,
      direction: row.direction || null,
      documentType: row.document_type || null,
      documentNo: row.document_no || null,
      documentDate: toDateOnlyString(row.document_date),
      dueDate: toDateOnlyString(row.due_date),
      amountTxn: roundAmount(row.amount_txn),
      amountBase: roundAmount(row.amount_base),
      openAmountTxnCurrent: roundAmount(row.open_amount_txn),
      openAmountBaseCurrent: roundAmount(row.open_amount_base),
      currencyCode: row.currency_code || null,
      fxRate: row.fx_rate === null ? null : toNumber(row.fx_rate),
      paymentTermSnapshot: row.payment_term_snapshot || null,
      dueDateSnapshot: toDateOnlyString(row.due_date_snapshot),
      currencyCodeSnapshot: row.currency_code_snapshot || null,
      fxRateSnapshot: row.fx_rate_snapshot === null ? null : toNumber(row.fx_rate_snapshot),
      postedJournalEntryId: parsePositiveInt(row.posted_journal_entry_id),
      reversalOfDocumentId: parsePositiveInt(row.reversal_of_document_id),
      reversedByDocumentId: parsePositiveInt(row.reversed_by_document_id),
      reversedByDocumentNo: row.reversed_by_document_no || null,
      reversedByDocumentDate: toDateOnlyString(row.reversed_by_document_date),
      postedAt: row.posted_at || null,
      reversedAt: row.reversed_at || null,
      statusCurrent: row.status || null,
    };
  });
}

async function loadStatementSettlementRows({
  req,
  filters,
  buildScopeFilter,
  assertScopeAccess,
  runQuery = query,
}) {
  const params = [filters.tenantId, filters.asOfDate];
  const conditions = ["b.tenant_id = ?", "b.settlement_date <= ?"];

  appendCommonEntityFilters({
    req,
    filters,
    params,
    conditions,
    buildScopeFilter,
    assertScopeAccess,
    legalEntityColumn: "b.legal_entity_id",
    counterpartyColumn: "b.counterparty_id",
    roleCustomerColumn: "cp.is_customer",
    roleVendorColumn: "cp.is_vendor",
    directionColumn: null,
  });

  const rowsResult = await runQuery(
    `SELECT
       b.id,
       b.tenant_id,
       b.legal_entity_id,
       b.counterparty_id,
       b.cash_transaction_id,
       b.sequence_namespace,
       b.fiscal_year,
       b.sequence_no,
       b.settlement_no,
       b.settlement_date,
       b.status,
       b.total_allocated_txn,
       b.total_allocated_base,
       b.currency_code,
       b.posted_journal_entry_id,
       b.reversal_of_settlement_batch_id,
       b.bank_statement_line_id,
       b.bank_transaction_ref,
       b.bank_attach_idempotency_key,
       b.bank_apply_idempotency_key,
       b.created_at,
       b.updated_at,
       b.posted_at,
       b.reversed_at,
       cp.code AS counterparty_code_current,
       cp.name AS counterparty_name_current,
       cp.is_customer AS counterparty_is_customer,
       cp.is_vendor AS counterparty_is_vendor,
       cp.status AS counterparty_status,
       le.code AS legal_entity_code,
       le.name AS legal_entity_name,
       reversed_by.id AS reversed_by_settlement_batch_id,
       reversed_by.settlement_no AS reversed_by_settlement_no,
       reversed_by.settlement_date AS reversed_by_settlement_date,
       original.settlement_no AS reversal_of_settlement_no,
       original.settlement_date AS reversal_of_settlement_date
     FROM cari_settlement_batches b
     LEFT JOIN counterparties cp
       ON cp.tenant_id = b.tenant_id
      AND cp.legal_entity_id = b.legal_entity_id
      AND cp.id = b.counterparty_id
     LEFT JOIN legal_entities le
       ON le.tenant_id = b.tenant_id
      AND le.id = b.legal_entity_id
     LEFT JOIN cari_settlement_batches reversed_by
       ON reversed_by.tenant_id = b.tenant_id
      AND reversed_by.legal_entity_id = b.legal_entity_id
      AND reversed_by.reversal_of_settlement_batch_id = b.id
     LEFT JOIN cari_settlement_batches original
       ON original.tenant_id = b.tenant_id
      AND original.legal_entity_id = b.legal_entity_id
      AND original.id = b.reversal_of_settlement_batch_id
     WHERE ${conditions.join(" AND ")}
     ORDER BY b.settlement_date ASC, b.id ASC`,
    params
  );

  return (rowsResult.rows || []).map((row) => {
    const isCustomer = parseDbBoolean(row.counterparty_is_customer);
    const isVendor = parseDbBoolean(row.counterparty_is_vendor);
    return {
      settlementBatchId: parsePositiveInt(row.id),
      tenantId: parsePositiveInt(row.tenant_id),
      legalEntityId: parsePositiveInt(row.legal_entity_id),
      legalEntityCode: row.legal_entity_code || null,
      legalEntityName: row.legal_entity_name || null,
      counterpartyId: parsePositiveInt(row.counterparty_id),
      cashTransactionId: parsePositiveInt(row.cash_transaction_id),
      counterpartyCodeCurrent: row.counterparty_code_current || null,
      counterpartyNameCurrent: row.counterparty_name_current || null,
      counterpartyType: deriveCounterpartyType({ isCustomer, isVendor }),
      isCustomer,
      isVendor,
      counterpartyStatus: row.counterparty_status || null,
      sequenceNamespace: row.sequence_namespace || null,
      fiscalYear: Number(row.fiscal_year || 0),
      sequenceNo: Number(row.sequence_no || 0),
      settlementNo: row.settlement_no || null,
      settlementDate: toDateOnlyString(row.settlement_date),
      statusCurrent: row.status || null,
      totalAllocatedTxn: roundAmount(row.total_allocated_txn),
      totalAllocatedBase: roundAmount(row.total_allocated_base),
      currencyCode: row.currency_code || null,
      postedJournalEntryId: parsePositiveInt(row.posted_journal_entry_id),
      reversalOfSettlementBatchId: parsePositiveInt(row.reversal_of_settlement_batch_id),
      reversalOfSettlementNo: row.reversal_of_settlement_no || null,
      reversalOfSettlementDate: toDateOnlyString(row.reversal_of_settlement_date),
      reversedBySettlementBatchId: parsePositiveInt(row.reversed_by_settlement_batch_id),
      reversedBySettlementNo: row.reversed_by_settlement_no || null,
      reversedBySettlementDate: toDateOnlyString(row.reversed_by_settlement_date),
      bankStatementLineId: parsePositiveInt(row.bank_statement_line_id),
      bankTransactionRef: row.bank_transaction_ref || null,
      bankAttachIdempotencyKey: row.bank_attach_idempotency_key || null,
      bankApplyIdempotencyKey: row.bank_apply_idempotency_key || null,
      createdAt: row.created_at || null,
      updatedAt: row.updated_at || null,
      postedAt: row.posted_at || null,
      reversedAt: row.reversed_at || null,
    };
  });
}

async function loadStatementAllocationRows({
  req,
  filters,
  buildScopeFilter,
  assertScopeAccess,
  runQuery = query,
}) {
  const params = [filters.tenantId, filters.asOfDate];
  const conditions = ["a.tenant_id = ?", "b.settlement_date <= ?"];

  appendCommonEntityFilters({
    req,
    filters,
    params,
    conditions,
    buildScopeFilter,
    assertScopeAccess,
    legalEntityColumn: "a.legal_entity_id",
    counterpartyColumn: "oi.counterparty_id",
    roleCustomerColumn: "cp.is_customer",
    roleVendorColumn: "cp.is_vendor",
    directionColumn: "d.direction",
  });

  const rowsResult = await runQuery(
    `SELECT
       a.id AS allocation_id,
       a.tenant_id,
       a.legal_entity_id,
       a.settlement_batch_id,
       a.open_item_id,
       a.allocation_date,
       a.allocation_amount_txn,
       a.allocation_amount_base,
       a.apply_idempotency_key,
       a.bank_statement_line_id,
       a.bank_apply_idempotency_key,
       a.note,
       b.settlement_no,
       b.settlement_date,
       b.status AS settlement_status,
       b.bank_statement_line_id AS settlement_bank_statement_line_id,
       b.bank_transaction_ref AS settlement_bank_transaction_ref,
       b.bank_apply_idempotency_key AS settlement_bank_apply_idempotency_key,
       b.reversal_of_settlement_batch_id,
       b_rev.id AS reversal_settlement_batch_id,
       b_rev.settlement_no AS reversal_settlement_no,
       b_rev.settlement_date AS reversal_settlement_date,
       oi.document_id,
       d.document_no,
       d.document_date,
       d.direction,
       d.document_type,
       d.counterparty_code_snapshot,
       d.counterparty_name_snapshot,
       cp.code AS counterparty_code_current,
       cp.name AS counterparty_name_current,
       cp.is_customer AS counterparty_is_customer,
       cp.is_vendor AS counterparty_is_vendor,
       le.code AS legal_entity_code,
       le.name AS legal_entity_name
     FROM cari_settlement_allocations a
     JOIN cari_settlement_batches b
       ON b.tenant_id = a.tenant_id
      AND b.legal_entity_id = a.legal_entity_id
      AND b.id = a.settlement_batch_id
     LEFT JOIN cari_settlement_batches b_rev
       ON b_rev.tenant_id = b.tenant_id
      AND b_rev.legal_entity_id = b.legal_entity_id
      AND b_rev.reversal_of_settlement_batch_id = b.id
     JOIN cari_open_items oi
       ON oi.tenant_id = a.tenant_id
      AND oi.legal_entity_id = a.legal_entity_id
      AND oi.id = a.open_item_id
     JOIN cari_documents d
       ON d.tenant_id = oi.tenant_id
      AND d.legal_entity_id = oi.legal_entity_id
      AND d.id = oi.document_id
     LEFT JOIN counterparties cp
       ON cp.tenant_id = oi.tenant_id
      AND cp.legal_entity_id = oi.legal_entity_id
      AND cp.id = oi.counterparty_id
     LEFT JOIN legal_entities le
       ON le.tenant_id = oi.tenant_id
      AND le.id = oi.legal_entity_id
     WHERE ${conditions.join(" AND ")}
     ORDER BY a.allocation_date ASC, a.id ASC`,
    params
  );

  return (rowsResult.rows || []).map((row) => {
    const reversalSettlementDate = toDateOnlyString(row.reversal_settlement_date);
    const activeAsOf = !reversalSettlementDate || reversalSettlementDate > filters.asOfDate;
    const isCustomer = parseDbBoolean(row.counterparty_is_customer);
    const isVendor = parseDbBoolean(row.counterparty_is_vendor);

    return {
      allocationId: parsePositiveInt(row.allocation_id),
      tenantId: parsePositiveInt(row.tenant_id),
      legalEntityId: parsePositiveInt(row.legal_entity_id),
      legalEntityCode: row.legal_entity_code || null,
      legalEntityName: row.legal_entity_name || null,
      settlementBatchId: parsePositiveInt(row.settlement_batch_id),
      settlementNo: row.settlement_no || null,
      settlementDate: toDateOnlyString(row.settlement_date),
      settlementStatusCurrent: row.settlement_status || null,
      activeAsOf,
      reversalSettlementBatchId: parsePositiveInt(row.reversal_settlement_batch_id),
      reversalSettlementNo: row.reversal_settlement_no || null,
      reversalSettlementDate,
      openItemId: parsePositiveInt(row.open_item_id),
      documentId: parsePositiveInt(row.document_id),
      documentNo: row.document_no || null,
      documentDate: toDateOnlyString(row.document_date),
      direction: row.direction || null,
      documentType: row.document_type || null,
      counterpartyCodeSnapshot: row.counterparty_code_snapshot || null,
      counterpartyNameSnapshot: row.counterparty_name_snapshot || null,
      counterpartyCodeCurrent: row.counterparty_code_current || null,
      counterpartyNameCurrent: row.counterparty_name_current || null,
      counterpartyType: deriveCounterpartyType({ isCustomer, isVendor }),
      isCustomer,
      isVendor,
      allocationDate: toDateOnlyString(row.allocation_date),
      allocationAmountTxn: roundAmount(row.allocation_amount_txn),
      allocationAmountBase: roundAmount(row.allocation_amount_base),
      applyIdempotencyKey: row.apply_idempotency_key || null,
      bankStatementLineId: parsePositiveInt(row.bank_statement_line_id),
      bankApplyIdempotencyKey: row.bank_apply_idempotency_key || null,
      note: row.note || null,
      settlementBankStatementLineId: parsePositiveInt(row.settlement_bank_statement_line_id),
      settlementBankTransactionRef: row.settlement_bank_transaction_ref || null,
      settlementBankApplyIdempotencyKey: row.settlement_bank_apply_idempotency_key || null,
      reversalOfSettlementBatchId: parsePositiveInt(row.reversal_of_settlement_batch_id),
    };
  });
}

async function loadUnappliedConsumptionMapAsOf({
  req,
  filters,
  buildScopeFilter,
  assertScopeAccess,
  runQuery = query,
}) {
  const params = [filters.tenantId, filters.asOfDate, filters.asOfDate];
  const conditions = [
    "al.tenant_id = ?",
    "al.action = 'cari.settlement.apply'",
    "al.resource_type = 'cari_settlement_batch'",
    "b.settlement_date <= ?",
    "(b_rev.id IS NULL OR b_rev.settlement_date > ?)",
  ];

  appendCommonEntityFilters({
    req,
    filters,
    params,
    conditions,
    buildScopeFilter,
    assertScopeAccess,
    legalEntityColumn: "b.legal_entity_id",
    counterpartyColumn: "b.counterparty_id",
    roleCustomerColumn: "cp.is_customer",
    roleVendorColumn: "cp.is_vendor",
    directionColumn: null,
  });

  const rowsResult = await runQuery(
    `SELECT
       al.payload_json,
       b.id AS settlement_batch_id,
       b.settlement_no,
       b.settlement_date,
       b.bank_statement_line_id,
       b.bank_transaction_ref,
       b.bank_apply_idempotency_key
     FROM audit_logs al
     JOIN cari_settlement_batches b
       ON b.tenant_id = al.tenant_id
      AND b.id = CAST(al.resource_id AS UNSIGNED)
     LEFT JOIN cari_settlement_batches b_rev
       ON b_rev.tenant_id = b.tenant_id
      AND b_rev.legal_entity_id = b.legal_entity_id
      AND b_rev.reversal_of_settlement_batch_id = b.id
     LEFT JOIN counterparties cp
       ON cp.tenant_id = b.tenant_id
      AND cp.legal_entity_id = b.legal_entity_id
      AND cp.id = b.counterparty_id
     WHERE ${conditions.join(" AND ")}
     ORDER BY b.settlement_date ASC, b.id ASC`,
    params
  );

  const consumptionByUnappliedId = new Map();

  for (const row of rowsResult.rows || []) {
    const payload = parseJsonPayload(row.payload_json);
    const unappliedConsumed = Array.isArray(payload?.unappliedConsumed)
      ? payload.unappliedConsumed
      : [];

    if (unappliedConsumed.length === 0) {
      continue;
    }

    const settlementBatchId = parsePositiveInt(row.settlement_batch_id);
    const settlementNo = row.settlement_no || null;
    const settlementDate = toDateOnlyString(row.settlement_date);

    for (const entry of unappliedConsumed) {
      const unappliedCashId = parsePositiveInt(entry?.unappliedCashId);
      if (!unappliedCashId) {
        continue;
      }

      const consumeTxn = roundAmount(entry?.consumeTxn);
      const consumeBase = roundAmount(entry?.consumeBase);
      if (consumeTxn <= AMOUNT_EPSILON && consumeBase <= AMOUNT_EPSILON) {
        continue;
      }

      if (!consumptionByUnappliedId.has(unappliedCashId)) {
        consumptionByUnappliedId.set(unappliedCashId, {
          consumeTxnTotal: 0,
          consumeBaseTotal: 0,
          events: [],
        });
      }

      const bucket = consumptionByUnappliedId.get(unappliedCashId);
      bucket.consumeTxnTotal = roundAmount(bucket.consumeTxnTotal + consumeTxn);
      bucket.consumeBaseTotal = roundAmount(bucket.consumeBaseTotal + consumeBase);
      bucket.events.push({
        settlementBatchId,
        settlementNo,
        settlementDate,
        consumeTxn,
        consumeBase,
        bankStatementLineId: parsePositiveInt(row.bank_statement_line_id),
        bankTransactionRef: row.bank_transaction_ref || null,
        bankApplyIdempotencyKey: row.bank_apply_idempotency_key || null,
      });
    }
  }

  return consumptionByUnappliedId;
}

async function loadUnappliedAsOfRows({
  req,
  filters,
  buildScopeFilter,
  assertScopeAccess,
  runQuery = query,
}) {
  const consumedMap = await loadUnappliedConsumptionMapAsOf({
    req,
    filters,
    buildScopeFilter,
    assertScopeAccess,
    runQuery,
  });

  const params = [filters.tenantId, filters.asOfDate];
  const conditions = ["u.tenant_id = ?", "u.receipt_date <= ?"];

  appendCommonEntityFilters({
    req,
    filters,
    params,
    conditions,
    buildScopeFilter,
    assertScopeAccess,
    legalEntityColumn: "u.legal_entity_id",
    counterpartyColumn: "u.counterparty_id",
    roleCustomerColumn: "cp.is_customer",
    roleVendorColumn: "cp.is_vendor",
    directionColumn: null,
  });

  const rowsResult = await runQuery(
    `SELECT
       u.id,
       u.tenant_id,
       u.legal_entity_id,
       u.counterparty_id,
       u.cash_receipt_no,
       u.receipt_date,
       u.status,
       u.amount_txn,
       u.amount_base,
       u.residual_amount_txn,
       u.residual_amount_base,
       u.currency_code,
       u.posted_journal_entry_id,
       u.settlement_batch_id,
       u.reversal_of_unapplied_cash_id,
       u.bank_statement_line_id,
       u.bank_transaction_ref,
       u.bank_attach_idempotency_key,
       u.bank_apply_idempotency_key,
       u.note,
       u.created_at,
       u.updated_at,
       cp.code AS counterparty_code_current,
       cp.name AS counterparty_name_current,
       cp.is_customer AS counterparty_is_customer,
       cp.is_vendor AS counterparty_is_vendor,
       cp.status AS counterparty_status,
       le.code AS legal_entity_code,
       le.name AS legal_entity_name,
       creator.id AS creator_settlement_batch_id,
       creator.settlement_no AS creator_settlement_no,
       creator.settlement_date AS creator_settlement_date,
       creator_reversal.id AS creator_reversal_settlement_batch_id,
       creator_reversal.settlement_no AS creator_reversal_settlement_no,
       creator_reversal.settlement_date AS creator_reversal_settlement_date
     FROM cari_unapplied_cash u
     LEFT JOIN counterparties cp
       ON cp.tenant_id = u.tenant_id
      AND cp.legal_entity_id = u.legal_entity_id
      AND cp.id = u.counterparty_id
     LEFT JOIN legal_entities le
       ON le.tenant_id = u.tenant_id
      AND le.id = u.legal_entity_id
     LEFT JOIN cari_settlement_batches creator
       ON creator.tenant_id = u.tenant_id
      AND creator.legal_entity_id = u.legal_entity_id
      AND creator.id = u.settlement_batch_id
     LEFT JOIN cari_settlement_batches creator_reversal
       ON creator_reversal.tenant_id = creator.tenant_id
      AND creator_reversal.legal_entity_id = creator.legal_entity_id
      AND creator_reversal.reversal_of_settlement_batch_id = creator.id
     WHERE ${conditions.join(" AND ")}
     ORDER BY u.receipt_date ASC, u.id ASC`,
    params
  );

  return (rowsResult.rows || []).map((row) => {
    const unappliedCashId = parsePositiveInt(row.id);
    const consumeBucket = consumedMap.get(unappliedCashId) || {
      consumeTxnTotal: 0,
      consumeBaseTotal: 0,
      events: [],
    };

    const creatorReversalDate = toDateOnlyString(row.creator_reversal_settlement_date);
    const creatorReversedByAsOf =
      Boolean(creatorReversalDate) && creatorReversalDate <= filters.asOfDate;

    const amountTxn = roundAmount(row.amount_txn);
    const amountBase = roundAmount(row.amount_base);
    const consumedTxnAsOf = creatorReversedByAsOf
      ? 0
      : roundAmount(consumeBucket.consumeTxnTotal);
    const consumedBaseAsOf = creatorReversedByAsOf
      ? 0
      : roundAmount(consumeBucket.consumeBaseTotal);

    const originalAmountTxnAsOf = creatorReversedByAsOf ? 0 : amountTxn;
    const originalAmountBaseAsOf = creatorReversedByAsOf ? 0 : amountBase;

    let residualAmountTxnAsOf = roundAmount(originalAmountTxnAsOf - consumedTxnAsOf);
    let residualAmountBaseAsOf = roundAmount(originalAmountBaseAsOf - consumedBaseAsOf);
    if (residualAmountTxnAsOf < 0 && Math.abs(residualAmountTxnAsOf) <= AMOUNT_EPSILON) {
      residualAmountTxnAsOf = 0;
    }
    if (residualAmountBaseAsOf < 0 && Math.abs(residualAmountBaseAsOf) <= AMOUNT_EPSILON) {
      residualAmountBaseAsOf = 0;
    }
    residualAmountTxnAsOf = Math.max(0, residualAmountTxnAsOf);
    residualAmountBaseAsOf = Math.max(0, residualAmountBaseAsOf);

    let asOfStatus = "PARTIALLY_APPLIED";
    if (creatorReversedByAsOf) {
      asOfStatus = "REVERSED";
    } else if (residualAmountTxnAsOf <= AMOUNT_EPSILON) {
      asOfStatus = "FULLY_APPLIED";
    } else if (amountsAreEqual(residualAmountTxnAsOf, originalAmountTxnAsOf)) {
      asOfStatus = "UNAPPLIED";
    }
    const isCustomer = parseDbBoolean(row.counterparty_is_customer);
    const isVendor = parseDbBoolean(row.counterparty_is_vendor);

    return {
      unappliedCashId,
      tenantId: parsePositiveInt(row.tenant_id),
      legalEntityId: parsePositiveInt(row.legal_entity_id),
      legalEntityCode: row.legal_entity_code || null,
      legalEntityName: row.legal_entity_name || null,
      counterpartyId: parsePositiveInt(row.counterparty_id),
      counterpartyCodeCurrent: row.counterparty_code_current || null,
      counterpartyNameCurrent: row.counterparty_name_current || null,
      counterpartyType: deriveCounterpartyType({ isCustomer, isVendor }),
      isCustomer,
      isVendor,
      counterpartyStatus: row.counterparty_status || null,
      cashReceiptNo: row.cash_receipt_no || null,
      receiptDate: toDateOnlyString(row.receipt_date),
      statusCurrent: row.status || null,
      asOfStatus,
      currencyCode: row.currency_code || null,
      amountTxn,
      amountBase,
      residualAmountTxnCurrent: roundAmount(row.residual_amount_txn),
      residualAmountBaseCurrent: roundAmount(row.residual_amount_base),
      residualAmountTxnAsOf,
      residualAmountBaseAsOf,
      consumedAmountTxnAsOf: consumedTxnAsOf,
      consumedAmountBaseAsOf: consumedBaseAsOf,
      postedJournalEntryId: parsePositiveInt(row.posted_journal_entry_id),
      settlementBatchId: parsePositiveInt(row.settlement_batch_id),
      reversalOfUnappliedCashId: parsePositiveInt(row.reversal_of_unapplied_cash_id),
      bankStatementLineId: parsePositiveInt(row.bank_statement_line_id),
      bankTransactionRef: row.bank_transaction_ref || null,
      bankAttachIdempotencyKey: row.bank_attach_idempotency_key || null,
      bankApplyIdempotencyKey: row.bank_apply_idempotency_key || null,
      note: row.note || null,
      creatorSettlementBatchId: parsePositiveInt(row.creator_settlement_batch_id),
      creatorSettlementNo: row.creator_settlement_no || null,
      creatorSettlementDate: toDateOnlyString(row.creator_settlement_date),
      creatorReversalSettlementBatchId: parsePositiveInt(row.creator_reversal_settlement_batch_id),
      creatorReversalSettlementNo: row.creator_reversal_settlement_no || null,
      creatorReversalSettlementDate: creatorReversalDate,
      consumedBySettlementsAsOf: consumeBucket.events,
      createdAt: row.created_at || null,
      updatedAt: row.updated_at || null,
    };
  });
}

function buildDocumentBalanceMap(openItemRows) {
  const map = new Map();

  for (const row of openItemRows) {
    const documentId = parsePositiveInt(row.documentId);
    if (!documentId) {
      continue;
    }

    if (!map.has(documentId)) {
      map.set(documentId, {
        originalAmountTxn: 0,
        originalAmountBase: 0,
        residualAmountTxnAsOf: 0,
        residualAmountBaseAsOf: 0,
        settledAmountTxnAsOf: 0,
        settledAmountBaseAsOf: 0,
      });
    }

    const bucket = map.get(documentId);
    bucket.originalAmountTxn = roundAmount(bucket.originalAmountTxn + toNumber(row.originalAmountTxn));
    bucket.originalAmountBase = roundAmount(bucket.originalAmountBase + toNumber(row.originalAmountBase));
    bucket.residualAmountTxnAsOf = roundAmount(
      bucket.residualAmountTxnAsOf + toNumber(row.residualAmountTxnAsOf)
    );
    bucket.residualAmountBaseAsOf = roundAmount(
      bucket.residualAmountBaseAsOf + toNumber(row.residualAmountBaseAsOf)
    );
    bucket.settledAmountTxnAsOf = roundAmount(
      bucket.settledAmountTxnAsOf + toNumber(row.settledAmountTxnAsOf)
    );
    bucket.settledAmountBaseAsOf = roundAmount(
      bucket.settledAmountBaseAsOf + toNumber(row.settledAmountBaseAsOf)
    );
  }

  return map;
}

function buildAllocationLinksByDocumentId(allocationRows) {
  const map = new Map();

  for (const row of allocationRows) {
    const documentId = parsePositiveInt(row.documentId);
    if (!documentId) {
      continue;
    }
    if (!map.has(documentId)) {
      map.set(documentId, []);
    }

    const bucket = map.get(documentId);
    const exists = bucket.some(
      (entry) =>
        entry.settlementBatchId === row.settlementBatchId &&
        entry.allocationId === row.allocationId
    );
    if (!exists) {
      bucket.push({
        allocationId: row.allocationId,
        settlementBatchId: row.settlementBatchId,
        settlementNo: row.settlementNo,
        settlementDate: row.settlementDate,
        allocationAmountTxn: row.allocationAmountTxn,
        allocationAmountBase: row.allocationAmountBase,
        activeAsOf: row.activeAsOf,
        reversalSettlementBatchId: row.reversalSettlementBatchId,
        reversalSettlementNo: row.reversalSettlementNo,
        reversalSettlementDate: row.reversalSettlementDate,
      });
    }
  }

  return map;
}

function resolveDocumentAsOfStatus(documentRow, documentBalanceMap, asOfDate) {
  if (parsePositiveInt(documentRow.reversalOfDocumentId)) {
    return "REVERSED";
  }
  const reversedByDate = toDateOnlyString(documentRow.reversedByDocumentDate);
  if (reversedByDate && reversedByDate <= asOfDate) {
    return "REVERSED";
  }

  const balance = documentBalanceMap.get(parsePositiveInt(documentRow.documentId));
  if (!balance) {
    return "SETTLED";
  }

  if (balance.residualAmountTxnAsOf <= AMOUNT_EPSILON) {
    return "SETTLED";
  }
  if (balance.originalAmountTxn - balance.residualAmountTxnAsOf <= AMOUNT_EPSILON) {
    return "POSTED";
  }
  return "PARTIALLY_SETTLED";
}

function summarizeStatementDocuments(rows) {
  let postedCount = 0;
  let partialCount = 0;
  let settledCount = 0;
  let reversedCount = 0;

  for (const row of rows) {
    const status = normalizeUpperText(row.asOfStatus);
    if (status === "POSTED") {
      postedCount += 1;
    } else if (status === "PARTIALLY_SETTLED") {
      partialCount += 1;
    } else if (status === "SETTLED") {
      settledCount += 1;
    } else if (status === "REVERSED") {
      reversedCount += 1;
    }
  }

  return {
    count: rows.length,
    postedCount,
    partiallySettledCount: partialCount,
    settledCount,
    reversedCount,
  };
}

function summarizeStatementSettlements(rows, asOfDate) {
  let postedCount = 0;
  let reversedCount = 0;
  let reversalRowsCount = 0;
  let activeAsOfCount = 0;

  for (const row of rows) {
    const isReversalRow = Boolean(parsePositiveInt(row.reversalOfSettlementBatchId));
    if (isReversalRow) {
      reversalRowsCount += 1;
      continue;
    }

    const reversalDate = toDateOnlyString(row.reversedBySettlementDate);
    const reversedByAsOf = reversalDate && reversalDate <= asOfDate;
    if (reversedByAsOf) {
      reversedCount += 1;
    } else {
      postedCount += 1;
      activeAsOfCount += 1;
    }
  }

  return {
    count: rows.length,
    postedCount,
    reversedCount,
    reversalRowsCount,
    activeAsOfCount,
  };
}

function summarizeStatementAllocations(rows) {
  let count = 0;
  let activeCount = 0;
  let reversedCount = 0;
  let allocationAmountTxnTotal = 0;
  let allocationAmountBaseTotal = 0;
  let activeAllocationAmountTxnTotal = 0;
  let activeAllocationAmountBaseTotal = 0;

  for (const row of rows) {
    count += 1;
    allocationAmountTxnTotal = roundAmount(
      allocationAmountTxnTotal + toNumber(row.allocationAmountTxn)
    );
    allocationAmountBaseTotal = roundAmount(
      allocationAmountBaseTotal + toNumber(row.allocationAmountBase)
    );

    if (row.activeAsOf) {
      activeCount += 1;
      activeAllocationAmountTxnTotal = roundAmount(
        activeAllocationAmountTxnTotal + toNumber(row.allocationAmountTxn)
      );
      activeAllocationAmountBaseTotal = roundAmount(
        activeAllocationAmountBaseTotal + toNumber(row.allocationAmountBase)
      );
    } else {
      reversedCount += 1;
    }
  }

  return {
    count,
    activeCount,
    reversedCount,
    allocationAmountTxnTotal,
    allocationAmountBaseTotal,
    activeAllocationAmountTxnTotal,
    activeAllocationAmountBaseTotal,
  };
}

export async function getCariAgingReport({
  req,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const openItemRows = await loadOpenItemAsOfRows({
    req,
    filters,
    buildScopeFilter,
    assertScopeAccess,
  });

  const filteredRows = openItemRows.filter((row) =>
    matchesOpenStatusFilter(row.asOfStatus, filters.status)
  );

  const pagination = paginateRows(filteredRows, filters.limit, filters.offset);
  const rowsForResponse = filters.includeDetails ? pagination.rows : [];

  const bucketSummary = buildAgingBucketSummary(filteredRows);
  const counterpartySummary = buildCounterpartySummary(filteredRows);
  const summary = summarizeOpenItemRows(filteredRows);

  const unappliedRows = await loadUnappliedAsOfRows({
    req,
    filters,
    buildScopeFilter,
    assertScopeAccess,
  });

  return {
    asOfDate: filters.asOfDate,
    direction: filters.direction,
    legalEntityId: filters.legalEntityId || null,
    counterpartyId: filters.counterpartyId || null,
    role: filters.role || null,
    statusFilter: filters.status || "ALL",
    total: pagination.total,
    limit: pagination.limit,
    offset: pagination.offset,
    summary,
    buckets: bucketSummary,
    counterparties: counterpartySummary,
    rows: rowsForResponse,
    unapplied: {
      summary: summarizeUnappliedRows(unappliedRows),
      rows: filters.includeDetails ? unappliedRows : [],
    },
  };
}

export async function getCariOpenItemsReport({
  req,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const openItemRows = await loadOpenItemAsOfRows({
    req,
    filters,
    buildScopeFilter,
    assertScopeAccess,
  });

  const filteredRows = openItemRows.filter((row) =>
    matchesOpenStatusFilter(row.asOfStatus, filters.status)
  );

  const pagination = paginateRows(filteredRows, filters.limit, filters.offset);
  const pagedRows = pagination.rows;

  const settlementReferenceMap = await loadSettlementReferenceMapForOpenItems({
    tenantId: filters.tenantId,
    asOfDate: filters.asOfDate,
    openItemIds: pagedRows.map((row) => row.openItemId),
  });

  const rows = pagedRows.map((row) => ({
    ...row,
    settlementReferences: settlementReferenceMap.get(row.openItemId) || [],
  }));

  const unappliedRows = await loadUnappliedAsOfRows({
    req,
    filters,
    buildScopeFilter,
    assertScopeAccess,
  });

  return {
    asOfDate: filters.asOfDate,
    direction: filters.direction || null,
    legalEntityId: filters.legalEntityId || null,
    counterpartyId: filters.counterpartyId || null,
    role: filters.role || null,
    statusFilter: filters.status || "ALL",
    total: pagination.total,
    limit: pagination.limit,
    offset: pagination.offset,
    summary: summarizeOpenItemRows(filteredRows),
    rows,
    unapplied: {
      summary: summarizeUnappliedRows(unappliedRows),
      rows: filters.includeDetails ? unappliedRows : [],
    },
  };
}

export async function getCariCounterpartyStatementReport({
  req,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const [openItemRows, documentRowsRaw, settlementRowsRaw, allocationRowsRaw, unappliedRows] =
    await Promise.all([
      loadOpenItemAsOfRows({
        req,
        filters,
        buildScopeFilter,
        assertScopeAccess,
      }),
      loadStatementDocumentRows({
        req,
        filters,
        buildScopeFilter,
        assertScopeAccess,
      }),
      loadStatementSettlementRows({
        req,
        filters,
        buildScopeFilter,
        assertScopeAccess,
      }),
      loadStatementAllocationRows({
        req,
        filters,
        buildScopeFilter,
        assertScopeAccess,
      }),
      loadUnappliedAsOfRows({
        req,
        filters,
        buildScopeFilter,
        assertScopeAccess,
      }),
    ]);

  const documentBalanceMap = buildDocumentBalanceMap(openItemRows);
  const allocationLinksByDocumentId = buildAllocationLinksByDocumentId(allocationRowsRaw);

  const documentsWithAsOf = documentRowsRaw.map((row) => {
    const documentBalance = documentBalanceMap.get(parsePositiveInt(row.documentId)) || {
      originalAmountTxn: 0,
      originalAmountBase: 0,
      residualAmountTxnAsOf: 0,
      residualAmountBaseAsOf: 0,
      settledAmountTxnAsOf: 0,
      settledAmountBaseAsOf: 0,
    };
    const asOfStatus = resolveDocumentAsOfStatus(row, documentBalanceMap, filters.asOfDate);

    return {
      ...row,
      asOfStatus,
      asOfOpenAmountTxn: roundAmount(documentBalance.residualAmountTxnAsOf),
      asOfOpenAmountBase: roundAmount(documentBalance.residualAmountBaseAsOf),
      asOfSettledAmountTxn: roundAmount(documentBalance.settledAmountTxnAsOf),
      asOfSettledAmountBase: roundAmount(documentBalance.settledAmountBaseAsOf),
      settlementLinks: allocationLinksByDocumentId.get(parsePositiveInt(row.documentId)) || [],
    };
  });

  const documents = documentsWithAsOf.filter((row) =>
    matchesStatementStatusFilter(row.asOfStatus, filters.status)
  );

  let allocationRows = allocationRowsRaw;
  let settlementRows = settlementRowsRaw;
  if (filters.direction) {
    const settlementIdsFromDirection = new Set(
      allocationRows
        .filter((row) => normalizeUpperText(row.direction) === normalizeUpperText(filters.direction))
        .map((row) => parsePositiveInt(row.settlementBatchId))
        .filter(Boolean)
    );

    allocationRows = allocationRows.filter(
      (row) => normalizeUpperText(row.direction) === normalizeUpperText(filters.direction)
    );

    settlementRows = settlementRows.filter((row) => {
      const settlementBatchId = parsePositiveInt(row.settlementBatchId);
      if (!settlementBatchId) {
        return false;
      }

      if (settlementIdsFromDirection.has(settlementBatchId)) {
        return true;
      }

      const originalId = parsePositiveInt(row.reversalOfSettlementBatchId);
      return Boolean(originalId && settlementIdsFromDirection.has(originalId));
    });
  }

  const documentPagination = paginateRows(documents, filters.limit, filters.offset);
  const allocationPagination = paginateRows(allocationRows, filters.limit, filters.offset);
  const settlementPagination = paginateRows(settlementRows, filters.limit, filters.offset);

  const openSummary = summarizeOpenItemRows(
    openItemRows.filter((row) => matchesOpenStatusFilter(row.asOfStatus, "OPEN"))
  );
  const documentSummary = summarizeStatementDocuments(documents);
  const settlementSummary = summarizeStatementSettlements(settlementRows, filters.asOfDate);
  const allocationSummary = summarizeStatementAllocations(allocationRows);
  const unappliedSummary = summarizeUnappliedRows(unappliedRows);

  return {
    asOfDate: filters.asOfDate,
    direction: filters.direction || null,
    legalEntityId: filters.legalEntityId || null,
    counterpartyId: filters.counterpartyId || null,
    role: filters.role || null,
    statusFilter: filters.status || "ALL",
    summary: {
      documents: documentSummary,
      settlements: settlementSummary,
      allocations: allocationSummary,
      openItems: openSummary,
      unapplied: unappliedSummary,
      reconcile: {
        openResidualAmountTxnFromOpenItems: openSummary.residualAmountTxnTotal,
        openResidualAmountBaseFromOpenItems: openSummary.residualAmountBaseTotal,
        openResidualAmountTxnFromDocuments: roundAmount(
          documents.reduce((sum, row) => sum + toNumber(row.asOfOpenAmountTxn), 0)
        ),
        openResidualAmountBaseFromDocuments: roundAmount(
          documents.reduce((sum, row) => sum + toNumber(row.asOfOpenAmountBase), 0)
        ),
      },
    },
    documents: {
      total: documentPagination.total,
      limit: documentPagination.limit,
      offset: documentPagination.offset,
      rows: filters.includeDetails ? documentPagination.rows : [],
    },
    settlements: {
      total: settlementPagination.total,
      limit: settlementPagination.limit,
      offset: settlementPagination.offset,
      rows: filters.includeDetails ? settlementPagination.rows : [],
    },
    allocations: {
      total: allocationPagination.total,
      limit: allocationPagination.limit,
      offset: allocationPagination.offset,
      rows: filters.includeDetails ? allocationPagination.rows : [],
    },
    unapplied: {
      total: unappliedRows.length,
      limit: documentPagination.limit,
      offset: documentPagination.offset,
      summary: unappliedSummary,
      rows: filters.includeDetails ? unappliedRows : [],
    },
  };
}
