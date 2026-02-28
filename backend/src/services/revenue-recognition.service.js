import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";

const RUN_STATUS = Object.freeze({
  DRAFT: "DRAFT",
  READY: "READY",
  POSTED: "POSTED",
  REVERSED: "REVERSED",
});

const ACCOUNT_FAMILY = Object.freeze({
  DEFREV: "DEFREV",
  PREPAID_EXPENSE: "PREPAID_EXPENSE",
  ACCRUED_REVENUE: "ACCRUED_REVENUE",
  ACCRUED_EXPENSE: "ACCRUED_EXPENSE",
});

const PR17B_ACTIVE_FAMILIES = new Set([
  ACCOUNT_FAMILY.DEFREV,
  ACCOUNT_FAMILY.PREPAID_EXPENSE,
]);
const PR17C_ACCRUAL_FAMILIES = new Set([
  ACCOUNT_FAMILY.ACCRUED_REVENUE,
  ACCOUNT_FAMILY.ACCRUED_EXPENSE,
]);
const PR17_POSTABLE_FAMILIES = new Set([
  ACCOUNT_FAMILY.DEFREV,
  ACCOUNT_FAMILY.PREPAID_EXPENSE,
  ACCOUNT_FAMILY.ACCRUED_REVENUE,
  ACCOUNT_FAMILY.ACCRUED_EXPENSE,
]);

const REVREC_PURPOSE_CODES = Object.freeze({
  DEFREV_SHORT_LIABILITY: "DEFREV_SHORT_LIABILITY",
  DEFREV_LONG_LIABILITY: "DEFREV_LONG_LIABILITY",
  DEFREV_REVENUE: "DEFREV_REVENUE",
  DEFREV_RECLASS: "DEFREV_RECLASS",
  PREPAID_EXP_SHORT_ASSET: "PREPAID_EXP_SHORT_ASSET",
  PREPAID_EXP_LONG_ASSET: "PREPAID_EXP_LONG_ASSET",
  PREPAID_EXPENSE: "PREPAID_EXPENSE",
  PREPAID_RECLASS: "PREPAID_RECLASS",
  ACCR_REV_SHORT_ASSET: "ACCR_REV_SHORT_ASSET",
  ACCR_REV_LONG_ASSET: "ACCR_REV_LONG_ASSET",
  ACCR_REV_REVENUE: "ACCR_REV_REVENUE",
  ACCR_REV_RECLASS: "ACCR_REV_RECLASS",
  ACCR_EXP_SHORT_LIABILITY: "ACCR_EXP_SHORT_LIABILITY",
  ACCR_EXP_LONG_LIABILITY: "ACCR_EXP_LONG_LIABILITY",
  ACCR_EXP_EXPENSE: "ACCR_EXP_EXPENSE",
  ACCR_EXP_RECLASS: "ACCR_EXP_RECLASS",
});

const PURPOSE_CODES_BY_FAMILY = Object.freeze({
  [ACCOUNT_FAMILY.DEFREV]: [
    REVREC_PURPOSE_CODES.DEFREV_SHORT_LIABILITY,
    REVREC_PURPOSE_CODES.DEFREV_LONG_LIABILITY,
    REVREC_PURPOSE_CODES.DEFREV_REVENUE,
    REVREC_PURPOSE_CODES.DEFREV_RECLASS,
  ],
  [ACCOUNT_FAMILY.PREPAID_EXPENSE]: [
    REVREC_PURPOSE_CODES.PREPAID_EXP_SHORT_ASSET,
    REVREC_PURPOSE_CODES.PREPAID_EXP_LONG_ASSET,
    REVREC_PURPOSE_CODES.PREPAID_EXPENSE,
    REVREC_PURPOSE_CODES.PREPAID_RECLASS,
  ],
  [ACCOUNT_FAMILY.ACCRUED_REVENUE]: [
    REVREC_PURPOSE_CODES.ACCR_REV_SHORT_ASSET,
    REVREC_PURPOSE_CODES.ACCR_REV_LONG_ASSET,
    REVREC_PURPOSE_CODES.ACCR_REV_REVENUE,
    REVREC_PURPOSE_CODES.ACCR_REV_RECLASS,
  ],
  [ACCOUNT_FAMILY.ACCRUED_EXPENSE]: [
    REVREC_PURPOSE_CODES.ACCR_EXP_SHORT_LIABILITY,
    REVREC_PURPOSE_CODES.ACCR_EXP_LONG_LIABILITY,
    REVREC_PURPOSE_CODES.ACCR_EXP_EXPENSE,
    REVREC_PURPOSE_CODES.ACCR_EXP_RECLASS,
  ],
});

const JOURNAL_BALANCE_EPSILON = 0.000001;
const REPORT_RECONCILE_EPSILON = 0.000001;

function toDecimalNumber(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toDateOnlyString(value, label = "date") {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw badRequest(`${label} must be a valid date`);
    }
    // Preserve DATE semantics from DB drivers that hydrate date-only columns to local midnight.
    const year = value.getFullYear();
    const month = String(value.getMonth() + 1).padStart(2, "0");
    const day = String(value.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }
  const raw = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}(?:\b|T)/.test(raw)) {
    return raw.slice(0, 10);
  }
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    throw badRequest(`${label} must be a valid date`);
  }
  const year = parsed.getUTCFullYear();
  const month = String(parsed.getUTCMonth() + 1).padStart(2, "0");
  const day = String(parsed.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function toBoolean(value) {
  if (value === true || value === false) {
    return value;
  }
  return Number(value) === 1;
}

function asUpper(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function normalizeAmount(value, label, { allowZero = true } = {}) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw badRequest(`${label} must be a numeric value`);
  }
  if (!allowZero && parsed <= 0) {
    throw badRequest(`${label} must be > 0`);
  }
  if (allowZero && parsed < 0) {
    throw badRequest(`${label} must be >= 0`);
  }
  return Number(parsed.toFixed(6));
}

function ensureBalancedJournalLines(lines, errorLabel = "Revenue-recognition journal") {
  let debitTotal = 0;
  let creditTotal = 0;
  for (const line of lines || []) {
    debitTotal += Number(line.debitBase || 0);
    creditTotal += Number(line.creditBase || 0);
  }
  if (Math.abs(debitTotal - creditTotal) > JOURNAL_BALANCE_EPSILON) {
    throw badRequest(`${errorLabel} is not balanced`);
  }
  return {
    totalDebit: Number(debitTotal.toFixed(6)),
    totalCredit: Number(creditTotal.toFixed(6)),
  };
}

function isDuplicateKeyError(err, constraintName = null) {
  const duplicate =
    Number(err?.errno) === 1062 ||
    String(err?.code || "").toUpperCase() === "ER_DUP_ENTRY";
  if (!duplicate) {
    return false;
  }
  if (!constraintName) {
    return true;
  }
  return String(err?.message || "").includes(constraintName);
}

function buildDeterministicUid(prefix, parts = []) {
  const serialized = parts
    .map((part) => String(part ?? "").trim())
    .join(":");
  return `${prefix}:${serialized}`.slice(0, 160);
}

function toUnixTimeMs(dateOnly) {
  const normalized = toDateOnlyString(dateOnly, "date");
  if (!normalized) {
    return null;
  }
  const parsed = new Date(`${normalized}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) {
    throw badRequest("date must be valid");
  }
  return parsed.getTime();
}

function toMonthStartDateOnly(dateOnly) {
  const normalized = toDateOnlyString(dateOnly, "date");
  if (!normalized) {
    return null;
  }
  return `${normalized.slice(0, 7)}-01`;
}

function toMonthEndDateOnly(dateOnly) {
  const normalized = toDateOnlyString(dateOnly, "date");
  if (!normalized) {
    return null;
  }
  const parsed = new Date(`${normalized.slice(0, 7)}-01T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) {
    throw badRequest("date must be valid");
  }
  parsed.setUTCMonth(parsed.getUTCMonth() + 1, 0);
  return parsed.toISOString().slice(0, 10);
}

function addMonthsDateOnly(dateOnly, monthDelta = 0) {
  const normalized = toDateOnlyString(dateOnly, "date");
  if (!normalized) {
    return null;
  }
  const parsed = new Date(`${normalized}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) {
    throw badRequest("date must be valid");
  }
  parsed.setUTCMonth(parsed.getUTCMonth() + Number(monthDelta || 0));
  return parsed.toISOString().slice(0, 10);
}

function enumerateMonthEndDatesInRange(startDate, endDate) {
  const normalizedStart = toDateOnlyString(startDate, "recognitionStartDate");
  const normalizedEnd = toDateOnlyString(endDate, "recognitionEndDate");
  if (!normalizedStart || !normalizedEnd) {
    return [];
  }
  if (normalizedStart > normalizedEnd) {
    throw badRequest("recognitionStartDate cannot be greater than recognitionEndDate");
  }

  const dates = [];
  let cursor = toMonthStartDateOnly(normalizedStart);
  const endMonth = toMonthStartDateOnly(normalizedEnd);
  while (cursor && endMonth && cursor <= endMonth) {
    dates.push(toMonthEndDateOnly(cursor));
    cursor = addMonthsDateOnly(cursor, 1);
  }
  return dates;
}

function splitAmountAcrossBuckets(totalAmount, bucketCount, label) {
  const parsedTotal = Number(totalAmount);
  if (!Number.isFinite(parsedTotal)) {
    throw badRequest(`${label} must be numeric`);
  }
  const count = Number(bucketCount || 0);
  if (!Number.isInteger(count) || count <= 0) {
    throw badRequest("bucketCount must be a positive integer");
  }
  if (count === 1) {
    return [Number(parsedTotal.toFixed(6))];
  }

  const chunk = Number((parsedTotal / count).toFixed(6));
  const values = [];
  for (let index = 0; index < count - 1; index += 1) {
    values.push(chunk);
  }
  const remainder = Number((parsedTotal - chunk * (count - 1)).toFixed(6));
  values.push(remainder);
  return values;
}

function resolveMaturityBucketForDate({
  maturityDate,
  periodEndDate,
}) {
  const maturityMs = toUnixTimeMs(maturityDate);
  const periodEndMs = toUnixTimeMs(periodEndDate);
  if (maturityMs === null || periodEndMs === null) {
    return "SHORT_TERM";
  }
  const oneYearMs = 365 * 24 * 60 * 60 * 1000;
  return maturityMs - periodEndMs > oneYearMs ? "LONG_TERM" : "SHORT_TERM";
}

function resolveFxRateFromAmounts(amountTxn, amountBase) {
  const txn = Math.abs(Number(amountTxn || 0));
  const base = Math.abs(Number(amountBase || 0));
  if (txn <= JOURNAL_BALANCE_EPSILON || base <= JOURNAL_BALANCE_EPSILON) {
    return 1;
  }
  const ratio = base / txn;
  if (!Number.isFinite(ratio) || ratio <= 0) {
    return 1;
  }
  return Number(ratio.toFixed(10));
}

function buildContractRevrecScheduleSourceUid({
  contractId,
  contractLineId,
  fiscalPeriodId,
  generationMode,
  sourceCariDocumentId,
}) {
  const modeCode = asUpper(generationMode) === "BY_LINKED_DOCUMENT" ? "DOC" : "LINE";
  return buildDeterministicUid("CONREV_SCHED", [
    contractId,
    contractLineId,
    fiscalPeriodId,
    modeCode,
    sourceCariDocumentId || 0,
  ]);
}

function buildContractRevrecLineSourceUid({
  scheduleSourceUid,
  maturityDate,
}) {
  return buildDeterministicUid("CONREV_LINE", [scheduleSourceUid, maturityDate]);
}

function mapScheduleRow(row) {
  if (!row) {
    return null;
  }
  return {
    id: parsePositiveInt(row.id),
    tenantId: parsePositiveInt(row.tenant_id),
    legalEntityId: parsePositiveInt(row.legal_entity_id),
    fiscalPeriodId: parsePositiveInt(row.fiscal_period_id),
    sourceEventUid: row.source_event_uid,
    status: row.status,
    accountFamily: row.account_family,
    maturityBucket: row.maturity_bucket,
    maturityDate: toDateOnlyString(row.maturity_date, "maturityDate"),
    reclassRequired: toBoolean(row.reclass_required),
    currencyCode: row.currency_code,
    fxRate: toDecimalNumber(row.fx_rate),
    amountTxn: toDecimalNumber(row.amount_txn),
    amountBase: toDecimalNumber(row.amount_base),
    periodStartDate: toDateOnlyString(row.period_start_date, "periodStartDate"),
    periodEndDate: toDateOnlyString(row.period_end_date, "periodEndDate"),
    createdByUserId: parsePositiveInt(row.created_by_user_id),
    postedJournalEntryId: parsePositiveInt(row.posted_journal_entry_id),
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
    lineCount:
      row.line_count === undefined || row.line_count === null
        ? null
        : Number(row.line_count),
  };
}

function mapRunRow(row) {
  if (!row) {
    return null;
  }
  return {
    id: parsePositiveInt(row.id),
    tenantId: parsePositiveInt(row.tenant_id),
    legalEntityId: parsePositiveInt(row.legal_entity_id),
    scheduleId: parsePositiveInt(row.schedule_id),
    fiscalPeriodId: parsePositiveInt(row.fiscal_period_id),
    runNo: row.run_no,
    sourceRunUid: row.source_run_uid,
    status: row.status,
    accountFamily: row.account_family,
    maturityBucket: row.maturity_bucket,
    maturityDate: toDateOnlyString(row.maturity_date, "maturityDate"),
    reclassRequired: toBoolean(row.reclass_required),
    currencyCode: row.currency_code,
    fxRate: toDecimalNumber(row.fx_rate),
    totalAmountTxn: toDecimalNumber(row.total_amount_txn),
    totalAmountBase: toDecimalNumber(row.total_amount_base),
    periodStartDate: toDateOnlyString(row.period_start_date, "periodStartDate"),
    periodEndDate: toDateOnlyString(row.period_end_date, "periodEndDate"),
    reversalOfRunId: parsePositiveInt(row.reversal_of_run_id),
    postedJournalEntryId: parsePositiveInt(row.posted_journal_entry_id),
    reversalJournalEntryId: parsePositiveInt(row.reversal_journal_entry_id),
    createdByUserId: parsePositiveInt(row.created_by_user_id),
    postedByUserId: parsePositiveInt(row.posted_by_user_id),
    reversedByUserId: parsePositiveInt(row.reversed_by_user_id),
    postedAt: row.posted_at || null,
    reversedAt: row.reversed_at || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
    lineCount:
      row.line_count === undefined || row.line_count === null
        ? null
        : Number(row.line_count),
  };
}

async function assertLegalEntityExists({
  tenantId,
  legalEntityId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId]
  );
  if (!result.rows?.[0]) {
    throw badRequest("legalEntityId not found for tenant");
  }
}

async function assertCurrencyExists({
  currencyCode,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT code
     FROM currencies
     WHERE code = ?
     LIMIT 1`,
    [currencyCode]
  );
  if (!result.rows?.[0]) {
    throw badRequest("currencyCode not found");
  }
}

async function resolvePeriodScope({
  tenantId,
  legalEntityId,
  fiscalPeriodId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
        fp.id,
        fp.start_date,
        fp.end_date
     FROM fiscal_periods fp
     JOIN books b ON b.calendar_id = fp.calendar_id
     WHERE fp.id = ?
       AND b.tenant_id = ?
       AND b.legal_entity_id = ?
     ORDER BY b.id ASC
     LIMIT 1`,
    [fiscalPeriodId, tenantId, legalEntityId]
  );
  return result.rows?.[0] || null;
}

async function fetchScheduleRow({
  tenantId,
  scheduleId,
  runQuery = query,
  forUpdate = false,
}) {
  const lockSql = forUpdate ? " FOR UPDATE" : "";
  const result = await runQuery(
    `SELECT
        rrs.id,
        rrs.tenant_id,
        rrs.legal_entity_id,
        rrs.fiscal_period_id,
        rrs.source_event_uid,
        rrs.status,
        rrs.account_family,
        rrs.maturity_bucket,
        rrs.maturity_date,
        rrs.reclass_required,
        rrs.currency_code,
        rrs.fx_rate,
        rrs.amount_txn,
        rrs.amount_base,
        rrs.period_start_date,
        rrs.period_end_date,
        rrs.created_by_user_id,
        rrs.posted_journal_entry_id,
        rrs.created_at,
        rrs.updated_at,
        (
          SELECT COUNT(*)
          FROM revenue_recognition_schedule_lines rrsl
          WHERE rrsl.tenant_id = rrs.tenant_id
            AND rrsl.schedule_id = rrs.id
        ) AS line_count
     FROM revenue_recognition_schedules rrs
     WHERE rrs.tenant_id = ?
       AND rrs.id = ?
     LIMIT 1${lockSql}`,
    [tenantId, scheduleId]
  );
  return result.rows?.[0] || null;
}

async function fetchScheduleRowBySourceEventUid({
  tenantId,
  legalEntityId,
  sourceEventUid,
  runQuery = query,
  forUpdate = false,
}) {
  const lockSql = forUpdate ? " FOR UPDATE" : "";
  const result = await runQuery(
    `SELECT
        rrs.id,
        rrs.tenant_id,
        rrs.legal_entity_id,
        rrs.fiscal_period_id,
        rrs.source_event_uid,
        rrs.status,
        rrs.account_family,
        rrs.maturity_bucket,
        rrs.maturity_date,
        rrs.reclass_required,
        rrs.currency_code,
        rrs.fx_rate,
        rrs.amount_txn,
        rrs.amount_base,
        rrs.period_start_date,
        rrs.period_end_date,
        rrs.created_by_user_id,
        rrs.posted_journal_entry_id,
        rrs.created_at,
        rrs.updated_at,
        (
          SELECT COUNT(*)
          FROM revenue_recognition_schedule_lines rrsl
          WHERE rrsl.tenant_id = rrs.tenant_id
            AND rrsl.schedule_id = rrs.id
        ) AS line_count
     FROM revenue_recognition_schedules rrs
     WHERE rrs.tenant_id = ?
       AND rrs.legal_entity_id = ?
       AND rrs.source_event_uid = ?
     LIMIT 1${lockSql}`,
    [tenantId, legalEntityId, sourceEventUid]
  );
  return result.rows?.[0] || null;
}

async function fetchScheduleLineSourceSetAndMaxLineNo({
  tenantId,
  legalEntityId,
  scheduleId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
        source_row_uid,
        line_no
     FROM revenue_recognition_schedule_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND schedule_id = ?`,
    [tenantId, legalEntityId, scheduleId]
  );
  const sourceUidSet = new Set();
  let maxLineNo = 0;
  for (const row of result.rows || []) {
    const uid = String(row.source_row_uid || "").trim();
    if (uid) {
      sourceUidSet.add(uid);
    }
    const lineNo = Number(row.line_no || 0);
    if (Number.isFinite(lineNo) && lineNo > maxLineNo) {
      maxLineNo = lineNo;
    }
  }
  return { sourceUidSet, maxLineNo };
}

async function refreshScheduleAggregates({
  tenantId,
  legalEntityId,
  scheduleId,
  targetStatus = "DRAFT",
  runQuery = query,
}) {
  const aggregateResult = await runQuery(
    `SELECT
        COALESCE(SUM(rrsl.amount_txn), 0) AS total_amount_txn,
        COALESCE(SUM(rrsl.amount_base), 0) AS total_amount_base,
        MAX(rrsl.maturity_date) AS max_maturity_date,
        MAX(CASE WHEN rrsl.maturity_bucket = 'LONG_TERM' THEN 1 ELSE 0 END) AS has_long_term,
        MAX(CASE WHEN rrsl.reclass_required = 1 THEN 1 ELSE 0 END) AS has_reclass_required
     FROM revenue_recognition_schedule_lines rrsl
     WHERE rrsl.tenant_id = ?
       AND rrsl.legal_entity_id = ?
       AND rrsl.schedule_id = ?`,
    [tenantId, legalEntityId, scheduleId]
  );
  const aggregate = aggregateResult.rows?.[0] || {};
  const totalAmountTxn = Number(Number(aggregate.total_amount_txn || 0).toFixed(6));
  const totalAmountBase = Number(Number(aggregate.total_amount_base || 0).toFixed(6));
  const maturityDate = toDateOnlyString(aggregate.max_maturity_date, "maturityDate");
  const maturityBucket = Number(aggregate.has_long_term || 0) > 0 ? "LONG_TERM" : "SHORT_TERM";
  const reclassRequired = Number(aggregate.has_reclass_required || 0) > 0 ? 1 : 0;

  await runQuery(
    `UPDATE revenue_recognition_schedules
     SET status = CASE
           WHEN status IN ('POSTED', 'REVERSED') THEN status
           ELSE ?
         END,
         maturity_bucket = ?,
         maturity_date = ?,
         reclass_required = ?,
         amount_txn = ?,
         amount_base = ?
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?`,
    [
      asUpper(targetStatus) || "DRAFT",
      maturityBucket,
      maturityDate,
      reclassRequired,
      totalAmountTxn,
      totalAmountBase,
      tenantId,
      legalEntityId,
      scheduleId,
    ]
  );
}

async function fetchScheduleLines({
  tenantId,
  legalEntityId,
  scheduleId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
        id,
        line_no,
        source_row_uid,
        account_family,
        maturity_bucket,
        maturity_date,
        reclass_required,
        currency_code,
        fx_rate,
        amount_txn,
        amount_base,
        fiscal_period_id,
        period_start_date,
        period_end_date
     FROM revenue_recognition_schedule_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND schedule_id = ?
     ORDER BY line_no ASC, id ASC`,
    [tenantId, legalEntityId, scheduleId]
  );
  return result.rows || [];
}

async function fetchRunRow({
  tenantId,
  runId,
  runQuery = query,
  forUpdate = false,
}) {
  const lockSql = forUpdate ? " FOR UPDATE" : "";
  const result = await runQuery(
    `SELECT
        rrr.id,
        rrr.tenant_id,
        rrr.legal_entity_id,
        rrr.schedule_id,
        rrr.fiscal_period_id,
        rrr.run_no,
        rrr.source_run_uid,
        rrr.status,
        rrr.account_family,
        rrr.maturity_bucket,
        rrr.maturity_date,
        rrr.reclass_required,
        rrr.currency_code,
        rrr.fx_rate,
        rrr.total_amount_txn,
        rrr.total_amount_base,
        rrr.period_start_date,
        rrr.period_end_date,
        rrr.reversal_of_run_id,
        rrr.posted_journal_entry_id,
        rrr.reversal_journal_entry_id,
        rrr.created_by_user_id,
        rrr.posted_by_user_id,
        rrr.reversed_by_user_id,
        rrr.posted_at,
        rrr.reversed_at,
        rrr.created_at,
        rrr.updated_at,
        (
          SELECT COUNT(*)
          FROM revenue_recognition_run_lines rrrl
          WHERE rrrl.tenant_id = rrr.tenant_id
            AND rrrl.run_id = rrr.id
        ) AS line_count
     FROM revenue_recognition_runs rrr
     WHERE rrr.tenant_id = ?
       AND rrr.id = ?
     LIMIT 1${lockSql}`,
    [tenantId, runId]
  );
  return result.rows?.[0] || null;
}

async function fetchRunLines({
  tenantId,
  legalEntityId,
  runId,
  runQuery = query,
  forUpdate = false,
}) {
  const lockSql = forUpdate ? " FOR UPDATE" : "";
  const result = await runQuery(
    `SELECT
        rrrl.id,
        rrrl.tenant_id,
        rrrl.legal_entity_id,
        rrrl.run_id,
        rrrl.schedule_line_id,
        rrrl.line_no,
        rrrl.source_row_uid,
        rrrl.status,
        rrrl.account_family,
        rrrl.maturity_bucket,
        rrrl.maturity_date,
        rrrl.reclass_required,
        rrrl.currency_code,
        rrrl.fx_rate,
        rrrl.amount_txn,
        rrrl.amount_base,
        rrrl.fiscal_period_id,
        rrrl.period_start_date,
        rrrl.period_end_date,
        rrrl.reversal_of_run_line_id,
        rrrl.posted_journal_entry_id,
        rrrl.posted_journal_line_id,
        rrsl.source_contract_id,
        rrsl.source_contract_line_id,
        rrsl.source_cari_document_id
     FROM revenue_recognition_run_lines rrrl
     LEFT JOIN revenue_recognition_schedule_lines rrsl
       ON rrsl.tenant_id = rrrl.tenant_id
      AND rrsl.legal_entity_id = rrrl.legal_entity_id
      AND rrsl.id = rrrl.schedule_line_id
     WHERE rrrl.tenant_id = ?
       AND rrrl.legal_entity_id = ?
       AND rrrl.run_id = ?
     ORDER BY rrrl.line_no ASC, rrrl.id ASC${lockSql}`,
    [tenantId, legalEntityId, runId]
  );
  return result.rows || [];
}

async function fetchContractLinePostingOverrides({
  tenantId,
  legalEntityId,
  contractLineIds,
  runQuery = query,
}) {
  const normalizedLineIds = Array.from(
    new Set((contractLineIds || []).map((value) => parsePositiveInt(value)).filter(Boolean))
  );
  const lineOverridesById = new Map();
  if (normalizedLineIds.length === 0) {
    return lineOverridesById;
  }

  const linePlaceholders = normalizedLineIds.map(() => "?").join(", ");
  const lineResult = await runQuery(
    `SELECT
        id,
        deferred_account_id,
        revenue_account_id
     FROM contract_lines
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id IN (${linePlaceholders})`,
    [tenantId, legalEntityId, ...normalizedLineIds]
  );
  const lineRows = lineResult.rows || [];
  const lineRowById = new Map(
    lineRows
      .map((row) => [parsePositiveInt(row.id), row])
      .filter(([lineId]) => Boolean(lineId))
  );
  const missingLineIds = normalizedLineIds.filter((lineId) => !lineRowById.has(lineId));
  if (missingLineIds.length > 0) {
    throw badRequest(
      `Posting account resolution failed: missing source contract lines ${missingLineIds.join(", ")}`
    );
  }

  const accountIdSet = new Set();
  for (const row of lineRows) {
    const deferredAccountId = parsePositiveInt(row.deferred_account_id);
    const revenueAccountId = parsePositiveInt(row.revenue_account_id);
    if (deferredAccountId) {
      accountIdSet.add(deferredAccountId);
    }
    if (revenueAccountId) {
      accountIdSet.add(revenueAccountId);
    }
  }

  const validPostingAccountIdSet = new Set();
  if (accountIdSet.size > 0) {
    const accountIds = Array.from(accountIdSet);
    const accountPlaceholders = accountIds.map(() => "?").join(", ");
    const accountResult = await runQuery(
      `SELECT a.id
       FROM accounts a
       JOIN charts_of_accounts c ON c.id = a.coa_id
       WHERE a.id IN (${accountPlaceholders})
         AND c.tenant_id = ?
         AND c.legal_entity_id = ?
         AND c.scope = 'LEGAL_ENTITY'
         AND a.is_active = TRUE
         AND a.allow_posting = TRUE`,
      [...accountIds, tenantId, legalEntityId]
    );
    for (const row of accountResult.rows || []) {
      const accountId = parsePositiveInt(row.id);
      if (accountId) {
        validPostingAccountIdSet.add(accountId);
      }
    }
  }

  for (const row of lineRows) {
    const lineId = parsePositiveInt(row.id);
    const deferredAccountId = parsePositiveInt(row.deferred_account_id);
    const revenueAccountId = parsePositiveInt(row.revenue_account_id);
    if (deferredAccountId && !validPostingAccountIdSet.has(deferredAccountId)) {
      throw badRequest(
        `contractLineId=${lineId} deferred_account_id=${deferredAccountId} is not an active posting account in legalEntity scope`
      );
    }
    if (revenueAccountId && !validPostingAccountIdSet.has(revenueAccountId)) {
      throw badRequest(
        `contractLineId=${lineId} revenue_account_id=${revenueAccountId} is not an active posting account in legalEntity scope`
      );
    }
    lineOverridesById.set(lineId, {
      deferredAccountId: deferredAccountId || null,
      revenueAccountId: revenueAccountId || null,
    });
  }

  return lineOverridesById;
}

async function fetchRunSubledgerEntries({
  tenantId,
  legalEntityId,
  runId,
  runQuery = query,
  forUpdate = false,
}) {
  const lockSql = forUpdate ? " FOR UPDATE" : "";
  const result = await runQuery(
    `SELECT
        id,
        tenant_id,
        legal_entity_id,
        run_id,
        run_line_id,
        schedule_line_id,
        entry_no,
        source_row_uid,
        entry_kind,
        status,
        account_family,
        maturity_bucket,
        maturity_date,
        reclass_required,
        currency_code,
        fx_rate,
        amount_txn,
        amount_base,
        fiscal_period_id,
        period_start_date,
        period_end_date,
        reversal_of_subledger_entry_id,
        posted_journal_entry_id,
        posted_journal_line_id
     FROM revenue_recognition_subledger_entries
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND run_id = ?
     ORDER BY entry_no ASC, id ASC${lockSql}`,
    [tenantId, legalEntityId, runId]
  );
  return result.rows || [];
}

async function fetchJournalWithLines({
  tenantId,
  journalEntryId,
  runQuery = query,
  forUpdate = false,
}) {
  const lockSql = forUpdate ? " FOR UPDATE" : "";
  const journalResult = await runQuery(
    `SELECT
        id,
        tenant_id,
        legal_entity_id,
        book_id,
        fiscal_period_id,
        journal_no,
        status,
        entry_date,
        document_date,
        currency_code,
        description,
        reference_no,
        total_debit_base,
        total_credit_base,
        posted_at,
        reversal_journal_entry_id
     FROM journal_entries
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1${lockSql}`,
    [tenantId, journalEntryId]
  );
  const journal = journalResult.rows?.[0] || null;
  if (!journal) {
    return null;
  }

  const linesResult = await runQuery(
    `SELECT
        id,
        line_no,
        account_id,
        description,
        subledger_reference_no,
        currency_code,
        amount_txn,
        debit_base,
        credit_base
     FROM journal_lines
     WHERE journal_entry_id = ?
     ORDER BY line_no ASC`,
    [journalEntryId]
  );

  return {
    journal,
    lines: linesResult.rows || [],
  };
}

async function resolveBookForRunPeriod({
  tenantId,
  legalEntityId,
  fiscalPeriodId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
        b.id,
        b.calendar_id,
        b.base_currency_code,
        fp.start_date,
        fp.end_date
     FROM books b
     JOIN fiscal_periods fp ON fp.calendar_id = b.calendar_id
     WHERE b.tenant_id = ?
       AND b.legal_entity_id = ?
       AND fp.id = ?
     ORDER BY
       CASE WHEN b.book_type = 'LOCAL' THEN 0 ELSE 1 END,
       b.id ASC
     LIMIT 1`,
    [tenantId, legalEntityId, fiscalPeriodId]
  );
  const row = result.rows?.[0] || null;
  if (!row) {
    throw badRequest("No book found for legalEntityId/fiscalPeriodId scope");
  }
  return row;
}

async function ensurePeriodOpenForBook({
  bookId,
  fiscalPeriodId,
  actionLabel,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT status
     FROM period_statuses
     WHERE book_id = ?
       AND fiscal_period_id = ?
     LIMIT 1`,
    [bookId, fiscalPeriodId]
  );
  const status = asUpper(result.rows?.[0]?.status || "OPEN");
  if (status !== "OPEN") {
    throw badRequest(`Period is ${status}; cannot ${actionLabel}`);
  }
}

function formatFamilySet(familySet) {
  return Array.from(familySet || [])
    .map((item) => String(item))
    .join(", ");
}

function assertPostingFamilySupported(accountFamily, allowedFamilies, phaseLabel) {
  const normalized = asUpper(accountFamily);
  if (!allowedFamilies?.has(normalized)) {
    throw badRequest(
      `accountFamily=${normalized} is not active for ${phaseLabel}; supported families: ${formatFamilySet(
        allowedFamilies
      )}`
    );
  }
  return normalized;
}

function assertAccrualFamilySupported(accountFamily, phaseLabel) {
  return assertPostingFamilySupported(accountFamily, PR17C_ACCRUAL_FAMILIES, phaseLabel);
}

function resolveJournalPrefixByFamily(accountFamily) {
  switch (asUpper(accountFamily)) {
    case ACCOUNT_FAMILY.DEFREV:
      return "REVDEF";
    case ACCOUNT_FAMILY.PREPAID_EXPENSE:
      return "REVPRE";
    case ACCOUNT_FAMILY.ACCRUED_REVENUE:
      return "REVACR";
    case ACCOUNT_FAMILY.ACCRUED_EXPENSE:
      return "REVACE";
    default:
      return "REVREC";
  }
}

function ensureAccrualMaturityBoundary({
  run,
  targetPeriodEndDate,
  actionLabel,
}) {
  const maturityDate = toDateOnlyString(run?.maturity_date, "maturityDate");
  const normalizedTargetEndDate = toDateOnlyString(targetPeriodEndDate, "periodEndDate");
  if (
    maturityDate &&
    normalizedTargetEndDate &&
    normalizedTargetEndDate < maturityDate
  ) {
    throw badRequest(
      `Accrual maturity boundary: cannot ${actionLabel} before maturityDate=${maturityDate}`
    );
  }
}

async function resolveRevenuePostingAccounts({
  tenantId,
  legalEntityId,
  accountFamily,
  allowedFamilies,
  phaseLabel = "revenue-recognition posting",
  runQuery = query,
}) {
  const normalizedFamily = assertPostingFamilySupported(
    accountFamily,
    allowedFamilies,
    phaseLabel
  );
  const purposeCodes = PURPOSE_CODES_BY_FAMILY[normalizedFamily] || [];
  if (purposeCodes.length === 0) {
    throw badRequest(`No posting purpose definitions for accountFamily=${normalizedFamily}`);
  }

  const placeholders = purposeCodes.map(() => "?").join(", ");
  const result = await runQuery(
    `SELECT
       jpa.purpose_code,
       a.id AS account_id,
       a.code AS account_code
     FROM journal_purpose_accounts jpa
     JOIN accounts a ON a.id = jpa.account_id
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE jpa.tenant_id = ?
       AND jpa.legal_entity_id = ?
       AND jpa.purpose_code IN (${placeholders})
       AND c.tenant_id = ?
       AND c.legal_entity_id = ?
       AND c.scope = 'LEGAL_ENTITY'
       AND a.is_active = TRUE
       AND a.allow_posting = TRUE`,
    [tenantId, legalEntityId, ...purposeCodes, tenantId, legalEntityId]
  );

  const byPurpose = new Map(
    (result.rows || []).map((row) => [
      asUpper(row.purpose_code),
      {
        id: parsePositiveInt(row.account_id),
        code: String(row.account_code || ""),
      },
    ])
  );

  return byPurpose;
}

function formatRunLineContext(runLine) {
  const runLineId = parsePositiveInt(runLine?.id);
  const scheduleLineId = parsePositiveInt(runLine?.schedule_line_id);
  const sourceContractLineId = parsePositiveInt(runLine?.source_contract_line_id);
  const context = [];
  if (runLineId) {
    context.push(`runLineId=${runLineId}`);
  }
  if (scheduleLineId) {
    context.push(`scheduleLineId=${scheduleLineId}`);
  }
  if (sourceContractLineId) {
    context.push(`sourceContractLineId=${sourceContractLineId}`);
  }
  return context.length > 0 ? context.join(", ") : "runLine context unavailable";
}

function resolveMappedPurposeAccountIdOrThrow({
  mappingByPurpose,
  purposeCode,
  runLine,
  accountLabel,
}) {
  const accountId = mappingByPurpose.get(asUpper(purposeCode))?.id;
  if (accountId) {
    return accountId;
  }
  throw badRequest(
    `Setup required: configure journal_purpose_accounts for ${purposeCode} (${accountLabel}; ${formatRunLineContext(
      runLine
    )})`
  );
}

function resolveRecognitionAccountIdForRunLine({
  runLine,
  accountLabel,
  contractLineAccountOverridesById,
  contractLineField,
  fallbackPurposeCode,
  mappingByPurpose,
}) {
  const overrideMap =
    contractLineAccountOverridesById instanceof Map
      ? contractLineAccountOverridesById
      : new Map();
  const sourceContractLineId = parsePositiveInt(runLine?.source_contract_line_id);
  if (sourceContractLineId) {
    const lineOverride = overrideMap.get(sourceContractLineId);
    if (!lineOverride) {
      throw badRequest(
        `Posting account resolution failed: source_contract_line_id=${sourceContractLineId} not found (${formatRunLineContext(
          runLine
        )})`
      );
    }
    const overrideAccountId = parsePositiveInt(lineOverride?.[contractLineField]);
    if (overrideAccountId) {
      return overrideAccountId;
    }
  }
  return resolveMappedPurposeAccountIdOrThrow({
    mappingByPurpose,
    purposeCode: fallbackPurposeCode,
    runLine,
    accountLabel,
  });
}

function buildRevenueJournalNo(prefix, runNo, runId) {
  const normalizedPrefix = asUpper(prefix || "REVREC").slice(0, 12) || "REVREC";
  const parsedRunId = parsePositiveInt(runId) || Date.now();
  const normalizedRunNo = String(runNo || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 18);
  const stamp = Date.now().toString(36).toUpperCase();
  const candidate = normalizedRunNo
    ? `${normalizedPrefix}-${normalizedRunNo}-${parsedRunId}-${stamp}`
    : `${normalizedPrefix}-${parsedRunId}-${stamp}`;
  return candidate.slice(0, 40);
}

function buildReversalRunNo(runNo, runId) {
  const normalizedRunNo = String(runNo || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 45);
  const base = normalizedRunNo || `RUN-${parsePositiveInt(runId) || Date.now()}`;
  return `${base}-REV`.slice(0, 80);
}

function buildPostingEntriesForRunLine({
  runNo,
  runLine,
  mappingByPurpose,
  contractLineAccountOverridesById,
}) {
  const accountFamily = assertPostingFamilySupported(
    runLine.account_family,
    PR17_POSTABLE_FAMILIES,
    "PR-17B/17C posting"
  );
  const maturityBucket = asUpper(runLine.maturity_bucket);
  const amountTxn = normalizeAmount(runLine.amount_txn, "runLine.amount_txn", {
    allowZero: false,
  });
  const amountBase = normalizeAmount(runLine.amount_base, "runLine.amount_base", {
    allowZero: false,
  });
  const currencyCode = asUpper(runLine.currency_code);
  const lineNo = Number(runLine.line_no || 0);
  const isLongTerm = maturityBucket === "LONG_TERM";
  const reclassRequired = toBoolean(runLine.reclass_required) && isLongTerm;
  const baseDescription = `Revenue-recognition ${runNo} line ${lineNo}`;
  const subledgerReferenceNo = `REVREC:${parsePositiveInt(runLine.run_id)}:${lineNo}`.slice(0, 100);

  if (!currencyCode || currencyCode.length !== 3) {
    throw badRequest("runLine.currency_code must be a 3-letter code");
  }

  const recognitionLines = [];
  if (accountFamily === ACCOUNT_FAMILY.DEFREV) {
    const deferredPurpose = isLongTerm
      ? REVREC_PURPOSE_CODES.DEFREV_LONG_LIABILITY
      : REVREC_PURPOSE_CODES.DEFREV_SHORT_LIABILITY;
    const deferredAccountId = resolveRecognitionAccountIdForRunLine({
      runLine,
      accountLabel: "DEFREV deferred account",
      contractLineAccountOverridesById,
      contractLineField: "deferredAccountId",
      fallbackPurposeCode: deferredPurpose,
      mappingByPurpose,
    });
    const revenueAccountId = resolveRecognitionAccountIdForRunLine({
      runLine,
      accountLabel: "DEFREV revenue account",
      contractLineAccountOverridesById,
      contractLineField: "revenueAccountId",
      fallbackPurposeCode: REVREC_PURPOSE_CODES.DEFREV_REVENUE,
      mappingByPurpose,
    });

    recognitionLines.push(
      {
        accountId: deferredAccountId,
        debitBase: amountBase,
        creditBase: 0,
        amountTxn,
        description: `${baseDescription} DEFREV recognition`,
        subledgerReferenceNo,
        currencyCode,
      },
      {
        accountId: revenueAccountId,
        debitBase: 0,
        creditBase: amountBase,
        amountTxn: Number((amountTxn * -1).toFixed(6)),
        description: `${baseDescription} DEFREV recognition`,
        subledgerReferenceNo,
        currencyCode,
      }
    );

    const entries = [
      {
        entryKind: "RECOGNITION",
        runLineId: parsePositiveInt(runLine.id),
        scheduleLineId: parsePositiveInt(runLine.schedule_line_id),
        accountFamily,
        maturityBucket,
        maturityDate: toDateOnlyString(runLine.maturity_date, "maturityDate"),
        reclassRequired: false,
        currencyCode,
        fxRate: runLine.fx_rate,
        amountTxn,
        amountBase,
        description: `${baseDescription} DEFREV recognition`,
        lines: recognitionLines,
      },
    ];

    if (reclassRequired) {
      const longAccountId = resolveMappedPurposeAccountIdOrThrow({
        mappingByPurpose,
        purposeCode: REVREC_PURPOSE_CODES.DEFREV_LONG_LIABILITY,
        runLine,
        accountLabel: "DEFREV long liability reclass account",
      });
      const shortAccountId = resolveMappedPurposeAccountIdOrThrow({
        mappingByPurpose,
        purposeCode: REVREC_PURPOSE_CODES.DEFREV_SHORT_LIABILITY,
        runLine,
        accountLabel: "DEFREV short liability reclass account",
      });
      if (longAccountId === shortAccountId) {
        throw badRequest("DEFREV long and short liability mappings must be different");
      }
      entries.push({
        entryKind: "RECLASS",
        runLineId: parsePositiveInt(runLine.id),
        scheduleLineId: parsePositiveInt(runLine.schedule_line_id),
        accountFamily,
        maturityBucket,
        maturityDate: toDateOnlyString(runLine.maturity_date, "maturityDate"),
        reclassRequired: true,
        currencyCode,
        fxRate: runLine.fx_rate,
        amountTxn,
        amountBase,
        description: `${baseDescription} DEFREV 480->380 reclass`,
        lines: [
          {
            accountId: longAccountId,
            debitBase: amountBase,
            creditBase: 0,
            amountTxn,
            description: `${baseDescription} DEFREV 480->380 reclass`,
            subledgerReferenceNo,
            currencyCode,
          },
          {
            accountId: shortAccountId,
            debitBase: 0,
            creditBase: amountBase,
            amountTxn: Number((amountTxn * -1).toFixed(6)),
            description: `${baseDescription} DEFREV 480->380 reclass`,
            subledgerReferenceNo,
            currencyCode,
          },
        ],
      });
    }

    return entries;
  }

  if (accountFamily === ACCOUNT_FAMILY.PREPAID_EXPENSE) {
    const assetPurpose = isLongTerm
      ? REVREC_PURPOSE_CODES.PREPAID_EXP_LONG_ASSET
      : REVREC_PURPOSE_CODES.PREPAID_EXP_SHORT_ASSET;
    const assetAccountId = resolveRecognitionAccountIdForRunLine({
      runLine,
      accountLabel: "PREPAID deferred asset account",
      contractLineAccountOverridesById,
      contractLineField: "deferredAccountId",
      fallbackPurposeCode: assetPurpose,
      mappingByPurpose,
    });
    const expenseAccountId = resolveRecognitionAccountIdForRunLine({
      runLine,
      accountLabel: "PREPAID expense account",
      contractLineAccountOverridesById,
      contractLineField: "revenueAccountId",
      fallbackPurposeCode: REVREC_PURPOSE_CODES.PREPAID_EXPENSE,
      mappingByPurpose,
    });

    const entries = [
      {
        entryKind: "RECOGNITION",
        runLineId: parsePositiveInt(runLine.id),
        scheduleLineId: parsePositiveInt(runLine.schedule_line_id),
        accountFamily,
        maturityBucket,
        maturityDate: toDateOnlyString(runLine.maturity_date, "maturityDate"),
        reclassRequired: false,
        currencyCode,
        fxRate: runLine.fx_rate,
        amountTxn,
        amountBase,
        description: `${baseDescription} PREPAID amortization`,
        lines: [
          {
            accountId: expenseAccountId,
            debitBase: amountBase,
            creditBase: 0,
            amountTxn,
            description: `${baseDescription} PREPAID amortization`,
            subledgerReferenceNo,
            currencyCode,
          },
          {
            accountId: assetAccountId,
            debitBase: 0,
            creditBase: amountBase,
            amountTxn: Number((amountTxn * -1).toFixed(6)),
            description: `${baseDescription} PREPAID amortization`,
            subledgerReferenceNo,
            currencyCode,
          },
        ],
      },
    ];

    if (reclassRequired) {
      const longAccountId = resolveMappedPurposeAccountIdOrThrow({
        mappingByPurpose,
        purposeCode: REVREC_PURPOSE_CODES.PREPAID_EXP_LONG_ASSET,
        runLine,
        accountLabel: "PREPAID long asset reclass account",
      });
      const shortAccountId = resolveMappedPurposeAccountIdOrThrow({
        mappingByPurpose,
        purposeCode: REVREC_PURPOSE_CODES.PREPAID_EXP_SHORT_ASSET,
        runLine,
        accountLabel: "PREPAID short asset reclass account",
      });
      if (longAccountId === shortAccountId) {
        throw badRequest("PREPAID long and short asset mappings must be different");
      }
      entries.push({
        entryKind: "RECLASS",
        runLineId: parsePositiveInt(runLine.id),
        scheduleLineId: parsePositiveInt(runLine.schedule_line_id),
        accountFamily,
        maturityBucket,
        maturityDate: toDateOnlyString(runLine.maturity_date, "maturityDate"),
        reclassRequired: true,
        currencyCode,
        fxRate: runLine.fx_rate,
        amountTxn,
        amountBase,
        description: `${baseDescription} PREPAID 280->180 reclass`,
        lines: [
          {
            accountId: shortAccountId,
            debitBase: amountBase,
            creditBase: 0,
            amountTxn,
            description: `${baseDescription} PREPAID 280->180 reclass`,
            subledgerReferenceNo,
            currencyCode,
          },
          {
            accountId: longAccountId,
            debitBase: 0,
            creditBase: amountBase,
            amountTxn: Number((amountTxn * -1).toFixed(6)),
            description: `${baseDescription} PREPAID 280->180 reclass`,
            subledgerReferenceNo,
            currencyCode,
          },
        ],
      });
    }

    return entries;
  }

  if (accountFamily === ACCOUNT_FAMILY.ACCRUED_REVENUE) {
    const accruedAssetPurpose = isLongTerm
      ? REVREC_PURPOSE_CODES.ACCR_REV_LONG_ASSET
      : REVREC_PURPOSE_CODES.ACCR_REV_SHORT_ASSET;
    const accruedAssetAccountId = resolveRecognitionAccountIdForRunLine({
      runLine,
      accountLabel: "ACCRUED_REVENUE accrued-asset account",
      contractLineAccountOverridesById,
      contractLineField: "deferredAccountId",
      fallbackPurposeCode: accruedAssetPurpose,
      mappingByPurpose,
    });
    const revenueAccountId = resolveRecognitionAccountIdForRunLine({
      runLine,
      accountLabel: "ACCRUED_REVENUE revenue account",
      contractLineAccountOverridesById,
      contractLineField: "revenueAccountId",
      fallbackPurposeCode: REVREC_PURPOSE_CODES.ACCR_REV_REVENUE,
      mappingByPurpose,
    });

    const entries = [
      {
        entryKind: "RECOGNITION",
        runLineId: parsePositiveInt(runLine.id),
        scheduleLineId: parsePositiveInt(runLine.schedule_line_id),
        accountFamily,
        maturityBucket,
        maturityDate: toDateOnlyString(runLine.maturity_date, "maturityDate"),
        reclassRequired: false,
        currencyCode,
        fxRate: runLine.fx_rate,
        amountTxn,
        amountBase,
        description: `${baseDescription} ACCR_REV accrual`,
        lines: [
          {
            accountId: accruedAssetAccountId,
            debitBase: amountBase,
            creditBase: 0,
            amountTxn,
            description: `${baseDescription} ACCR_REV accrual`,
            subledgerReferenceNo,
            currencyCode,
          },
          {
            accountId: revenueAccountId,
            debitBase: 0,
            creditBase: amountBase,
            amountTxn: Number((amountTxn * -1).toFixed(6)),
            description: `${baseDescription} ACCR_REV accrual`,
            subledgerReferenceNo,
            currencyCode,
          },
        ],
      },
    ];

    if (reclassRequired) {
      const longAccountId = resolveMappedPurposeAccountIdOrThrow({
        mappingByPurpose,
        purposeCode: REVREC_PURPOSE_CODES.ACCR_REV_LONG_ASSET,
        runLine,
        accountLabel: "ACCRUED_REVENUE long asset reclass account",
      });
      const shortAccountId = resolveMappedPurposeAccountIdOrThrow({
        mappingByPurpose,
        purposeCode: REVREC_PURPOSE_CODES.ACCR_REV_SHORT_ASSET,
        runLine,
        accountLabel: "ACCRUED_REVENUE short asset reclass account",
      });
      if (longAccountId === shortAccountId) {
        throw badRequest("ACCR_REV long and short asset mappings must be different");
      }
      entries.push({
        entryKind: "RECLASS",
        runLineId: parsePositiveInt(runLine.id),
        scheduleLineId: parsePositiveInt(runLine.schedule_line_id),
        accountFamily,
        maturityBucket,
        maturityDate: toDateOnlyString(runLine.maturity_date, "maturityDate"),
        reclassRequired: true,
        currencyCode,
        fxRate: runLine.fx_rate,
        amountTxn,
        amountBase,
        description: `${baseDescription} ACCR_REV 281->181 reclass`,
        lines: [
          {
            accountId: shortAccountId,
            debitBase: amountBase,
            creditBase: 0,
            amountTxn,
            description: `${baseDescription} ACCR_REV 281->181 reclass`,
            subledgerReferenceNo,
            currencyCode,
          },
          {
            accountId: longAccountId,
            debitBase: 0,
            creditBase: amountBase,
            amountTxn: Number((amountTxn * -1).toFixed(6)),
            description: `${baseDescription} ACCR_REV 281->181 reclass`,
            subledgerReferenceNo,
            currencyCode,
          },
        ],
      });
    }

    return entries;
  }

  if (accountFamily === ACCOUNT_FAMILY.ACCRUED_EXPENSE) {
    const accruedLiabilityPurpose = isLongTerm
      ? REVREC_PURPOSE_CODES.ACCR_EXP_LONG_LIABILITY
      : REVREC_PURPOSE_CODES.ACCR_EXP_SHORT_LIABILITY;
    const accruedLiabilityAccountId = resolveRecognitionAccountIdForRunLine({
      runLine,
      accountLabel: "ACCRUED_EXPENSE accrued-liability account",
      contractLineAccountOverridesById,
      contractLineField: "deferredAccountId",
      fallbackPurposeCode: accruedLiabilityPurpose,
      mappingByPurpose,
    });
    const expenseAccountId = resolveRecognitionAccountIdForRunLine({
      runLine,
      accountLabel: "ACCRUED_EXPENSE expense account",
      contractLineAccountOverridesById,
      contractLineField: "revenueAccountId",
      fallbackPurposeCode: REVREC_PURPOSE_CODES.ACCR_EXP_EXPENSE,
      mappingByPurpose,
    });

    const entries = [
      {
        entryKind: "RECOGNITION",
        runLineId: parsePositiveInt(runLine.id),
        scheduleLineId: parsePositiveInt(runLine.schedule_line_id),
        accountFamily,
        maturityBucket,
        maturityDate: toDateOnlyString(runLine.maturity_date, "maturityDate"),
        reclassRequired: false,
        currencyCode,
        fxRate: runLine.fx_rate,
        amountTxn,
        amountBase,
        description: `${baseDescription} ACCR_EXP accrual`,
        lines: [
          {
            accountId: expenseAccountId,
            debitBase: amountBase,
            creditBase: 0,
            amountTxn,
            description: `${baseDescription} ACCR_EXP accrual`,
            subledgerReferenceNo,
            currencyCode,
          },
          {
            accountId: accruedLiabilityAccountId,
            debitBase: 0,
            creditBase: amountBase,
            amountTxn: Number((amountTxn * -1).toFixed(6)),
            description: `${baseDescription} ACCR_EXP accrual`,
            subledgerReferenceNo,
            currencyCode,
          },
        ],
      },
    ];

    if (reclassRequired) {
      const longAccountId = resolveMappedPurposeAccountIdOrThrow({
        mappingByPurpose,
        purposeCode: REVREC_PURPOSE_CODES.ACCR_EXP_LONG_LIABILITY,
        runLine,
        accountLabel: "ACCRUED_EXPENSE long liability reclass account",
      });
      const shortAccountId = resolveMappedPurposeAccountIdOrThrow({
        mappingByPurpose,
        purposeCode: REVREC_PURPOSE_CODES.ACCR_EXP_SHORT_LIABILITY,
        runLine,
        accountLabel: "ACCRUED_EXPENSE short liability reclass account",
      });
      if (longAccountId === shortAccountId) {
        throw badRequest("ACCR_EXP long and short liability mappings must be different");
      }
      entries.push({
        entryKind: "RECLASS",
        runLineId: parsePositiveInt(runLine.id),
        scheduleLineId: parsePositiveInt(runLine.schedule_line_id),
        accountFamily,
        maturityBucket,
        maturityDate: toDateOnlyString(runLine.maturity_date, "maturityDate"),
        reclassRequired: true,
        currencyCode,
        fxRate: runLine.fx_rate,
        amountTxn,
        amountBase,
        description: `${baseDescription} ACCR_EXP 481->381 reclass`,
        lines: [
          {
            accountId: longAccountId,
            debitBase: amountBase,
            creditBase: 0,
            amountTxn,
            description: `${baseDescription} ACCR_EXP 481->381 reclass`,
            subledgerReferenceNo,
            currencyCode,
          },
          {
            accountId: shortAccountId,
            debitBase: 0,
            creditBase: amountBase,
            amountTxn: Number((amountTxn * -1).toFixed(6)),
            description: `${baseDescription} ACCR_EXP 481->381 reclass`,
            subledgerReferenceNo,
            currencyCode,
          },
        ],
      });
    }

    return entries;
  }

  throw badRequest(`Unsupported accountFamily=${accountFamily} for posting`);
}

async function insertPostedJournalWithLinesTx(tx, payload) {
  const totals = ensureBalancedJournalLines(payload.lines, "Revenue-recognition posting journal");
  const insertResult = await tx.query(
    `INSERT INTO journal_entries (
        tenant_id,
        legal_entity_id,
        book_id,
        fiscal_period_id,
        journal_no,
        source_type,
        status,
        entry_date,
        document_date,
        currency_code,
        description,
        reference_no,
        total_debit_base,
        total_credit_base,
        created_by_user_id,
        posted_by_user_id,
        posted_at
     )
     VALUES (?, ?, ?, ?, ?, 'SYSTEM', 'POSTED', ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
    [
      payload.tenantId,
      payload.legalEntityId,
      payload.bookId,
      payload.fiscalPeriodId,
      payload.journalNo,
      payload.entryDate,
      payload.documentDate,
      payload.currencyCode,
      payload.description,
      payload.referenceNo,
      totals.totalDebit,
      totals.totalCredit,
      payload.userId,
      payload.userId,
    ]
  );

  const journalEntryId = parsePositiveInt(insertResult.rows?.insertId);
  if (!journalEntryId) {
    throw badRequest("Failed to create posted revenue-recognition journal");
  }

  for (let index = 0; index < payload.lines.length; index += 1) {
    const line = payload.lines[index];
    // eslint-disable-next-line no-await-in-loop
    await tx.query(
      `INSERT INTO journal_lines (
          journal_entry_id,
          line_no,
          account_id,
          operating_unit_id,
          counterparty_legal_entity_id,
          description,
          subledger_reference_no,
          currency_code,
          amount_txn,
          debit_base,
          credit_base,
          tax_code
       )
       VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, NULL)`,
      [
        journalEntryId,
        index + 1,
        parsePositiveInt(line.accountId),
        line.description || null,
        line.subledgerReferenceNo || null,
        line.currencyCode,
        Number(line.amountTxn || 0),
        Number(line.debitBase || 0),
        Number(line.creditBase || 0),
      ]
    );
  }

  return {
    journalEntryId,
    totalDebit: totals.totalDebit,
    totalCredit: totals.totalCredit,
    lineCount: payload.lines.length,
  };
}

async function insertSubledgerEntriesTx(tx, payload) {
  for (let index = 0; index < payload.entries.length; index += 1) {
    const entry = payload.entries[index];
    const sourceRowUid = buildDeterministicUid("SUBLEDGER", [
      payload.runId,
      entry.entryKind,
      entry.runLineId,
      entry.reversalOfSubledgerEntryId || 0,
      index + 1,
    ]);
    // eslint-disable-next-line no-await-in-loop
    await tx.query(
      `INSERT INTO revenue_recognition_subledger_entries (
          tenant_id,
          legal_entity_id,
          run_id,
          run_line_id,
          schedule_line_id,
          entry_no,
          source_row_uid,
          entry_kind,
          status,
          account_family,
          maturity_bucket,
          maturity_date,
          reclass_required,
          currency_code,
          fx_rate,
          amount_txn,
          amount_base,
          fiscal_period_id,
          period_start_date,
          period_end_date,
          reversal_of_subledger_entry_id,
          posted_journal_entry_id,
          posted_journal_line_id,
          created_by_user_id
       )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'POSTED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)`,
      [
        payload.tenantId,
        payload.legalEntityId,
        payload.runId,
        entry.runLineId,
        entry.scheduleLineId,
        index + 1,
        sourceRowUid,
        entry.entryKind,
        entry.accountFamily,
        entry.maturityBucket,
        entry.maturityDate,
        entry.reclassRequired ? 1 : 0,
        entry.currencyCode,
        entry.fxRate,
        entry.amountTxn,
        entry.amountBase,
        payload.fiscalPeriodId,
        payload.periodStartDate,
        payload.periodEndDate,
        entry.reversalOfSubledgerEntryId || null,
        payload.postedJournalEntryId,
        payload.userId,
      ]
    );
  }
}

async function assertNoDuplicateOpenRunLinesTx({
  tenantId,
  legalEntityId,
  scheduleLineIds,
  runQuery = query,
}) {
  if (!Array.isArray(scheduleLineIds) || scheduleLineIds.length === 0) {
    return;
  }
  const placeholders = scheduleLineIds.map(() => "?").join(", ");
  const result = await runQuery(
    `SELECT
        rrrl.id AS run_line_id,
        rrrl.run_id,
        rrrl.schedule_line_id,
        rrrl.status,
        rrr.status AS run_status
     FROM revenue_recognition_run_lines rrrl
     JOIN revenue_recognition_runs rrr
       ON rrr.id = rrrl.run_id
      AND rrr.tenant_id = rrrl.tenant_id
     WHERE rrrl.tenant_id = ?
       AND rrrl.legal_entity_id = ?
       AND rrrl.schedule_line_id IN (${placeholders})
       AND rrrl.status IN ('OPEN', 'POSTED')
       AND rrr.status IN ('DRAFT', 'READY', 'POSTED')
     LIMIT 1`,
    [tenantId, legalEntityId, ...scheduleLineIds]
  );

  const existing = result.rows?.[0] || null;
  if (existing) {
    throw badRequest(
      `Duplicate rerun guard: schedule_line_id=${existing.schedule_line_id} already has open run_line_id=${existing.run_line_id}`
    );
  }
}

function buildDefaultScheduleSourceUid(payload) {
  return buildDeterministicUid("SCHED", [
    payload.tenantId,
    payload.legalEntityId,
    payload.fiscalPeriodId,
    payload.accountFamily,
    payload.maturityBucket,
    payload.maturityDate,
    payload.currencyCode,
    payload.amountTxn,
    payload.amountBase,
  ]);
}

function buildDefaultRunSourceUid(payload) {
  return buildDeterministicUid("RUN", [
    payload.tenantId,
    payload.legalEntityId,
    payload.scheduleId || 0,
    payload.fiscalPeriodId,
    payload.accountFamily,
    payload.maturityBucket,
    payload.maturityDate,
    payload.currencyCode,
    payload.totalAmountTxn,
    payload.totalAmountBase,
  ]);
}

function buildDefaultRunNo(payload) {
  const timestamp = Date.now();
  return `RRUN-${payload.legalEntityId}-${payload.fiscalPeriodId}-${timestamp}`.slice(0, 80);
}

function roundAmount(value) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return Number(numeric.toFixed(6));
}

function resolveReportPagination(filters) {
  const limit =
    Number.isInteger(filters?.limit) && Number(filters.limit) > 0
      ? Number(filters.limit)
      : 100;
  const offset =
    Number.isInteger(filters?.offset) && Number(filters.offset) >= 0
      ? Number(filters.offset)
      : 0;
  return { limit, offset };
}

function normalizeReportFamilyFilter(accountFamily, allowedFamilies = null) {
  const normalizedAllowed = Array.from(
    new Set(
      (Array.isArray(allowedFamilies) && allowedFamilies.length > 0
        ? allowedFamilies
        : Array.from(PR17_POSTABLE_FAMILIES)
      ).map((family) => asUpper(family))
    )
  );

  if (!accountFamily) {
    return normalizedAllowed;
  }

  const normalizedRequested = asUpper(accountFamily);
  if (!normalizedAllowed.includes(normalizedRequested)) {
    return [];
  }
  return [normalizedRequested];
}

function signedAmountSql(alias, columnName) {
  return `CASE WHEN ${alias}.entry_kind = 'REVERSAL' THEN -${alias}.${columnName} ELSE ${alias}.${columnName} END`;
}

function shortBucketAmountSql(alias, columnName) {
  const signedSql = signedAmountSql(alias, columnName);
  return `CASE
    WHEN ${alias}.entry_kind = 'RECLASS' AND ${alias}.maturity_bucket = 'LONG_TERM' THEN (${signedSql})
    WHEN ${alias}.maturity_bucket = 'SHORT_TERM' THEN (${signedSql})
    ELSE 0
  END`;
}

function longBucketAmountSql(alias, columnName) {
  const signedSql = signedAmountSql(alias, columnName);
  return `CASE
    WHEN ${alias}.entry_kind = 'RECLASS' AND ${alias}.maturity_bucket = 'LONG_TERM' THEN -(${signedSql})
    WHEN ${alias}.maturity_bucket = 'LONG_TERM' THEN (${signedSql})
    ELSE 0
  END`;
}

function buildReportEmptySummary() {
  return {
    openingAmountTxn: 0,
    openingAmountBase: 0,
    movementAmountTxn: 0,
    movementAmountBase: 0,
    closingAmountTxn: 0,
    closingAmountBase: 0,
    shortTermAmountTxn: 0,
    shortTermAmountBase: 0,
    longTermAmountTxn: 0,
    longTermAmountBase: 0,
    totalAmountTxn: 0,
    totalAmountBase: 0,
    grossMovementAmountTxn: 0,
    grossMovementAmountBase: 0,
    entryCount: 0,
    journalCount: 0,
  };
}

function buildReportEmptyReconciliation() {
  return {
    totalGroups: 0,
    matchedGroups: 0,
    unmatchedGroups: 0,
    differenceBaseTotal: 0,
    rows: [],
    reconciled: true,
  };
}

function buildReportStubRows(reportCode, filters) {
  const pagination = resolveReportPagination(filters);
  return {
    reportCode,
    accountFamily: filters.accountFamily || null,
    asOfDate: filters.asOfDate || null,
    legalEntityId: filters.legalEntityId || null,
    fiscalPeriodId: filters.fiscalPeriodId || null,
    windowStartDate: null,
    windowEndDate: null,
    rows: [],
    total: 0,
    limit: pagination.limit,
    offset: pagination.offset,
    summary: buildReportEmptySummary(),
    reconciliation: buildReportEmptyReconciliation(),
    reconciled: true,
    scaffolded: false,
  };
}

function mapSplitRow(row) {
  const shortTermAmountTxn = roundAmount(row.short_term_amount_txn);
  const shortTermAmountBase = roundAmount(row.short_term_amount_base);
  const longTermAmountTxn = roundAmount(row.long_term_amount_txn);
  const longTermAmountBase = roundAmount(row.long_term_amount_base);

  return {
    legalEntityId: parsePositiveInt(row.legal_entity_id),
    currencyCode: asUpper(row.currency_code),
    accountFamily: asUpper(row.account_family),
    shortTermAmountTxn,
    shortTermAmountBase,
    longTermAmountTxn,
    longTermAmountBase,
    totalAmountTxn: roundAmount(shortTermAmountTxn + longTermAmountTxn),
    totalAmountBase: roundAmount(shortTermAmountBase + longTermAmountBase),
    grossMovementAmountTxn: roundAmount(row.gross_movement_amount_txn),
    grossMovementAmountBase: roundAmount(row.gross_movement_amount_base),
    entryCount: Number(row.entry_count || 0),
    journalCount: Number(row.journal_count || 0),
    firstPeriodStartDate: toDateOnlyString(row.first_period_start_date, "firstPeriodStartDate"),
    lastPeriodEndDate: toDateOnlyString(row.last_period_end_date, "lastPeriodEndDate"),
  };
}

function mapRollforwardRow(row) {
  return {
    legalEntityId: parsePositiveInt(row.legal_entity_id),
    currencyCode: asUpper(row.currency_code),
    accountFamily: asUpper(row.account_family),
    openingAmountTxn: roundAmount(row.opening_amount_txn),
    openingAmountBase: roundAmount(row.opening_amount_base),
    movementAmountTxn: roundAmount(row.movement_amount_txn),
    movementAmountBase: roundAmount(row.movement_amount_base),
    closingAmountTxn: roundAmount(row.closing_amount_txn),
    closingAmountBase: roundAmount(row.closing_amount_base),
    closingShortTermAmountTxn: roundAmount(row.closing_short_term_amount_txn),
    closingShortTermAmountBase: roundAmount(row.closing_short_term_amount_base),
    closingLongTermAmountTxn: roundAmount(row.closing_long_term_amount_txn),
    closingLongTermAmountBase: roundAmount(row.closing_long_term_amount_base),
    entryCount: Number(row.entry_count || 0),
    journalCount: Number(row.journal_count || 0),
  };
}

function mapReconciliationRow(row) {
  const differenceBase = roundAmount(row.difference_base);
  return {
    legalEntityId: parsePositiveInt(row.legal_entity_id),
    fiscalPeriodId: parsePositiveInt(row.fiscal_period_id),
    currencyCode: asUpper(row.currency_code),
    accountFamily: asUpper(row.account_family),
    subledgerAmountBase: roundAmount(row.subledger_amount_base),
    glAmountBase: roundAmount(row.gl_amount_base),
    differenceBase,
    journalCount: Number(row.journal_count || 0),
    matches: Math.abs(differenceBase) <= REPORT_RECONCILE_EPSILON,
  };
}

function buildReconciliationPayload(rows) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const matchedGroups = safeRows.filter((row) => row.matches).length;
  const differenceBaseTotal = roundAmount(
    safeRows.reduce((sum, row) => sum + Number(row.differenceBase || 0), 0)
  );
  return {
    totalGroups: safeRows.length,
    matchedGroups,
    unmatchedGroups: safeRows.length - matchedGroups,
    differenceBaseTotal,
    rows: safeRows,
    reconciled: safeRows.every((row) => row.matches),
  };
}

function buildReportResponse({
  reportCode,
  filters,
  rows,
  total,
  limit,
  offset,
  summary,
  reconciliation,
  windowStartDate = null,
  windowEndDate = null,
}) {
  const reconciliationPayload = reconciliation || buildReportEmptyReconciliation();
  return {
    reportCode,
    accountFamily: filters.accountFamily || null,
    asOfDate: filters.asOfDate || null,
    legalEntityId: filters.legalEntityId || null,
    fiscalPeriodId: filters.fiscalPeriodId || null,
    windowStartDate,
    windowEndDate,
    rows: Array.isArray(rows) ? rows : [],
    total: Number(total || 0),
    limit: Number(limit || 0),
    offset: Number(offset || 0),
    summary: summary || buildReportEmptySummary(),
    reconciliation: reconciliationPayload,
    reconciled: Boolean(reconciliationPayload.reconciled),
    scaffolded: false,
  };
}

function toDateOnlyStringLocalSafe(value, label = "date") {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw badRequest(`${label} must be a valid date`);
    }
    const year = value.getFullYear();
    const month = String(value.getMonth() + 1).padStart(2, "0");
    const day = String(value.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }
  return toDateOnlyString(value, label);
}

async function resolveRollforwardWindow({
  tenantId,
  legalEntityId,
  fiscalPeriodId,
  asOfDate,
  runQuery = query,
}) {
  if (fiscalPeriodId) {
    let period = null;
    if (legalEntityId) {
      period = await resolvePeriodScope({
        tenantId,
        legalEntityId,
        fiscalPeriodId,
        runQuery,
      });
    } else {
      const periodResult = await runQuery(
        `SELECT id, start_date, end_date
         FROM fiscal_periods
         WHERE id = ?
         LIMIT 1`,
        [fiscalPeriodId]
      );
      period = periodResult.rows?.[0] || null;
    }

    if (!period) {
      throw badRequest("fiscalPeriodId is not valid in scope");
    }

    const startDate = toDateOnlyStringLocalSafe(period.start_date, "periodStartDate");
    const periodEndDate = toDateOnlyStringLocalSafe(period.end_date, "periodEndDate");
    if (asOfDate && asOfDate < startDate) {
      throw badRequest("asOfDate cannot be earlier than fiscal period start_date");
    }
    const endDate = asOfDate && asOfDate < periodEndDate ? asOfDate : periodEndDate;
    return { startDate, endDate };
  }

  if (asOfDate) {
    return {
      startDate: `${String(asOfDate).slice(0, 4)}-01-01`,
      endDate: asOfDate,
    };
  }

  return {
    startDate: null,
    endDate: null,
  };
}

function buildSubledgerWhereClause({
  req,
  filters,
  buildScopeFilter,
  assertScopeAccess,
  accountFamilies = null,
  alias = "rse",
  includeFiscalPeriod = true,
  includeAsOfDate = true,
  periodEndDateFrom = null,
  periodEndDateTo = null,
}) {
  const conditions = [`${alias}.tenant_id = ?`];
  const params = [filters.tenantId];

  if (typeof buildScopeFilter === "function") {
    conditions.push(buildScopeFilter(req, "legal_entity", `${alias}.legal_entity_id`, params));
  }

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push(`${alias}.legal_entity_id = ?`);
    params.push(filters.legalEntityId);
  }
  if (includeFiscalPeriod && filters.fiscalPeriodId) {
    conditions.push(`${alias}.fiscal_period_id = ?`);
    params.push(filters.fiscalPeriodId);
  }
  if (includeAsOfDate && filters.asOfDate) {
    conditions.push(`${alias}.period_end_date <= ?`);
    params.push(filters.asOfDate);
  }
  if (periodEndDateFrom) {
    conditions.push(`${alias}.period_end_date >= ?`);
    params.push(periodEndDateFrom);
  }
  if (periodEndDateTo) {
    conditions.push(`${alias}.period_end_date <= ?`);
    params.push(periodEndDateTo);
  }

  const families = normalizeReportFamilyFilter(filters.accountFamily, accountFamilies);
  if (families.length === 0) {
    return {
      whereSql: "1 = 0",
      params: [],
      families: [],
    };
  }
  if (families.length === 1) {
    conditions.push(`${alias}.account_family = ?`);
    params.push(families[0]);
  } else {
    conditions.push(`${alias}.account_family IN (${families.map(() => "?").join(", ")})`);
    params.push(...families);
  }

  return {
    whereSql: conditions.join(" AND "),
    params,
    families,
  };
}

async function loadRevenueReconciliationRows({
  req,
  filters,
  buildScopeFilter,
  assertScopeAccess,
  accountFamilies = null,
  includeFiscalPeriod = true,
  includeAsOfDate = true,
  periodEndDateFrom = null,
  periodEndDateTo = null,
}) {
  const whereBundle = buildSubledgerWhereClause({
    req,
    filters,
    buildScopeFilter,
    assertScopeAccess,
    accountFamilies,
    includeFiscalPeriod,
    includeAsOfDate,
    periodEndDateFrom,
    periodEndDateTo,
  });
  if (whereBundle.families.length === 0) {
    return buildReportEmptyReconciliation();
  }

  const result = await query(
    `SELECT
        journal_agg.legal_entity_id,
        journal_agg.fiscal_period_id,
        journal_agg.currency_code,
        journal_agg.account_family,
        SUM(journal_agg.subledger_amount_base_signed) AS subledger_amount_base,
        SUM(journal_agg.gl_amount_base_signed) AS gl_amount_base,
        SUM(journal_agg.subledger_amount_base_signed - journal_agg.gl_amount_base_signed) AS difference_base,
        COUNT(*) AS journal_count
     FROM (
       SELECT
         rse.legal_entity_id,
         rse.fiscal_period_id,
         rse.currency_code,
         rse.account_family,
         rse.posted_journal_entry_id,
         SUM(${signedAmountSql("rse", "amount_base")}) AS subledger_amount_base_signed,
         (
           CASE
             WHEN MAX(CASE WHEN rse.entry_kind = 'REVERSAL' THEN 1 ELSE 0 END) = 1
              AND MAX(CASE WHEN rse.entry_kind <> 'REVERSAL' THEN 1 ELSE 0 END) = 0
             THEN -1
             ELSE 1
           END
         ) * COALESCE(MAX(je.total_debit_base), 0) AS gl_amount_base_signed
       FROM revenue_recognition_subledger_entries rse
       LEFT JOIN journal_entries je
         ON je.tenant_id = rse.tenant_id
        AND je.id = rse.posted_journal_entry_id
       WHERE ${whereBundle.whereSql}
       GROUP BY
         rse.legal_entity_id,
         rse.fiscal_period_id,
         rse.currency_code,
         rse.account_family,
         rse.posted_journal_entry_id
     ) AS journal_agg
     GROUP BY
       journal_agg.legal_entity_id,
       journal_agg.fiscal_period_id,
       journal_agg.currency_code,
       journal_agg.account_family
     ORDER BY
       journal_agg.legal_entity_id ASC,
       journal_agg.fiscal_period_id ASC,
       journal_agg.currency_code ASC,
       journal_agg.account_family ASC`,
    whereBundle.params
  );

  return buildReconciliationPayload((result.rows || []).map((row) => mapReconciliationRow(row)));
}

async function loadRevenueSplitReport({
  req,
  filters,
  assertScopeAccess,
  buildScopeFilter,
  reportCode,
  allowedFamilies,
}) {
  const { limit, offset } = resolveReportPagination(filters);
  const whereBundle = buildSubledgerWhereClause({
    req,
    filters,
    buildScopeFilter,
    assertScopeAccess,
    accountFamilies: allowedFamilies,
  });
  if (whereBundle.families.length === 0) {
    const empty = buildReportStubRows(reportCode, { ...filters, limit, offset });
    return {
      ...empty,
      accountFamily:
        whereBundle.families.length === 1 ? whereBundle.families[0] : filters.accountFamily || null,
    };
  }

  const totalResult = await query(
    `SELECT COUNT(*) AS total
     FROM (
       SELECT 1
       FROM revenue_recognition_subledger_entries rse
       WHERE ${whereBundle.whereSql}
       GROUP BY
         rse.legal_entity_id,
         rse.currency_code,
         rse.account_family
     ) AS grouped_rows`,
    whereBundle.params
  );
  const total = Number(totalResult.rows?.[0]?.total || 0);

  const rowsResult = await query(
    `SELECT
        rse.legal_entity_id,
        rse.currency_code,
        rse.account_family,
        COALESCE(SUM(${shortBucketAmountSql("rse", "amount_txn")}), 0) AS short_term_amount_txn,
        COALESCE(SUM(${shortBucketAmountSql("rse", "amount_base")}), 0) AS short_term_amount_base,
        COALESCE(SUM(${longBucketAmountSql("rse", "amount_txn")}), 0) AS long_term_amount_txn,
        COALESCE(SUM(${longBucketAmountSql("rse", "amount_base")}), 0) AS long_term_amount_base,
        COALESCE(SUM(${signedAmountSql("rse", "amount_txn")}), 0) AS gross_movement_amount_txn,
        COALESCE(SUM(${signedAmountSql("rse", "amount_base")}), 0) AS gross_movement_amount_base,
        COUNT(*) AS entry_count,
        COUNT(DISTINCT rse.posted_journal_entry_id) AS journal_count,
        MIN(rse.period_start_date) AS first_period_start_date,
        MAX(rse.period_end_date) AS last_period_end_date
     FROM revenue_recognition_subledger_entries rse
     WHERE ${whereBundle.whereSql}
     GROUP BY
       rse.legal_entity_id,
       rse.currency_code,
       rse.account_family
     ORDER BY
       rse.legal_entity_id ASC,
       rse.currency_code ASC,
       rse.account_family ASC
     LIMIT ${limit} OFFSET ${offset}`,
    whereBundle.params
  );

  const summaryResult = await query(
    `SELECT
        COALESCE(SUM(${shortBucketAmountSql("rse", "amount_txn")}), 0) AS short_term_amount_txn,
        COALESCE(SUM(${shortBucketAmountSql("rse", "amount_base")}), 0) AS short_term_amount_base,
        COALESCE(SUM(${longBucketAmountSql("rse", "amount_txn")}), 0) AS long_term_amount_txn,
        COALESCE(SUM(${longBucketAmountSql("rse", "amount_base")}), 0) AS long_term_amount_base,
        COALESCE(SUM(${signedAmountSql("rse", "amount_txn")}), 0) AS gross_movement_amount_txn,
        COALESCE(SUM(${signedAmountSql("rse", "amount_base")}), 0) AS gross_movement_amount_base,
        COUNT(*) AS entry_count,
        COUNT(DISTINCT rse.posted_journal_entry_id) AS journal_count
     FROM revenue_recognition_subledger_entries rse
     WHERE ${whereBundle.whereSql}`,
    whereBundle.params
  );
  const summaryRow = summaryResult.rows?.[0] || {};
  const shortTermAmountTxn = roundAmount(summaryRow.short_term_amount_txn);
  const shortTermAmountBase = roundAmount(summaryRow.short_term_amount_base);
  const longTermAmountTxn = roundAmount(summaryRow.long_term_amount_txn);
  const longTermAmountBase = roundAmount(summaryRow.long_term_amount_base);
  const summary = {
    ...buildReportEmptySummary(),
    shortTermAmountTxn,
    shortTermAmountBase,
    longTermAmountTxn,
    longTermAmountBase,
    totalAmountTxn: roundAmount(shortTermAmountTxn + longTermAmountTxn),
    totalAmountBase: roundAmount(shortTermAmountBase + longTermAmountBase),
    grossMovementAmountTxn: roundAmount(summaryRow.gross_movement_amount_txn),
    grossMovementAmountBase: roundAmount(summaryRow.gross_movement_amount_base),
    entryCount: Number(summaryRow.entry_count || 0),
    journalCount: Number(summaryRow.journal_count || 0),
  };

  const reconciliation = await loadRevenueReconciliationRows({
    req,
    filters,
    buildScopeFilter,
    assertScopeAccess,
    accountFamilies: allowedFamilies,
  });

  return buildReportResponse({
    reportCode,
    filters,
    rows: (rowsResult.rows || []).map((row) => mapSplitRow(row)),
    total,
    limit,
    offset,
    summary,
    reconciliation,
  });
}

export async function resolveRevenueRunScope(runId, tenantId) {
  const parsedRunId = parsePositiveInt(runId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedRunId || !parsedTenantId) {
    return null;
  }

  const result = await query(
    `SELECT legal_entity_id
     FROM revenue_recognition_runs
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [parsedTenantId, parsedRunId]
  );
  const row = result.rows?.[0] || null;
  if (!row) {
    return null;
  }

  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: Number(row.legal_entity_id),
  };
}

export async function listRevenueRecognitionSchedules({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["rrs.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "rrs.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("rrs.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }
  if (filters.fiscalPeriodId) {
    conditions.push("rrs.fiscal_period_id = ?");
    params.push(filters.fiscalPeriodId);
  }
  if (filters.accountFamily) {
    conditions.push("rrs.account_family = ?");
    params.push(filters.accountFamily);
  }
  if (filters.status) {
    conditions.push("rrs.status = ?");
    params.push(filters.status);
  }
  if (filters.q) {
    conditions.push("rrs.source_event_uid LIKE ?");
    params.push(`%${filters.q}%`);
  }

  const whereSql = conditions.join(" AND ");
  const totalResult = await query(
    `SELECT COUNT(*) AS total
     FROM revenue_recognition_schedules rrs
     WHERE ${whereSql}`,
    params
  );
  const total = Number(totalResult.rows?.[0]?.total || 0);
  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset =
    Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;

  const listResult = await query(
    `SELECT
        rrs.id,
        rrs.tenant_id,
        rrs.legal_entity_id,
        rrs.fiscal_period_id,
        rrs.source_event_uid,
        rrs.status,
        rrs.account_family,
        rrs.maturity_bucket,
        rrs.maturity_date,
        rrs.reclass_required,
        rrs.currency_code,
        rrs.fx_rate,
        rrs.amount_txn,
        rrs.amount_base,
        rrs.period_start_date,
        rrs.period_end_date,
        rrs.created_by_user_id,
        rrs.posted_journal_entry_id,
        rrs.created_at,
        rrs.updated_at,
        (
          SELECT COUNT(*)
          FROM revenue_recognition_schedule_lines rrsl
          WHERE rrsl.tenant_id = rrs.tenant_id
            AND rrsl.schedule_id = rrs.id
        ) AS line_count
     FROM revenue_recognition_schedules rrs
     WHERE ${whereSql}
     ORDER BY rrs.updated_at DESC, rrs.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: (listResult.rows || []).map((row) => mapScheduleRow(row)),
    total,
    limit: safeLimit,
    offset: safeOffset,
  };
}

export async function generateRevenueRecognitionSchedule({
  req,
  payload,
  assertScopeAccess,
}) {
  assertScopeAccess(req, "legal_entity", payload.legalEntityId, "legalEntityId");
  const sourceEventUid = payload.sourceEventUid || buildDefaultScheduleSourceUid(payload);

  try {
    return await withTransaction(async (tx) => {
      await assertLegalEntityExists({
        tenantId: payload.tenantId,
        legalEntityId: payload.legalEntityId,
        runQuery: tx.query,
      });
      await assertCurrencyExists({
        currencyCode: payload.currencyCode,
        runQuery: tx.query,
      });

      const period = await resolvePeriodScope({
        tenantId: payload.tenantId,
        legalEntityId: payload.legalEntityId,
        fiscalPeriodId: payload.fiscalPeriodId,
        runQuery: tx.query,
      });
      if (!period) {
        throw badRequest("fiscalPeriodId is not valid in legalEntity scope");
      }

      const scheduleInsert = await tx.query(
        `INSERT INTO revenue_recognition_schedules (
            tenant_id,
            legal_entity_id,
            fiscal_period_id,
            source_event_uid,
            status,
            account_family,
            maturity_bucket,
            maturity_date,
            reclass_required,
            currency_code,
            fx_rate,
            amount_txn,
            amount_base,
            period_start_date,
            period_end_date,
            created_by_user_id
         )
         VALUES (?, ?, ?, ?, 'READY', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          payload.tenantId,
          payload.legalEntityId,
          payload.fiscalPeriodId,
          sourceEventUid,
          payload.accountFamily,
          payload.maturityBucket,
          payload.maturityDate,
          payload.reclassRequired ? 1 : 0,
          payload.currencyCode,
          payload.fxRate,
          payload.amountTxn,
          payload.amountBase,
          period.start_date,
          period.end_date,
          payload.userId,
        ]
      );

      const scheduleId = parsePositiveInt(scheduleInsert.rows?.insertId);
      if (!scheduleId) {
        throw new Error("Failed to create revenue recognition schedule");
      }

      const sourceRowUid = buildDeterministicUid("SCHED_LINE", [sourceEventUid, 1]);
      await tx.query(
        `INSERT INTO revenue_recognition_schedule_lines (
            tenant_id,
            legal_entity_id,
            schedule_id,
            line_no,
            source_row_uid,
            status,
            account_family,
            maturity_bucket,
            maturity_date,
            reclass_required,
            currency_code,
            fx_rate,
            amount_txn,
            amount_base,
            period_start_date,
            period_end_date,
            fiscal_period_id,
            created_by_user_id
         )
         VALUES (?, ?, ?, 1, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          payload.tenantId,
          payload.legalEntityId,
          scheduleId,
          sourceRowUid,
          payload.accountFamily,
          payload.maturityBucket,
          payload.maturityDate,
          payload.reclassRequired ? 1 : 0,
          payload.currencyCode,
          payload.fxRate,
          payload.amountTxn,
          payload.amountBase,
          period.start_date,
          period.end_date,
          payload.fiscalPeriodId,
          payload.userId,
        ]
      );

      const created = await fetchScheduleRow({
        tenantId: payload.tenantId,
        scheduleId,
        runQuery: tx.query,
      });
      if (!created) {
        throw new Error("Schedule create readback failed");
      }
      return mapScheduleRow(created);
    });
  } catch (err) {
    if (isDuplicateKeyError(err, "uk_revrec_sched_source_uid")) {
      throw badRequest("sourceEventUid already exists in legalEntity scope");
    }
    if (isDuplicateKeyError(err, "uk_revrec_sched_line_source_uid")) {
      throw badRequest("Schedule line source identity already exists");
    }
    throw err;
  }
}

function buildContractLineRevrecBuckets({
  lineRow,
  periodStartDate,
  periodEndDate,
}) {
  const recognitionMethod = asUpper(lineRow?.recognition_method);
  const lineAmountTxn = Number(lineRow?.line_amount_txn || 0);
  const lineAmountBase = Number(lineRow?.line_amount_base || 0);
  if (!Number.isFinite(lineAmountTxn) || !Number.isFinite(lineAmountBase)) {
    throw badRequest("Contract line amounts must be numeric for RevRec generation");
  }
  if (Math.abs(lineAmountTxn) <= JOURNAL_BALANCE_EPSILON) {
    throw badRequest("Contract line amount_txn must be non-zero for RevRec generation");
  }
  if (Math.abs(lineAmountBase) <= JOURNAL_BALANCE_EPSILON) {
    throw badRequest("Contract line amount_base must be non-zero for RevRec generation");
  }

  if (recognitionMethod === "MANUAL") {
    return {
      reason: "MANUAL recognition_method is excluded from auto generation",
      allBuckets: [],
      inPeriodBuckets: [],
    };
  }

  let bucketDates = [];
  if (recognitionMethod === "STRAIGHT_LINE") {
    const recognitionStartDate = toDateOnlyString(
      lineRow?.recognition_start_date,
      "recognitionStartDate"
    );
    const recognitionEndDate = toDateOnlyString(
      lineRow?.recognition_end_date,
      "recognitionEndDate"
    );
    if (!recognitionStartDate || !recognitionEndDate) {
      throw badRequest(
        "STRAIGHT_LINE contract lines require recognition_start_date and recognition_end_date"
      );
    }
    bucketDates = enumerateMonthEndDatesInRange(recognitionStartDate, recognitionEndDate);
  } else if (recognitionMethod === "MILESTONE") {
    const recognitionStartDate = toDateOnlyString(
      lineRow?.recognition_start_date,
      "recognitionStartDate"
    );
    const recognitionEndDate = toDateOnlyString(
      lineRow?.recognition_end_date,
      "recognitionEndDate"
    );
    if (!recognitionStartDate || !recognitionEndDate) {
      throw badRequest(
        "MILESTONE contract lines require recognition_start_date and recognition_end_date"
      );
    }
    if (recognitionStartDate !== recognitionEndDate) {
      throw badRequest(
        "MILESTONE contract lines require recognition_start_date and recognition_end_date to match"
      );
    }
    bucketDates = [recognitionStartDate];
  } else {
    throw badRequest(`Unsupported recognition_method for RevRec: ${recognitionMethod}`);
  }

  if (bucketDates.length === 0) {
    return {
      reason: "No recognition buckets resolved",
      allBuckets: [],
      inPeriodBuckets: [],
    };
  }

  const bucketAmountTxn = splitAmountAcrossBuckets(
    lineAmountTxn,
    bucketDates.length,
    "lineAmountTxn"
  );
  const bucketAmountBase = splitAmountAcrossBuckets(
    lineAmountBase,
    bucketDates.length,
    "lineAmountBase"
  );
  const normalizedPeriodStartDate = toDateOnlyString(periodStartDate, "periodStartDate");
  const normalizedPeriodEndDate = toDateOnlyString(periodEndDate, "periodEndDate");

  const allBuckets = bucketDates.map((bucketDate, index) => {
    const amountTxn = Number(bucketAmountTxn[index] || 0);
    const amountBase = Number(bucketAmountBase[index] || 0);
    const maturityBucket = resolveMaturityBucketForDate({
      maturityDate: bucketDate,
      periodEndDate: normalizedPeriodEndDate,
    });
    return {
      maturityDate: bucketDate,
      maturityBucket,
      reclassRequired: maturityBucket === "LONG_TERM",
      amountTxn: Number(amountTxn.toFixed(6)),
      amountBase: Number(amountBase.toFixed(6)),
      fxRate: resolveFxRateFromAmounts(amountTxn, amountBase),
    };
  });

  const inPeriodBuckets = allBuckets.filter(
    (bucket) =>
      bucket.maturityDate >= normalizedPeriodStartDate &&
      bucket.maturityDate <= normalizedPeriodEndDate
  );

  return {
    reason: null,
    allBuckets,
    inPeriodBuckets,
  };
}

export async function generateRevenueRecognitionSchedulesFromContract({
  payload,
  runQuery = query,
}) {
  const normalizedMode =
    asUpper(payload?.generationMode) === "BY_LINKED_DOCUMENT"
      ? "BY_LINKED_DOCUMENT"
      : "BY_CONTRACT_LINE";
  const regenerateMissingOnly = payload?.regenerateMissingOnly !== false;
  const normalizedScheduleStatus = asUpper(payload?.scheduleStatus || "DRAFT");
  if (!["DRAFT", "READY"].includes(normalizedScheduleStatus)) {
    throw badRequest("scheduleStatus must be DRAFT or READY");
  }
  const accountFamily = asUpper(payload?.accountFamily);
  if (!PR17_POSTABLE_FAMILIES.has(accountFamily)) {
    throw badRequest(`Unsupported accountFamily for contract RevRec generation: ${accountFamily}`);
  }

  const lineRows = Array.isArray(payload?.lineRows) ? payload.lineRows : [];
  if (lineRows.length === 0) {
    throw badRequest("No contract lines selected for RevRec generation");
  }

  await assertLegalEntityExists({
    tenantId: payload.tenantId,
    legalEntityId: payload.legalEntityId,
    runQuery,
  });
  await assertCurrencyExists({
    currencyCode: payload.currencyCode,
    runQuery,
  });

  const period = await resolvePeriodScope({
    tenantId: payload.tenantId,
    legalEntityId: payload.legalEntityId,
    fiscalPeriodId: payload.fiscalPeriodId,
    runQuery,
  });
  if (!period) {
    throw badRequest("fiscalPeriodId is not valid in legalEntity scope");
  }

  const periodStartDate = toDateOnlyString(period.start_date, "periodStartDate");
  const periodEndDate = toDateOnlyString(period.end_date, "periodEndDate");
  let generatedScheduleCount = 0;
  let generatedLineCount = 0;
  let skippedLineCount = 0;
  let idempotentReplay = true;
  const scheduleById = new Map();

  for (const lineRow of lineRows) {
    const contractLineId = parsePositiveInt(lineRow?.id);
    if (!contractLineId) {
      throw badRequest("Selected contract line has invalid id");
    }

    const bucketPlan = buildContractLineRevrecBuckets({
      lineRow,
      periodStartDate,
      periodEndDate,
    });
    if (!Array.isArray(bucketPlan.inPeriodBuckets) || bucketPlan.inPeriodBuckets.length === 0) {
      skippedLineCount += 1;
      continue;
    }

    const scheduleSourceUid = buildContractRevrecScheduleSourceUid({
      contractId: payload.contractId,
      contractLineId,
      fiscalPeriodId: payload.fiscalPeriodId,
      generationMode: normalizedMode,
      sourceCariDocumentId: payload.sourceCariDocumentId,
    });

    let schedule = await fetchScheduleRowBySourceEventUid({
      tenantId: payload.tenantId,
      legalEntityId: payload.legalEntityId,
      sourceEventUid: scheduleSourceUid,
      runQuery,
      forUpdate: true,
    });

    let scheduleId = parsePositiveInt(schedule?.id);
    if (!scheduleId) {
      const firstBucket = bucketPlan.inPeriodBuckets[0];
      const maturityDate = bucketPlan.inPeriodBuckets.reduce(
        (maxDate, bucket) => (bucket.maturityDate > maxDate ? bucket.maturityDate : maxDate),
        firstBucket.maturityDate
      );
      const hasLongTermBucket = bucketPlan.inPeriodBuckets.some(
        (bucket) => bucket.maturityBucket === "LONG_TERM"
      );
      const hasReclassRequired = bucketPlan.inPeriodBuckets.some((bucket) => bucket.reclassRequired);
      const totalAmountTxn = bucketPlan.inPeriodBuckets.reduce(
        (sum, bucket) => sum + Number(bucket.amountTxn || 0),
        0
      );
      const totalAmountBase = bucketPlan.inPeriodBuckets.reduce(
        (sum, bucket) => sum + Number(bucket.amountBase || 0),
        0
      );
      const fxRate = resolveFxRateFromAmounts(totalAmountTxn, totalAmountBase);

      try {
        const insertResult = await runQuery(
          `INSERT INTO revenue_recognition_schedules (
              tenant_id,
              legal_entity_id,
              fiscal_period_id,
              source_event_uid,
              status,
              account_family,
              maturity_bucket,
              maturity_date,
              reclass_required,
              currency_code,
              fx_rate,
              amount_txn,
              amount_base,
              period_start_date,
              period_end_date,
              created_by_user_id
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            payload.tenantId,
            payload.legalEntityId,
            payload.fiscalPeriodId,
            scheduleSourceUid,
            normalizedScheduleStatus,
            accountFamily,
            hasLongTermBucket ? "LONG_TERM" : "SHORT_TERM",
            maturityDate,
            hasReclassRequired ? 1 : 0,
            asUpper(payload.currencyCode),
            fxRate,
            Number(totalAmountTxn.toFixed(6)),
            Number(totalAmountBase.toFixed(6)),
            periodStartDate,
            periodEndDate,
            payload.userId,
          ]
        );
        scheduleId = parsePositiveInt(insertResult.rows?.insertId);
      } catch (err) {
        if (!isDuplicateKeyError(err, "uk_revrec_sched_source_uid")) {
          throw err;
        }
        const replaySchedule = await fetchScheduleRowBySourceEventUid({
          tenantId: payload.tenantId,
          legalEntityId: payload.legalEntityId,
          sourceEventUid: scheduleSourceUid,
          runQuery,
          forUpdate: true,
        });
        scheduleId = parsePositiveInt(replaySchedule?.id);
      }

      if (!scheduleId) {
        throw new Error("Failed to create/fetch contract RevRec schedule");
      }
      schedule = await fetchScheduleRow({
        tenantId: payload.tenantId,
        scheduleId,
        runQuery,
        forUpdate: true,
      });
      generatedScheduleCount += 1;
      idempotentReplay = false;
    } else {
      if (parsePositiveInt(schedule.fiscal_period_id) !== payload.fiscalPeriodId) {
        throw badRequest(
          `RevRec schedule source collision for contractLineId=${contractLineId} and fiscalPeriodId mismatch`
        );
      }
      if (asUpper(schedule.account_family) !== accountFamily) {
        throw badRequest(
          `RevRec schedule accountFamily mismatch for contractLineId=${contractLineId}`
        );
      }
    }

    if (!scheduleId) {
      throw new Error("RevRec schedule id not resolved");
    }

    const scheduleStatus = asUpper(schedule?.status);
    if (!["DRAFT", "READY", "POSTED", "REVERSED"].includes(scheduleStatus)) {
      throw badRequest(`Unsupported schedule status: ${scheduleStatus}`);
    }

    const { sourceUidSet, maxLineNo } = await fetchScheduleLineSourceSetAndMaxLineNo({
      tenantId: payload.tenantId,
      legalEntityId: payload.legalEntityId,
      scheduleId,
      runQuery,
    });
    let nextLineNo = maxLineNo;

    for (const bucket of bucketPlan.inPeriodBuckets) {
      const lineSourceUid = buildContractRevrecLineSourceUid({
        scheduleSourceUid,
        maturityDate: bucket.maturityDate,
      });
      if (sourceUidSet.has(lineSourceUid)) {
        if (!regenerateMissingOnly) {
          throw badRequest(
            `Duplicate RevRec schedule bucket exists for contractLineId=${contractLineId}, maturityDate=${bucket.maturityDate}`
          );
        }
        skippedLineCount += 1;
        continue;
      }
      if (!["DRAFT", "READY"].includes(scheduleStatus)) {
        throw badRequest(
          `Schedule ${scheduleId} is ${scheduleStatus}; cannot append missing contract RevRec lines`
        );
      }

      nextLineNo += 1;
      try {
        await runQuery(
          `INSERT INTO revenue_recognition_schedule_lines (
              tenant_id,
              legal_entity_id,
              schedule_id,
              line_no,
              source_row_uid,
              source_contract_id,
              source_contract_line_id,
              source_cari_document_id,
              status,
              account_family,
              maturity_bucket,
              maturity_date,
              reclass_required,
              currency_code,
              fx_rate,
              amount_txn,
              amount_base,
              period_start_date,
              period_end_date,
              fiscal_period_id,
              created_by_user_id
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            payload.tenantId,
            payload.legalEntityId,
            scheduleId,
            nextLineNo,
            lineSourceUid,
            payload.contractId,
            contractLineId,
            payload.sourceCariDocumentId || null,
            accountFamily,
            bucket.maturityBucket,
            bucket.maturityDate,
            bucket.reclassRequired ? 1 : 0,
            asUpper(payload.currencyCode),
            bucket.fxRate,
            Number(bucket.amountTxn || 0).toFixed(6),
            Number(bucket.amountBase || 0).toFixed(6),
            periodStartDate,
            periodEndDate,
            payload.fiscalPeriodId,
            payload.userId,
          ]
        );
      } catch (err) {
        if (!isDuplicateKeyError(err, "uk_revrec_sched_line_source_uid")) {
          throw err;
        }
        if (!regenerateMissingOnly) {
          throw badRequest(
            `Duplicate RevRec schedule bucket exists for contractLineId=${contractLineId}, maturityDate=${bucket.maturityDate}`
          );
        }
        skippedLineCount += 1;
        nextLineNo -= 1;
        continue;
      }
      sourceUidSet.add(lineSourceUid);
      generatedLineCount += 1;
      idempotentReplay = false;
    }

    await refreshScheduleAggregates({
      tenantId: payload.tenantId,
      legalEntityId: payload.legalEntityId,
      scheduleId,
      targetStatus: normalizedScheduleStatus,
      runQuery,
    });

    const updatedSchedule = await fetchScheduleRow({
      tenantId: payload.tenantId,
      scheduleId,
      runQuery,
    });
    if (updatedSchedule) {
      scheduleById.set(parsePositiveInt(updatedSchedule.id), mapScheduleRow(updatedSchedule));
    }
  }

  if (scheduleById.size === 0) {
    throw badRequest(
      "No contract recognition buckets found in the selected fiscal period for RevRec generation"
    );
  }

  return {
    idempotentReplay: Boolean(idempotentReplay),
    generationMode: normalizedMode,
    accountFamily,
    sourceCariDocumentId: payload.sourceCariDocumentId || null,
    generatedScheduleCount,
    generatedLineCount,
    skippedLineCount,
    rows: Array.from(scheduleById.values()),
  };
}

export async function listRevenueRecognitionRuns({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["rrr.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "rrr.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("rrr.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }
  if (filters.fiscalPeriodId) {
    conditions.push("rrr.fiscal_period_id = ?");
    params.push(filters.fiscalPeriodId);
  }
  if (filters.accountFamily) {
    conditions.push("rrr.account_family = ?");
    params.push(filters.accountFamily);
  }
  if (filters.status) {
    conditions.push("rrr.status = ?");
    params.push(filters.status);
  }
  if (filters.q) {
    conditions.push("(rrr.run_no LIKE ? OR rrr.source_run_uid LIKE ?)");
    const like = `%${filters.q}%`;
    params.push(like, like);
  }

  const whereSql = conditions.join(" AND ");
  const totalResult = await query(
    `SELECT COUNT(*) AS total
     FROM revenue_recognition_runs rrr
     WHERE ${whereSql}`,
    params
  );
  const total = Number(totalResult.rows?.[0]?.total || 0);
  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset =
    Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;

  const listResult = await query(
    `SELECT
        rrr.id,
        rrr.tenant_id,
        rrr.legal_entity_id,
        rrr.schedule_id,
        rrr.fiscal_period_id,
        rrr.run_no,
        rrr.source_run_uid,
        rrr.status,
        rrr.account_family,
        rrr.maturity_bucket,
        rrr.maturity_date,
        rrr.reclass_required,
        rrr.currency_code,
        rrr.fx_rate,
        rrr.total_amount_txn,
        rrr.total_amount_base,
        rrr.period_start_date,
        rrr.period_end_date,
        rrr.reversal_of_run_id,
        rrr.posted_journal_entry_id,
        rrr.reversal_journal_entry_id,
        rrr.created_by_user_id,
        rrr.posted_by_user_id,
        rrr.reversed_by_user_id,
        rrr.posted_at,
        rrr.reversed_at,
        rrr.created_at,
        rrr.updated_at,
        (
          SELECT COUNT(*)
          FROM revenue_recognition_run_lines rrrl
          WHERE rrrl.tenant_id = rrr.tenant_id
            AND rrrl.run_id = rrr.id
        ) AS line_count
     FROM revenue_recognition_runs rrr
     WHERE ${whereSql}
     ORDER BY rrr.updated_at DESC, rrr.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: (listResult.rows || []).map((row) => mapRunRow(row)),
    total,
    limit: safeLimit,
    offset: safeOffset,
  };
}

export async function createRevenueRecognitionRun({
  req,
  payload,
  assertScopeAccess,
}) {
  assertScopeAccess(req, "legal_entity", payload.legalEntityId, "legalEntityId");
  const sourceRunUid = payload.sourceRunUid || buildDefaultRunSourceUid(payload);
  const runNo = payload.runNo || buildDefaultRunNo(payload);

  try {
    return await withTransaction(async (tx) => {
      await assertLegalEntityExists({
        tenantId: payload.tenantId,
        legalEntityId: payload.legalEntityId,
        runQuery: tx.query,
      });
      await assertCurrencyExists({
        currencyCode: payload.currencyCode,
        runQuery: tx.query,
      });

      const period = await resolvePeriodScope({
        tenantId: payload.tenantId,
        legalEntityId: payload.legalEntityId,
        fiscalPeriodId: payload.fiscalPeriodId,
        runQuery: tx.query,
      });
      if (!period) {
        throw badRequest("fiscalPeriodId is not valid in legalEntity scope");
      }

      let scheduleLines = [];
      if (payload.scheduleId) {
        const schedule = await fetchScheduleRow({
          tenantId: payload.tenantId,
          scheduleId: payload.scheduleId,
          runQuery: tx.query,
        });
        if (!schedule) {
          throw badRequest("scheduleId not found for tenant");
        }
        if (parsePositiveInt(schedule.legal_entity_id) !== payload.legalEntityId) {
          throw badRequest("scheduleId must belong to legalEntityId");
        }
        scheduleLines = await fetchScheduleLines({
          tenantId: payload.tenantId,
          legalEntityId: payload.legalEntityId,
          scheduleId: payload.scheduleId,
          runQuery: tx.query,
        });

        const scheduleFamily = asUpper(schedule.account_family);
        if (scheduleFamily && scheduleFamily !== asUpper(payload.accountFamily)) {
          throw badRequest(
            `scheduleId accountFamily=${scheduleFamily} must match run accountFamily=${asUpper(
              payload.accountFamily
            )}`
          );
        }

        await assertNoDuplicateOpenRunLinesTx({
          tenantId: payload.tenantId,
          legalEntityId: payload.legalEntityId,
          scheduleLineIds: scheduleLines
            .map((line) => parsePositiveInt(line.id))
            .filter((id) => Boolean(id)),
          runQuery: tx.query,
        });
      }

      const runInsert = await tx.query(
        `INSERT INTO revenue_recognition_runs (
            tenant_id,
            legal_entity_id,
            schedule_id,
            fiscal_period_id,
            run_no,
            source_run_uid,
            status,
            account_family,
            maturity_bucket,
            maturity_date,
            reclass_required,
            currency_code,
            fx_rate,
            total_amount_txn,
            total_amount_base,
            period_start_date,
            period_end_date,
            created_by_user_id
         )
         VALUES (?, ?, ?, ?, ?, ?, 'DRAFT', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          payload.tenantId,
          payload.legalEntityId,
          payload.scheduleId,
          payload.fiscalPeriodId,
          runNo,
          sourceRunUid,
          payload.accountFamily,
          payload.maturityBucket,
          payload.maturityDate,
          payload.reclassRequired ? 1 : 0,
          payload.currencyCode,
          payload.fxRate,
          payload.totalAmountTxn,
          payload.totalAmountBase,
          period.start_date,
          period.end_date,
          payload.userId,
        ]
      );

      const runId = parsePositiveInt(runInsert.rows?.insertId);
      if (!runId) {
        throw new Error("Failed to create revenue recognition run");
      }

      if (scheduleLines.length > 0) {
        for (let index = 0; index < scheduleLines.length; index += 1) {
          const line = scheduleLines[index];
          await tx.query(
            `INSERT INTO revenue_recognition_run_lines (
                tenant_id,
                legal_entity_id,
                run_id,
                schedule_line_id,
                line_no,
                source_row_uid,
                status,
                account_family,
                maturity_bucket,
                maturity_date,
                reclass_required,
                currency_code,
                fx_rate,
                amount_txn,
                amount_base,
                fiscal_period_id,
                period_start_date,
                period_end_date
             )
             VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              payload.tenantId,
              payload.legalEntityId,
              runId,
              line.id,
              index + 1,
              buildDeterministicUid("RUN_LINE", [sourceRunUid, line.id, index + 1]),
              line.account_family,
              line.maturity_bucket,
              toDateOnlyString(line.maturity_date, "maturityDate"),
              toBoolean(line.reclass_required) ? 1 : 0,
              line.currency_code,
              line.fx_rate,
              line.amount_txn,
              line.amount_base,
              line.fiscal_period_id,
              toDateOnlyString(line.period_start_date, "periodStartDate"),
              toDateOnlyString(line.period_end_date, "periodEndDate"),
            ]
          );
        }
      } else {
        await tx.query(
          `INSERT INTO revenue_recognition_run_lines (
              tenant_id,
              legal_entity_id,
              run_id,
              schedule_line_id,
              line_no,
              source_row_uid,
              status,
              account_family,
              maturity_bucket,
              maturity_date,
              reclass_required,
              currency_code,
              fx_rate,
              amount_txn,
              amount_base,
              fiscal_period_id,
              period_start_date,
              period_end_date
           )
           VALUES (?, ?, ?, NULL, 1, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            payload.tenantId,
            payload.legalEntityId,
            runId,
            buildDeterministicUid("RUN_LINE", [sourceRunUid, 1]),
            payload.accountFamily,
            payload.maturityBucket,
            payload.maturityDate,
            payload.reclassRequired ? 1 : 0,
            payload.currencyCode,
            payload.fxRate,
            payload.totalAmountTxn,
            payload.totalAmountBase,
            payload.fiscalPeriodId,
            period.start_date,
            period.end_date,
          ]
        );
      }

      const created = await fetchRunRow({
        tenantId: payload.tenantId,
        runId,
        runQuery: tx.query,
      });
      if (!created) {
        throw new Error("Run create readback failed");
      }
      return mapRunRow(created);
    });
  } catch (err) {
    if (isDuplicateKeyError(err, "uk_revrec_runs_source_uid")) {
      throw badRequest("sourceRunUid already exists in legalEntity scope");
    }
    if (isDuplicateKeyError(err, "uk_revrec_runs_run_no")) {
      throw badRequest("runNo must be unique in legalEntity scope");
    }
    if (isDuplicateKeyError(err, "uk_revrec_run_line_source_uid")) {
      throw badRequest("Run line source identity already exists");
    }
    throw err;
  }
}

export async function generateRevenueAccrual({
  req,
  payload,
  assertScopeAccess,
}) {
  assertAccrualFamilySupported(payload.accountFamily, "PR-17C accrual generation");
  return createRevenueRecognitionRun({
    req,
    payload,
    assertScopeAccess,
  });
}

export async function settleRevenueAccrual({
  req,
  payload,
  assertScopeAccess,
}) {
  return postRevenueRecognitionRun({
    req,
    payload,
    assertScopeAccess,
    allowedFamilies: PR17C_ACCRUAL_FAMILIES,
    actionLabel: "settle accrual",
    allowPeriodOverride: true,
    targetPeriodIdField: "settlementPeriodId",
    enforceMaturityBoundary: true,
    postedRunLineStatus: "SETTLED",
    alreadyPostedMessage: "Accrual is already settled/posted",
    alreadyReversedMessage: "Accrual is already reversed/closed and cannot be settled",
    invalidStatusMessageBuilder: (status) => `Accrual status ${status} cannot be settled`,
  });
}

export async function reverseRevenueAccrual({
  req,
  payload,
  assertScopeAccess,
}) {
  return reverseRevenueRecognitionRun({
    req,
    payload,
    assertScopeAccess,
    allowedFamilies: PR17C_ACCRUAL_FAMILIES,
    actionLabel: "reverse accrual",
    enforceMaturityBoundary: true,
    invalidStatusMessage: "Accrual reverse is allowed only from settled/posted accrual state",
    alreadyReversedMessage: "Accrual is already reversed",
  });
}

export async function postRevenueRecognitionRun({
  req,
  payload,
  assertScopeAccess,
  allowedFamilies = PR17B_ACTIVE_FAMILIES,
  actionLabel = "post revenue-recognition run",
  allowPeriodOverride = false,
  targetPeriodIdField = "settlementPeriodId",
  enforceMaturityBoundary = false,
  postedRunLineStatus = "POSTED",
  alreadyPostedMessage = "Run is already POSTED",
  alreadyReversedMessage = "Run is already REVERSED and cannot be posted",
  invalidStatusMessageBuilder = (status) => `Run status ${status} cannot be posted`,
}) {
  const existing = await fetchRunRow({
    tenantId: payload.tenantId,
    runId: payload.runId,
  });
  if (!existing) {
    throw badRequest("Revenue-recognition run not found");
  }
  assertScopeAccess(req, "legal_entity", existing.legal_entity_id, "runId");

  return withTransaction(async (tx) => {
    const run = await fetchRunRow({
      tenantId: payload.tenantId,
      runId: payload.runId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!run) {
      throw badRequest("Revenue-recognition run not found");
    }
    assertScopeAccess(req, "legal_entity", run.legal_entity_id, "runId");

    const runStatus = asUpper(run.status);
    if (runStatus === RUN_STATUS.POSTED) {
      throw badRequest(alreadyPostedMessage);
    }
    if (runStatus === RUN_STATUS.REVERSED) {
      throw badRequest(alreadyReversedMessage);
    }
    if (runStatus !== RUN_STATUS.DRAFT && runStatus !== RUN_STATUS.READY) {
      throw badRequest(invalidStatusMessageBuilder(run.status));
    }

    const accountFamily = assertPostingFamilySupported(
      run.account_family,
      allowedFamilies,
      actionLabel
    );
    const tenantId = parsePositiveInt(run.tenant_id);
    const legalEntityId = parsePositiveInt(run.legal_entity_id);
    const fallbackFiscalPeriodId = parsePositiveInt(run.fiscal_period_id);
    const overridePeriodId =
      allowPeriodOverride && targetPeriodIdField
        ? parsePositiveInt(payload?.[targetPeriodIdField])
        : null;
    const fiscalPeriodId = overridePeriodId || fallbackFiscalPeriodId;
    if (!fiscalPeriodId) {
      throw badRequest("fiscalPeriodId could not be resolved for posting");
    }

    let periodStartDate = toDateOnlyString(run.period_start_date, "periodStartDate");
    let periodEndDate = toDateOnlyString(run.period_end_date, "periodEndDate");
    if (overridePeriodId) {
      const postingPeriod = await resolvePeriodScope({
        tenantId,
        legalEntityId,
        fiscalPeriodId,
        runQuery: tx.query,
      });
      if (!postingPeriod) {
        throw badRequest(`${targetPeriodIdField} is not valid in legalEntity scope`);
      }
      periodStartDate = toDateOnlyString(postingPeriod.start_date, "periodStartDate");
      periodEndDate = toDateOnlyString(postingPeriod.end_date, "periodEndDate");
    }

    const postingBook = await resolveBookForRunPeriod({
      tenantId,
      legalEntityId,
      fiscalPeriodId,
      runQuery: tx.query,
    });
    const bookId = parsePositiveInt(postingBook.id);
    await ensurePeriodOpenForBook({
      bookId,
      fiscalPeriodId,
      actionLabel,
      runQuery: tx.query,
    });

    if (enforceMaturityBoundary) {
      ensureAccrualMaturityBoundary({
        run,
        targetPeriodEndDate: periodEndDate,
        actionLabel,
      });
    }

    const mappingByPurpose = await resolveRevenuePostingAccounts({
      tenantId,
      legalEntityId,
      accountFamily,
      allowedFamilies,
      phaseLabel: actionLabel,
      runQuery: tx.query,
    });

    const runLines = await fetchRunLines({
      tenantId,
      legalEntityId,
      runId: payload.runId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (runLines.length === 0) {
      throw badRequest("Run has no lines to post");
    }
    const sourceContractLineIds = Array.from(
      new Set(runLines.map((line) => parsePositiveInt(line.source_contract_line_id)).filter(Boolean))
    );
    const contractLineAccountOverridesById = await fetchContractLinePostingOverrides({
      tenantId,
      legalEntityId,
      contractLineIds: sourceContractLineIds,
      runQuery: tx.query,
    });

    const postingEntries = [];
    const journalLines = [];
    for (const runLine of runLines) {
      const normalizedStatus = asUpper(runLine.status);
      if (normalizedStatus === "REVERSED") {
        throw badRequest("Run line is already REVERSED and cannot be posted");
      }
      const lineEntries = buildPostingEntriesForRunLine({
        runNo: run.run_no,
        runLine,
        mappingByPurpose,
        contractLineAccountOverridesById,
      });
      postingEntries.push(...lineEntries);
      for (const entry of lineEntries) {
        journalLines.push(...entry.lines);
      }
    }

    if (journalLines.length === 0) {
      throw badRequest("Run posting did not produce journal lines");
    }
    ensureBalancedJournalLines(journalLines, "Revenue-recognition posting journal");

    const journalNo = buildRevenueJournalNo(resolveJournalPrefixByFamily(accountFamily), run.run_no, payload.runId);
    const journalResult = await insertPostedJournalWithLinesTx(tx, {
      tenantId,
      legalEntityId,
      bookId,
      fiscalPeriodId,
      journalNo,
      entryDate: periodEndDate,
      documentDate: periodEndDate,
      currencyCode: asUpper(run.currency_code),
      description: `Revenue-recognition post ${run.run_no}`,
      referenceNo: String(run.run_no || "").slice(0, 100) || null,
      userId: payload.userId,
      lines: journalLines,
    });

    await insertSubledgerEntriesTx(tx, {
      tenantId,
      legalEntityId,
      runId: payload.runId,
      entries: postingEntries,
      fiscalPeriodId,
      periodStartDate,
      periodEndDate,
      postedJournalEntryId: journalResult.journalEntryId,
      userId: payload.userId,
    });

    await tx.query(
      `UPDATE revenue_recognition_run_lines
       SET status = ?,
           fiscal_period_id = ?,
           period_start_date = ?,
           period_end_date = ?,
           posted_journal_entry_id = ?
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND run_id = ?`,
      [
        postedRunLineStatus,
        fiscalPeriodId,
        periodStartDate,
        periodEndDate,
        journalResult.journalEntryId,
        tenantId,
        legalEntityId,
        payload.runId,
      ]
    );

    await tx.query(
      `UPDATE revenue_recognition_schedule_lines sl
       JOIN revenue_recognition_run_lines rl
         ON rl.tenant_id = sl.tenant_id
        AND rl.legal_entity_id = sl.legal_entity_id
        AND rl.schedule_line_id = sl.id
       SET sl.status = 'SETTLED',
           sl.posted_journal_entry_id = ?
       WHERE rl.tenant_id = ?
         AND rl.legal_entity_id = ?
         AND rl.run_id = ?`,
      [journalResult.journalEntryId, tenantId, legalEntityId, payload.runId]
    );

    await tx.query(
      `UPDATE revenue_recognition_runs
       SET status = 'POSTED',
           fiscal_period_id = ?,
           period_start_date = ?,
           period_end_date = ?,
           posted_journal_entry_id = ?,
           posted_by_user_id = ?,
           posted_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [
        fiscalPeriodId,
        periodStartDate,
        periodEndDate,
        journalResult.journalEntryId,
        payload.userId,
        tenantId,
        payload.runId,
      ]
    );

    if (parsePositiveInt(run.schedule_id)) {
      await tx.query(
        `UPDATE revenue_recognition_schedules
         SET status = 'POSTED',
             posted_journal_entry_id = ?
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND id = ?`,
        [journalResult.journalEntryId, tenantId, legalEntityId, parsePositiveInt(run.schedule_id)]
      );
    }

    const updated = await fetchRunRow({
      tenantId,
      runId: payload.runId,
      runQuery: tx.query,
    });
    if (!updated) {
      throw new Error("Run post readback failed");
    }

    return {
      row: mapRunRow(updated),
      journal: {
        journalEntryId: journalResult.journalEntryId,
        journalNo,
        lineCount: journalResult.lineCount,
        totalDebitBase: journalResult.totalDebit,
        totalCreditBase: journalResult.totalCredit,
      },
      subledgerEntryCount: postingEntries.length,
    };
  });
}

export async function reverseRevenueRecognitionRun({
  req,
  payload,
  assertScopeAccess,
  allowedFamilies = PR17B_ACTIVE_FAMILIES,
  actionLabel = "reverse revenue-recognition run",
  enforceMaturityBoundary = false,
  invalidStatusMessage = "Reverse is allowed only from POSTED status",
  alreadyReversedMessage = "Run is already reversed",
}) {
  const existing = await fetchRunRow({
    tenantId: payload.tenantId,
    runId: payload.runId,
  });
  if (!existing) {
    throw badRequest("Revenue-recognition run not found");
  }
  assertScopeAccess(req, "legal_entity", existing.legal_entity_id, "runId");

  return withTransaction(async (tx) => {
    const run = await fetchRunRow({
      tenantId: payload.tenantId,
      runId: payload.runId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!run) {
      throw badRequest("Revenue-recognition run not found");
    }
    assertScopeAccess(req, "legal_entity", run.legal_entity_id, "runId");

    if (asUpper(run.status) !== RUN_STATUS.POSTED) {
      throw badRequest(invalidStatusMessage);
    }
    if (parsePositiveInt(run.reversal_journal_entry_id)) {
      throw badRequest(alreadyReversedMessage);
    }
    assertPostingFamilySupported(run.account_family, allowedFamilies, actionLabel);

    const tenantId = parsePositiveInt(run.tenant_id);
    const legalEntityId = parsePositiveInt(run.legal_entity_id);
    const originalJournalEntryId = parsePositiveInt(run.posted_journal_entry_id);
    if (!originalJournalEntryId) {
      throw badRequest("Run posted journal linkage is missing");
    }

    const reversalPeriodId =
      parsePositiveInt(payload.reversalPeriodId) || parsePositiveInt(run.fiscal_period_id);
    if (!reversalPeriodId) {
      throw badRequest("reversalPeriodId could not be resolved");
    }

    const period = await resolvePeriodScope({
      tenantId,
      legalEntityId,
      fiscalPeriodId: reversalPeriodId,
      runQuery: tx.query,
    });
    if (!period) {
      throw badRequest("reversalPeriodId is not valid in legalEntity scope");
    }

    const postingBook = await resolveBookForRunPeriod({
      tenantId,
      legalEntityId,
      fiscalPeriodId: reversalPeriodId,
      runQuery: tx.query,
    });
    const bookId = parsePositiveInt(postingBook.id);
    await ensurePeriodOpenForBook({
      bookId,
      fiscalPeriodId: reversalPeriodId,
      actionLabel,
      runQuery: tx.query,
    });

    if (enforceMaturityBoundary) {
      ensureAccrualMaturityBoundary({
        run,
        targetPeriodEndDate: toDateOnlyString(period.end_date, "periodEndDate"),
        actionLabel,
      });
    }

    const originalJournalBundle = await fetchJournalWithLines({
      tenantId,
      journalEntryId: originalJournalEntryId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!originalJournalBundle?.journal) {
      throw badRequest("Original posted journal not found");
    }
    if (asUpper(originalJournalBundle.journal.status) !== RUN_STATUS.POSTED) {
      throw badRequest("Original posted journal is not in POSTED status");
    }
    if (parsePositiveInt(originalJournalBundle.journal.reversal_journal_entry_id)) {
      throw badRequest("Original posted journal is already reversed");
    }
    if (!Array.isArray(originalJournalBundle.lines) || originalJournalBundle.lines.length === 0) {
      throw badRequest("Original posted journal has no lines to reverse");
    }

    const reversalLines = originalJournalBundle.lines.map((line) => ({
      accountId: parsePositiveInt(line.account_id),
      debitBase: Number(line.credit_base || 0),
      creditBase: Number(line.debit_base || 0),
      amountTxn: Number((Number(line.amount_txn || 0) * -1).toFixed(6)),
      description: String(line.description || `Reversal of ${run.run_no}`),
      subledgerReferenceNo: line.subledger_reference_no || null,
      currencyCode: asUpper(line.currency_code || run.currency_code),
    }));

    const reversalJournalNo = buildRevenueJournalNo("REVREV", run.run_no, payload.runId);
    const reversalJournalResult = await insertPostedJournalWithLinesTx(tx, {
      tenantId,
      legalEntityId,
      bookId,
      fiscalPeriodId: reversalPeriodId,
      journalNo: reversalJournalNo,
      entryDate: toDateOnlyString(period.end_date, "periodEndDate"),
      documentDate: toDateOnlyString(period.end_date, "periodEndDate"),
      currencyCode: asUpper(run.currency_code),
      description: `Reversal of revenue-recognition run ${run.run_no}`,
      referenceNo: String(run.run_no || "").slice(0, 100) || null,
      userId: payload.userId,
      lines: reversalLines,
    });

    const reverseReason = payload.reason || "Revenue-recognition run reversal";
    await tx.query(
      `UPDATE journal_entries
       SET status = 'REVERSED',
           reversed_by_user_id = ?,
           reversed_at = CURRENT_TIMESTAMP,
           reversal_journal_entry_id = ?,
           reverse_reason = ?
       WHERE tenant_id = ?
         AND id = ?
         AND status = 'POSTED'`,
      [
        payload.userId,
        reversalJournalResult.journalEntryId,
        String(reverseReason).slice(0, 255),
        tenantId,
        originalJournalEntryId,
      ]
    );

    const originalRunLines = await fetchRunLines({
      tenantId,
      legalEntityId,
      runId: payload.runId,
      runQuery: tx.query,
      forUpdate: true,
    });
    const originalSubledger = await fetchRunSubledgerEntries({
      tenantId,
      legalEntityId,
      runId: payload.runId,
      runQuery: tx.query,
      forUpdate: true,
    });

    const reversalRunNo = buildReversalRunNo(run.run_no, payload.runId);
    const reversalSourceRunUid = buildDeterministicUid("RUN_REV", [
      payload.runId,
      reversalJournalResult.journalEntryId,
      reversalPeriodId,
    ]);
    const reversalRunInsert = await tx.query(
      `INSERT INTO revenue_recognition_runs (
          tenant_id,
          legal_entity_id,
          schedule_id,
          fiscal_period_id,
          run_no,
          source_run_uid,
          status,
          account_family,
          maturity_bucket,
          maturity_date,
          reclass_required,
          currency_code,
          fx_rate,
          total_amount_txn,
          total_amount_base,
          period_start_date,
          period_end_date,
          reversal_of_run_id,
          posted_journal_entry_id,
          created_by_user_id,
          posted_by_user_id,
          posted_at
       )
       VALUES (?, ?, ?, ?, ?, ?, 'POSTED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
      [
        tenantId,
        legalEntityId,
        parsePositiveInt(run.schedule_id),
        reversalPeriodId,
        reversalRunNo,
        reversalSourceRunUid,
        asUpper(run.account_family),
        asUpper(run.maturity_bucket),
        toDateOnlyString(run.maturity_date, "maturityDate"),
        toBoolean(run.reclass_required) ? 1 : 0,
        asUpper(run.currency_code),
        run.fx_rate,
        run.total_amount_txn,
        run.total_amount_base,
        toDateOnlyString(period.start_date, "periodStartDate"),
        toDateOnlyString(period.end_date, "periodEndDate"),
        payload.runId,
        reversalJournalResult.journalEntryId,
        payload.userId,
        payload.userId,
      ]
    );
    const reversalRunId = parsePositiveInt(reversalRunInsert.rows?.insertId);
    if (!reversalRunId) {
      throw badRequest("Failed to create reversal run");
    }

    const reversalRunLineIdByOriginal = new Map();
    for (let index = 0; index < originalRunLines.length; index += 1) {
      const originalLine = originalRunLines[index];
      // eslint-disable-next-line no-await-in-loop
      const reversalRunLineInsert = await tx.query(
        `INSERT INTO revenue_recognition_run_lines (
            tenant_id,
            legal_entity_id,
            run_id,
            schedule_line_id,
            line_no,
            source_row_uid,
            status,
            account_family,
            maturity_bucket,
            maturity_date,
            reclass_required,
            currency_code,
            fx_rate,
            amount_txn,
            amount_base,
            fiscal_period_id,
            period_start_date,
            period_end_date,
            reversal_of_run_line_id,
            posted_journal_entry_id
         )
         VALUES (?, ?, ?, ?, ?, ?, 'POSTED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          tenantId,
          legalEntityId,
          reversalRunId,
          parsePositiveInt(originalLine.schedule_line_id),
          index + 1,
          buildDeterministicUid("RUN_LINE_REV", [reversalRunId, originalLine.id, index + 1]),
          asUpper(originalLine.account_family),
          asUpper(originalLine.maturity_bucket),
          toDateOnlyString(originalLine.maturity_date, "maturityDate"),
          toBoolean(originalLine.reclass_required) ? 1 : 0,
          asUpper(originalLine.currency_code),
          originalLine.fx_rate,
          originalLine.amount_txn,
          originalLine.amount_base,
          reversalPeriodId,
          toDateOnlyString(period.start_date, "periodStartDate"),
          toDateOnlyString(period.end_date, "periodEndDate"),
          parsePositiveInt(originalLine.id),
          reversalJournalResult.journalEntryId,
        ]
      );
      const reversalRunLineId = parsePositiveInt(reversalRunLineInsert.rows?.insertId);
      if (!reversalRunLineId) {
        throw badRequest("Failed to create reversal run line");
      }
      reversalRunLineIdByOriginal.set(parsePositiveInt(originalLine.id), reversalRunLineId);
    }

    const reversalSubledgerEntries = [];
    for (const originalEntry of originalSubledger) {
      const originalRunLineId = parsePositiveInt(originalEntry.run_line_id);
      const reversalRunLineId = reversalRunLineIdByOriginal.get(originalRunLineId) || null;
      reversalSubledgerEntries.push({
        runLineId: reversalRunLineId,
        scheduleLineId: parsePositiveInt(originalEntry.schedule_line_id),
        entryKind: "REVERSAL",
        accountFamily: asUpper(originalEntry.account_family),
        maturityBucket: asUpper(originalEntry.maturity_bucket),
        maturityDate: toDateOnlyString(originalEntry.maturity_date, "maturityDate"),
        reclassRequired: toBoolean(originalEntry.reclass_required),
        currencyCode: asUpper(originalEntry.currency_code),
        fxRate: originalEntry.fx_rate,
        amountTxn: originalEntry.amount_txn,
        amountBase: originalEntry.amount_base,
        reversalOfSubledgerEntryId: parsePositiveInt(originalEntry.id),
      });
    }

    if (reversalSubledgerEntries.length > 0) {
      await insertSubledgerEntriesTx(tx, {
        tenantId,
        legalEntityId,
        runId: reversalRunId,
        entries: reversalSubledgerEntries,
        fiscalPeriodId: reversalPeriodId,
        periodStartDate: toDateOnlyString(period.start_date, "periodStartDate"),
        periodEndDate: toDateOnlyString(period.end_date, "periodEndDate"),
        postedJournalEntryId: reversalJournalResult.journalEntryId,
        userId: payload.userId,
      });
    }

    await tx.query(
      `UPDATE revenue_recognition_run_lines
       SET status = 'REVERSED'
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND run_id = ?`,
      [tenantId, legalEntityId, payload.runId]
    );

    await tx.query(
      `UPDATE revenue_recognition_subledger_entries
       SET status = 'REVERSED'
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND run_id = ?`,
      [tenantId, legalEntityId, payload.runId]
    );

    await tx.query(
      `UPDATE revenue_recognition_runs
       SET status = 'REVERSED',
           reversal_journal_entry_id = ?,
           reversed_by_user_id = ?,
           reversed_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [reversalJournalResult.journalEntryId, payload.userId, tenantId, payload.runId]
    );

    const updatedOriginal = await fetchRunRow({
      tenantId,
      runId: payload.runId,
      runQuery: tx.query,
    });
    const reversalRun = await fetchRunRow({
      tenantId,
      runId: reversalRunId,
      runQuery: tx.query,
    });
    if (!updatedOriginal || !reversalRun) {
      throw new Error("Run reverse readback failed");
    }

    return {
      row: mapRunRow(updatedOriginal),
      reversalRun: mapRunRow(reversalRun),
      journal: {
        originalPostedJournalEntryId: originalJournalEntryId,
        reversalJournalEntryId: reversalJournalResult.journalEntryId,
        reversalJournalNo,
        lineCount: reversalJournalResult.lineCount,
        totalDebitBase: reversalJournalResult.totalDebit,
        totalCreditBase: reversalJournalResult.totalCredit,
      },
    };
  });
}

async function loadRevenueRollforwardReport({
  req,
  filters,
  assertScopeAccess,
  buildScopeFilter,
}) {
  const { limit, offset } = resolveReportPagination(filters);
  const reportCode = "FUTURE_YEAR_ROLLFORWARD";
  const window = await resolveRollforwardWindow({
    tenantId: filters.tenantId,
    legalEntityId: filters.legalEntityId || null,
    fiscalPeriodId: filters.fiscalPeriodId || null,
    asOfDate: filters.asOfDate || null,
  });

  const rollforwardStartDate = window.startDate || "1000-01-01";
  const rollforwardEndDate = window.endDate || "9999-12-31";
  const windowAmountParams = [
    rollforwardStartDate,
    rollforwardStartDate,
    rollforwardStartDate,
    rollforwardEndDate,
    rollforwardStartDate,
    rollforwardEndDate,
    rollforwardEndDate,
    rollforwardEndDate,
    rollforwardEndDate,
    rollforwardEndDate,
    rollforwardEndDate,
    rollforwardEndDate,
  ];

  const whereBundle = buildSubledgerWhereClause({
    req,
    filters,
    buildScopeFilter,
    assertScopeAccess,
    accountFamilies: Array.from(PR17_POSTABLE_FAMILIES),
    includeFiscalPeriod: false,
    includeAsOfDate: false,
    periodEndDateTo: window.endDate || null,
  });
  if (whereBundle.families.length === 0) {
    const empty = buildReportStubRows(reportCode, { ...filters, limit, offset });
    return {
      ...empty,
      windowStartDate: window.startDate,
      windowEndDate: window.endDate,
    };
  }

  const totalResult = await query(
    `SELECT COUNT(*) AS total
     FROM (
       SELECT 1
       FROM revenue_recognition_subledger_entries rse
       WHERE ${whereBundle.whereSql}
       GROUP BY
         rse.legal_entity_id,
         rse.currency_code,
         rse.account_family
     ) AS grouped_rows`,
    whereBundle.params
  );
  const total = Number(totalResult.rows?.[0]?.total || 0);

  const rowsResult = await query(
    `SELECT
        rse.legal_entity_id,
        rse.currency_code,
        rse.account_family,
        COALESCE(SUM(CASE WHEN rse.period_end_date < ? THEN ${signedAmountSql("rse", "amount_txn")} ELSE 0 END), 0) AS opening_amount_txn,
        COALESCE(SUM(CASE WHEN rse.period_end_date < ? THEN ${signedAmountSql("rse", "amount_base")} ELSE 0 END), 0) AS opening_amount_base,
        COALESCE(SUM(CASE WHEN rse.period_end_date >= ? AND rse.period_end_date <= ? THEN ${signedAmountSql("rse", "amount_txn")} ELSE 0 END), 0) AS movement_amount_txn,
        COALESCE(SUM(CASE WHEN rse.period_end_date >= ? AND rse.period_end_date <= ? THEN ${signedAmountSql("rse", "amount_base")} ELSE 0 END), 0) AS movement_amount_base,
        COALESCE(SUM(CASE WHEN rse.period_end_date <= ? THEN ${signedAmountSql("rse", "amount_txn")} ELSE 0 END), 0) AS closing_amount_txn,
        COALESCE(SUM(CASE WHEN rse.period_end_date <= ? THEN ${signedAmountSql("rse", "amount_base")} ELSE 0 END), 0) AS closing_amount_base,
        COALESCE(SUM(CASE WHEN rse.period_end_date <= ? THEN ${shortBucketAmountSql("rse", "amount_txn")} ELSE 0 END), 0) AS closing_short_term_amount_txn,
        COALESCE(SUM(CASE WHEN rse.period_end_date <= ? THEN ${shortBucketAmountSql("rse", "amount_base")} ELSE 0 END), 0) AS closing_short_term_amount_base,
        COALESCE(SUM(CASE WHEN rse.period_end_date <= ? THEN ${longBucketAmountSql("rse", "amount_txn")} ELSE 0 END), 0) AS closing_long_term_amount_txn,
        COALESCE(SUM(CASE WHEN rse.period_end_date <= ? THEN ${longBucketAmountSql("rse", "amount_base")} ELSE 0 END), 0) AS closing_long_term_amount_base,
        COUNT(*) AS entry_count,
        COUNT(DISTINCT rse.posted_journal_entry_id) AS journal_count
     FROM revenue_recognition_subledger_entries rse
     WHERE ${whereBundle.whereSql}
     GROUP BY
       rse.legal_entity_id,
       rse.currency_code,
       rse.account_family
     ORDER BY
       rse.legal_entity_id ASC,
       rse.currency_code ASC,
       rse.account_family ASC
     LIMIT ${limit} OFFSET ${offset}`,
    [...windowAmountParams, ...whereBundle.params]
  );

  const summaryResult = await query(
    `SELECT
        COALESCE(SUM(CASE WHEN rse.period_end_date < ? THEN ${signedAmountSql("rse", "amount_txn")} ELSE 0 END), 0) AS opening_amount_txn,
        COALESCE(SUM(CASE WHEN rse.period_end_date < ? THEN ${signedAmountSql("rse", "amount_base")} ELSE 0 END), 0) AS opening_amount_base,
        COALESCE(SUM(CASE WHEN rse.period_end_date >= ? AND rse.period_end_date <= ? THEN ${signedAmountSql("rse", "amount_txn")} ELSE 0 END), 0) AS movement_amount_txn,
        COALESCE(SUM(CASE WHEN rse.period_end_date >= ? AND rse.period_end_date <= ? THEN ${signedAmountSql("rse", "amount_base")} ELSE 0 END), 0) AS movement_amount_base,
        COALESCE(SUM(CASE WHEN rse.period_end_date <= ? THEN ${signedAmountSql("rse", "amount_txn")} ELSE 0 END), 0) AS closing_amount_txn,
        COALESCE(SUM(CASE WHEN rse.period_end_date <= ? THEN ${signedAmountSql("rse", "amount_base")} ELSE 0 END), 0) AS closing_amount_base,
        COALESCE(SUM(CASE WHEN rse.period_end_date <= ? THEN ${shortBucketAmountSql("rse", "amount_txn")} ELSE 0 END), 0) AS short_term_amount_txn,
        COALESCE(SUM(CASE WHEN rse.period_end_date <= ? THEN ${shortBucketAmountSql("rse", "amount_base")} ELSE 0 END), 0) AS short_term_amount_base,
        COALESCE(SUM(CASE WHEN rse.period_end_date <= ? THEN ${longBucketAmountSql("rse", "amount_txn")} ELSE 0 END), 0) AS long_term_amount_txn,
        COALESCE(SUM(CASE WHEN rse.period_end_date <= ? THEN ${longBucketAmountSql("rse", "amount_base")} ELSE 0 END), 0) AS long_term_amount_base,
        COUNT(*) AS entry_count,
        COUNT(DISTINCT rse.posted_journal_entry_id) AS journal_count
     FROM revenue_recognition_subledger_entries rse
     WHERE ${whereBundle.whereSql}`,
    [...windowAmountParams, ...whereBundle.params]
  );
  const summaryRow = summaryResult.rows?.[0] || {};
  const summary = {
    ...buildReportEmptySummary(),
    openingAmountTxn: roundAmount(summaryRow.opening_amount_txn),
    openingAmountBase: roundAmount(summaryRow.opening_amount_base),
    movementAmountTxn: roundAmount(summaryRow.movement_amount_txn),
    movementAmountBase: roundAmount(summaryRow.movement_amount_base),
    closingAmountTxn: roundAmount(summaryRow.closing_amount_txn),
    closingAmountBase: roundAmount(summaryRow.closing_amount_base),
    shortTermAmountTxn: roundAmount(summaryRow.short_term_amount_txn),
    shortTermAmountBase: roundAmount(summaryRow.short_term_amount_base),
    longTermAmountTxn: roundAmount(summaryRow.long_term_amount_txn),
    longTermAmountBase: roundAmount(summaryRow.long_term_amount_base),
    totalAmountTxn: roundAmount(summaryRow.closing_amount_txn),
    totalAmountBase: roundAmount(summaryRow.closing_amount_base),
    entryCount: Number(summaryRow.entry_count || 0),
    journalCount: Number(summaryRow.journal_count || 0),
  };

  const reconciliation = await loadRevenueReconciliationRows({
    req,
    filters,
    buildScopeFilter,
    assertScopeAccess,
    accountFamilies: Array.from(PR17_POSTABLE_FAMILIES),
    includeFiscalPeriod: false,
    includeAsOfDate: false,
    periodEndDateFrom: window.startDate || null,
    periodEndDateTo: window.endDate || null,
  });

  return buildReportResponse({
    reportCode,
    filters,
    rows: (rowsResult.rows || []).map((row) => mapRollforwardRow(row)),
    total,
    limit,
    offset,
    summary,
    reconciliation,
    windowStartDate: window.startDate,
    windowEndDate: window.endDate,
  });
}

export async function getRevenueFutureYearRollforwardReport({
  req,
  filters,
  assertScopeAccess,
  buildScopeFilter,
}) {
  return loadRevenueRollforwardReport({
    req,
    filters,
    assertScopeAccess,
    buildScopeFilter,
  });
}

export async function getRevenueDeferredRevenueSplitReport({
  req,
  filters,
  assertScopeAccess,
  buildScopeFilter,
}) {
  return loadRevenueSplitReport({
    req,
    filters,
    assertScopeAccess,
    buildScopeFilter,
    reportCode: "DEFERRED_REVENUE_SPLIT",
    allowedFamilies: [ACCOUNT_FAMILY.DEFREV],
  });
}

export async function getRevenueAccrualSplitReport({
  req,
  filters,
  assertScopeAccess,
  buildScopeFilter,
}) {
  return loadRevenueSplitReport({
    req,
    filters,
    assertScopeAccess,
    buildScopeFilter,
    reportCode: "ACCRUAL_SPLIT",
    allowedFamilies: [ACCOUNT_FAMILY.ACCRUED_REVENUE, ACCOUNT_FAMILY.ACCRUED_EXPENSE],
  });
}

export async function getRevenuePrepaidExpenseSplitReport({
  req,
  filters,
  assertScopeAccess,
  buildScopeFilter,
}) {
  return loadRevenueSplitReport({
    req,
    filters,
    assertScopeAccess,
    buildScopeFilter,
    reportCode: "PREPAID_EXPENSE_SPLIT",
    allowedFamilies: [ACCOUNT_FAMILY.PREPAID_EXPENSE],
  });
}
