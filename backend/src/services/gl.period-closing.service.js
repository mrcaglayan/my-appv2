import { badRequest } from "../routes/_utils.js";
import {
  findPeriodCloseRunLines,
  findPeriodCloseRuns,
  upsertPeriodStatus,
} from "./gl.period-closing.queries.js";

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

export async function listPeriodCloseRuns({
  req,
  tenantId,
  filters,
  assertBookBelongsToTenant,
  assertScopeAccess,
  buildScopeFilter,
  mapPeriodCloseRunRow,
}) {
  const { bookId, fiscalPeriodId, status, includeLines } = filters;

  if (bookId) {
    const book = await assertBookBelongsToTenant(tenantId, bookId, "bookId");
    assertScopeAccess(
      req,
      "legal_entity",
      toPositiveInt(book.legal_entity_id),
      "bookId"
    );
  }

  const params = [tenantId];
  const conditions = ["r.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "b.legal_entity_id", params));

  if (bookId) {
    conditions.push("r.book_id = ?");
    params.push(bookId);
  }
  if (fiscalPeriodId) {
    conditions.push("r.fiscal_period_id = ?");
    params.push(fiscalPeriodId);
  }
  if (status) {
    conditions.push("r.status = ?");
    params.push(status);
  }

  const runRows = await findPeriodCloseRuns({ conditions, params });
  const rows = runRows.map((row) => mapPeriodCloseRunRow(row));

  if (includeLines && rows.length > 0) {
    const runIds = rows.map((row) => toPositiveInt(row.id)).filter(Boolean);
    const lineRows = await findPeriodCloseRunLines({ runIds });

    const linesByRunId = new Map();
    for (const line of lineRows) {
      const runId = toPositiveInt(line.period_close_run_id);
      if (!runId) {
        continue;
      }

      if (!linesByRunId.has(runId)) {
        linesByRunId.set(runId, []);
      }

      linesByRunId.get(runId).push({
        lineType: String(line.line_type || ""),
        accountId: toPositiveInt(line.account_id),
        accountCode: line.account_code || null,
        accountName: line.account_name || null,
        closingBalance: Number(line.closing_balance || 0),
        debitBase: Number(line.debit_base || 0),
        creditBase: Number(line.credit_base || 0),
      });
    }

    for (const row of rows) {
      row.lines = linesByRunId.get(toPositiveInt(row.id)) || [];
    }
  }

  return {
    tenantId,
    rows,
  };
}

export async function closePeriodStatus({
  tenantId,
  bookId,
  fiscalPeriodId,
  status,
  note,
  userId,
  assertBookBelongsToTenant,
  assertFiscalPeriodBelongsToCalendar,
  getEffectivePeriodStatus,
}) {
  const book = await assertBookBelongsToTenant(tenantId, bookId, "bookId");
  await assertFiscalPeriodBelongsToCalendar(
    toPositiveInt(book.calendar_id),
    fiscalPeriodId,
    "periodId"
  );

  const currentStatus = await getEffectivePeriodStatus(bookId, fiscalPeriodId);
  if (currentStatus === "HARD_CLOSED" && status !== "HARD_CLOSED") {
    throw badRequest("HARD_CLOSED periods cannot be re-opened or softened");
  }

  await upsertPeriodStatus({
    bookId,
    fiscalPeriodId,
    status,
    userId,
    note,
  });

  return {
    bookId,
    fiscalPeriodId,
    status,
    previousStatus: currentStatus,
  };
}
