import { badRequest, parsePositiveInt } from "./_utils.js";

export function parsePeriodCloseRunFilters(rawQuery = {}, closeRunStatuses) {
  const bookId = parsePositiveInt(rawQuery.bookId);
  const fiscalPeriodId = parsePositiveInt(rawQuery.fiscalPeriodId);
  const status = rawQuery.status ? String(rawQuery.status).toUpperCase() : null;
  const includeLines = String(rawQuery.includeLines || "").toLowerCase() === "true";

  if (status && !closeRunStatuses.has(status)) {
    throw badRequest("status must be one of IN_PROGRESS, COMPLETED, FAILED, REOPENED");
  }

  return {
    bookId,
    fiscalPeriodId,
    status,
    includeLines,
  };
}

export function parsePeriodStatusCloseInput(req, periodStatuses) {
  const bookId = parsePositiveInt(req.params?.bookId);
  const fiscalPeriodId = parsePositiveInt(req.params?.periodId);
  if (!bookId || !fiscalPeriodId) {
    throw badRequest("bookId and periodId must be positive integers");
  }

  const status = String(req.body?.status || "SOFT_CLOSED").toUpperCase();
  if (!periodStatuses.has(status)) {
    throw badRequest("status must be one of OPEN, SOFT_CLOSED, HARD_CLOSED");
  }

  return {
    bookId,
    fiscalPeriodId,
    status,
    note: req.body?.note ? String(req.body.note) : null,
  };
}
