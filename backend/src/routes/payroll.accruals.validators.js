import { badRequest } from "./_utils.js";
import {
  normalizeText,
  parseBooleanFlag,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";
import { parsePayrollRunIdParam } from "./payroll.runs.validators.js";

export function parsePayrollAccrualPreviewInput(req) {
  return {
    tenantId: requireTenantId(req),
    runId: parsePayrollRunIdParam(req),
  };
}

export function parsePayrollRunReviewInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    runId: parsePayrollRunIdParam(req),
    note: normalizeText(req.body?.note, "note", 500),
  };
}

export function parsePayrollRunFinalizeInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const runId = parsePayrollRunIdParam(req);
  const forceFromImported = parseBooleanFlag(
    req.body?.forceFromImported ?? req.body?.force_from_imported,
    false
  );
  const note = normalizeText(req.body?.note, "note", 500);

  if (!tenantId || !userId || !runId) {
    throw badRequest("Invalid payroll run finalize request");
  }

  return {
    tenantId,
    userId,
    runId,
    forceFromImported,
    note,
  };
}

