import {
  normalizeEnum,
  normalizeText,
  parseBooleanFlag,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";
import { parsePayrollRunIdParam } from "./payroll.runs.validators.js";

const PAYMENT_SYNC_SCOPE_VALUES = ["ALL", "NET_PAY", "STATUTORY"];

export function parsePayrollPaymentSyncPreviewInput(req) {
  return {
    tenantId: requireTenantId(req),
    runId: parsePayrollRunIdParam(req),
    scope: normalizeEnum(
      req.query?.scope,
      "scope",
      PAYMENT_SYNC_SCOPE_VALUES,
      "ALL"
    ),
    allowB04OnlySettlement: parseBooleanFlag(
      req.query?.allowB04OnlySettlement ?? req.query?.allow_b04_only_settlement,
      false
    ),
  };
}

export function parsePayrollPaymentSyncApplyInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    runId: parsePayrollRunIdParam(req),
    scope: normalizeEnum(
      req.body?.scope,
      "scope",
      PAYMENT_SYNC_SCOPE_VALUES,
      "ALL"
    ),
    note: normalizeText(req.body?.note, "note", 500),
    allowB04OnlySettlement: parseBooleanFlag(
      req.body?.allowB04OnlySettlement ?? req.body?.allow_b04_only_settlement,
      false
    ),
  };
}

