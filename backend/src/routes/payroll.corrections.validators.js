import {
  normalizeCode,
  normalizeCurrencyCode,
  normalizeEnum,
  normalizeText,
  optionalPositiveInt,
  parseDateOnly,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";
import { parsePayrollRunIdParam } from "./payroll.runs.validators.js";

const CORRECTION_SHELL_TYPES = ["OFF_CYCLE", "RETRO"];

export function parsePayrollRunCorrectionsReadInput(req) {
  return {
    tenantId: requireTenantId(req),
    runId: parsePayrollRunIdParam(req),
  };
}

export function parsePayrollRunReverseInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    runId: parsePayrollRunIdParam(req),
    reason: normalizeText(req.body?.reason, "reason", 500, { required: true }),
    note: normalizeText(req.body?.note, "note", 500),
    idempotencyKey: normalizeText(
      req.body?.idempotencyKey ?? req.body?.idempotency_key,
      "idempotencyKey",
      190
    ),
  };
}

export function parsePayrollCorrectionShellCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);

  return {
    tenantId,
    userId,
    correctionType: normalizeEnum(
      req.body?.correctionType ?? req.body?.correction_type,
      "correctionType",
      CORRECTION_SHELL_TYPES
    ),
    originalRunId: optionalPositiveInt(
      req.body?.originalRunId ?? req.body?.original_run_id,
      "originalRunId"
    ),
    entityCode: normalizeCode(
      req.body?.entityCode ?? req.body?.entity_code,
      "entityCode",
      60
    ),
    providerCode: normalizeCode(
      req.body?.providerCode ?? req.body?.provider_code,
      "providerCode",
      60
    ),
    payrollPeriod: (() => {
      const raw = req.body?.payrollPeriod ?? req.body?.payroll_period;
      if (raw === undefined || raw === null || raw === "") {
        return null;
      }
      return parseDateOnly(raw, "payrollPeriod");
    })(),
    payDate: (() => {
      const raw = req.body?.payDate ?? req.body?.pay_date;
      if (raw === undefined || raw === null || raw === "") {
        return null;
      }
      return parseDateOnly(raw, "payDate");
    })(),
    currencyCode: (() => {
      const raw = req.body?.currencyCode ?? req.body?.currency_code;
      if (raw === undefined || raw === null || raw === "") {
        return null;
      }
      return normalizeCurrencyCode(raw, "currencyCode");
    })(),
    reason: normalizeText(req.body?.reason, "reason", 500),
    idempotencyKey: normalizeText(
      req.body?.idempotencyKey ?? req.body?.idempotency_key,
      "idempotencyKey",
      190
    ),
  };
}

