import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeText,
  parseBooleanFlag,
  parseDateOnly,
  parsePagination,
  requirePositiveInt,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const POLICY_STATUS_VALUES = new Set(["ACTIVE", "PAUSED", "DISABLED"]);
const RUN_STATUS_VALUES = new Set(["RUNNING", "COMPLETED", "PARTIAL", "FAILED"]);
const RUN_TRIGGER_MODE_VALUES = new Set(["MANUAL", "SCHEDULED", "JOB"]);

function up(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function parsePolicyIdParam(req) {
  const policyId = parsePositiveInt(req.params?.policyId ?? req.params?.id);
  if (!policyId) throw badRequest("policyId must be a positive integer");
  return policyId;
}

function parseRunIdParam(req) {
  const runId = parsePositiveInt(req.params?.runId ?? req.params?.id);
  if (!runId) throw badRequest("runId must be a positive integer");
  return runId;
}

function parseSnapshotIdParam(req) {
  const snapshotId = parsePositiveInt(req.params?.snapshotId ?? req.params?.id);
  if (!snapshotId) throw badRequest("snapshotId must be a positive integer");
  return snapshotId;
}

function optionalPositiveInt(value, label) {
  if (value === undefined || value === null || value === "") return null;
  const parsed = parsePositiveInt(value);
  if (!parsed) throw badRequest(`${label} must be a positive integer`);
  return parsed;
}

function optionalUpper(value, label, maxLength = 80) {
  if (value === undefined || value === null || value === "") return null;
  const parsed = up(value);
  if (!parsed) return null;
  if (parsed.length > maxLength) {
    throw badRequest(`${label} cannot exceed ${maxLength} characters`);
  }
  return parsed;
}

function optionalDate(value, label) {
  if (value === undefined || value === null || value === "") return null;
  return parseDateOnly(value, label);
}

function optionalRetentionDays(value) {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 36500) {
    throw badRequest("retentionDays must be an integer between 1 and 36500");
  }
  return parsed;
}

function optionalObject(value, label) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value !== "object" || Array.isArray(value)) {
    throw badRequest(`${label} must be a JSON object`);
  }
  return value;
}

function parsePolicyStatus(value) {
  const status = optionalUpper(value, "status", 20);
  if (!status) return null;
  if (!POLICY_STATUS_VALUES.has(status)) {
    throw badRequest(`status must be one of: ${Array.from(POLICY_STATUS_VALUES).join(", ")}`);
  }
  return status;
}

function parseRunStatus(value) {
  const status = optionalUpper(value, "status", 20);
  if (!status) return null;
  if (!RUN_STATUS_VALUES.has(status)) {
    throw badRequest(`status must be one of: ${Array.from(RUN_STATUS_VALUES).join(", ")}`);
  }
  return status;
}

function parseTriggerMode(value) {
  const triggerMode = optionalUpper(value, "triggerMode", 20);
  if (!triggerMode) return null;
  if (!RUN_TRIGGER_MODE_VALUES.has(triggerMode)) {
    throw badRequest(
      `triggerMode must be one of: ${Array.from(RUN_TRIGGER_MODE_VALUES).join(", ")}`
    );
  }
  return triggerMode;
}

export function parseRetentionPolicyListInput(req) {
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });
  return {
    tenantId: requireTenantId(req),
    legalEntityId: optionalPositiveInt(req.query?.legalEntityId ?? req.query?.legal_entity_id, "legalEntityId"),
    datasetCode: optionalUpper(req.query?.datasetCode ?? req.query?.dataset_code, "datasetCode", 60),
    actionCode: optionalUpper(req.query?.actionCode ?? req.query?.action_code, "actionCode", 20),
    status: parsePolicyStatus(req.query?.status),
    q: normalizeText(req.query?.q, "q", 120),
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseRetentionPolicyCreateInput(req) {
  const datasetCode = optionalUpper(req.body?.datasetCode ?? req.body?.dataset_code, "datasetCode", 60);
  const actionCode = optionalUpper(req.body?.actionCode ?? req.body?.action_code, "actionCode", 20);

  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    policyCode: optionalUpper(req.body?.policyCode ?? req.body?.policy_code, "policyCode", 60),
    policyName: normalizeText(req.body?.policyName ?? req.body?.policy_name, "policyName", 190, {
      required: true,
    }),
    datasetCode,
    actionCode,
    retentionDays: requirePositiveInt(req.body?.retentionDays ?? req.body?.retention_days, "retentionDays"),
    legalEntityId: optionalPositiveInt(req.body?.legalEntityId ?? req.body?.legal_entity_id, "legalEntityId"),
    status: parsePolicyStatus(req.body?.status) || "ACTIVE",
    configJson: optionalObject(req.body?.configJson ?? req.body?.config_json, "configJson"),
  };
}

export function parseRetentionPolicyUpdateInput(req) {
  const input = {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    policyId: parsePolicyIdParam(req),
    policyName: normalizeText(req.body?.policyName ?? req.body?.policy_name, "policyName", 190),
    retentionDays: optionalRetentionDays(req.body?.retentionDays ?? req.body?.retention_days),
    status: parsePolicyStatus(req.body?.status),
    configJson: req.body?.configJson ?? req.body?.config_json,
  };

  if (input.configJson !== undefined) {
    input.configJson = optionalObject(input.configJson, "configJson");
  }

  if (
    input.policyName === null &&
    input.retentionDays === null &&
    input.status === null &&
    input.configJson === undefined
  ) {
    throw badRequest("At least one updatable field is required");
  }

  return input;
}

export function parseRetentionRunExecuteInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    policyId: parsePolicyIdParam(req),
    runIdempotencyKey: normalizeText(
      req.body?.runIdempotencyKey ?? req.body?.run_idempotency_key,
      "runIdempotencyKey",
      190
    ),
    triggerMode:
      parseTriggerMode(req.body?.triggerMode ?? req.body?.trigger_mode) ||
      "MANUAL",
    asyncMode: parseBooleanFlag(req.query?.async ?? req.body?.async, false),
  };
}

export function parseRetentionRunListInput(req) {
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });
  return {
    tenantId: requireTenantId(req),
    legalEntityId: optionalPositiveInt(req.query?.legalEntityId ?? req.query?.legal_entity_id, "legalEntityId"),
    policyId: optionalPositiveInt(req.query?.policyId ?? req.query?.policy_id, "policyId"),
    datasetCode: optionalUpper(req.query?.datasetCode ?? req.query?.dataset_code, "datasetCode", 60),
    status: parseRunStatus(req.query?.status),
    triggerMode: parseTriggerMode(req.query?.triggerMode ?? req.query?.trigger_mode),
    dateFrom: optionalDate(req.query?.dateFrom ?? req.query?.date_from, "dateFrom"),
    dateTo: optionalDate(req.query?.dateTo ?? req.query?.date_to, "dateTo"),
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseRetentionRunReadInput(req) {
  return {
    tenantId: requireTenantId(req),
    runId: parseRunIdParam(req),
  };
}

export function parseExportSnapshotListInput(req) {
  const pagination = parsePagination(req.query, { limit: 100, offset: 0, maxLimit: 500 });
  return {
    tenantId: requireTenantId(req),
    legalEntityId: optionalPositiveInt(req.query?.legalEntityId ?? req.query?.legal_entity_id, "legalEntityId"),
    payrollPeriodCloseId: optionalPositiveInt(
      req.query?.payrollPeriodCloseId ?? req.query?.payroll_period_close_id ?? req.query?.closeId,
      "payrollPeriodCloseId"
    ),
    status: optionalUpper(req.query?.status, "status", 20),
    periodStart: optionalDate(req.query?.periodStart ?? req.query?.period_start, "periodStart"),
    periodEnd: optionalDate(req.query?.periodEnd ?? req.query?.period_end, "periodEnd"),
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseExportSnapshotCreateInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    payrollPeriodCloseId: requirePositiveInt(
      req.body?.payrollPeriodCloseId ?? req.body?.payroll_period_close_id ?? req.body?.closeId,
      "payrollPeriodCloseId"
    ),
    snapshotType: optionalUpper(req.body?.snapshotType ?? req.body?.snapshot_type, "snapshotType", 40),
    idempotencyKey: normalizeText(
      req.body?.idempotencyKey ?? req.body?.idempotency_key,
      "idempotencyKey",
      190
    ),
  };
}

export function parseExportSnapshotReadInput(req) {
  return {
    tenantId: requireTenantId(req),
    snapshotId: parseSnapshotIdParam(req),
  };
}
