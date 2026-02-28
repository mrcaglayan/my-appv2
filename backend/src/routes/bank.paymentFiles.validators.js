import { badRequest, parsePositiveInt } from "./_utils.js";
import { requireTenantId, requireUserId } from "./cash.validators.common.js";

function normalizeText(value, label, maxLen, { required = false } = {}) {
  const text = String(value || "").trim();
  if (!text) {
    if (required) throw badRequest(`${label} is required`);
    return null;
  }
  if (text.length > maxLen) {
    throw badRequest(`${label} cannot exceed ${maxLen} characters`);
  }
  return text;
}

function normalizeFileFormatCode(value) {
  return (
    String(value || "GENERIC_CSV_V1")
      .trim()
      .toUpperCase() || "GENERIC_CSV_V1"
  );
}

function parseBatchIdParam(req) {
  const batchId = parsePositiveInt(req.params?.id ?? req.params?.batchId);
  if (!batchId) throw badRequest("batchId must be a positive integer");
  return batchId;
}

export function parseBankPaymentBatchExportsListInput(req) {
  return {
    tenantId: requireTenantId(req),
    batchId: parseBatchIdParam(req),
  };
}

export function parseBankPaymentBatchExportCreateInput(req) {
  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    batchId: parseBatchIdParam(req),
    fileFormatCode: normalizeFileFormatCode(
      req.body?.fileFormatCode ?? req.body?.file_format_code
    ),
    exportRequestId: normalizeText(
      req.body?.exportRequestId ?? req.body?.export_request_id,
      "exportRequestId",
      190
    ),
    markSent:
      String(req.body?.markSent ?? req.body?.mark_sent ?? "")
        .trim()
        .toLowerCase() === "true" ||
      String(req.body?.markSent ?? req.body?.mark_sent ?? "").trim() === "1",
  };
}

export function parseBankPaymentBatchAckImportListInput(req) {
  return {
    tenantId: requireTenantId(req),
    batchId: parseBatchIdParam(req),
  };
}

export function parseBankPaymentBatchAckImportCreateInput(req) {
  const ackText = String(req.body?.ackText ?? req.body?.ack_text ?? "");
  if (!ackText.trim()) throw badRequest("ackText is required");

  return {
    tenantId: requireTenantId(req),
    userId: requireUserId(req),
    batchId: parseBatchIdParam(req),
    fileFormatCode: normalizeFileFormatCode(
      req.body?.fileFormatCode ?? req.body?.file_format_code
    ),
    ackRequestId: normalizeText(
      req.body?.ackRequestId ?? req.body?.ack_request_id,
      "ackRequestId",
      190
    ),
    fileName:
      normalizeText(req.body?.fileName ?? req.body?.file_name, "fileName", 255) ||
      "ack.csv",
    exportId: (() => {
      const raw = req.body?.exportId ?? req.body?.export_id;
      if (raw === undefined || raw === null || raw === "") return null;
      const parsed = parsePositiveInt(raw);
      if (!parsed) throw badRequest("exportId must be a positive integer");
      return parsed;
    })(),
    ackText,
  };
}

export default {
  parseBankPaymentBatchExportsListInput,
  parseBankPaymentBatchExportCreateInput,
  parseBankPaymentBatchAckImportListInput,
  parseBankPaymentBatchAckImportCreateInput,
};
