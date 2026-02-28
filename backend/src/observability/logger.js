import crypto from "node:crypto";

const SERVICE_NAME = "saap-api";
const SHOW_ERROR_STACK =
  String(process.env.NODE_ENV || "").trim().toLowerCase() !== "production";

function normalizeRequestId(value) {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return null;
  }
  return normalized.slice(0, 80);
}

export function resolveRequestId(req, explicitRequestId = null) {
  const explicit = normalizeRequestId(explicitRequestId);
  if (explicit) {
    return explicit;
  }

  const existing = normalizeRequestId(req?.requestId);
  if (existing) {
    return existing;
  }

  const fromHeader = normalizeRequestId(req?.headers?.["x-request-id"]);
  if (fromHeader) {
    return fromHeader;
  }

  const fromCorrelationHeader = normalizeRequestId(
    req?.headers?.["x-correlation-id"]
  );
  if (fromCorrelationHeader) {
    return fromCorrelationHeader;
  }

  return crypto.randomUUID();
}

export function buildRequestLogMeta(req, extraMeta = {}) {
  return {
    requestId: resolveRequestId(req),
    method: req?.method || null,
    path: req?.originalUrl || req?.url || null,
    ...extraMeta,
  };
}

function normalizeError(err) {
  if (!err) {
    return null;
  }

  const payload = {
    name: err.name || "Error",
    message: err.message || "Unknown error",
  };

  if (SHOW_ERROR_STACK && err.stack) {
    payload.stack = String(err.stack);
  }

  if (err.status) {
    payload.status = Number(err.status);
  }

  return payload;
}

function writeLog(level, message, meta = {}, err = null) {
  const payload = {
    timestamp: new Date().toISOString(),
    level,
    service: SERVICE_NAME,
    message: String(message || ""),
    ...meta,
  };

  const normalizedError = normalizeError(err);
  if (normalizedError) {
    payload.error = normalizedError;
  }

  const line = JSON.stringify(payload);
  if (level === "error" || level === "warn") {
    console.error(line);
    return;
  }
  console.log(line);
}

export function logInfo(message, meta = {}) {
  writeLog("info", message, meta);
}

export function logWarn(message, meta = {}, err = null) {
  writeLog("warn", message, meta, err);
}

export function logError(message, meta = {}, err = null) {
  writeLog("error", message, meta, err);
}
