import { badRequest, parsePositiveInt } from "./_utils.js";

export function requireTenantId(req, label = "tenantId") {
  const tenantId = parsePositiveInt(req.user?.tenantId ?? req.body?.tenantId ?? req.query?.tenantId);
  if (!tenantId) {
    throw badRequest(`${label} is required`);
  }
  return tenantId;
}

export function requireUserId(req, label = "Authenticated user") {
  const userId = parsePositiveInt(req.user?.userId);
  if (!userId) {
    throw badRequest(`${label} is required`);
  }
  return userId;
}

export function optionalPositiveInt(value, label) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const parsed = parsePositiveInt(value);
  if (!parsed) {
    throw badRequest(`${label} must be a positive integer`);
  }
  return parsed;
}

export function requirePositiveInt(value, label) {
  const parsed = optionalPositiveInt(value, label);
  if (!parsed) {
    throw badRequest(`${label} must be a positive integer`);
  }
  return parsed;
}

export function normalizeCode(value, label, maxLength) {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!normalized) {
    throw badRequest(`${label} is required`);
  }
  if (normalized.length > maxLength) {
    throw badRequest(`${label} cannot exceed ${maxLength} characters`);
  }
  return normalized;
}

export function normalizeText(value, label, maxLength, { required = false } = {}) {
  const normalized = String(value || "").trim();
  if (!normalized) {
    if (required) {
      throw badRequest(`${label} is required`);
    }
    return null;
  }
  if (normalized.length > maxLength) {
    throw badRequest(`${label} cannot exceed ${maxLength} characters`);
  }
  return normalized;
}

export function normalizeCurrencyCode(value, label = "currencyCode") {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!normalized || normalized.length !== 3) {
    throw badRequest(`${label} must be a 3-letter code`);
  }
  return normalized;
}

export function normalizeEnum(value, label, allowedValues, fallback = null) {
  const normalized = String(value || fallback || "")
    .trim()
    .toUpperCase();
  if (!normalized) {
    throw badRequest(`${label} is required`);
  }
  if (!allowedValues.includes(normalized)) {
    throw badRequest(`${label} must be one of ${allowedValues.join(", ")}`);
  }
  return normalized;
}

export function parseBooleanFlag(value, fallback = false) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  if (typeof value === "boolean") {
    return value;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "n"].includes(normalized)) {
    return false;
  }
  throw badRequest("Boolean value is invalid");
}

export function parseAmount(
  value,
  label,
  { allowZero = false, required = false, allowNegative = false } = {}
) {
  if (value === undefined || value === null || value === "") {
    if (required) {
      throw badRequest(`${label} is required`);
    }
    return null;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw badRequest(`${label} must be a numeric value`);
  }

  if (allowNegative) {
    if (!allowZero && parsed === 0) {
      throw badRequest(`${label} must be non-zero`);
    }
  } else if (allowZero) {
    if (parsed < 0) {
      throw badRequest(`${label} must be >= 0`);
    }
  } else if (parsed <= 0) {
    throw badRequest(`${label} must be > 0`);
  }

  return parsed.toFixed(6);
}

export function parseDateOnly(value, label, fallback = null) {
  const source = value ?? fallback;
  const raw = String(source || "").trim();
  if (!raw) {
    throw badRequest(`${label} is required`);
  }

  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw badRequest(`${label} must be YYYY-MM-DD`);
  }

  const parsed = new Date(`${raw}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== raw) {
    throw badRequest(`${label} must be a valid date`);
  }

  return raw;
}

export function parseDateTime(value, label, fallback = null) {
  const source = value ?? fallback;
  if (source === null || source === undefined || source === "") {
    throw badRequest(`${label} is required`);
  }

  const parsed = new Date(source);
  if (Number.isNaN(parsed.getTime())) {
    throw badRequest(`${label} must be a valid datetime`);
  }

  return parsed.toISOString().slice(0, 19).replace("T", " ");
}

export function parsePagination(query = {}, defaults = {}) {
  const fallbackLimit = Number(defaults.limit || 50);
  const fallbackOffset = Number(defaults.offset || 0);
  const maxLimit = Number(defaults.maxLimit || 200);

  const rawLimit = Number(query.limit);
  const rawOffset = Number(query.offset);

  const limit =
    Number.isInteger(rawLimit) && rawLimit > 0
      ? Math.min(rawLimit, maxLimit)
      : fallbackLimit;
  const offset =
    Number.isInteger(rawOffset) && rawOffset >= 0 ? rawOffset : fallbackOffset;

  return { limit, offset };
}

