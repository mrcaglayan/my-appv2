const DEFAULT_AUTH_COOKIE_NAME = "saap_access";
const DEFAULT_AUTH_COOKIE_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
const VALID_SAME_SITE_VALUES = new Set(["lax", "strict", "none"]);

function parseBooleanEnv(value, fallback = false) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }

  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function parsePositiveIntEnv(value, fallback) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function normalizeSameSite(value, fallback = "lax") {
  const normalized = String(value || fallback).trim().toLowerCase();
  if (!VALID_SAME_SITE_VALUES.has(normalized)) {
    return fallback;
  }
  return normalized;
}

function getAuthCookieSecure() {
  if (process.env.AUTH_COOKIE_SECURE === undefined) {
    return String(process.env.NODE_ENV || "").toLowerCase() === "production";
  }
  return parseBooleanEnv(process.env.AUTH_COOKIE_SECURE, false);
}

function getAuthCookieSameSite() {
  const fallback = getAuthCookieSecure() ? "none" : "lax";
  return normalizeSameSite(process.env.AUTH_COOKIE_SAMESITE, fallback);
}

function getAuthCookiePath() {
  const path = String(process.env.AUTH_COOKIE_PATH || "/").trim();
  return path || "/";
}

function getAuthCookieDomain() {
  const domain = String(process.env.AUTH_COOKIE_DOMAIN || "").trim();
  return domain || null;
}

function getAuthCookieMaxAgeMs() {
  return parsePositiveIntEnv(
    process.env.AUTH_COOKIE_MAX_AGE_MS,
    DEFAULT_AUTH_COOKIE_MAX_AGE_MS
  );
}

function parseCookieHeader(cookieHeader) {
  const cookieMap = new Map();
  const rawHeader = String(cookieHeader || "").trim();
  if (!rawHeader) {
    return cookieMap;
  }

  const parts = rawHeader.split(";");
  for (const part of parts) {
    const segment = String(part || "").trim();
    if (!segment) {
      continue;
    }

    const separatorIndex = segment.indexOf("=");
    if (separatorIndex <= 0) {
      continue;
    }

    const name = segment.slice(0, separatorIndex).trim();
    const rawValue = segment.slice(separatorIndex + 1).trim();
    if (!name) {
      continue;
    }

    try {
      cookieMap.set(name, decodeURIComponent(rawValue));
    } catch {
      cookieMap.set(name, rawValue);
    }
  }

  return cookieMap;
}

export function getAuthCookieName() {
  const configuredName = String(
    process.env.AUTH_COOKIE_NAME || DEFAULT_AUTH_COOKIE_NAME
  ).trim();
  return configuredName || DEFAULT_AUTH_COOKIE_NAME;
}

export function getAuthCookieOptions() {
  const options = {
    httpOnly: true,
    secure: getAuthCookieSecure(),
    sameSite: getAuthCookieSameSite(),
    path: getAuthCookiePath(),
    maxAge: getAuthCookieMaxAgeMs(),
  };

  const domain = getAuthCookieDomain();
  if (domain) {
    options.domain = domain;
  }

  return options;
}

export function getAuthCookieClearOptions() {
  const options = getAuthCookieOptions();
  delete options.maxAge;
  return options;
}

export function readCookieValue(req, cookieName = getAuthCookieName()) {
  const cookies = parseCookieHeader(req?.headers?.cookie);
  const value = cookies.get(cookieName);
  if (!value) {
    return null;
  }
  return String(value);
}

