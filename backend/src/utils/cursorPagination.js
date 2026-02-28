import { badRequest, parsePositiveInt } from "../routes/_utils.js";

function base64UrlEncodeUtf8(text) {
  return Buffer.from(String(text ?? ""), "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function base64UrlDecodeUtf8(token) {
  const normalized = String(token || "")
    .trim()
    .replace(/-/g, "+")
    .replace(/_/g, "/");
  const padded =
    normalized + "=".repeat((4 - (normalized.length % 4 || 4)) % 4);
  return Buffer.from(padded, "base64").toString("utf8");
}

export function encodeCursorToken(payload) {
  return base64UrlEncodeUtf8(JSON.stringify(payload ?? {}));
}

export function decodeCursorToken(token, label = "cursor") {
  if (token === undefined || token === null || token === "") {
    return null;
  }
  const raw = String(token).trim();
  if (!raw || raw.length > 1200) {
    throw badRequest(`${label} is invalid`);
  }
  try {
    const decoded = base64UrlDecodeUtf8(raw);
    const parsed = JSON.parse(decoded);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("invalid-cursor-shape");
    }
    return parsed;
  } catch {
    throw badRequest(`${label} is invalid`);
  }
}

export function requireCursorId(cursorObj, fieldName = "id", label = "cursor") {
  const value = parsePositiveInt(cursorObj?.[fieldName]);
  if (!value) {
    throw badRequest(`${label} is invalid`);
  }
  return value;
}

export function requireCursorDateOnly(cursorObj, fieldName, label = "cursor") {
  const value = String(cursorObj?.[fieldName] || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw badRequest(`${label} is invalid`);
  }
  return value;
}

export function requireCursorDateTime(cursorObj, fieldName, label = "cursor") {
  const raw = cursorObj?.[fieldName];
  if (raw === undefined || raw === null || raw === "") {
    throw badRequest(`${label} is invalid`);
  }
  const text = String(raw).trim();
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(text)) {
    return text;
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    throw badRequest(`${label} is invalid`);
  }
  return parsed.toISOString().slice(0, 19).replace("T", " ");
}

export function toCursorDateOnly(value) {
  if (!value) return null;
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

export function toCursorDateTime(value) {
  if (!value) return null;
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 19).replace("T", " ");
}

