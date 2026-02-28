const DEFAULT_SENSITIVE_KEY_FRAGMENTS = [
  "password",
  "secret",
  "token",
  "api_key",
  "apikey",
  "access_token",
  "refresh_token",
  "client_secret",
  "private_key",
  "webhook_secret",
  "authorization",
  "iban",
  "account_number",
  "routing_number",
];

export function maskString(value, { keepEnd = 4 } = {}) {
  const text = String(value || "");
  if (!text) return "";
  if (text.length <= keepEnd) return "*".repeat(text.length);
  return `${"*".repeat(Math.max(4, text.length - keepEnd))}${text.slice(-keepEnd)}`;
}

function looksSensitiveKey(key) {
  const normalized = String(key || "").toLowerCase();
  return DEFAULT_SENSITIVE_KEY_FRAGMENTS.some((frag) => normalized.includes(frag));
}

export function redactObject(input) {
  if (input === null || input === undefined) return input;
  if (Array.isArray(input)) return input.map((item) => redactObject(item));
  if (typeof input !== "object") return input;

  const out = {};
  for (const [key, value] of Object.entries(input)) {
    if (looksSensitiveKey(key)) {
      out[key] = typeof value === "string" ? maskString(value) : "***";
      continue;
    }
    if (Array.isArray(value)) {
      out[key] = value.map((item) => (typeof item === "object" && item !== null ? redactObject(item) : item));
      continue;
    }
    if (typeof value === "object" && value !== null) {
      out[key] = redactObject(value);
      continue;
    }
    out[key] = value;
  }
  return out;
}

export function redactRawPayloadText(text, { maxLen = 4000 } = {}) {
  const raw = String(text || "");
  if (!raw) return raw;

  try {
    const parsed = JSON.parse(raw);
    const redacted = JSON.stringify(redactObject(parsed));
    return redacted.length > maxLen ? `${redacted.slice(0, maxLen)}...[TRUNCATED]` : redacted;
  } catch {
    let out = raw
      .replace(/(authorization\s*[:=]\s*)(.+)/gi, "$1***")
      .replace(/(token\s*[:=]\s*)(.+)/gi, "$1***")
      .replace(/(secret\s*[:=]\s*)(.+)/gi, "$1***");
    if (out.length > maxLen) out = `${out.slice(0, maxLen)}...[TRUNCATED]`;
    return out;
  }
}

export default {
  maskString,
  redactObject,
  redactRawPayloadText,
};

