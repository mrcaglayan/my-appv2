function toUpper(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

export function normalizeLookupQuery(value) {
  return String(value || "").trim();
}

export function resolveInlineCounterpartyRoleFlags(direction) {
  const normalized = toUpper(direction);
  if (normalized === "AR") {
    return { isCustomer: true, isVendor: false };
  }
  if (normalized === "AP") {
    return { isCustomer: false, isVendor: true };
  }
  return { isCustomer: true, isVendor: true };
}

function sanitizeCodeSegment(value, fallback = "CARD") {
  const normalized = toUpper(value)
    .replace(/[^A-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || fallback;
}

function buildUtcStamp() {
  return new Date()
    .toISOString()
    .replace(/[-:]/g, "")
    .replace("T", "")
    .slice(2, 14);
}

function buildRandomSuffix(length = 4) {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let output = "";
  for (let index = 0; index < length; index += 1) {
    const position = Math.floor(Math.random() * alphabet.length);
    output += alphabet[position];
  }
  return output;
}

export function buildInlineCounterpartyCode({ legalEntityId, name }) {
  const legalEntitySegment = sanitizeCodeSegment(legalEntityId, "LE");
  const nameSegment = sanitizeCodeSegment(name, "CARD").slice(0, 16);
  const stamp = buildUtcStamp();
  const suffix = buildRandomSuffix(4);
  return `CP-${legalEntitySegment}-${nameSegment}-${stamp}-${suffix}`.slice(0, 60);
}

export function prependOrReplaceCounterpartyOption(rows, nextRow) {
  const nextId = Number(nextRow?.id || 0);
  if (!nextId) {
    return Array.isArray(rows) ? rows : [];
  }
  const base = Array.isArray(rows) ? rows : [];
  const filtered = base.filter((row) => Number(row?.id || 0) !== nextId);
  return [nextRow, ...filtered];
}
