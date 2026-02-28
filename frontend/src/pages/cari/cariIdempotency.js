const STORAGE_PREFIX = "cari:settlement:pending";

export function buildSettlementIntentScope(form = {}) {
  const legalEntityId = String(form?.legalEntityId || "na");
  const counterpartyId = String(form?.counterpartyId || "na");
  const direction = String(form?.direction || "na");
  return `${legalEntityId}:${counterpartyId}:${direction}`;
}

export function buildSettlementIntentFingerprint(form = {}) {
  return JSON.stringify({
    currencyCode: String(form?.currencyCode || ""),
    incomingAmountTxn: Number(form?.incomingAmountTxn || 0),
    settlementDate: String(form?.settlementDate || ""),
  });
}

function getScopedStorageKey(intentScope) {
  return `${STORAGE_PREFIX}:${intentScope || "na:na:na"}`;
}

export function loadPendingIdempotencyKey(intentScope, intentFingerprint) {
  const raw = sessionStorage.getItem(getScopedStorageKey(intentScope)) || "";
  if (!raw) return "";
  try {
    const parsed = JSON.parse(raw);
    if (!parsed?.key) return "";
    if (intentFingerprint && parsed?.fingerprint !== intentFingerprint) return "";
    return String(parsed.key);
  } catch {
    return "";
  }
}

export function createPendingIdempotencyKey(intentScope, intentFingerprint) {
  const existing = loadPendingIdempotencyKey(intentScope, intentFingerprint);
  if (existing) return existing;
  const key = `CARI-SET-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  sessionStorage.setItem(
    getScopedStorageKey(intentScope),
    JSON.stringify({ key, fingerprint: intentFingerprint || "" })
  );
  return key;
}

export function clearPendingIdempotencyKey(intentScope) {
  sessionStorage.removeItem(getScopedStorageKey(intentScope));
}

export function shouldClearPendingKeyAfterError(error) {
  const status = Number(error?.status || error?.response?.status || 0);
  return status === 400 || status === 401 || status === 403;
}

export function createEphemeralIdempotencyKey(prefix = "CARI-IDEMP") {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}
