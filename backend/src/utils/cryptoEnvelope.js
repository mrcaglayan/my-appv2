import crypto from "node:crypto";

function parseKeysFromEnv() {
  const raw = String(process.env.APP_ENCRYPTION_KEYS_JSON || "{}").trim() || "{}";
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("APP_ENCRYPTION_KEYS_JSON must be valid JSON");
  }

  const out = {};
  for (const [kid, base64Key] of Object.entries(parsed || {})) {
    const key = Buffer.from(String(base64Key || ""), "base64");
    if (key.length !== 32) {
      throw new Error(`Encryption key ${kid} must be 32 bytes (base64-decoded)`);
    }
    out[String(kid)] = key;
  }
  return out;
}

function getKeyMaterial() {
  const keys = parseKeysFromEnv();
  const activeKid = String(process.env.APP_ENCRYPTION_ACTIVE_KEY_VERSION || "").trim();
  if (!activeKid) {
    throw new Error("APP_ENCRYPTION_ACTIVE_KEY_VERSION is required");
  }
  if (!keys[activeKid]) {
    throw new Error(`Active encryption key version not found: ${activeKid}`);
  }
  return { keys, activeKid, activeKey: keys[activeKid] };
}

export function encryptJson(value) {
  const { activeKid, activeKey } = getKeyMaterial();
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", activeKey, iv);
  const plaintext = Buffer.from(JSON.stringify(value ?? {}), "utf8");
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return {
    alg: "AES-256-GCM",
    kid: activeKid,
    iv_b64: iv.toString("base64"),
    tag_b64: tag.toString("base64"),
    ct_b64: ciphertext.toString("base64"),
    enc_at: new Date().toISOString(),
  };
}

export function decryptJson(envelope) {
  if (!envelope || typeof envelope !== "object") {
    return {};
  }
  const { keys } = getKeyMaterial();
  const key = keys[String(envelope.kid || "")];
  if (!key) {
    throw new Error(`Missing decryption key for kid=${envelope.kid}`);
  }
  const iv = Buffer.from(String(envelope.iv_b64 || ""), "base64");
  const tag = Buffer.from(String(envelope.tag_b64 || ""), "base64");
  const ciphertext = Buffer.from(String(envelope.ct_b64 || ""), "base64");
  const decipher = crypto.createDecipheriv("aes-256-gcm", key, iv);
  decipher.setAuthTag(tag);
  const plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
  return JSON.parse(plaintext.toString("utf8"));
}

export function serializeEnvelope(envelope) {
  return JSON.stringify(envelope ?? null);
}

export function parseEnvelopeText(text) {
  if (!text) return null;
  try {
    return JSON.parse(String(text));
  } catch {
    return null;
  }
}

export function assertEncryptionConfigured() {
  const env = String(process.env.NODE_ENV || "development")
    .trim()
    .toLowerCase();
  if (["production", "staging"].includes(env)) {
    getKeyMaterial();
  }
}

export default {
  encryptJson,
  decryptJson,
  serializeEnvelope,
  parseEnvelopeText,
  assertEncryptionConfigured,
};

