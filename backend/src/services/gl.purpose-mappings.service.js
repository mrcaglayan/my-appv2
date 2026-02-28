import { query } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";

const CARI_REQUIRED_PURPOSE_CODES = Object.freeze([
  "CARI_AR_CONTROL",
  "CARI_AR_OFFSET",
  "CARI_AP_CONTROL",
  "CARI_AP_OFFSET",
]);
const CARI_CONTEXT_PURPOSE_CODES = Object.freeze([
  "CARI_AR_CONTROL_CASH",
  "CARI_AR_OFFSET_CASH",
  "CARI_AP_CONTROL_CASH",
  "CARI_AP_OFFSET_CASH",
  "CARI_AR_CONTROL_MANUAL",
  "CARI_AR_OFFSET_MANUAL",
  "CARI_AP_CONTROL_MANUAL",
  "CARI_AP_OFFSET_MANUAL",
  "CARI_AR_CONTROL_ON_ACCOUNT",
  "CARI_AR_OFFSET_ON_ACCOUNT",
  "CARI_AP_CONTROL_ON_ACCOUNT",
  "CARI_AP_OFFSET_ON_ACCOUNT",
]);
const CARI_PURPOSE_CODES = Object.freeze([
  ...CARI_REQUIRED_PURPOSE_CODES,
  ...CARI_CONTEXT_PURPOSE_CODES,
]);
const CARI_PURPOSE_CODE_SET = new Set(CARI_PURPOSE_CODES);
const SHAREHOLDER_PURPOSE_PREFIX = "SHAREHOLDER_";
const SHAREHOLDER_CONFIG_ENDPOINT = "/api/v1/org/shareholder-journal-config";

function toDbBoolean(value) {
  return value === true || Number(value) === 1;
}

function normalizePurposeCode(value) {
  const purposeCode = String(value || "")
    .trim()
    .toUpperCase();
  if (!purposeCode) {
    throw badRequest("purposeCode is required");
  }
  return purposeCode;
}

function assertPurposeCodeSupportedForCariMapping(purposeCode) {
  if (purposeCode.startsWith(SHAREHOLDER_PURPOSE_PREFIX)) {
    throw badRequest(
      `Shareholder purpose codes must be configured via ${SHAREHOLDER_CONFIG_ENDPOINT}`
    );
  }
  if (!CARI_PURPOSE_CODE_SET.has(purposeCode)) {
    throw badRequest(
      `purposeCode must be one of: ${CARI_PURPOSE_CODES.join(", ")}`
    );
  }
}

function mapPurposeMappingRow(row, legalEntityId) {
  if (!row) {
    return null;
  }

  const accountId = parsePositiveInt(row.account_id);
  const accountTenantId = parsePositiveInt(row.tenant_id);
  const accountLegalEntityId = parsePositiveInt(row.coa_legal_entity_id);
  const scope = String(row.coa_scope || "").toUpperCase();
  const isActive = toDbBoolean(row.is_active);
  const allowPosting = toDbBoolean(row.allow_posting);
  const accountInLegalEntityChart =
    scope === "LEGAL_ENTITY" && accountLegalEntityId === legalEntityId;

  return {
    purposeCode: String(row.purpose_code || "").trim().toUpperCase(),
    accountId,
    accountCode: String(row.account_code || ""),
    accountName: String(row.account_name || ""),
    accountType: String(row.account_type || "").toUpperCase(),
    normalSide: String(row.normal_side || "").toUpperCase(),
    isActive,
    allowPosting,
    validForCariPosting:
      Boolean(accountId) &&
      Boolean(accountTenantId) &&
      accountInLegalEntityChart &&
      isActive &&
      allowPosting,
  };
}

async function loadAccountForPurposeMapping({
  tenantId,
  legalEntityId,
  accountId,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT
       a.id AS account_id,
       a.code AS account_code,
       a.name AS account_name,
       a.account_type,
       a.normal_side,
       a.is_active,
       a.allow_posting,
       c.tenant_id,
       c.scope AS coa_scope,
       c.legal_entity_id AS coa_legal_entity_id
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE a.id = ?
       AND c.tenant_id = ?
     LIMIT 1`,
    [accountId, tenantId]
  );

  const row = result.rows?.[0] || null;
  if (!row) {
    throw badRequest("accountId not found for tenant");
  }

  const scope = String(row.coa_scope || "").toUpperCase();
  if (scope !== "LEGAL_ENTITY") {
    throw badRequest("accountId must belong to a LEGAL_ENTITY chart");
  }

  const accountLegalEntityId = parsePositiveInt(row.coa_legal_entity_id);
  if (accountLegalEntityId !== legalEntityId) {
    throw badRequest("accountId must belong to selected legalEntityId");
  }

  if (!toDbBoolean(row.is_active)) {
    throw badRequest("accountId must reference an active account");
  }

  if (!toDbBoolean(row.allow_posting)) {
    throw badRequest("accountId must reference a postable account");
  }

  return row;
}

export function getCariPurposeCodes() {
  return [...CARI_PURPOSE_CODES];
}

export async function listPurposeMappings({
  tenantId,
  legalEntityId,
  runQuery = query,
}) {
  const normalizedTenantId = parsePositiveInt(tenantId);
  const normalizedLegalEntityId = parsePositiveInt(legalEntityId);
  if (!normalizedTenantId) {
    throw badRequest("tenantId is required");
  }
  if (!normalizedLegalEntityId) {
    throw badRequest("legalEntityId is required");
  }

  const placeholders = CARI_PURPOSE_CODES.map(() => "?").join(", ");
  const result = await runQuery(
    `SELECT
       jpa.purpose_code,
       a.id AS account_id,
       a.code AS account_code,
       a.name AS account_name,
       a.account_type,
       a.normal_side,
       a.is_active,
       a.allow_posting,
       c.tenant_id,
       c.scope AS coa_scope,
       c.legal_entity_id AS coa_legal_entity_id
     FROM journal_purpose_accounts jpa
     LEFT JOIN accounts a ON a.id = jpa.account_id
     LEFT JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE jpa.tenant_id = ?
       AND jpa.legal_entity_id = ?
       AND jpa.purpose_code IN (${placeholders})`,
    [normalizedTenantId, normalizedLegalEntityId, ...CARI_PURPOSE_CODES]
  );

  const byPurposeCode = new Map(
    (result.rows || []).map((row) => [
      String(row.purpose_code || "").trim().toUpperCase(),
      mapPurposeMappingRow(row, normalizedLegalEntityId),
    ])
  );

  return CARI_PURPOSE_CODES.map((purposeCode) => {
    const existing = byPurposeCode.get(purposeCode);
    if (existing) {
      return existing;
    }
    return {
      purposeCode,
      accountId: null,
      accountCode: null,
      accountName: null,
      accountType: null,
      normalSide: null,
      isActive: false,
      allowPosting: false,
      validForCariPosting: false,
    };
  });
}

export async function upsertPurposeMapping({
  tenantId,
  legalEntityId,
  purposeCode,
  accountId,
  runQuery = query,
}) {
  const normalizedTenantId = parsePositiveInt(tenantId);
  const normalizedLegalEntityId = parsePositiveInt(legalEntityId);
  const normalizedAccountId = parsePositiveInt(accountId);
  const normalizedPurposeCode = normalizePurposeCode(purposeCode);

  if (!normalizedTenantId) {
    throw badRequest("tenantId is required");
  }
  if (!normalizedLegalEntityId) {
    throw badRequest("legalEntityId is required");
  }
  if (!normalizedAccountId) {
    throw badRequest("accountId must be a positive integer");
  }
  assertPurposeCodeSupportedForCariMapping(normalizedPurposeCode);

  const accountRow = await loadAccountForPurposeMapping({
    tenantId: normalizedTenantId,
    legalEntityId: normalizedLegalEntityId,
    accountId: normalizedAccountId,
    runQuery,
  });

  await runQuery(
    `INSERT INTO journal_purpose_accounts (
        tenant_id,
        legal_entity_id,
        purpose_code,
        account_id
     )
     VALUES (?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       account_id = VALUES(account_id),
       updated_at = CURRENT_TIMESTAMP`,
    [
      normalizedTenantId,
      normalizedLegalEntityId,
      normalizedPurposeCode,
      normalizedAccountId,
    ]
  );

  return {
    purposeCode: normalizedPurposeCode,
    accountId: normalizedAccountId,
    accountCode: String(accountRow.account_code || ""),
    accountName: String(accountRow.account_name || ""),
    accountType: String(accountRow.account_type || "").toUpperCase(),
    normalSide: String(accountRow.normal_side || "").toUpperCase(),
    isActive: true,
    allowPosting: true,
    validForCariPosting: true,
  };
}
