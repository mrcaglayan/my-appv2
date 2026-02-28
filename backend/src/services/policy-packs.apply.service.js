import { withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { getPolicyPack } from "./policy-packs.service.js";
import { upsertJournalPurposeAccountTx } from "./org.write.queries.js";
import { assertShareholderParentAccount } from "./org.shareholder.helpers.js";

const APPLY_MODES = new Set(["MERGE", "OVERWRITE"]);
const SHAREHOLDER_EXPECTED_SIDE_BY_PURPOSE = Object.freeze({
  SHAREHOLDER_CAPITAL_CREDIT_PARENT: "CREDIT",
  SHAREHOLDER_COMMITMENT_DEBIT_PARENT: "DEBIT",
});
const DISTINCT_PURPOSE_PAIRS = Object.freeze([
  Object.freeze({
    left: "CARI_AR_CONTROL",
    right: "CARI_AR_OFFSET",
    label: "CARI_AR_CONTROL and CARI_AR_OFFSET",
  }),
  Object.freeze({
    left: "CARI_AP_CONTROL",
    right: "CARI_AP_OFFSET",
    label: "CARI_AP_CONTROL and CARI_AP_OFFSET",
  }),
  Object.freeze({
    left: "CARI_AR_CONTROL_CASH",
    right: "CARI_AR_OFFSET_CASH",
    label: "CARI_AR_CONTROL_CASH and CARI_AR_OFFSET_CASH",
  }),
  Object.freeze({
    left: "CARI_AP_CONTROL_CASH",
    right: "CARI_AP_OFFSET_CASH",
    label: "CARI_AP_CONTROL_CASH and CARI_AP_OFFSET_CASH",
  }),
  Object.freeze({
    left: "CARI_AR_CONTROL_MANUAL",
    right: "CARI_AR_OFFSET_MANUAL",
    label: "CARI_AR_CONTROL_MANUAL and CARI_AR_OFFSET_MANUAL",
  }),
  Object.freeze({
    left: "CARI_AP_CONTROL_MANUAL",
    right: "CARI_AP_OFFSET_MANUAL",
    label: "CARI_AP_CONTROL_MANUAL and CARI_AP_OFFSET_MANUAL",
  }),
  Object.freeze({
    left: "CARI_AR_CONTROL_ON_ACCOUNT",
    right: "CARI_AR_OFFSET_ON_ACCOUNT",
    label: "CARI_AR_CONTROL_ON_ACCOUNT and CARI_AR_OFFSET_ON_ACCOUNT",
  }),
  Object.freeze({
    left: "CARI_AP_CONTROL_ON_ACCOUNT",
    right: "CARI_AP_OFFSET_ON_ACCOUNT",
    label: "CARI_AP_CONTROL_ON_ACCOUNT and CARI_AP_OFFSET_ON_ACCOUNT",
  }),
  Object.freeze({
    left: "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
    right: "SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
    label:
      "SHAREHOLDER_CAPITAL_CREDIT_PARENT and SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
  }),
]);

function toUpper(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function toDbBoolean(value) {
  return value === true || Number(value) === 1;
}

function normalizeApplyMode(value) {
  const normalizedMode = toUpper(value || "MERGE");
  if (!APPLY_MODES.has(normalizedMode)) {
    throw badRequest("mode must be MERGE or OVERWRITE");
  }
  return normalizedMode;
}

function normalizeRows(rows) {
  if (!Array.isArray(rows)) {
    throw badRequest("rows must be an array");
  }
  if (rows.length === 0) {
    throw badRequest("rows must include at least one mapping row");
  }

  const normalized = [];
  const seenPurposeCodes = new Set();
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i] || {};
    const purposeCode = toUpper(row.purposeCode);
    const accountId = parsePositiveInt(row.accountId);
    if (!purposeCode) {
      throw badRequest(`rows[${i}].purposeCode is required`);
    }
    if (!accountId) {
      throw badRequest(`rows[${i}].accountId must be a positive integer`);
    }
    if (seenPurposeCodes.has(purposeCode)) {
      throw badRequest(`Duplicate purposeCode in rows: ${purposeCode}`);
    }
    seenPurposeCodes.add(purposeCode);
    normalized.push({
      purposeCode,
      accountId,
    });
  }

  return normalized;
}

function buildPurposeDefinitionMap(pack) {
  const byPurposeCode = new Map();
  for (const module of pack?.modules || []) {
    const moduleKey = String(module?.moduleKey || "").trim();
    for (const target of module?.purposeTargets || []) {
      const purposeCode = toUpper(target?.purposeCode);
      if (!purposeCode) {
        continue;
      }
      if (byPurposeCode.has(purposeCode)) {
        throw badRequest(`Pack contains duplicate purpose target: ${purposeCode}`);
      }
      byPurposeCode.set(purposeCode, {
        moduleKey,
        target,
      });
    }
  }
  return byPurposeCode;
}

function assertRowsBelongToPack(rows, purposeDefinitionByCode, packId) {
  for (const row of rows) {
    if (!purposeDefinitionByCode.has(row.purposeCode)) {
      throw badRequest(
        `purposeCode ${row.purposeCode} is not managed by policy pack ${packId}`
      );
    }
  }
}

async function loadExistingManagedMappings(tx, tenantId, legalEntityId, managedPurposeCodes) {
  if (managedPurposeCodes.length === 0) {
    return new Map();
  }

  const placeholders = managedPurposeCodes.map(() => "?").join(", ");
  const result = await tx.query(
    `SELECT purpose_code, account_id
     FROM journal_purpose_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND purpose_code IN (${placeholders})`,
    [tenantId, legalEntityId, ...managedPurposeCodes]
  );

  return new Map(
    (result.rows || []).map((row) => [
      toUpper(row.purpose_code),
      parsePositiveInt(row.account_id),
    ])
  );
}

async function loadAccountValidationRow(tx, tenantId, legalEntityId, accountId, fieldLabel) {
  const result = await tx.query(
    `SELECT
       a.id,
       a.code,
       a.name,
       a.account_type,
       a.normal_side,
       a.allow_posting,
       a.is_active,
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
    throw badRequest(`${fieldLabel} not found for tenant`);
  }

  if (toUpper(row.coa_scope) !== "LEGAL_ENTITY") {
    throw badRequest(`${fieldLabel} must belong to a LEGAL_ENTITY chart`);
  }
  if (parsePositiveInt(row.coa_legal_entity_id) !== legalEntityId) {
    throw badRequest(`${fieldLabel} must belong to selected legalEntityId`);
  }
  if (!toDbBoolean(row.is_active)) {
    throw badRequest(`${fieldLabel} must reference an active account`);
  }

  return row;
}

function validateCariPurposeRow({ accountRow, definition, fieldLabel }) {
  if (!toDbBoolean(accountRow.allow_posting)) {
    throw badRequest(`${fieldLabel} must reference a postable account`);
  }

  const rules = definition?.target?.rules || {};
  if (rules.accountType && toUpper(accountRow.account_type) !== toUpper(rules.accountType)) {
    throw badRequest(`${fieldLabel} must have accountType=${toUpper(rules.accountType)}`);
  }
  if (rules.normalSide && toUpper(accountRow.normal_side) !== toUpper(rules.normalSide)) {
    throw badRequest(`${fieldLabel} must have normalSide=${toUpper(rules.normalSide)}`);
  }
}

async function validateShareholderPurposeRow({
  tx,
  tenantId,
  legalEntityId,
  purposeCode,
  accountId,
  fieldLabel,
}) {
  const expectedNormalSide = SHAREHOLDER_EXPECTED_SIDE_BY_PURPOSE[purposeCode];
  if (!expectedNormalSide) {
    throw badRequest(`Unsupported shareholder purposeCode: ${purposeCode}`);
  }
  await assertShareholderParentAccount(
    tx,
    tenantId,
    legalEntityId,
    accountId,
    fieldLabel,
    expectedNormalSide
  );
}

function buildEffectiveMapping({
  mode,
  existingByPurpose,
  providedByPurpose,
}) {
  if (mode === "OVERWRITE") {
    return new Map(providedByPurpose);
  }
  const merged = new Map(existingByPurpose);
  for (const [purposeCode, accountId] of providedByPurpose.entries()) {
    merged.set(purposeCode, accountId);
  }
  return merged;
}

function assertDistinctPurposePairs(effectiveByPurpose) {
  for (const pair of DISTINCT_PURPOSE_PAIRS) {
    const leftAccountId = parsePositiveInt(effectiveByPurpose.get(pair.left));
    const rightAccountId = parsePositiveInt(effectiveByPurpose.get(pair.right));
    if (!leftAccountId || !rightAccountId) {
      continue;
    }
    if (leftAccountId === rightAccountId) {
      throw badRequest(`${pair.label} must be mapped to different accounts`);
    }
  }
}

async function deleteManagedMappingsForOverwrite({
  tx,
  tenantId,
  legalEntityId,
  managedPurposeCodes,
}) {
  if (managedPurposeCodes.length === 0) {
    return;
  }
  const placeholders = managedPurposeCodes.map(() => "?").join(", ");
  await tx.query(
    `DELETE FROM journal_purpose_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND purpose_code IN (${placeholders})`,
    [tenantId, legalEntityId, ...managedPurposeCodes]
  );
}

async function insertApplyMetadata({
  tx,
  tenantId,
  legalEntityId,
  packId,
  mode,
  rows,
  userId,
}) {
  const payloadJson = JSON.stringify({
    packId,
    mode,
    rowCount: rows.length,
    rows: rows.map((row) => ({
      purposeCode: row.purposeCode,
      accountId: row.accountId,
    })),
  });

  const insertResult = await tx.query(
    `INSERT INTO legal_entity_policy_packs (
        tenant_id,
        legal_entity_id,
        pack_id,
        mode,
        payload_json,
        applied_by_user_id
     )
     VALUES (?, ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, packId, mode, payloadJson, userId]
  );
  const metadataId = parsePositiveInt(insertResult.rows?.insertId);
  if (!metadataId) {
    throw badRequest("Failed to write policy pack apply metadata");
  }

  const metadataResult = await tx.query(
    `SELECT id, applied_at
     FROM legal_entity_policy_packs
     WHERE id = ?
     LIMIT 1`,
    [metadataId]
  );
  const metadataRow = metadataResult.rows?.[0] || null;
  return {
    metadataId,
    appliedAt: metadataRow?.applied_at || null,
  };
}

export async function applyPolicyPack({
  tenantId,
  userId,
  legalEntityId,
  packId,
  mode,
  rows,
}) {
  const normalizedTenantId = parsePositiveInt(tenantId);
  const normalizedUserId = parsePositiveInt(userId);
  const normalizedLegalEntityId = parsePositiveInt(legalEntityId);
  const normalizedPackId = toUpper(packId);
  const normalizedMode = normalizeApplyMode(mode);
  const normalizedRows = normalizeRows(rows);

  if (!normalizedTenantId) {
    throw badRequest("tenantId is required");
  }
  if (!normalizedUserId) {
    throw badRequest("userId is required");
  }
  if (!normalizedLegalEntityId) {
    throw badRequest("legalEntityId is required");
  }
  if (!normalizedPackId) {
    throw badRequest("packId is required");
  }

  const pack = getPolicyPack(normalizedPackId);
  if (!pack) {
    return null;
  }

  const purposeDefinitionByCode = buildPurposeDefinitionMap(pack);
  assertRowsBelongToPack(normalizedRows, purposeDefinitionByCode, pack.packId);
  const managedPurposeCodes = Array.from(purposeDefinitionByCode.keys());

  return withTransaction(async (tx) => {
    const existingByPurpose = await loadExistingManagedMappings(
      tx,
      normalizedTenantId,
      normalizedLegalEntityId,
      managedPurposeCodes
    );

    const providedByPurpose = new Map();
    const validatedRows = [];
    for (let i = 0; i < normalizedRows.length; i += 1) {
      const row = normalizedRows[i];
      const fieldLabel = `rows[${i}].accountId`;
      const definition = purposeDefinitionByCode.get(row.purposeCode);
      const accountRow = await loadAccountValidationRow(
        tx,
        normalizedTenantId,
        normalizedLegalEntityId,
        row.accountId,
        fieldLabel
      );

      if (definition?.moduleKey === "shareholderCommitment") {
        await validateShareholderPurposeRow({
          tx,
          tenantId: normalizedTenantId,
          legalEntityId: normalizedLegalEntityId,
          purposeCode: row.purposeCode,
          accountId: row.accountId,
          fieldLabel,
        });
      } else {
        validateCariPurposeRow({
          accountRow,
          definition,
          fieldLabel,
        });
      }

      providedByPurpose.set(row.purposeCode, row.accountId);
      validatedRows.push({
        purposeCode: row.purposeCode,
        accountId: row.accountId,
      });
    }

    const effectiveByPurpose = buildEffectiveMapping({
      mode: normalizedMode,
      existingByPurpose,
      providedByPurpose,
    });
    assertDistinctPurposePairs(effectiveByPurpose);

    if (normalizedMode === "OVERWRITE") {
      await deleteManagedMappingsForOverwrite({
        tx,
        tenantId: normalizedTenantId,
        legalEntityId: normalizedLegalEntityId,
        managedPurposeCodes,
      });
    }

    for (const row of validatedRows) {
      await upsertJournalPurposeAccountTx(tx, {
        tenantId: normalizedTenantId,
        legalEntityId: normalizedLegalEntityId,
        purposeCode: row.purposeCode,
        accountId: row.accountId,
      });
    }

    const metadata = await insertApplyMetadata({
      tx,
      tenantId: normalizedTenantId,
      legalEntityId: normalizedLegalEntityId,
      packId: pack.packId,
      mode: normalizedMode,
      rows: validatedRows,
      userId: normalizedUserId,
    });

    return {
      packId: pack.packId,
      legalEntityId: normalizedLegalEntityId,
      mode: normalizedMode,
      rows: validatedRows,
      metadata,
    };
  });
}
