import { query } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";

const CARI_REQUIRED_PURPOSE_CODES = Object.freeze([
  "CARI_AR_CONTROL",
  "CARI_AR_OFFSET",
  "CARI_AP_CONTROL",
  "CARI_AP_OFFSET",
]);

const SHAREHOLDER_REQUIRED_PURPOSE_CODES = Object.freeze([
  "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
  "SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
]);

const SHAREHOLDER_EXPECTED_NORMAL_SIDE = Object.freeze({
  SHAREHOLDER_CAPITAL_CREDIT_PARENT: "CREDIT",
  SHAREHOLDER_COMMITMENT_DEBIT_PARENT: "DEBIT",
});

const CARI_DISTINCT_PAIRS = Object.freeze([
  Object.freeze({
    left: "CARI_AR_CONTROL",
    right: "CARI_AR_OFFSET",
  }),
  Object.freeze({
    left: "CARI_AP_CONTROL",
    right: "CARI_AP_OFFSET",
  }),
]);

const SHAREHOLDER_DISTINCT_PAIRS = Object.freeze([
  Object.freeze({
    left: "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
    right: "SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
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

function addInvalidMapping(invalidMappings, nextRow) {
  const purposeCode = toUpper(nextRow?.purposeCode);
  const reason = toUpper(nextRow?.reason);
  if (!purposeCode || !reason) {
    return;
  }
  const exists = invalidMappings.some(
    (row) => toUpper(row.purposeCode) === purposeCode && toUpper(row.reason) === reason
  );
  if (exists) {
    return;
  }
  invalidMappings.push({
    purposeCode,
    reason,
    accountId: parsePositiveInt(nextRow.accountId) || null,
    accountCode: String(nextRow.accountCode || "") || null,
    details: nextRow.details || null,
  });
}

function buildReadinessRow({
  legalEntityId,
  requiredPurposeCodes,
  missingPurposeCodes,
  invalidMappings,
}) {
  return {
    legalEntityId,
    ready: missingPurposeCodes.length === 0 && invalidMappings.length === 0,
    requiredPurposeCodes: [...requiredPurposeCodes],
    missingPurposeCodes,
    invalidMappings,
  };
}

async function resolveTargetLegalEntityIds({
  tenantId,
  legalEntityId = null,
  runQuery = query,
}) {
  const normalizedTenantId = parsePositiveInt(tenantId);
  if (!normalizedTenantId) {
    throw badRequest("tenantId is required");
  }

  const normalizedLegalEntityId = parsePositiveInt(legalEntityId);
  if (normalizedLegalEntityId) {
    return [normalizedLegalEntityId];
  }

  const result = await runQuery(
    `SELECT id
     FROM legal_entities
     WHERE tenant_id = ?
     ORDER BY id`,
    [normalizedTenantId]
  );
  return (result.rows || [])
    .map((row) => parsePositiveInt(row.id))
    .filter(Boolean);
}

async function loadPurposeMappingsByLegalEntity({
  tenantId,
  legalEntityIds,
  requiredPurposeCodes,
  runQuery = query,
}) {
  if (!Array.isArray(legalEntityIds) || legalEntityIds.length === 0) {
    return new Map();
  }
  if (!Array.isArray(requiredPurposeCodes) || requiredPurposeCodes.length === 0) {
    return new Map();
  }

  const legalEntityPlaceholders = legalEntityIds.map(() => "?").join(", ");
  const purposePlaceholders = requiredPurposeCodes.map(() => "?").join(", ");
  const result = await runQuery(
    `SELECT
       jpa.legal_entity_id,
       jpa.purpose_code,
       jpa.account_id AS mapped_account_id,
       a.id AS account_id,
       a.code AS account_code,
       a.account_type,
       a.normal_side,
       a.allow_posting,
       a.is_active,
       c.tenant_id AS account_tenant_id,
       c.scope AS coa_scope,
       c.legal_entity_id AS coa_legal_entity_id
     FROM journal_purpose_accounts jpa
     LEFT JOIN accounts a ON a.id = jpa.account_id
     LEFT JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE jpa.tenant_id = ?
       AND jpa.legal_entity_id IN (${legalEntityPlaceholders})
       AND jpa.purpose_code IN (${purposePlaceholders})`,
    [tenantId, ...legalEntityIds, ...requiredPurposeCodes]
  );

  const byLegalEntity = new Map();
  for (const row of result.rows || []) {
    const entityId = parsePositiveInt(row.legal_entity_id);
    const purposeCode = toUpper(row.purpose_code);
    if (!entityId || !purposeCode) {
      continue;
    }
    if (!byLegalEntity.has(entityId)) {
      byLegalEntity.set(entityId, new Map());
    }
    byLegalEntity.get(entityId).set(purposeCode, row);
  }

  return byLegalEntity;
}

function evaluateCommonMappingValidity({
  tenantId,
  legalEntityId,
  row,
}) {
  const invalids = [];
  const mappedAccountId = parsePositiveInt(row?.mapped_account_id);
  const accountId = parsePositiveInt(row?.account_id) || mappedAccountId || null;
  const accountCode = String(row?.account_code || "");
  const accountExists = parsePositiveInt(row?.account_id);

  if (!mappedAccountId) {
    invalids.push({
      reason: "MAPPED_ACCOUNT_ID_INVALID",
      accountId,
      accountCode,
    });
    return invalids;
  }
  if (!accountExists) {
    invalids.push({
      reason: "ACCOUNT_NOT_FOUND",
      accountId,
      accountCode,
    });
    return invalids;
  }

  if (parsePositiveInt(row?.account_tenant_id) !== parsePositiveInt(tenantId)) {
    invalids.push({
      reason: "ACCOUNT_TENANT_MISMATCH",
      accountId,
      accountCode,
    });
  }

  if (toUpper(row?.coa_scope) !== "LEGAL_ENTITY") {
    invalids.push({
      reason: "ACCOUNT_SCOPE_NOT_LEGAL_ENTITY",
      accountId,
      accountCode,
    });
  }

  if (parsePositiveInt(row?.coa_legal_entity_id) !== parsePositiveInt(legalEntityId)) {
    invalids.push({
      reason: "ACCOUNT_LEGAL_ENTITY_MISMATCH",
      accountId,
      accountCode,
      details: {
        expectedLegalEntityId: parsePositiveInt(legalEntityId),
        actualLegalEntityId: parsePositiveInt(row?.coa_legal_entity_id) || null,
      },
    });
  }

  if (!toDbBoolean(row?.is_active)) {
    invalids.push({
      reason: "ACCOUNT_INACTIVE",
      accountId,
      accountCode,
    });
  }

  return invalids;
}

function evaluateCariPurposeRow({
  tenantId,
  legalEntityId,
  purposeCode,
  row,
}) {
  const invalids = evaluateCommonMappingValidity({
    tenantId,
    legalEntityId,
    row,
  }).map((invalid) => ({
    purposeCode,
    ...invalid,
  }));

  const accountExists = parsePositiveInt(row?.account_id);
  const accountId = accountExists || parsePositiveInt(row?.mapped_account_id);
  const accountCode = String(row?.account_code || "");
  if (accountExists && !toDbBoolean(row?.allow_posting)) {
    invalids.push({
      purposeCode,
      reason: "ACCOUNT_NOT_POSTABLE",
      accountId: accountId || null,
      accountCode: accountCode || null,
    });
  }

  return invalids;
}

function evaluateShareholderPurposeRow({
  tenantId,
  legalEntityId,
  purposeCode,
  row,
}) {
  const invalids = evaluateCommonMappingValidity({
    tenantId,
    legalEntityId,
    row,
  }).map((invalid) => ({
    purposeCode,
    ...invalid,
  }));

  const accountExists = parsePositiveInt(row?.account_id);
  const accountId = accountExists || parsePositiveInt(row?.mapped_account_id);
  const accountCode = String(row?.account_code || "");
  if (!row) {
    return invalids;
  }
  if (!accountExists) {
    return invalids;
  }

  if (toUpper(row?.account_type) !== "EQUITY") {
    invalids.push({
      purposeCode,
      reason: "ACCOUNT_TYPE_NOT_EQUITY",
      accountId: accountId || null,
      accountCode: accountCode || null,
    });
  }

  if (toDbBoolean(row?.allow_posting)) {
    invalids.push({
      purposeCode,
      reason: "ACCOUNT_MUST_BE_NON_POSTABLE",
      accountId: accountId || null,
      accountCode: accountCode || null,
    });
  }

  const expectedNormalSide = SHAREHOLDER_EXPECTED_NORMAL_SIDE[purposeCode];
  if (expectedNormalSide && toUpper(row?.normal_side) !== expectedNormalSide) {
    invalids.push({
      purposeCode,
      reason: "ACCOUNT_NORMAL_SIDE_MISMATCH",
      accountId: accountId || null,
      accountCode: accountCode || null,
      details: {
        expectedNormalSide,
        actualNormalSide: toUpper(row?.normal_side) || null,
      },
    });
  }

  return invalids;
}

function evaluateDistinctPurposePairs({
  purposeMap,
  distinctPairs,
}) {
  const invalids = [];
  for (const pair of distinctPairs) {
    const leftPurpose = toUpper(pair?.left);
    const rightPurpose = toUpper(pair?.right);
    if (!leftPurpose || !rightPurpose) {
      continue;
    }

    const left = purposeMap.get(leftPurpose);
    const right = purposeMap.get(rightPurpose);
    const leftAccountId = parsePositiveInt(left?.mapped_account_id);
    const rightAccountId = parsePositiveInt(right?.mapped_account_id);

    if (!leftAccountId || !rightAccountId) {
      continue;
    }
    if (leftAccountId !== rightAccountId) {
      continue;
    }

    invalids.push({
      purposeCode: leftPurpose,
      reason: "PURPOSES_MUST_MAP_TO_DIFFERENT_ACCOUNTS",
      accountId: leftAccountId,
      accountCode: String(left?.account_code || "") || null,
      details: {
        pairedPurposeCode: rightPurpose,
      },
    });
    invalids.push({
      purposeCode: rightPurpose,
      reason: "PURPOSES_MUST_MAP_TO_DIFFERENT_ACCOUNTS",
      accountId: rightAccountId,
      accountCode: String(right?.account_code || "") || null,
      details: {
        pairedPurposeCode: leftPurpose,
      },
    });
  }
  return invalids;
}

function buildModuleReadinessByLegalEntity({
  tenantId,
  legalEntityIds,
  requiredPurposeCodes,
  purposeMapByLegalEntity,
  distinctPairs,
  evaluatePurposeRow,
}) {
  const rows = [];

  for (const legalEntityId of legalEntityIds) {
    const purposeMap = purposeMapByLegalEntity.get(legalEntityId) || new Map();
    const missingPurposeCodes = [];
    const invalidMappings = [];

    for (const purposeCode of requiredPurposeCodes) {
      const row = purposeMap.get(purposeCode);
      if (!row) {
        missingPurposeCodes.push(purposeCode);
        continue;
      }

      const invalidRows = evaluatePurposeRow({
        tenantId,
        legalEntityId,
        purposeCode,
        row,
      });
      for (const invalid of invalidRows) {
        addInvalidMapping(invalidMappings, invalid);
      }
    }

    const distinctInvalids = evaluateDistinctPurposePairs({
      purposeMap,
      distinctPairs,
    });
    for (const invalid of distinctInvalids) {
      addInvalidMapping(invalidMappings, invalid);
    }

    rows.push(
      buildReadinessRow({
        legalEntityId,
        requiredPurposeCodes,
        missingPurposeCodes,
        invalidMappings,
      })
    );
  }

  return rows;
}

export async function getCariPostingReadiness(
  tenantId,
  legalEntityId = null,
  { runQuery = query } = {}
) {
  const legalEntityIds = await resolveTargetLegalEntityIds({
    tenantId,
    legalEntityId,
    runQuery,
  });
  const purposeMapByLegalEntity = await loadPurposeMappingsByLegalEntity({
    tenantId,
    legalEntityIds,
    requiredPurposeCodes: CARI_REQUIRED_PURPOSE_CODES,
    runQuery,
  });

  const byLegalEntity = buildModuleReadinessByLegalEntity({
    tenantId,
    legalEntityIds,
    requiredPurposeCodes: CARI_REQUIRED_PURPOSE_CODES,
    purposeMapByLegalEntity,
    distinctPairs: CARI_DISTINCT_PAIRS,
    evaluatePurposeRow: evaluateCariPurposeRow,
  });

  return {
    moduleKey: "cariPosting",
    byLegalEntity,
  };
}

export async function getShareholderCommitmentReadiness(
  tenantId,
  legalEntityId = null,
  { runQuery = query } = {}
) {
  const legalEntityIds = await resolveTargetLegalEntityIds({
    tenantId,
    legalEntityId,
    runQuery,
  });
  const purposeMapByLegalEntity = await loadPurposeMappingsByLegalEntity({
    tenantId,
    legalEntityIds,
    requiredPurposeCodes: SHAREHOLDER_REQUIRED_PURPOSE_CODES,
    runQuery,
  });

  const byLegalEntity = buildModuleReadinessByLegalEntity({
    tenantId,
    legalEntityIds,
    requiredPurposeCodes: SHAREHOLDER_REQUIRED_PURPOSE_CODES,
    purposeMapByLegalEntity,
    distinctPairs: SHAREHOLDER_DISTINCT_PAIRS,
    evaluatePurposeRow: evaluateShareholderPurposeRow,
  });

  return {
    moduleKey: "shareholderCommitment",
    byLegalEntity,
  };
}

export async function getModuleReadiness(
  tenantId,
  legalEntityId = null,
  { runQuery = query } = {}
) {
  const normalizedTenantId = parsePositiveInt(tenantId);
  if (!normalizedTenantId) {
    throw badRequest("tenantId is required");
  }

  const normalizedLegalEntityId = parsePositiveInt(legalEntityId);
  const [cariPosting, shareholderCommitment] = await Promise.all([
    getCariPostingReadiness(normalizedTenantId, normalizedLegalEntityId, {
      runQuery,
    }),
    getShareholderCommitmentReadiness(normalizedTenantId, normalizedLegalEntityId, {
      runQuery,
    }),
  ]);

  return {
    tenantId: normalizedTenantId,
    legalEntityId: normalizedLegalEntityId || null,
    modules: {
      cariPosting: {
        byLegalEntity: cariPosting.byLegalEntity,
      },
      shareholderCommitment: {
        byLegalEntity: shareholderCommitment.byLegalEntity,
      },
    },
  };
}
