import { badRequest, parsePositiveInt } from "../routes/_utils.js";

const OWNERSHIP_SCALE = 6;
const OWNERSHIP_EPSILON = 0.0000001;

function toNonNegativeAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }
  return parsed;
}

function roundOwnership(value) {
  const factor = 10 ** OWNERSHIP_SCALE;
  return Math.round(Number(value || 0) * factor) / factor;
}

export async function recalculateShareholderOwnershipPctTx(
  tx,
  tenantId,
  legalEntityId
) {
  const normalizedTenantId = parsePositiveInt(tenantId);
  const normalizedLegalEntityId = parsePositiveInt(legalEntityId);
  if (!normalizedTenantId || !normalizedLegalEntityId) {
    throw badRequest("tenantId and legalEntityId are required for ownership recalculation");
  }

  const shareholderResult = await tx.query(
    `SELECT id, committed_capital
     FROM shareholders
     WHERE tenant_id = ?
       AND legal_entity_id = ?
     ORDER BY id
     FOR UPDATE`,
    [normalizedTenantId, normalizedLegalEntityId]
  );
  const rows = Array.isArray(shareholderResult.rows) ? shareholderResult.rows : [];
  if (rows.length === 0) {
    return {
      recalculated: false,
      shareholderCount: 0,
      totalCommittedCapital: 0,
    };
  }

  const normalizedRows = rows.map((row) => ({
    shareholderId: parsePositiveInt(row.id),
    committedCapital: toNonNegativeAmount(row.committed_capital),
  }));
  const totalCommittedCapital = normalizedRows.reduce(
    (sum, row) => sum + row.committedCapital,
    0
  );

  const ownershipByShareholderId = new Map();
  if (totalCommittedCapital <= OWNERSHIP_EPSILON) {
    for (const row of normalizedRows) {
      if (row.shareholderId) {
        ownershipByShareholderId.set(row.shareholderId, 0);
      }
    }
  } else {
    let runningTotalPct = 0;
    for (let i = 0; i < normalizedRows.length; i += 1) {
      const row = normalizedRows[i];
      if (!row.shareholderId) {
        continue;
      }
      if (i === normalizedRows.length - 1) {
        ownershipByShareholderId.set(
          row.shareholderId,
          Math.max(0, roundOwnership(100 - runningTotalPct))
        );
        continue;
      }
      const rawPct = (row.committedCapital / totalCommittedCapital) * 100;
      const roundedPct = roundOwnership(rawPct);
      runningTotalPct = roundOwnership(runningTotalPct + roundedPct);
      ownershipByShareholderId.set(row.shareholderId, roundedPct);
    }
  }

  for (const row of normalizedRows) {
    if (!row.shareholderId) {
      continue;
    }
    const ownershipPct = roundOwnership(
      ownershipByShareholderId.get(row.shareholderId) || 0
    );
    // eslint-disable-next-line no-await-in-loop
    await tx.query(
      `UPDATE shareholders
       SET ownership_pct = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND id = ?
       LIMIT 1`,
      [
        ownershipPct,
        normalizedTenantId,
        normalizedLegalEntityId,
        row.shareholderId,
      ]
    );
  }

  return {
    recalculated: true,
    shareholderCount: normalizedRows.length,
    totalCommittedCapital,
  };
}
