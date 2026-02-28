import { withTransaction } from "../db.js";
import {
  assertCurrencyExists,
  assertLegalEntityBelongsToTenant,
  assertOperatingUnitBelongsToTenant,
} from "../tenantGuards.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  countCashRegisters,
  countChildAccounts,
  findAccountForCashRules,
  findCashRegisterByCode,
  findCashRegisterById,
  findCashRegisterScopeById,
  findOpenCashSessionByRegisterId,
  insertCashRegister,
  listCashRegisters,
  markAccountAsCashControlled,
  updateCashRegister,
  updateCashRegisterStatus,
} from "./cash.queries.js";

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

export function assertRegisterOperationalConfig(
  register,
  {
    requireActive = true,
    requireCashControlledAccount = true,
    label = "Cash register",
  } = {}
) {
  if (!register) {
    throw badRequest(`${label} not found`);
  }

  if (requireActive && String(register.status || "").toUpperCase() !== "ACTIVE") {
    throw badRequest("Cash register is not ACTIVE");
  }

  if (!parseDbBoolean(register.account_is_active)) {
    throw badRequest("Cash register account must be active");
  }
  if (!parseDbBoolean(register.account_allow_posting)) {
    throw badRequest("Cash register account must allow posting");
  }
  if (parsePositiveInt(register.account_parent_account_id)) {
    throw badRequest("Cash register account must be a leaf account");
  }

  if (String(register.account_scope || "").toUpperCase() !== "LEGAL_ENTITY") {
    throw badRequest("Cash register account must belong to a LEGAL_ENTITY chart of accounts");
  }

  if (parsePositiveInt(register.account_legal_entity_id) !== parsePositiveInt(register.legal_entity_id)) {
    throw badRequest("Cash register account/legalEntity configuration is invalid");
  }

  if (
    requireCashControlledAccount &&
    !parseDbBoolean(register.account_is_cash_controlled)
  ) {
    throw badRequest("Cash register account must be cash-controlled");
  }
}

async function assertAccountEligibleForCashRegister({
  tenantId,
  legalEntityId,
  accountId,
  label,
  runQuery,
}) {
  const account = await findAccountForCashRules({
    tenantId,
    accountId,
    runQuery,
  });
  if (!account) {
    throw badRequest(`${label} not found for tenant`);
  }

  if (String(account.scope || "").toUpperCase() !== "LEGAL_ENTITY") {
    throw badRequest(`${label} must belong to a LEGAL_ENTITY chart of accounts`);
  }
  if (parsePositiveInt(account.legal_entity_id) !== legalEntityId) {
    throw badRequest(`${label} must belong to the selected legalEntityId`);
  }
  if (!parseDbBoolean(account.allow_posting)) {
    throw badRequest(`${label} must allow posting`);
  }
  if (!parseDbBoolean(account.is_active)) {
    throw badRequest(`${label} must be active`);
  }

  const children = await countChildAccounts({ accountId, runQuery });
  if (children > 0) {
    throw badRequest(`${label} must be a leaf account`);
  }

  return account;
}

function isDuplicateConstraintError(err) {
  return Number(err?.errno) === 1062;
}

export async function resolveCashRegisterScope(registerId, tenantId) {
  const parsedRegisterId = parsePositiveInt(registerId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedRegisterId || !parsedTenantId) {
    return null;
  }

  const row = await findCashRegisterScopeById({
    tenantId: parsedTenantId,
    registerId: parsedRegisterId,
  });
  if (!row) {
    return null;
  }

  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: Number(row.legal_entity_id),
  };
}

export async function listCashRegisterRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["cr.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "cr.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("cr.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.operatingUnitId) {
    assertScopeAccess(req, "operating_unit", filters.operatingUnitId, "operatingUnitId");
    conditions.push("cr.operating_unit_id = ?");
    params.push(filters.operatingUnitId);
  }

  if (filters.status) {
    conditions.push("cr.status = ?");
    params.push(filters.status);
  }

  if (filters.q) {
    conditions.push("(cr.code LIKE ? OR cr.name LIKE ?)");
    params.push(`%${filters.q}%`, `%${filters.q}%`);
  }

  const whereSql = conditions.join(" AND ");
  const total = await countCashRegisters({ whereSql, params });
  const rows = await listCashRegisters({
    whereSql,
    params,
    limit: filters.limit,
    offset: filters.offset,
  });

  return {
    rows,
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getCashRegisterByIdForTenant({
  req,
  tenantId,
  registerId,
  assertScopeAccess,
}) {
  const row = await findCashRegisterById({ tenantId, registerId });
  if (!row) {
    throw badRequest("Cash register not found");
  }

  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "registerId");
  if (row.operating_unit_id) {
    assertScopeAccess(req, "operating_unit", row.operating_unit_id, "registerId");
  }

  return row;
}

export async function upsertCashRegister({
  req,
  payload,
  assertScopeAccess,
}) {
  const tenantId = payload.tenantId;

  await assertLegalEntityBelongsToTenant(tenantId, payload.legalEntityId, "legalEntityId");
  assertScopeAccess(req, "legal_entity", payload.legalEntityId, "legalEntityId");

  if (payload.operatingUnitId) {
    const operatingUnit = await assertOperatingUnitBelongsToTenant(
      tenantId,
      payload.operatingUnitId,
      "operatingUnitId"
    );
    if (parsePositiveInt(operatingUnit.legal_entity_id) !== payload.legalEntityId) {
      throw badRequest("operatingUnitId must belong to legalEntityId");
    }
    assertScopeAccess(req, "operating_unit", payload.operatingUnitId, "operatingUnitId");
  }

  await assertCurrencyExists(payload.currencyCode, "currencyCode");

  await assertAccountEligibleForCashRegister({
    tenantId,
    legalEntityId: payload.legalEntityId,
    accountId: payload.accountId,
    label: "accountId",
  });

  if (payload.varianceGainAccountId) {
    await assertAccountEligibleForCashRegister({
      tenantId,
      legalEntityId: payload.legalEntityId,
      accountId: payload.varianceGainAccountId,
      label: "varianceGainAccountId",
    });
  }

  if (payload.varianceLossAccountId) {
    await assertAccountEligibleForCashRegister({
      tenantId,
      legalEntityId: payload.legalEntityId,
      accountId: payload.varianceLossAccountId,
      label: "varianceLossAccountId",
    });
  }

  const existingById = payload.id
    ? await findCashRegisterById({ tenantId, registerId: payload.id })
    : null;

  if (payload.id && !existingById) {
    throw badRequest("Cash register not found");
  }

  const existingByCode = await findCashRegisterByCode({
    tenantId,
    code: payload.code,
  });

  if (existingById) {
    assertScopeAccess(req, "legal_entity", existingById.legal_entity_id, "id");
    if (existingById.operating_unit_id) {
      assertScopeAccess(req, "operating_unit", existingById.operating_unit_id, "id");
    }
  } else if (existingByCode) {
    assertScopeAccess(req, "legal_entity", existingByCode.legal_entity_id, "code");
    if (existingByCode.operating_unit_id) {
      assertScopeAccess(req, "operating_unit", existingByCode.operating_unit_id, "code");
    }
  }

  try {
    const savedRow = await withTransaction(async (tx) => {
      let registerId = existingById?.id || existingByCode?.id || null;

      if (registerId) {
        await updateCashRegister({
          id: registerId,
          payload,
          runQuery: tx.query,
        });
      } else {
        registerId = await insertCashRegister({
          payload,
          runQuery: tx.query,
        });
      }

      await markAccountAsCashControlled({
        accountId: payload.accountId,
        runQuery: tx.query,
      });

      return findCashRegisterById({
        tenantId,
        registerId,
        runQuery: tx.query,
      });
    });

    if (!savedRow) {
      throw new Error("Cash register upsert failed");
    }

    return savedRow;
  } catch (err) {
    if (isDuplicateConstraintError(err)) {
      throw badRequest("Cash register code/account must be unique within tenant");
    }
    throw err;
  }
}

export async function setCashRegisterStatus({
  req,
  tenantId,
  registerId,
  targetStatus,
  assertScopeAccess,
}) {
  const register = await findCashRegisterById({ tenantId, registerId });
  if (!register) {
    throw badRequest("Cash register not found");
  }

  assertScopeAccess(req, "legal_entity", register.legal_entity_id, "registerId");
  if (register.operating_unit_id) {
    assertScopeAccess(req, "operating_unit", register.operating_unit_id, "registerId");
  }

  const normalizedCurrent = String(register.status || "").toUpperCase();
  const normalizedTarget = String(targetStatus || "").toUpperCase();
  if (normalizedCurrent === normalizedTarget) {
    return register;
  }

  if (normalizedTarget === "INACTIVE") {
    const openSession = await findOpenCashSessionByRegisterId({
      tenantId,
      registerId,
    });
    if (openSession) {
      throw badRequest("Cannot inactivate register while an OPEN session exists");
    }
  }

  await updateCashRegisterStatus({
    tenantId,
    registerId,
    status: normalizedTarget,
  });

  const saved = await findCashRegisterById({ tenantId, registerId });
  if (!saved) {
    throw new Error("Cash register status update failed");
  }
  return saved;
}
