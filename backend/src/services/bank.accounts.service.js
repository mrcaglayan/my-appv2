import { query, withTransaction } from "../db.js";
import {
  assertAccountBelongsToTenant,
  assertCurrencyExists,
  assertLegalEntityBelongsToTenant,
} from "../tenantGuards.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function isDuplicateConstraintError(err) {
  return Number(err?.errno) === 1062;
}

function toNullableString(value, maxLength = 255) {
  if (value === undefined || value === null) {
    return null;
  }
  const normalized = String(value).trim();
  if (!normalized) {
    return null;
  }
  return normalized.slice(0, maxLength);
}

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

async function countChildAccounts({ accountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT COUNT(*) AS total
     FROM accounts
     WHERE parent_account_id = ?`,
    [accountId]
  );
  return Number(result.rows?.[0]?.total || 0);
}

async function findBankAccountScopeById({ tenantId, bankAccountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, legal_entity_id
     FROM bank_accounts
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [bankAccountId, tenantId]
  );
  return result.rows?.[0] || null;
}

async function findBankAccountById({ tenantId, bankAccountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        ba.id,
        ba.tenant_id,
        ba.legal_entity_id,
        ba.code,
        ba.name,
        ba.currency_code,
        ba.gl_account_id,
        ba.bank_name,
        ba.branch_name,
        ba.iban,
        ba.account_no,
        ba.is_active,
        ba.created_by_user_id,
        ba.created_at,
        ba.updated_at,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name,
        a.code AS gl_account_code,
        a.name AS gl_account_name,
        a.account_type AS gl_account_type,
        a.allow_posting AS gl_account_allow_posting,
        a.is_active AS gl_account_is_active
     FROM bank_accounts ba
     JOIN legal_entities le
       ON le.id = ba.legal_entity_id
      AND le.tenant_id = ba.tenant_id
     JOIN accounts a
       ON a.id = ba.gl_account_id
     WHERE ba.tenant_id = ?
       AND ba.id = ?
     LIMIT 1`,
    [tenantId, bankAccountId]
  );
  return result.rows?.[0] || null;
}

async function findBankAccountByCode({
  tenantId,
  legalEntityId,
  code,
  runQuery = query,
}) {
  const result = await runQuery(
    `SELECT id, tenant_id, legal_entity_id, code, gl_account_id, is_active
     FROM bank_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, legalEntityId, code]
  );
  return result.rows?.[0] || null;
}

async function fetchBankLinkableGlAccount({
  tenantId,
  legalEntityId,
  accountId,
  label = "glAccountId",
  runQuery = query,
}) {
  await assertAccountBelongsToTenant(tenantId, accountId, label, { runQuery });

  const result = await runQuery(
    `SELECT
        a.id,
        a.code,
        a.name,
        a.account_type,
        a.normal_side,
        a.allow_posting,
        a.parent_account_id,
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
    throw badRequest(`${label} not found for tenant`);
  }

  if (normalizeUpperText(row.coa_scope) !== "LEGAL_ENTITY") {
    throw badRequest(`${label} must belong to a LEGAL_ENTITY chart`);
  }
  if (parsePositiveInt(row.coa_legal_entity_id) !== parsePositiveInt(legalEntityId)) {
    throw badRequest(`${label} must belong to legalEntityId`);
  }
  if (normalizeUpperText(row.account_type) !== "ASSET") {
    throw badRequest(`${label} must be an ASSET account`);
  }
  if (!parseDbBoolean(row.is_active)) {
    throw badRequest(`${label} must reference an ACTIVE account`);
  }
  if (!parseDbBoolean(row.allow_posting)) {
    throw badRequest(`${label} must reference a postable account`);
  }

  const childCount = await countChildAccounts({
    accountId,
    runQuery,
  });
  if (childCount > 0) {
    throw badRequest(`${label} must reference a leaf account`);
  }

  return row;
}

async function insertBankAccount({ payload, runQuery = query }) {
  const result = await runQuery(
    `INSERT INTO bank_accounts (
        tenant_id,
        legal_entity_id,
        code,
        name,
        currency_code,
        gl_account_id,
        bank_name,
        branch_name,
        iban,
        account_no,
        is_active,
        created_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      payload.tenantId,
      payload.legalEntityId,
      payload.code,
      payload.name,
      payload.currencyCode,
      payload.glAccountId,
      toNullableString(payload.bankName, 255),
      toNullableString(payload.branchName, 255),
      toNullableString(payload.iban, 64),
      toNullableString(payload.accountNo, 80),
      payload.isActive ? 1 : 0,
      payload.userId,
    ]
  );
  return parsePositiveInt(result.rows?.insertId);
}

async function updateBankAccountRow({ bankAccountId, payload, runQuery = query }) {
  await runQuery(
    `UPDATE bank_accounts
     SET code = ?,
         name = ?,
         currency_code = ?,
         gl_account_id = ?,
         bank_name = ?,
         branch_name = ?,
         iban = ?,
         account_no = ?,
         is_active = ?
     WHERE id = ?`,
    [
      payload.code,
      payload.name,
      payload.currencyCode,
      payload.glAccountId,
      toNullableString(payload.bankName, 255),
      toNullableString(payload.branchName, 255),
      toNullableString(payload.iban, 64),
      toNullableString(payload.accountNo, 80),
      payload.isActive ? 1 : 0,
      bankAccountId,
    ]
  );
}

export async function resolveBankAccountScope(bankAccountId, tenantId) {
  const parsedBankAccountId = parsePositiveInt(bankAccountId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedBankAccountId || !parsedTenantId) {
    return null;
  }

  const row = await findBankAccountScopeById({
    tenantId: parsedTenantId,
    bankAccountId: parsedBankAccountId,
  });
  if (!row) {
    return null;
  }

  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: parsePositiveInt(row.legal_entity_id),
  };
}

export async function listBankAccountRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["ba.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "ba.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("ba.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.isActive !== null) {
    conditions.push("ba.is_active = ?");
    params.push(filters.isActive ? 1 : 0);
  }

  if (filters.q) {
    conditions.push(
      "(ba.code LIKE ? OR ba.name LIKE ? OR ba.bank_name LIKE ? OR ba.iban LIKE ? OR ba.account_no LIKE ?)"
    );
    const like = `%${filters.q}%`;
    params.push(like, like, like, like, like);
  }

  const whereSql = conditions.join(" AND ");

  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM bank_accounts ba
     WHERE ${whereSql}`,
    params
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const safeLimit =
    Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset =
    Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const listResult = await query(
    `SELECT
        ba.id,
        ba.tenant_id,
        ba.legal_entity_id,
        ba.code,
        ba.name,
        ba.currency_code,
        ba.gl_account_id,
        ba.bank_name,
        ba.branch_name,
        ba.iban,
        ba.account_no,
        ba.is_active,
        ba.created_by_user_id,
        ba.created_at,
        ba.updated_at,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name,
        a.code AS gl_account_code,
        a.name AS gl_account_name
     FROM bank_accounts ba
     JOIN legal_entities le
       ON le.id = ba.legal_entity_id
      AND le.tenant_id = ba.tenant_id
     JOIN accounts a
       ON a.id = ba.gl_account_id
     WHERE ${whereSql}
     ORDER BY ba.legal_entity_id, ba.code, ba.id
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: listResult.rows || [],
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getBankAccountByIdForTenant({
  req,
  tenantId,
  bankAccountId,
  assertScopeAccess,
}) {
  const row = await findBankAccountById({ tenantId, bankAccountId });
  if (!row) {
    throw badRequest("Bank account not found");
  }
  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "bankAccountId");
  return row;
}

export async function createBankAccount({
  req,
  payload,
  assertScopeAccess,
}) {
  await assertLegalEntityBelongsToTenant(payload.tenantId, payload.legalEntityId, "legalEntityId");
  assertScopeAccess(req, "legal_entity", payload.legalEntityId, "legalEntityId");
  await assertCurrencyExists(payload.currencyCode, "currencyCode");

  await fetchBankLinkableGlAccount({
    tenantId: payload.tenantId,
    legalEntityId: payload.legalEntityId,
    accountId: payload.glAccountId,
    label: "glAccountId",
  });

  try {
    const row = await withTransaction(async (tx) => {
      const insertId = await insertBankAccount({
        payload,
        runQuery: tx.query,
      });
      if (!insertId) {
        throw new Error("Failed to create bank account");
      }
      return findBankAccountById({
        tenantId: payload.tenantId,
        bankAccountId: insertId,
        runQuery: tx.query,
      });
    });
    if (!row) {
      throw new Error("Failed to load created bank account");
    }
    return row;
  } catch (err) {
    if (isDuplicateConstraintError(err)) {
      throw badRequest("Bank account code and GL account link must be unique within legalEntityId");
    }
    throw err;
  }
}

export async function updateBankAccountById({
  req,
  payload,
  assertScopeAccess,
}) {
  const existing = await findBankAccountById({
    tenantId: payload.tenantId,
    bankAccountId: payload.bankAccountId,
  });
  if (!existing) {
    throw badRequest("Bank account not found");
  }

  assertScopeAccess(req, "legal_entity", existing.legal_entity_id, "bankAccountId");
  if (parsePositiveInt(existing.legal_entity_id) !== parsePositiveInt(payload.legalEntityId)) {
    throw badRequest("legalEntityId cannot be changed for an existing bank account");
  }

  await assertLegalEntityBelongsToTenant(payload.tenantId, payload.legalEntityId, "legalEntityId");
  assertScopeAccess(req, "legal_entity", payload.legalEntityId, "legalEntityId");
  await assertCurrencyExists(payload.currencyCode, "currencyCode");

  await fetchBankLinkableGlAccount({
    tenantId: payload.tenantId,
    legalEntityId: payload.legalEntityId,
    accountId: payload.glAccountId,
    label: "glAccountId",
  });

  try {
    const row = await withTransaction(async (tx) => {
      await updateBankAccountRow({
        bankAccountId: payload.bankAccountId,
        payload,
        runQuery: tx.query,
      });
      return findBankAccountById({
        tenantId: payload.tenantId,
        bankAccountId: payload.bankAccountId,
        runQuery: tx.query,
      });
    });
    if (!row) {
      throw new Error("Failed to load updated bank account");
    }
    return row;
  } catch (err) {
    if (isDuplicateConstraintError(err)) {
      throw badRequest("Bank account code and GL account link must be unique within legalEntityId");
    }
    throw err;
  }
}

export async function setBankAccountActive({
  req,
  tenantId,
  bankAccountId,
  isActive,
  assertScopeAccess,
}) {
  const existing = await findBankAccountById({
    tenantId,
    bankAccountId,
  });
  if (!existing) {
    throw badRequest("Bank account not found");
  }

  assertScopeAccess(req, "legal_entity", existing.legal_entity_id, "bankAccountId");
  if (parseDbBoolean(existing.is_active) === Boolean(isActive)) {
    return existing;
  }

  await query(
    `UPDATE bank_accounts
     SET is_active = ?
     WHERE id = ?
       AND tenant_id = ?`,
    [isActive ? 1 : 0, bankAccountId, tenantId]
  );

  const updated = await findBankAccountById({
    tenantId,
    bankAccountId,
  });
  if (!updated) {
    throw new Error("Failed to load updated bank account status");
  }
  return updated;
}
