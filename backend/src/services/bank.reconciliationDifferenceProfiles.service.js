import { query } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { resolveBankAccountScope } from "./bank.accounts.service.js";
import { evaluateBankApprovalNeed } from "./bank.governance.service.js";
import { submitBankApprovalRequest } from "./bank.approvals.service.js";

function u(value) {
  return String(value || "").trim().toUpperCase();
}

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function toAmount(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Number(n.toFixed(6)) : null;
}

function hydrate(row) {
  if (!row) return null;
  return {
    ...row,
    max_abs_difference:
      row.max_abs_difference === null || row.max_abs_difference === undefined
        ? null
        : toAmount(row.max_abs_difference),
  };
}

async function getLegalEntityById({ tenantId, legalEntityId, runQuery = query }) {
  const res = await runQuery(
    `SELECT id, tenant_id, code, name
     FROM legal_entities
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId]
  );
  return res.rows?.[0] || null;
}

async function getBankAccountRow({ tenantId, bankAccountId, runQuery = query }) {
  const res = await runQuery(
    `SELECT id, tenant_id, legal_entity_id, code, name, is_active
     FROM bank_accounts
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, bankAccountId]
  );
  return res.rows?.[0] || null;
}

async function getGlAccountForTenantEntity({ tenantId, legalEntityId, accountId, label, runQuery = query }) {
  if (!accountId) return null;
  const res = await runQuery(
    `SELECT
        a.id,
        a.code,
        a.name,
        a.account_type,
        a.allow_posting,
        a.is_active,
        c.tenant_id,
        c.scope AS coa_scope,
        c.legal_entity_id AS coa_legal_entity_id
     FROM accounts a
     JOIN charts_of_accounts c
       ON c.id = a.coa_id
     WHERE a.id = ?
       AND c.tenant_id = ?
     LIMIT 1`,
    [accountId, tenantId]
  );
  const row = res.rows?.[0] || null;
  if (!row) throw badRequest(`${label} not found`);
  if (!parseDbBoolean(row.is_active)) throw badRequest(`${label} is inactive`);
  if (!parseDbBoolean(row.allow_posting)) throw badRequest(`${label} must be postable`);
  if (u(row.coa_scope) !== "LEGAL_ENTITY") {
    throw badRequest(`${label} must belong to a LEGAL_ENTITY chart`);
  }
  if (parsePositiveInt(row.coa_legal_entity_id) !== parsePositiveInt(legalEntityId)) {
    throw badRequest(`${label} must belong to selected legalEntityId`);
  }
  return row;
}

async function getProfileByIdRaw({ tenantId, profileId, runQuery = query }) {
  const res = await runQuery(
    `SELECT
        p.*,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        ea.code AS expense_account_code,
        ea.name AS expense_account_name,
        fga.code AS fx_gain_account_code,
        fga.name AS fx_gain_account_name,
        fla.code AS fx_loss_account_code,
        fla.name AS fx_loss_account_name
     FROM bank_reconciliation_difference_profiles p
     JOIN legal_entities le
       ON le.tenant_id = p.tenant_id
      AND le.id = p.legal_entity_id
     LEFT JOIN bank_accounts ba
       ON ba.tenant_id = p.tenant_id
      AND ba.legal_entity_id = p.legal_entity_id
      AND ba.id = p.bank_account_id
     LEFT JOIN accounts ea ON ea.id = p.expense_account_id
     LEFT JOIN accounts fga ON fga.id = p.fx_gain_account_id
     LEFT JOIN accounts fla ON fla.id = p.fx_loss_account_id
     WHERE p.tenant_id = ?
       AND p.id = ?
     LIMIT 1`,
    [tenantId, profileId]
  );
  return res.rows?.[0] || null;
}

function validateProfilePayloadShape(input) {
  const diffType = u(input.differenceType ?? input.difference_type);
  if (!["FEE", "FX"].includes(diffType)) {
    throw badRequest("differenceType must be FEE or FX");
  }
  if (input.maxAbsDifference === null || input.maxAbsDifference === undefined) {
    throw badRequest("maxAbsDifference is required");
  }
  const max = Number(input.maxAbsDifference);
  if (!Number.isFinite(max) || max < 0) {
    throw badRequest("maxAbsDifference must be >= 0");
  }
  if (diffType === "FEE" && !parsePositiveInt(input.expenseAccountId ?? input.expense_account_id)) {
    throw badRequest("expenseAccountId is required for FEE profiles");
  }
  if (
    diffType === "FX" &&
    (!parsePositiveInt(input.fxGainAccountId ?? input.fx_gain_account_id) ||
      !parsePositiveInt(input.fxLossAccountId ?? input.fx_loss_account_id))
  ) {
    throw badRequest("fxGainAccountId and fxLossAccountId are required for FX profiles");
  }
}

async function validateCreateOrUpdateContext({ req, tenantId, input, current = null, assertScopeAccess }) {
  const scopeType = u(input.scopeType ?? current?.scope_type ?? "LEGAL_ENTITY");
  let legalEntityId = parsePositiveInt(input.legalEntityId ?? current?.legal_entity_id) || null;
  let bankAccountId =
    input.bankAccountId !== undefined ? parsePositiveInt(input.bankAccountId) : parsePositiveInt(current?.bank_account_id);
  if (scopeType === "BANK_ACCOUNT") {
    if (!bankAccountId) throw badRequest("bankAccountId is required for BANK_ACCOUNT scope");
    const ba = await getBankAccountRow({ tenantId, bankAccountId });
    if (!ba) throw badRequest("bankAccountId not found");
    if (!parseDbBoolean(ba.is_active)) throw badRequest("bankAccountId is inactive");
    legalEntityId = parsePositiveInt(ba.legal_entity_id);
  } else {
    // Repo-native constraint: keep profiles legal-entity anchored even for GLOBAL.
    if (!legalEntityId) throw badRequest("legalEntityId is required");
    if (scopeType === "GLOBAL") {
      bankAccountId = null;
    }
  }
  const le = await getLegalEntityById({ tenantId, legalEntityId });
  if (!le) throw badRequest("legalEntityId not found");
  assertScopeAccess(req, "legal_entity", legalEntityId, bankAccountId ? "bankAccountId" : "legalEntityId");

  const diffType = u(input.differenceType ?? current?.difference_type);
  const expenseAccountId =
    input.expenseAccountId !== undefined ? parsePositiveInt(input.expenseAccountId) : parsePositiveInt(current?.expense_account_id);
  const fxGainAccountId =
    input.fxGainAccountId !== undefined ? parsePositiveInt(input.fxGainAccountId) : parsePositiveInt(current?.fx_gain_account_id);
  const fxLossAccountId =
    input.fxLossAccountId !== undefined ? parsePositiveInt(input.fxLossAccountId) : parsePositiveInt(current?.fx_loss_account_id);

  validateProfilePayloadShape({
    ...input,
    differenceType: diffType,
    maxAbsDifference:
      input.maxAbsDifference !== undefined ? input.maxAbsDifference : current?.max_abs_difference,
    expenseAccountId,
    fxGainAccountId,
    fxLossAccountId,
  });

  if (diffType === "FEE") {
    await getGlAccountForTenantEntity({
      tenantId,
      legalEntityId,
      accountId: expenseAccountId,
      label: "expenseAccountId",
    });
  } else {
    await getGlAccountForTenantEntity({
      tenantId,
      legalEntityId,
      accountId: fxGainAccountId,
      label: "fxGainAccountId",
    });
    await getGlAccountForTenantEntity({
      tenantId,
      legalEntityId,
      accountId: fxLossAccountId,
      label: "fxLossAccountId",
    });
  }

  return {
    scopeType,
    legalEntityId,
    bankAccountId: bankAccountId || null,
    differenceType: diffType,
    expenseAccountId: expenseAccountId || null,
    fxGainAccountId: fxGainAccountId || null,
    fxLossAccountId: fxLossAccountId || null,
  };
}

export async function resolveDifferenceProfileScope(profileId, tenantId) {
  const parsedProfileId = parsePositiveInt(profileId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedProfileId || !parsedTenantId) return null;
  const row = await getProfileByIdRaw({ tenantId: parsedTenantId, profileId: parsedProfileId });
  if (!row) return null;
  return { scopeType: "LEGAL_ENTITY", scopeId: parsePositiveInt(row.legal_entity_id) };
}

export async function listDifferenceProfiles({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const where = ["p.tenant_id = ?"];
  where.push(buildScopeFilter(req, "legal_entity", "p.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    where.push("p.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }
  if (filters.bankAccountId) {
    const bankScope = await resolveBankAccountScope(filters.bankAccountId, tenantId);
    if (!bankScope) throw badRequest("bankAccountId not found");
    assertScopeAccess(req, "legal_entity", bankScope.scopeId, "bankAccountId");
    where.push("p.bank_account_id = ?");
    params.push(filters.bankAccountId);
  }
  if (filters.status) {
    where.push("p.status = ?");
    params.push(filters.status);
  }
  if (filters.differenceType) {
    where.push("p.difference_type = ?");
    params.push(filters.differenceType);
  }
  if (filters.q) {
    const like = `%${filters.q}%`;
    where.push("(p.profile_code LIKE ? OR p.profile_name LIKE ?)");
    params.push(like, like);
  }

  const whereSql = where.join(" AND ");
  const countRes = await query(
    `SELECT COUNT(*) AS total
     FROM bank_reconciliation_difference_profiles p
     WHERE ${whereSql}`,
    params
  );
  const total = Number(countRes.rows?.[0]?.total || 0);

  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset = Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const listRes = await query(
    `SELECT
        p.*,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        ea.code AS expense_account_code,
        fga.code AS fx_gain_account_code,
        fla.code AS fx_loss_account_code
     FROM bank_reconciliation_difference_profiles p
     JOIN legal_entities le
       ON le.tenant_id = p.tenant_id
      AND le.id = p.legal_entity_id
     LEFT JOIN bank_accounts ba
       ON ba.tenant_id = p.tenant_id
      AND ba.legal_entity_id = p.legal_entity_id
      AND ba.id = p.bank_account_id
     LEFT JOIN accounts ea ON ea.id = p.expense_account_id
     LEFT JOIN accounts fga ON fga.id = p.fx_gain_account_id
     LEFT JOIN accounts fla ON fla.id = p.fx_loss_account_id
     WHERE ${whereSql}
     ORDER BY
       CASE WHEN p.status = 'ACTIVE' THEN 0 WHEN p.status = 'PAUSED' THEN 1 ELSE 2 END,
       p.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: (listRes.rows || []).map(hydrate),
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getDifferenceProfileById({ req, tenantId, profileId, assertScopeAccess }) {
  const row = await getProfileByIdRaw({ tenantId, profileId });
  if (!row) throw badRequest("Difference profile not found");
  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "profileId");
  return hydrate(row);
}

export async function createDifferenceProfile({ req, input, assertScopeAccess }) {
  const ctx = await validateCreateOrUpdateContext({
    req,
    tenantId: input.tenantId,
    input,
    current: null,
    assertScopeAccess,
  });

  await query(
    `INSERT INTO bank_reconciliation_difference_profiles (
        tenant_id, legal_entity_id,
        profile_code, profile_name, status,
        scope_type, bank_account_id,
        difference_type, direction_policy, tolerance_mode, max_abs_difference,
        expense_account_id, fx_gain_account_id, fx_loss_account_id,
        currency_code, description_prefix, effective_from, effective_to,
        created_by_user_id, updated_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ABSOLUTE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      input.tenantId,
      ctx.legalEntityId,
      input.profileCode,
      input.profileName,
      input.status,
      ctx.scopeType,
      ctx.bankAccountId,
      ctx.differenceType,
      input.directionPolicy || "BOTH",
      toAmount(input.maxAbsDifference ?? 0) ?? 0,
      ctx.expenseAccountId,
      ctx.fxGainAccountId,
      ctx.fxLossAccountId,
      input.currencyCode || null,
      input.descriptionPrefix || null,
      input.effectiveFrom || null,
      input.effectiveTo || null,
      input.userId,
      input.userId,
    ]
  );

  const lookup = await query(
    `SELECT id
     FROM bank_reconciliation_difference_profiles
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND profile_code = ?
     ORDER BY id DESC
     LIMIT 1`,
    [input.tenantId, ctx.legalEntityId, input.profileCode]
  );
  const profileId = parsePositiveInt(lookup.rows?.[0]?.id);
  const row = await getDifferenceProfileById({
    req,
    tenantId: input.tenantId,
    profileId,
    assertScopeAccess,
  });
  return maybeStageDifferenceProfileApproval({
    req,
    tenantId: input.tenantId,
    userId: input.userId,
    row,
    actionType: "CREATE",
    assertScopeAccess,
  });
}

export async function updateDifferenceProfile({ req, input, assertScopeAccess }) {
  const current = await getProfileByIdRaw({ tenantId: input.tenantId, profileId: input.profileId });
  if (!current) throw badRequest("Difference profile not found");
  assertScopeAccess(req, "legal_entity", current.legal_entity_id, "profileId");

  const ctx = await validateCreateOrUpdateContext({
    req,
    tenantId: input.tenantId,
    input: {
      ...input,
      scopeType: current.scope_type,
      legalEntityId: current.legal_entity_id,
      bankAccountId: current.bank_account_id,
      differenceType: current.difference_type,
    },
    current,
    assertScopeAccess,
  });

  const nextProfileName =
    input.profileName !== undefined ? input.profileName : current.profile_name;
  const nextStatus = input.status !== undefined ? input.status : current.status;
  const nextMaxAbsDifference =
    input.maxAbsDifference !== undefined ? input.maxAbsDifference : current.max_abs_difference;
  const nextExpenseAccountId =
    input.expenseAccountId !== undefined ? input.expenseAccountId : current.expense_account_id;
  const nextFxGainAccountId =
    input.fxGainAccountId !== undefined ? input.fxGainAccountId : current.fx_gain_account_id;
  const nextFxLossAccountId =
    input.fxLossAccountId !== undefined ? input.fxLossAccountId : current.fx_loss_account_id;
  const nextCurrencyCode =
    input.currencyCode !== undefined ? input.currencyCode : current.currency_code;
  const nextDescriptionPrefix =
    input.descriptionPrefix !== undefined ? input.descriptionPrefix : current.description_prefix;
  const nextEffectiveFrom =
    input.effectiveFrom !== undefined ? input.effectiveFrom : current.effective_from;
  const nextEffectiveTo = input.effectiveTo !== undefined ? input.effectiveTo : current.effective_to;

  await query(
    `UPDATE bank_reconciliation_difference_profiles
     SET profile_name = ?,
         status = ?,
         max_abs_difference = ?,
         expense_account_id = ?,
         fx_gain_account_id = ?,
         fx_loss_account_id = ?,
         currency_code = ?,
         description_prefix = ?,
         effective_from = ?,
         effective_to = ?,
         updated_by_user_id = ?
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND id = ?`,
    [
      nextProfileName,
      nextStatus,
      toAmount(nextMaxAbsDifference ?? 0) ?? 0,
      ctx.differenceType === "FEE" ? parsePositiveInt(nextExpenseAccountId) || null : null,
      ctx.differenceType === "FX" ? parsePositiveInt(nextFxGainAccountId) || null : null,
      ctx.differenceType === "FX" ? parsePositiveInt(nextFxLossAccountId) || null : null,
      nextCurrencyCode || null,
      nextDescriptionPrefix || null,
      nextEffectiveFrom || null,
      nextEffectiveTo || null,
      input.userId,
      input.tenantId,
      current.legal_entity_id,
      input.profileId,
    ]
  );

  const row = await getDifferenceProfileById({
    req,
    tenantId: input.tenantId,
    profileId: input.profileId,
    assertScopeAccess,
  });
  return maybeStageDifferenceProfileApproval({
    req,
    tenantId: input.tenantId,
    userId: input.userId,
    row,
    actionType: "UPDATE",
    assertScopeAccess,
  });
}

export async function getDifferenceProfileForAutomation({ tenantId, profileId, runQuery = query }) {
  const row = await getProfileByIdRaw({ tenantId, profileId, runQuery });
  const hydrated = hydrate(row);
  if (!hydrated) return null;
  if (u(hydrated.approval_state || "APPROVED") !== "APPROVED") {
    throw badRequest("Difference profile is pending approval");
  }
  return hydrated;
}

async function maybeStageDifferenceProfileApproval({
  req,
  tenantId,
  userId,
  row,
  actionType,
  assertScopeAccess,
}) {
  if (!row) return { row, approval_required: false };
  const legalEntityId = parsePositiveInt(row.legal_entity_id) || null;
  const bankAccountId = parsePositiveInt(row.bank_account_id) || null;
  if (legalEntityId) {
    assertScopeAccess(req, "legal_entity", legalEntityId, "profileId");
  }

  const gov = await evaluateBankApprovalNeed({
    tenantId,
    targetType: "DIFF_PROFILE",
    actionType,
    legalEntityId,
    bankAccountId,
    currencyCode: row.currency_code || null,
  });
  if (!gov?.approvalRequired && !gov?.approval_required) {
    return { row, approval_required: false };
  }

  const submitRes = await submitBankApprovalRequest({
    tenantId,
    userId,
    requestInput: {
      requestKey: `B09:DIFF_PROFILE:${tenantId}:${row.id}:${u(actionType)}:v${Number(
        row.version_no || 1
      )}:${String(row.updated_at || "")}`,
      targetType: "DIFF_PROFILE",
      targetId: row.id,
      actionType: u(actionType),
      legalEntityId,
      bankAccountId,
      currencyCode: row.currency_code || null,
      actionPayload: { profileId: row.id },
    },
    snapshotBuilder: async () => ({
      profile_id: row.id,
      profile_code: row.profile_code,
      profile_name: row.profile_name,
      status: row.status,
      approval_state: row.approval_state || "APPROVED",
      version_no: Number(row.version_no || 1),
      legal_entity_id: legalEntityId,
      bank_account_id: bankAccountId,
      difference_type: row.difference_type,
      max_abs_difference: row.max_abs_difference,
      currency_code: row.currency_code || null,
    }),
    policyOverride: gov,
  });
  const approvalRequestId = parsePositiveInt(submitRes?.item?.id) || null;
  if (!approvalRequestId) return { row, approval_required: false };

  await query(
    `UPDATE bank_reconciliation_difference_profiles
     SET approval_state = 'PENDING_APPROVAL',
         approval_request_id = ?,
         status = CASE WHEN status = 'ACTIVE' THEN 'PAUSED' ELSE status END,
         version_no = CASE WHEN ? = 'UPDATE' THEN version_no + 1 ELSE version_no END,
         updated_by_user_id = COALESCE(?, updated_by_user_id)
     WHERE tenant_id = ?
       AND id = ?`,
    [approvalRequestId, u(actionType), userId || null, tenantId, row.id]
  );

  const staged = await getDifferenceProfileById({
    req,
    tenantId,
    profileId: row.id,
    assertScopeAccess,
  });
  return {
    row: staged,
    approval_required: true,
    approval_request: submitRes.item,
    idempotent: Boolean(submitRes?.idempotent),
  };
}

export async function activateApprovedDifferenceProfileChange({
  tenantId,
  profileId,
  approvalRequestId,
  approvedByUserId,
}) {
  const current = await getProfileByIdRaw({ tenantId, profileId });
  if (!current) throw badRequest("Difference profile not found");
  await query(
    `UPDATE bank_reconciliation_difference_profiles
     SET approval_state = 'APPROVED',
         approval_request_id = ?,
         status = CASE WHEN status = 'PAUSED' THEN 'ACTIVE' ELSE status END,
         updated_by_user_id = ?
     WHERE tenant_id = ?
       AND id = ?`,
    [approvalRequestId || null, approvedByUserId || null, tenantId, profileId]
  );
  return { profile_id: profileId, activated: true };
}

export default {
  resolveDifferenceProfileScope,
  listDifferenceProfiles,
  getDifferenceProfileById,
  createDifferenceProfile,
  updateDifferenceProfile,
  getDifferenceProfileForAutomation,
};
