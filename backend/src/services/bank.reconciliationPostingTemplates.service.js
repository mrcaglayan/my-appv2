import { query } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { evaluateBankApprovalNeed } from "./bank.governance.service.js";
import { submitBankApprovalRequest } from "./bank.approvals.service.js";

function u(value) {
  return String(value || "").trim().toUpperCase();
}

function safeJsonParse(value, fallback = null) {
  if (value === undefined || value === null) return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function toAmount(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Number(n.toFixed(6)) : null;
}

function hydrateTemplateRow(row) {
  if (!row) return null;
  return {
    ...row,
    min_amount_abs: row.min_amount_abs === null ? null : toAmount(row.min_amount_abs),
    max_amount_abs: row.max_amount_abs === null ? null : toAmount(row.max_amount_abs),
    tax_rate: row.tax_rate === null ? null : Number(Number(row.tax_rate).toFixed(4)),
  };
}

async function getTemplateById({ tenantId, templateId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        t.*,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        ba.currency_code AS bank_account_currency_code,
        a.code AS counter_account_code,
        a.name AS counter_account_name,
        a.account_type AS counter_account_type
     FROM bank_reconciliation_posting_templates t
     LEFT JOIN legal_entities le
       ON le.tenant_id = t.tenant_id
      AND le.id = t.legal_entity_id
     LEFT JOIN bank_accounts ba
       ON ba.tenant_id = t.tenant_id
      AND ba.legal_entity_id = t.legal_entity_id
      AND ba.id = t.bank_account_id
     LEFT JOIN accounts a
       ON a.id = t.counter_account_id
     WHERE t.tenant_id = ?
       AND t.id = ?
     LIMIT 1`,
    [tenantId, templateId]
  );
  return hydrateTemplateRow(result.rows?.[0] || null);
}

async function getLegalEntityById({ tenantId, legalEntityId }) {
  const result = await query(
    `SELECT id, code, name
     FROM legal_entities
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId]
  );
  return result.rows?.[0] || null;
}

async function getBankAccountScope({ tenantId, bankAccountId }) {
  const result = await query(
    `SELECT id, legal_entity_id, currency_code, code, name, is_active
     FROM bank_accounts
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, bankAccountId]
  );
  return result.rows?.[0] || null;
}

async function getCounterAccount({ tenantId, accountId }) {
  const result = await query(
    `SELECT
        a.id,
        c.tenant_id AS tenant_id,
        c.legal_entity_id AS legal_entity_id,
        c.scope AS scope,
        a.account_type,
        a.allow_posting,
        a.is_active,
        a.code,
        a.name
     FROM accounts a
     JOIN charts_of_accounts c
       ON c.id = a.coa_id
     WHERE c.tenant_id = ?
       AND a.id = ?
     LIMIT 1`,
    [tenantId, accountId]
  );
  return result.rows?.[0] || null;
}

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function normalizeTaxWriteFields({ taxMode, taxAccountId, taxRate }) {
  const mode = u(taxMode || "NONE");
  if (mode === "NONE") {
    const hasTaxAccount = Boolean(parsePositiveInt(taxAccountId));
    const hasTaxRate =
      taxRate !== undefined && taxRate !== null && String(taxRate).trim() !== "";
    if (hasTaxAccount || hasTaxRate) {
      throw badRequest("taxAccountId and taxRate must be omitted when taxMode=NONE");
    }
    return {
      taxMode: "NONE",
      taxAccountId: null,
      taxRate: null,
    };
  }

  if (mode !== "INCLUDED") {
    throw badRequest("taxMode must be one of NONE, INCLUDED");
  }

  const parsedTaxAccountId = parsePositiveInt(taxAccountId);
  if (!parsedTaxAccountId) {
    throw badRequest("taxAccountId is required when taxMode=INCLUDED");
  }
  const parsedTaxRate = Number(taxRate);
  if (!Number.isFinite(parsedTaxRate) || parsedTaxRate <= 0 || parsedTaxRate >= 100) {
    throw badRequest("taxRate must be > 0 and < 100 when taxMode=INCLUDED");
  }
  return {
    taxMode: "INCLUDED",
    taxAccountId: parsedTaxAccountId,
    taxRate: Number(parsedTaxRate.toFixed(4)),
  };
}

function normalizeTemplateScopeForWrite(input, current = null) {
  const next = {
    scopeType: input.scopeType ?? current?.scope_type ?? "LEGAL_ENTITY",
    legalEntityId:
      input.legalEntityId !== undefined ? input.legalEntityId : current?.legal_entity_id ?? null,
    bankAccountId:
      input.bankAccountId !== undefined ? input.bankAccountId : current?.bank_account_id ?? null,
  };

  if (next.scopeType === "BANK_ACCOUNT") {
    if (!next.bankAccountId) throw badRequest("bankAccountId is required for BANK_ACCOUNT scope");
  }
  if (next.scopeType === "LEGAL_ENTITY" && !next.legalEntityId) {
    throw badRequest("legalEntityId is required for LEGAL_ENTITY scope");
  }
  if (next.scopeType === "GLOBAL") {
    // Repo-native constraint: v1 templates must still be legal-entity anchored for GL posting.
    if (!next.legalEntityId) {
      throw badRequest("legalEntityId is required for GLOBAL templates in this repo (v1)");
    }
    next.bankAccountId = null;
  }
  return next;
}

async function validateTemplateWriteContext({ req, tenantId, input, scope, assertScopeAccess }) {
  let resolvedLegalEntityId = parsePositiveInt(scope.legalEntityId) || null;
  let bankAccount = null;

  if (scope.bankAccountId) {
    bankAccount = await getBankAccountScope({
      tenantId,
      bankAccountId: scope.bankAccountId,
    });
    if (!bankAccount) throw badRequest("bankAccountId not found");
    if (!parseDbBoolean(bankAccount.is_active)) {
      throw badRequest("Selected bankAccountId is not active");
    }
    resolvedLegalEntityId = parsePositiveInt(bankAccount.legal_entity_id);
    if (!resolvedLegalEntityId) throw badRequest("bankAccountId legal entity scope is invalid");
  }

  if (!resolvedLegalEntityId) {
    throw badRequest("legalEntityId is required for B08 templates in this repo");
  }

  const legalEntity = await getLegalEntityById({ tenantId, legalEntityId: resolvedLegalEntityId });
  if (!legalEntity) throw badRequest("legalEntityId not found");
  assertScopeAccess(req, "legal_entity", resolvedLegalEntityId, scope.bankAccountId ? "bankAccountId" : "legalEntityId");

  const counterAccountId = parsePositiveInt(input.counterAccountId ?? input.counter_account_id);
  const counterAccount = counterAccountId
    ? await getCounterAccount({ tenantId, accountId: counterAccountId })
    : null;
  if (counterAccountId && !counterAccount) {
    throw badRequest("counterAccountId not found");
  }
  const taxAccountId = parsePositiveInt(input.taxAccountId ?? input.tax_account_id);
  const taxAccount = taxAccountId ? await getCounterAccount({ tenantId, accountId: taxAccountId }) : null;
  if (taxAccountId && !taxAccount) {
    throw badRequest("taxAccountId not found");
  }

  function assertTemplateGlAccount(account, { label }) {
    if (!account) return;
    if (!parseDbBoolean(account.is_active)) {
      throw badRequest(`${label} GL account is inactive`);
    }
    if (!parseDbBoolean(account.allow_posting)) {
      throw badRequest(`${label} GL account must be postable`);
    }
    if (u(account.scope) !== "LEGAL_ENTITY") {
      throw badRequest(`${label} GL account must be LEGAL_ENTITY scoped`);
    }
    if (parsePositiveInt(account.legal_entity_id) !== resolvedLegalEntityId) {
      throw badRequest(`${label} GL account must belong to the selected template legal entity`);
    }
  }

  assertTemplateGlAccount(counterAccount, { label: "Counter" });
  assertTemplateGlAccount(taxAccount, { label: "Tax" });

  return {
    legalEntityId: resolvedLegalEntityId,
    legalEntity,
    bankAccount,
    counterAccount,
    taxAccount,
  };
}

export async function resolvePostingTemplateScope(templateId, tenantId) {
  const parsedTemplateId = parsePositiveInt(templateId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedTemplateId || !parsedTenantId) return null;
  const row = await getTemplateById({ tenantId: parsedTenantId, templateId: parsedTemplateId });
  if (!row) return null;
  if (parsePositiveInt(row.legal_entity_id)) {
    return { scopeType: "LEGAL_ENTITY", scopeId: parsePositiveInt(row.legal_entity_id) };
  }
  return null;
}

export async function listPostingTemplateRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["t.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "t.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("t.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }
  if (filters.bankAccountId) {
    const bankAccount = await getBankAccountScope({ tenantId, bankAccountId: filters.bankAccountId });
    if (!bankAccount) throw badRequest("bankAccountId not found");
    assertScopeAccess(req, "legal_entity", bankAccount.legal_entity_id, "bankAccountId");
    conditions.push("t.bank_account_id = ?");
    params.push(filters.bankAccountId);
  }
  if (filters.status) {
    conditions.push("t.status = ?");
    params.push(filters.status);
  }
  if (filters.q) {
    const like = `%${filters.q}%`;
    conditions.push("(t.template_code LIKE ? OR t.template_name LIKE ?)");
    params.push(like, like);
  }

  const whereSql = conditions.join(" AND ");
  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM bank_reconciliation_posting_templates t
     WHERE ${whereSql}`,
    params
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset = Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const listResult = await query(
    `SELECT
        t.*,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        a.code AS counter_account_code,
        a.name AS counter_account_name,
        a.account_type AS counter_account_type
     FROM bank_reconciliation_posting_templates t
     LEFT JOIN legal_entities le
       ON le.tenant_id = t.tenant_id
      AND le.id = t.legal_entity_id
     LEFT JOIN bank_accounts ba
       ON ba.tenant_id = t.tenant_id
      AND ba.legal_entity_id = t.legal_entity_id
      AND ba.id = t.bank_account_id
     LEFT JOIN accounts a
       ON a.id = t.counter_account_id
     WHERE ${whereSql}
     ORDER BY
       CASE WHEN t.status = 'ACTIVE' THEN 0 WHEN t.status = 'PAUSED' THEN 1 ELSE 2 END,
       t.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  return {
    rows: (listResult.rows || []).map(hydrateTemplateRow),
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function getPostingTemplateByIdForTenant({
  req,
  tenantId,
  templateId,
  assertScopeAccess,
}) {
  const row = await getTemplateById({ tenantId, templateId });
  if (!row) throw badRequest("Posting template not found");
  if (parsePositiveInt(row.legal_entity_id)) {
    assertScopeAccess(req, "legal_entity", row.legal_entity_id, "templateId");
  }
  return row;
}

export async function createPostingTemplate({
  req,
  input,
  assertScopeAccess,
}) {
  const normalizedTax = normalizeTaxWriteFields({
    taxMode: input.taxMode,
    taxAccountId: input.taxAccountId,
    taxRate: input.taxRate,
  });
  const normalizedInput = {
    ...input,
    taxMode: normalizedTax.taxMode,
    taxAccountId: normalizedTax.taxAccountId,
    taxRate: normalizedTax.taxRate,
  };
  const scope = normalizeTemplateScopeForWrite(normalizedInput);
  const validated = await validateTemplateWriteContext({
    req,
    tenantId: normalizedInput.tenantId,
    input: normalizedInput,
    scope,
    assertScopeAccess,
  });

  const insertResult = await query(
    `INSERT INTO bank_reconciliation_posting_templates (
        tenant_id,
        legal_entity_id,
        template_code,
        template_name,
        status,
        scope_type,
        bank_account_id,
        entry_kind,
        direction_policy,
        counter_account_id,
        tax_account_id,
        tax_mode,
        tax_rate,
        currency_code,
        min_amount_abs,
        max_amount_abs,
        description_mode,
        fixed_description,
        description_prefix,
        journal_source_code,
        journal_doc_type,
        effective_from,
        effective_to,
        created_by_user_id,
        updated_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      normalizedInput.tenantId,
      validated.legalEntityId,
      normalizedInput.templateCode,
      normalizedInput.templateName,
      normalizedInput.status,
      scope.scopeType,
      scope.scopeType === "BANK_ACCOUNT" ? scope.bankAccountId : null,
      normalizedInput.entryKind,
      normalizedInput.directionPolicy,
      normalizedInput.counterAccountId,
      normalizedInput.taxAccountId || null,
      normalizedInput.taxMode || "NONE",
      normalizedInput.taxRate === null ? null : normalizedInput.taxRate,
      normalizedInput.currencyCode || null,
      normalizedInput.minAmountAbs === null ? null : normalizedInput.minAmountAbs,
      normalizedInput.maxAmountAbs === null ? null : normalizedInput.maxAmountAbs,
      normalizedInput.descriptionMode,
      normalizedInput.fixedDescription || null,
      normalizedInput.descriptionPrefix || null,
      u(normalizedInput.journalSourceCode || "BANK_AUTO_POST"),
      u(normalizedInput.journalDocType || "BANK_AUTO"),
      normalizedInput.effectiveFrom || null,
      normalizedInput.effectiveTo || null,
      normalizedInput.userId || null,
      normalizedInput.userId || null,
    ]
  );

  const created = await getTemplateById({
    tenantId: normalizedInput.tenantId,
    templateId: parsePositiveInt(insertResult.rows?.insertId),
  });
  return maybeStagePostingTemplateApproval({
    req,
    tenantId: normalizedInput.tenantId,
    userId: normalizedInput.userId,
    row: created,
    actionType: "CREATE",
    assertScopeAccess,
  });
}

export async function updatePostingTemplate({
  req,
  input,
  assertScopeAccess,
}) {
  const current = await getTemplateById({ tenantId: input.tenantId, templateId: input.templateId });
  if (!current) throw badRequest("Posting template not found");
  if (parsePositiveInt(current.legal_entity_id)) {
    assertScopeAccess(req, "legal_entity", current.legal_entity_id, "templateId");
  }

  const normalizedTax = normalizeTaxWriteFields({
    taxMode: input.taxMode !== undefined ? input.taxMode : current.tax_mode,
    taxAccountId:
      input.taxAccountId !== undefined ? input.taxAccountId : current.tax_account_id,
    taxRate: input.taxRate !== undefined ? input.taxRate : current.tax_rate,
  });
  const scope = normalizeTemplateScopeForWrite(input, current);
  const validated = await validateTemplateWriteContext({
    req,
    tenantId: input.tenantId,
    input: {
      ...current,
      ...input,
      counterAccountId:
        input.counterAccountId !== undefined ? input.counterAccountId : current.counter_account_id,
      taxMode: normalizedTax.taxMode,
      taxAccountId: normalizedTax.taxAccountId,
      taxRate: normalizedTax.taxRate,
    },
    scope,
    assertScopeAccess,
  });

  const next = {
    templateName: input.templateName !== undefined ? input.templateName : current.template_name,
    status: input.status !== undefined ? input.status : current.status,
    entryKind: input.entryKind !== undefined ? input.entryKind : current.entry_kind,
    directionPolicy:
      input.directionPolicy !== undefined ? input.directionPolicy : current.direction_policy,
    counterAccountId:
      input.counterAccountId !== undefined ? input.counterAccountId : current.counter_account_id,
    taxAccountId: normalizedTax.taxAccountId,
    taxMode: normalizedTax.taxMode,
    taxRate: normalizedTax.taxRate,
    currencyCode:
      input.currencyCode !== undefined ? input.currencyCode : current.currency_code,
    minAmountAbs:
      input.minAmountAbs !== undefined ? input.minAmountAbs : current.min_amount_abs,
    maxAmountAbs:
      input.maxAmountAbs !== undefined ? input.maxAmountAbs : current.max_amount_abs,
    descriptionMode:
      input.descriptionMode !== undefined ? input.descriptionMode : current.description_mode,
    fixedDescription:
      input.fixedDescription !== undefined ? input.fixedDescription : current.fixed_description,
    descriptionPrefix:
      input.descriptionPrefix !== undefined ? input.descriptionPrefix : current.description_prefix,
    journalSourceCode:
      input.journalSourceCode !== undefined
        ? input.journalSourceCode
        : current.journal_source_code,
    journalDocType:
      input.journalDocType !== undefined ? input.journalDocType : current.journal_doc_type,
    effectiveFrom:
      input.effectiveFrom !== undefined ? input.effectiveFrom : current.effective_from,
    effectiveTo: input.effectiveTo !== undefined ? input.effectiveTo : current.effective_to,
  };

  await query(
    `UPDATE bank_reconciliation_posting_templates
     SET legal_entity_id = ?,
         template_name = ?,
         status = ?,
         scope_type = ?,
         bank_account_id = ?,
         entry_kind = ?,
         direction_policy = ?,
         counter_account_id = ?,
         tax_account_id = ?,
         tax_mode = ?,
         tax_rate = ?,
         currency_code = ?,
         min_amount_abs = ?,
         max_amount_abs = ?,
         description_mode = ?,
         fixed_description = ?,
         description_prefix = ?,
         journal_source_code = ?,
         journal_doc_type = ?,
         effective_from = ?,
         effective_to = ?,
         updated_by_user_id = ?
     WHERE tenant_id = ?
       AND id = ?`,
    [
      validated.legalEntityId,
      next.templateName,
      next.status,
      scope.scopeType,
      scope.scopeType === "BANK_ACCOUNT" ? scope.bankAccountId : null,
      next.entryKind,
      next.directionPolicy,
      next.counterAccountId,
      next.taxAccountId || null,
      next.taxMode || "NONE",
      next.taxRate === null ? null : next.taxRate,
      next.currencyCode || null,
      next.minAmountAbs === null ? null : next.minAmountAbs,
      next.maxAmountAbs === null ? null : next.maxAmountAbs,
      next.descriptionMode,
      next.fixedDescription || null,
      next.descriptionPrefix || null,
      u(next.journalSourceCode || "BANK_AUTO_POST"),
      u(next.journalDocType || "BANK_AUTO"),
      next.effectiveFrom || null,
      next.effectiveTo || null,
      input.userId || null,
      input.tenantId,
      input.templateId,
    ]
  );

  const updated = await getTemplateById({ tenantId: input.tenantId, templateId: input.templateId });
  return maybeStagePostingTemplateApproval({
    req,
    tenantId: input.tenantId,
    userId: input.userId,
    row: updated,
    actionType: "UPDATE",
    assertScopeAccess,
  });
}

export async function getPostingTemplateByIdForAutoPost({
  tenantId,
  templateId,
  runQuery = query,
}) {
  const row = await getTemplateById({ tenantId, templateId, runQuery });
  if (!row) return null;
  if (u(row.approval_state || "APPROVED") !== "APPROVED") {
    throw badRequest("Posting template is pending approval");
  }
  return row;
}

async function maybeStagePostingTemplateApproval({
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
    assertScopeAccess(req, "legal_entity", legalEntityId, "templateId");
  }

  const gov = await evaluateBankApprovalNeed({
    tenantId,
    targetType: "POST_TEMPLATE",
    actionType,
    legalEntityId,
    bankAccountId,
  });
  if (!gov?.approvalRequired && !gov?.approval_required) {
    return { row, approval_required: false };
  }

  const submitRes = await submitBankApprovalRequest({
    tenantId,
    userId,
    requestInput: {
      requestKey: `B09:POST_TEMPLATE:${tenantId}:${row.id}:${u(actionType)}:v${Number(
        row.version_no || 1
      )}:${String(row.updated_at || "")}`,
      targetType: "POST_TEMPLATE",
      targetId: row.id,
      actionType: u(actionType),
      legalEntityId,
      bankAccountId,
      currencyCode: row.currency_code || null,
      actionPayload: { templateId: row.id },
    },
    snapshotBuilder: async () => ({
      template_id: row.id,
      template_code: row.template_code,
      template_name: row.template_name,
      status: row.status,
      approval_state: row.approval_state || "APPROVED",
      version_no: Number(row.version_no || 1),
      scope_type: row.scope_type,
      legal_entity_id: legalEntityId,
      bank_account_id: bankAccountId,
      entry_kind: row.entry_kind,
      direction_policy: row.direction_policy,
      counter_account_id: row.counter_account_id,
      currency_code: row.currency_code || null,
    }),
    policyOverride: gov,
  });
  const approvalRequestId = parsePositiveInt(submitRes?.item?.id) || null;
  if (!approvalRequestId) return { row, approval_required: false };

  await query(
    `UPDATE bank_reconciliation_posting_templates
     SET approval_state = 'PENDING_APPROVAL',
         approval_request_id = ?,
         status = CASE WHEN status = 'ACTIVE' THEN 'PAUSED' ELSE status END,
         version_no = CASE WHEN ? = 'UPDATE' THEN version_no + 1 ELSE version_no END,
         updated_by_user_id = COALESCE(?, updated_by_user_id)
     WHERE tenant_id = ?
       AND id = ?`,
    [approvalRequestId, u(actionType), userId || null, tenantId, row.id]
  );

  const staged = await getTemplateById({ tenantId, templateId: row.id });
  return {
    row: staged,
    approval_required: true,
    approval_request: submitRes.item,
    idempotent: Boolean(submitRes?.idempotent),
  };
}

export async function activateApprovedPostingTemplateChange({
  tenantId,
  templateId,
  approvalRequestId,
  approvedByUserId,
}) {
  const current = await getTemplateById({ tenantId, templateId });
  if (!current) throw badRequest("Posting template not found");
  await query(
    `UPDATE bank_reconciliation_posting_templates
     SET approval_state = 'APPROVED',
         approval_request_id = ?,
         status = CASE WHEN status = 'PAUSED' THEN 'ACTIVE' ELSE status END,
         updated_by_user_id = ?
     WHERE tenant_id = ?
       AND id = ?`,
    [approvalRequestId || null, approvedByUserId || null, tenantId, templateId]
  );
  return { template_id: templateId, activated: true };
}

export default {
  resolvePostingTemplateScope,
  listPostingTemplateRows,
  getPostingTemplateByIdForTenant,
  createPostingTemplate,
  updatePostingTemplate,
  getPostingTemplateByIdForAutoPost,
};
