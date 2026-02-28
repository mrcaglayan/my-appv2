import { query, withTransaction } from "../db.js";
import {
  assertAccountBelongsToTenant,
  assertCurrencyExists,
  assertLegalEntityBelongsToTenant,
} from "../tenantGuards.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";

export const EXPECTED_SIDE_BY_COMPONENT = Object.freeze({
  BASE_SALARY_EXPENSE: "DEBIT",
  OVERTIME_EXPENSE: "DEBIT",
  BONUS_EXPENSE: "DEBIT",
  ALLOWANCES_EXPENSE: "DEBIT",
  EMPLOYER_TAX_EXPENSE: "DEBIT",
  EMPLOYER_SOCIAL_SECURITY_EXPENSE: "DEBIT",
  PAYROLL_NET_PAYABLE: "CREDIT",
  EMPLOYEE_TAX_PAYABLE: "CREDIT",
  EMPLOYEE_SOCIAL_SECURITY_PAYABLE: "CREDIT",
  EMPLOYER_TAX_PAYABLE: "CREDIT",
  EMPLOYER_SOCIAL_SECURITY_PAYABLE: "CREDIT",
  OTHER_DEDUCTIONS_PAYABLE: "CREDIT",
});

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function toDateOnly(value) {
  if (!value) {
    return null;
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      return null;
    }
    return value.toISOString().slice(0, 10);
  }
  const asString = String(value);
  if (/^\d{4}-\d{2}-\d{2}/.test(asString)) {
    return asString.slice(0, 10);
  }
  const parsed = new Date(asString);
  if (Number.isNaN(parsed.getTime())) {
    return asString.slice(0, 10);
  }
  return parsed.toISOString().slice(0, 10);
}

function toAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Number(parsed.toFixed(6));
}

function conflictError(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}

async function countChildAccounts({ accountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT COUNT(*) AS child_count
     FROM accounts
     WHERE parent_account_id = ?`,
    [accountId]
  );
  return Number(result.rows?.[0]?.child_count || 0);
}

async function fetchPayrollMappingAccount({
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
    throw badRequest(`${label} must belong to the same legal entity`);
  }
  if (!parseDbBoolean(row.is_active)) {
    throw badRequest(`${label} must reference an ACTIVE account`);
  }
  if (!parseDbBoolean(row.allow_posting)) {
    throw badRequest(`${label} must reference a postable account`);
  }
  const childCount = await countChildAccounts({ accountId, runQuery });
  if (childCount > 0) {
    throw badRequest(`${label} must reference a leaf account`);
  }
  return row;
}

async function getLegalEntityMappingContext({ tenantId, legalEntityId, runQuery = query }) {
  await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
  const result = await runQuery(
    `SELECT id, tenant_id, code, name, functional_currency_code
     FROM legal_entities
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId]
  );
  const row = result.rows?.[0] || null;
  if (!row) {
    throw badRequest("legalEntityId not found for tenant");
  }
  return row;
}

async function writePayrollMappingAudit({
  tenantId,
  legalEntityId,
  mappingId = null,
  action,
  payload = null,
  userId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO payroll_component_gl_mapping_audit (
        tenant_id,
        legal_entity_id,
        mapping_id,
        action,
        payload_json,
        acted_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, ?)`,
    [tenantId, legalEntityId, mappingId, action, safeJson(payload), userId]
  );
}

async function findPayrollMappingById({ tenantId, mappingId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        m.*,
        a.code AS gl_account_code,
        a.name AS gl_account_name,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM payroll_component_gl_mappings m
     JOIN accounts a ON a.id = m.gl_account_id
     JOIN legal_entities le
       ON le.id = m.legal_entity_id
      AND le.tenant_id = m.tenant_id
     WHERE m.tenant_id = ?
       AND m.id = ?
     LIMIT 1`,
    [tenantId, mappingId]
  );
  const row = result.rows?.[0] || null;
  if (!row) {
    return null;
  }
  return {
    ...row,
    is_active: parseDbBoolean(row.is_active),
    effective_from: toDateOnly(row.effective_from),
    effective_to: toDateOnly(row.effective_to),
  };
}

export async function listPayrollComponentMappingRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["m.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "m.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("m.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }
  if (filters.entityCode) {
    conditions.push("m.entity_code = ?");
    params.push(filters.entityCode);
  }
  if (filters.providerCode) {
    conditions.push("(m.provider_code = ? OR m.provider_code IS NULL)");
    params.push(filters.providerCode);
  }
  if (filters.currencyCode) {
    conditions.push("m.currency_code = ?");
    params.push(filters.currencyCode);
  }
  if (filters.componentCode) {
    conditions.push("m.component_code = ?");
    params.push(filters.componentCode);
  }
  if (filters.activeOnly) {
    conditions.push("m.is_active = 1");
  }
  if (filters.asOfDate) {
    conditions.push("m.effective_from <= ?");
    conditions.push("(m.effective_to IS NULL OR m.effective_to >= ?)");
    params.push(filters.asOfDate, filters.asOfDate);
  }

  const whereSql = conditions.join(" AND ");
  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM payroll_component_gl_mappings m
     WHERE ${whereSql}`,
    params
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const safeLimit =
    Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 200;
  const safeOffset =
    Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;

  const listResult = await query(
    `SELECT
        m.id,
        m.tenant_id,
        m.legal_entity_id,
        m.entity_code,
        m.provider_code,
        m.currency_code,
        m.component_code,
        m.entry_side,
        m.gl_account_id,
        m.effective_from,
        m.effective_to,
        m.is_active,
        m.notes,
        m.created_by_user_id,
        m.created_at,
        m.updated_at,
        a.code AS gl_account_code,
        a.name AS gl_account_name,
        le.name AS legal_entity_name
     FROM payroll_component_gl_mappings m
     JOIN accounts a ON a.id = m.gl_account_id
     JOIN legal_entities le
       ON le.id = m.legal_entity_id
      AND le.tenant_id = m.tenant_id
     WHERE ${whereSql}
     ORDER BY
       m.component_code ASC,
       m.provider_code IS NULL ASC,
       m.provider_code ASC,
       m.effective_from DESC,
       m.id DESC
     LIMIT ${safeLimit} OFFSET ${safeOffset}`,
    params
  );

  const rows = (listResult.rows || []).map((row) => ({
    ...row,
    is_active: parseDbBoolean(row.is_active),
    effective_from: toDateOnly(row.effective_from),
    effective_to: toDateOnly(row.effective_to),
  }));

  return {
    rows,
    total,
    limit: filters.limit,
    offset: filters.offset,
  };
}

export async function upsertPayrollComponentMapping({
  req,
  payload,
  assertScopeAccess,
}) {
  assertScopeAccess(req, "legal_entity", payload.legalEntityId, "legalEntityId");
  await assertCurrencyExists(payload.currencyCode, "currencyCode");

  const expectedSide = EXPECTED_SIDE_BY_COMPONENT[payload.componentCode];
  if (expectedSide && expectedSide !== payload.entrySide) {
    throw badRequest(`entrySide must be ${expectedSide} for ${payload.componentCode}`);
  }
  if (payload.effectiveTo && payload.effectiveTo < payload.effectiveFrom) {
    throw badRequest("effectiveTo cannot be before effectiveFrom");
  }

  const row = await withTransaction(async (tx) => {
    const legalEntity = await getLegalEntityMappingContext({
      tenantId: payload.tenantId,
      legalEntityId: payload.legalEntityId,
      runQuery: tx.query,
    });
    const entityCode = normalizeUpperText(legalEntity.code);
    if (!entityCode) {
      throw badRequest("Legal entity code is required for payroll mappings");
    }
    if (payload.entityCodeInput && payload.entityCodeInput !== entityCode) {
      throw badRequest("entityCode does not match selected legalEntityId");
    }

    await fetchPayrollMappingAccount({
      tenantId: payload.tenantId,
      legalEntityId: payload.legalEntityId,
      accountId: payload.glAccountId,
      label: "glAccountId",
      runQuery: tx.query,
    });

    if (payload.closePreviousOpenMapping) {
      const previousOpen = await tx.query(
        `SELECT id, effective_from
         FROM payroll_component_gl_mappings
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND component_code = ?
           AND currency_code = ?
           AND is_active = 1
           AND ((provider_code IS NULL AND ? IS NULL) OR provider_code = ?)
           AND effective_to IS NULL
           AND effective_from < ?
         ORDER BY effective_from DESC, id DESC
         LIMIT 1`,
        [
          payload.tenantId,
          payload.legalEntityId,
          payload.componentCode,
          payload.currencyCode,
          payload.providerCode,
          payload.providerCode,
          payload.effectiveFrom,
        ]
      );
      const prev = previousOpen.rows?.[0] || null;
      if (prev?.id) {
        await tx.query(
          `UPDATE payroll_component_gl_mappings
           SET effective_to = DATE_SUB(?, INTERVAL 1 DAY),
               updated_at = CURRENT_TIMESTAMP
           WHERE tenant_id = ?
             AND legal_entity_id = ?
             AND id = ?`,
          [payload.effectiveFrom, payload.tenantId, payload.legalEntityId, prev.id]
        );
        await writePayrollMappingAudit({
          tenantId: payload.tenantId,
          legalEntityId: payload.legalEntityId,
          mappingId: parsePositiveInt(prev.id),
          action: "CLOSED_PREVIOUS",
          payload: {
            closed_due_to_new_mapping_effective_from: payload.effectiveFrom,
            componentCode: payload.componentCode,
            providerCode: payload.providerCode,
            currencyCode: payload.currencyCode,
          },
          userId: payload.userId,
          runQuery: tx.query,
        });
      }
    }

    const overlap = await tx.query(
      `SELECT id
       FROM payroll_component_gl_mappings
       WHERE tenant_id = ?
         AND legal_entity_id = ?
         AND component_code = ?
         AND currency_code = ?
         AND is_active = 1
         AND ((provider_code IS NULL AND ? IS NULL) OR provider_code = ?)
         AND NOT (
           COALESCE(effective_to, '9999-12-31') < ?
           OR effective_from > COALESCE(?, '9999-12-31')
         )
       LIMIT 1`,
      [
        payload.tenantId,
        payload.legalEntityId,
        payload.componentCode,
        payload.currencyCode,
        payload.providerCode,
        payload.providerCode,
        payload.effectiveFrom,
        payload.effectiveTo,
      ]
    );
    if (overlap.rows?.[0]?.id) {
      throw conflictError("Overlapping effective-dated mapping exists for this key");
    }

    const insertResult = await tx.query(
      `INSERT INTO payroll_component_gl_mappings (
          tenant_id,
          legal_entity_id,
          entity_code,
          provider_code,
          currency_code,
          component_code,
          entry_side,
          gl_account_id,
          effective_from,
          effective_to,
          is_active,
          notes,
          created_by_user_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
      [
        payload.tenantId,
        payload.legalEntityId,
        entityCode,
        payload.providerCode,
        payload.currencyCode,
        payload.componentCode,
        payload.entrySide,
        payload.glAccountId,
        payload.effectiveFrom,
        payload.effectiveTo,
        payload.notes,
        payload.userId,
      ]
    );

    const mappingId = parsePositiveInt(insertResult.rows?.insertId);
    if (!mappingId) {
      throw new Error("Failed to create payroll component mapping");
    }

    await writePayrollMappingAudit({
      tenantId: payload.tenantId,
      legalEntityId: payload.legalEntityId,
      mappingId,
      action: "CREATED",
      payload: {
        componentCode: payload.componentCode,
        entrySide: payload.entrySide,
        providerCode: payload.providerCode,
        currencyCode: payload.currencyCode,
        glAccountId: payload.glAccountId,
        effectiveFrom: payload.effectiveFrom,
        effectiveTo: payload.effectiveTo,
        notes: payload.notes,
      },
      userId: payload.userId,
      runQuery: tx.query,
    });

    return findPayrollMappingById({
      tenantId: payload.tenantId,
      mappingId,
      runQuery: tx.query,
    });
  });

  if (!row) {
    throw new Error("Failed to load created payroll component mapping");
  }

  return row;
}

export async function setPayrollComponentMappingActive({
  req,
  payload,
  assertScopeAccess,
}) {
  const result = await withTransaction(async (tx) => {
    const existing = await findPayrollMappingById({
      tenantId: payload.tenantId,
      mappingId: payload.mappingId,
      runQuery: tx.query,
    });
    if (!existing) {
      throw badRequest("Payroll mapping not found");
    }

    assertScopeAccess(req, "legal_entity", existing.legal_entity_id, "mappingId");

    const nextIsActive = Boolean(payload.isActive);
    const currentIsActive = Boolean(existing.is_active);
    if (nextIsActive === currentIsActive) {
      return { row: existing, idempotentReplay: true };
    }

    await tx.query(
      `UPDATE payroll_component_gl_mappings
       SET is_active = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ?
         AND id = ?`,
      [nextIsActive ? 1 : 0, payload.tenantId, payload.mappingId]
    );

    await writePayrollMappingAudit({
      tenantId: payload.tenantId,
      legalEntityId: parsePositiveInt(existing.legal_entity_id),
      mappingId: payload.mappingId,
      action: nextIsActive ? "REACTIVATED" : "DEACTIVATED",
      payload: {
        note: payload.note || null,
      },
      userId: payload.userId,
      runQuery: tx.query,
    });

    const row = await findPayrollMappingById({
      tenantId: payload.tenantId,
      mappingId: payload.mappingId,
      runQuery: tx.query,
    });
    return {
      row,
      idempotentReplay: false,
    };
  });

  return result;
}

export function buildPayrollAccrualComponentAmountsFromRun(run) {
  const totals = [
    ["BASE_SALARY_EXPENSE", run?.total_base_salary, "DEBIT"],
    ["OVERTIME_EXPENSE", run?.total_overtime_pay, "DEBIT"],
    ["BONUS_EXPENSE", run?.total_bonus_pay, "DEBIT"],
    ["ALLOWANCES_EXPENSE", run?.total_allowances, "DEBIT"],
    ["EMPLOYER_TAX_EXPENSE", run?.total_employer_tax, "DEBIT"],
    ["EMPLOYER_SOCIAL_SECURITY_EXPENSE", run?.total_employer_social_security, "DEBIT"],
    ["PAYROLL_NET_PAYABLE", run?.total_net_pay, "CREDIT"],
    ["EMPLOYEE_TAX_PAYABLE", run?.total_employee_tax, "CREDIT"],
    ["EMPLOYEE_SOCIAL_SECURITY_PAYABLE", run?.total_employee_social_security, "CREDIT"],
    ["EMPLOYER_TAX_PAYABLE", run?.total_employer_tax, "CREDIT"],
    ["EMPLOYER_SOCIAL_SECURITY_PAYABLE", run?.total_employer_social_security, "CREDIT"],
    ["OTHER_DEDUCTIONS_PAYABLE", run?.total_other_deductions, "CREDIT"],
  ];

  return totals
    .map(([componentCode, rawAmount, entrySide]) => ({
      componentCode,
      entrySide,
      amount: toAmount(rawAmount),
    }))
    .filter((row) => row.amount > 0);
}

export async function findApplicablePayrollComponentMapping({
  tenantId,
  legalEntityId,
  providerCode,
  currencyCode,
  componentCode,
  asOfDate,
  runQuery = query,
}) {
  const normalizedProviderCode = normalizeUpperText(providerCode) || null;
  const result = await runQuery(
    `SELECT
        m.id,
        m.tenant_id,
        m.legal_entity_id,
        m.entity_code,
        m.provider_code,
        m.currency_code,
        m.component_code,
        m.entry_side,
        m.gl_account_id,
        m.effective_from,
        m.effective_to,
        m.is_active,
        m.notes,
        a.code AS gl_account_code,
        a.name AS gl_account_name,
        a.account_type,
        a.normal_side,
        a.allow_posting,
        a.is_active AS account_is_active,
        c.scope AS coa_scope,
        c.legal_entity_id AS coa_legal_entity_id,
        (SELECT COUNT(*) FROM accounts ch WHERE ch.parent_account_id = a.id) AS child_count
     FROM payroll_component_gl_mappings m
     JOIN accounts a ON a.id = m.gl_account_id
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE m.tenant_id = ?
       AND m.legal_entity_id = ?
       AND m.currency_code = ?
       AND m.component_code = ?
       AND m.is_active = 1
       AND m.effective_from <= ?
       AND (m.effective_to IS NULL OR m.effective_to >= ?)
       AND (m.provider_code = ? OR m.provider_code IS NULL)
     ORDER BY
       CASE WHEN m.provider_code = ? THEN 0 ELSE 1 END ASC,
       m.effective_from DESC,
       m.id DESC
     LIMIT 1`,
    [
      tenantId,
      legalEntityId,
      currencyCode,
      componentCode,
      asOfDate,
      asOfDate,
      normalizedProviderCode,
      normalizedProviderCode,
    ]
  );

  const row = result.rows?.[0] || null;
  if (!row) {
    return null;
  }

  return {
    ...row,
    is_active: parseDbBoolean(row.is_active),
    allow_posting: parseDbBoolean(row.allow_posting),
    account_is_active: parseDbBoolean(row.account_is_active),
    effective_from: toDateOnly(row.effective_from),
    effective_to: toDateOnly(row.effective_to),
    child_count: Number(row.child_count || 0),
  };
}
