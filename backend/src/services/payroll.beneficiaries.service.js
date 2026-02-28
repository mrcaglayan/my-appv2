import crypto from "node:crypto";
import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function normalizeEmployeeCode(value) {
  return normalizeUpperText(value || "");
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function parseOptionalJson(value) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function toDateOnly(value) {
  if (!value) return null;
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return value.toISOString().slice(0, 10);
  }
  const text = String(value);
  if (/^\d{4}-\d{2}-\d{2}/.test(text)) return text.slice(0, 10);
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

function makeNotFound(message) {
  const err = new Error(message);
  err.status = 404;
  return err;
}

function makeConflict(message, details = null) {
  const err = new Error(message);
  err.status = 409;
  if (details) err.details = details;
  return err;
}

function cleanAccountIdentifier(value) {
  return String(value || "")
    .replace(/\s+/g, "")
    .trim();
}

function maskLast4FromAccountLike({ iban, accountNumber, account_number }) {
  const raw = cleanAccountIdentifier(iban || accountNumber || account_number);
  return raw ? raw.slice(-4) : null;
}

function fingerprintAccount({
  iban,
  accountNumber,
  account_number,
  routingNumber,
  routing_number,
  swiftBic,
  swift_bic,
  currencyCode,
  currency_code,
}) {
  const base = [
    cleanAccountIdentifier(iban).toUpperCase(),
    cleanAccountIdentifier(accountNumber || account_number),
    cleanAccountIdentifier(routingNumber || routing_number),
    normalizeUpperText(swiftBic || swift_bic),
    normalizeUpperText(currencyCode || currency_code),
  ].join("|");
  return crypto.createHash("sha256").update(base, "utf8").digest("hex");
}

function snapshotHashFromMasterRow(row) {
  const payload = {
    tenant_id: parsePositiveInt(row.tenant_id),
    legal_entity_id: parsePositiveInt(row.legal_entity_id),
    employee_code: normalizeEmployeeCode(row.employee_code),
    employee_name: String(row.employee_name || ""),
    currency_code: normalizeUpperText(row.currency_code),
    account_holder_name: String(row.account_holder_name || ""),
    bank_name: String(row.bank_name || ""),
    bank_branch_name: String(row.bank_branch_name || ""),
    country_code: normalizeUpperText(row.country_code || ""),
    iban: cleanAccountIdentifier(row.iban).toUpperCase(),
    account_number: cleanAccountIdentifier(row.account_number),
    routing_number: cleanAccountIdentifier(row.routing_number),
    swift_bic: normalizeUpperText(row.swift_bic || ""),
    verification_status: normalizeUpperText(row.verification_status || "UNVERIFIED"),
  };
  return crypto.createHash("sha256").update(JSON.stringify(payload), "utf8").digest("hex");
}

function maskedBankRefFromSnapshot(snapshotRow) {
  const iban = cleanAccountIdentifier(snapshotRow?.iban);
  if (iban) return `IBAN ****${iban.slice(-4)}`;
  const accountNumber = cleanAccountIdentifier(snapshotRow?.account_number);
  if (accountNumber) return `ACCT ****${accountNumber.slice(-4)}`;
  const last4 = String(snapshotRow?.account_last4 || "").trim();
  if (last4) return `****${last4}`;
  return `SNAP#${snapshotRow?.id || ""}`.trim();
}

function mapBeneficiaryAccountRow(row) {
  if (!row) return null;
  return {
    ...row,
    employee_code: normalizeEmployeeCode(row.employee_code),
    currency_code: normalizeUpperText(row.currency_code),
    country_code: row.country_code ? normalizeUpperText(row.country_code) : null,
    swift_bic: row.swift_bic ? normalizeUpperText(row.swift_bic) : null,
    status: normalizeUpperText(row.status),
    verification_status: normalizeUpperText(row.verification_status),
    source_type: normalizeUpperText(row.source_type),
    is_primary: row.is_primary === 1 || row.is_primary === true || row.is_primary === "1",
    effective_from: toDateOnly(row.effective_from),
    effective_to: toDateOnly(row.effective_to),
    payload_json: parseOptionalJson(row.payload_json),
  };
}

function mapBeneficiarySnapshotRow(row) {
  if (!row) return null;
  return {
    ...row,
    employee_code: normalizeEmployeeCode(row.employee_code),
    currency_code: normalizeUpperText(row.currency_code),
    country_code: row.country_code ? normalizeUpperText(row.country_code) : null,
    swift_bic: row.swift_bic ? normalizeUpperText(row.swift_bic) : null,
    verification_status: normalizeUpperText(row.verification_status || "UNVERIFIED"),
    payload_json: parseOptionalJson(row.payload_json),
  };
}

async function getBeneficiaryAccountScopeRow({ tenantId, accountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, tenant_id, legal_entity_id
     FROM payroll_beneficiary_bank_accounts
     WHERE tenant_id = ? AND id = ?
     LIMIT 1`,
    [tenantId, accountId]
  );
  return result.rows?.[0] || null;
}

async function getPayrollLiabilityScopeRow({ tenantId, liabilityId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, tenant_id, legal_entity_id
     FROM payroll_run_liabilities
     WHERE tenant_id = ? AND id = ?
     LIMIT 1`,
    [tenantId, liabilityId]
  );
  return result.rows?.[0] || null;
}

async function getBeneficiaryAccountById({
  tenantId,
  legalEntityId = null,
  accountId,
  runQuery = query,
  forUpdate = false,
}) {
  const params = [tenantId, accountId];
  let sql = `SELECT *
             FROM payroll_beneficiary_bank_accounts
             WHERE tenant_id = ? AND id = ?`;
  if (legalEntityId) {
    sql += ` AND legal_entity_id = ?`;
    params.push(legalEntityId);
  }
  sql += ` LIMIT 1`;
  if (forUpdate) sql += ` FOR UPDATE`;
  const result = await runQuery(sql, params);
  return result.rows?.[0] || null;
}

async function writeBeneficiaryAudit({
  tenantId,
  legalEntityId,
  accountId,
  employeeCode,
  action,
  before = null,
  after = null,
  reason = null,
  userId = null,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO payroll_beneficiary_bank_account_audit (
        tenant_id, legal_entity_id, beneficiary_bank_account_id, employee_code,
        action, before_json, after_json, reason, acted_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      tenantId,
      legalEntityId,
      accountId,
      normalizeEmployeeCode(employeeCode),
      action,
      safeJson(before),
      safeJson(after),
      reason || null,
      userId,
    ]
  );
}

async function listBeneficiaryAccountsByEmployee({
  tenantId,
  legalEntityId,
  employeeCode,
  currencyCode = null,
  status = null,
  runQuery = query,
}) {
  const conditions = ["tenant_id = ?", "legal_entity_id = ?", "employee_code = ?"];
  const params = [tenantId, legalEntityId, normalizeEmployeeCode(employeeCode)];
  if (currencyCode) {
    conditions.push("currency_code = ?");
    params.push(normalizeUpperText(currencyCode));
  }
  if (status) {
    conditions.push("status = ?");
    params.push(normalizeUpperText(status));
  }
  const result = await runQuery(
    `SELECT *
     FROM payroll_beneficiary_bank_accounts
     WHERE ${conditions.join(" AND ")}
     ORDER BY
       CASE WHEN status = 'ACTIVE' THEN 0 ELSE 1 END,
       CASE WHEN is_primary = 1 THEN 0 ELSE 1 END,
       id DESC`,
    params
  );
  return (result.rows || []).map(mapBeneficiaryAccountRow);
}

async function resolvePrimaryBeneficiaryBankAccountInternal({
  tenantId,
  legalEntityId,
  employeeCode,
  currencyCode,
  asOfDate = null,
  runQuery = query,
}) {
  const normalizedEmployeeCode = normalizeEmployeeCode(employeeCode);
  const normalizedCurrencyCode = normalizeUpperText(currencyCode);
  if (!normalizedEmployeeCode || !normalizedCurrencyCode) return null;
  const asOf = toDateOnly(asOfDate);
  const result = await runQuery(
    `SELECT *
     FROM payroll_beneficiary_bank_accounts
     WHERE tenant_id = ?
       AND legal_entity_id = ?
       AND employee_code = ?
       AND currency_code = ?
       AND status = 'ACTIVE'
       AND is_primary = 1
       AND (effective_from IS NULL OR effective_from <= COALESCE(?, CURDATE()))
       AND (effective_to IS NULL OR effective_to >= COALESCE(?, CURDATE()))
       AND (
         NULLIF(TRIM(COALESCE(iban, '')), '') IS NOT NULL
         OR NULLIF(TRIM(COALESCE(account_number, '')), '') IS NOT NULL
       )
     ORDER BY id DESC
     LIMIT 1`,
    [tenantId, legalEntityId, normalizedEmployeeCode, normalizedCurrencyCode, asOf, asOf]
  );
  return mapBeneficiaryAccountRow(result.rows?.[0] || null);
}

async function createOrGetBeneficiarySnapshotFromMasterInternal({
  tenantId,
  legalEntityId,
  masterAccount,
  userId = null,
  runQuery = query,
}) {
  const snapshotHash = snapshotHashFromMasterRow(masterAccount);
  const existing = await runQuery(
    `SELECT *
     FROM payroll_beneficiary_bank_snapshots
     WHERE tenant_id = ? AND legal_entity_id = ? AND snapshot_hash = ?
     LIMIT 1`,
    [tenantId, legalEntityId, snapshotHash]
  );
  if (existing.rows?.[0]) return mapBeneficiarySnapshotRow(existing.rows[0]);

  const insertResult = await runQuery(
    `INSERT INTO payroll_beneficiary_bank_snapshots (
        tenant_id, legal_entity_id, employee_code, employee_name,
        source_beneficiary_bank_account_id, snapshot_hash, currency_code,
        account_holder_name, bank_name, bank_branch_name,
        country_code, iban, account_number, routing_number, swift_bic, account_last4,
        verification_status, payload_json, created_by_user_id
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      tenantId,
      legalEntityId,
      normalizeEmployeeCode(masterAccount.employee_code),
      masterAccount.employee_name || null,
      parsePositiveInt(masterAccount.id),
      snapshotHash,
      normalizeUpperText(masterAccount.currency_code),
      masterAccount.account_holder_name,
      masterAccount.bank_name,
      masterAccount.bank_branch_name || null,
      masterAccount.country_code || null,
      masterAccount.iban || null,
      masterAccount.account_number || null,
      masterAccount.routing_number || null,
      masterAccount.swift_bic || null,
      masterAccount.account_last4 || maskLast4FromAccountLike(masterAccount),
      normalizeUpperText(masterAccount.verification_status || "UNVERIFIED"),
      safeJson({
        source_type: normalizeUpperText(masterAccount.source_type || "MANUAL"),
        external_ref: masterAccount.external_ref || null,
      }),
      userId,
    ]
  );
  const snapshotId = parsePositiveInt(insertResult.rows?.insertId);
  if (!snapshotId) throw new Error("Failed to create payroll beneficiary snapshot");
  const row = await runQuery(
    `SELECT *
     FROM payroll_beneficiary_bank_snapshots
     WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?
     LIMIT 1`,
    [tenantId, legalEntityId, snapshotId]
  );
  return mapBeneficiarySnapshotRow(row.rows?.[0] || null);
}

export async function resolvePayrollBeneficiaryAccountScope(accountId, tenantId) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedAccountId = parsePositiveInt(accountId);
  if (!parsedTenantId || !parsedAccountId) return null;
  const row = await getBeneficiaryAccountScopeRow({ tenantId: parsedTenantId, accountId: parsedAccountId });
  if (!row) return null;
  return { scopeType: "LEGAL_ENTITY", scopeId: parsePositiveInt(row.legal_entity_id) };
}

export async function resolvePayrollBeneficiaryLiabilitySnapshotScope(liabilityId, tenantId) {
  const parsedTenantId = parsePositiveInt(tenantId);
  const parsedLiabilityId = parsePositiveInt(liabilityId);
  if (!parsedTenantId || !parsedLiabilityId) return null;
  const row = await getPayrollLiabilityScopeRow({ tenantId: parsedTenantId, liabilityId: parsedLiabilityId });
  if (!row) return null;
  return { scopeType: "LEGAL_ENTITY", scopeId: parsePositiveInt(row.legal_entity_id) };
}

export async function listPayrollEmployeeBeneficiaryBankAccounts({
  req,
  tenantId,
  legalEntityId,
  employeeCode,
  currencyCode = null,
  status = null,
  assertScopeAccess,
}) {
  assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
  const items = await listBeneficiaryAccountsByEmployee({
    tenantId,
    legalEntityId,
    employeeCode,
    currencyCode,
    status,
  });
  return {
    legal_entity_id: legalEntityId,
    employee_code: normalizeEmployeeCode(employeeCode),
    items,
  };
}

export async function createPayrollEmployeeBeneficiaryBankAccount({
  req,
  tenantId,
  userId,
  input,
  assertScopeAccess,
}) {
  assertScopeAccess(req, "legal_entity", input.legalEntityId, "legalEntityId");

  return withTransaction(async (tx) => {
    if (input.effectiveFrom && input.effectiveTo && input.effectiveFrom > input.effectiveTo) {
      throw badRequest("effectiveTo must be >= effectiveFrom");
    }

    const accountLast4 = maskLast4FromAccountLike(input);
    const accountFingerprint = fingerprintAccount(input);

    if (input.isPrimary) {
      await tx.query(
        `UPDATE payroll_beneficiary_bank_accounts
         SET is_primary = 0, updated_by_user_id = ?, updated_at = CURRENT_TIMESTAMP
         WHERE tenant_id = ? AND legal_entity_id = ? AND employee_code = ? AND currency_code = ?
           AND status = 'ACTIVE'`,
        [
          userId,
          tenantId,
          input.legalEntityId,
          normalizeEmployeeCode(input.employeeCode),
          normalizeUpperText(input.currencyCode),
        ]
      );
    }

    const ins = await tx.query(
      `INSERT INTO payroll_beneficiary_bank_accounts (
          tenant_id, legal_entity_id, employee_code, employee_name,
          account_holder_name, bank_name, bank_branch_name, country_code, currency_code,
          iban, account_number, routing_number, swift_bic,
          account_last4, account_fingerprint,
          is_primary, status, effective_from, effective_to,
          verification_status, source_type, external_ref, payload_json,
          created_by_user_id, updated_by_user_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        tenantId,
        input.legalEntityId,
        normalizeEmployeeCode(input.employeeCode),
        input.employeeName || null,
        input.accountHolderName,
        input.bankName,
        input.bankBranchName || null,
        input.countryCode || null,
        normalizeUpperText(input.currencyCode),
        input.iban || null,
        input.accountNumber || null,
        input.routingNumber || null,
        input.swiftBic || null,
        accountLast4 || null,
        accountFingerprint || null,
        input.isPrimary ? 1 : 0,
        input.effectiveFrom || null,
        input.effectiveTo || null,
        normalizeUpperText(input.verificationStatus || "UNVERIFIED"),
        normalizeUpperText(input.sourceType || "MANUAL"),
        input.externalRef || null,
        safeJson(null),
        userId,
        userId,
      ]
    );
    const accountId = parsePositiveInt(ins.rows?.insertId);
    if (!accountId) throw new Error("Failed to create payroll beneficiary bank account");

    const row = await getBeneficiaryAccountById({
      tenantId,
      legalEntityId: input.legalEntityId,
      accountId,
      runQuery: tx.query,
    });
    if (!row) throw new Error("Created payroll beneficiary bank account not found");

    await writeBeneficiaryAudit({
      tenantId,
      legalEntityId: input.legalEntityId,
      accountId,
      employeeCode: input.employeeCode,
      action: "CREATED",
      before: null,
      after: row,
      reason: input.reason || "Created payroll beneficiary bank account",
      userId,
      runQuery: tx.query,
    });

    return { item: mapBeneficiaryAccountRow(row) };
  });
}

export async function updatePayrollEmployeeBeneficiaryBankAccount({
  req,
  tenantId,
  userId,
  accountId,
  input,
  assertScopeAccess,
}) {
  return withTransaction(async (tx) => {
    const current = await getBeneficiaryAccountById({
      tenantId,
      accountId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!current) throw makeNotFound("Payroll beneficiary bank account not found");

    const legalEntityId = parsePositiveInt(current.legal_entity_id);
    assertScopeAccess(req, "legal_entity", legalEntityId, "accountId");

    if (
      current.is_primary === 1 &&
      input.currencyCode &&
      normalizeUpperText(input.currencyCode) !== normalizeUpperText(current.currency_code)
    ) {
      throw makeConflict("Cannot change currency_code of a primary beneficiary account; set another primary first");
    }

    const merged = {
      ...current,
      employee_name:
        input.employeeName !== undefined ? input.employeeName : current.employee_name,
      account_holder_name:
        input.accountHolderName !== undefined ? input.accountHolderName : current.account_holder_name,
      bank_name: input.bankName !== undefined ? input.bankName : current.bank_name,
      bank_branch_name:
        input.bankBranchName !== undefined ? input.bankBranchName : current.bank_branch_name,
      country_code: input.countryCode !== undefined ? input.countryCode : current.country_code,
      currency_code:
        input.currencyCode !== undefined && input.currencyCode !== null
          ? input.currencyCode
          : current.currency_code,
      iban: input.iban !== undefined ? input.iban : current.iban,
      account_number: input.accountNumber !== undefined ? input.accountNumber : current.account_number,
      routing_number: input.routingNumber !== undefined ? input.routingNumber : current.routing_number,
      swift_bic: input.swiftBic !== undefined ? input.swiftBic : current.swift_bic,
      status: input.status || current.status,
      verification_status: input.verificationStatus || current.verification_status,
      effective_from:
        input.effectiveFrom !== undefined ? input.effectiveFrom : toDateOnly(current.effective_from),
      effective_to:
        input.effectiveTo !== undefined ? input.effectiveTo : toDateOnly(current.effective_to),
      external_ref:
        input.externalRef !== undefined ? input.externalRef : current.external_ref,
    };

    if (merged.effective_from && merged.effective_to && merged.effective_from > merged.effective_to) {
      throw badRequest("effectiveTo must be >= effectiveFrom");
    }
    if (!String(merged.iban || "").trim() && !String(merged.account_number || "").trim()) {
      throw badRequest("Either iban or accountNumber is required");
    }

    const nextStatus = normalizeUpperText(merged.status || "ACTIVE");
    const nextIsPrimary = nextStatus === "INACTIVE" ? 0 : Number(current.is_primary || 0);
    const accountLast4 = maskLast4FromAccountLike(merged);
    const accountFingerprint = fingerprintAccount(merged);

    await tx.query(
      `UPDATE payroll_beneficiary_bank_accounts
       SET employee_name = ?,
           account_holder_name = ?,
           bank_name = ?,
           bank_branch_name = ?,
           country_code = ?,
           currency_code = ?,
           iban = ?,
           account_number = ?,
           routing_number = ?,
           swift_bic = ?,
           account_last4 = ?,
           account_fingerprint = ?,
           is_primary = ?,
           status = ?,
           verification_status = ?,
           effective_from = ?,
           effective_to = ?,
           external_ref = ?,
           updated_by_user_id = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
      [
        merged.employee_name || null,
        merged.account_holder_name,
        merged.bank_name,
        merged.bank_branch_name || null,
        merged.country_code || null,
        normalizeUpperText(merged.currency_code),
        merged.iban || null,
        merged.account_number || null,
        merged.routing_number || null,
        merged.swift_bic || null,
        accountLast4 || null,
        accountFingerprint || null,
        nextIsPrimary,
        nextStatus,
        normalizeUpperText(merged.verification_status || "UNVERIFIED"),
        merged.effective_from || null,
        merged.effective_to || null,
        merged.external_ref || null,
        userId,
        tenantId,
        legalEntityId,
        accountId,
      ]
    );

    const updated = await getBeneficiaryAccountById({
      tenantId,
      legalEntityId,
      accountId,
      runQuery: tx.query,
    });
    if (!updated) throw new Error("Updated payroll beneficiary bank account not found");

    await writeBeneficiaryAudit({
      tenantId,
      legalEntityId,
      accountId,
      employeeCode: updated.employee_code,
      action: normalizeUpperText(updated.status) === "INACTIVE" ? "DEACTIVATED" : "UPDATED",
      before: mapBeneficiaryAccountRow(current),
      after: updated,
      reason: input.reason || "Updated payroll beneficiary bank account",
      userId,
      runQuery: tx.query,
    });

    return { item: mapBeneficiaryAccountRow(updated) };
  });
}

export async function setPrimaryPayrollEmployeeBeneficiaryBankAccount({
  req,
  tenantId,
  userId,
  accountId,
  reason,
  assertScopeAccess,
}) {
  return withTransaction(async (tx) => {
    const current = await getBeneficiaryAccountById({
      tenantId,
      accountId,
      runQuery: tx.query,
      forUpdate: true,
    });
    if (!current) throw makeNotFound("Payroll beneficiary bank account not found");

    const legalEntityId = parsePositiveInt(current.legal_entity_id);
    assertScopeAccess(req, "legal_entity", legalEntityId, "accountId");
    if (normalizeUpperText(current.status) !== "ACTIVE") {
      throw makeConflict("Only ACTIVE beneficiary bank accounts can be set as primary");
    }

    await tx.query(
      `UPDATE payroll_beneficiary_bank_accounts
       SET is_primary = 0, updated_by_user_id = ?, updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND legal_entity_id = ? AND employee_code = ? AND currency_code = ?
         AND status = 'ACTIVE'`,
      [
        userId,
        tenantId,
        legalEntityId,
        normalizeEmployeeCode(current.employee_code),
        normalizeUpperText(current.currency_code),
      ]
    );

    await tx.query(
      `UPDATE payroll_beneficiary_bank_accounts
       SET is_primary = 1, updated_by_user_id = ?, updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
      [userId, tenantId, legalEntityId, accountId]
    );

    const updated = await getBeneficiaryAccountById({
      tenantId,
      legalEntityId,
      accountId,
      runQuery: tx.query,
    });
    if (!updated) throw new Error("Primary payroll beneficiary bank account not found after update");

    await writeBeneficiaryAudit({
      tenantId,
      legalEntityId,
      accountId,
      employeeCode: updated.employee_code,
      action: "SET_PRIMARY",
      before: mapBeneficiaryAccountRow(current),
      after: updated,
      reason: reason || "Set primary payroll beneficiary bank account",
      userId,
      runQuery: tx.query,
    });

    return { item: mapBeneficiaryAccountRow(updated) };
  });
}

export async function resolvePrimaryPayrollBeneficiaryBankAccount({
  tenantId,
  legalEntityId,
  employeeCode,
  currencyCode,
  asOfDate = null,
  runQuery = query,
}) {
  return resolvePrimaryBeneficiaryBankAccountInternal({
    tenantId,
    legalEntityId,
    employeeCode,
    currencyCode,
    asOfDate,
    runQuery,
  });
}

export async function createOrGetPayrollBeneficiarySnapshotFromMaster({
  tenantId,
  legalEntityId,
  masterAccount,
  userId = null,
  runQuery = query,
}) {
  return createOrGetBeneficiarySnapshotFromMasterInternal({
    tenantId,
    legalEntityId,
    masterAccount,
    userId,
    runQuery,
  });
}

export async function markPayrollBeneficiarySnapshotNotRequiredTx({
  tenantId,
  legalEntityId,
  paymentLinkId,
  runQuery = query,
}) {
  await runQuery(
    `UPDATE payroll_liability_payment_links
     SET beneficiary_bank_snapshot_id = NULL,
         beneficiary_snapshot_status = 'NOT_REQUIRED',
         updated_at = CURRENT_TIMESTAMP
     WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
    [tenantId, legalEntityId, paymentLinkId]
  );
}

export async function attachPayrollBeneficiarySnapshotToLinkTx({
  tenantId,
  legalEntityId,
  paymentLinkId,
  paymentBatchLineId = null,
  payrollLiabilityId = null,
  beneficiaryType,
  employeeCode,
  employeeName = null,
  currencyCode,
  asOfDate = null,
  userId = null,
  runQuery = query,
}) {
  const normalizedBeneficiaryType = normalizeUpperText(beneficiaryType);
  if (normalizedBeneficiaryType !== "EMPLOYEE") {
    await markPayrollBeneficiarySnapshotNotRequiredTx({
      tenantId,
      legalEntityId,
      paymentLinkId,
      runQuery,
    });
    return { required: false, snapshot: null, status: "NOT_REQUIRED" };
  }

  const normalizedEmployeeCode = normalizeEmployeeCode(employeeCode);
  if (!normalizedEmployeeCode) {
    throw makeConflict("Employee beneficiary setup is missing employee_code", {
      payroll_liability_id: parsePositiveInt(payrollLiabilityId),
      action_required:
        "Ensure payroll liability has employee_code and configure a primary beneficiary bank account",
    });
  }

  const master = await resolvePrimaryBeneficiaryBankAccountInternal({
    tenantId,
    legalEntityId,
    employeeCode: normalizedEmployeeCode,
    currencyCode,
    asOfDate,
    runQuery,
  });
  if (!master) {
    const err = makeConflict(
      `No active primary beneficiary bank account for employee ${normalizedEmployeeCode} (${normalizeUpperText(
        currencyCode
      )})`,
      {
        payroll_liability_id: parsePositiveInt(payrollLiabilityId),
        employee_code: normalizedEmployeeCode,
        currency_code: normalizeUpperText(currencyCode),
        action_required:
          "Set primary beneficiary bank account before creating payroll payment batch",
      }
    );
    err.code = "PAYROLL_BENEFICIARY_MISSING";
    throw err;
  }

  const snapshot = await createOrGetBeneficiarySnapshotFromMasterInternal({
    tenantId,
    legalEntityId,
    masterAccount: {
      ...master,
      employee_name: employeeName || master.employee_name || null,
    },
    userId,
    runQuery,
  });

  await runQuery(
    `UPDATE payroll_liability_payment_links
     SET beneficiary_bank_snapshot_id = ?,
         beneficiary_snapshot_status = 'CAPTURED',
         updated_at = CURRENT_TIMESTAMP
     WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
    [parsePositiveInt(snapshot.id), tenantId, legalEntityId, paymentLinkId]
  );

  if (paymentBatchLineId) {
    await runQuery(
      `UPDATE payment_batch_lines
       SET beneficiary_bank_ref = COALESCE(NULLIF(beneficiary_bank_ref, ''), ?),
           updated_at = CURRENT_TIMESTAMP
       WHERE tenant_id = ? AND legal_entity_id = ? AND id = ?`,
      [maskedBankRefFromSnapshot(snapshot), tenantId, legalEntityId, paymentBatchLineId]
    );
  }

  return { required: true, snapshot, status: "CAPTURED" };
}

export async function assertPayrollBeneficiarySetupForLiabilities({
  tenantId,
  legalEntityId,
  liabilities,
  runCurrencyCode,
  asOfDate = null,
  runQuery = query,
}) {
  const employeeLiabilities = (liabilities || []).filter(
    (row) => normalizeUpperText(row?.beneficiary_type) === "EMPLOYEE"
  );
  if (employeeLiabilities.length === 0) {
    return { checked_count: 0, missing: [] };
  }

  const seen = new Set();
  const missing = [];
  for (const row of employeeLiabilities) {
    const employeeCode = normalizeEmployeeCode(row?.employee_code);
    const currencyCode = normalizeUpperText(row?.currency_code || runCurrencyCode);
    if (!employeeCode || !currencyCode) {
      missing.push({
        payroll_liability_id: parsePositiveInt(row?.id),
        employee_code: employeeCode || null,
        currency_code: currencyCode || null,
        reason: "missing_employee_code_or_currency",
      });
      continue;
    }

    const key = `${employeeCode}|${currencyCode}`;
    if (seen.has(key)) continue;
    seen.add(key);

    // eslint-disable-next-line no-await-in-loop
    const master = await resolvePrimaryBeneficiaryBankAccountInternal({
      tenantId,
      legalEntityId,
      employeeCode,
      currencyCode,
      asOfDate,
      runQuery,
    });
    if (!master) {
      missing.push({
        employee_code: employeeCode,
        currency_code: currencyCode,
        reason: "missing_active_primary_beneficiary_bank_account",
      });
    }
  }

  if (missing.length > 0) {
    const sampleText = missing
      .slice(0, 5)
      .map(
        (row) =>
          `${row.employee_code || `liability#${row.payroll_liability_id || "?"}`}/${row.currency_code || "?"}`
      )
      .join(", ");
    const suffix = missing.length > 5 ? ` (+${missing.length - 5} more)` : "";
    const err = makeConflict(
      `Payroll payment batch preparation blocked: missing beneficiary bank setup (${sampleText}${suffix})`,
      {
        missing,
        action_required:
          "Create active primary payroll beneficiary bank account(s) for listed employee_code + currency_code pairs",
      }
    );
    err.code = "PAYROLL_BENEFICIARY_MISSING";
    throw err;
  }

  return { checked_count: seen.size, missing: [] };
}

export async function getPayrollLiabilityBeneficiarySnapshot({
  req,
  tenantId,
  liabilityId,
  assertScopeAccess,
}) {
  const result = await query(
    `SELECT
        l.id AS payroll_liability_id,
        l.tenant_id,
        l.legal_entity_id,
        l.run_id,
        l.liability_type,
        l.liability_group,
        l.employee_code,
        l.employee_name,
        l.beneficiary_type,
        l.beneficiary_name,
        l.status AS liability_status,
        l.currency_code,
        pl.id AS payment_link_id,
        pl.payment_batch_id,
        pl.payment_batch_line_id,
        pl.beneficiary_bank_snapshot_id,
        pl.beneficiary_snapshot_status,
        s.id AS snapshot_id,
        s.employee_code AS snapshot_employee_code,
        s.employee_name AS snapshot_employee_name,
        s.source_beneficiary_bank_account_id,
        s.snapshot_hash,
        s.currency_code AS snapshot_currency_code,
        s.account_holder_name,
        s.bank_name,
        s.bank_branch_name,
        s.country_code,
        s.iban,
        s.account_number,
        s.routing_number,
        s.swift_bic,
        s.account_last4,
        s.verification_status,
        s.payload_json,
        s.created_by_user_id,
        s.created_at
     FROM payroll_run_liabilities l
     LEFT JOIN payroll_liability_payment_links pl
       ON pl.tenant_id = l.tenant_id
      AND pl.legal_entity_id = l.legal_entity_id
      AND pl.run_id = l.run_id
      AND pl.payroll_liability_id = l.id
      AND pl.id = (
        SELECT pl2.id
        FROM payroll_liability_payment_links pl2
        WHERE pl2.tenant_id = l.tenant_id
          AND pl2.legal_entity_id = l.legal_entity_id
          AND pl2.run_id = l.run_id
          AND pl2.payroll_liability_id = l.id
        ORDER BY pl2.id DESC
        LIMIT 1
      )
     LEFT JOIN payroll_beneficiary_bank_snapshots s
       ON s.tenant_id = pl.tenant_id
      AND s.legal_entity_id = pl.legal_entity_id
      AND s.id = pl.beneficiary_bank_snapshot_id
     WHERE l.tenant_id = ? AND l.id = ?
     LIMIT 1`,
    [tenantId, liabilityId]
  );
  const row = result.rows?.[0] || null;
  if (!row) throw makeNotFound("Payroll liability not found");

  const legalEntityId = parsePositiveInt(row.legal_entity_id);
  assertScopeAccess(req, "legal_entity", legalEntityId, "liabilityId");

  const snapshot =
    row.snapshot_id != null
      ? mapBeneficiarySnapshotRow({
          id: row.snapshot_id,
          tenant_id: row.tenant_id,
          legal_entity_id: row.legal_entity_id,
          employee_code: row.snapshot_employee_code,
          employee_name: row.snapshot_employee_name,
          source_beneficiary_bank_account_id: row.source_beneficiary_bank_account_id,
          snapshot_hash: row.snapshot_hash,
          currency_code: row.snapshot_currency_code,
          account_holder_name: row.account_holder_name,
          bank_name: row.bank_name,
          bank_branch_name: row.bank_branch_name,
          country_code: row.country_code,
          iban: row.iban,
          account_number: row.account_number,
          routing_number: row.routing_number,
          swift_bic: row.swift_bic,
          account_last4: row.account_last4,
          verification_status: row.verification_status,
          payload_json: row.payload_json,
          created_by_user_id: row.created_by_user_id,
          created_at: row.created_at,
        })
      : null;

  return {
    item: {
      payroll_liability_id: parsePositiveInt(row.payroll_liability_id),
      legal_entity_id: legalEntityId,
      run_id: parsePositiveInt(row.run_id),
      liability_type: row.liability_type,
      liability_group: row.liability_group,
      employee_code: row.employee_code || null,
      employee_name: row.employee_name || null,
      beneficiary_type: row.beneficiary_type || null,
      beneficiary_name: row.beneficiary_name || null,
      liability_status: normalizeUpperText(row.liability_status),
      currency_code: normalizeUpperText(row.currency_code),
      payment_link_id: parsePositiveInt(row.payment_link_id),
      payment_batch_id: parsePositiveInt(row.payment_batch_id),
      payment_batch_line_id: parsePositiveInt(row.payment_batch_line_id),
      beneficiary_bank_snapshot_id: parsePositiveInt(row.beneficiary_bank_snapshot_id),
      beneficiary_snapshot_status: normalizeUpperText(row.beneficiary_snapshot_status || "PENDING"),
      snapshot,
    },
  };
}

export default {
  resolvePayrollBeneficiaryAccountScope,
  resolvePayrollBeneficiaryLiabilitySnapshotScope,
  listPayrollEmployeeBeneficiaryBankAccounts,
  createPayrollEmployeeBeneficiaryBankAccount,
  updatePayrollEmployeeBeneficiaryBankAccount,
  setPrimaryPayrollEmployeeBeneficiaryBankAccount,
  resolvePrimaryPayrollBeneficiaryBankAccount,
  createOrGetPayrollBeneficiarySnapshotFromMaster,
  markPayrollBeneficiarySnapshotNotRequiredTx,
  attachPayrollBeneficiarySnapshotToLinkTx,
  assertPayrollBeneficiarySetupForLiabilities,
  getPayrollLiabilityBeneficiarySnapshot,
};
