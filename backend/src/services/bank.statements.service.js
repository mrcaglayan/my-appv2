import crypto from "node:crypto";
import { query, withTransaction } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import {
  decodeCursorToken,
  encodeCursorToken,
  requireCursorDateOnly,
  requireCursorDateTime,
  requireCursorId,
  toCursorDateOnly,
  toCursorDateTime,
} from "../utils/cursorPagination.js";
import { parseStatementCsv } from "./bank.parsers.csv.js";

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function normalizeUpperText(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function isDuplicateEntryError(err) {
  return Number(err?.errno) === 1062 || normalizeUpperText(err?.code) === "ER_DUP_ENTRY";
}

function duplicateKeyName(err) {
  const message = String(err?.sqlMessage || err?.message || "");
  const keyMatch = message.match(/for key ['`"]([^'"`]+)['`"]/i);
  return keyMatch?.[1] || "";
}

function conflictError(message) {
  const err = new Error(message);
  err.status = 409;
  return err;
}

function sha256(value) {
  return crypto.createHash("sha256").update(String(value ?? ""), "utf8").digest("hex");
}

function normalizeHashPart(value) {
  if (value === undefined || value === null) {
    return "";
  }
  return String(value)
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase();
}

function toHashDecimal(value) {
  if (value === undefined || value === null || value === "") {
    return "";
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return "";
  }
  return parsed.toFixed(6);
}

function buildStatementLineHash(bankAccountId, row) {
  const key = [
    parsePositiveInt(bankAccountId) || "",
    normalizeHashPart(row?.txn_date),
    normalizeHashPart(row?.value_date),
    normalizeHashPart(row?.currency_code),
    normalizeHashPart(toHashDecimal(row?.amount)),
    normalizeHashPart(toHashDecimal(row?.balance_after)),
    normalizeHashPart(row?.reference_no),
    normalizeHashPart(row?.description),
  ].join("|");
  return sha256(key);
}

function safeJson(value) {
  if (value === undefined) {
    return null;
  }
  return JSON.stringify(value ?? null);
}

function isLineHashDuplicate(err) {
  if (!isDuplicateEntryError(err)) {
    return false;
  }
  const keyName = duplicateKeyName(err);
  if (!keyName) {
    return true;
  }
  return keyName.includes("uk_bank_stmt_lines_hash");
}

function isImportChecksumDuplicate(err) {
  if (!isDuplicateEntryError(err)) {
    return false;
  }
  const keyName = duplicateKeyName(err);
  return keyName.includes("uk_bank_stmt_imports_checksum");
}

function normalizeDateOnlyText(value, field) {
  const text = String(value || "").trim();
  if (!text) {
    throw badRequest(`${field} is required`);
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    throw badRequest(`${field} must be YYYY-MM-DD`);
  }
  const parsed = new Date(`${text}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== text) {
    throw badRequest(`${field} must be a valid date`);
  }
  return text;
}

function normalizeOptionalDateOnlyText(value, field) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  return normalizeDateOnlyText(value, field);
}

function normalizeNormalizedStatementLine(line, lineNo) {
  if (!line || typeof line !== "object") {
    throw badRequest(`Line ${lineNo} is invalid`);
  }
  const txnDate = normalizeDateOnlyText(line.txn_date ?? line.booking_date, `Line ${lineNo} txn_date`);
  const valueDate = normalizeOptionalDateOnlyText(
    line.value_date ?? line.valueDate,
    `Line ${lineNo} value_date`
  );
  const description = String(line.description || "").trim();
  if (!description) {
    throw badRequest(`Line ${lineNo} description is required`);
  }
  if (description.length > 500) {
    throw badRequest(`Line ${lineNo} description cannot exceed 500 characters`);
  }
  const referenceNoRaw =
    line.reference_no ??
    line.reference ??
    line.external_txn_id ??
    null;
  const referenceNo = referenceNoRaw === null ? null : String(referenceNoRaw).trim() || null;
  if (referenceNo && referenceNo.length > 255) {
    throw badRequest(`Line ${lineNo} reference_no cannot exceed 255 characters`);
  }

  const amount = Number(line.amount);
  if (!Number.isFinite(amount) || amount === 0) {
    throw badRequest(`Line ${lineNo} amount must be a non-zero number`);
  }
  const currencyCode = normalizeUpperText(line.currency_code ?? line.currencyCode);
  if (!currencyCode || currencyCode.length !== 3) {
    throw badRequest(`Line ${lineNo} currency_code must be a 3-letter code`);
  }

  let balanceAfter = null;
  if (line.balance_after !== undefined && line.balance_after !== null && line.balance_after !== "") {
    const parsedBalance = Number(line.balance_after);
    if (!Number.isFinite(parsedBalance)) {
      throw badRequest(`Line ${lineNo} balance_after must be numeric`);
    }
    balanceAfter = Number(parsedBalance.toFixed(6));
  }

  return {
    line_no: lineNo,
    txn_date: txnDate,
    value_date: valueDate,
    description,
    reference_no: referenceNo,
    amount: Number(amount.toFixed(6)),
    currency_code: currencyCode,
    balance_after: balanceAfter,
    raw_row_json: {
      source: "B05_CONNECTOR",
      ...line,
    },
  };
}

async function findBankAccountScopeById({ tenantId, bankAccountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, legal_entity_id
     FROM bank_accounts
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, bankAccountId]
  );
  return result.rows?.[0] || null;
}

async function findBankAccountForImport({ tenantId, bankAccountId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        ba.id,
        ba.tenant_id,
        ba.legal_entity_id,
        ba.code,
        ba.name,
        ba.currency_code,
        ba.is_active,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM bank_accounts ba
     JOIN legal_entities le
       ON le.id = ba.legal_entity_id
      AND le.tenant_id = ba.tenant_id
     WHERE ba.tenant_id = ?
       AND ba.id = ?
     LIMIT 1`,
    [tenantId, bankAccountId]
  );
  return result.rows?.[0] || null;
}

async function findBankStatementImportScopeById({ tenantId, importId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, legal_entity_id, bank_account_id
     FROM bank_statement_imports
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, importId]
  );
  return result.rows?.[0] || null;
}

async function findBankStatementLineScopeById({ tenantId, lineId, runQuery = query }) {
  const result = await runQuery(
    `SELECT id, legal_entity_id, import_id, bank_account_id
     FROM bank_statement_lines
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, lineId]
  );
  return result.rows?.[0] || null;
}

async function findBankStatementImportById({ tenantId, importId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        i.id,
        i.tenant_id,
        i.legal_entity_id,
        i.bank_account_id,
        i.import_source,
        i.original_filename,
        i.file_checksum,
        i.period_start,
        i.period_end,
        i.status,
        i.line_count_total,
        i.line_count_inserted,
        i.line_count_duplicates,
        i.raw_meta_json,
        i.imported_by_user_id,
        i.imported_at,
        i.created_at,
        i.updated_at,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        ba.currency_code AS bank_account_currency_code,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM bank_statement_imports i
     JOIN bank_accounts ba
       ON ba.id = i.bank_account_id
      AND ba.tenant_id = i.tenant_id
      AND ba.legal_entity_id = i.legal_entity_id
     JOIN legal_entities le
       ON le.id = i.legal_entity_id
      AND le.tenant_id = i.tenant_id
     WHERE i.tenant_id = ?
       AND i.id = ?
     LIMIT 1`,
    [tenantId, importId]
  );
  return result.rows?.[0] || null;
}

async function findBankStatementLineById({ tenantId, lineId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
        l.id,
        l.tenant_id,
        l.legal_entity_id,
        l.import_id,
        l.bank_account_id,
        l.line_no,
        l.txn_date,
        l.value_date,
        l.description,
        l.reference_no,
        l.amount,
        l.currency_code,
        l.balance_after,
        l.line_hash,
        l.recon_status,
        l.raw_row_json,
        l.created_at,
        i.original_filename,
        i.status AS import_status,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM bank_statement_lines l
     JOIN bank_statement_imports i
       ON i.id = l.import_id
      AND i.tenant_id = l.tenant_id
      AND i.legal_entity_id = l.legal_entity_id
     JOIN bank_accounts ba
       ON ba.id = l.bank_account_id
      AND ba.tenant_id = l.tenant_id
      AND ba.legal_entity_id = l.legal_entity_id
     JOIN legal_entities le
       ON le.id = l.legal_entity_id
      AND le.tenant_id = l.tenant_id
     WHERE l.tenant_id = ?
       AND l.id = ?
     LIMIT 1`,
    [tenantId, lineId]
  );
  return result.rows?.[0] || null;
}

function ensureStatusAllowed(status, label) {
  const normalized = normalizeUpperText(status);
  if (!normalized) {
    return null;
  }
  return normalized;
}

async function assertBankAccountFilterScope({
  req,
  tenantId,
  bankAccountId,
  assertScopeAccess,
  label = "bankAccountId",
}) {
  const bankRow = await findBankAccountScopeById({ tenantId, bankAccountId });
  if (!bankRow) {
    throw badRequest(`${label} not found`);
  }
  assertScopeAccess(req, "legal_entity", bankRow.legal_entity_id, label);
  return bankRow;
}

async function assertImportFilterScope({
  req,
  tenantId,
  importId,
  assertScopeAccess,
  label = "importId",
}) {
  const importRow = await findBankStatementImportScopeById({ tenantId, importId });
  if (!importRow) {
    throw badRequest(`${label} not found`);
  }
  assertScopeAccess(req, "legal_entity", importRow.legal_entity_id, label);
  return importRow;
}

export async function resolveBankStatementImportScope(importId, tenantId) {
  const parsedImportId = parsePositiveInt(importId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedImportId || !parsedTenantId) {
    return null;
  }

  const row = await findBankStatementImportScopeById({
    tenantId: parsedTenantId,
    importId: parsedImportId,
  });
  if (!row) {
    return null;
  }

  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: parsePositiveInt(row.legal_entity_id),
  };
}

export async function resolveBankStatementLineScope(lineId, tenantId) {
  const parsedLineId = parsePositiveInt(lineId);
  const parsedTenantId = parsePositiveInt(tenantId);
  if (!parsedLineId || !parsedTenantId) {
    return null;
  }

  const row = await findBankStatementLineScopeById({
    tenantId: parsedTenantId,
    lineId: parsedLineId,
  });
  if (!row) {
    return null;
  }

  return {
    scopeType: "LEGAL_ENTITY",
    scopeId: parsePositiveInt(row.legal_entity_id),
  };
}

export async function importBankStatementCsv({
  req,
  payload,
  assertScopeAccess,
}) {
  const bankAccount = await findBankAccountForImport({
    tenantId: payload.tenantId,
    bankAccountId: payload.bankAccountId,
  });
  if (!bankAccount) {
    throw badRequest("bankAccountId not found");
  }

  assertScopeAccess(req, "legal_entity", bankAccount.legal_entity_id, "bankAccountId");
  if (!parseDbBoolean(bankAccount.is_active)) {
    throw badRequest("Selected bank account is not active");
  }

  const fileChecksum = sha256(payload.csvText);
  const existingImport = await query(
    `SELECT id
     FROM bank_statement_imports
     WHERE tenant_id = ?
       AND bank_account_id = ?
       AND file_checksum = ?
     LIMIT 1`,
    [payload.tenantId, payload.bankAccountId, fileChecksum]
  );
  if (existingImport.rows?.[0]?.id) {
    throw conflictError(
      "This statement file was already imported for the selected bank account"
    );
  }

  let parsedRows;
  try {
    parsedRows = parseStatementCsv(payload.csvText);
  } catch (err) {
    throw badRequest(`CSV parse failed: ${err?.message || "Invalid statement CSV"}`);
  }

  const mismatchedCurrencyRow = parsedRows.find(
    (row) => normalizeUpperText(row.currency_code) !== normalizeUpperText(bankAccount.currency_code)
  );
  if (mismatchedCurrencyRow) {
    throw badRequest(
      `Statement currency mismatch. Bank account currency=${bankAccount.currency_code}, row currency=${mismatchedCurrencyRow.currency_code}`
    );
  }

  const txnDates = parsedRows.map((row) => String(row.txn_date)).sort();
  const periodStart = txnDates[0] || null;
  const periodEnd = txnDates[txnDates.length - 1] || null;

  try {
    const createdRow = await withTransaction(async (tx) => {
      const importInsert = await tx.query(
        `INSERT INTO bank_statement_imports (
            tenant_id,
            legal_entity_id,
            bank_account_id,
            import_source,
            original_filename,
            file_checksum,
            period_start,
            period_end,
            status,
            raw_meta_json,
            imported_by_user_id
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'IMPORTED', ?, ?)`,
        [
          payload.tenantId,
          bankAccount.legal_entity_id,
          payload.bankAccountId,
          normalizeUpperText(payload.importSource || "CSV"),
          payload.originalFilename,
          fileChecksum,
          periodStart,
          periodEnd,
          safeJson({
            parser: "csv-v1",
            phase: "import_started",
          }),
          payload.userId,
        ]
      );

      const importId = parsePositiveInt(importInsert.rows?.insertId);
      if (!importId) {
        throw new Error("Failed to create bank statement import");
      }

      let inserted = 0;
      let duplicates = 0;

      for (const row of parsedRows) {
        const lineHash = buildStatementLineHash(payload.bankAccountId, row);

        try {
          await tx.query(
            `INSERT INTO bank_statement_lines (
                tenant_id,
                legal_entity_id,
                import_id,
                bank_account_id,
                line_no,
                txn_date,
                value_date,
                description,
                reference_no,
                amount,
                currency_code,
                balance_after,
                line_hash,
                recon_status,
                raw_row_json
              )
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'UNMATCHED', ?)`,
            [
              payload.tenantId,
              bankAccount.legal_entity_id,
              importId,
              payload.bankAccountId,
              row.line_no,
              row.txn_date,
              row.value_date,
              row.description,
              row.reference_no,
              Number(row.amount).toFixed(6),
              row.currency_code,
              row.balance_after === null ? null : Number(row.balance_after).toFixed(6),
              lineHash,
              safeJson(row.raw_row_json),
            ]
          );
          inserted += 1;
        } catch (err) {
          if (isLineHashDuplicate(err)) {
            duplicates += 1;
            continue;
          }
          throw err;
        }
      }

      const importMeta = {
        parser: "csv-v1",
        totalRowsParsed: parsedRows.length,
        inserted,
        duplicates,
        bankAccountCode: bankAccount.code,
        bankAccountCurrency: bankAccount.currency_code,
      };

      await tx.query(
        `UPDATE bank_statement_imports
         SET line_count_total = ?,
             line_count_inserted = ?,
             line_count_duplicates = ?,
             raw_meta_json = ?
         WHERE tenant_id = ?
           AND id = ?`,
        [
          parsedRows.length,
          inserted,
          duplicates,
          safeJson(importMeta),
          payload.tenantId,
          importId,
        ]
      );

      return findBankStatementImportById({
        tenantId: payload.tenantId,
        importId,
        runQuery: tx.query,
      });
    });

    if (!createdRow) {
      throw new Error("Failed to load created bank statement import");
    }
    return createdRow;
  } catch (err) {
    if (isImportChecksumDuplicate(err)) {
      throw conflictError(
        "This statement file was already imported for the selected bank account"
      );
    }
    throw err;
  }
}

export async function importNormalizedBankStatementLines({
  payload,
}) {
  const tenantId = parsePositiveInt(payload?.tenantId);
  const bankAccountId = parsePositiveInt(payload?.bankAccountId);
  if (!tenantId || !bankAccountId) {
    throw badRequest("tenantId and bankAccountId are required");
  }

  const bankAccount = await findBankAccountForImport({ tenantId, bankAccountId });
  if (!bankAccount) {
    throw badRequest("bankAccountId not found");
  }
  if (!parseDbBoolean(bankAccount.is_active)) {
    throw badRequest("Selected bank account is not active");
  }

  const sourceLines = Array.isArray(payload?.lines) ? payload.lines : [];
  if (sourceLines.length === 0) {
    throw badRequest("lines must contain at least one row");
  }

  const normalizedLines = sourceLines.map((line, index) =>
    normalizeNormalizedStatementLine(line, index + 1)
  );

  const mismatchedCurrency = normalizedLines.find(
    (row) => normalizeUpperText(row.currency_code) !== normalizeUpperText(bankAccount.currency_code)
  );
  if (mismatchedCurrency) {
    throw badRequest(
      `Statement currency mismatch. Bank account currency=${bankAccount.currency_code}, row currency=${mismatchedCurrency.currency_code}`
    );
  }

  const sourceRef = String(payload?.sourceRef || payload?.source_ref || "").trim();
  const sourceFilename = String(payload?.sourceFilename || payload?.source_filename || "").trim();
  const importSource = normalizeUpperText(payload?.importSource || payload?.import_source || "API");
  const userId = parsePositiveInt(payload?.userId ?? payload?.user_id);
  const sourceMeta = payload?.sourceMeta ?? payload?.source_meta ?? {};

  const periodDates = normalizedLines.map((row) => row.txn_date).sort();
  const periodStart = periodDates[0] || null;
  const periodEnd = periodDates[periodDates.length - 1] || null;

  const checksumSeed = {
    sourceRef: sourceRef || null,
    importSource,
    bankAccountId,
    lineHashesPreview: normalizedLines.map((row) =>
      buildStatementLineHash(bankAccountId, row)
    ),
  };
  const fileChecksum = sha256(safeJson(checksumSeed) || "");

  try {
    const importRow = await withTransaction(async (tx) => {
      const importInsert = await tx.query(
        `INSERT INTO bank_statement_imports (
            tenant_id,
            legal_entity_id,
            bank_account_id,
            import_source,
            original_filename,
            file_checksum,
            period_start,
            period_end,
            status,
            raw_meta_json,
            imported_by_user_id
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'IMPORTED', ?, ?)`,
        [
          tenantId,
          bankAccount.legal_entity_id,
          bankAccountId,
          importSource,
          sourceFilename || `${importSource.toLowerCase()}-normalized`,
          fileChecksum,
          periodStart,
          periodEnd,
          safeJson({
            parser: "normalized-v1",
            source_ref: sourceRef || null,
            source_meta: sourceMeta || {},
            totalRowsParsed: normalizedLines.length,
          }),
          userId || null,
        ]
      );
      const importId = parsePositiveInt(importInsert.rows?.insertId);
      if (!importId) {
        throw new Error("Failed to create normalized bank statement import");
      }

      let inserted = 0;
      let duplicates = 0;
      for (const row of normalizedLines) {
        const lineHash = buildStatementLineHash(bankAccountId, row);
        try {
          await tx.query(
            `INSERT INTO bank_statement_lines (
                tenant_id,
                legal_entity_id,
                import_id,
                bank_account_id,
                line_no,
                txn_date,
                value_date,
                description,
                reference_no,
                amount,
                currency_code,
                balance_after,
                line_hash,
                recon_status,
                raw_row_json
              )
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'UNMATCHED', ?)`,
            [
              tenantId,
              bankAccount.legal_entity_id,
              importId,
              bankAccountId,
              row.line_no,
              row.txn_date,
              row.value_date,
              row.description,
              row.reference_no,
              Number(row.amount).toFixed(6),
              row.currency_code,
              row.balance_after === null ? null : Number(row.balance_after).toFixed(6),
              lineHash,
              safeJson(row.raw_row_json),
            ]
          );
          inserted += 1;
        } catch (err) {
          if (isLineHashDuplicate(err)) {
            duplicates += 1;
            continue;
          }
          throw err;
        }
      }

      await tx.query(
        `UPDATE bank_statement_imports
         SET line_count_total = ?,
             line_count_inserted = ?,
             line_count_duplicates = ?,
             raw_meta_json = ?
         WHERE tenant_id = ?
           AND id = ?`,
        [
          normalizedLines.length,
          inserted,
          duplicates,
          safeJson({
            parser: "normalized-v1",
            source_ref: sourceRef || null,
            source_meta: sourceMeta || {},
            totalRowsParsed: normalizedLines.length,
            inserted,
            duplicates,
            bankAccountCode: bankAccount.code,
            bankAccountCurrency: bankAccount.currency_code,
          }),
          tenantId,
          importId,
        ]
      );

      return findBankStatementImportById({ tenantId, importId, runQuery: tx.query });
    });

    return {
      import_id: parsePositiveInt(importRow?.id),
      import_ref:
        sourceRef ||
        `B02NORM-${parsePositiveInt(importRow?.id) || "UNKNOWN"}`,
      imported_count: Number(importRow?.line_count_inserted || 0),
      duplicate_count: Number(importRow?.line_count_duplicates || 0),
      import_row: importRow || null,
    };
  } catch (err) {
    if (isImportChecksumDuplicate(err)) {
      throw conflictError("This normalized statement batch was already imported for the selected bank account");
    }
    throw err;
  }
}

export async function listBankStatementImportRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["i.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "i.legal_entity_id", params));

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("i.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.bankAccountId) {
    const bankScope = await assertBankAccountFilterScope({
      req,
      tenantId,
      bankAccountId: filters.bankAccountId,
      assertScopeAccess,
    });
    if (
      filters.legalEntityId &&
      parsePositiveInt(bankScope.legal_entity_id) !== parsePositiveInt(filters.legalEntityId)
    ) {
      throw badRequest("bankAccountId does not belong to legalEntityId");
    }
    conditions.push("i.bank_account_id = ?");
    params.push(filters.bankAccountId);
  }

  const status = ensureStatusAllowed(filters.status, "status");
  if (status) {
    conditions.push("i.status = ?");
    params.push(status);
  }

  const baseConditions = [...conditions];
  const baseParams = [...params];
  const cursorToken = filters.cursor || null;
  const cursor = decodeCursorToken(cursorToken);
  const pageConditions = [...baseConditions];
  const pageParams = [...baseParams];
  if (cursorToken) {
    const cursorImportedAt = requireCursorDateTime(cursor, "importedAt");
    const cursorId = requireCursorId(cursor, "id");
    pageConditions.push("(i.imported_at < ? OR (i.imported_at = ? AND i.id < ?))");
    pageParams.push(cursorImportedAt, cursorImportedAt, cursorId);
  }
  const whereSql = pageConditions.join(" AND ");
  const countWhereSql = baseConditions.join(" AND ");

  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM bank_statement_imports i
     WHERE ${countWhereSql}`,
    baseParams
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const safeLimit =
    Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const safeOffset =
    Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const effectiveOffset = cursorToken ? 0 : safeOffset;

  const listResult = await query(
    `SELECT
        i.id,
        i.tenant_id,
        i.legal_entity_id,
        i.bank_account_id,
        i.import_source,
        i.original_filename,
        i.file_checksum,
        i.period_start,
        i.period_end,
        i.status,
        i.line_count_total,
        i.line_count_inserted,
        i.line_count_duplicates,
        i.raw_meta_json,
        i.imported_by_user_id,
        i.imported_at,
        i.created_at,
        i.updated_at,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        ba.currency_code AS bank_account_currency_code,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM bank_statement_imports i
     JOIN bank_accounts ba
       ON ba.id = i.bank_account_id
      AND ba.tenant_id = i.tenant_id
      AND ba.legal_entity_id = i.legal_entity_id
     JOIN legal_entities le
       ON le.id = i.legal_entity_id
      AND le.tenant_id = i.tenant_id
     WHERE ${whereSql}
     ORDER BY i.imported_at DESC, i.id DESC
     LIMIT ${safeLimit} OFFSET ${effectiveOffset}`,
    pageParams
  );

  const rows = listResult.rows || [];
  const lastRow = rows.length > 0 ? rows[rows.length - 1] : null;
  const nextCursor =
    cursorToken || safeOffset === 0
      ? rows.length === safeLimit && lastRow
        ? encodeCursorToken({
            importedAt: toCursorDateTime(lastRow.imported_at),
            id: parsePositiveInt(lastRow.id),
          })
        : null
      : null;

  return {
    rows,
    total,
    limit: filters.limit,
    offset: cursorToken ? 0 : filters.offset,
    pageMode: cursorToken ? "CURSOR" : "OFFSET",
    nextCursor,
  };
}

export async function getBankStatementImportByIdForTenant({
  req,
  tenantId,
  importId,
  assertScopeAccess,
}) {
  const row = await findBankStatementImportById({ tenantId, importId });
  if (!row) {
    throw badRequest("Bank statement import not found");
  }
  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "importId");
  return row;
}

export async function listBankStatementLineRows({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const params = [tenantId];
  const conditions = ["l.tenant_id = ?"];
  conditions.push(buildScopeFilter(req, "legal_entity", "l.legal_entity_id", params));

  let scopedBank = null;
  let scopedImport = null;

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    conditions.push("l.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }

  if (filters.bankAccountId) {
    scopedBank = await assertBankAccountFilterScope({
      req,
      tenantId,
      bankAccountId: filters.bankAccountId,
      assertScopeAccess,
    });
    if (
      filters.legalEntityId &&
      parsePositiveInt(scopedBank.legal_entity_id) !== parsePositiveInt(filters.legalEntityId)
    ) {
      throw badRequest("bankAccountId does not belong to legalEntityId");
    }
    conditions.push("l.bank_account_id = ?");
    params.push(filters.bankAccountId);
  }

  if (filters.importId) {
    scopedImport = await assertImportFilterScope({
      req,
      tenantId,
      importId: filters.importId,
      assertScopeAccess,
    });
    if (
      filters.legalEntityId &&
      parsePositiveInt(scopedImport.legal_entity_id) !== parsePositiveInt(filters.legalEntityId)
    ) {
      throw badRequest("importId does not belong to legalEntityId");
    }
    if (
      filters.bankAccountId &&
      parsePositiveInt(scopedImport.bank_account_id) !== parsePositiveInt(filters.bankAccountId)
    ) {
      throw badRequest("importId does not belong to bankAccountId");
    }
    conditions.push("l.import_id = ?");
    params.push(filters.importId);
  }

  if (filters.reconStatus) {
    conditions.push("l.recon_status = ?");
    params.push(normalizeUpperText(filters.reconStatus));
  }

  const baseConditions = [...conditions];
  const baseParams = [...params];
  const cursorToken = filters.cursor || null;
  const cursor = decodeCursorToken(cursorToken);
  const pageConditions = [...baseConditions];
  const pageParams = [...baseParams];
  if (cursorToken) {
    const cursorTxnDate = requireCursorDateOnly(cursor, "txnDate");
    const cursorId = requireCursorId(cursor, "id");
    pageConditions.push("(l.txn_date < ? OR (l.txn_date = ? AND l.id < ?))");
    pageParams.push(cursorTxnDate, cursorTxnDate, cursorId);
  }
  const whereSql = pageConditions.join(" AND ");
  const countWhereSql = baseConditions.join(" AND ");

  const countResult = await query(
    `SELECT COUNT(*) AS total
     FROM bank_statement_lines l
     WHERE ${countWhereSql}`,
    baseParams
  );
  const total = Number(countResult.rows?.[0]?.total || 0);

  const safeLimit =
    Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 200;
  const safeOffset =
    Number.isInteger(filters.offset) && filters.offset >= 0 ? filters.offset : 0;
  const effectiveOffset = cursorToken ? 0 : safeOffset;

  const listResult = await query(
    `SELECT
        l.id,
        l.tenant_id,
        l.legal_entity_id,
        l.import_id,
        l.bank_account_id,
        l.line_no,
        l.txn_date,
        l.value_date,
        l.description,
        l.reference_no,
        l.amount,
        l.currency_code,
        l.balance_after,
        l.line_hash,
        l.recon_status,
        l.raw_row_json,
        l.created_at,
        i.original_filename,
        i.imported_at,
        ba.code AS bank_account_code,
        ba.name AS bank_account_name,
        le.code AS legal_entity_code,
        le.name AS legal_entity_name
     FROM bank_statement_lines l
     JOIN bank_statement_imports i
       ON i.id = l.import_id
      AND i.tenant_id = l.tenant_id
      AND i.legal_entity_id = l.legal_entity_id
     JOIN bank_accounts ba
       ON ba.id = l.bank_account_id
      AND ba.tenant_id = l.tenant_id
      AND ba.legal_entity_id = l.legal_entity_id
     JOIN legal_entities le
       ON le.id = l.legal_entity_id
      AND le.tenant_id = l.tenant_id
     WHERE ${whereSql}
     ORDER BY l.txn_date DESC, l.id DESC
     LIMIT ${safeLimit} OFFSET ${effectiveOffset}`,
    pageParams
  );

  const rows = listResult.rows || [];
  const lastRow = rows.length > 0 ? rows[rows.length - 1] : null;
  const nextCursor =
    cursorToken || safeOffset === 0
      ? rows.length === safeLimit && lastRow
        ? encodeCursorToken({
            txnDate: toCursorDateOnly(lastRow.txn_date),
            id: parsePositiveInt(lastRow.id),
          })
        : null
      : null;

  return {
    rows,
    total,
    limit: filters.limit,
    offset: cursorToken ? 0 : filters.offset,
    pageMode: cursorToken ? "CURSOR" : "OFFSET",
    nextCursor,
  };
}

export async function getBankStatementLineByIdForTenant({
  req,
  tenantId,
  lineId,
  assertScopeAccess,
}) {
  const row = await findBankStatementLineById({ tenantId, lineId });
  if (!row) {
    throw badRequest("Bank statement line not found");
  }
  assertScopeAccess(req, "legal_entity", row.legal_entity_id, "lineId");
  return row;
}
