import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function inClause(values) {
  if (!Array.isArray(values) || values.length === 0) {
    throw new Error("inClause values must be non-empty");
  }
  return values.map(() => "?").join(", ");
}

async function expectDbError(work, { label, errno }) {
  try {
    await work();
  } catch (err) {
    if (errno !== undefined && err?.errno !== errno) {
      throw new Error(
        `${label} expected errno=${errno}, got errno=${String(err?.errno)} message=${String(
          err?.message || "unknown"
        )}`
      );
    }
    return;
  }
  throw new Error(`${label} expected DB error but operation succeeded`);
}

async function assertSchemaExists() {
  const requiredTables = [
    "counterparties",
    "counterparty_contacts",
    "counterparty_addresses",
    "payment_terms",
    "cari_documents",
    "cari_open_items",
    "cari_settlement_batches",
    "cari_settlement_allocations",
    "cari_unapplied_cash",
  ];

  const tableRows = await query(
    `SELECT table_name AS table_name
     FROM information_schema.tables
     WHERE table_schema = DATABASE()
       AND table_name IN (${inClause(requiredTables)})`,
    requiredTables
  );
  const existingTables = new Set(tableRows.rows.map((row) => String(row.table_name)));
  const missingTables = requiredTables.filter((name) => !existingTables.has(name));
  assert(missingTables.length === 0, `Missing Cari tables: ${missingTables.join(", ")}`);

  const requiredColumns = [
    ["counterparties", "is_customer"],
    ["counterparties", "is_vendor"],
    ["cari_settlement_batches", "sequence_namespace"],
    ["cari_settlement_batches", "fiscal_year"],
    ["cari_settlement_batches", "sequence_no"],
    ["cari_settlement_batches", "settlement_no"],
    ["cari_settlement_batches", "status"],
    ["cari_settlement_batches", "posted_journal_entry_id"],
    ["cari_settlement_batches", "reversal_of_settlement_batch_id"],
    ["cari_settlement_batches", "bank_statement_line_id"],
    ["cari_settlement_batches", "bank_attach_idempotency_key"],
    ["cari_settlement_batches", "bank_apply_idempotency_key"],
    ["cari_documents", "status"],
    ["cari_documents", "posted_journal_entry_id"],
    ["cari_documents", "reversal_of_document_id"],
    ["cari_documents", "counterparty_code_snapshot"],
    ["cari_documents", "counterparty_name_snapshot"],
    ["cari_documents", "payment_term_snapshot"],
    ["cari_documents", "due_date_snapshot"],
    ["cari_documents", "currency_code_snapshot"],
    ["cari_documents", "fx_rate_snapshot"],
    ["cari_open_items", "original_amount_txn"],
    ["cari_open_items", "original_amount_base"],
    ["cari_open_items", "residual_amount_txn"],
    ["cari_open_items", "residual_amount_base"],
    ["cari_open_items", "document_date"],
    ["cari_open_items", "due_date"],
    ["cari_open_items", "status"],
    ["cari_settlement_allocations", "allocation_amount_txn"],
    ["cari_settlement_allocations", "allocation_amount_base"],
    ["cari_settlement_allocations", "settlement_batch_id"],
    ["cari_settlement_allocations", "open_item_id"],
    ["cari_settlement_allocations", "apply_idempotency_key"],
    ["cari_unapplied_cash", "bank_statement_line_id"],
    ["cari_unapplied_cash", "bank_attach_idempotency_key"],
    ["cari_unapplied_cash", "bank_apply_idempotency_key"],
  ];

  const columnsRows = await query(
    `SELECT
       table_name AS table_name,
       column_name AS column_name,
       is_nullable AS is_nullable
     FROM information_schema.columns
     WHERE table_schema = DATABASE()
       AND table_name IN (${inClause(requiredTables)})`,
    requiredTables
  );
  const columnMap = new Map();
  for (const row of columnsRows.rows) {
    const tableName = String(row.table_name);
    const key = `${tableName}.${String(row.column_name)}`;
    columnMap.set(key, String(row.is_nullable));
  }

  for (const [tableName, columnName] of requiredColumns) {
    const key = `${tableName}.${columnName}`;
    assert(columnMap.has(key), `Missing required column: ${key}`);
  }

  const nullableBankLinkColumns = [
    ["cari_settlement_batches", "bank_statement_line_id"],
    ["cari_settlement_batches", "bank_attach_idempotency_key"],
    ["cari_settlement_batches", "bank_apply_idempotency_key"],
    ["cari_settlement_allocations", "bank_statement_line_id"],
    ["cari_settlement_allocations", "bank_apply_idempotency_key"],
    ["cari_unapplied_cash", "bank_statement_line_id"],
    ["cari_unapplied_cash", "bank_attach_idempotency_key"],
    ["cari_unapplied_cash", "bank_apply_idempotency_key"],
  ];

  for (const [tableName, columnName] of nullableBankLinkColumns) {
    const key = `${tableName}.${columnName}`;
    const nullableFlag = columnMap.get(key);
    assert(nullableFlag === "YES", `Bank link column must be nullable: ${key}`);
  }

  const expectedIndexes = new Map([
    ["uk_counterparties_tenant_entity_code", 0],
    ["ix_counterparties_tenant_entity_roles", 1],
    ["uk_cari_docs_tenant_entity_adr_doc_no", 0],
    ["uk_cari_settle_batches_tenant_seq", 0],
    ["uk_cari_alloc_apply_idempo", 0],
    ["uk_cari_settle_batches_bank_attach_idempo", 0],
    ["uk_cari_settle_batches_bank_apply_idempo", 0],
    ["uk_cari_unap_bank_attach_idempo", 0],
    ["uk_cari_unap_bank_apply_idempo", 0],
    ["ix_cari_docs_tenant_document_date", 1],
    ["ix_cari_docs_tenant_due_date", 1],
    ["ix_cari_oi_tenant_due_date", 1],
    ["ix_cari_oi_tenant_residual_txn", 1],
    ["ix_cari_oi_tenant_residual_base", 1],
  ]);

  const indexRows = await query(
    `SELECT index_name AS index_name, non_unique AS non_unique
     FROM information_schema.statistics
     WHERE table_schema = DATABASE()
       AND index_name IN (${inClause([...expectedIndexes.keys()])})`,
    [...expectedIndexes.keys()]
  );
  const indexMap = new Map();
  for (const row of indexRows.rows) {
    indexMap.set(String(row.index_name), toNumber(row.non_unique));
  }

  for (const [indexName, expectedNonUnique] of expectedIndexes.entries()) {
    assert(indexMap.has(indexName), `Missing required index: ${indexName}`);
    assert(
      indexMap.get(indexName) === expectedNonUnique,
      `Index non_unique mismatch for ${indexName}; expected ${expectedNonUnique}, got ${indexMap.get(
        indexName
      )}`
    );
  }

  const expectedFks = [
    "fk_counterparties_entity_tenant",
    "fk_cp_contacts_counterparty_tenant",
    "fk_cp_addresses_counterparty_tenant",
    "fk_cari_docs_journal_tenant",
    "fk_cari_docs_reversal_tenant",
    "fk_cari_oi_document_tenant",
    "fk_cari_settle_reversal_tenant",
    "fk_cari_alloc_batch_tenant",
    "fk_cari_alloc_open_item_tenant",
    "fk_cari_unap_settle_batch_tenant",
    "fk_cari_unap_reversal_tenant",
  ];
  const fkRows = await query(
    `SELECT constraint_name AS constraint_name
     FROM information_schema.referential_constraints
     WHERE constraint_schema = DATABASE()
       AND constraint_name IN (${inClause(expectedFks)})`,
    expectedFks
  );
  const existingFks = new Set(fkRows.rows.map((row) => String(row.constraint_name)));
  const missingFks = expectedFks.filter((fkName) => !existingFks.has(fkName));
  assert(missingFks.length === 0, `Missing required FK constraints: ${missingFks.join(", ")}`);
}

async function createFixture() {
  const stamp = Date.now();
  const fiscalYear = 2026;
  const tenantCode = `CARI_PR01_T_${stamp}`;
  const groupCode = `CARI_PR01_G_${stamp}`;
  const legalEntityCode = `CARI_PR01_LE_${stamp}`;
  const paymentTermCode = `CARI_NET_${stamp}`;
  const counterpartyCode = `CARI_CP_${stamp}`;
  const currencyCode = "USD";

  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `CARI PR01 Tenant ${stamp}`]
  );

  const tenantResult = await query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [tenantCode]
  );
  const tenantId = toNumber(tenantResult.rows[0]?.id);
  assert(tenantId > 0, "Failed to create tenant fixture");

  const usCountryResult = await query(
    `SELECT id
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(usCountryResult.rows[0]?.id);
  assert(countryId > 0, "US country seed row is required");

  const groupInsert = await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, groupCode, `CARI PR01 Group ${stamp}`]
  );
  const groupCompanyId = toNumber(groupInsert.rows?.insertId);
  assert(groupCompanyId > 0, "Failed to create group company fixture");

  const legalEntityInsert = await query(
    `INSERT INTO legal_entities (
        tenant_id,
        group_company_id,
        code,
        name,
        country_id,
        functional_currency_code,
        status
      )
      VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      groupCompanyId,
      legalEntityCode,
      `CARI PR01 Legal Entity ${stamp}`,
      countryId,
      currencyCode,
    ]
  );
  const legalEntityId = toNumber(legalEntityInsert.rows?.insertId);
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  const paymentTermInsert = await query(
    `INSERT INTO payment_terms (
        tenant_id,
        legal_entity_id,
        code,
        name,
        due_days,
        grace_days,
        is_end_of_month,
        status
      )
      VALUES (?, ?, ?, ?, 30, 0, FALSE, 'ACTIVE')`,
    [tenantId, legalEntityId, paymentTermCode, "Net 30"]
  );
  const paymentTermId = toNumber(paymentTermInsert.rows?.insertId);
  assert(paymentTermId > 0, "Failed to create payment term fixture");

  const counterpartyInsert = await query(
    `INSERT INTO counterparties (
        tenant_id,
        legal_entity_id,
        code,
        name,
        is_customer,
        is_vendor,
        default_currency_code,
        default_payment_term_id,
        status
      )
      VALUES (?, ?, ?, ?, TRUE, FALSE, ?, ?, 'ACTIVE')`,
    [
      tenantId,
      legalEntityId,
      counterpartyCode,
      `CARI Counterparty ${stamp}`,
      currencyCode,
      paymentTermId,
    ]
  );
  const counterpartyId = toNumber(counterpartyInsert.rows?.insertId);
  assert(counterpartyId > 0, "Failed to create counterparty fixture");

  const documentDate = `${fiscalYear}-01-15`;
  const dueDate = `${fiscalYear}-02-14`;
  const documentNo = `CARI-DOC-${stamp}-1`;

  const documentInsert = await query(
    `INSERT INTO cari_documents (
        tenant_id,
        legal_entity_id,
        counterparty_id,
        payment_term_id,
        direction,
        document_type,
        sequence_namespace,
        fiscal_year,
        sequence_no,
        document_no,
        status,
        document_date,
        due_date,
        amount_txn,
        amount_base,
        open_amount_txn,
        open_amount_base,
        currency_code,
        fx_rate,
        counterparty_code_snapshot,
        counterparty_name_snapshot,
        payment_term_snapshot,
        due_date_snapshot,
        currency_code_snapshot,
        fx_rate_snapshot
      )
      VALUES (
        ?, ?, ?, ?, 'AR', 'INVOICE', 'CARI_AR', ?, ?, ?, 'DRAFT', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
      )`,
    [
      tenantId,
      legalEntityId,
      counterpartyId,
      paymentTermId,
      fiscalYear,
      1,
      documentNo,
      documentDate,
      dueDate,
      100,
      100,
      100,
      100,
      currencyCode,
      1,
      counterpartyCode,
      `CARI Counterparty ${stamp}`,
      paymentTermCode,
      dueDate,
      currencyCode,
      1,
    ]
  );
  const documentId = toNumber(documentInsert.rows?.insertId);
  assert(documentId > 0, "Failed to create cari document fixture");

  const openItemInsert = await query(
    `INSERT INTO cari_open_items (
        tenant_id,
        legal_entity_id,
        counterparty_id,
        document_id,
        item_no,
        status,
        document_date,
        due_date,
        original_amount_txn,
        original_amount_base,
        residual_amount_txn,
        residual_amount_base,
        settled_amount_txn,
        settled_amount_base,
        currency_code
      )
      VALUES (?, ?, ?, ?, 1, 'OPEN', ?, ?, 100, 100, 100, 100, 0, 0, ?)`,
    [tenantId, legalEntityId, counterpartyId, documentId, documentDate, dueDate, currencyCode]
  );
  const openItemId = toNumber(openItemInsert.rows?.insertId);
  assert(openItemId > 0, "Failed to create open item fixture");

  const settlementInsert = await query(
    `INSERT INTO cari_settlement_batches (
        tenant_id,
        legal_entity_id,
        counterparty_id,
        sequence_namespace,
        fiscal_year,
        sequence_no,
        settlement_no,
        settlement_date,
        status,
        total_allocated_txn,
        total_allocated_base,
        currency_code
      )
      VALUES (?, ?, ?, 'CARI_SETTLE', ?, 1, ?, ?, 'DRAFT', 0, 0, ?)`,
    [tenantId, legalEntityId, counterpartyId, fiscalYear, `CARI-SETTLE-${stamp}-1`, dueDate, currencyCode]
  );
  const settlementBatchId = toNumber(settlementInsert.rows?.insertId);
  assert(settlementBatchId > 0, "Failed to create settlement batch fixture");

  return {
    stamp,
    fiscalYear,
    tenantId,
    legalEntityId,
    counterpartyId,
    paymentTermId,
    documentId,
    openItemId,
    settlementBatchId,
    currencyCode,
    counterpartyCode,
    documentDate,
    dueDate,
    paymentTermCode,
  };
}

async function runConstraintAssertions(fixture) {
  const {
    stamp,
    fiscalYear,
    tenantId,
    legalEntityId,
    counterpartyId,
    paymentTermId,
    documentId,
    openItemId,
    settlementBatchId,
    currencyCode,
    counterpartyCode,
    documentDate,
    dueDate,
    paymentTermCode,
  } = fixture;

  await expectDbError(
    () =>
      query(
        `INSERT INTO counterparties (
            tenant_id,
            legal_entity_id,
            code,
            name,
            is_customer,
            is_vendor,
            default_currency_code,
            default_payment_term_id,
            status
          )
          VALUES (?, ?, ?, ?, TRUE, FALSE, ?, ?, 'ACTIVE')`,
        [
          tenantId,
          legalEntityId,
          counterpartyCode,
          `Duplicate Counterparty ${stamp}`,
          currencyCode,
          paymentTermId,
        ]
      ),
    {
      label: "duplicate counterparty code in same tenant/legal_entity",
      errno: 1062,
    }
  );

  await expectDbError(
    () =>
      query(
        `INSERT INTO cari_documents (
            tenant_id,
            legal_entity_id,
            counterparty_id,
            payment_term_id,
            direction,
            document_type,
            sequence_namespace,
            fiscal_year,
            sequence_no,
            document_no,
            status,
            document_date,
            due_date,
            amount_txn,
            amount_base,
            open_amount_txn,
            open_amount_base,
            currency_code,
            fx_rate,
            counterparty_code_snapshot,
            counterparty_name_snapshot,
            payment_term_snapshot,
            due_date_snapshot,
            currency_code_snapshot,
            fx_rate_snapshot
          )
          VALUES (
            ?, ?, ?, ?, 'AR', 'INVOICE', 'CARI_AR', ?, 2, ?, 'DRAFT', ?, ?, 120, 120, 120, 120, ?, 1, ?, ?, ?, ?, ?, 1
          )`,
        [
          tenantId,
          legalEntityId,
          counterpartyId,
          paymentTermId,
          fiscalYear,
          `CARI-DOC-${stamp}-1`,
          documentDate,
          dueDate,
          currencyCode,
          counterpartyCode,
          `CARI Counterparty ${stamp}`,
          paymentTermCode,
          dueDate,
          currencyCode,
        ]
      ),
    {
      label: "duplicate cari document number in same numbering dimensions",
      errno: 1062,
    }
  );

  await expectDbError(
    () =>
      query(
        `INSERT INTO cari_settlement_batches (
            tenant_id,
            legal_entity_id,
            counterparty_id,
            sequence_namespace,
            fiscal_year,
            sequence_no,
            settlement_no,
            settlement_date,
            status,
            total_allocated_txn,
            total_allocated_base,
            currency_code
          )
          VALUES (?, ?, ?, 'CARI_SETTLE', ?, 1, ?, ?, 'DRAFT', 0, 0, ?)`,
        [
          tenantId,
          legalEntityId,
          counterpartyId,
          fiscalYear,
          `CARI-SETTLE-${stamp}-DUP`,
          dueDate,
          currencyCode,
        ]
      ),
    {
      label: "duplicate settlement sequence in same tenant/legal_entity namespace/year",
      errno: 1062,
    }
  );

  await expectDbError(
    () =>
      query(
        `INSERT INTO cari_open_items (
            tenant_id,
            legal_entity_id,
            counterparty_id,
            document_id,
            item_no,
            status,
            document_date,
            due_date,
            original_amount_txn,
            original_amount_base,
            residual_amount_txn,
            residual_amount_base,
            settled_amount_txn,
            settled_amount_base,
            currency_code
          )
          VALUES (?, ?, ?, ?, 99, 'OPEN', ?, ?, 100, 100, -1, 100, 0, 0, ?)`,
        [tenantId, legalEntityId, counterpartyId, documentId, documentDate, dueDate, currencyCode]
      ),
    {
      label: "negative residual/open amounts must fail",
      errno: 3819,
    }
  );

  await expectDbError(
    () =>
      query(
        `INSERT INTO cari_settlement_allocations (
            tenant_id,
            legal_entity_id,
            settlement_batch_id,
            open_item_id,
            allocation_date,
            allocation_amount_txn,
            allocation_amount_base
          )
          VALUES (?, ?, ?, ?, ?, 0, 0)`,
        [tenantId, legalEntityId, settlementBatchId, openItemId, dueDate]
      ),
    {
      label: "allocation <= 0 must fail",
      errno: 3819,
    }
  );

  const applyIdempotencyKey = `CARI-APPLY-IDEMP-${stamp}`;
  await query(
    `INSERT INTO cari_settlement_allocations (
        tenant_id,
        legal_entity_id,
        settlement_batch_id,
        open_item_id,
        allocation_date,
        allocation_amount_txn,
        allocation_amount_base,
        apply_idempotency_key
      )
      VALUES (?, ?, ?, ?, ?, 10, 10, ?)`,
    [tenantId, legalEntityId, settlementBatchId, openItemId, dueDate, applyIdempotencyKey]
  );

  await expectDbError(
    () =>
      query(
        `INSERT INTO cari_settlement_allocations (
            tenant_id,
            legal_entity_id,
            settlement_batch_id,
            open_item_id,
            allocation_date,
            allocation_amount_txn,
            allocation_amount_base,
            apply_idempotency_key
          )
          VALUES (?, ?, ?, ?, ?, 5, 5, ?)`,
        [tenantId, legalEntityId, settlementBatchId, openItemId, dueDate, applyIdempotencyKey]
      ),
    {
      label: "duplicate settlement apply idempotency key must fail",
      errno: 1062,
    }
  );

  const bankAttachIdempotencyKey = `CARI-BANK-ATTACH-${stamp}`;
  await query(
    `INSERT INTO cari_settlement_batches (
        tenant_id,
        legal_entity_id,
        counterparty_id,
        sequence_namespace,
        fiscal_year,
        sequence_no,
        settlement_no,
        settlement_date,
        status,
        total_allocated_txn,
        total_allocated_base,
        currency_code,
        bank_attach_idempotency_key
      )
      VALUES (?, ?, ?, 'CARI_SETTLE', ?, 10, ?, ?, 'DRAFT', 0, 0, ?, ?)`,
    [
      tenantId,
      legalEntityId,
      counterpartyId,
      fiscalYear,
      `CARI-SETTLE-${stamp}-10`,
      dueDate,
      currencyCode,
      bankAttachIdempotencyKey,
    ]
  );

  await expectDbError(
    () =>
      query(
        `INSERT INTO cari_settlement_batches (
            tenant_id,
            legal_entity_id,
            counterparty_id,
            sequence_namespace,
            fiscal_year,
            sequence_no,
            settlement_no,
            settlement_date,
            status,
            total_allocated_txn,
            total_allocated_base,
            currency_code,
            bank_attach_idempotency_key
          )
          VALUES (?, ?, ?, 'CARI_SETTLE', ?, 11, ?, ?, 'DRAFT', 0, 0, ?, ?)`,
        [
          tenantId,
          legalEntityId,
          counterpartyId,
          fiscalYear,
          `CARI-SETTLE-${stamp}-11`,
          dueDate,
          currencyCode,
          bankAttachIdempotencyKey,
        ]
      ),
    {
      label: "duplicate bank attach/apply idempotency key must fail",
      errno: 1062,
    }
  );
}

async function main() {
  await runMigrations();
  await assertSchemaExists();
  const fixture = await createFixture();
  await runConstraintAssertions(fixture);

  console.log("CARI PR-01 schema + DB constraint test passed.");
  console.log(
    JSON.stringify(
      {
        tenantId: fixture.tenantId,
        legalEntityId: fixture.legalEntityId,
        documentId: fixture.documentId,
        openItemId: fixture.openItemId,
        settlementBatchId: fixture.settlementBatchId,
      },
      null,
      2
    )
  );
}

main()
  .then(async () => {
    await closePool();
    process.exit(0);
  })
  .catch(async (err) => {
    console.error(err);
    try {
      await closePool();
    } catch {
      // ignore pool close failures
    }
    process.exit(1);
  });
