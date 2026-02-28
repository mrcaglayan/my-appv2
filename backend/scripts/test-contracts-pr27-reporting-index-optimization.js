import { closePool, query } from "../src/db.js";
import { runMigrations } from "../src/migrationRunner.js";
import { seedCore } from "../src/seedCore.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function getColumn(tableName, columnName) {
  const result = await query(
    `SELECT
        table_name,
        column_name,
        is_nullable,
        column_type
     FROM information_schema.columns
     WHERE table_schema = DATABASE()
       AND table_name = ?
       AND column_name = ?
     LIMIT 1`,
    [tableName, columnName]
  );
  return result.rows?.[0] || null;
}

async function getIndexColumns(tableName, indexName) {
  const result = await query(
    `SELECT
        index_name,
        seq_in_index,
        column_name
     FROM information_schema.statistics
     WHERE table_schema = DATABASE()
       AND table_name = ?
       AND index_name = ?
     ORDER BY seq_in_index ASC`,
    [tableName, indexName]
  );
  return (result.rows || []).map((row) =>
    String(row.column_name || row.COLUMN_NAME || "").toLowerCase()
  );
}

async function hasForeignKey(tableName, constraintName) {
  const result = await query(
    `SELECT 1
     FROM information_schema.referential_constraints
     WHERE constraint_schema = DATABASE()
       AND table_name = ?
       AND constraint_name = ?
     LIMIT 1`,
    [tableName, constraintName]
  );
  return Boolean(result.rows?.[0]);
}

async function main() {
  await runMigrations();
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const legalEntityColumn = await getColumn("contract_lines", "legal_entity_id");
  assert(legalEntityColumn, "contract_lines.legal_entity_id column is missing");
  const isNullable = String(
    legalEntityColumn.is_nullable || legalEntityColumn.IS_NULLABLE || ""
  ).toUpperCase();
  assert(
    isNullable === "NO",
    "contract_lines.legal_entity_id must be NOT NULL"
  );

  const uniqueTenantEntityColumns = await getIndexColumns(
    "contract_lines",
    "uk_contract_lines_tenant_entity_id"
  );
  assert(
    uniqueTenantEntityColumns.join(",") === "tenant_id,legal_entity_id,id",
    "uk_contract_lines_tenant_entity_id must be (tenant_id, legal_entity_id, id)"
  );

  const reportScopeColumns = await getIndexColumns(
    "contract_lines",
    "ix_contract_lines_report_scope"
  );
  assert(
    reportScopeColumns.join(",") ===
      "tenant_id,legal_entity_id,status,contract_id,line_no",
    "ix_contract_lines_report_scope must be (tenant_id, legal_entity_id, status, contract_id, line_no)"
  );

  const lineOrderColumns = await getIndexColumns(
    "contract_lines",
    "ix_contract_lines_contract_line_order"
  );
  assert(
    lineOrderColumns.join(",") ===
      "tenant_id,legal_entity_id,contract_id,line_no,id",
    "ix_contract_lines_contract_line_order must be (tenant_id, legal_entity_id, contract_id, line_no, id)"
  );

  assert(
    await hasForeignKey("contract_lines", "fk_contract_lines_contract_tenant_entity"),
    "fk_contract_lines_contract_tenant_entity is missing"
  );

  const nullCountResult = await query(
    `SELECT COUNT(*) AS row_count
     FROM contract_lines
     WHERE legal_entity_id IS NULL`
  );
  const nullCount = Number(
    nullCountResult.rows?.[0]?.row_count ??
      nullCountResult.rows?.[0]?.ROW_COUNT ??
      0
  );
  assert(nullCount === 0, "contract_lines.legal_entity_id contains NULL rows");

  const mismatchCountResult = await query(
    `SELECT COUNT(*) AS row_count
     FROM contract_lines cl
     JOIN contracts c
       ON c.tenant_id = cl.tenant_id
      AND c.id = cl.contract_id
     WHERE cl.legal_entity_id <> c.legal_entity_id`
  );
  const mismatchCount = Number(
    mismatchCountResult.rows?.[0]?.row_count ??
      mismatchCountResult.rows?.[0]?.ROW_COUNT ??
      0
  );
  assert(
    mismatchCount === 0,
    "contract_lines.legal_entity_id must match parent contracts.legal_entity_id"
  );

  console.log("Contracts PR-27 reporting/index optimization schema checks passed.");
  console.log(
    JSON.stringify(
      {
        checkedColumn: "contract_lines.legal_entity_id",
        checkedIndexes: [
          "uk_contract_lines_tenant_entity_id",
          "ix_contract_lines_report_scope",
          "ix_contract_lines_contract_line_order",
        ],
        checkedForeignKey: "fk_contract_lines_contract_tenant_entity",
      },
      null,
      2
    )
  );
}

main()
  .catch((err) => {
    console.error("Contracts PR-27 test failed:", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
