const ignorableErrnos = new Set([
  1050, // ER_TABLE_EXISTS_ERROR
  1060, // ER_DUP_FIELDNAME
  1061, // ER_DUP_KEYNAME
  1091, // ER_CANT_DROP_FIELD_OR_KEY
  1826, // ER_FK_DUP_NAME
]);

async function safeExecute(connection, sql, params = []) {
  try {
    await connection.execute(sql, params);
  } catch (err) {
    if (ignorableErrnos.has(err?.errno)) {
      return;
    }
    throw err;
  }
}

async function hasIndex(connection, tableName, indexName) {
  const [rows] = await connection.execute(
    `SELECT 1
     FROM information_schema.statistics
     WHERE table_schema = DATABASE()
       AND table_name = ?
       AND index_name = ?
     LIMIT 1`,
    [tableName, indexName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

const migration032BankStatementImports = {
  key: "m032_bank_statement_imports",
  description: "Bank statement imports and normalized statement line queue foundation",
  async up(connection) {
    // m031 introduced bank_accounts, but add a tenant-safe composite key for downstream composite FKs.
    if (!(await hasIndex(connection, "bank_accounts", "uk_bank_accounts_tenant_entity_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE bank_accounts
         ADD UNIQUE KEY uk_bank_accounts_tenant_entity_id (tenant_id, legal_entity_id, id)`
      );
    }

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_statement_imports (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         bank_account_id BIGINT UNSIGNED NOT NULL,
         import_source ENUM('CSV','API','MANUAL') NOT NULL DEFAULT 'CSV',
         original_filename VARCHAR(255) NOT NULL,
         file_checksum CHAR(64) NOT NULL,
         period_start DATE NULL,
         period_end DATE NULL,
         status ENUM('IMPORTED','FAILED') NOT NULL DEFAULT 'IMPORTED',
         line_count_total INT UNSIGNED NOT NULL DEFAULT 0,
         line_count_inserted INT UNSIGNED NOT NULL DEFAULT 0,
         line_count_duplicates INT UNSIGNED NOT NULL DEFAULT 0,
         raw_meta_json JSON NULL,
         imported_by_user_id INT NULL,
         imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
           ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_stmt_imports_checksum (tenant_id, bank_account_id, file_checksum),
         UNIQUE KEY uk_bank_stmt_imports_tenant_entity_id (tenant_id, legal_entity_id, id),
         KEY ix_bank_stmt_imports_scope_time (tenant_id, legal_entity_id, imported_at),
         KEY ix_bank_stmt_imports_bank (tenant_id, legal_entity_id, bank_account_id),
         KEY ix_bank_stmt_imports_status (tenant_id, legal_entity_id, status),
         CONSTRAINT fk_bank_stmt_imports_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_stmt_imports_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_stmt_imports_bank_account
           FOREIGN KEY (tenant_id, legal_entity_id, bank_account_id)
           REFERENCES bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_stmt_imports_user_tenant
           FOREIGN KEY (tenant_id, imported_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_statement_lines (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         import_id BIGINT UNSIGNED NOT NULL,
         bank_account_id BIGINT UNSIGNED NOT NULL,
         line_no INT UNSIGNED NOT NULL,
         txn_date DATE NOT NULL,
         value_date DATE NULL,
         description VARCHAR(500) NOT NULL,
         reference_no VARCHAR(255) NULL,
         amount DECIMAL(20,6) NOT NULL,
         currency_code CHAR(3) NOT NULL,
         balance_after DECIMAL(20,6) NULL,
         line_hash CHAR(64) NOT NULL,
         recon_status ENUM('UNMATCHED','MATCHED','IGNORED') NOT NULL DEFAULT 'UNMATCHED',
         raw_row_json JSON NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_stmt_lines_hash (tenant_id, bank_account_id, line_hash),
         UNIQUE KEY uk_bank_stmt_lines_import_lineno (tenant_id, import_id, line_no),
         UNIQUE KEY uk_bank_stmt_lines_tenant_entity_id (tenant_id, legal_entity_id, id),
         KEY ix_bank_stmt_lines_scope_recon (tenant_id, legal_entity_id, recon_status),
         KEY ix_bank_stmt_lines_scope_txn_date (tenant_id, legal_entity_id, txn_date),
         KEY ix_bank_stmt_lines_import (tenant_id, legal_entity_id, import_id),
         KEY ix_bank_stmt_lines_bank (tenant_id, legal_entity_id, bank_account_id),
         CONSTRAINT fk_bank_stmt_lines_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_stmt_lines_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_stmt_lines_import
           FOREIGN KEY (tenant_id, legal_entity_id, import_id)
           REFERENCES bank_statement_imports(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_stmt_lines_bank_account
           FOREIGN KEY (tenant_id, legal_entity_id, bank_account_id)
           REFERENCES bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_stmt_lines_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },
};

export default migration032BankStatementImports;
