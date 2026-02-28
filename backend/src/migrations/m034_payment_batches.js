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

const migration034PaymentBatches = {
  key: "m034_payment_batches",
  description: "Generic payment batch engine foundation (batches, lines, exports, audit)",
  async up(connection) {
    // m032 adds this key, but guard here so PR-B04 stays resilient if DB state is older than expected.
    if (!(await hasIndex(connection, "bank_accounts", "uk_bank_accounts_tenant_entity_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE bank_accounts
         ADD UNIQUE KEY uk_bank_accounts_tenant_entity_id (tenant_id, legal_entity_id, id)`
      );
    }

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payment_batches (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         batch_no VARCHAR(60) NOT NULL,
         source_type ENUM('PAYROLL','AP','TAX','MANUAL') NOT NULL,
         source_id BIGINT UNSIGNED NULL,
         bank_account_id BIGINT UNSIGNED NOT NULL,
         currency_code CHAR(3) NOT NULL,
         total_amount DECIMAL(20,6) NOT NULL DEFAULT 0,
         status ENUM('DRAFT','APPROVED','EXPORTED','POSTED','FAILED','CANCELLED')
           NOT NULL DEFAULT 'DRAFT',
         idempotency_key VARCHAR(120) NULL,
         last_export_file_name VARCHAR(255) NULL,
         last_export_checksum CHAR(64) NULL,
         posted_journal_entry_id BIGINT UNSIGNED NULL,
         notes VARCHAR(500) NULL,
         created_by_user_id INT NOT NULL,
         approved_by_user_id INT NULL,
         exported_by_user_id INT NULL,
         posted_by_user_id INT NULL,
         cancelled_by_user_id INT NULL,
         approved_at TIMESTAMP NULL,
         exported_at TIMESTAMP NULL,
         posted_at TIMESTAMP NULL,
         cancelled_at TIMESTAMP NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
           ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payment_batches_tenant_entity_batch_no (tenant_id, legal_entity_id, batch_no),
         UNIQUE KEY uk_payment_batches_tenant_entity_idempotency (tenant_id, legal_entity_id, idempotency_key),
         UNIQUE KEY uk_payment_batches_tenant_entity_id (tenant_id, legal_entity_id, id),
         KEY ix_payment_batches_scope_status (tenant_id, legal_entity_id, status),
         KEY ix_payment_batches_scope_source (tenant_id, legal_entity_id, source_type, source_id),
         KEY ix_payment_batches_scope_bank (tenant_id, legal_entity_id, bank_account_id),
         KEY ix_payment_batches_posted_journal (posted_journal_entry_id),
         CONSTRAINT fk_payment_batches_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payment_batches_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payment_batches_bank_account
           FOREIGN KEY (tenant_id, legal_entity_id, bank_account_id)
           REFERENCES bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payment_batches_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code),
         CONSTRAINT fk_payment_batches_posted_journal
           FOREIGN KEY (posted_journal_entry_id) REFERENCES journal_entries(id),
         CONSTRAINT fk_payment_batches_created_user_tenant
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payment_batches_approved_user_tenant
           FOREIGN KEY (tenant_id, approved_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payment_batches_exported_user_tenant
           FOREIGN KEY (tenant_id, exported_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payment_batches_posted_user_tenant
           FOREIGN KEY (tenant_id, posted_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payment_batches_cancelled_user_tenant
           FOREIGN KEY (tenant_id, cancelled_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payment_batch_lines (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         batch_id BIGINT UNSIGNED NOT NULL,
         line_no INT UNSIGNED NOT NULL,
         beneficiary_type VARCHAR(30) NOT NULL,
         beneficiary_id BIGINT UNSIGNED NULL,
         beneficiary_name VARCHAR(255) NOT NULL,
         beneficiary_bank_ref VARCHAR(255) NULL,
         payable_entity_type VARCHAR(40) NOT NULL,
         payable_entity_id BIGINT UNSIGNED NULL,
         payable_gl_account_id BIGINT UNSIGNED NOT NULL,
         payable_ref VARCHAR(120) NULL,
         amount DECIMAL(20,6) NOT NULL,
         status ENUM('PENDING','PAID','FAILED','CANCELLED') NOT NULL DEFAULT 'PENDING',
         external_payment_ref VARCHAR(120) NULL,
         settlement_journal_line_ref VARCHAR(120) NULL,
         notes VARCHAR(500) NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
           ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payment_batch_lines_tenant_entity_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payment_batch_lines_batch_line (tenant_id, legal_entity_id, batch_id, line_no),
         KEY ix_payment_batch_lines_scope_batch (tenant_id, legal_entity_id, batch_id),
         KEY ix_payment_batch_lines_scope_payable (tenant_id, legal_entity_id, payable_entity_type, payable_entity_id),
         KEY ix_payment_batch_lines_scope_status (tenant_id, legal_entity_id, status),
         CONSTRAINT fk_payment_batch_lines_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payment_batch_lines_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payment_batch_lines_batch
           FOREIGN KEY (tenant_id, legal_entity_id, batch_id)
           REFERENCES payment_batches(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payment_batch_lines_payable_gl
           FOREIGN KEY (payable_gl_account_id) REFERENCES accounts(id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payment_batch_exports (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         batch_id BIGINT UNSIGNED NOT NULL,
         export_format ENUM('CSV') NOT NULL DEFAULT 'CSV',
         export_status ENUM('GENERATED','FAILED') NOT NULL DEFAULT 'GENERATED',
         file_name VARCHAR(255) NOT NULL,
         file_checksum CHAR(64) NOT NULL,
         export_payload_text MEDIUMTEXT NULL,
         raw_meta_json JSON NULL,
         exported_by_user_id INT NULL,
         exported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payment_batch_exports_tenant_entity_id (tenant_id, legal_entity_id, id),
         KEY ix_payment_batch_exports_scope_batch (tenant_id, legal_entity_id, batch_id),
         KEY ix_payment_batch_exports_scope_checksum (tenant_id, legal_entity_id, file_checksum),
         KEY ix_payment_batch_exports_scope_time (tenant_id, legal_entity_id, exported_at),
         CONSTRAINT fk_payment_batch_exports_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payment_batch_exports_batch
           FOREIGN KEY (tenant_id, legal_entity_id, batch_id)
           REFERENCES payment_batches(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payment_batch_exports_user_tenant
           FOREIGN KEY (tenant_id, exported_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payment_batch_audit (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         batch_id BIGINT UNSIGNED NOT NULL,
         action ENUM('CREATED','UPDATED','APPROVED','EXPORTED','POSTED','CANCELLED','FAILED','STATUS')
           NOT NULL,
         payload_json JSON NULL,
         acted_by_user_id INT NULL,
         acted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payment_batch_audit_tenant_entity_id (tenant_id, legal_entity_id, id),
         KEY ix_payment_batch_audit_scope_batch (tenant_id, legal_entity_id, batch_id),
         KEY ix_payment_batch_audit_scope_action (tenant_id, legal_entity_id, action),
         KEY ix_payment_batch_audit_scope_time (tenant_id, legal_entity_id, acted_at),
         CONSTRAINT fk_payment_batch_audit_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payment_batch_audit_batch
           FOREIGN KEY (tenant_id, legal_entity_id, batch_id)
           REFERENCES payment_batches(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payment_batch_audit_user_tenant
           FOREIGN KEY (tenant_id, acted_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },
};

export default migration034PaymentBatches;
