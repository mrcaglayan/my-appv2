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
    if (ignorableErrnos.has(Number(err?.errno))) {
      return;
    }
    throw err;
  }
}

const migration056BankPaymentFileAcks = {
  key: "m056_bank_payment_file_acks",
  description: "Bank payment file exports/ack imports on top of generic payment batches (B06)",
  async up(connection) {
    await safeExecute(
      connection,
      `ALTER TABLE payment_batches
         ADD COLUMN bank_file_format_code VARCHAR(50) NULL AFTER status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batches
         ADD COLUMN bank_export_status VARCHAR(20) NOT NULL DEFAULT 'NOT_EXPORTED' AFTER bank_file_format_code`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batches
         ADD COLUMN bank_ack_status VARCHAR(20) NOT NULL DEFAULT 'NOT_ACKED' AFTER bank_export_status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batches
         ADD COLUMN last_ack_imported_at TIMESTAMP NULL AFTER exported_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batches
         ADD KEY ix_payment_batches_scope_bank_exec (tenant_id, legal_entity_id, bank_export_status, bank_ack_status)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN exported_amount DECIMAL(20,6) NOT NULL DEFAULT 0 AFTER amount`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN executed_amount DECIMAL(20,6) NOT NULL DEFAULT 0 AFTER exported_amount`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN bank_execution_status VARCHAR(30) NOT NULL DEFAULT 'NONE' AFTER executed_amount`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN bank_reference VARCHAR(190) NULL AFTER bank_execution_status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN ack_status VARCHAR(30) NULL AFTER bank_reference`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN ack_code VARCHAR(50) NULL AFTER ack_status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN ack_message VARCHAR(255) NULL AFTER ack_code`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN exported_at TIMESTAMP NULL AFTER ack_message`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN acknowledged_at TIMESTAMP NULL AFTER exported_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD KEY ix_payment_batch_lines_bank_exec (tenant_id, legal_entity_id, bank_execution_status)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD KEY ix_payment_batch_lines_ack_status (tenant_id, legal_entity_id, ack_status)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_exports
         ADD COLUMN export_request_id VARCHAR(190) NULL AFTER batch_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_exports
         ADD COLUMN bank_file_format_code VARCHAR(50) NULL AFTER export_format`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_exports
         MODIFY COLUMN export_status ENUM('GENERATED','SENT','FAILED') NOT NULL DEFAULT 'GENERATED'`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_exports
         ADD UNIQUE KEY uk_payment_batch_exports_scope_request_id (tenant_id, legal_entity_id, export_request_id)`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payment_batch_export_lines (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         payment_batch_export_id BIGINT UNSIGNED NOT NULL,
         payment_batch_line_id BIGINT UNSIGNED NOT NULL,
         line_ref VARCHAR(190) NOT NULL,
         beneficiary_name VARCHAR(255) NULL,
         beneficiary_account VARCHAR(190) NULL,
         amount DECIMAL(20,6) NOT NULL,
         currency_code CHAR(3) NOT NULL,
         reference_text VARCHAR(255) NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payment_batch_export_lines_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payment_batch_export_lines_export_lineref (tenant_id, legal_entity_id, payment_batch_export_id, line_ref),
         UNIQUE KEY uk_payment_batch_export_lines_export_line (tenant_id, legal_entity_id, payment_batch_export_id, payment_batch_line_id),
         KEY ix_payment_batch_export_lines_scope_export (tenant_id, legal_entity_id, payment_batch_export_id),
         KEY ix_payment_batch_export_lines_scope_line (tenant_id, legal_entity_id, payment_batch_line_id),
         CONSTRAINT fk_payment_batch_export_lines_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payment_batch_export_lines_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payment_batch_export_lines_export
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_export_id)
           REFERENCES payment_batch_exports(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payment_batch_export_lines_line
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_line_id)
           REFERENCES payment_batch_lines(tenant_id, legal_entity_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payment_batch_ack_imports (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         batch_id BIGINT UNSIGNED NOT NULL,
         payment_batch_export_id BIGINT UNSIGNED NULL,
         ack_request_id VARCHAR(190) NULL,
         file_format_code VARCHAR(50) NOT NULL,
         file_name VARCHAR(255) NULL,
         file_sha256 CHAR(64) NULL,
         status VARCHAR(20) NOT NULL DEFAULT 'APPLIED',
         total_rows INT NOT NULL DEFAULT 0,
         applied_rows INT NOT NULL DEFAULT 0,
         duplicate_rows INT NOT NULL DEFAULT 0,
         error_rows INT NOT NULL DEFAULT 0,
         payload_json JSON NULL,
         created_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payment_batch_ack_imports_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payment_batch_ack_imports_scope_request_id (tenant_id, legal_entity_id, ack_request_id),
         KEY ix_payment_batch_ack_imports_scope_batch (tenant_id, legal_entity_id, batch_id),
         KEY ix_payment_batch_ack_imports_scope_export (tenant_id, legal_entity_id, payment_batch_export_id),
         CONSTRAINT fk_payment_batch_ack_imports_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payment_batch_ack_imports_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payment_batch_ack_imports_batch
           FOREIGN KEY (tenant_id, legal_entity_id, batch_id)
           REFERENCES payment_batches(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payment_batch_ack_imports_export
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_export_id)
           REFERENCES payment_batch_exports(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payment_batch_ack_imports_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payment_batch_ack_import_lines (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         ack_import_id BIGINT UNSIGNED NOT NULL,
         payment_batch_line_id BIGINT UNSIGNED NULL,
         line_ref VARCHAR(190) NULL,
         bank_reference VARCHAR(190) NULL,
         ack_status VARCHAR(30) NOT NULL,
         ack_code VARCHAR(50) NULL,
         ack_message VARCHAR(255) NULL,
         ack_amount DECIMAL(20,6) NULL,
         currency_code CHAR(3) NULL,
         executed_at TIMESTAMP NULL,
         row_hash CHAR(64) NOT NULL,
         payload_json JSON NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payment_batch_ack_import_lines_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payment_batch_ack_import_lines_row_hash (tenant_id, legal_entity_id, ack_import_id, row_hash),
         KEY ix_payment_batch_ack_import_lines_scope_ack (tenant_id, legal_entity_id, ack_import_id),
         KEY ix_payment_batch_ack_import_lines_scope_line (tenant_id, legal_entity_id, payment_batch_line_id),
         CONSTRAINT fk_payment_batch_ack_import_lines_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payment_batch_ack_import_lines_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payment_batch_ack_import_lines_ack
           FOREIGN KEY (tenant_id, legal_entity_id, ack_import_id)
           REFERENCES payment_batch_ack_imports(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payment_batch_ack_import_lines_line
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_line_id)
           REFERENCES payment_batch_lines(tenant_id, legal_entity_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },

  async down(connection) {
    await connection.execute(`DROP TABLE IF EXISTS payment_batch_ack_import_lines`);
    await connection.execute(`DROP TABLE IF EXISTS payment_batch_ack_imports`);
    await connection.execute(`DROP TABLE IF EXISTS payment_batch_export_lines`);
    // Additive ALTER reversals intentionally omitted.
  },
};

export default migration056BankPaymentFileAcks;
