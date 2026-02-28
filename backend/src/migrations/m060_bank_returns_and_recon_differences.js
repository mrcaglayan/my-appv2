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

const migration060BankReturnsAndReconDifferences = {
  key: "m060_bank_returns_and_recon_differences",
  description: "Bank returns/rejections event tracking and reconciliation difference handling (B08-B)",
  async up(connection) {
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN return_status VARCHAR(30) NULL AFTER ack_status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN returned_amount DECIMAL(20,6) NOT NULL DEFAULT 0 AFTER executed_amount`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN return_reason_code VARCHAR(50) NULL AFTER return_status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD COLUMN last_returned_at TIMESTAMP NULL AFTER acknowledged_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD KEY ix_payment_batch_lines_return_status (tenant_id, legal_entity_id, return_status)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD COLUMN reconciliation_difference_type VARCHAR(20) NULL AFTER auto_post_journal_entry_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD COLUMN reconciliation_difference_amount DECIMAL(20,6) NULL AFTER reconciliation_difference_type`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD COLUMN reconciliation_difference_profile_id BIGINT UNSIGNED NULL AFTER reconciliation_difference_amount`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD COLUMN reconciliation_difference_journal_entry_id BIGINT UNSIGNED NULL AFTER reconciliation_difference_profile_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD KEY ix_bank_statement_lines_diff_journal (reconciliation_difference_journal_entry_id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD KEY ix_bank_statement_lines_diff_profile (reconciliation_difference_profile_id)`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_payment_return_events (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         event_request_id VARCHAR(190) NULL,
         source_type VARCHAR(20) NOT NULL,
         source_ref VARCHAR(190) NULL,
         payment_batch_id BIGINT UNSIGNED NOT NULL,
         payment_batch_line_id BIGINT UNSIGNED NOT NULL,
         bank_statement_line_id BIGINT UNSIGNED NULL,
         payment_batch_ack_import_id BIGINT UNSIGNED NULL,
         payment_batch_ack_import_line_id BIGINT UNSIGNED NULL,
         event_type VARCHAR(30) NOT NULL,
         event_status VARCHAR(20) NOT NULL DEFAULT 'CONFIRMED',
         amount DECIMAL(20,6) NOT NULL,
         currency_code CHAR(3) NOT NULL,
         bank_reference VARCHAR(190) NULL,
         reason_code VARCHAR(50) NULL,
         reason_message VARCHAR(255) NULL,
         payload_json JSON NULL,
         created_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_payment_return_events_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_bank_payment_return_events_request (tenant_id, legal_entity_id, event_request_id),
         KEY ix_bank_payment_return_events_line (tenant_id, legal_entity_id, payment_batch_line_id),
         KEY ix_bank_payment_return_events_batch (tenant_id, legal_entity_id, payment_batch_id),
         KEY ix_bank_payment_return_events_statement (tenant_id, legal_entity_id, bank_statement_line_id),
         KEY ix_bank_payment_return_events_ack_line (tenant_id, legal_entity_id, payment_batch_ack_import_line_id),
         KEY ix_bank_payment_return_events_type_status (tenant_id, legal_entity_id, event_type, event_status),
         CONSTRAINT fk_bpre_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bpre_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bpre_payment_batch
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_id)
           REFERENCES payment_batches(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bpre_payment_batch_line
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_line_id)
           REFERENCES payment_batch_lines(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bpre_statement_line
           FOREIGN KEY (tenant_id, legal_entity_id, bank_statement_line_id)
           REFERENCES bank_statement_lines(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bpre_ack_import
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_ack_import_id)
           REFERENCES payment_batch_ack_imports(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bpre_ack_import_line
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_ack_import_line_id)
           REFERENCES payment_batch_ack_import_lines(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bpre_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_reconciliation_difference_profiles (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         profile_code VARCHAR(60) NOT NULL,
         profile_name VARCHAR(190) NOT NULL,
         status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
         scope_type VARCHAR(20) NOT NULL DEFAULT 'LEGAL_ENTITY',
         bank_account_id BIGINT UNSIGNED NULL,
         difference_type VARCHAR(20) NOT NULL,
         direction_policy VARCHAR(20) NOT NULL DEFAULT 'BOTH',
         tolerance_mode VARCHAR(20) NOT NULL DEFAULT 'ABSOLUTE',
         max_abs_difference DECIMAL(20,6) NOT NULL DEFAULT 0,
         expense_account_id BIGINT UNSIGNED NULL,
         fx_gain_account_id BIGINT UNSIGNED NULL,
         fx_loss_account_id BIGINT UNSIGNED NULL,
         currency_code CHAR(3) NULL,
         description_prefix VARCHAR(100) NULL,
         effective_from DATE NULL,
         effective_to DATE NULL,
         created_by_user_id INT NULL,
         updated_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_brdp_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_brdp_code (tenant_id, legal_entity_id, profile_code),
         KEY ix_brdp_status (tenant_id, legal_entity_id, status),
         KEY ix_brdp_scope (tenant_id, legal_entity_id, scope_type, bank_account_id),
         KEY ix_brdp_type (tenant_id, legal_entity_id, difference_type),
         CONSTRAINT fk_brdp_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_brdp_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_brdp_bank_account
           FOREIGN KEY (tenant_id, legal_entity_id, bank_account_id)
           REFERENCES bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_brdp_expense_account
           FOREIGN KEY (expense_account_id) REFERENCES accounts(id),
         CONSTRAINT fk_brdp_fx_gain_account
           FOREIGN KEY (fx_gain_account_id) REFERENCES accounts(id),
         CONSTRAINT fk_brdp_fx_loss_account
           FOREIGN KEY (fx_loss_account_id) REFERENCES accounts(id),
         CONSTRAINT fk_brdp_created_by
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_brdp_updated_by
           FOREIGN KEY (tenant_id, updated_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_reconciliation_difference_adjustments (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         bank_statement_line_id BIGINT UNSIGNED NOT NULL,
         payment_batch_id BIGINT UNSIGNED NOT NULL,
         payment_batch_line_id BIGINT UNSIGNED NOT NULL,
         difference_profile_id BIGINT UNSIGNED NOT NULL,
         difference_type VARCHAR(20) NOT NULL,
         difference_amount DECIMAL(20,6) NOT NULL,
         currency_code CHAR(3) NOT NULL,
         journal_entry_id BIGINT UNSIGNED NOT NULL,
         status VARCHAR(20) NOT NULL DEFAULT 'POSTED',
         payload_json JSON NULL,
         created_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_brda_scope_statement_line (tenant_id, legal_entity_id, bank_statement_line_id),
         KEY ix_brda_scope_payment_line (tenant_id, legal_entity_id, payment_batch_line_id),
         KEY ix_brda_scope_profile (tenant_id, legal_entity_id, difference_profile_id),
         KEY ix_brda_scope_journal (tenant_id, legal_entity_id, journal_entry_id),
         CONSTRAINT fk_brda_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_brda_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_brda_statement_line
           FOREIGN KEY (tenant_id, legal_entity_id, bank_statement_line_id)
           REFERENCES bank_statement_lines(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_brda_payment_batch
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_id)
           REFERENCES payment_batches(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_brda_payment_batch_line
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_line_id)
           REFERENCES payment_batch_lines(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_brda_profile
           FOREIGN KEY (tenant_id, legal_entity_id, difference_profile_id)
           REFERENCES bank_reconciliation_difference_profiles(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_brda_journal
           FOREIGN KEY (journal_entry_id) REFERENCES journal_entries(id),
         CONSTRAINT fk_brda_created_by
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },

  async down(connection) {
    await connection.execute(`DROP TABLE IF EXISTS bank_reconciliation_difference_adjustments`);
    await connection.execute(`DROP TABLE IF EXISTS bank_reconciliation_difference_profiles`);
    await connection.execute(`DROP TABLE IF EXISTS bank_payment_return_events`);
    // Additive ALTER reversals intentionally omitted.
  },
};

export default migration060BankReturnsAndReconDifferences;
