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

const migration059BankReconciliationAutopostTemplates = {
  key: "m059_bank_reconciliation_autopost_templates",
  description: "Bank reconciliation auto-posting templates and auto-post traceability (B08-A)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_reconciliation_posting_templates (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NULL,
         template_code VARCHAR(60) NOT NULL,
         template_name VARCHAR(190) NOT NULL,
         status ENUM('ACTIVE','PAUSED','DISABLED') NOT NULL DEFAULT 'ACTIVE',
         scope_type ENUM('GLOBAL','LEGAL_ENTITY','BANK_ACCOUNT') NOT NULL DEFAULT 'LEGAL_ENTITY',
         bank_account_id BIGINT UNSIGNED NULL,
         entry_kind VARCHAR(40) NOT NULL DEFAULT 'BANK_MISC',
         direction_policy ENUM('OUTFLOW_ONLY','INFLOW_ONLY','BOTH') NOT NULL DEFAULT 'BOTH',
         counter_account_id BIGINT UNSIGNED NOT NULL,
         tax_account_id BIGINT UNSIGNED NULL,
         tax_mode ENUM('NONE') NOT NULL DEFAULT 'NONE',
         tax_rate DECIMAL(9,4) NULL,
         currency_code CHAR(3) NULL,
         min_amount_abs DECIMAL(20,6) NULL,
         max_amount_abs DECIMAL(20,6) NULL,
         description_mode ENUM('USE_STATEMENT_TEXT','FIXED_TEXT','PREFIXED') NOT NULL DEFAULT 'USE_STATEMENT_TEXT',
         fixed_description VARCHAR(255) NULL,
         description_prefix VARCHAR(100) NULL,
         journal_source_code VARCHAR(30) NOT NULL DEFAULT 'BANK_AUTO_POST',
         journal_doc_type VARCHAR(30) NOT NULL DEFAULT 'BANK_AUTO',
         effective_from DATE NULL,
         effective_to DATE NULL,
         created_by_user_id INT NULL,
         updated_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_recon_post_tpl_scope_id (tenant_id, id),
         UNIQUE KEY uk_bank_recon_post_tpl_code (tenant_id, template_code),
         KEY ix_bank_recon_post_tpl_status (tenant_id, status),
         KEY ix_bank_recon_post_tpl_scope (tenant_id, scope_type, legal_entity_id, bank_account_id),
         KEY ix_bank_recon_post_tpl_effective (tenant_id, effective_from, effective_to),
         KEY ix_bank_recon_post_tpl_counter (tenant_id, legal_entity_id, counter_account_id),
         CONSTRAINT fk_bank_recon_post_tpl_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_recon_post_tpl_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_recon_post_tpl_bank_account
           FOREIGN KEY (tenant_id, legal_entity_id, bank_account_id)
           REFERENCES bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_recon_post_tpl_counter_account
           FOREIGN KEY (counter_account_id) REFERENCES accounts(id),
         CONSTRAINT fk_bank_recon_post_tpl_tax_account
           FOREIGN KEY (tax_account_id) REFERENCES accounts(id),
         CONSTRAINT fk_bank_recon_post_tpl_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code),
         CONSTRAINT fk_bank_recon_post_tpl_created_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_bank_recon_post_tpl_updated_user
           FOREIGN KEY (tenant_id, updated_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_reconciliation_auto_postings (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         statement_line_id BIGINT UNSIGNED NOT NULL,
         bank_reconciliation_posting_template_id BIGINT UNSIGNED NOT NULL,
         journal_entry_id BIGINT UNSIGNED NOT NULL,
         status ENUM('POSTED','REVERSED') NOT NULL DEFAULT 'POSTED',
         posted_amount DECIMAL(20,6) NOT NULL,
         currency_code CHAR(3) NOT NULL,
         reversal_journal_entry_id BIGINT UNSIGNED NULL,
         reversed_at TIMESTAMP NULL,
         reverse_reason VARCHAR(255) NULL,
         payload_json JSON NULL,
         created_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_recon_auto_post_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_bank_recon_auto_post_line (tenant_id, legal_entity_id, statement_line_id),
         KEY ix_bank_recon_auto_post_tpl (tenant_id, legal_entity_id, bank_reconciliation_posting_template_id),
         KEY ix_bank_recon_auto_post_journal (tenant_id, legal_entity_id, journal_entry_id),
         KEY ix_bank_recon_auto_post_status (tenant_id, legal_entity_id, status),
         CONSTRAINT fk_bank_recon_auto_post_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_recon_auto_post_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_recon_auto_post_statement_line
           FOREIGN KEY (tenant_id, legal_entity_id, statement_line_id)
           REFERENCES bank_statement_lines(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_recon_auto_post_template
           FOREIGN KEY (tenant_id, bank_reconciliation_posting_template_id)
           REFERENCES bank_reconciliation_posting_templates(tenant_id, id),
         CONSTRAINT fk_bank_recon_auto_post_journal
           FOREIGN KEY (journal_entry_id) REFERENCES journal_entries(id),
         CONSTRAINT fk_bank_recon_auto_post_reversal_journal
           FOREIGN KEY (reversal_journal_entry_id) REFERENCES journal_entries(id),
         CONSTRAINT fk_bank_recon_auto_post_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code),
         CONSTRAINT fk_bank_recon_auto_post_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD COLUMN auto_post_template_id BIGINT UNSIGNED NULL AFTER reconciliation_confidence`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD COLUMN auto_post_journal_entry_id BIGINT UNSIGNED NULL AFTER auto_post_template_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD KEY ix_bank_stmt_lines_auto_post_template (tenant_id, legal_entity_id, auto_post_template_id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD KEY ix_bank_stmt_lines_auto_post_journal (tenant_id, legal_entity_id, auto_post_journal_entry_id)`
    );
  },

  async down(connection) {
    await connection.execute(`DROP TABLE IF EXISTS bank_reconciliation_auto_postings`);
    await connection.execute(`DROP TABLE IF EXISTS bank_reconciliation_posting_templates`);
    // Additive ALTER reversals intentionally omitted for safety.
  },
};

export default migration059BankReconciliationAutopostTemplates;
