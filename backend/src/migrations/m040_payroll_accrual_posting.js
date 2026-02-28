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

const migration040PayrollAccrualPosting = {
  key: "m040_payroll_accrual_posting",
  description:
    "Payroll accrual posting foundation (effective-dated component mappings + payroll run accrual metadata)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_component_gl_mappings (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         entity_code VARCHAR(60) NOT NULL,
         provider_code VARCHAR(60) NULL,
         currency_code CHAR(3) NOT NULL,
         component_code VARCHAR(80) NOT NULL,
         entry_side ENUM('DEBIT','CREDIT') NOT NULL,
         gl_account_id BIGINT UNSIGNED NOT NULL,
         effective_from DATE NOT NULL,
         effective_to DATE NULL,
         is_active BOOLEAN NOT NULL DEFAULT TRUE,
         notes VARCHAR(500) NULL,
         created_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_component_gl_mappings_scope_id (tenant_id, legal_entity_id, id),
         KEY ix_payroll_component_gl_mappings_lookup (
           tenant_id, legal_entity_id, component_code, currency_code, effective_from
         ),
         KEY ix_payroll_component_gl_mappings_provider (tenant_id, legal_entity_id, provider_code),
         KEY ix_payroll_component_gl_mappings_active (tenant_id, legal_entity_id, is_active),
         KEY ix_payroll_component_gl_mappings_gl (gl_account_id),
         CONSTRAINT fk_payroll_component_gl_mappings_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_component_gl_mappings_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_component_gl_mappings_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code),
         CONSTRAINT fk_payroll_component_gl_mappings_gl
           FOREIGN KEY (gl_account_id) REFERENCES accounts(id),
         CONSTRAINT fk_payroll_component_gl_mappings_user_tenant
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_component_gl_mapping_audit (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         mapping_id BIGINT UNSIGNED NULL,
         action VARCHAR(40) NOT NULL,
         payload_json JSON NULL,
         acted_by_user_id INT NULL,
         acted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_component_gl_mapping_audit_scope_id (tenant_id, legal_entity_id, id),
         KEY ix_payroll_component_gl_mapping_audit_scope_mapping (tenant_id, legal_entity_id, mapping_id),
         KEY ix_payroll_component_gl_mapping_audit_scope_action (tenant_id, legal_entity_id, action),
         KEY ix_payroll_component_gl_mapping_audit_scope_time (tenant_id, legal_entity_id, acted_at),
         CONSTRAINT fk_payroll_component_gl_mapping_audit_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_component_gl_mapping_audit_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_component_gl_mapping_audit_mapping
           FOREIGN KEY (mapping_id) REFERENCES payroll_component_gl_mappings(id),
         CONSTRAINT fk_payroll_component_gl_mapping_audit_user_tenant
           FOREIGN KEY (tenant_id, acted_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN reviewed_by_user_id INT NULL AFTER imported_by_user_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN reviewed_at TIMESTAMP NULL AFTER reviewed_by_user_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN finalized_by_user_id INT NULL AFTER reviewed_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN finalized_at TIMESTAMP NULL AFTER finalized_by_user_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN accrual_journal_entry_id BIGINT UNSIGNED NULL AFTER finalized_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN accrual_posted_by_user_id INT NULL AFTER accrual_journal_entry_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN accrual_posted_at TIMESTAMP NULL AFTER accrual_posted_by_user_id`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD KEY ix_payroll_runs_scope_status_reviewed (tenant_id, legal_entity_id, reviewed_at)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD KEY ix_payroll_runs_scope_status_finalized (tenant_id, legal_entity_id, finalized_at)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD KEY ix_payroll_runs_accrual_journal_entry (accrual_journal_entry_id)`
    );
  },
};

export default migration040PayrollAccrualPosting;
