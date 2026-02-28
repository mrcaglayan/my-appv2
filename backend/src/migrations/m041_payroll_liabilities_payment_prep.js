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

const migration041PayrollLiabilitiesPaymentPrep = {
  key: "m041_payroll_liabilities_payment_prep",
  description:
    "Payroll liabilities subledger and payment batch preparation links (PR-P03)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_run_liabilities (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         run_id BIGINT UNSIGNED NOT NULL,
         liability_key VARCHAR(180) NOT NULL,
         liability_type VARCHAR(50) NOT NULL,
         liability_group VARCHAR(30) NOT NULL,
         source_run_line_id BIGINT UNSIGNED NULL,
         employee_code VARCHAR(100) NULL,
         employee_name VARCHAR(255) NULL,
         cost_center_code VARCHAR(100) NULL,
         beneficiary_type VARCHAR(30) NOT NULL,
         beneficiary_id BIGINT UNSIGNED NULL,
         beneficiary_name VARCHAR(255) NOT NULL,
         beneficiary_bank_ref VARCHAR(255) NULL,
         payable_component_code VARCHAR(80) NOT NULL,
         payable_gl_account_id BIGINT UNSIGNED NOT NULL,
         payable_ref VARCHAR(120) NULL,
         amount DECIMAL(20,6) NOT NULL,
         currency_code CHAR(3) NOT NULL,
         status ENUM('OPEN','IN_BATCH','PAID','CANCELLED') NOT NULL DEFAULT 'OPEN',
         reserved_payment_batch_id BIGINT UNSIGNED NULL,
         paid_at TIMESTAMP NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
           ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_run_liabilities_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payroll_run_liabilities_scope_key (tenant_id, legal_entity_id, liability_key),
         KEY ix_payroll_run_liabilities_scope_run (tenant_id, legal_entity_id, run_id),
         KEY ix_payroll_run_liabilities_scope_status (tenant_id, legal_entity_id, status),
         KEY ix_payroll_run_liabilities_scope_type (tenant_id, legal_entity_id, liability_type),
         KEY ix_payroll_run_liabilities_scope_group (tenant_id, legal_entity_id, liability_group),
         KEY ix_payroll_run_liabilities_reserved_batch (tenant_id, legal_entity_id, reserved_payment_batch_id),
         KEY ix_payroll_run_liabilities_source_line (tenant_id, legal_entity_id, source_run_line_id),
         CONSTRAINT fk_payroll_run_liabilities_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_run_liabilities_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_run_liabilities_run
           FOREIGN KEY (tenant_id, legal_entity_id, run_id)
           REFERENCES payroll_runs(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_run_liabilities_run_line
           FOREIGN KEY (tenant_id, legal_entity_id, source_run_line_id)
           REFERENCES payroll_run_lines(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_run_liabilities_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code),
         CONSTRAINT fk_payroll_run_liabilities_gl
           FOREIGN KEY (payable_gl_account_id) REFERENCES accounts(id),
         CONSTRAINT fk_payroll_run_liabilities_reserved_batch
           FOREIGN KEY (tenant_id, legal_entity_id, reserved_payment_batch_id)
           REFERENCES payment_batches(tenant_id, legal_entity_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_liability_payment_links (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         run_id BIGINT UNSIGNED NOT NULL,
         payroll_liability_id BIGINT UNSIGNED NOT NULL,
         payment_batch_id BIGINT UNSIGNED NOT NULL,
         payment_batch_line_id BIGINT UNSIGNED NULL,
         allocated_amount DECIMAL(20,6) NOT NULL,
         status ENUM('LINKED','PAID','RELEASED') NOT NULL DEFAULT 'LINKED',
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
           ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_liability_payment_links_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payroll_liability_payment_links_liability_batch (
           tenant_id, legal_entity_id, payroll_liability_id, payment_batch_id
         ),
         KEY ix_payroll_liability_payment_links_scope_run (tenant_id, legal_entity_id, run_id),
         KEY ix_payroll_liability_payment_links_scope_batch (tenant_id, legal_entity_id, payment_batch_id),
         KEY ix_payroll_liability_payment_links_scope_batch_line (tenant_id, legal_entity_id, payment_batch_line_id),
         CONSTRAINT fk_payroll_liability_payment_links_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_liability_payment_links_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_liability_payment_links_run
           FOREIGN KEY (tenant_id, legal_entity_id, run_id)
           REFERENCES payroll_runs(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_payment_links_liability
           FOREIGN KEY (tenant_id, legal_entity_id, payroll_liability_id)
           REFERENCES payroll_run_liabilities(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_payment_links_batch
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_id)
           REFERENCES payment_batches(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_payment_links_batch_line
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_line_id)
           REFERENCES payment_batch_lines(tenant_id, legal_entity_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_liability_audit (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         run_id BIGINT UNSIGNED NOT NULL,
         payroll_liability_id BIGINT UNSIGNED NULL,
         action VARCHAR(40) NOT NULL,
         payload_json JSON NULL,
         acted_by_user_id INT NULL,
         acted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_liability_audit_scope_id (tenant_id, legal_entity_id, id),
         KEY ix_payroll_liability_audit_scope_run (tenant_id, legal_entity_id, run_id),
         KEY ix_payroll_liability_audit_scope_liability (tenant_id, legal_entity_id, payroll_liability_id),
         KEY ix_payroll_liability_audit_scope_action (tenant_id, legal_entity_id, action),
         KEY ix_payroll_liability_audit_scope_time (tenant_id, legal_entity_id, acted_at),
         CONSTRAINT fk_payroll_liability_audit_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_liability_audit_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_liability_audit_run
           FOREIGN KEY (tenant_id, legal_entity_id, run_id)
           REFERENCES payroll_runs(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_audit_liability
           FOREIGN KEY (tenant_id, legal_entity_id, payroll_liability_id)
           REFERENCES payroll_run_liabilities(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_audit_user_tenant
           FOREIGN KEY (tenant_id, acted_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN liabilities_built_by_user_id INT NULL AFTER accrual_posted_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN liabilities_built_at TIMESTAMP NULL AFTER liabilities_built_by_user_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD KEY ix_payroll_runs_liabilities_built_at (tenant_id, legal_entity_id, liabilities_built_at)`
    );
  },
};

export default migration041PayrollLiabilitiesPaymentPrep;

