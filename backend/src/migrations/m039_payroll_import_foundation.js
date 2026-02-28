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

const migration039PayrollImportFoundation = {
  key: "m039_payroll_import_foundation",
  description: "Payroll import foundation (runs, lines, audit; tenant/legal-entity scoped)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_runs (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         run_no VARCHAR(60) NOT NULL,
         provider_code VARCHAR(60) NOT NULL,
         entity_code VARCHAR(60) NOT NULL,
         payroll_period DATE NOT NULL,
         pay_date DATE NOT NULL,
         currency_code CHAR(3) NOT NULL,
         source_batch_ref VARCHAR(120) NULL,
         original_filename VARCHAR(255) NOT NULL,
         file_checksum CHAR(64) NOT NULL,
         status ENUM('IMPORTED','REVIEWED','FINALIZED') NOT NULL DEFAULT 'IMPORTED',
         line_count_total INT UNSIGNED NOT NULL DEFAULT 0,
         line_count_inserted INT UNSIGNED NOT NULL DEFAULT 0,
         line_count_duplicates INT UNSIGNED NOT NULL DEFAULT 0,
         employee_count INT UNSIGNED NOT NULL DEFAULT 0,
         total_base_salary DECIMAL(20,6) NOT NULL DEFAULT 0,
         total_overtime_pay DECIMAL(20,6) NOT NULL DEFAULT 0,
         total_bonus_pay DECIMAL(20,6) NOT NULL DEFAULT 0,
         total_allowances DECIMAL(20,6) NOT NULL DEFAULT 0,
         total_gross_pay DECIMAL(20,6) NOT NULL DEFAULT 0,
         total_employee_tax DECIMAL(20,6) NOT NULL DEFAULT 0,
         total_employee_social_security DECIMAL(20,6) NOT NULL DEFAULT 0,
         total_other_deductions DECIMAL(20,6) NOT NULL DEFAULT 0,
         total_net_pay DECIMAL(20,6) NOT NULL DEFAULT 0,
         total_employer_tax DECIMAL(20,6) NOT NULL DEFAULT 0,
         total_employer_social_security DECIMAL(20,6) NOT NULL DEFAULT 0,
         raw_meta_json JSON NULL,
         imported_by_user_id INT NULL,
         imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
           ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_runs_tenant_entity_run_no (tenant_id, legal_entity_id, run_no),
         UNIQUE KEY uk_payroll_runs_tenant_entity_checksum (
           tenant_id, legal_entity_id, payroll_period, provider_code, file_checksum
         ),
         UNIQUE KEY uk_payroll_runs_tenant_entity_id (tenant_id, legal_entity_id, id),
         KEY ix_payroll_runs_scope_period (tenant_id, legal_entity_id, payroll_period),
         KEY ix_payroll_runs_scope_status (tenant_id, legal_entity_id, status),
         KEY ix_payroll_runs_scope_provider (tenant_id, legal_entity_id, provider_code),
         KEY ix_payroll_runs_scope_time (tenant_id, legal_entity_id, imported_at),
         CONSTRAINT fk_payroll_runs_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_runs_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_runs_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code),
         CONSTRAINT fk_payroll_runs_imported_user_tenant
           FOREIGN KEY (tenant_id, imported_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_run_lines (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         run_id BIGINT UNSIGNED NOT NULL,
         line_no INT UNSIGNED NOT NULL,
         employee_code VARCHAR(100) NOT NULL,
         employee_name VARCHAR(255) NOT NULL,
         cost_center_code VARCHAR(100) NULL,
         base_salary DECIMAL(20,6) NOT NULL DEFAULT 0,
         overtime_pay DECIMAL(20,6) NOT NULL DEFAULT 0,
         bonus_pay DECIMAL(20,6) NOT NULL DEFAULT 0,
         allowances_total DECIMAL(20,6) NOT NULL DEFAULT 0,
         gross_pay DECIMAL(20,6) NOT NULL DEFAULT 0,
         employee_tax DECIMAL(20,6) NOT NULL DEFAULT 0,
         employee_social_security DECIMAL(20,6) NOT NULL DEFAULT 0,
         other_deductions DECIMAL(20,6) NOT NULL DEFAULT 0,
         net_pay DECIMAL(20,6) NOT NULL DEFAULT 0,
         employer_tax DECIMAL(20,6) NOT NULL DEFAULT 0,
         employer_social_security DECIMAL(20,6) NOT NULL DEFAULT 0,
         line_hash CHAR(64) NOT NULL,
         raw_row_json JSON NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_run_lines_tenant_entity_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payroll_run_lines_tenant_run_line (tenant_id, legal_entity_id, run_id, line_no),
         UNIQUE KEY uk_payroll_run_lines_tenant_run_hash (tenant_id, legal_entity_id, run_id, line_hash),
         KEY ix_payroll_run_lines_scope_run (tenant_id, legal_entity_id, run_id),
         KEY ix_payroll_run_lines_employee (tenant_id, legal_entity_id, employee_code),
         KEY ix_payroll_run_lines_cost_center (tenant_id, legal_entity_id, cost_center_code),
         CONSTRAINT fk_payroll_run_lines_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_run_lines_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_run_lines_run
           FOREIGN KEY (tenant_id, legal_entity_id, run_id)
           REFERENCES payroll_runs(tenant_id, legal_entity_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_run_audit (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         run_id BIGINT UNSIGNED NOT NULL,
         action ENUM('IMPORTED','STATUS','VALIDATION') NOT NULL,
         payload_json JSON NULL,
         acted_by_user_id INT NULL,
         acted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_run_audit_tenant_entity_id (tenant_id, legal_entity_id, id),
         KEY ix_payroll_run_audit_scope_run (tenant_id, legal_entity_id, run_id),
         KEY ix_payroll_run_audit_scope_action (tenant_id, legal_entity_id, action),
         KEY ix_payroll_run_audit_scope_time (tenant_id, legal_entity_id, acted_at),
         CONSTRAINT fk_payroll_run_audit_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_run_audit_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_run_audit_run
           FOREIGN KEY (tenant_id, legal_entity_id, run_id)
           REFERENCES payroll_runs(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_run_audit_user_tenant
           FOREIGN KEY (tenant_id, acted_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },
};

export default migration039PayrollImportFoundation;
