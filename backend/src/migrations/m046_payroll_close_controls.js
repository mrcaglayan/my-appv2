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

const migration046PayrollCloseControls = {
  key: "m046_payroll_close_controls",
  description: "Payroll period close controls, checklist, locks, and audit (PR-P08)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_period_closes (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         period_start DATE NOT NULL,
         period_end DATE NOT NULL,
         status ENUM('DRAFT','READY','REQUESTED','CLOSED','REOPENED') NOT NULL DEFAULT 'DRAFT',
         checklist_version INT UNSIGNED NOT NULL DEFAULT 1,
         total_checks INT UNSIGNED NOT NULL DEFAULT 0,
         passed_checks INT UNSIGNED NOT NULL DEFAULT 0,
         failed_checks INT UNSIGNED NOT NULL DEFAULT 0,
         warning_checks INT UNSIGNED NOT NULL DEFAULT 0,
         lock_run_changes TINYINT(1) NOT NULL DEFAULT 1,
         lock_manual_settlements TINYINT(1) NOT NULL DEFAULT 1,
         lock_payment_prep TINYINT(1) NOT NULL DEFAULT 0,
         prepare_note VARCHAR(500) NULL,
         request_note VARCHAR(500) NULL,
         close_note VARCHAR(500) NULL,
         reopen_reason VARCHAR(500) NULL,
         request_idempotency_key VARCHAR(190) NULL,
         close_idempotency_key VARCHAR(190) NULL,
         prepared_by_user_id INT NULL,
         prepared_at TIMESTAMP NULL,
         requested_by_user_id INT NULL,
         requested_at TIMESTAMP NULL,
         approved_by_user_id INT NULL,
         approved_at TIMESTAMP NULL,
         closed_by_user_id INT NULL,
         closed_at TIMESTAMP NULL,
         reopened_by_user_id INT NULL,
         reopened_at TIMESTAMP NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_period_closes_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payroll_period_closes_scope_period
           (tenant_id, legal_entity_id, period_start, period_end),
         UNIQUE KEY uk_payroll_period_closes_scope_request_idem
           (tenant_id, legal_entity_id, request_idempotency_key),
         UNIQUE KEY uk_payroll_period_closes_scope_close_idem
           (tenant_id, legal_entity_id, close_idempotency_key),
         KEY ix_payroll_period_closes_scope_status (tenant_id, legal_entity_id, status),
         KEY ix_payroll_period_closes_scope_period (tenant_id, legal_entity_id, period_start, period_end),
         CONSTRAINT fk_payroll_period_closes_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_period_closes_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_period_closes_prepared_user
           FOREIGN KEY (tenant_id, prepared_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payroll_period_closes_requested_user
           FOREIGN KEY (tenant_id, requested_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payroll_period_closes_approved_user
           FOREIGN KEY (tenant_id, approved_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payroll_period_closes_closed_user
           FOREIGN KEY (tenant_id, closed_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payroll_period_closes_reopened_user
           FOREIGN KEY (tenant_id, reopened_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_period_close_checks (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         payroll_period_close_id BIGINT UNSIGNED NOT NULL,
         check_code VARCHAR(80) NOT NULL,
         check_name VARCHAR(190) NOT NULL,
         severity ENUM('ERROR','WARN','INFO') NOT NULL DEFAULT 'ERROR',
         status ENUM('PASS','FAIL','WARN') NOT NULL DEFAULT 'FAIL',
         metric_value DECIMAL(20,6) NULL,
         metric_text VARCHAR(255) NULL,
         details_json JSON NULL,
         sort_order INT NOT NULL DEFAULT 100,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_period_close_checks_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payroll_period_close_checks_scope_code
           (tenant_id, legal_entity_id, payroll_period_close_id, check_code),
         KEY ix_payroll_period_close_checks_scope_close
           (tenant_id, legal_entity_id, payroll_period_close_id),
         KEY ix_payroll_period_close_checks_scope_status
           (tenant_id, legal_entity_id, status),
         CONSTRAINT fk_payroll_period_close_checks_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_period_close_checks_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_period_close_checks_close
           FOREIGN KEY (tenant_id, legal_entity_id, payroll_period_close_id)
           REFERENCES payroll_period_closes(tenant_id, legal_entity_id, id)
           ON DELETE CASCADE
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_period_close_audit (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         payroll_period_close_id BIGINT UNSIGNED NOT NULL,
         action ENUM('PREPARED','REQUESTED','CLOSED','REOPENED','STATUS') NOT NULL,
         action_status VARCHAR(20) NOT NULL DEFAULT 'CONFIRMED',
         note VARCHAR(500) NULL,
         payload_json JSON NULL,
         acted_by_user_id INT NULL,
         acted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_period_close_audit_scope_id (tenant_id, legal_entity_id, id),
         KEY ix_payroll_period_close_audit_scope_close
           (tenant_id, legal_entity_id, payroll_period_close_id),
         KEY ix_payroll_period_close_audit_scope_action
           (tenant_id, legal_entity_id, action),
         KEY ix_payroll_period_close_audit_scope_time
           (tenant_id, legal_entity_id, acted_at),
         CONSTRAINT fk_payroll_period_close_audit_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_period_close_audit_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_period_close_audit_close
           FOREIGN KEY (tenant_id, legal_entity_id, payroll_period_close_id)
           REFERENCES payroll_period_closes(tenant_id, legal_entity_id, id)
           ON DELETE CASCADE,
         CONSTRAINT fk_payroll_period_close_audit_user
           FOREIGN KEY (tenant_id, acted_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },

  async down(connection) {
    await safeExecute(connection, `DROP TABLE IF EXISTS payroll_period_close_audit`);
    await safeExecute(connection, `DROP TABLE IF EXISTS payroll_period_close_checks`);
    await safeExecute(connection, `DROP TABLE IF EXISTS payroll_period_closes`);
  },
};

export default migration046PayrollCloseControls;
