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

const migration057BankReconciliationRulesAndExceptions = {
  key: "m057_bank_reconciliation_rules_and_exceptions",
  description: "Bank reconciliation rules engine and exception queue (B07)",
  async up(connection) {
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD COLUMN reconciliation_method VARCHAR(20) NULL AFTER recon_status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD COLUMN reconciliation_rule_id BIGINT UNSIGNED NULL AFTER reconciliation_method`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD COLUMN reconciliation_confidence DECIMAL(5,2) NULL AFTER reconciliation_rule_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD KEY ix_bank_stmt_lines_recon_rule (tenant_id, legal_entity_id, reconciliation_rule_id)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_matches
         ADD COLUMN reconciliation_rule_id BIGINT UNSIGNED NULL AFTER matched_entity_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_matches
         ADD COLUMN reconciliation_confidence DECIMAL(5,2) NULL AFTER reconciliation_rule_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_matches
         ADD KEY ix_bank_recon_match_rule (tenant_id, legal_entity_id, reconciliation_rule_id)`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_reconciliation_rules (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NULL,
         rule_code VARCHAR(60) NOT NULL,
         rule_name VARCHAR(190) NOT NULL,
         status ENUM('ACTIVE','PAUSED','DISABLED') NOT NULL DEFAULT 'ACTIVE',
         priority INT NOT NULL DEFAULT 100,
         scope_type ENUM('GLOBAL','LEGAL_ENTITY','BANK_ACCOUNT') NOT NULL DEFAULT 'GLOBAL',
         bank_account_id BIGINT UNSIGNED NULL,
         match_type VARCHAR(40) NOT NULL,
         conditions_json JSON NOT NULL,
         action_type VARCHAR(40) NOT NULL,
         action_payload_json JSON NULL,
         stop_on_match TINYINT(1) NOT NULL DEFAULT 1,
         effective_from DATE NULL,
         effective_to DATE NULL,
         created_by_user_id INT NULL,
         updated_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_recon_rules_scope_id (tenant_id, id),
         UNIQUE KEY uk_bank_recon_rules_code (tenant_id, rule_code),
         KEY ix_bank_recon_rules_status_priority (tenant_id, status, priority, id),
         KEY ix_bank_recon_rules_scope (tenant_id, scope_type, legal_entity_id, bank_account_id),
         KEY ix_bank_recon_rules_effective (tenant_id, effective_from, effective_to),
         CONSTRAINT fk_bank_recon_rules_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_recon_rules_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_recon_rules_bank_account
           FOREIGN KEY (tenant_id, legal_entity_id, bank_account_id)
           REFERENCES bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_recon_rules_created_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_bank_recon_rules_updated_user
           FOREIGN KEY (tenant_id, updated_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_reconciliation_auto_runs (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NULL,
         bank_account_id BIGINT UNSIGNED NULL,
         run_request_id VARCHAR(190) NULL,
         run_mode ENUM('PREVIEW','APPLY') NOT NULL,
         status ENUM('SUCCESS','PARTIAL','FAILED') NOT NULL DEFAULT 'SUCCESS',
         date_from DATE NULL,
         date_to DATE NULL,
         scanned_count INT NOT NULL DEFAULT 0,
         matched_count INT NOT NULL DEFAULT 0,
         reconciled_count INT NOT NULL DEFAULT 0,
         exception_count INT NOT NULL DEFAULT 0,
         skipped_count INT NOT NULL DEFAULT 0,
         error_count INT NOT NULL DEFAULT 0,
         payload_json JSON NULL,
         created_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_recon_auto_runs_scope_id (tenant_id, id),
         UNIQUE KEY uk_bank_recon_auto_runs_request (tenant_id, run_request_id),
         KEY ix_bank_recon_auto_runs_scope (tenant_id, legal_entity_id, bank_account_id),
         KEY ix_bank_recon_auto_runs_mode_status (tenant_id, run_mode, status),
         KEY ix_bank_recon_auto_runs_created (tenant_id, created_at),
         CONSTRAINT fk_bank_recon_auto_runs_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_recon_auto_runs_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_recon_auto_runs_bank_account
           FOREIGN KEY (tenant_id, legal_entity_id, bank_account_id)
           REFERENCES bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_recon_auto_runs_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_reconciliation_exceptions (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         statement_line_id BIGINT UNSIGNED NOT NULL,
         bank_account_id BIGINT UNSIGNED NOT NULL,
         status ENUM('OPEN','ASSIGNED','RESOLVED','IGNORED') NOT NULL DEFAULT 'OPEN',
         severity ENUM('LOW','MEDIUM','HIGH') NOT NULL DEFAULT 'MEDIUM',
         reason_code VARCHAR(50) NOT NULL,
         reason_message VARCHAR(255) NULL,
         matched_rule_id BIGINT UNSIGNED NULL,
         suggested_action_type VARCHAR(40) NULL,
         suggested_payload_json JSON NULL,
         assigned_to_user_id INT NULL,
         assigned_at TIMESTAMP NULL,
         resolved_by_user_id INT NULL,
         resolved_at TIMESTAMP NULL,
         resolution_code VARCHAR(50) NULL,
         resolution_note VARCHAR(500) NULL,
         first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         occurrence_count INT NOT NULL DEFAULT 1,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_recon_ex_scope_id (tenant_id, legal_entity_id, id),
         KEY ix_bank_recon_ex_line (tenant_id, legal_entity_id, statement_line_id),
         KEY ix_bank_recon_ex_status (tenant_id, legal_entity_id, status),
         KEY ix_bank_recon_ex_reason (tenant_id, legal_entity_id, reason_code),
         KEY ix_bank_recon_ex_rule (tenant_id, legal_entity_id, matched_rule_id),
         KEY ix_bank_recon_ex_bank (tenant_id, legal_entity_id, bank_account_id),
         CONSTRAINT fk_bank_recon_ex_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_recon_ex_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_recon_ex_line
           FOREIGN KEY (tenant_id, legal_entity_id, statement_line_id)
           REFERENCES bank_statement_lines(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_recon_ex_bank
           FOREIGN KEY (tenant_id, legal_entity_id, bank_account_id)
           REFERENCES bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_recon_ex_rule
           FOREIGN KEY (tenant_id, matched_rule_id) REFERENCES bank_reconciliation_rules(tenant_id, id),
         CONSTRAINT fk_bank_recon_ex_assigned_user
           FOREIGN KEY (tenant_id, assigned_to_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_bank_recon_ex_resolved_user
           FOREIGN KEY (tenant_id, resolved_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_reconciliation_exception_events (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         bank_reconciliation_exception_id BIGINT UNSIGNED NOT NULL,
         event_type VARCHAR(30) NOT NULL,
         payload_json JSON NULL,
         acted_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_recon_ex_events_scope_id (tenant_id, legal_entity_id, id),
         KEY ix_bank_recon_ex_events_exception (tenant_id, legal_entity_id, bank_reconciliation_exception_id),
         KEY ix_bank_recon_ex_events_type (tenant_id, legal_entity_id, event_type),
         CONSTRAINT fk_bank_recon_ex_events_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_recon_ex_events_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_recon_ex_events_exception
           FOREIGN KEY (tenant_id, legal_entity_id, bank_reconciliation_exception_id)
           REFERENCES bank_reconciliation_exceptions(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_recon_ex_events_user
           FOREIGN KEY (tenant_id, acted_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },

  async down(connection) {
    await connection.execute(`DROP TABLE IF EXISTS bank_reconciliation_exception_events`);
    await connection.execute(`DROP TABLE IF EXISTS bank_reconciliation_exceptions`);
    await connection.execute(`DROP TABLE IF EXISTS bank_reconciliation_auto_runs`);
    await connection.execute(`DROP TABLE IF EXISTS bank_reconciliation_rules`);
    // Additive ALTER reversals intentionally omitted for safety.
  },
};

export default migration057BankReconciliationRulesAndExceptions;
