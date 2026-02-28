const ignorableErrnos = new Set([
  1060, // ER_DUP_FIELDNAME
  1061, // ER_DUP_KEYNAME
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

const statements = [
  `
  CREATE TABLE IF NOT EXISTS rbac_audit_logs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    actor_user_id INT NULL,
    target_user_id INT NULL,
    action VARCHAR(120) NOT NULL,
    resource_type VARCHAR(80) NOT NULL,
    resource_id VARCHAR(120) NULL,
    scope_type ENUM('TENANT','GROUP','COUNTRY','LEGAL_ENTITY','OPERATING_UNIT') NULL,
    scope_id BIGINT UNSIGNED NULL,
    request_id VARCHAR(80) NULL,
    ip_address VARCHAR(64) NULL,
    user_agent VARCHAR(255) NULL,
    payload_json JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY ix_rbac_audit_tenant_time (tenant_id, created_at),
    KEY ix_rbac_audit_action_time (tenant_id, action, created_at),
    KEY ix_rbac_audit_scope_time (tenant_id, scope_type, scope_id, created_at),
    CONSTRAINT fk_rbac_audit_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_rbac_audit_actor_user
      FOREIGN KEY (actor_user_id) REFERENCES users(id),
    CONSTRAINT fk_rbac_audit_target_user
      FOREIGN KEY (target_user_id) REFERENCES users(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS consolidation_run_entries (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    consolidation_run_id BIGINT UNSIGNED NOT NULL,
    tenant_id BIGINT UNSIGNED NOT NULL,
    consolidation_group_id BIGINT UNSIGNED NOT NULL,
    fiscal_period_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    group_account_id BIGINT UNSIGNED NOT NULL,
    source_currency_code CHAR(3) NOT NULL,
    presentation_currency_code CHAR(3) NOT NULL,
    consolidation_method ENUM('FULL','EQUITY','PROPORTIONATE') NOT NULL DEFAULT 'FULL',
    ownership_pct DECIMAL(9,6) NOT NULL DEFAULT 1.000000,
    translation_rate DECIMAL(20,10) NOT NULL DEFAULT 1.0000000000,
    local_debit_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    local_credit_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    local_balance_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    translated_debit DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    translated_credit DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    translated_balance DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_cons_run_entry_unique (
      consolidation_run_id,
      legal_entity_id,
      group_account_id
    ),
    KEY ix_cons_run_entries_run (consolidation_run_id),
    KEY ix_cons_run_entries_tenant_group_period (tenant_id, consolidation_group_id, fiscal_period_id),
    KEY ix_cons_run_entries_entity_account (legal_entity_id, group_account_id),
    CONSTRAINT fk_cons_run_entry_run
      FOREIGN KEY (consolidation_run_id) REFERENCES consolidation_runs(id),
    CONSTRAINT fk_cons_run_entry_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_cons_run_entry_group
      FOREIGN KEY (consolidation_group_id) REFERENCES consolidation_groups(id),
    CONSTRAINT fk_cons_run_entry_period
      FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
    CONSTRAINT fk_cons_run_entry_entity
      FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_cons_run_entry_account
      FOREIGN KEY (group_account_id) REFERENCES accounts(id),
    CONSTRAINT fk_cons_run_entry_source_currency
      FOREIGN KEY (source_currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_cons_run_entry_present_currency
      FOREIGN KEY (presentation_currency_code) REFERENCES currencies(code)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
];

const migration003RbacAuditAndConsolidationRunEntries = {
  key: "m003_rbac_audit_and_consolidation_run_entries",
  description:
    "RBAC audit trail and consolidation run translated entries table",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration003RbacAuditAndConsolidationRunEntries;
