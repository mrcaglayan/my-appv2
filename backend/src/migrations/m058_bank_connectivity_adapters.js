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

const migration058BankConnectivityAdapters = {
  key: "m058_bank_connectivity_adapters",
  description: "Bank connectivity connectors, account mappings, and statement sync runs (PR-B05)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_connectors (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         connector_code VARCHAR(60) NOT NULL,
         connector_name VARCHAR(190) NOT NULL,
         provider_code VARCHAR(60) NOT NULL,
         connector_type ENUM('OPEN_BANKING','HOST_TO_HOST','SFTP','API') NOT NULL DEFAULT 'OPEN_BANKING',
         status ENUM('DRAFT','ACTIVE','PAUSED','ERROR','DISABLED') NOT NULL DEFAULT 'DRAFT',
         adapter_version VARCHAR(40) NOT NULL DEFAULT 'v1',
         config_json JSON NULL,
         credentials_encrypted_json LONGTEXT NULL,
         credentials_key_version VARCHAR(40) NULL,
         last_cursor VARCHAR(255) NULL,
         last_sync_at TIMESTAMP NULL,
         last_success_at TIMESTAMP NULL,
         last_error_at TIMESTAMP NULL,
         last_error_message VARCHAR(500) NULL,
         sync_mode ENUM('MANUAL','SCHEDULED') NOT NULL DEFAULT 'MANUAL',
         sync_frequency_minutes INT NULL,
         next_sync_at TIMESTAMP NULL,
         created_by_user_id INT NULL,
         updated_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_connectors_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_bank_connectors_code (tenant_id, connector_code),
         KEY ix_bank_connectors_scope_le (tenant_id, legal_entity_id),
         KEY ix_bank_connectors_status (tenant_id, legal_entity_id, status),
         KEY ix_bank_connectors_provider (tenant_id, legal_entity_id, provider_code),
         KEY ix_bank_connectors_due (tenant_id, status, sync_mode, next_sync_at),
         CONSTRAINT fk_bank_connectors_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_connectors_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_connectors_created_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_bank_connectors_updated_user
           FOREIGN KEY (tenant_id, updated_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_connector_account_links (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         bank_connector_id BIGINT UNSIGNED NOT NULL,
         external_account_id VARCHAR(190) NOT NULL,
         external_account_name VARCHAR(190) NULL,
         external_currency_code CHAR(3) NOT NULL,
         bank_account_id BIGINT UNSIGNED NOT NULL,
         status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
         created_by_user_id INT NULL,
         updated_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_connector_account_links_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_bank_connector_account_links_connector_ext
           (tenant_id, legal_entity_id, bank_connector_id, external_account_id),
         KEY ix_bank_connector_account_links_connector (tenant_id, legal_entity_id, bank_connector_id),
         KEY ix_bank_connector_account_links_bank_account (tenant_id, legal_entity_id, bank_account_id),
         KEY ix_bank_connector_account_links_status (tenant_id, legal_entity_id, status),
         CONSTRAINT fk_bank_connector_account_links_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_connector_account_links_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_connector_account_links_connector
           FOREIGN KEY (tenant_id, legal_entity_id, bank_connector_id)
           REFERENCES bank_connectors(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_connector_account_links_bank_account
           FOREIGN KEY (tenant_id, legal_entity_id, bank_account_id)
           REFERENCES bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_connector_account_links_created_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_bank_connector_account_links_updated_user
           FOREIGN KEY (tenant_id, updated_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_connector_sync_runs (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         bank_connector_id BIGINT UNSIGNED NOT NULL,
         run_type ENUM('STATEMENT_PULL') NOT NULL DEFAULT 'STATEMENT_PULL',
         status ENUM('RUNNING','SUCCESS','PARTIAL','FAILED') NOT NULL DEFAULT 'RUNNING',
         request_id VARCHAR(190) NULL,
         started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         finished_at TIMESTAMP NULL,
         window_from DATE NULL,
         window_to DATE NULL,
         cursor_before VARCHAR(255) NULL,
         cursor_after VARCHAR(255) NULL,
         fetched_count INT NOT NULL DEFAULT 0,
         imported_count INT NOT NULL DEFAULT 0,
         duplicate_count INT NOT NULL DEFAULT 0,
         skipped_unmapped_count INT NOT NULL DEFAULT 0,
         error_count INT NOT NULL DEFAULT 0,
         payload_json JSON NULL,
         error_message VARCHAR(500) NULL,
         triggered_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_connector_sync_runs_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_bank_connector_sync_runs_request
           (tenant_id, legal_entity_id, bank_connector_id, request_id),
         KEY ix_bank_connector_sync_runs_connector (tenant_id, legal_entity_id, bank_connector_id),
         KEY ix_bank_connector_sync_runs_status (tenant_id, legal_entity_id, status),
         KEY ix_bank_connector_sync_runs_started (tenant_id, legal_entity_id, started_at),
         CONSTRAINT fk_bank_connector_sync_runs_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_connector_sync_runs_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_connector_sync_runs_connector
           FOREIGN KEY (tenant_id, legal_entity_id, bank_connector_id)
           REFERENCES bank_connectors(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_connector_sync_runs_user
           FOREIGN KEY (tenant_id, triggered_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_connector_sync_run_imports (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         bank_connector_sync_run_id BIGINT UNSIGNED NOT NULL,
         bank_account_id BIGINT UNSIGNED NOT NULL,
         external_account_id VARCHAR(190) NOT NULL,
         bank_statement_import_id BIGINT UNSIGNED NOT NULL,
         import_ref VARCHAR(190) NOT NULL,
         imported_count INT NOT NULL DEFAULT 0,
         duplicate_count INT NOT NULL DEFAULT 0,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_connector_sync_run_imports_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_bank_connector_sync_run_imports_run_account_ref
           (tenant_id, legal_entity_id, bank_connector_sync_run_id, external_account_id, bank_statement_import_id),
         KEY ix_bank_connector_sync_run_imports_run (tenant_id, legal_entity_id, bank_connector_sync_run_id),
         KEY ix_bank_connector_sync_run_imports_bank_account (tenant_id, legal_entity_id, bank_account_id),
         CONSTRAINT fk_bank_connector_sync_run_imports_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_connector_sync_run_imports_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_connector_sync_run_imports_run
           FOREIGN KEY (tenant_id, legal_entity_id, bank_connector_sync_run_id)
           REFERENCES bank_connector_sync_runs(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_connector_sync_run_imports_bank_account
           FOREIGN KEY (tenant_id, legal_entity_id, bank_account_id)
           REFERENCES bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_connector_sync_run_imports_import
           FOREIGN KEY (tenant_id, legal_entity_id, bank_statement_import_id)
           REFERENCES bank_statement_imports(tenant_id, legal_entity_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },

  async down(connection) {
    await connection.execute(`DROP TABLE IF EXISTS bank_connector_sync_run_imports`);
    await connection.execute(`DROP TABLE IF EXISTS bank_connector_sync_runs`);
    await connection.execute(`DROP TABLE IF EXISTS bank_connector_account_links`);
    await connection.execute(`DROP TABLE IF EXISTS bank_connectors`);
  },
};

export default migration058BankConnectivityAdapters;
