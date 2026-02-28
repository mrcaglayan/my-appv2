const IGNORABLE_ERRNOS = new Set([
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
    if (IGNORABLE_ERRNOS.has(Number(err?.errno))) return;
    throw err;
  }
}

const migration065DataRetentionArchival = {
  key: "m065_data_retention_archival",
  description: "Retention policies, retention runs, and immutable period export snapshots (H07)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS data_retention_policies (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NULL,
         policy_code VARCHAR(60) NOT NULL,
         policy_name VARCHAR(190) NOT NULL,
         dataset_code VARCHAR(60) NOT NULL,
         action_code ENUM('MASK','PURGE','ARCHIVE') NOT NULL,
         retention_days INT UNSIGNED NOT NULL,
         scope_type ENUM('TENANT','LEGAL_ENTITY') NOT NULL DEFAULT 'TENANT',
         status ENUM('ACTIVE','PAUSED','DISABLED') NOT NULL DEFAULT 'ACTIVE',
         config_json JSON NULL,
         last_run_at DATETIME NULL,
         last_run_status VARCHAR(20) NULL,
         last_run_note VARCHAR(500) NULL,
         created_by_user_id INT NULL,
         updated_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_data_retention_policies_scope_id (tenant_id, id),
         UNIQUE KEY uk_data_retention_policies_scope_code (tenant_id, policy_code),
         KEY ix_data_retention_policies_scope_status (tenant_id, status),
         KEY ix_data_retention_policies_scope_dataset (tenant_id, dataset_code, action_code),
         KEY ix_data_retention_policies_scope_legal_entity (tenant_id, legal_entity_id),
         CONSTRAINT fk_data_retention_policies_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_data_retention_policies_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_data_retention_policies_created_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_data_retention_policies_updated_user
           FOREIGN KEY (tenant_id, updated_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS data_retention_runs (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         data_retention_policy_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NULL,
         trigger_mode ENUM('MANUAL','SCHEDULED','JOB') NOT NULL DEFAULT 'MANUAL',
         status ENUM('RUNNING','COMPLETED','PARTIAL','FAILED') NOT NULL DEFAULT 'RUNNING',
         run_idempotency_key VARCHAR(190) NULL,
         retention_cutoff_date DATE NOT NULL,
         scanned_rows INT UNSIGNED NOT NULL DEFAULT 0,
         affected_rows INT UNSIGNED NOT NULL DEFAULT 0,
         masked_rows INT UNSIGNED NOT NULL DEFAULT 0,
         purged_rows INT UNSIGNED NOT NULL DEFAULT 0,
         archived_rows INT UNSIGNED NOT NULL DEFAULT 0,
         error_rows INT UNSIGNED NOT NULL DEFAULT 0,
         payload_json JSON NULL,
         error_text VARCHAR(500) NULL,
         started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         finished_at TIMESTAMP NULL,
         acted_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_data_retention_runs_scope_id (tenant_id, id),
         UNIQUE KEY uk_data_retention_runs_scope_idempotency (tenant_id, run_idempotency_key),
         KEY ix_data_retention_runs_scope_policy (tenant_id, data_retention_policy_id, started_at),
         KEY ix_data_retention_runs_scope_status (tenant_id, status, started_at),
         KEY ix_data_retention_runs_scope_legal_entity (tenant_id, legal_entity_id, started_at),
         CONSTRAINT fk_data_retention_runs_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_data_retention_runs_policy
           FOREIGN KEY (tenant_id, data_retention_policy_id) REFERENCES data_retention_policies(tenant_id, id),
         CONSTRAINT fk_data_retention_runs_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_data_retention_runs_user
           FOREIGN KEY (tenant_id, acted_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS sensitive_data_audit_archive (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         source_sensitive_data_audit_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NULL,
         module_code VARCHAR(40) NOT NULL,
         object_type VARCHAR(60) NOT NULL,
         object_id BIGINT UNSIGNED NOT NULL,
         action VARCHAR(30) NOT NULL,
         payload_json JSON NULL,
         note VARCHAR(500) NULL,
         acted_by_user_id INT NULL,
         acted_at DATETIME NOT NULL,
         archived_by_user_id INT NULL,
         archived_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_sensitive_data_audit_archive_scope_source (tenant_id, source_sensitive_data_audit_id),
         KEY ix_sensitive_data_audit_archive_scope_module (tenant_id, module_code, acted_at),
         KEY ix_sensitive_data_audit_archive_scope_object (tenant_id, object_type, object_id),
         CONSTRAINT fk_sensitive_data_audit_archive_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_sensitive_data_audit_archive_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_sensitive_data_audit_archive_archived_user
           FOREIGN KEY (tenant_id, archived_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS period_export_snapshots (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         snapshot_type ENUM('PAYROLL_CLOSE_PERIOD') NOT NULL DEFAULT 'PAYROLL_CLOSE_PERIOD',
         period_start DATE NOT NULL,
         period_end DATE NOT NULL,
         payroll_period_close_id BIGINT UNSIGNED NULL,
         status ENUM('READY','FAILED') NOT NULL DEFAULT 'READY',
         snapshot_hash CHAR(64) NOT NULL,
         snapshot_meta_json JSON NULL,
         idempotency_key VARCHAR(190) NULL,
         created_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_period_export_snapshots_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_period_export_snapshots_scope_idempotency (tenant_id, legal_entity_id, idempotency_key),
         KEY ix_period_export_snapshots_scope_period (tenant_id, legal_entity_id, period_start, period_end),
         KEY ix_period_export_snapshots_scope_status (tenant_id, legal_entity_id, status),
         CONSTRAINT fk_period_export_snapshots_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_period_export_snapshots_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_period_export_snapshots_close
           FOREIGN KEY (tenant_id, legal_entity_id, payroll_period_close_id)
           REFERENCES payroll_period_closes(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_period_export_snapshots_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS period_export_snapshot_items (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         period_export_snapshot_id BIGINT UNSIGNED NOT NULL,
         item_code VARCHAR(80) NOT NULL,
         item_count INT UNSIGNED NOT NULL DEFAULT 0,
         item_hash CHAR(64) NOT NULL,
         payload_json JSON NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_period_export_snapshot_items_scope_item
           (tenant_id, legal_entity_id, period_export_snapshot_id, item_code),
         KEY ix_period_export_snapshot_items_scope_snapshot
           (tenant_id, legal_entity_id, period_export_snapshot_id),
         CONSTRAINT fk_period_export_snapshot_items_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_period_export_snapshot_items_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_period_export_snapshot_items_snapshot
           FOREIGN KEY (tenant_id, legal_entity_id, period_export_snapshot_id)
           REFERENCES period_export_snapshots(tenant_id, legal_entity_id, id)
           ON DELETE CASCADE
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },

  async down(connection) {
    await safeExecute(connection, `DROP TABLE IF EXISTS period_export_snapshot_items`);
    await safeExecute(connection, `DROP TABLE IF EXISTS period_export_snapshots`);
    await safeExecute(connection, `DROP TABLE IF EXISTS sensitive_data_audit_archive`);
    await safeExecute(connection, `DROP TABLE IF EXISTS data_retention_runs`);
    await safeExecute(connection, `DROP TABLE IF EXISTS data_retention_policies`);
  },
};

export default migration065DataRetentionArchival;
