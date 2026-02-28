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
    if (ignorableErrnos.has(err?.errno)) return;
    throw err;
  }
}

const migration047PayrollProviderAdapters = {
  key: "m047_payroll_provider_adapters",
  description:
    "Payroll provider adapter connections, employee refs, preview/apply import jobs, and run traceability (PR-P09)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_provider_connections (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         provider_code VARCHAR(60) NOT NULL,
         provider_name VARCHAR(120) NOT NULL,
         adapter_version VARCHAR(40) NOT NULL DEFAULT 'v1',
         status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
         is_default TINYINT(1) NOT NULL DEFAULT 0,
         settings_json JSON NULL,
         secrets_json JSON NULL,
         created_by_user_id INT NULL,
         updated_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_provider_connections_scope_id (tenant_id, legal_entity_id, id),
         KEY ix_payroll_provider_connections_scope_le (tenant_id, legal_entity_id),
         KEY ix_payroll_provider_connections_scope_provider (tenant_id, legal_entity_id, provider_code),
         KEY ix_payroll_provider_connections_scope_status (tenant_id, legal_entity_id, status),
         CONSTRAINT fk_payroll_provider_connections_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_provider_connections_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_provider_connections_created_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payroll_provider_connections_updated_user
           FOREIGN KEY (tenant_id, updated_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_employee_provider_refs (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         provider_code VARCHAR(60) NOT NULL,
         external_employee_id VARCHAR(120) NOT NULL,
         external_employee_code VARCHAR(120) NULL,
         internal_employee_code VARCHAR(100) NOT NULL,
         internal_employee_name VARCHAR(255) NULL,
         status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
         is_primary TINYINT(1) NOT NULL DEFAULT 1,
         payload_json JSON NULL,
         created_by_user_id INT NULL,
         updated_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_employee_provider_refs_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payroll_employee_provider_refs_lookup
           (tenant_id, legal_entity_id, provider_code, external_employee_id),
         KEY ix_payroll_employee_provider_refs_internal
           (tenant_id, legal_entity_id, internal_employee_code),
         KEY ix_payroll_employee_provider_refs_status
           (tenant_id, legal_entity_id, status),
         CONSTRAINT fk_payroll_employee_provider_refs_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_employee_provider_refs_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_employee_provider_refs_created_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payroll_employee_provider_refs_updated_user
           FOREIGN KEY (tenant_id, updated_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_provider_import_jobs (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         payroll_provider_connection_id BIGINT UNSIGNED NOT NULL,
         provider_code VARCHAR(60) NOT NULL,
         adapter_version VARCHAR(40) NOT NULL DEFAULT 'v1',
         payroll_period DATE NOT NULL,
         period_start DATE NULL,
         period_end DATE NULL,
         pay_date DATE NULL,
         currency_code CHAR(3) NOT NULL,
         import_key VARCHAR(190) NULL,
         raw_payload_hash CHAR(64) NOT NULL,
         normalized_payload_hash CHAR(64) NULL,
         source_format ENUM('CSV','JSON') NOT NULL,
         source_filename VARCHAR(255) NULL,
         status ENUM('PREVIEWED','APPLYING','APPLIED','REJECTED','FAILED') NOT NULL DEFAULT 'PREVIEWED',
         preview_summary_json JSON NULL,
         validation_errors_json JSON NULL,
         match_errors_json JSON NULL,
         match_warnings_json JSON NULL,
         raw_payload_text LONGTEXT NULL,
         normalized_payload_json JSON NULL,
         applied_payroll_run_id BIGINT UNSIGNED NULL,
         requested_by_user_id INT NULL,
         requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         applied_by_user_id INT NULL,
         applied_at TIMESTAMP NULL,
         apply_idempotency_key VARCHAR(190) NULL,
         failure_message VARCHAR(500) NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_provider_import_jobs_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payroll_provider_import_jobs_preview_import_key
           (tenant_id, legal_entity_id, provider_code, import_key),
         UNIQUE KEY uk_payroll_provider_import_jobs_apply_idem
           (tenant_id, apply_idempotency_key),
         UNIQUE KEY uk_payroll_provider_import_jobs_payload_hash
           (tenant_id, legal_entity_id, provider_code, payroll_period, raw_payload_hash),
         KEY ix_payroll_provider_import_jobs_scope_status (tenant_id, legal_entity_id, status),
         KEY ix_payroll_provider_import_jobs_scope_period (tenant_id, legal_entity_id, payroll_period),
         KEY ix_payroll_provider_import_jobs_scope_provider (tenant_id, legal_entity_id, provider_code),
         KEY ix_payroll_provider_import_jobs_connection (tenant_id, legal_entity_id, payroll_provider_connection_id),
         CONSTRAINT fk_payroll_provider_import_jobs_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_provider_import_jobs_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_provider_import_jobs_connection
           FOREIGN KEY (tenant_id, legal_entity_id, payroll_provider_connection_id)
           REFERENCES payroll_provider_connections(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_provider_import_jobs_requested_user
           FOREIGN KEY (tenant_id, requested_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payroll_provider_import_jobs_applied_user
           FOREIGN KEY (tenant_id, applied_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_provider_import_audit (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         payroll_provider_import_job_id BIGINT UNSIGNED NOT NULL,
         action ENUM('PREVIEWED','APPLY_STARTED','APPLIED','FAILED','REJECTED','STATUS') NOT NULL,
         payload_json JSON NULL,
         note VARCHAR(500) NULL,
         acted_by_user_id INT NULL,
         acted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_provider_import_audit_scope_id (tenant_id, legal_entity_id, id),
         KEY ix_payroll_provider_import_audit_scope_job (tenant_id, legal_entity_id, payroll_provider_import_job_id),
         KEY ix_payroll_provider_import_audit_scope_action (tenant_id, legal_entity_id, action),
         CONSTRAINT fk_payroll_provider_import_audit_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_provider_import_audit_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_provider_import_audit_job
           FOREIGN KEY (tenant_id, legal_entity_id, payroll_provider_import_job_id)
           REFERENCES payroll_provider_import_jobs(tenant_id, legal_entity_id, id)
           ON DELETE CASCADE,
         CONSTRAINT fk_payroll_provider_import_audit_user
           FOREIGN KEY (tenant_id, acted_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN source_type ENUM('MANUAL','CSV_IMPORT','PROVIDER_IMPORT') NOT NULL DEFAULT 'MANUAL' AFTER status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN source_provider_code VARCHAR(60) NULL AFTER source_type`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN source_provider_import_job_id BIGINT UNSIGNED NULL AFTER source_provider_code`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD KEY ix_payroll_runs_source_provider_import_job (source_provider_import_job_id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD CONSTRAINT fk_payroll_runs_source_provider_import_job
         FOREIGN KEY (tenant_id, legal_entity_id, source_provider_import_job_id)
         REFERENCES payroll_provider_import_jobs(tenant_id, legal_entity_id, id)`
    );
  },

  async down(connection) {
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs DROP FOREIGN KEY fk_payroll_runs_source_provider_import_job`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs DROP KEY ix_payroll_runs_source_provider_import_job`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs DROP COLUMN source_provider_import_job_id`
    );
    await safeExecute(connection, `ALTER TABLE payroll_runs DROP COLUMN source_provider_code`);
    await safeExecute(connection, `ALTER TABLE payroll_runs DROP COLUMN source_type`);

    await safeExecute(connection, `DROP TABLE IF EXISTS payroll_provider_import_audit`);
    await safeExecute(connection, `DROP TABLE IF EXISTS payroll_provider_import_jobs`);
    await safeExecute(connection, `DROP TABLE IF EXISTS payroll_employee_provider_refs`);
    await safeExecute(connection, `DROP TABLE IF EXISTS payroll_provider_connections`);
  },
};

export default migration047PayrollProviderAdapters;
