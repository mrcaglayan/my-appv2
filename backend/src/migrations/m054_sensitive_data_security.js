const IGNORABLE_ERRNOS = new Set([
  1050, // ER_TABLE_EXISTS_ERROR
  1060, // ER_DUP_FIELDNAME
  1061, // ER_DUP_KEYNAME
  1091, // ER_CANT_DROP_FIELD_OR_KEY
  1146, // ER_NO_SUCH_TABLE (optional bank tables)
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

const migration054SensitiveDataSecurity = {
  key: "m054_sensitive_data_security",
  description:
    "Sensitive data hardening: encrypted secrets columns, raw payload retention status, and audit table (H01)",
  async up(connection) {
    await safeExecute(
      connection,
      `ALTER TABLE payroll_provider_connections
         ADD COLUMN secrets_encrypted_json LONGTEXT NULL AFTER settings_json`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_provider_connections
         ADD COLUMN secrets_key_version VARCHAR(40) NULL AFTER secrets_encrypted_json`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_provider_connections
         ADD COLUMN secrets_migrated_at DATETIME NULL AFTER secrets_key_version`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_provider_import_jobs
         ADD COLUMN raw_payload_retention_status ENUM('ACTIVE','MASKED','PURGED') NOT NULL DEFAULT 'ACTIVE' AFTER raw_payload_text`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_provider_import_jobs
         ADD COLUMN raw_payload_masked_at DATETIME NULL AFTER raw_payload_retention_status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_provider_import_jobs
         ADD COLUMN raw_payload_purged_at DATETIME NULL AFTER raw_payload_masked_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_provider_import_jobs
         ADD COLUMN raw_payload_redaction_note VARCHAR(500) NULL AFTER raw_payload_purged_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_provider_import_jobs
         ADD KEY ix_payroll_provider_import_jobs_retention_status (raw_payload_retention_status)`
    );

    // Optional future bank tables: keep additive and ignore missing-table errors.
    await safeExecute(
      connection,
      `ALTER TABLE bank_provider_connections
         ADD COLUMN secrets_encrypted_json LONGTEXT NULL AFTER settings_json`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_provider_connections
         ADD COLUMN secrets_key_version VARCHAR(40) NULL AFTER secrets_encrypted_json`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_provider_connections
         ADD COLUMN secrets_migrated_at DATETIME NULL AFTER secrets_key_version`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_webhook_endpoints
         ADD COLUMN secret_encrypted_json LONGTEXT NULL AFTER webhook_secret`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_webhook_endpoints
         ADD COLUMN secret_key_version VARCHAR(40) NULL AFTER secret_encrypted_json`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_feed_events
         ADD COLUMN raw_payload_retention_status ENUM('ACTIVE','MASKED','PURGED') NOT NULL DEFAULT 'ACTIVE' AFTER raw_payload_text`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_feed_events
         ADD COLUMN raw_payload_masked_at DATETIME NULL AFTER raw_payload_retention_status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_feed_events
         ADD COLUMN raw_payload_purged_at DATETIME NULL AFTER raw_payload_masked_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_webhook_events
         ADD COLUMN raw_payload_retention_status ENUM('ACTIVE','MASKED','PURGED') NOT NULL DEFAULT 'ACTIVE' AFTER raw_payload_text`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_webhook_events
         ADD COLUMN raw_payload_masked_at DATETIME NULL AFTER raw_payload_retention_status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_webhook_events
         ADD COLUMN raw_payload_purged_at DATETIME NULL AFTER raw_payload_masked_at`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS sensitive_data_audit (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NULL,
         module_code VARCHAR(40) NOT NULL,
         object_type VARCHAR(60) NOT NULL,
         object_id BIGINT UNSIGNED NOT NULL,
         action VARCHAR(30) NOT NULL,
         payload_json JSON NULL,
         note VARCHAR(500) NULL,
         acted_by_user_id INT NULL,
         acted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         KEY ix_sensitive_data_audit_tenant (tenant_id),
         KEY ix_sensitive_data_audit_obj (tenant_id, module_code, object_type, object_id),
         KEY ix_sensitive_data_audit_action (tenant_id, action),
         KEY ix_sensitive_data_audit_acted_at (acted_at),
         CONSTRAINT fk_sensitive_data_audit_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_sensitive_data_audit_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_sensitive_data_audit_user
           FOREIGN KEY (tenant_id, acted_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },

  async down(connection) {
    await safeExecute(connection, `DROP TABLE IF EXISTS sensitive_data_audit`);
    // ALTER TABLE reversals intentionally omitted for additive hardening migration safety.
  },
};

export default migration054SensitiveDataSecurity;

