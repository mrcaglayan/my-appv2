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

const migration064ExceptionWorkbench = {
  key: "m064_exception_workbench",
  description: "Unified exception workbench for bank and payroll operations (H06)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS exception_workbench (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NULL,
         module_code ENUM('BANK','PAYROLL') NOT NULL,
         exception_type VARCHAR(80) NOT NULL,
         source_type VARCHAR(80) NOT NULL,
         source_key VARCHAR(255) NOT NULL,
         source_ref VARCHAR(190) NULL,
         source_ref_id BIGINT UNSIGNED NULL,
         source_status_code VARCHAR(30) NULL,
         severity ENUM('LOW','MEDIUM','HIGH','CRITICAL') NOT NULL DEFAULT 'MEDIUM',
         status ENUM('OPEN','IN_REVIEW','RESOLVED','IGNORED') NOT NULL DEFAULT 'OPEN',
         owner_user_id INT NULL,
         title VARCHAR(190) NOT NULL,
         description VARCHAR(500) NULL,
         payload_json JSON NULL,
         resolution_action VARCHAR(80) NULL,
         resolution_note VARCHAR(500) NULL,
         resolved_by_user_id INT NULL,
         resolved_at TIMESTAMP NULL,
         first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_exception_workbench_scope_id (tenant_id, id),
         UNIQUE KEY uk_exception_workbench_scope_source_key (tenant_id, source_key),
         KEY ix_exception_workbench_scope_status (tenant_id, status),
         KEY ix_exception_workbench_scope_module (tenant_id, module_code, status),
         KEY ix_exception_workbench_scope_legal_entity (tenant_id, legal_entity_id, status),
         KEY ix_exception_workbench_scope_owner (tenant_id, owner_user_id, status),
         KEY ix_exception_workbench_scope_last_seen (tenant_id, last_seen_at),
         CONSTRAINT fk_exception_workbench_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_exception_workbench_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_exception_workbench_owner_user
           FOREIGN KEY (tenant_id, owner_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_exception_workbench_resolved_user
           FOREIGN KEY (tenant_id, resolved_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS exception_workbench_audit (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         exception_workbench_id BIGINT UNSIGNED NOT NULL,
         event_type VARCHAR(40) NOT NULL,
         from_status VARCHAR(20) NULL,
         to_status VARCHAR(20) NULL,
         payload_json JSON NULL,
         acted_by_user_id INT NULL,
         acted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_exception_workbench_audit_scope_id (tenant_id, id),
         KEY ix_exception_workbench_audit_scope_exception (tenant_id, exception_workbench_id),
         KEY ix_exception_workbench_audit_scope_event (tenant_id, event_type),
         KEY ix_exception_workbench_audit_scope_time (tenant_id, acted_at),
         CONSTRAINT fk_exception_workbench_audit_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_exception_workbench_audit_exception
           FOREIGN KEY (tenant_id, exception_workbench_id) REFERENCES exception_workbench(tenant_id, id)
           ON DELETE CASCADE,
         CONSTRAINT fk_exception_workbench_audit_user
           FOREIGN KEY (tenant_id, acted_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },

  async down(connection) {
    await safeExecute(connection, `DROP TABLE IF EXISTS exception_workbench_audit`);
    await safeExecute(connection, `DROP TABLE IF EXISTS exception_workbench`);
  },
};

export default migration064ExceptionWorkbench;
