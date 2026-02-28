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

const migration033BankReconciliation = {
  key: "m033_bank_reconciliation",
  description: "Bank reconciliation core tables and statement-line PARTIAL status support",
  async up(connection) {
    // PR-B03 introduces PARTIAL reconciliation status.
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
       MODIFY COLUMN recon_status ENUM('UNMATCHED','PARTIAL','MATCHED','IGNORED')
         NOT NULL DEFAULT 'UNMATCHED'`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_reconciliation_matches (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         statement_line_id BIGINT UNSIGNED NOT NULL,
         match_type VARCHAR(30) NOT NULL DEFAULT 'MANUAL',
         matched_entity_type ENUM('JOURNAL','PAYMENT_BATCH','CASH_TXN','MANUAL_ADJUSTMENT') NOT NULL,
         matched_entity_id BIGINT UNSIGNED NOT NULL,
         matched_amount DECIMAL(20,6) NOT NULL,
         status ENUM('ACTIVE','REVERSED') NOT NULL DEFAULT 'ACTIVE',
         notes VARCHAR(500) NULL,
         matched_by_user_id INT NULL,
         matched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         reversed_by_user_id INT NULL,
         reversed_at TIMESTAMP NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
           ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_recon_match_tenant_entity_id (tenant_id, legal_entity_id, id),
         KEY ix_bank_recon_match_line_status (tenant_id, legal_entity_id, statement_line_id, status),
         KEY ix_bank_recon_match_entity (tenant_id, legal_entity_id, matched_entity_type, matched_entity_id),
         KEY ix_bank_recon_match_status (tenant_id, legal_entity_id, status),
         CONSTRAINT fk_bank_recon_match_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_recon_match_line
           FOREIGN KEY (tenant_id, legal_entity_id, statement_line_id)
           REFERENCES bank_statement_lines(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_recon_match_user_tenant
           FOREIGN KEY (tenant_id, matched_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_bank_recon_match_rev_user_tenant
           FOREIGN KEY (tenant_id, reversed_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_reconciliation_audit (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         statement_line_id BIGINT UNSIGNED NOT NULL,
         action ENUM('SUGGESTED','MATCHED','UNMATCHED','IGNORE','UNIGNORE','AUTO_STATUS') NOT NULL,
         payload_json JSON NULL,
         acted_by_user_id INT NULL,
         acted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_recon_audit_tenant_entity_id (tenant_id, legal_entity_id, id),
         KEY ix_bank_recon_audit_line (tenant_id, legal_entity_id, statement_line_id),
         KEY ix_bank_recon_audit_action (tenant_id, legal_entity_id, action),
         KEY ix_bank_recon_audit_time (tenant_id, legal_entity_id, acted_at),
         CONSTRAINT fk_bank_recon_audit_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_recon_audit_line
           FOREIGN KEY (tenant_id, legal_entity_id, statement_line_id)
           REFERENCES bank_statement_lines(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bank_recon_audit_user_tenant
           FOREIGN KEY (tenant_id, acted_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },
};

export default migration033BankReconciliation;
