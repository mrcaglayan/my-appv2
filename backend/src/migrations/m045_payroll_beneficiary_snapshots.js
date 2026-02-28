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

const migration045PayrollBeneficiarySnapshots = {
  key: "m045_payroll_beneficiary_snapshots",
  description:
    "Payroll beneficiary bank master and immutable snapshots for payroll payment links (PR-P07)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_beneficiary_bank_accounts (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         employee_code VARCHAR(100) NOT NULL,
         employee_name VARCHAR(255) NULL,
         account_holder_name VARCHAR(190) NOT NULL,
         bank_name VARCHAR(190) NOT NULL,
         bank_branch_name VARCHAR(190) NULL,
         country_code CHAR(2) NULL,
         currency_code CHAR(3) NOT NULL,
         iban VARCHAR(64) NULL,
         account_number VARCHAR(64) NULL,
         routing_number VARCHAR(64) NULL,
         swift_bic VARCHAR(32) NULL,
         account_last4 VARCHAR(8) NULL,
         account_fingerprint CHAR(64) NULL,
         is_primary TINYINT(1) NOT NULL DEFAULT 0,
         status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
         effective_from DATE NULL,
         effective_to DATE NULL,
         verification_status VARCHAR(20) NOT NULL DEFAULT 'UNVERIFIED',
         source_type VARCHAR(20) NOT NULL DEFAULT 'MANUAL',
         external_ref VARCHAR(190) NULL,
         payload_json JSON NULL,
         created_by_user_id INT NULL,
         updated_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_beneficiary_bank_accounts_scope_id (tenant_id, legal_entity_id, id),
         KEY ix_payroll_beneficiary_bank_accounts_scope_employee
           (tenant_id, legal_entity_id, employee_code),
         KEY ix_payroll_beneficiary_bank_accounts_scope_primary
           (tenant_id, legal_entity_id, employee_code, currency_code, is_primary, status),
         KEY ix_payroll_beneficiary_bank_accounts_scope_status
           (tenant_id, legal_entity_id, status),
         KEY ix_payroll_beneficiary_bank_accounts_scope_effective
           (tenant_id, legal_entity_id, effective_from, effective_to),
         CONSTRAINT fk_payroll_beneficiary_bank_accounts_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_beneficiary_bank_accounts_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_beneficiary_bank_accounts_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code),
         CONSTRAINT fk_payroll_beneficiary_bank_accounts_created_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payroll_beneficiary_bank_accounts_updated_user
           FOREIGN KEY (tenant_id, updated_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_beneficiary_bank_account_audit (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         beneficiary_bank_account_id BIGINT UNSIGNED NOT NULL,
         employee_code VARCHAR(100) NOT NULL,
         action VARCHAR(30) NOT NULL,
         before_json JSON NULL,
         after_json JSON NULL,
         reason VARCHAR(255) NULL,
         acted_by_user_id INT NULL,
         acted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_beneficiary_bank_account_audit_scope_id (tenant_id, legal_entity_id, id),
         KEY ix_payroll_beneficiary_bank_account_audit_scope_account
           (tenant_id, legal_entity_id, beneficiary_bank_account_id),
         KEY ix_payroll_beneficiary_bank_account_audit_scope_employee
           (tenant_id, legal_entity_id, employee_code),
         KEY ix_payroll_beneficiary_bank_account_audit_scope_action
           (tenant_id, legal_entity_id, action),
         KEY ix_payroll_beneficiary_bank_account_audit_scope_time
           (tenant_id, legal_entity_id, acted_at),
         CONSTRAINT fk_payroll_beneficiary_bank_account_audit_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_beneficiary_bank_account_audit_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_beneficiary_bank_account_audit_account
           FOREIGN KEY (tenant_id, legal_entity_id, beneficiary_bank_account_id)
           REFERENCES payroll_beneficiary_bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_beneficiary_bank_account_audit_user
           FOREIGN KEY (tenant_id, acted_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_beneficiary_bank_snapshots (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         employee_code VARCHAR(100) NOT NULL,
         employee_name VARCHAR(255) NULL,
         source_beneficiary_bank_account_id BIGINT UNSIGNED NULL,
         snapshot_hash CHAR(64) NOT NULL,
         currency_code CHAR(3) NOT NULL,
         account_holder_name VARCHAR(190) NOT NULL,
         bank_name VARCHAR(190) NOT NULL,
         bank_branch_name VARCHAR(190) NULL,
         country_code CHAR(2) NULL,
         iban VARCHAR(64) NULL,
         account_number VARCHAR(64) NULL,
         routing_number VARCHAR(64) NULL,
         swift_bic VARCHAR(32) NULL,
         account_last4 VARCHAR(8) NULL,
         verification_status VARCHAR(20) NOT NULL DEFAULT 'UNVERIFIED',
         payload_json JSON NULL,
         created_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_beneficiary_bank_snapshots_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payroll_beneficiary_bank_snapshots_scope_hash
           (tenant_id, legal_entity_id, snapshot_hash),
         KEY ix_payroll_beneficiary_bank_snapshots_scope_employee
           (tenant_id, legal_entity_id, employee_code),
         KEY ix_payroll_beneficiary_bank_snapshots_scope_source
           (tenant_id, legal_entity_id, source_beneficiary_bank_account_id),
         CONSTRAINT fk_payroll_beneficiary_bank_snapshots_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_beneficiary_bank_snapshots_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_beneficiary_bank_snapshots_source_account
           FOREIGN KEY (tenant_id, legal_entity_id, source_beneficiary_bank_account_id)
           REFERENCES payroll_beneficiary_bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_beneficiary_bank_snapshots_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code),
         CONSTRAINT fk_payroll_beneficiary_bank_snapshots_user
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_payment_links
         ADD COLUMN beneficiary_bank_snapshot_id BIGINT UNSIGNED NULL
           AFTER payroll_liability_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_payment_links
         ADD COLUMN beneficiary_snapshot_status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
           AFTER beneficiary_bank_snapshot_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_payment_links
         ADD KEY ix_payroll_liability_payment_links_scope_beneficiary_snapshot
           (tenant_id, legal_entity_id, beneficiary_bank_snapshot_id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_payment_links
         ADD CONSTRAINT fk_payroll_liability_payment_links_beneficiary_snapshot
           FOREIGN KEY (tenant_id, legal_entity_id, beneficiary_bank_snapshot_id)
           REFERENCES payroll_beneficiary_bank_snapshots(tenant_id, legal_entity_id, id)`
    );

    await safeExecute(
      connection,
      `UPDATE payroll_liability_payment_links pl
       JOIN payroll_run_liabilities l
         ON l.tenant_id = pl.tenant_id
        AND l.legal_entity_id = pl.legal_entity_id
        AND l.id = pl.payroll_liability_id
       SET pl.beneficiary_snapshot_status =
         CASE
           WHEN UPPER(COALESCE(l.beneficiary_type, '')) = 'EMPLOYEE' THEN 'PENDING'
           ELSE 'NOT_REQUIRED'
         END`
    );
  },

  async down(connection) {
    await safeExecute(
      connection,
      `DROP TABLE IF EXISTS payroll_beneficiary_bank_account_audit`
    );
    await safeExecute(
      connection,
      `DROP TABLE IF EXISTS payroll_beneficiary_bank_snapshots`
    );
    await safeExecute(
      connection,
      `DROP TABLE IF EXISTS payroll_beneficiary_bank_accounts`
    );
    // ALTER rollback intentionally omitted for dev simplicity.
  },
};

export default migration045PayrollBeneficiarySnapshots;
