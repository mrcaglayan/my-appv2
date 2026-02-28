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

const migration042PayrollPaymentSettlementSync = {
  key: "m042_payroll_payment_settlement_sync",
  description:
    "Payroll payment settlement sync evidence, link sync state, and run sync timestamps (PR-P04)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_liability_settlements (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         settlement_key VARCHAR(190) NOT NULL,
         run_id BIGINT UNSIGNED NOT NULL,
         payroll_liability_id BIGINT UNSIGNED NOT NULL,
         payroll_liability_payment_link_id BIGINT UNSIGNED NOT NULL,
         payment_batch_id BIGINT UNSIGNED NOT NULL,
         payment_batch_line_id BIGINT UNSIGNED NULL,
         bank_statement_line_id BIGINT UNSIGNED NULL,
         settlement_source ENUM('B04_ONLY','B03_RECON','MANUAL_OVERRIDE') NOT NULL,
         settled_amount DECIMAL(20,6) NOT NULL,
         currency_code CHAR(3) NOT NULL,
         settled_at TIMESTAMP NOT NULL,
         payload_json JSON NULL,
         created_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_liability_settlements_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payroll_liability_settlements_scope_key (tenant_id, legal_entity_id, settlement_key),
         KEY ix_payroll_liability_settlements_scope_run (tenant_id, legal_entity_id, run_id),
         KEY ix_payroll_liability_settlements_scope_liability (tenant_id, legal_entity_id, payroll_liability_id),
         KEY ix_payroll_liability_settlements_scope_link (tenant_id, legal_entity_id, payroll_liability_payment_link_id),
         KEY ix_payroll_liability_settlements_scope_batch (tenant_id, legal_entity_id, payment_batch_id),
         KEY ix_payroll_liability_settlements_scope_batch_line (tenant_id, legal_entity_id, payment_batch_line_id),
         KEY ix_payroll_liability_settlements_scope_stmt_line (tenant_id, legal_entity_id, bank_statement_line_id),
         KEY ix_payroll_liability_settlements_scope_time (tenant_id, legal_entity_id, settled_at),
         CONSTRAINT fk_payroll_liability_settlements_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_liability_settlements_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_liability_settlements_run
           FOREIGN KEY (tenant_id, legal_entity_id, run_id)
           REFERENCES payroll_runs(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_settlements_liability
           FOREIGN KEY (tenant_id, legal_entity_id, payroll_liability_id)
           REFERENCES payroll_run_liabilities(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_settlements_link
           FOREIGN KEY (tenant_id, legal_entity_id, payroll_liability_payment_link_id)
           REFERENCES payroll_liability_payment_links(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_settlements_batch
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_id)
           REFERENCES payment_batches(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_settlements_batch_line
           FOREIGN KEY (tenant_id, legal_entity_id, payment_batch_line_id)
           REFERENCES payment_batch_lines(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_settlements_stmt_line
           FOREIGN KEY (tenant_id, legal_entity_id, bank_statement_line_id)
           REFERENCES bank_statement_lines(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_settlements_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code),
         CONSTRAINT fk_payroll_liability_settlements_user_tenant
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_payment_links
         ADD COLUMN settled_amount DECIMAL(20,6) NOT NULL DEFAULT 0 AFTER allocated_amount`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_payment_links
         ADD COLUMN settled_at TIMESTAMP NULL AFTER settled_amount`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_payment_links
         ADD COLUMN released_at TIMESTAMP NULL AFTER settled_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_payment_links
         ADD COLUMN last_sync_at TIMESTAMP NULL AFTER released_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_payment_links
         ADD COLUMN sync_note VARCHAR(255) NULL AFTER last_sync_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_payment_links
         ADD KEY ix_payroll_liability_payment_links_scope_settled_at
           (tenant_id, legal_entity_id, settled_at)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_payment_links
         ADD KEY ix_payroll_liability_payment_links_scope_last_sync_at
           (tenant_id, legal_entity_id, last_sync_at)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD COLUMN paid_payment_batch_id BIGINT UNSIGNED NULL AFTER paid_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD COLUMN paid_payment_batch_line_id BIGINT UNSIGNED NULL AFTER paid_payment_batch_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD COLUMN paid_bank_statement_line_id BIGINT UNSIGNED NULL AFTER paid_payment_batch_line_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD KEY ix_payroll_run_liabilities_scope_paid_batch
           (tenant_id, legal_entity_id, paid_payment_batch_id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD KEY ix_payroll_run_liabilities_scope_paid_batch_line
           (tenant_id, legal_entity_id, paid_payment_batch_line_id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD KEY ix_payroll_run_liabilities_scope_paid_stmt_line
           (tenant_id, legal_entity_id, paid_bank_statement_line_id)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN payment_sync_last_preview_at TIMESTAMP NULL AFTER liabilities_built_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN payment_sync_last_applied_at TIMESTAMP NULL AFTER payment_sync_last_preview_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD KEY ix_payroll_runs_payment_sync_last_applied_at
           (tenant_id, legal_entity_id, payment_sync_last_applied_at)`
    );
  },

  async down(connection) {
    await safeExecute(connection, `DROP TABLE IF EXISTS payroll_liability_settlements`);
    // Alter rollback intentionally omitted for dev safety/compatibility.
  },
};

export default migration042PayrollPaymentSettlementSync;
