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

const migration043PayrollCorrectionsReversals = {
  key: "m043_payroll_corrections_reversals",
  description:
    "Payroll corrections and reversals (run type, shell links, safe reversal metadata) (PR-P05)",
  async up(connection) {
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         MODIFY COLUMN status ENUM('DRAFT','IMPORTED','REVIEWED','FINALIZED')
           NOT NULL DEFAULT 'IMPORTED'`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN run_type ENUM('REGULAR','REVERSAL','OFF_CYCLE','RETRO')
           NOT NULL DEFAULT 'REGULAR' AFTER status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN correction_of_run_id BIGINT UNSIGNED NULL AFTER run_type`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN correction_reason VARCHAR(500) NULL AFTER correction_of_run_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN is_reversed TINYINT(1) NOT NULL DEFAULT 0 AFTER correction_reason`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN reversed_by_run_id BIGINT UNSIGNED NULL AFTER is_reversed`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD COLUMN reversed_at TIMESTAMP NULL AFTER reversed_by_run_id`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD KEY ix_payroll_runs_scope_run_type (tenant_id, legal_entity_id, run_type)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD KEY ix_payroll_runs_scope_is_reversed (tenant_id, legal_entity_id, is_reversed)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD KEY ix_payroll_runs_scope_correction_of
           (tenant_id, legal_entity_id, correction_of_run_id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD KEY ix_payroll_runs_scope_reversed_by
           (tenant_id, legal_entity_id, reversed_by_run_id)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD CONSTRAINT fk_payroll_runs_correction_of_run
           FOREIGN KEY (tenant_id, legal_entity_id, correction_of_run_id)
           REFERENCES payroll_runs(tenant_id, legal_entity_id, id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_runs
         ADD CONSTRAINT fk_payroll_runs_reversed_by_run
           FOREIGN KEY (tenant_id, legal_entity_id, reversed_by_run_id)
           REFERENCES payroll_runs(tenant_id, legal_entity_id, id)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_audit
         MODIFY COLUMN action ENUM(
           'IMPORTED',
           'STATUS',
           'VALIDATION',
           'REVERSED',
           'CREATED_AS_REVERSAL',
           'CORRECTION_SHELL_CREATED',
           'IMPORTED_TO_CORRECTION_SHELL'
         ) NOT NULL`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_run_corrections (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         original_run_id BIGINT UNSIGNED NULL,
         correction_run_id BIGINT UNSIGNED NOT NULL,
         correction_type ENUM('REVERSAL','OFF_CYCLE','RETRO') NOT NULL,
         status ENUM('CREATED','APPLIED','CANCELLED') NOT NULL DEFAULT 'CREATED',
         idempotency_key VARCHAR(190) NULL,
         notes VARCHAR(500) NULL,
         created_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
           ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_run_corrections_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payroll_run_corrections_scope_correction_run
           (tenant_id, legal_entity_id, correction_run_id),
         UNIQUE KEY uk_payroll_run_corrections_scope_idempotency
           (tenant_id, legal_entity_id, idempotency_key),
         KEY ix_payroll_run_corrections_scope_original_run
           (tenant_id, legal_entity_id, original_run_id),
         KEY ix_payroll_run_corrections_scope_type
           (tenant_id, legal_entity_id, correction_type),
         KEY ix_payroll_run_corrections_scope_status
           (tenant_id, legal_entity_id, status),
         KEY ix_payroll_run_corrections_scope_time
           (tenant_id, legal_entity_id, created_at),
         CONSTRAINT fk_payroll_run_corrections_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_run_corrections_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_run_corrections_original_run
           FOREIGN KEY (tenant_id, legal_entity_id, original_run_id)
           REFERENCES payroll_runs(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_run_corrections_correction_run
           FOREIGN KEY (tenant_id, legal_entity_id, correction_run_id)
           REFERENCES payroll_runs(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_run_corrections_created_user_tenant
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD COLUMN cancelled_at TIMESTAMP NULL AFTER paid_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD COLUMN cancelled_reason VARCHAR(255) NULL AFTER cancelled_at`
    );
  },

  async down(connection) {
    await safeExecute(connection, `DROP TABLE IF EXISTS payroll_run_corrections`);
    // Alter rollback intentionally omitted for dev simplicity.
  },
};

export default migration043PayrollCorrectionsReversals;

