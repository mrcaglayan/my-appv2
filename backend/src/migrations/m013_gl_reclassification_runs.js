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
  CREATE TABLE IF NOT EXISTS gl_reclassification_runs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    book_id BIGINT UNSIGNED NOT NULL,
    fiscal_period_id BIGINT UNSIGNED NOT NULL,
    source_account_id BIGINT UNSIGNED NOT NULL,
    source_balance DECIMAL(20,6) NOT NULL,
    source_balance_side ENUM('DEBIT','CREDIT') NOT NULL,
    reclass_amount DECIMAL(20,6) NOT NULL,
    allocation_mode ENUM('PERCENT','AMOUNT') NOT NULL,
    entry_date DATE NOT NULL,
    document_date DATE NOT NULL,
    currency_code CHAR(3) NOT NULL,
    journal_entry_id BIGINT UNSIGNED NULL,
    status ENUM('DRAFT_CREATED','FAILED') NOT NULL DEFAULT 'DRAFT_CREATED',
    description VARCHAR(500) NULL,
    reference_no VARCHAR(100) NULL,
    note VARCHAR(500) NULL,
    metadata_json JSON NULL,
    created_by_user_id INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY ix_gl_reclass_runs_tenant_entity (tenant_id, legal_entity_id, created_at),
    KEY ix_gl_reclass_runs_tenant_book_period (tenant_id, book_id, fiscal_period_id),
    KEY ix_gl_reclass_runs_tenant_journal (tenant_id, journal_entry_id),
    CONSTRAINT fk_gl_reclass_runs_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_gl_reclass_runs_legal_entity
      FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_gl_reclass_runs_book
      FOREIGN KEY (book_id) REFERENCES books(id),
    CONSTRAINT fk_gl_reclass_runs_period
      FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
    CONSTRAINT fk_gl_reclass_runs_source_account
      FOREIGN KEY (source_account_id) REFERENCES accounts(id),
    CONSTRAINT fk_gl_reclass_runs_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_gl_reclass_runs_journal
      FOREIGN KEY (journal_entry_id) REFERENCES journal_entries(id),
    CONSTRAINT fk_gl_reclass_runs_user
      FOREIGN KEY (created_by_user_id) REFERENCES users(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS gl_reclassification_run_targets (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    reclassification_run_id BIGINT UNSIGNED NOT NULL,
    tenant_id BIGINT UNSIGNED NOT NULL,
    target_account_id BIGINT UNSIGNED NOT NULL,
    allocation_pct DECIMAL(9,6) NULL,
    allocation_amount DECIMAL(20,6) NULL,
    applied_amount DECIMAL(20,6) NOT NULL,
    debit_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    credit_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_gl_reclass_run_target (reclassification_run_id, target_account_id),
    KEY ix_gl_reclass_run_targets_tenant (tenant_id, created_at),
    CONSTRAINT fk_gl_reclass_targets_run
      FOREIGN KEY (reclassification_run_id) REFERENCES gl_reclassification_runs(id),
    CONSTRAINT fk_gl_reclass_targets_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_gl_reclass_targets_account
      FOREIGN KEY (target_account_id) REFERENCES accounts(id),
    CHECK ((debit_base > 0 AND credit_base = 0) OR (credit_base > 0 AND debit_base = 0))
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
];

const migration013GlReclassificationRuns = {
  key: "m013_gl_reclassification_runs",
  description:
    "GL balance reclassification run audit tables with target allocation details",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration013GlReclassificationRuns;

