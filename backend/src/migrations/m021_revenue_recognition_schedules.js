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
  CREATE TABLE IF NOT EXISTS revenue_recognition_schedules (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    fiscal_period_id BIGINT UNSIGNED NOT NULL,
    source_event_uid VARCHAR(160) NOT NULL,
    status ENUM('DRAFT','READY','POSTED','REVERSED') NOT NULL DEFAULT 'DRAFT',
    account_family ENUM('DEFREV','ACCRUED_REVENUE','ACCRUED_EXPENSE','PREPAID_EXPENSE') NOT NULL,
    maturity_bucket ENUM('SHORT_TERM','LONG_TERM') NOT NULL,
    maturity_date DATE NOT NULL,
    reclass_required BOOLEAN NOT NULL DEFAULT FALSE,
    currency_code CHAR(3) NOT NULL,
    fx_rate DECIMAL(20,10) NULL,
    amount_txn DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    amount_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    period_start_date DATE NOT NULL,
    period_end_date DATE NOT NULL,
    created_by_user_id INT NOT NULL,
    posted_journal_entry_id BIGINT UNSIGNED NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_revrec_sched_source_uid (tenant_id, legal_entity_id, source_event_uid),
    UNIQUE KEY uk_revrec_sched_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_revrec_sched_tenant_entity_id (tenant_id, legal_entity_id, id),
    KEY ix_revrec_sched_scope (tenant_id, legal_entity_id, fiscal_period_id, status),
    KEY ix_revrec_sched_family_bucket (
      tenant_id,
      legal_entity_id,
      account_family,
      maturity_bucket,
      maturity_date
    ),
    KEY ix_revrec_sched_creator_tenant_user (tenant_id, created_by_user_id),
    CONSTRAINT fk_revrec_sched_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_revrec_sched_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_revrec_sched_period
      FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
    CONSTRAINT fk_revrec_sched_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_revrec_sched_creator_user
      FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
    CONSTRAINT fk_revrec_sched_posted_journal_tenant
      FOREIGN KEY (tenant_id, posted_journal_entry_id) REFERENCES journal_entries(tenant_id, id),
    CHECK (fx_rate IS NULL OR fx_rate > 0),
    CHECK (period_end_date >= period_start_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS revenue_recognition_schedule_lines (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    schedule_id BIGINT UNSIGNED NOT NULL,
    line_no INT UNSIGNED NOT NULL,
    source_row_uid VARCHAR(160) NOT NULL,
    source_contract_id BIGINT UNSIGNED NULL,
    source_contract_line_id BIGINT UNSIGNED NULL,
    source_cari_document_id BIGINT UNSIGNED NULL,
    status ENUM('OPEN','SETTLED','REVERSED','CLOSED') NOT NULL DEFAULT 'OPEN',
    account_family ENUM('DEFREV','ACCRUED_REVENUE','ACCRUED_EXPENSE','PREPAID_EXPENSE') NOT NULL,
    maturity_bucket ENUM('SHORT_TERM','LONG_TERM') NOT NULL,
    maturity_date DATE NOT NULL,
    reclass_required BOOLEAN NOT NULL DEFAULT FALSE,
    currency_code CHAR(3) NOT NULL,
    fx_rate DECIMAL(20,10) NULL,
    amount_txn DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    amount_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    period_start_date DATE NOT NULL,
    period_end_date DATE NOT NULL,
    fiscal_period_id BIGINT UNSIGNED NOT NULL,
    created_by_user_id INT NOT NULL,
    posted_journal_entry_id BIGINT UNSIGNED NULL,
    posted_journal_line_id BIGINT UNSIGNED NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_revrec_sched_line_source_uid (tenant_id, legal_entity_id, source_row_uid),
    UNIQUE KEY uk_revrec_sched_lines_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_revrec_sched_lines_tenant_entity_id (tenant_id, legal_entity_id, id),
    UNIQUE KEY uk_revrec_sched_line_no (tenant_id, schedule_id, line_no),
    KEY ix_revrec_sched_lines_scope (
      tenant_id,
      legal_entity_id,
      schedule_id,
      status,
      account_family
    ),
    KEY ix_revrec_sched_lines_posted_journal (tenant_id, posted_journal_entry_id),
    KEY ix_revrec_sched_lines_creator_tenant_user (tenant_id, created_by_user_id),
    CONSTRAINT fk_revrec_sched_lines_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_revrec_sched_lines_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_revrec_sched_lines_schedule_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, schedule_id)
      REFERENCES revenue_recognition_schedules(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_revrec_sched_lines_contract_tenant
      FOREIGN KEY (tenant_id, source_contract_id) REFERENCES contracts(tenant_id, id),
    CONSTRAINT fk_revrec_sched_lines_contract_line_tenant
      FOREIGN KEY (tenant_id, source_contract_line_id) REFERENCES contract_lines(tenant_id, id),
    CONSTRAINT fk_revrec_sched_lines_cari_doc_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, source_cari_document_id)
      REFERENCES cari_documents(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_revrec_sched_lines_period
      FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
    CONSTRAINT fk_revrec_sched_lines_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_revrec_sched_lines_creator_user
      FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
    CONSTRAINT fk_revrec_sched_lines_posted_journal_tenant
      FOREIGN KEY (tenant_id, posted_journal_entry_id) REFERENCES journal_entries(tenant_id, id),
    CONSTRAINT fk_revrec_sched_lines_posted_jline
      FOREIGN KEY (posted_journal_line_id) REFERENCES journal_lines(id),
    CHECK (fx_rate IS NULL OR fx_rate > 0),
    CHECK (period_end_date >= period_start_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS revenue_recognition_runs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    schedule_id BIGINT UNSIGNED NULL,
    fiscal_period_id BIGINT UNSIGNED NOT NULL,
    run_no VARCHAR(80) NOT NULL,
    source_run_uid VARCHAR(160) NOT NULL,
    status ENUM('DRAFT','READY','POSTED','REVERSED') NOT NULL DEFAULT 'DRAFT',
    account_family ENUM('DEFREV','ACCRUED_REVENUE','ACCRUED_EXPENSE','PREPAID_EXPENSE') NOT NULL,
    maturity_bucket ENUM('SHORT_TERM','LONG_TERM') NOT NULL,
    maturity_date DATE NOT NULL,
    reclass_required BOOLEAN NOT NULL DEFAULT FALSE,
    currency_code CHAR(3) NOT NULL,
    fx_rate DECIMAL(20,10) NULL,
    total_amount_txn DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    total_amount_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    period_start_date DATE NOT NULL,
    period_end_date DATE NOT NULL,
    reversal_of_run_id BIGINT UNSIGNED NULL,
    posted_journal_entry_id BIGINT UNSIGNED NULL,
    reversal_journal_entry_id BIGINT UNSIGNED NULL,
    created_by_user_id INT NOT NULL,
    posted_by_user_id INT NULL,
    reversed_by_user_id INT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    posted_at TIMESTAMP NULL,
    reversed_at TIMESTAMP NULL,
    UNIQUE KEY uk_revrec_runs_source_uid (tenant_id, legal_entity_id, source_run_uid),
    UNIQUE KEY uk_revrec_runs_run_no (tenant_id, legal_entity_id, run_no),
    UNIQUE KEY uk_revrec_runs_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_revrec_runs_tenant_entity_id (tenant_id, legal_entity_id, id),
    KEY ix_revrec_runs_scope (tenant_id, legal_entity_id, fiscal_period_id, status),
    KEY ix_revrec_runs_schedule (tenant_id, legal_entity_id, schedule_id),
    KEY ix_revrec_runs_posted_journal (tenant_id, posted_journal_entry_id),
    CONSTRAINT fk_revrec_runs_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_revrec_runs_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_revrec_runs_schedule_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, schedule_id)
      REFERENCES revenue_recognition_schedules(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_revrec_runs_period
      FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
    CONSTRAINT fk_revrec_runs_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_revrec_runs_reversal_tenant
      FOREIGN KEY (tenant_id, reversal_of_run_id) REFERENCES revenue_recognition_runs(tenant_id, id),
    CONSTRAINT fk_revrec_runs_posted_journal_tenant
      FOREIGN KEY (tenant_id, posted_journal_entry_id) REFERENCES journal_entries(tenant_id, id),
    CONSTRAINT fk_revrec_runs_reversal_journal_tenant
      FOREIGN KEY (tenant_id, reversal_journal_entry_id) REFERENCES journal_entries(tenant_id, id),
    CONSTRAINT fk_revrec_runs_creator_user
      FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
    CONSTRAINT fk_revrec_runs_posted_by_user
      FOREIGN KEY (tenant_id, posted_by_user_id) REFERENCES users(tenant_id, id),
    CONSTRAINT fk_revrec_runs_reversed_by_user
      FOREIGN KEY (tenant_id, reversed_by_user_id) REFERENCES users(tenant_id, id),
    CHECK (fx_rate IS NULL OR fx_rate > 0),
    CHECK (period_end_date >= period_start_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS revenue_recognition_run_lines (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    schedule_line_id BIGINT UNSIGNED NULL,
    line_no INT UNSIGNED NOT NULL,
    source_row_uid VARCHAR(160) NOT NULL,
    status ENUM('OPEN','POSTED','REVERSED','SETTLED','CLOSED') NOT NULL DEFAULT 'OPEN',
    account_family ENUM('DEFREV','ACCRUED_REVENUE','ACCRUED_EXPENSE','PREPAID_EXPENSE') NOT NULL,
    maturity_bucket ENUM('SHORT_TERM','LONG_TERM') NOT NULL,
    maturity_date DATE NOT NULL,
    reclass_required BOOLEAN NOT NULL DEFAULT FALSE,
    currency_code CHAR(3) NOT NULL,
    fx_rate DECIMAL(20,10) NULL,
    amount_txn DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    amount_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    fiscal_period_id BIGINT UNSIGNED NOT NULL,
    period_start_date DATE NOT NULL,
    period_end_date DATE NOT NULL,
    reversal_of_run_line_id BIGINT UNSIGNED NULL,
    posted_journal_entry_id BIGINT UNSIGNED NULL,
    posted_journal_line_id BIGINT UNSIGNED NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_revrec_run_line_source_uid (tenant_id, legal_entity_id, source_row_uid),
    UNIQUE KEY uk_revrec_run_lines_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_revrec_run_lines_tenant_entity_id (tenant_id, legal_entity_id, id),
    UNIQUE KEY uk_revrec_run_line_no (tenant_id, run_id, line_no),
    KEY ix_revrec_run_lines_scope (tenant_id, legal_entity_id, run_id, status),
    KEY ix_revrec_run_lines_schedule_line (tenant_id, legal_entity_id, schedule_line_id),
    KEY ix_revrec_run_lines_posted_journal (tenant_id, posted_journal_entry_id),
    CONSTRAINT fk_revrec_run_lines_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_revrec_run_lines_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_revrec_run_lines_run_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, run_id)
      REFERENCES revenue_recognition_runs(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_revrec_run_lines_schedule_line_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, schedule_line_id)
      REFERENCES revenue_recognition_schedule_lines(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_revrec_run_lines_period
      FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
    CONSTRAINT fk_revrec_run_lines_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_revrec_run_lines_reversal_tenant
      FOREIGN KEY (tenant_id, reversal_of_run_line_id)
      REFERENCES revenue_recognition_run_lines(tenant_id, id),
    CONSTRAINT fk_revrec_run_lines_posted_journal_tenant
      FOREIGN KEY (tenant_id, posted_journal_entry_id) REFERENCES journal_entries(tenant_id, id),
    CONSTRAINT fk_revrec_run_lines_posted_jline
      FOREIGN KEY (posted_journal_line_id) REFERENCES journal_lines(id),
    CHECK (fx_rate IS NULL OR fx_rate > 0),
    CHECK (period_end_date >= period_start_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS revenue_recognition_subledger_entries (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    run_line_id BIGINT UNSIGNED NULL,
    schedule_line_id BIGINT UNSIGNED NULL,
    entry_no INT UNSIGNED NOT NULL,
    source_row_uid VARCHAR(160) NOT NULL,
    entry_kind ENUM('RECOGNITION','RECLASS','REVERSAL','SETTLEMENT') NOT NULL DEFAULT 'RECOGNITION',
    status ENUM('OPEN','POSTED','REVERSED','SETTLED','CLOSED') NOT NULL DEFAULT 'OPEN',
    account_family ENUM('DEFREV','ACCRUED_REVENUE','ACCRUED_EXPENSE','PREPAID_EXPENSE') NOT NULL,
    maturity_bucket ENUM('SHORT_TERM','LONG_TERM') NOT NULL,
    maturity_date DATE NOT NULL,
    reclass_required BOOLEAN NOT NULL DEFAULT FALSE,
    currency_code CHAR(3) NOT NULL,
    fx_rate DECIMAL(20,10) NULL,
    amount_txn DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    amount_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    fiscal_period_id BIGINT UNSIGNED NOT NULL,
    period_start_date DATE NOT NULL,
    period_end_date DATE NOT NULL,
    reversal_of_subledger_entry_id BIGINT UNSIGNED NULL,
    posted_journal_entry_id BIGINT UNSIGNED NULL,
    posted_journal_line_id BIGINT UNSIGNED NULL,
    created_by_user_id INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_revrec_subledger_source_uid (tenant_id, legal_entity_id, source_row_uid),
    UNIQUE KEY uk_revrec_subledger_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_revrec_subledger_tenant_entity_id (tenant_id, legal_entity_id, id),
    UNIQUE KEY uk_revrec_subledger_entry_no (tenant_id, run_id, entry_no),
    KEY ix_revrec_subledger_scope (
      tenant_id,
      legal_entity_id,
      fiscal_period_id,
      account_family,
      maturity_bucket,
      status
    ),
    KEY ix_revrec_subledger_run_line (tenant_id, legal_entity_id, run_line_id),
    KEY ix_revrec_subledger_schedule_line (tenant_id, legal_entity_id, schedule_line_id),
    KEY ix_revrec_subledger_posted_journal (tenant_id, posted_journal_entry_id),
    KEY ix_revrec_subledger_creator_tenant_user (tenant_id, created_by_user_id),
    CONSTRAINT fk_revrec_subledger_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_revrec_subledger_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_revrec_subledger_run_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, run_id)
      REFERENCES revenue_recognition_runs(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_revrec_subledger_run_line_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, run_line_id)
      REFERENCES revenue_recognition_run_lines(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_revrec_subledger_schedule_line_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, schedule_line_id)
      REFERENCES revenue_recognition_schedule_lines(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_revrec_subledger_period
      FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
    CONSTRAINT fk_revrec_subledger_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_revrec_subledger_reversal_tenant
      FOREIGN KEY (tenant_id, reversal_of_subledger_entry_id)
      REFERENCES revenue_recognition_subledger_entries(tenant_id, id),
    CONSTRAINT fk_revrec_subledger_posted_journal_tenant
      FOREIGN KEY (tenant_id, posted_journal_entry_id) REFERENCES journal_entries(tenant_id, id),
    CONSTRAINT fk_revrec_subledger_posted_jline
      FOREIGN KEY (posted_journal_line_id) REFERENCES journal_lines(id),
    CONSTRAINT fk_revrec_subledger_creator_user
      FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
    CHECK (fx_rate IS NULL OR fx_rate > 0),
    CHECK (period_end_date >= period_start_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
];

const migration021RevenueRecognitionSchedules = {
  key: "m021_revenue_recognition_schedules",
  description: "Revenue recognition foundation schema (schedules, runs, subledger)",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration021RevenueRecognitionSchedules;
