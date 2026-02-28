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
  CREATE TABLE IF NOT EXISTS period_close_runs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    book_id BIGINT UNSIGNED NOT NULL,
    fiscal_period_id BIGINT UNSIGNED NOT NULL,
    next_fiscal_period_id BIGINT UNSIGNED NULL,
    run_hash CHAR(64) NOT NULL,
    close_status ENUM('SOFT_CLOSED','HARD_CLOSED') NOT NULL DEFAULT 'SOFT_CLOSED',
    status ENUM('IN_PROGRESS','COMPLETED','FAILED','REOPENED') NOT NULL DEFAULT 'IN_PROGRESS',
    year_end_closed BOOLEAN NOT NULL DEFAULT FALSE,
    retained_earnings_account_id BIGINT UNSIGNED NULL,
    carry_forward_journal_entry_id BIGINT UNSIGNED NULL,
    year_end_journal_entry_id BIGINT UNSIGNED NULL,
    source_journal_count INT NOT NULL DEFAULT 0,
    source_debit_total DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    source_credit_total DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    started_by_user_id INT NOT NULL,
    completed_by_user_id INT NULL,
    reopened_by_user_id INT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    reopened_at TIMESTAMP NULL,
    note VARCHAR(500) NULL,
    metadata_json JSON NULL,
    UNIQUE KEY uk_period_close_run_hash (book_id, fiscal_period_id, run_hash),
    KEY ix_period_close_runs_tenant_book_period (tenant_id, book_id, fiscal_period_id),
    KEY ix_period_close_runs_tenant_status (tenant_id, status),
    CONSTRAINT fk_period_close_runs_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_period_close_runs_book
      FOREIGN KEY (book_id) REFERENCES books(id),
    CONSTRAINT fk_period_close_runs_period
      FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
    CONSTRAINT fk_period_close_runs_next_period
      FOREIGN KEY (next_fiscal_period_id) REFERENCES fiscal_periods(id),
    CONSTRAINT fk_period_close_runs_retained_acc
      FOREIGN KEY (retained_earnings_account_id) REFERENCES accounts(id),
    CONSTRAINT fk_period_close_runs_carry_journal
      FOREIGN KEY (carry_forward_journal_entry_id) REFERENCES journal_entries(id),
    CONSTRAINT fk_period_close_runs_year_end_journal
      FOREIGN KEY (year_end_journal_entry_id) REFERENCES journal_entries(id),
    CONSTRAINT fk_period_close_runs_started_by
      FOREIGN KEY (started_by_user_id) REFERENCES users(id),
    CONSTRAINT fk_period_close_runs_completed_by
      FOREIGN KEY (completed_by_user_id) REFERENCES users(id),
    CONSTRAINT fk_period_close_runs_reopened_by
      FOREIGN KEY (reopened_by_user_id) REFERENCES users(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS period_close_run_lines (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    period_close_run_id BIGINT UNSIGNED NOT NULL,
    tenant_id BIGINT UNSIGNED NOT NULL,
    line_type ENUM('CARRY_FORWARD','YEAR_END') NOT NULL,
    account_id BIGINT UNSIGNED NOT NULL,
    closing_balance DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    debit_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    credit_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_period_close_run_line (period_close_run_id, line_type, account_id),
    KEY ix_period_close_run_lines_tenant_type (tenant_id, line_type),
    CONSTRAINT fk_period_close_run_lines_run
      FOREIGN KEY (period_close_run_id) REFERENCES period_close_runs(id),
    CONSTRAINT fk_period_close_run_lines_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_period_close_run_lines_account
      FOREIGN KEY (account_id) REFERENCES accounts(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
];

const migration005PeriodCloseRuns = {
  key: "m005_period_close_runs",
  description:
    "Period close run orchestration and line-level audit trail tables",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration005PeriodCloseRuns;
