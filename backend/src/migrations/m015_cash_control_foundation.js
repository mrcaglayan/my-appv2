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
  ALTER TABLE accounts
  ADD COLUMN is_cash_controlled BOOLEAN NOT NULL DEFAULT FALSE
  `,
  `
  ALTER TABLE accounts
  ADD INDEX ix_accounts_cash_controlled (is_cash_controlled)
  `,
  `
  ALTER TABLE journal_entries
  MODIFY COLUMN source_type
    ENUM('MANUAL','SYSTEM','INTERCOMPANY','ELIMINATION','ADJUSTMENT','CASH')
    NOT NULL DEFAULT 'MANUAL'
  `,
  `
  CREATE TABLE IF NOT EXISTS cash_registers (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    operating_unit_id BIGINT UNSIGNED NULL,
    account_id BIGINT UNSIGNED NOT NULL,
    code VARCHAR(60) NOT NULL,
    name VARCHAR(255) NOT NULL,
    register_type ENUM('VAULT','DRAWER','TILL') NOT NULL DEFAULT 'DRAWER',
    session_mode ENUM('REQUIRED','OPTIONAL','NONE') NOT NULL DEFAULT 'REQUIRED',
    currency_code CHAR(3) NOT NULL,
    status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
    allow_negative BOOLEAN NOT NULL DEFAULT FALSE,
    variance_gain_account_id BIGINT UNSIGNED NULL,
    variance_loss_account_id BIGINT UNSIGNED NULL,
    max_txn_amount DECIMAL(20,6) NULL,
    requires_approval_over_amount DECIMAL(20,6) NULL,
    created_by_user_id INT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_cash_register_tenant_code (tenant_id, code),
    UNIQUE KEY uk_cash_register_account (account_id),
    KEY ix_cash_register_tenant_entity (tenant_id, legal_entity_id, status),
    KEY ix_cash_register_tenant_ou (tenant_id, operating_unit_id, status),
    CONSTRAINT fk_cash_register_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_cash_register_legal_entity
      FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_cash_register_operating_unit
      FOREIGN KEY (operating_unit_id) REFERENCES operating_units(id),
    CONSTRAINT fk_cash_register_account
      FOREIGN KEY (account_id) REFERENCES accounts(id),
    CONSTRAINT fk_cash_register_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_cash_register_variance_gain_account
      FOREIGN KEY (variance_gain_account_id) REFERENCES accounts(id),
    CONSTRAINT fk_cash_register_variance_loss_account
      FOREIGN KEY (variance_loss_account_id) REFERENCES accounts(id),
    CONSTRAINT fk_cash_register_created_by
      FOREIGN KEY (created_by_user_id) REFERENCES users(id),
    CHECK (max_txn_amount IS NULL OR max_txn_amount > 0),
    CHECK (requires_approval_over_amount IS NULL OR requires_approval_over_amount >= 0),
    CHECK (
      max_txn_amount IS NULL
      OR requires_approval_over_amount IS NULL
      OR requires_approval_over_amount <= max_txn_amount
    )
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS cash_sessions (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    cash_register_id BIGINT UNSIGNED NOT NULL,
    status ENUM('OPEN','CLOSED','CANCELLED') NOT NULL DEFAULT 'OPEN',
    opening_amount DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    expected_closing_amount DECIMAL(20,6) NULL,
    counted_closing_amount DECIMAL(20,6) NULL,
    variance_amount DECIMAL(20,6) NULL,
    opened_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    opened_by_user_id INT NOT NULL,
    closed_at TIMESTAMP NULL,
    closed_by_user_id INT NULL,
    closed_reason ENUM('END_SHIFT','FORCED_CLOSE','COUNT_CORRECTION') NULL,
    close_note VARCHAR(500) NULL,
    approved_by_user_id INT NULL,
    approved_at TIMESTAMP NULL,
    open_session_register_id BIGINT UNSIGNED
      GENERATED ALWAYS AS (
        CASE
          WHEN status = 'OPEN' THEN cash_register_id
          ELSE NULL
        END
      ) STORED,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_cash_sessions_one_open_per_register (open_session_register_id),
    KEY ix_cash_sessions_tenant_register_status (tenant_id, cash_register_id, status),
    KEY ix_cash_sessions_tenant_opened_at (tenant_id, opened_at),
    CONSTRAINT fk_cash_session_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_cash_session_register
      FOREIGN KEY (cash_register_id) REFERENCES cash_registers(id),
    CONSTRAINT fk_cash_session_opened_by
      FOREIGN KEY (opened_by_user_id) REFERENCES users(id),
    CONSTRAINT fk_cash_session_closed_by
      FOREIGN KEY (closed_by_user_id) REFERENCES users(id),
    CONSTRAINT fk_cash_session_approved_by
      FOREIGN KEY (approved_by_user_id) REFERENCES users(id),
    CHECK (opening_amount >= 0),
    CHECK (expected_closing_amount IS NULL OR expected_closing_amount >= 0),
    CHECK (counted_closing_amount IS NULL OR counted_closing_amount >= 0),
    CHECK (
      (status = 'OPEN' AND closed_at IS NULL)
      OR (status IN ('CLOSED','CANCELLED') AND closed_at IS NOT NULL)
    )
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS cash_transactions (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    cash_register_id BIGINT UNSIGNED NOT NULL,
    cash_session_id BIGINT UNSIGNED NULL,
    txn_no VARCHAR(60) NOT NULL,
    txn_type ENUM(
      'RECEIPT',
      'PAYOUT',
      'DEPOSIT_TO_BANK',
      'WITHDRAWAL_FROM_BANK',
      'TRANSFER_OUT',
      'TRANSFER_IN',
      'VARIANCE',
      'OPENING_FLOAT',
      'CLOSING_ADJUSTMENT'
    ) NOT NULL,
    status ENUM('DRAFT','SUBMITTED','APPROVED','POSTED','REVERSED','CANCELLED') NOT NULL DEFAULT 'DRAFT',
    txn_datetime DATETIME NOT NULL,
    book_date DATE NOT NULL,
    amount DECIMAL(20,6) NOT NULL,
    currency_code CHAR(3) NOT NULL,
    description VARCHAR(500) NULL,
    reference_no VARCHAR(100) NULL,
    source_doc_type ENUM(
      'AP_PAYMENT',
      'AR_RECEIPT',
      'EXPENSE_CLAIM',
      'PETTY_CASH_VOUCHER',
      'BANK_DEPOSIT_SLIP',
      'OTHER'
    ) NULL,
    source_doc_id VARCHAR(80) NULL,
    counterparty_type ENUM('CUSTOMER','VENDOR','EMPLOYEE','LEGAL_ENTITY','OTHER') NULL,
    counterparty_id BIGINT UNSIGNED NULL,
    counter_account_id BIGINT UNSIGNED NULL,
    counter_cash_register_id BIGINT UNSIGNED NULL,
    posted_journal_entry_id BIGINT UNSIGNED NULL,
    reversal_of_transaction_id BIGINT UNSIGNED NULL,
    cancel_reason VARCHAR(255) NULL,
    override_cash_control BOOLEAN NOT NULL DEFAULT FALSE,
    override_reason VARCHAR(500) NULL,
    idempotency_key VARCHAR(100) NULL,
    created_by_user_id INT NOT NULL,
    submitted_by_user_id INT NULL,
    approved_by_user_id INT NULL,
    posted_by_user_id INT NULL,
    reversed_by_user_id INT NULL,
    cancelled_by_user_id INT NULL,
    submitted_at TIMESTAMP NULL,
    approved_at TIMESTAMP NULL,
    posted_at TIMESTAMP NULL,
    reversed_at TIMESTAMP NULL,
    cancelled_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_cash_txn_tenant_register_txn_no (tenant_id, cash_register_id, txn_no),
    UNIQUE KEY uk_cash_txn_tenant_register_idempotency (tenant_id, cash_register_id, idempotency_key),
    UNIQUE KEY uk_cash_txn_posted_journal_entry (posted_journal_entry_id),
    KEY ix_cash_txn_tenant_register_status_date (tenant_id, cash_register_id, status, book_date),
    KEY ix_cash_txn_tenant_type_status_date (tenant_id, txn_type, status, book_date),
    KEY ix_cash_txn_tenant_session (tenant_id, cash_session_id),
    CONSTRAINT fk_cash_txn_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_cash_txn_register
      FOREIGN KEY (cash_register_id) REFERENCES cash_registers(id),
    CONSTRAINT fk_cash_txn_session
      FOREIGN KEY (cash_session_id) REFERENCES cash_sessions(id),
    CONSTRAINT fk_cash_txn_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_cash_txn_counter_account
      FOREIGN KEY (counter_account_id) REFERENCES accounts(id),
    CONSTRAINT fk_cash_txn_counter_register
      FOREIGN KEY (counter_cash_register_id) REFERENCES cash_registers(id),
    CONSTRAINT fk_cash_txn_journal
      FOREIGN KEY (posted_journal_entry_id) REFERENCES journal_entries(id),
    CONSTRAINT fk_cash_txn_reversal_of
      FOREIGN KEY (reversal_of_transaction_id) REFERENCES cash_transactions(id),
    CONSTRAINT fk_cash_txn_created_by
      FOREIGN KEY (created_by_user_id) REFERENCES users(id),
    CONSTRAINT fk_cash_txn_submitted_by
      FOREIGN KEY (submitted_by_user_id) REFERENCES users(id),
    CONSTRAINT fk_cash_txn_approved_by
      FOREIGN KEY (approved_by_user_id) REFERENCES users(id),
    CONSTRAINT fk_cash_txn_posted_by
      FOREIGN KEY (posted_by_user_id) REFERENCES users(id),
    CONSTRAINT fk_cash_txn_reversed_by
      FOREIGN KEY (reversed_by_user_id) REFERENCES users(id),
    CONSTRAINT fk_cash_txn_cancelled_by
      FOREIGN KEY (cancelled_by_user_id) REFERENCES users(id),
    CHECK (amount > 0),
    CHECK (idempotency_key IS NULL OR CHAR_LENGTH(idempotency_key) > 0),
    CHECK (override_cash_control = FALSE OR override_reason IS NOT NULL)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
];

const migration015CashControlFoundation = {
  key: "m015_cash_control_foundation",
  description:
    "Add foundational cash-control schema (registers, sessions, transactions) and CASH journal source type",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration015CashControlFoundation;
