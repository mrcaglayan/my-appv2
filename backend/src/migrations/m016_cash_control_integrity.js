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
  ALTER TABLE cash_sessions
  ADD COLUMN open_session_register_id BIGINT UNSIGNED
    GENERATED ALWAYS AS (
      CASE
        WHEN status = 'OPEN' THEN cash_register_id
        ELSE NULL
      END
    ) STORED
  `,
  `
  ALTER TABLE cash_sessions
  ADD UNIQUE KEY uk_cash_sessions_one_open_per_register (open_session_register_id)
  `,
  `
  ALTER TABLE cash_sessions
  ADD INDEX ix_cash_sessions_tenant_status_opened_closed (tenant_id, status, opened_at, closed_at)
  `,
  `
  ALTER TABLE cash_transactions
  ADD UNIQUE KEY uk_cash_txn_tenant_register_idempotency (
    tenant_id,
    cash_register_id,
    idempotency_key
  )
  `,
  `
  ALTER TABLE cash_transactions
  ADD UNIQUE KEY uk_cash_txn_single_reversal (reversal_of_transaction_id)
  `,
  `
  ALTER TABLE cash_transactions
  ADD UNIQUE KEY uk_cash_txn_tenant_register_id (tenant_id, cash_register_id, id)
  `,
  `
  ALTER TABLE cash_transactions
  ADD CONSTRAINT fk_cash_txn_reversal_same_register
    FOREIGN KEY (tenant_id, cash_register_id, reversal_of_transaction_id)
    REFERENCES cash_transactions (tenant_id, cash_register_id, id)
  `,
  `
  ALTER TABLE cash_transactions
  ADD INDEX ix_cash_txn_idempotency_lookup (
    tenant_id,
    cash_register_id,
    idempotency_key,
    status,
    updated_at
  )
  `,
  `
  ALTER TABLE cash_transactions
  ADD INDEX ix_cash_txn_tenant_status_datetime (tenant_id, status, txn_datetime)
  `,
  `
  ALTER TABLE cash_transactions
  ADD INDEX ix_cash_txn_tenant_status_updated (tenant_id, status, updated_at)
  `,
];

const migration016CashControlIntegrity = {
  key: "m016_cash_control_integrity",
  description:
    "Harden cash-control concurrency and integrity with idempotency, reversal-link, and status path indexes",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration016CashControlIntegrity;
