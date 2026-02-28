const ignorableErrnos = new Set([
  1060, // duplicate column
  1061, // duplicate index/key
  1826, // duplicate constraint name
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

async function hasColumn(connection, tableName, columnName) {
  const [rows] = await connection.execute(
    `SELECT 1
     FROM information_schema.columns
     WHERE table_schema = DATABASE()
       AND table_name = ?
       AND column_name = ?
     LIMIT 1`,
    [tableName, columnName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

async function hasIndex(connection, tableName, indexName) {
  const [rows] = await connection.execute(
    `SELECT 1
     FROM information_schema.statistics
     WHERE table_schema = DATABASE()
       AND table_name = ?
       AND index_name = ?
     LIMIT 1`,
    [tableName, indexName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

async function hasForeignKey(connection, tableName, constraintName) {
  const [rows] = await connection.execute(
    `SELECT 1
     FROM information_schema.table_constraints
     WHERE constraint_schema = DATABASE()
       AND table_name = ?
       AND constraint_type = 'FOREIGN KEY'
       AND constraint_name = ?
     LIMIT 1`,
    [tableName, constraintName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

const migration022CounterpartyAccountMapping = {
  key: "m022_counterparty_account_mapping",
  description: "Add per-counterparty AR/AP account mapping",
  async up(connection) {
    if (!(await hasColumn(connection, "counterparties", "ar_account_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD COLUMN ar_account_id BIGINT UNSIGNED NULL AFTER default_payment_term_id`
      );
    }

    if (!(await hasColumn(connection, "counterparties", "ap_account_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD COLUMN ap_account_id BIGINT UNSIGNED NULL AFTER ar_account_id`
      );
    }

    if (!(await hasIndex(connection, "counterparties", "ix_counterparties_ar_account"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD KEY ix_counterparties_ar_account (ar_account_id)`
      );
    }

    if (!(await hasIndex(connection, "counterparties", "ix_counterparties_ap_account"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD KEY ix_counterparties_ap_account (ap_account_id)`
      );
    }

    if (!(await hasForeignKey(connection, "counterparties", "fk_counterparties_ar_account"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD CONSTRAINT fk_counterparties_ar_account
         FOREIGN KEY (ar_account_id) REFERENCES accounts(id)`
      );
    }

    if (!(await hasForeignKey(connection, "counterparties", "fk_counterparties_ap_account"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD CONSTRAINT fk_counterparties_ap_account
         FOREIGN KEY (ap_account_id) REFERENCES accounts(id)`
      );
    }
  },
};

export default migration022CounterpartyAccountMapping;
