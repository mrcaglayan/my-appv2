const ignorableErrnos = new Set([
  1060, // ER_DUP_FIELDNAME
  1061, // ER_DUP_KEYNAME
  1091, // ER_CANT_DROP_FIELD_OR_KEY
  1826, // ER_FK_DUP_NAME / duplicate constraint name
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

async function hasCounterpartyRoleCheckConstraint(connection) {
  try {
    const [rows] = await connection.execute(
      `SELECT tc.constraint_name
       FROM information_schema.table_constraints tc
       JOIN information_schema.check_constraints cc
         ON cc.constraint_schema = tc.constraint_schema
        AND cc.constraint_name = tc.constraint_name
       WHERE tc.constraint_schema = DATABASE()
         AND tc.table_name = 'counterparties'
         AND tc.constraint_type = 'CHECK'
         AND cc.check_clause LIKE '%is_customer%'
         AND cc.check_clause LIKE '%is_vendor%'
       LIMIT 1`
    );
    return Array.isArray(rows) && rows.length > 0;
  } catch (err) {
    // Some engines/existing versions don't expose information_schema.check_constraints.
    // Fallback to constraint-name lookup so migration stays portable.
    if (err?.errno !== 1109) {
      throw err;
    }

    const [rows] = await connection.execute(
      `SELECT 1
       FROM information_schema.table_constraints
       WHERE constraint_schema = DATABASE()
         AND table_name = 'counterparties'
         AND constraint_type = 'CHECK'
         AND constraint_name = 'chk_counterparties_customer_or_vendor'
       LIMIT 1`
    );
    return Array.isArray(rows) && rows.length > 0;
  }
}

const migration019CounterpartyRoleFlags = {
  key: "m019_counterparty_role_flags",
  description:
    "Migrate counterparties role model to dual booleans (is_customer/is_vendor) and retire counterparty_type",
  async up(connection) {
    if (!(await hasColumn(connection, "counterparties", "is_customer"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD COLUMN is_customer BOOLEAN NOT NULL DEFAULT FALSE AFTER name`
      );
    }

    if (!(await hasColumn(connection, "counterparties", "is_vendor"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD COLUMN is_vendor BOOLEAN NOT NULL DEFAULT FALSE AFTER is_customer`
      );
    }

    const hasLegacyTypeColumn = await hasColumn(connection, "counterparties", "counterparty_type");

    if (hasLegacyTypeColumn) {
      await safeExecute(
        connection,
        `UPDATE counterparties
         SET
           is_customer = CASE
             WHEN counterparty_type IN ('CUSTOMER', 'BOTH') THEN TRUE
             ELSE FALSE
           END,
           is_vendor = CASE
             WHEN counterparty_type IN ('VENDOR', 'BOTH') THEN TRUE
             ELSE FALSE
           END`
      );
    }

    // Legacy rows with OTHER or invalid flags are normalized to customer role.
    await safeExecute(
      connection,
      `UPDATE counterparties
       SET is_customer = TRUE
       WHERE COALESCE(is_customer, 0) = 0
         AND COALESCE(is_vendor, 0) = 0`
    );

    if (!(await hasIndex(connection, "counterparties", "ix_counterparties_tenant_entity_roles"))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD KEY ix_counterparties_tenant_entity_roles (
           tenant_id,
           legal_entity_id,
           is_customer,
           is_vendor
         )`
      );
    }

    if (!(await hasCounterpartyRoleCheckConstraint(connection))) {
      await safeExecute(
        connection,
        `ALTER TABLE counterparties
         ADD CONSTRAINT chk_counterparties_customer_or_vendor
         CHECK (is_customer OR is_vendor)`
      );
    }

    if (hasLegacyTypeColumn) {
      await safeExecute(connection, "ALTER TABLE counterparties DROP COLUMN counterparty_type");
    }
  },
};

export default migration019CounterpartyRoleFlags;
