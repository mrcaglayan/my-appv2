const ignorableErrnos = new Set([
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
     FROM information_schema.referential_constraints
     WHERE constraint_schema = DATABASE()
       AND table_name = ?
       AND constraint_name = ?
     LIMIT 1`,
    [tableName, constraintName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

const migration026ContractLineReportingOptimization = {
  key: "m026_contract_line_reporting_optimization",
  description:
    "Optimize contract line reporting with legal-entity denormalization and composite indexes",
  async up(connection) {
    if (!(await hasColumn(connection, "contract_lines", "legal_entity_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE contract_lines
         ADD COLUMN legal_entity_id BIGINT UNSIGNED NULL
         AFTER tenant_id`
      );
    }

    await safeExecute(
      connection,
      `UPDATE contract_lines cl
       JOIN contracts c
         ON c.tenant_id = cl.tenant_id
        AND c.id = cl.contract_id
       SET cl.legal_entity_id = c.legal_entity_id
       WHERE cl.legal_entity_id IS NULL`
    );

    await safeExecute(
      connection,
      `ALTER TABLE contract_lines
       MODIFY COLUMN legal_entity_id BIGINT UNSIGNED NOT NULL`
    );

    if (!(await hasIndex(connection, "contract_lines", "uk_contract_lines_tenant_entity_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE contract_lines
         ADD UNIQUE KEY uk_contract_lines_tenant_entity_id (tenant_id, legal_entity_id, id)`
      );
    }

    if (!(await hasIndex(connection, "contract_lines", "ix_contract_lines_report_scope"))) {
      await safeExecute(
        connection,
        `ALTER TABLE contract_lines
         ADD KEY ix_contract_lines_report_scope (
           tenant_id,
           legal_entity_id,
           status,
           contract_id,
           line_no
         )`
      );
    }

    if (!(await hasIndex(connection, "contract_lines", "ix_contract_lines_contract_line_order"))) {
      await safeExecute(
        connection,
        `ALTER TABLE contract_lines
         ADD KEY ix_contract_lines_contract_line_order (
           tenant_id,
           legal_entity_id,
           contract_id,
           line_no,
           id
         )`
      );
    }

    if (!(await hasForeignKey(connection, "contract_lines", "fk_contract_lines_entity_tenant"))) {
      await safeExecute(
        connection,
        `ALTER TABLE contract_lines
         ADD CONSTRAINT fk_contract_lines_entity_tenant
         FOREIGN KEY (tenant_id, legal_entity_id)
         REFERENCES legal_entities(tenant_id, id)`
      );
    }

    if (
      !(await hasForeignKey(
        connection,
        "contract_lines",
        "fk_contract_lines_contract_tenant_entity"
      ))
    ) {
      await safeExecute(
        connection,
        `ALTER TABLE contract_lines
         ADD CONSTRAINT fk_contract_lines_contract_tenant_entity
         FOREIGN KEY (tenant_id, legal_entity_id, contract_id)
         REFERENCES contracts(tenant_id, legal_entity_id, id)
         ON UPDATE CASCADE`
      );
    }
  },
};

export default migration026ContractLineReportingOptimization;
