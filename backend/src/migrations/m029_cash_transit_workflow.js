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

const migration029CashTransitWorkflow = {
  key: "m029_cash_transit_workflow",
  description:
    "Add cash transit workflow table for cross-OU transfer pairing, status lifecycle, and idempotent receive/cancel/reverse controls",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS cash_transit_transfers (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         source_cash_register_id BIGINT UNSIGNED NOT NULL,
         target_cash_register_id BIGINT UNSIGNED NOT NULL,
         source_operating_unit_id BIGINT UNSIGNED NULL,
         target_operating_unit_id BIGINT UNSIGNED NULL,
         transfer_out_cash_transaction_id BIGINT UNSIGNED NOT NULL,
         transfer_in_cash_transaction_id BIGINT UNSIGNED NULL,
         status ENUM('INITIATED','IN_TRANSIT','RECEIVED','CANCELED','REVERSED') NOT NULL DEFAULT 'INITIATED',
         amount DECIMAL(20,6) NOT NULL,
         currency_code CHAR(3) NOT NULL,
         transit_account_id BIGINT UNSIGNED NOT NULL,
         initiated_by_user_id INT NOT NULL,
         received_by_user_id INT NULL,
         canceled_by_user_id INT NULL,
         reversed_by_user_id INT NULL,
         initiated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         in_transit_at TIMESTAMP NULL,
         received_at TIMESTAMP NULL,
         canceled_at TIMESTAMP NULL,
         reversed_at TIMESTAMP NULL,
         cancel_reason VARCHAR(255) NULL,
         reverse_reason VARCHAR(255) NULL,
         idempotency_key VARCHAR(100) NULL,
         integration_event_uid VARCHAR(100) NULL,
         source_module VARCHAR(40) NOT NULL DEFAULT 'CASH',
         source_entity_type VARCHAR(60) NOT NULL DEFAULT 'cash_transaction',
         source_entity_id VARCHAR(120) NULL,
         note VARCHAR(500) NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_cash_transit_tenant_out_txn (tenant_id, transfer_out_cash_transaction_id),
         UNIQUE KEY uk_cash_transit_tenant_in_txn (tenant_id, transfer_in_cash_transaction_id),
         UNIQUE KEY uk_cash_transit_tenant_source_register_idem (
           tenant_id,
           source_cash_register_id,
           idempotency_key
         ),
         UNIQUE KEY uk_cash_transit_tenant_event_uid (tenant_id, integration_event_uid),
         KEY ix_cash_transit_tenant_status (tenant_id, status, updated_at),
         KEY ix_cash_transit_tenant_source_target_status (
           tenant_id,
           source_cash_register_id,
           target_cash_register_id,
           status
         ),
         CONSTRAINT fk_cash_transit_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_cash_transit_legal_entity
           FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id),
         CONSTRAINT fk_cash_transit_source_register
           FOREIGN KEY (source_cash_register_id) REFERENCES cash_registers(id),
         CONSTRAINT fk_cash_transit_target_register
           FOREIGN KEY (target_cash_register_id) REFERENCES cash_registers(id),
         CONSTRAINT fk_cash_transit_source_operating_unit
           FOREIGN KEY (source_operating_unit_id) REFERENCES operating_units(id),
         CONSTRAINT fk_cash_transit_target_operating_unit
           FOREIGN KEY (target_operating_unit_id) REFERENCES operating_units(id),
         CONSTRAINT fk_cash_transit_out_txn_tenant
           FOREIGN KEY (tenant_id, transfer_out_cash_transaction_id)
           REFERENCES cash_transactions(tenant_id, id),
         CONSTRAINT fk_cash_transit_in_txn_tenant
           FOREIGN KEY (tenant_id, transfer_in_cash_transaction_id)
           REFERENCES cash_transactions(tenant_id, id),
         CONSTRAINT fk_cash_transit_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code),
         CONSTRAINT fk_cash_transit_account
           FOREIGN KEY (transit_account_id) REFERENCES accounts(id),
         CONSTRAINT fk_cash_transit_initiated_by
           FOREIGN KEY (initiated_by_user_id) REFERENCES users(id),
         CONSTRAINT fk_cash_transit_received_by
           FOREIGN KEY (received_by_user_id) REFERENCES users(id),
         CONSTRAINT fk_cash_transit_canceled_by
           FOREIGN KEY (canceled_by_user_id) REFERENCES users(id),
         CONSTRAINT fk_cash_transit_reversed_by
           FOREIGN KEY (reversed_by_user_id) REFERENCES users(id),
         CHECK (amount > 0),
         CHECK (source_cash_register_id <> target_cash_register_id),
         CHECK (idempotency_key IS NULL OR CHAR_LENGTH(idempotency_key) > 0),
         CHECK (integration_event_uid IS NULL OR CHAR_LENGTH(integration_event_uid) > 0)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    if (!(await hasIndex(connection, "cash_transit_transfers", "ix_cash_transit_tenant_status"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transit_transfers
         ADD KEY ix_cash_transit_tenant_status (tenant_id, status, updated_at)`
      );
    }
    if (
      !(await hasIndex(
        connection,
        "cash_transit_transfers",
        "ix_cash_transit_tenant_source_target_status"
      ))
    ) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transit_transfers
         ADD KEY ix_cash_transit_tenant_source_target_status (
           tenant_id,
           source_cash_register_id,
           target_cash_register_id,
           status
         )`
      );
    }
    if (!(await hasForeignKey(connection, "cash_transit_transfers", "fk_cash_transit_out_txn_tenant"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transit_transfers
         ADD CONSTRAINT fk_cash_transit_out_txn_tenant
         FOREIGN KEY (tenant_id, transfer_out_cash_transaction_id)
         REFERENCES cash_transactions(tenant_id, id)`
      );
    }
    if (!(await hasForeignKey(connection, "cash_transit_transfers", "fk_cash_transit_in_txn_tenant"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transit_transfers
         ADD CONSTRAINT fk_cash_transit_in_txn_tenant
         FOREIGN KEY (tenant_id, transfer_in_cash_transaction_id)
         REFERENCES cash_transactions(tenant_id, id)`
      );
    }
  },
};

export default migration029CashTransitWorkflow;
