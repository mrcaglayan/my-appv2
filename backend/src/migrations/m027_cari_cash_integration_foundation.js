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

const migration027CariCashIntegrationFoundation = {
  key: "m027_cari_cash_integration_foundation",
  description:
    "Add Cari-Cash integration link metadata, source references, and integration event identity constraints",
  async up(connection) {
    if (!(await hasColumn(connection, "cash_transactions", "source_module"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD COLUMN source_module VARCHAR(40) NULL
         AFTER source_doc_id`
      );
    }
    if (!(await hasColumn(connection, "cash_transactions", "source_entity_type"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD COLUMN source_entity_type VARCHAR(60) NULL
         AFTER source_module`
      );
    }
    if (!(await hasColumn(connection, "cash_transactions", "source_entity_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD COLUMN source_entity_id VARCHAR(120) NULL
         AFTER source_entity_type`
      );
    }
    if (!(await hasColumn(connection, "cash_transactions", "integration_link_status"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD COLUMN integration_link_status VARCHAR(30) NOT NULL DEFAULT 'UNLINKED'
         AFTER source_entity_id`
      );
    }
    if (!(await hasColumn(connection, "cash_transactions", "linked_cari_settlement_batch_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD COLUMN linked_cari_settlement_batch_id BIGINT UNSIGNED NULL
         AFTER counter_cash_register_id`
      );
    }
    if (!(await hasColumn(connection, "cash_transactions", "linked_cari_unapplied_cash_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD COLUMN linked_cari_unapplied_cash_id BIGINT UNSIGNED NULL
         AFTER linked_cari_settlement_batch_id`
      );
    }
    if (!(await hasColumn(connection, "cash_transactions", "integration_event_uid"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD COLUMN integration_event_uid VARCHAR(100) NULL
         AFTER idempotency_key`
      );
    }

    if (!(await hasColumn(connection, "cari_settlement_batches", "cash_transaction_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_settlement_batches
         ADD COLUMN cash_transaction_id BIGINT UNSIGNED NULL
         AFTER counterparty_id`
      );
    }
    if (!(await hasColumn(connection, "cari_settlement_batches", "source_module"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_settlement_batches
         ADD COLUMN source_module VARCHAR(40) NULL
         AFTER bank_apply_idempotency_key`
      );
    }
    if (!(await hasColumn(connection, "cari_settlement_batches", "source_entity_type"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_settlement_batches
         ADD COLUMN source_entity_type VARCHAR(60) NULL
         AFTER source_module`
      );
    }
    if (!(await hasColumn(connection, "cari_settlement_batches", "source_entity_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_settlement_batches
         ADD COLUMN source_entity_id VARCHAR(120) NULL
         AFTER source_entity_type`
      );
    }
    if (!(await hasColumn(connection, "cari_settlement_batches", "integration_link_status"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_settlement_batches
         ADD COLUMN integration_link_status VARCHAR(30) NOT NULL DEFAULT 'UNLINKED'
         AFTER source_entity_id`
      );
    }
    if (!(await hasColumn(connection, "cari_settlement_batches", "integration_event_uid"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_settlement_batches
         ADD COLUMN integration_event_uid VARCHAR(100) NULL
         AFTER integration_link_status`
      );
    }

    if (!(await hasColumn(connection, "cari_unapplied_cash", "cash_transaction_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_unapplied_cash
         ADD COLUMN cash_transaction_id BIGINT UNSIGNED NULL
         AFTER counterparty_id`
      );
    }
    if (!(await hasColumn(connection, "cari_unapplied_cash", "source_module"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_unapplied_cash
         ADD COLUMN source_module VARCHAR(40) NULL
         AFTER bank_apply_idempotency_key`
      );
    }
    if (!(await hasColumn(connection, "cari_unapplied_cash", "source_entity_type"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_unapplied_cash
         ADD COLUMN source_entity_type VARCHAR(60) NULL
         AFTER source_module`
      );
    }
    if (!(await hasColumn(connection, "cari_unapplied_cash", "source_entity_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_unapplied_cash
         ADD COLUMN source_entity_id VARCHAR(120) NULL
         AFTER source_entity_type`
      );
    }
    if (!(await hasColumn(connection, "cari_unapplied_cash", "integration_link_status"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_unapplied_cash
         ADD COLUMN integration_link_status VARCHAR(30) NOT NULL DEFAULT 'UNLINKED'
         AFTER source_entity_id`
      );
    }
    if (!(await hasColumn(connection, "cari_unapplied_cash", "integration_event_uid"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_unapplied_cash
         ADD COLUMN integration_event_uid VARCHAR(100) NULL
         AFTER integration_link_status`
      );
    }

    if (!(await hasIndex(connection, "cash_transactions", "uk_cash_txn_tenant_id_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD UNIQUE KEY uk_cash_txn_tenant_id_id (tenant_id, id)`
      );
    }
    if (!(await hasIndex(connection, "cash_transactions", "uk_cash_txn_tenant_integration_event_uid"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD UNIQUE KEY uk_cash_txn_tenant_integration_event_uid (
           tenant_id,
           integration_event_uid
         )`
      );
    }
    if (!(await hasIndex(connection, "cash_transactions", "ix_cash_txn_tenant_source_ref"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD KEY ix_cash_txn_tenant_source_ref (
           tenant_id,
           source_module,
           source_entity_type,
           source_entity_id
         )`
      );
    }
    if (!(await hasIndex(connection, "cash_transactions", "ix_cash_txn_tenant_link_status"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD KEY ix_cash_txn_tenant_link_status (
           tenant_id,
           integration_link_status
         )`
      );
    }
    if (!(await hasIndex(connection, "cash_transactions", "ix_cash_txn_tenant_linked_settlement"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD KEY ix_cash_txn_tenant_linked_settlement (
           tenant_id,
           linked_cari_settlement_batch_id
         )`
      );
    }
    if (!(await hasIndex(connection, "cash_transactions", "ix_cash_txn_tenant_linked_unapplied"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD KEY ix_cash_txn_tenant_linked_unapplied (
           tenant_id,
           linked_cari_unapplied_cash_id
         )`
      );
    }

    if (!(await hasIndex(connection, "cari_settlement_batches", "uk_cari_settle_batches_tenant_cash_txn"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_settlement_batches
         ADD UNIQUE KEY uk_cari_settle_batches_tenant_cash_txn (
           tenant_id,
           cash_transaction_id
         )`
      );
    }
    if (
      !(await hasIndex(connection, "cari_settlement_batches", "uk_cari_settle_batches_tenant_event_uid"))
    ) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_settlement_batches
         ADD UNIQUE KEY uk_cari_settle_batches_tenant_event_uid (
           tenant_id,
           legal_entity_id,
           integration_event_uid
         )`
      );
    }
    if (!(await hasIndex(connection, "cari_settlement_batches", "ix_cari_settle_batches_tenant_source_ref"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_settlement_batches
         ADD KEY ix_cari_settle_batches_tenant_source_ref (
           tenant_id,
           legal_entity_id,
           source_module,
           source_entity_type,
           source_entity_id
         )`
      );
    }
    if (
      !(await hasIndex(connection, "cari_settlement_batches", "ix_cari_settle_batches_tenant_link_status"))
    ) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_settlement_batches
         ADD KEY ix_cari_settle_batches_tenant_link_status (
           tenant_id,
           legal_entity_id,
           integration_link_status
         )`
      );
    }

    if (!(await hasIndex(connection, "cari_unapplied_cash", "uk_cari_unap_tenant_cash_txn"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_unapplied_cash
         ADD UNIQUE KEY uk_cari_unap_tenant_cash_txn (
           tenant_id,
           cash_transaction_id
         )`
      );
    }
    if (!(await hasIndex(connection, "cari_unapplied_cash", "uk_cari_unap_tenant_event_uid"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_unapplied_cash
         ADD UNIQUE KEY uk_cari_unap_tenant_event_uid (
           tenant_id,
           legal_entity_id,
           integration_event_uid
         )`
      );
    }
    if (!(await hasIndex(connection, "cari_unapplied_cash", "ix_cari_unap_tenant_source_ref"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_unapplied_cash
         ADD KEY ix_cari_unap_tenant_source_ref (
           tenant_id,
           legal_entity_id,
           source_module,
           source_entity_type,
           source_entity_id
         )`
      );
    }
    if (!(await hasIndex(connection, "cari_unapplied_cash", "ix_cari_unap_tenant_link_status"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_unapplied_cash
         ADD KEY ix_cari_unap_tenant_link_status (
           tenant_id,
           legal_entity_id,
           integration_link_status
         )`
      );
    }

    if (!(await hasForeignKey(connection, "cari_settlement_batches", "fk_cari_settle_cash_txn_tenant"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_settlement_batches
         ADD CONSTRAINT fk_cari_settle_cash_txn_tenant
         FOREIGN KEY (tenant_id, cash_transaction_id)
         REFERENCES cash_transactions(tenant_id, id)`
      );
    }
    if (!(await hasForeignKey(connection, "cari_unapplied_cash", "fk_cari_unap_cash_txn_tenant"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_unapplied_cash
         ADD CONSTRAINT fk_cari_unap_cash_txn_tenant
         FOREIGN KEY (tenant_id, cash_transaction_id)
         REFERENCES cash_transactions(tenant_id, id)`
      );
    }
    if (!(await hasForeignKey(connection, "cash_transactions", "fk_cash_txn_link_settle_tenant"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD CONSTRAINT fk_cash_txn_link_settle_tenant
         FOREIGN KEY (tenant_id, linked_cari_settlement_batch_id)
         REFERENCES cari_settlement_batches(tenant_id, id)`
      );
    }
    if (!(await hasForeignKey(connection, "cash_transactions", "fk_cash_txn_link_unap_tenant"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cash_transactions
         ADD CONSTRAINT fk_cash_txn_link_unap_tenant
         FOREIGN KEY (tenant_id, linked_cari_unapplied_cash_id)
         REFERENCES cari_unapplied_cash(tenant_id, id)`
      );
    }
  },
};

export default migration027CariCashIntegrationFoundation;
