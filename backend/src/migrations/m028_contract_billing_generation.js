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

const migration028ContractBillingGeneration = {
  key: "m028_contract_billing_generation",
  description:
    "Contract-driven Cari billing generation foundation with idempotent batch ledger and source metadata",
  async up(connection) {
    if (!(await hasColumn(connection, "cari_documents", "source_module"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_documents
         ADD COLUMN source_module VARCHAR(40) NULL
         AFTER fx_rate_snapshot`
      );
    }
    if (!(await hasColumn(connection, "cari_documents", "source_entity_type"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_documents
         ADD COLUMN source_entity_type VARCHAR(60) NULL
         AFTER source_module`
      );
    }
    if (!(await hasColumn(connection, "cari_documents", "source_entity_id"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_documents
         ADD COLUMN source_entity_id VARCHAR(120) NULL
         AFTER source_entity_type`
      );
    }
    if (!(await hasColumn(connection, "cari_documents", "integration_link_status"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_documents
         ADD COLUMN integration_link_status VARCHAR(30) NOT NULL DEFAULT 'UNLINKED'
         AFTER source_entity_id`
      );
    }
    if (!(await hasColumn(connection, "cari_documents", "integration_event_uid"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_documents
         ADD COLUMN integration_event_uid VARCHAR(100) NULL
         AFTER integration_link_status`
      );
    }

    if (!(await hasIndex(connection, "cari_documents", "uk_cari_docs_tenant_entity_event_uid"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_documents
         ADD UNIQUE KEY uk_cari_docs_tenant_entity_event_uid (
           tenant_id,
           legal_entity_id,
           integration_event_uid
         )`
      );
    }
    if (!(await hasIndex(connection, "cari_documents", "ix_cari_docs_tenant_source_ref"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_documents
         ADD KEY ix_cari_docs_tenant_source_ref (
           tenant_id,
           legal_entity_id,
           source_module,
           source_entity_type,
           source_entity_id
         )`
      );
    }
    if (!(await hasIndex(connection, "cari_documents", "ix_cari_docs_tenant_link_status"))) {
      await safeExecute(
        connection,
        `ALTER TABLE cari_documents
         ADD KEY ix_cari_docs_tenant_link_status (
           tenant_id,
           legal_entity_id,
           integration_link_status
         )`
      );
    }

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS contract_billing_batches (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        tenant_id BIGINT UNSIGNED NOT NULL,
        legal_entity_id BIGINT UNSIGNED NOT NULL,
        contract_id BIGINT UNSIGNED NOT NULL,
        idempotency_key VARCHAR(100) NOT NULL,
        integration_event_uid VARCHAR(100) NOT NULL,
        source_module VARCHAR(40) NULL,
        source_entity_type VARCHAR(60) NULL,
        source_entity_id VARCHAR(120) NULL,
        doc_type ENUM('INVOICE','ADVANCE','ADJUSTMENT') NOT NULL,
        amount_strategy ENUM('FULL','PARTIAL','MILESTONE') NOT NULL DEFAULT 'FULL',
        billing_date DATE NOT NULL,
        due_date DATE NULL,
        amount_txn DECIMAL(20,6) NOT NULL,
        amount_base DECIMAL(20,6) NOT NULL,
        currency_code CHAR(3) NOT NULL,
        selected_line_ids_json JSON NULL,
        status ENUM('PENDING','COMPLETED','FAILED') NOT NULL DEFAULT 'PENDING',
        generated_document_id BIGINT UNSIGNED NULL,
        generated_link_id BIGINT UNSIGNED NULL,
        payload_json JSON NULL,
        created_by_user_id INT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_contract_bill_batch_tenant_id_id (tenant_id, id),
        UNIQUE KEY uk_contract_bill_batch_scope_idempo (
          tenant_id,
          legal_entity_id,
          contract_id,
          idempotency_key
        ),
        UNIQUE KEY uk_contract_bill_batch_scope_event_uid (
          tenant_id,
          legal_entity_id,
          integration_event_uid
        ),
        KEY ix_contract_bill_batch_scope (
          tenant_id,
          legal_entity_id,
          contract_id,
          status
        ),
        KEY ix_contract_bill_batch_source_ref (
          tenant_id,
          legal_entity_id,
          source_module,
          source_entity_type,
          source_entity_id
        ),
        KEY ix_contract_bill_batch_document (
          tenant_id,
          legal_entity_id,
          generated_document_id
        ),
        KEY ix_contract_bill_batch_link (
          tenant_id,
          generated_link_id
        ),
        CONSTRAINT fk_contract_bill_batches_tenant
          FOREIGN KEY (tenant_id) REFERENCES tenants(id),
        CONSTRAINT fk_contract_bill_batches_entity_tenant
          FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
        CONSTRAINT fk_contract_bill_batches_contract_tenant
          FOREIGN KEY (tenant_id, legal_entity_id, contract_id)
          REFERENCES contracts(tenant_id, legal_entity_id, id),
        CONSTRAINT fk_contract_bill_batches_currency
          FOREIGN KEY (currency_code) REFERENCES currencies(code),
        CONSTRAINT fk_contract_bill_batches_document_tenant
          FOREIGN KEY (tenant_id, legal_entity_id, generated_document_id)
          REFERENCES cari_documents(tenant_id, legal_entity_id, id),
        CONSTRAINT fk_contract_bill_batches_link_tenant
          FOREIGN KEY (tenant_id, generated_link_id)
          REFERENCES contract_document_links(tenant_id, id),
        CONSTRAINT fk_contract_bill_batches_creator_user
          FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id),
        CHECK (amount_txn > 0),
        CHECK (amount_base > 0),
        CHECK (CHAR_LENGTH(idempotency_key) > 0),
        CHECK (CHAR_LENGTH(integration_event_uid) > 0),
        CHECK (due_date IS NULL OR due_date >= billing_date)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },
};

export default migration028ContractBillingGeneration;
