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
  CREATE TABLE IF NOT EXISTS contracts (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    counterparty_id BIGINT UNSIGNED NOT NULL,
    contract_no VARCHAR(80) NOT NULL,
    contract_type ENUM('CUSTOMER','VENDOR') NOT NULL,
    status ENUM('DRAFT','ACTIVE','SUSPENDED','CLOSED','CANCELLED') NOT NULL DEFAULT 'DRAFT',
    currency_code CHAR(3) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NULL,
    total_amount_txn DECIMAL(20,6) NOT NULL DEFAULT 0,
    total_amount_base DECIMAL(20,6) NOT NULL DEFAULT 0,
    notes VARCHAR(500) NULL,
    created_by_user_id INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_contract_no (tenant_id, legal_entity_id, contract_no),
    UNIQUE KEY uk_contracts_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_contracts_tenant_entity_id (tenant_id, legal_entity_id, id),
    KEY ix_contract_tenant_id (tenant_id),
    KEY ix_contract_scope (tenant_id, legal_entity_id, counterparty_id, status),
    KEY ix_contract_creator_tenant_user (tenant_id, created_by_user_id),
    CONSTRAINT fk_contracts_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_contracts_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_contracts_counterparty_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, counterparty_id)
      REFERENCES counterparties(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_contracts_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_contracts_creator_user
      FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS contract_lines (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    contract_id BIGINT UNSIGNED NOT NULL,
    line_no INT NOT NULL,
    description VARCHAR(255) NOT NULL,
    line_amount_txn DECIMAL(20,6) NOT NULL,
    line_amount_base DECIMAL(20,6) NOT NULL,
    recognition_method ENUM('STRAIGHT_LINE','MILESTONE','MANUAL') NOT NULL DEFAULT 'STRAIGHT_LINE',
    recognition_start_date DATE NULL,
    recognition_end_date DATE NULL,
    deferred_account_id BIGINT UNSIGNED NULL,
    revenue_account_id BIGINT UNSIGNED NULL,
    status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_contract_lines_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_contract_line_no (tenant_id, contract_id, line_no),
    KEY ix_contract_line_scope (tenant_id, contract_id, status),
    CONSTRAINT fk_contract_lines_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_contract_lines_contract_tenant
      FOREIGN KEY (tenant_id, contract_id) REFERENCES contracts(tenant_id, id),
    CONSTRAINT fk_contract_lines_deferred_account
      FOREIGN KEY (deferred_account_id) REFERENCES accounts(id),
    CONSTRAINT fk_contract_lines_revenue_account
      FOREIGN KEY (revenue_account_id) REFERENCES accounts(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS contract_document_links (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    contract_id BIGINT UNSIGNED NOT NULL,
    cari_document_id BIGINT UNSIGNED NOT NULL,
    link_type ENUM('BILLING','ADVANCE','ADJUSTMENT') NOT NULL,
    linked_amount_txn DECIMAL(20,6) NOT NULL DEFAULT 0,
    linked_amount_base DECIMAL(20,6) NOT NULL DEFAULT 0,
    created_by_user_id INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_contract_doc_links_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_contract_doc_link (tenant_id, contract_id, cari_document_id, link_type),
    KEY ix_contract_doc_link_scope (tenant_id, legal_entity_id, contract_id, cari_document_id),
    KEY ix_contract_doc_link_creator_tenant_user (tenant_id, created_by_user_id),
    CONSTRAINT fk_contract_doc_links_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_contract_doc_links_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_contract_doc_links_contract_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, contract_id)
      REFERENCES contracts(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_contract_doc_links_cari_doc_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, cari_document_id)
      REFERENCES cari_documents(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_contract_doc_links_creator_user
      FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
];

const migration020ContractsFoundation = {
  key: "m020_contracts_foundation",
  description: "Contracts domain foundation and document links",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration020ContractsFoundation;
