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

const statements = [
  `
  CREATE TABLE IF NOT EXISTS contract_document_link_events (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    contract_id BIGINT UNSIGNED NOT NULL,
    contract_document_link_id BIGINT UNSIGNED NOT NULL,
    action_type ENUM('ADJUST','UNLINK') NOT NULL,
    delta_amount_txn DECIMAL(20,6) NOT NULL,
    delta_amount_base DECIMAL(20,6) NOT NULL,
    reason VARCHAR(500) NOT NULL,
    created_by_user_id INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_contract_doc_link_events_tenant_id_id (tenant_id, id),
    KEY ix_contract_doc_link_events_scope (
      tenant_id,
      legal_entity_id,
      contract_id,
      contract_document_link_id,
      created_at
    ),
    KEY ix_contract_doc_link_events_creator_tenant_user (tenant_id, created_by_user_id),
    CONSTRAINT fk_contract_doc_link_events_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_contract_doc_link_events_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_contract_doc_link_events_contract_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, contract_id)
      REFERENCES contracts(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_contract_doc_link_events_link_tenant
      FOREIGN KEY (tenant_id, contract_document_link_id)
      REFERENCES contract_document_links(tenant_id, id),
    CONSTRAINT fk_contract_doc_link_events_creator_user
      FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
];

const migration023ContractDocumentLinkEvents = {
  key: "m023_contract_document_link_events",
  description: "Append-only adjustment/unlink events for contract document links",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration023ContractDocumentLinkEvents;
