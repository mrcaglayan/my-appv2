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
  CREATE TABLE IF NOT EXISTS shareholders (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    shareholder_type ENUM('INDIVIDUAL','CORPORATE') NOT NULL DEFAULT 'INDIVIDUAL',
    tax_id VARCHAR(80) NULL,
    ownership_pct DECIMAL(9,6) NULL,
    committed_capital DECIMAL(20,6) NOT NULL DEFAULT 0,
    paid_capital DECIMAL(20,6) NOT NULL DEFAULT 0,
    currency_code CHAR(3) NOT NULL,
    status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
    notes VARCHAR(500) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_shareholder_tenant_entity_code (tenant_id, legal_entity_id, code),
    KEY ix_shareholders_tenant_entity (tenant_id, legal_entity_id),
    KEY ix_shareholders_tenant_status (tenant_id, status),
    CONSTRAINT fk_shareholders_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_shareholders_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_shareholders_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
];

const migration007ShareholdersMaster = {
  key: "m007_shareholders_master",
  description:
    "Add tenant-safe shareholder master records for legal entities and capital tracking",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration007ShareholdersMaster;
