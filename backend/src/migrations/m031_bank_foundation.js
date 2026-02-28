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

const migration031BankFoundation = {
  key: "m031_bank_foundation",
  description: "Bank accounts foundation (tenant/legal-entity scoped master + GL link)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_accounts (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         code VARCHAR(60) NOT NULL,
         name VARCHAR(255) NOT NULL,
         currency_code CHAR(3) NOT NULL,
         gl_account_id BIGINT UNSIGNED NOT NULL,
         bank_name VARCHAR(255) NULL,
         branch_name VARCHAR(255) NULL,
         iban VARCHAR(64) NULL,
         account_no VARCHAR(80) NULL,
         is_active BOOLEAN NOT NULL DEFAULT TRUE,
         created_by_user_id INT NOT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
           ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_bank_accounts_entity_code (tenant_id, legal_entity_id, code),
         UNIQUE KEY uk_bank_accounts_entity_gl (tenant_id, legal_entity_id, gl_account_id),
         KEY ix_bank_accounts_tenant_entity_active (tenant_id, legal_entity_id, is_active),
         KEY ix_bank_accounts_gl_account (gl_account_id),
         CONSTRAINT fk_bank_accounts_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bank_accounts_entity_tenant
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bank_accounts_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code),
         CONSTRAINT fk_bank_accounts_gl_account
           FOREIGN KEY (gl_account_id) REFERENCES accounts(id),
         CONSTRAINT fk_bank_accounts_user_tenant
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },
};

export default migration031BankFoundation;
