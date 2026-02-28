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
  ALTER TABLE shareholders
  ADD COLUMN capital_sub_account_id BIGINT UNSIGNED NULL AFTER paid_capital
  `,
  `
  ALTER TABLE shareholders
  ADD INDEX ix_shareholders_tenant_sub_account (tenant_id, capital_sub_account_id)
  `,
  `
  ALTER TABLE shareholders
  ADD UNIQUE KEY uk_shareholder_entity_sub_account (
    tenant_id,
    legal_entity_id,
    capital_sub_account_id
  )
  `,
  `
  ALTER TABLE shareholders
  ADD CONSTRAINT fk_shareholders_capital_sub_account
  FOREIGN KEY (capital_sub_account_id) REFERENCES accounts(id)
  `,
];

const migration008ShareholderCapitalSubAccount = {
  key: "m008_shareholder_capital_sub_account",
  description:
    "Link shareholders to optional equity sub-accounts for journal-based capital tracking",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration008ShareholderCapitalSubAccount;
