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
  ADD COLUMN commitment_debit_sub_account_id BIGINT UNSIGNED NULL AFTER capital_sub_account_id
  `,
  `
  ALTER TABLE shareholders
  ADD INDEX ix_shareholders_tenant_commitment_debit_sub_account (
    tenant_id,
    commitment_debit_sub_account_id
  )
  `,
  `
  ALTER TABLE shareholders
  ADD UNIQUE KEY uk_shareholder_entity_commitment_debit_sub_account (
    tenant_id,
    legal_entity_id,
    commitment_debit_sub_account_id
  )
  `,
  `
  ALTER TABLE shareholders
  ADD CONSTRAINT fk_shareholders_commitment_debit_sub_account
  FOREIGN KEY (commitment_debit_sub_account_id) REFERENCES accounts(id)
  `,
];

const migration012ShareholderCommitmentDebitSubAccount = {
  key: "m012_shareholder_commitment_debit_sub_account",
  description:
    "Store per-shareholder commitment debit sub-account mapping for auto commitment journals",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration012ShareholderCommitmentDebitSubAccount;
