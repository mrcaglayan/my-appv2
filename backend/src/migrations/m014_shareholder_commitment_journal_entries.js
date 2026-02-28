const ignorableErrnos = new Set([
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
  CREATE TABLE IF NOT EXISTS shareholder_commitment_journal_entries (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    shareholder_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    journal_entry_id BIGINT UNSIGNED NOT NULL,
    line_group_key VARCHAR(100) NULL,
    amount DECIMAL(20,6) NOT NULL,
    currency_code CHAR(3) NOT NULL,
    created_by_user_id INT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_sh_commitment_shareholder_journal (shareholder_id, journal_entry_id),
    KEY ix_sh_commitment_shareholder (shareholder_id),
    KEY ix_sh_commitment_legal_entity (legal_entity_id),
    KEY ix_sh_commitment_journal_entry (journal_entry_id),
    KEY ix_sh_commitment_shareholder_created_at (shareholder_id, created_at),
    CONSTRAINT fk_sh_commitment_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_sh_commitment_shareholder
      FOREIGN KEY (shareholder_id) REFERENCES shareholders(id),
    CONSTRAINT fk_sh_commitment_legal_entity
      FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_sh_commitment_journal_entry
      FOREIGN KEY (journal_entry_id) REFERENCES journal_entries(id),
    CONSTRAINT fk_sh_commitment_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_sh_commitment_created_by
      FOREIGN KEY (created_by_user_id) REFERENCES users(id),
    CHECK (amount > 0)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
];

const migration014ShareholderCommitmentJournalEntries = {
  key: "m014_shareholder_commitment_journal_entries",
  description:
    "Track per-shareholder commitment journalized amounts for safe delta-based batch posting",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration014ShareholderCommitmentJournalEntries;
