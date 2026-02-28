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
  ALTER TABLE journal_entries
  ADD COLUMN intercompany_source_journal_entry_id BIGINT UNSIGNED NULL
  `,
  `
  ALTER TABLE journal_entries
  ADD INDEX ix_journal_ic_source (tenant_id, intercompany_source_journal_entry_id)
  `,
  `
  ALTER TABLE journal_entries
  ADD CONSTRAINT fk_journal_ic_source_entry
  FOREIGN KEY (intercompany_source_journal_entry_id) REFERENCES journal_entries(id)
  `,
];

const migration010IntercompanyJournalLinks = {
  key: "m010_intercompany_journal_links",
  description:
    "Add source-to-mirror intercompany journal linking metadata for controlled partner posting",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration010IntercompanyJournalLinks;
