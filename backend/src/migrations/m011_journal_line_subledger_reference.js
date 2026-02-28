const ignorableErrnos = new Set([
  1060, // ER_DUP_FIELDNAME
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
  ALTER TABLE journal_lines
  ADD COLUMN subledger_reference_no VARCHAR(100) NULL AFTER description
  `,
];

const migration011JournalLineSubledgerReference = {
  key: "m011_journal_line_subledger_reference",
  description:
    "Add optional subledger reference number on journal lines to enforce branch-level subledger policy",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration011JournalLineSubledgerReference;
