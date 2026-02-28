const ignorableErrnos = new Set([
  1060, // ER_DUP_FIELDNAME
  1061, // ER_DUP_KEYNAME
  1091, // ER_CANT_DROP_FIELD_OR_KEY
  1826, // ER_FK_DUP_NAME / duplicate constraint name
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

async function hasForeignKey(connection, tableName, constraintName) {
  const [rows] = await connection.execute(
    `SELECT 1
     FROM information_schema.referential_constraints
     WHERE constraint_schema = DATABASE()
       AND table_name = ?
       AND constraint_name = ?
     LIMIT 1`,
    [tableName, constraintName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

async function hasCheckConstraint(connection, tableName, constraintName) {
  const [rows] = await connection.execute(
    `SELECT 1
     FROM information_schema.table_constraints
     WHERE constraint_schema = DATABASE()
       AND table_name = ?
       AND constraint_type = 'CHECK'
       AND constraint_name = ?
     LIMIT 1`,
    [tableName, constraintName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

const migration025ContractDocumentLinkFxSnapshots = {
  key: "m025_contract_document_link_fx_snapshots",
  description:
    "Enable cross-currency contract-document linking with link-level currency/fx snapshots",
  async up(connection) {
    if (!(await hasColumn(connection, "contract_document_links", "contract_currency_code_snapshot"))) {
      await safeExecute(
        connection,
        `ALTER TABLE contract_document_links
         ADD COLUMN contract_currency_code_snapshot CHAR(3) NULL
         AFTER linked_amount_base`
      );
    }

    if (!(await hasColumn(connection, "contract_document_links", "document_currency_code_snapshot"))) {
      await safeExecute(
        connection,
        `ALTER TABLE contract_document_links
         ADD COLUMN document_currency_code_snapshot CHAR(3) NULL
         AFTER contract_currency_code_snapshot`
      );
    }

    if (!(await hasColumn(connection, "contract_document_links", "link_fx_rate_snapshot"))) {
      await safeExecute(
        connection,
        `ALTER TABLE contract_document_links
         ADD COLUMN link_fx_rate_snapshot DECIMAL(20,10) NULL
         AFTER document_currency_code_snapshot`
      );
    }

    await safeExecute(
      connection,
      `UPDATE contract_document_links l
       JOIN contracts c
         ON c.tenant_id = l.tenant_id
        AND c.id = l.contract_id
       JOIN cari_documents d
         ON d.tenant_id = l.tenant_id
        AND d.legal_entity_id = l.legal_entity_id
        AND d.id = l.cari_document_id
       SET
         l.contract_currency_code_snapshot = COALESCE(
           l.contract_currency_code_snapshot,
           c.currency_code
         ),
         l.document_currency_code_snapshot = COALESCE(
           l.document_currency_code_snapshot,
           d.currency_code
         ),
         l.link_fx_rate_snapshot = COALESCE(
           l.link_fx_rate_snapshot,
           CASE
             WHEN ABS(COALESCE(l.linked_amount_txn, 0)) > 0
               THEN ABS(COALESCE(l.linked_amount_base, 0) / l.linked_amount_txn)
             ELSE NULL
           END,
           CASE
             WHEN UPPER(COALESCE(c.currency_code, '')) = UPPER(COALESCE(d.currency_code, ''))
               THEN 1
             ELSE NULL
           END,
           1
         )
       WHERE l.contract_currency_code_snapshot IS NULL
          OR l.document_currency_code_snapshot IS NULL
          OR l.link_fx_rate_snapshot IS NULL`
    );

    await safeExecute(
      connection,
      `ALTER TABLE contract_document_links
       MODIFY COLUMN contract_currency_code_snapshot CHAR(3) NOT NULL`
    );
    await safeExecute(
      connection,
      `ALTER TABLE contract_document_links
       MODIFY COLUMN document_currency_code_snapshot CHAR(3) NOT NULL`
    );
    await safeExecute(
      connection,
      `ALTER TABLE contract_document_links
       MODIFY COLUMN link_fx_rate_snapshot DECIMAL(20,10) NOT NULL`
    );

    if (
      !(await hasForeignKey(
        connection,
        "contract_document_links",
        "fk_contract_doc_links_contract_currency"
      ))
    ) {
      await safeExecute(
        connection,
        `ALTER TABLE contract_document_links
         ADD CONSTRAINT fk_contract_doc_links_contract_currency
         FOREIGN KEY (contract_currency_code_snapshot) REFERENCES currencies(code)`
      );
    }
    if (
      !(await hasForeignKey(
        connection,
        "contract_document_links",
        "fk_contract_doc_links_document_currency"
      ))
    ) {
      await safeExecute(
        connection,
        `ALTER TABLE contract_document_links
         ADD CONSTRAINT fk_contract_doc_links_document_currency
         FOREIGN KEY (document_currency_code_snapshot) REFERENCES currencies(code)`
      );
    }

    if (
      !(await hasCheckConstraint(
        connection,
        "contract_document_links",
        "chk_contract_doc_links_link_fx_rate"
      ))
    ) {
      await safeExecute(
        connection,
        `ALTER TABLE contract_document_links
         ADD CONSTRAINT chk_contract_doc_links_link_fx_rate
         CHECK (link_fx_rate_snapshot > 0)`
      );
    }
  },
};

export default migration025ContractDocumentLinkFxSnapshots;
