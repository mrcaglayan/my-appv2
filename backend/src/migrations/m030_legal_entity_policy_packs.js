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

async function hasIndex(connection, tableName, indexName) {
  const [rows] = await connection.execute(
    `SELECT 1
     FROM information_schema.statistics
     WHERE table_schema = DATABASE()
       AND table_name = ?
       AND index_name = ?
     LIMIT 1`,
    [tableName, indexName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

const migration030LegalEntityPolicyPacks = {
  key: "m030_legal_entity_policy_packs",
  description:
    "Add policy pack apply history table for legal entities (tenant-scoped audit metadata)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS legal_entity_policy_packs (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         pack_id VARCHAR(80) NOT NULL,
         mode ENUM('MERGE','OVERWRITE') NOT NULL,
         payload_json JSON NULL,
         applied_by_user_id INT NULL,
         applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         KEY ix_le_policy_packs_tenant_legal_applied (
           tenant_id,
           legal_entity_id,
           applied_at
         ),
         CONSTRAINT fk_le_policy_packs_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_le_policy_packs_legal_entity
           FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id),
         CONSTRAINT fk_le_policy_packs_user
           FOREIGN KEY (applied_by_user_id) REFERENCES users(id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    if (
      !(await hasIndex(
        connection,
        "legal_entity_policy_packs",
        "ix_le_policy_packs_tenant_legal_applied"
      ))
    ) {
      await safeExecute(
        connection,
        `ALTER TABLE legal_entity_policy_packs
         ADD KEY ix_le_policy_packs_tenant_legal_applied (
           tenant_id,
           legal_entity_id,
           applied_at
         )`
      );
    }
  },
};

export default migration030LegalEntityPolicyPacks;

