const IGNORABLE_ERRNOS = new Set([
  1050, // ER_TABLE_EXISTS_ERROR
  1061, // ER_DUP_KEYNAME
  1826, // ER_FK_DUP_NAME
]);

async function safeExecute(connection, sql, params = []) {
  try {
    await connection.execute(sql, params);
  } catch (err) {
    if (IGNORABLE_ERRNOS.has(Number(err?.errno))) {
      return;
    }
    throw err;
  }
}

const migration067UserPreferences = {
  key: "m067_user_preferences",
  description: "Per-user server-side preferences for cross-device context persistence (UX04)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS user_preferences (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         user_id INT NOT NULL,
         preference_key VARCHAR(80) NOT NULL,
         preference_value_json JSON NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_user_preferences_scope_key (tenant_id, user_id, preference_key),
         KEY ix_user_preferences_scope_user (tenant_id, user_id),
         CONSTRAINT fk_user_preferences_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_user_preferences_user
           FOREIGN KEY (tenant_id, user_id) REFERENCES users(tenant_id, id)
           ON DELETE CASCADE
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },

  async down(connection) {
    await safeExecute(connection, `DROP TABLE IF EXISTS user_preferences`);
  },
};

export default migration067UserPreferences;

