import { ensureDatabaseExists, pool } from "./db.js";
import migrations from "./migrations/index.js";

const MIGRATIONS_TABLE = "schema_migrations";

async function ensureMigrationsTable(connection) {
  await connection.execute(`
    CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
      id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
      migration_key VARCHAR(255) NOT NULL UNIQUE,
      description VARCHAR(255) NULL,
      applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `);
}

async function getAppliedMigrationKeys(connection) {
  const [rows] = await connection.execute(
    `SELECT migration_key FROM ${MIGRATIONS_TABLE} ORDER BY id`
  );
  return new Set(rows.map((row) => row.migration_key));
}

async function applyMigration(connection, migration) {
  await migration.up(connection);
  await connection.execute(
    `INSERT INTO ${MIGRATIONS_TABLE} (migration_key, description) VALUES (?, ?)`,
    [migration.key, migration.description || null]
  );
}

export async function runMigrations() {
  await ensureDatabaseExists();
  const connection = await pool.getConnection();

  try {
    await ensureMigrationsTable(connection);
    const appliedKeys = await getAppliedMigrationKeys(connection);

    for (const migration of migrations) {
      if (appliedKeys.has(migration.key)) {
        continue;
      }

      console.log(`[migrate] applying ${migration.key}...`);
      await applyMigration(connection, migration);
      console.log(`[migrate] applied ${migration.key}`);
    }
  } finally {
    connection.release();
  }
}

export async function getMigrationStatus() {
  await ensureDatabaseExists();
  const connection = await pool.getConnection();

  try {
    await ensureMigrationsTable(connection);
    const appliedKeys = await getAppliedMigrationKeys(connection);

    return migrations.map((migration) => ({
      key: migration.key,
      description: migration.description || "",
      applied: appliedKeys.has(migration.key),
    }));
  } finally {
    connection.release();
  }
}
