import { closePool } from "./db.js";
import { runMigrations } from "./migrationRunner.js";

async function run() {
  await runMigrations();
  console.log("Database schema is ready via migrations");
}

run()
  .then(async () => {
    await closePool();
    process.exit(0);
  })
  .catch(async (err) => {
    console.error("Failed to initialize database schema", err);
    await closePool();
    process.exit(1);
  });
