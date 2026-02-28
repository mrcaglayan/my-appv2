import { closePool } from "./db.js";
import { getMigrationStatus, runMigrations } from "./migrationRunner.js";

const mode = process.argv[2] || "up";

async function main() {
  if (mode === "status") {
    const status = await getMigrationStatus();
    console.table(status);
    return;
  }

  await runMigrations();
}

main()
  .then(async () => {
    await closePool();
    process.exit(0);
  })
  .catch(async (err) => {
    console.error(err);
    await closePool();
    process.exit(1);
  });
