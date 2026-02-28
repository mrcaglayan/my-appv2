import { closePool, dropDatabaseIfExists } from "./db.js";

async function run() {
  await dropDatabaseIfExists();
  console.log("Database dropped (if it existed)");
}

run()
  .then(async () => {
    await closePool();
    process.exit(0);
  })
  .catch(async (err) => {
    console.error("Failed to drop database", err);
    await closePool();
    process.exit(1);
  });
