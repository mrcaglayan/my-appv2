import { closePool } from "./db.js";
import { seedCore } from "./seedCore.js";

async function main() {
  const shouldCreateDefaultTenant =
    String(process.env.SEED_CORE_CREATE_DEFAULT_TENANT || "").toLowerCase() === "true";

  const result = await seedCore({
    ensureDefaultTenantIfMissing: shouldCreateDefaultTenant,
  });

  console.log("Core seed completed:", result);
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
