import bcrypt from "bcrypt";
import { closePool, query } from "./db.js";
import { runMigrations } from "./migrationRunner.js";

function normalizeEmail(value) {
  const email = String(value || "")
    .trim()
    .toLowerCase();
  if (!email) {
    throw new Error("PROVIDER_ADMIN_EMAIL is required");
  }
  if (!email.includes("@") || !email.includes(".")) {
    throw new Error("PROVIDER_ADMIN_EMAIL is invalid");
  }
  return email;
}

function validatePassword(value) {
  const password = String(value || "");
  if (password.length < 8) {
    throw new Error("PROVIDER_ADMIN_PASSWORD must be at least 8 characters");
  }
  if (password.length > 128) {
    throw new Error("PROVIDER_ADMIN_PASSWORD cannot exceed 128 characters");
  }
  return password;
}

function normalizeName(value) {
  const name = String(value || "").trim();
  if (!name) {
    throw new Error("PROVIDER_ADMIN_NAME is required");
  }
  if (name.length > 255) {
    throw new Error("PROVIDER_ADMIN_NAME cannot exceed 255 characters");
  }
  return name;
}

async function run() {
  const email = normalizeEmail(process.env.PROVIDER_ADMIN_EMAIL || "");
  const password = validatePassword(process.env.PROVIDER_ADMIN_PASSWORD || "");
  const name = normalizeName(process.env.PROVIDER_ADMIN_NAME || "Provider Admin");
  const passwordHash = await bcrypt.hash(password, 10);

  await runMigrations();
  await query(
    `INSERT INTO provider_admin_users (email, password_hash, name, status)
     VALUES (?, ?, ?, 'ACTIVE')
     ON DUPLICATE KEY UPDATE
       password_hash = VALUES(password_hash),
       name = VALUES(name),
       status = 'ACTIVE'`,
    [email, passwordHash, name]
  );

  console.log("Provider admin seeded:", { email, name });
}

run()
  .then(async () => {
    await closePool();
    process.exit(0);
  })
  .catch(async (err) => {
    console.error(err);
    await closePool();
    process.exit(1);
  });
