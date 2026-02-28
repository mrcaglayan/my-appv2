import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");

  const routeSource = await readFile(
    path.resolve(root, "backend/src/routes/cari.payment-term.routes.js"),
    "utf8"
  );
  const validatorSource = await readFile(
    path.resolve(root, "backend/src/routes/cari.payment-term.validators.js"),
    "utf8"
  );
  const serviceSource = await readFile(
    path.resolve(root, "backend/src/services/cari.payment-term.service.js"),
    "utf8"
  );
  const frontendApiSource = await readFile(
    path.resolve(root, "frontend/src/api/cariPaymentTerms.js"),
    "utf8"
  );

  assert(
    routeSource.includes('router.post(\n  "/"') &&
      routeSource.includes('requirePermission("cari.card.upsert"'),
    "Payment-term route should expose POST / with upsert permission guard"
  );
  assert(
    routeSource.includes("parsePaymentTermCreateInput") &&
      routeSource.includes("createPaymentTerm"),
    "Payment-term route should parse create payload and call create service"
  );
  assert(
    validatorSource.includes("export function parsePaymentTermCreateInput"),
    "Payment-term validators should export create payload parser"
  );
  assert(
    validatorSource.includes("legalEntityId is required") &&
      validatorSource.includes("dueDays") &&
      validatorSource.includes("graceDays"),
    "Payment-term create parser should validate legalEntity/dueDays/graceDays"
  );
  assert(
    serviceSource.includes("export async function createPaymentTerm") &&
      serviceSource.includes("INSERT INTO payment_terms"),
    "Payment-term service should implement createPaymentTerm insert flow"
  );
  assert(
    frontendApiSource.includes("export async function createCariPaymentTerm") &&
      frontendApiSource.includes('api.post("/api/v1/cari/payment-terms", payload)'),
    "Frontend payment-term API module should expose createCariPaymentTerm client"
  );

  console.log(
    "RS-DEP-01 smoke test passed (payment term write API + permission + frontend client wiring)."
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
