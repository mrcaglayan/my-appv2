import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function hasText(source, value) {
  const escaped = value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(escaped).test(source);
}

function getImplementedRoutesBlock(source) {
  const start = source.indexOf("const implementedRoutes = [");
  if (start < 0) {
    return "";
  }
  const end = source.indexOf("const implementedPaths =", start);
  if (end < 0) {
    return source.slice(start);
  }
  return source.slice(start, end);
}

async function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
  const app = await readFile(path.resolve(root, "frontend/src/App.jsx"), "utf8");
  const page = await readFile(
    path.resolve(root, "frontend/src/pages/cari/CariSettlementsPage.jsx"),
    "utf8"
  );
  const utils = await readFile(
    path.resolve(root, "frontend/src/pages/cari/cariSettlementsUtils.js"),
    "utf8"
  );
  const idempotency = await readFile(
    path.resolve(root, "frontend/src/pages/cari/cariIdempotency.js"),
    "utf8"
  );
  const implementedRoutesBlock = getImplementedRoutesBlock(app);

  assert(implementedRoutesBlock, "missing implementedRoutes block");
  assert(
    hasText(implementedRoutesBlock, 'appPath: "/app/cari-settlements"'),
    "missing /app/cari-settlements route in implementedRoutes"
  );
  assert(
    hasText(implementedRoutesBlock, "element: <CariSettlementsPage />"),
    "cari-settlements route must mount CariSettlementsPage"
  );
  assert(
    /appPath:\s*["']\/app\/cari-settlements["'][\s\S]*?element:\s*<CariSettlementsPage\s*\/>/.test(
      implementedRoutesBlock
    ),
    "cari-settlements route entry should mount CariSettlementsPage directly"
  );

  assert(
    page.includes("hasPermission(\"cari.settlement.apply\")") &&
      page.includes("hasPermission(\"cari.settlement.reverse\")") &&
      page.includes("hasPermission(\"cari.bank.attach\")") &&
      page.includes("hasPermission(\"cari.bank.apply\")"),
    "action-level permission checks are missing"
  );
  assert(
    page.includes("getCariOpenItemsReport") &&
      page.includes("loadPendingIdempotencyKey") &&
      page.includes("createPendingIdempotencyKey") &&
      page.includes("createEphemeralIdempotencyKey"),
    "preview/idempotency wiring is missing"
  );
  assert(
    page.includes("Direction is required for auto-allocation."),
    "auto-allocate direction guard message is missing"
  );
  assert(
    page.includes("bankApplyIdempotencyKey") &&
      page.includes("idempotencyKey"),
    "bank/apply idempotency key fields are missing"
  );
  assert(
    page.includes("disabled={!canBankAttach || bankAttachSubmitting}") &&
      page.includes("disabled={!canBankApply || bankApplySubmitting}"),
    "bank in-flight submit guards are missing"
  );
  assert(
    page.includes("Bu istek daha once uygulanmis; mevcut sonuc gosteriliyor."),
    "idempotent replay info message is missing"
  );

  assert(
    utils.includes("residualAmountTxnAsOf"),
    "auto-allocation preview must use residualAmountTxnAsOf"
  );
  assert(
    utils.includes("allocations: Array.isArray(form.allocations) ? form.allocations : []"),
    "settlement payload allocations mapping is missing"
  );

  assert(
    idempotency.includes('const STORAGE_PREFIX = "cari:settlement:pending"'),
    "idempotency storage prefix is missing"
  );
  assert(
    idempotency.includes("legalEntityId") &&
      idempotency.includes("counterpartyId") &&
      idempotency.includes("direction"),
    "idempotency intent scope should include legalEntityId/counterpartyId/direction"
  );
  assert(
    idempotency.includes("currencyCode") &&
      idempotency.includes("incomingAmountTxn") &&
      idempotency.includes("settlementDate"),
    "idempotency fingerprint should include currencyCode/incomingAmountTxn/settlementDate"
  );
  assert(
    idempotency.includes("status === 400 || status === 401 || status === 403"),
    "final-failure key cleanup classification is missing"
  );

  console.log("PR-13 frontend settlement smoke passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
