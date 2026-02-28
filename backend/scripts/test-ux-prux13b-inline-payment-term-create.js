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

  const paymentTermRoutesSource = await readFile(
    path.resolve(root, "backend/src/routes/cari.payment-term.routes.js"),
    "utf8"
  );
  const counterpartyPageSource = await readFile(
    path.resolve(root, "frontend/src/pages/cari/CariCounterpartyPage.jsx"),
    "utf8"
  );
  const counterpartyFormSource = await readFile(
    path.resolve(root, "frontend/src/pages/cari/CounterpartyForm.jsx"),
    "utf8"
  );

  assert(
    paymentTermRoutesSource.includes("router.post(") &&
      paymentTermRoutesSource.includes('requirePermission("cari.card.upsert"'),
    "Payment term write route should remain permission-gated by cari.card.upsert"
  );

  assert(
    counterpartyPageSource.includes("createCariPaymentTerm") &&
      counterpartyPageSource.includes("runInlinePaymentTermCreate"),
    "CariCounterpartyPage should call createCariPaymentTerm via shared inline create helper"
  );
  assert(
    counterpartyPageSource.includes("handleInlineCreatePaymentTermForCreateForm") &&
      counterpartyPageSource.includes("handleInlineCreatePaymentTermForEditForm"),
    "CariCounterpartyPage should expose inline create handlers for create/edit forms"
  );
  assert(
    counterpartyPageSource.includes("onPaymentTermLookupQueryChange={handleCreatePaymentTermLookupInput}") &&
      counterpartyPageSource.includes("onPaymentTermLookupQueryChange={handleEditPaymentTermLookupInput}"),
    "CariCounterpartyPage should wire payment-term lookup query callbacks into both forms"
  );
  assert(
    counterpartyPageSource.includes("canInlineCreatePaymentTermInCreateForm") &&
      counterpartyPageSource.includes("canInlineCreatePaymentTermInEditForm"),
    "CariCounterpartyPage should gate inline payment-term creation by role/scope/form state"
  );
  assert(
    counterpartyPageSource.includes("buildInlinePaymentTermCode") &&
      counterpartyPageSource.includes("prependOrReplacePaymentTermOption"),
    "CariCounterpartyPage should generate inline payment-term codes and upsert created options"
  );

  assert(
    counterpartyFormSource.includes("onPaymentTermLookupQueryChange") &&
      counterpartyFormSource.includes("onInlineCreatePaymentTerm"),
    "CounterpartyForm should expose payment-term lookup input and inline create callbacks"
  );
  assert(
    counterpartyFormSource.includes('Create "${inlineCreatePaymentTermLabel || "new payment term"}"'),
    "CounterpartyForm should render inline payment-term create CTA with typed lookup label"
  );

  console.log(
    "PR-UX13-B smoke test passed (inline payment-term create wiring on counterparty payment-term lookups)."
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
