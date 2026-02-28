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
  const docsSource = await readFile(
    path.resolve(root, "frontend/src/pages/cari/CariDocumentsPage.jsx"),
    "utf8"
  );
  const settlementsSource = await readFile(
    path.resolve(root, "frontend/src/pages/cari/CariSettlementsPage.jsx"),
    "utf8"
  );

  assert(
    docsSource.includes('import Combobox from "../../components/Combobox.jsx";'),
    "CariDocumentsPage should import shared Combobox"
  );
  assert(
    docsSource.includes("listCariCounterparties"),
    "CariDocumentsPage should load counterparties for typeahead"
  );
  assert(
    docsSource.includes("Counterparty Lookup"),
    "CariDocumentsPage should render counterparty typeahead field(s)"
  );
  assert(
    docsSource.includes("createCounterpartyLookupOptions"),
    "CariDocumentsPage should provide create-form lookup options"
  );
  assert(
    docsSource.includes("editCounterpartyLookupOptions"),
    "CariDocumentsPage should provide edit-form lookup options"
  );

  assert(
    settlementsSource.includes('import Combobox from "../../components/Combobox.jsx";'),
    "CariSettlementsPage should import shared Combobox"
  );
  assert(
    settlementsSource.includes("counterpartyLookupOptions"),
    "CariSettlementsPage should provide apply-form lookup options"
  );
  assert(
    settlementsSource.includes("bankApplyCounterpartyLookupOptions"),
    "CariSettlementsPage should provide bank-apply lookup options"
  );
  assert(
    settlementsSource.includes("Counterparty Lookup") &&
      settlementsSource.includes("counterpartyLookup"),
    "CariSettlementsPage should render counterparty typeahead fields"
  );

  console.log("PR-UX11 smoke test passed (counterparty typeahead wired in Cari Documents/Settlements).");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
