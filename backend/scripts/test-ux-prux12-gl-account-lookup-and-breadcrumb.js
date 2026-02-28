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

  const glReadRoutesSource = await readFile(
    path.resolve(root, "backend/src/routes/gl.read.routes.js"),
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
  const settlementsPageSource = await readFile(
    path.resolve(root, "frontend/src/pages/cari/CariSettlementsPage.jsx"),
    "utf8"
  );

  assert(
    glReadRoutesSource.includes("const q = normalizeSearchText(req.query.q);"),
    "GL account route should parse q query text"
  );
  assert(
    glReadRoutesSource.includes("(a.code LIKE ? OR a.name LIKE ?)"),
    "GL account route should apply q filter to code/name"
  );
  assert(
    glReadRoutesSource.includes("account_breadcrumb"),
    "GL account route should include breadcrumb fields in response rows"
  );

  assert(
    counterpartyPageSource.includes("queryText: createAccountLookupQuery") &&
      counterpartyPageSource.includes("queryText: editAccountLookupQuery"),
    "CariCounterpartyPage should wire lookup query text into account API loader"
  );
  assert(
    counterpartyPageSource.includes("q: normalizedQuery || undefined"),
    "CariCounterpartyPage should call listAccounts with q parameter"
  );
  assert(
    counterpartyPageSource.includes("onAccountLookupQueryChange={handleCreateAccountLookupInput}") &&
      counterpartyPageSource.includes("onAccountLookupQueryChange={handleEditAccountLookupInput}"),
    "CariCounterpartyPage should pass account lookup query handlers to form"
  );

  assert(
    counterpartyFormSource.includes("AR Control Account Override") &&
      counterpartyFormSource.includes("AP Control Account Override"),
    "CounterpartyForm should keep AR/AP account override fields"
  );
  assert(
    counterpartyFormSource.includes("onAccountLookupQueryChange"),
    "CounterpartyForm should expose lookup input callback for server-side q searches"
  );
  assert(
    counterpartyFormSource.includes("buildAccountLookupDescription"),
    "CounterpartyForm should render breadcrumb-capable account descriptions"
  );

  assert(
    settlementsPageSource.includes("linkedCashAccountLookupOptions"),
    "CariSettlementsPage should build linked cash account combobox options"
  );
  assert(
    settlementsPageSource.includes("mapGlAccountLookupOption"),
    "CariSettlementsPage should map account rows to lookup options with breadcrumb display"
  );
  assert(
    settlementsPageSource.includes("q: normalizedQuery || undefined"),
    "CariSettlementsPage should query GL accounts with q for linked cash counterAccount"
  );
  assert(
    settlementsPageSource.includes("counterAccount") &&
      settlementsPageSource.includes("setLinkedCashAccountQuery"),
    "CariSettlementsPage should wire counterAccount lookup input to q state"
  );

  console.log(
    "PR-UX12 smoke test passed (GL account q-search API + breadcrumb lookup wiring on Cari account selectors)."
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
