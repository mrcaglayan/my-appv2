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
  const helperSource = await readFile(
    path.resolve(root, "frontend/src/pages/cari/counterpartyInlineCreate.js"),
    "utf8"
  );
  const documentsSource = await readFile(
    path.resolve(root, "frontend/src/pages/cari/CariDocumentsPage.jsx"),
    "utf8"
  );
  const settlementsSource = await readFile(
    path.resolve(root, "frontend/src/pages/cari/CariSettlementsPage.jsx"),
    "utf8"
  );

  assert(
    helperSource.includes("export function buildInlineCounterpartyCode"),
    "Inline create helper should export code generator"
  );
  assert(
    helperSource.includes("export function resolveInlineCounterpartyRoleFlags"),
    "Inline create helper should export direction-to-role resolver"
  );
  assert(
    helperSource.includes("export function prependOrReplaceCounterpartyOption"),
    "Inline create helper should export option list upsert helper"
  );

  assert(
    documentsSource.includes("createCariCounterparty") &&
      documentsSource.includes("handleInlineCreateCounterpartyForCreateForm") &&
      documentsSource.includes("handleInlineCreateCounterpartyForEditForm"),
    "CariDocumentsPage should wire inline create handlers for create/edit forms"
  );
  assert(
    documentsSource.includes("canUpsertCards") &&
      documentsSource.includes("setCreateCounterpartyLookupQuery") &&
      documentsSource.includes("setEditCounterpartyLookupQuery"),
    "CariDocumentsPage should gate inline create by permission and track typed lookup input"
  );
  assert(
    documentsSource.includes('Create "${createInlineCounterpartyName || "new counterparty"}"') &&
      documentsSource.includes('Create "${editInlineCounterpartyName || "new counterparty"}"'),
    "CariDocumentsPage should expose inline create actions next to counterparty lookups"
  );

  assert(
    settlementsSource.includes("createCariCounterparty") &&
      settlementsSource.includes("handleInlineCreateCounterpartyForApplyForm") &&
      settlementsSource.includes("handleInlineCreateCounterpartyForBankApplyForm"),
    "CariSettlementsPage should wire inline create handlers for apply/bank-apply forms"
  );
  assert(
    settlementsSource.includes("setApplyCounterpartyLookupQuery") &&
      settlementsSource.includes("setBankApplyCounterpartyLookupQuery"),
    "CariSettlementsPage should track typed lookup input for inline create"
  );
  assert(
    settlementsSource.includes('Create "${applyInlineCounterpartyName || "new counterparty"}"') &&
      settlementsSource.includes(
        'Create "${bankApplyInlineCounterpartyName || "new counterparty"}"'
      ),
    "CariSettlementsPage should render inline create actions for both lookup contexts"
  );

  console.log(
    "PR-UX13-A smoke test passed (inline counterparty create wiring on Cari lookup flows)."
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
