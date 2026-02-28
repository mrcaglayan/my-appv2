import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolveContractsPermissionGates } from "../../frontend/src/pages/contracts/contractsUtils.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
  const pageSource = await readFile(
    path.resolve(root, "frontend/src/pages/contracts/ContractsPage.jsx"),
    "utf8"
  );
  const apiSource = await readFile(
    path.resolve(root, "frontend/src/api/contracts.js"),
    "utf8"
  );

  assert(
    apiSource.includes("export async function listContractLinkableDocuments"),
    "contracts API client should expose listContractLinkableDocuments()"
  );
  assert(
    apiSource.includes("/linkable-documents"),
    "contracts API client should call /linkable-documents endpoint"
  );

  assert(
    pageSource.includes("listContractLinkableDocuments"),
    "ContractsPage should use contract-scoped linkable-documents endpoint"
  );
  assert(
    !pageSource.includes("listCariDocuments"),
    "ContractsPage should not import/use direct cari documents API for link picker"
  );
  assert(
    pageSource.includes("Picker hidden: contract.link_document missing."),
    "ContractsPage should show contract permission fallback for document picker"
  );

  const noLinkPermission = resolveContractsPermissionGates(["contract.read"]);
  assert(
    noLinkPermission.shouldFetchDocuments === false,
    "Document picker fetch should be blocked when contract.link_document is missing"
  );

  const withLegacyCariReadOnly = resolveContractsPermissionGates([
    "contract.read",
    "cari.doc.read",
  ]);
  assert(
    withLegacyCariReadOnly.shouldFetchDocuments === false,
    "cari.doc.read alone should not enable contract-scoped linkable-documents fetch"
  );

  const withContractLinkPermission = resolveContractsPermissionGates([
    "contract.read",
    "contract.link_document",
  ]);
  assert(
    withContractLinkPermission.shouldFetchDocuments === true,
    "contract.link_document should enable contract-scoped linkable-documents fetch"
  );

  console.log("PR-25 frontend linkable-documents smoke passed.");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
