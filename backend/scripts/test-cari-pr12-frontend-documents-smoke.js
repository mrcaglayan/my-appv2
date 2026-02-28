import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function hasPath(source, pathValue) {
  const escaped = pathValue.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
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
  const page = await readFile(path.resolve(root, "frontend/src/pages/cari/CariDocumentsPage.jsx"), "utf8");
  const utils = await readFile(
    path.resolve(root, "frontend/src/pages/cari/cariDocumentsUtils.js"),
    "utf8"
  );
  const messages = await readFile(path.resolve(root, "frontend/src/i18n/messages.js"), "utf8");
  const implementedRoutesBlock = getImplementedRoutesBlock(app);

  assert(implementedRoutesBlock, "missing implementedRoutes block");
  assert(
    hasPath(implementedRoutesBlock, 'appPath: "/app/cari-belgeler"'),
    "missing /app/cari-belgeler route in implementedRoutes"
  );
  assert(
    implementedRoutesBlock.includes("element: <CariDocumentsPage />"),
    "cari-belgeler route must mount CariDocumentsPage"
  );
  assert(
    !/appPath:\s*["']\/app\/cari-belgeler["'][\s\S]{0,500}permissions\s*:/.test(
      implementedRoutesBlock
    ),
    "PR-12 route must not introduce route-level permissions field"
  );

  assert(
    page.includes("listCariDocuments") &&
      page.includes("createCariDocument") &&
      page.includes("updateCariDocument") &&
      page.includes("cancelCariDocument") &&
      page.includes("postCariDocument") &&
      page.includes("reverseCariDocument"),
    "page should use all cariDocuments API functions"
  );
  assert(
    page.includes("Date From") && page.includes("Date To"),
    "document page should render date range filters"
  );
  assert(
    page.includes("Create Draft Document") &&
      page.includes("Update Draft Document") &&
      page.includes("Cancel Draft") &&
      page.includes("Post Draft") &&
      page.includes("Reverse Posted Document"),
    "document page should render expected action labels/buttons"
  );
  assert(
    page.includes("reversalOfDocumentId"),
    "document page should reference reversalOfDocumentId"
  );
  assert(
    !page.includes("reversedDocumentId"),
    "document page must not reference reversedDocumentId"
  );
  assert(
    page.includes("response?.row?.id") &&
      page.includes("response?.row?.documentNo") &&
      page.includes("response?.journal?.reversalJournalEntryId"),
    "reverse action should read nested response.row/journal linkage fields"
  );
  assert(
    utils.includes("dateFrom: filters.dateFrom || filters.documentDateFrom || undefined"),
    "utils should support dateFrom alias mapping"
  );
  assert(
    utils.includes("dateTo: filters.dateTo || filters.documentDateTo || undefined"),
    "utils should support dateTo alias mapping"
  );
  assert(
    /cariDocuments\s*:\s*\{/.test(messages),
    "messages should include cariDocuments section"
  );

  console.log("PR-12 frontend documents smoke passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
