import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function assert(c, m) {
  if (!c) throw new Error(m);
}
function hasPath(source, pathValue) {
  const escaped = pathValue.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(escaped).test(source);
}
function getImplementedRoutesBlock(source) {
  const start = source.indexOf("const implementedRoutes = [");
  if (start < 0) return "";
  const end = source.indexOf("const implementedPaths =", start);
  if (end < 0) return source.slice(start);
  return source.slice(start, end);
}
function countOccurrences(source, literal) {
  const escaped = literal.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(escaped, "g");
  return (source.match(re) || []).length;
}
function hasCariCommonImport(source) {
  return /from\s+['"]\.\/cariCommon\.js['"]/.test(source);
}

async function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
  const app = await readFile(path.resolve(root, "frontend/src/App.jsx"), "utf8");
  const sidebar = await readFile(path.resolve(root, "frontend/src/layouts/sidebarConfig.js"), "utf8");
  const i18nMessages = await readFile(path.resolve(root, "frontend/src/i18n/messages.js"), "utf8");
  const apiCommon = await readFile(path.resolve(root, "frontend/src/api/cariCommon.js"), "utf8");
  const apiDocs = await readFile(path.resolve(root, "frontend/src/api/cariDocuments.js"), "utf8");
  const apiSettle = await readFile(path.resolve(root, "frontend/src/api/cariSettlements.js"), "utf8");
  const apiAudit = await readFile(path.resolve(root, "frontend/src/api/cariAudit.js"), "utf8");
  const apiCounterparty = await readFile(
    path.resolve(root, "frontend/src/api/cariCounterparty.js"),
    "utf8"
  );
  const apiPaymentTerms = await readFile(
    path.resolve(root, "frontend/src/api/cariPaymentTerms.js"),
    "utf8"
  );
  const apiReports = await readFile(path.resolve(root, "frontend/src/api/cariReports.js"), "utf8");
  const implementedRoutesBlock = getImplementedRoutesBlock(app);

  assert(implementedRoutesBlock, "missing implementedRoutes block in App.jsx");
  assert(
    hasPath(implementedRoutesBlock, "/app/cari-belgeler"),
    "missing /app/cari-belgeler in implementedRoutes"
  );
  assert(
    hasPath(implementedRoutesBlock, "/app/cari-settlements"),
    "missing /app/cari-settlements in implementedRoutes"
  );
  assert(
    hasPath(implementedRoutesBlock, "/app/cari-audit"),
    "missing /app/cari-audit in implementedRoutes"
  );
  assert(hasPath(sidebar, "/app/cari-belgeler"), "missing sidebar cari-belgeler");
  assert(hasPath(sidebar, "/app/cari-settlements"), "missing sidebar cari-settlements");
  assert(hasPath(sidebar, "/app/cari-audit"), "missing sidebar cari-audit");
  assert(
    /sidebar\s*:\s*\{[\s\S]*?byPath\s*:\s*\{/.test(i18nMessages),
    "missing i18n sidebar.byPath structure"
  );
  assert(
    countOccurrences(i18nMessages, "/app/cari-belgeler") >= 2,
    "missing i18n key /app/cari-belgeler in tr/en maps"
  );
  assert(
    countOccurrences(i18nMessages, "/app/cari-settlements") >= 2,
    "missing i18n key /app/cari-settlements in tr/en maps"
  );
  assert(
    countOccurrences(i18nMessages, "/app/cari-audit") >= 2,
    "missing i18n key /app/cari-audit in tr/en maps"
  );
  assert(apiCommon.includes("parseCariApiError"), "cariCommon parser missing");
  assert(apiCommon.includes("toCariQueryString"), "cariCommon query helper missing");
  assert(apiCommon.includes("response"), "cariCommon should keep axios-compatible response shape");
  assert(apiDocs.includes("/api/v1/cari/documents"), "docs api path missing");
  assert(apiSettle.includes("/api/v1/cari/settlements/apply"), "settlement api path missing");
  assert(apiAudit.includes("/api/v1/cari/audit"), "audit api path missing");
  assert(hasCariCommonImport(apiCounterparty), "cariCounterparty should use cariCommon");
  assert(hasCariCommonImport(apiPaymentTerms), "cariPaymentTerms should use cariCommon");
  assert(hasCariCommonImport(apiReports), "cariReports should use cariCommon");

  console.log("PR-11 smoke passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
