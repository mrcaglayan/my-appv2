import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
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

function countOccurrences(source, literal) {
  const escaped = literal.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pattern = new RegExp(escaped, "g");
  return (source.match(pattern) || []).length;
}

function extractWindow(source, marker, span = 480) {
  const start = source.indexOf(marker);
  if (start < 0) {
    return "";
  }
  return source.slice(start, start + span);
}

async function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
  const app = await readFile(path.resolve(root, "frontend/src/App.jsx"), "utf8");
  const sidebar = await readFile(path.resolve(root, "frontend/src/layouts/sidebarConfig.js"), "utf8");
  const messages = await readFile(path.resolve(root, "frontend/src/i18n/messages.js"), "utf8");

  const implementedRoutesBlock = getImplementedRoutesBlock(app);
  assert(implementedRoutesBlock, "missing implementedRoutes block in App.jsx");

  assert(
    implementedRoutesBlock.includes('appPath: "/app/contracts"'),
    "missing /app/contracts in implementedRoutes"
  );
  assert(
    implementedRoutesBlock.includes('element: <ContractsPage />'),
    "/app/contracts must use <ContractsPage />"
  );
  const hasRevenueRoutePath =
    implementedRoutesBlock.includes('appPath: "/app/gelecek-yillar-gelirleri"') ||
    implementedRoutesBlock.includes("appPath: PERIODIZATION_REVENUE_CANONICAL_PATH");
  assert(hasRevenueRoutePath, "missing /app/gelecek-yillar-gelirleri in implementedRoutes");
  assert(
    implementedRoutesBlock.includes('element: <FutureYearRevenuePage />'),
    "/app/gelecek-yillar-gelirleri must use <FutureYearRevenuePage />"
  );

  const contractsSidebarWindow = extractWindow(sidebar, 'to: "/app/contracts"');
  assert(contractsSidebarWindow, "missing /app/contracts sidebar item");
  assert(
    /requiredPermissions:\s*\[\s*"contract\.read"\s*\]/.test(contractsSidebarWindow),
    "/app/contracts sidebar item must require contract.read"
  );
  assert(
    /implemented:\s*true/.test(contractsSidebarWindow),
    "/app/contracts sidebar item must set implemented: true"
  );

  const revenueSidebarWindow = extractWindow(sidebar, 'to: "/app/gelecek-yillar-gelirleri"', 700);
  assert(revenueSidebarWindow, "missing /app/gelecek-yillar-gelirleri sidebar item");
  assert(
    revenueSidebarWindow.includes('"revenue.schedule.read"'),
    "revenue sidebar item missing revenue.schedule.read"
  );
  assert(
    revenueSidebarWindow.includes('"revenue.run.read"'),
    "revenue sidebar item missing revenue.run.read"
  );
  assert(
    revenueSidebarWindow.includes('"revenue.report.read"'),
    "revenue sidebar item missing revenue.report.read"
  );
  assert(
    /implemented:\s*true/.test(revenueSidebarWindow),
    "revenue sidebar item must set implemented: true"
  );

  assert(
    countOccurrences(messages, "/app/contracts") >= 2,
    "messages.js must contain /app/contracts in tr and en sidebar.byPath maps"
  );
  assert(
    countOccurrences(messages, "/app/gelecek-yillar-gelirleri") >= 2,
    "messages.js must contain /app/gelecek-yillar-gelirleri in tr and en sidebar.byPath maps"
  );

  console.log("PR-18 frontend smoke passed.");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

