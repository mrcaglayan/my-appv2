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

function extractWindow(source, marker, span = 480) {
  const start = source.indexOf(marker);
  if (start < 0) {
    return "";
  }
  return source.slice(start, start + span);
}

function countOccurrences(source, literal) {
  const escaped = literal.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pattern = new RegExp(escaped, "g");
  return (source.match(pattern) || []).length;
}

function assertRouteAlias(implementedRoutesBlock, aliasPath) {
  const aliasWindow = extractWindow(implementedRoutesBlock, `appPath: "${aliasPath}"`, 420);
  assert(aliasWindow, `missing ${aliasPath} alias route in implementedRoutes`);
  assert(
    aliasWindow.includes('permissionPath: PERIODIZATION_REVENUE_CANONICAL_PATH'),
    `${aliasPath} alias should inherit canonical route permissions`
  );
  assert(
    aliasWindow.includes("Navigate to={PERIODIZATION_REVENUE_CANONICAL_PATH} replace"),
    `${aliasPath} alias should redirect to canonical route`
  );
}

async function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
  const app = await readFile(path.resolve(root, "frontend/src/App.jsx"), "utf8");
  const sidebar = await readFile(path.resolve(root, "frontend/src/layouts/sidebarConfig.js"), "utf8");
  const messages = await readFile(path.resolve(root, "frontend/src/i18n/messages.js"), "utf8");

  const implementedRoutesBlock = getImplementedRoutesBlock(app);
  assert(implementedRoutesBlock, "missing implementedRoutes block in App.jsx");
  assert(
    app.includes(
      'const PERIODIZATION_REVENUE_CANONICAL_PATH = "/app/gelecek-yillar-gelirleri";'
    ),
    "App.jsx should define a canonical periodization route constant"
  );

  const canonicalWindow = extractWindow(
    implementedRoutesBlock,
    "appPath: PERIODIZATION_REVENUE_CANONICAL_PATH",
    300
  );
  assert(canonicalWindow, "canonical periodization route should use constant appPath");
  assert(
    canonicalWindow.includes("element: <FutureYearRevenuePage />"),
    "canonical periodization route should render <FutureYearRevenuePage />"
  );
  assert(
    app.includes("route.permissionPath || route.appPath"),
    "implemented route mapper should support permissionPath override for aliases"
  );

  assertRouteAlias(implementedRoutesBlock, "/app/donemsellik-ve-tahakkuklar");
  assertRouteAlias(implementedRoutesBlock, "/app/periodization-and-accruals");

  assert(
    /label:\s*"Donemsellik ve Tahakkuklar"[\s\S]{0,220}to:\s*"\/app\/gelecek-yillar-gelirleri"/.test(
      sidebar
    ),
    "revenue sidebar label should be renamed to Donemsellik ve Tahakkuklar"
  );
  const revenueSidebarWindow = extractWindow(sidebar, 'to: "/app/gelecek-yillar-gelirleri"', 520);
  assert(
    revenueSidebarWindow.includes('"revenue.schedule.read"') &&
      revenueSidebarWindow.includes('"revenue.run.read"') &&
      revenueSidebarWindow.includes('"revenue.report.read"'),
    "revenue sidebar permissions must remain unchanged"
  );

  assert(
    countOccurrences(messages, "/app/gelecek-yillar-gelirleri") >= 2,
    "messages.js must keep canonical /app/gelecek-yillar-gelirleri mapping in tr and en"
  );
  assert(
    countOccurrences(messages, "/app/donemsellik-ve-tahakkuklar") >= 2,
    "messages.js must include /app/donemsellik-ve-tahakkuklar alias mapping in tr and en"
  );
  assert(
    countOccurrences(messages, "/app/periodization-and-accruals") >= 2,
    "messages.js must include /app/periodization-and-accruals alias mapping in tr and en"
  );

  console.log("PR-28 frontend rename + route alias smoke passed.");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
