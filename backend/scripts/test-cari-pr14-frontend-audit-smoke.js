import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

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
    path.resolve(root, "frontend/src/pages/cari/CariAuditPage.jsx"),
    "utf8"
  );
  const messages = await readFile(path.resolve(root, "frontend/src/i18n/messages.js"), "utf8");
  const utilsPath = path.resolve(root, "frontend/src/pages/cari/cariAuditUtils.js");
  const utilsSource = await readFile(utilsPath, "utf8");
  const implementedRoutesBlock = getImplementedRoutesBlock(app);

  assert(implementedRoutesBlock, "missing implementedRoutes block");
  assert(
    hasText(implementedRoutesBlock, 'appPath: "/app/cari-audit"'),
    "missing /app/cari-audit route in implementedRoutes"
  );
  assert(
    hasText(implementedRoutesBlock, "element: <CariAuditPage />"),
    "cari-audit route should mount CariAuditPage"
  );
  assert(
    !/appPath:\s*["']\/app\/cari-audit["'][\s\S]*?ModulePlaceholderPage/.test(
      implementedRoutesBlock
    ),
    "cari-audit route must not remain on ModulePlaceholderPage"
  );

  assert(
    page.includes("listCariAudit") &&
      page.includes("toDayBoundsForAuditFilters") &&
      page.includes("includePayload"),
    "page should use audit api + date-bound helper + includePayload toggle"
  );
  assert(
    page.includes("legalEntityId") &&
      page.includes("action") &&
      page.includes("resourceType") &&
      page.includes("resourceId") &&
      page.includes("actorUserId") &&
      page.includes("requestId") &&
      page.includes("createdFrom") &&
      page.includes("createdTo"),
    "filter form should include all backend query params"
  );
  assert(
    page.includes("Copy") && page.includes("navigator") && page.includes("clipboard"),
    "requestId copy interaction is missing"
  );
  assert(
    page.includes("Expand payload") && page.includes("Collapse payload"),
    "payload collapsed/expand behavior is missing"
  );

  assert(
    /cariAudit\s*:\s*\{/.test(messages),
    "messages.js must include cariAudit section"
  );

  assert(
    utilsSource.includes("timezoneOffsetMinutes"),
    "cariAuditUtils should support timezone offset coverage"
  );
  assert(
    utilsSource.includes("23") && utilsSource.includes("59") && utilsSource.includes("999"),
    "cariAuditUtils should include end-of-day conversion"
  );

  const { toDayBoundsForAuditFilters } = await import(pathToFileURL(utilsPath).href);
  const utcPlus430Bounds = toDayBoundsForAuditFilters(
    { createdFrom: "2026-06-15", createdTo: "2026-06-15" },
    { timezoneOffsetMinutes: 270 }
  );
  assert(
    utcPlus430Bounds.createdFrom === "2026-06-14T19:30:00.000Z",
    "UTC+04:30 createdFrom conversion should map to previous-day 19:30:00.000Z"
  );
  assert(
    utcPlus430Bounds.createdTo === "2026-06-15T19:29:59.999Z",
    "UTC+04:30 createdTo conversion should keep end-of-day inclusion boundary"
  );
  assert(
    new Date(utcPlus430Bounds.createdTo).getTime() -
      new Date(utcPlus430Bounds.createdFrom).getTime() ===
      24 * 60 * 60 * 1000 - 1,
    "day-bound range should be exactly 24h-1ms"
  );

  const emptyBounds = toDayBoundsForAuditFilters({ createdFrom: "", createdTo: "" });
  assert(
    emptyBounds.createdFrom === "" && emptyBounds.createdTo === "",
    "empty date filters should remain empty"
  );

  console.log("PR-14 frontend audit smoke passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
