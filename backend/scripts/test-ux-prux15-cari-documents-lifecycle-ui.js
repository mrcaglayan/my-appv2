import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
  const lifecycleRulesPath = path.resolve(
    root,
    "frontend/src/lifecycle/lifecycleRules.js"
  );
  const documentsPagePath = path.resolve(
    root,
    "frontend/src/pages/cari/CariDocumentsPage.jsx"
  );

  const lifecycleModule = await import(pathToFileURL(lifecycleRulesPath).href);
  const documentsSource = await readFile(documentsPagePath, "utf8");

  const { getLifecycleStatusMeta, getLifecycleAllowedActions, buildLifecycleTimelineSteps } =
    lifecycleModule;

  const postedMeta = getLifecycleStatusMeta("cariDocument", "posted");
  assert(
    postedMeta?.code === "POSTED",
    "cariDocument lifecycle should resolve POSTED status metadata"
  );

  const draftActions = getLifecycleAllowedActions("cariDocument", "DRAFT");
  const draftActionSet = new Set((draftActions || []).map((row) => row.action));
  assert(
    draftActionSet.has("post") && draftActionSet.has("cancel"),
    "cariDocument DRAFT should expose post/cancel lifecycle transitions"
  );

  const timeline = buildLifecycleTimelineSteps("cariDocument", "POSTED", [
    { statusCode: "DRAFT", at: "2026-01-01T09:00:00.000Z" },
    { statusCode: "POSTED", at: "2026-01-02T11:00:00.000Z" },
  ]);
  const postedStep = timeline.find((row) => row.statusCode === "POSTED");
  assert(
    postedStep?.state === "current" && postedStep?.eventAt,
    "cariDocument lifecycle timeline should mark POSTED as current with event metadata"
  );

  assert(
    documentsSource.includes('import StatusTimeline from "../../components/StatusTimeline.jsx";'),
    "CariDocumentsPage should import StatusTimeline component"
  );
  assert(
    documentsSource.includes("buildLifecycleTimelineSteps") &&
      documentsSource.includes("getLifecycleAllowedActions") &&
      documentsSource.includes("getLifecycleStatusMeta"),
    "CariDocumentsPage should use shared lifecycle rules helpers"
  );
  assert(
    documentsSource.includes("buildDocumentLifecycleEvents"),
    "CariDocumentsPage should map document timestamps into lifecycle events"
  );
  assert(
    documentsSource.includes("Lifecycle Snapshot") &&
      documentsSource.includes("Document Lifecycle Timeline"),
    "CariDocumentsPage should render lifecycle snapshot and timeline sections"
  );

  console.log(
    "PR-UX15 smoke test passed (Cari Documents lifecycle UI wired to shared StatusTimeline/rules)."
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
