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
  const statusTimelinePath = path.resolve(
    root,
    "frontend/src/components/StatusTimeline.jsx"
  );

  const lifecycleModule = await import(pathToFileURL(lifecycleRulesPath).href);
  const statusTimelineSource = await readFile(statusTimelinePath, "utf8");

  const {
    LIFECYCLE_ENTITY_TYPES,
    getLifecycleDefinition,
    getLifecycleStatusMeta,
    getLifecycleAllowedActions,
    buildLifecycleTimelineSteps,
  } = lifecycleModule;

  assert(
    Array.isArray(LIFECYCLE_ENTITY_TYPES) &&
      LIFECYCLE_ENTITY_TYPES.includes("cariDocument") &&
      LIFECYCLE_ENTITY_TYPES.includes("cashTransaction") &&
      LIFECYCLE_ENTITY_TYPES.includes("cashSession") &&
      LIFECYCLE_ENTITY_TYPES.includes("payrollRun") &&
      LIFECYCLE_ENTITY_TYPES.includes("payrollClose"),
    "Lifecycle rules should expose canonical entity types"
  );

  const cashTxnDefinition = getLifecycleDefinition("cashTransaction");
  assert(
    cashTxnDefinition &&
      cashTxnDefinition.statuses.some((row) => row.code === "DRAFT") &&
      cashTxnDefinition.statuses.some((row) => row.code === "POSTED"),
    "cashTransaction lifecycle definition should include DRAFT and POSTED"
  );

  const postedMeta = getLifecycleStatusMeta("cariDocument", "posted");
  assert(
    postedMeta?.code === "POSTED" && /posted/i.test(String(postedMeta?.label || "")),
    "Lifecycle status metadata should normalize status lookup"
  );

  const actionsFromDraft = getLifecycleAllowedActions("cashTransaction", "DRAFT");
  const actionNames = new Set(actionsFromDraft.map((row) => row.action));
  assert(
    actionNames.has("submit") && actionNames.has("post") && actionNames.has("cancel"),
    "cashTransaction DRAFT should allow submit/post/cancel lifecycle actions"
  );

  const timeline = buildLifecycleTimelineSteps("payrollClose", "REQUESTED", [
    { statusCode: "READY", at: "2026-02-01T10:00:00.000Z", actorName: "ops.user" },
    { statusCode: "REQUESTED", at: "2026-02-01T11:00:00.000Z", actorName: "ops.user" },
  ]);
  const requestedRow = timeline.find((row) => row.statusCode === "REQUESTED");
  const closedRow = timeline.find((row) => row.statusCode === "CLOSED");
  assert(
    requestedRow?.state === "current" && requestedRow?.eventAt,
    "Timeline should mark the current lifecycle status and keep event metadata"
  );
  assert(
    closedRow?.state === "upcoming",
    "Timeline should keep later statuses in upcoming state"
  );

  assert(
    /export\s+default\s+function\s+StatusTimeline\s*\(/.test(statusTimelineSource),
    "StatusTimeline component export is missing"
  );
  assert(
    statusTimelineSource.includes("<ol") && statusTimelineSource.includes("stateClasses"),
    "StatusTimeline should render ordered lifecycle steps with visual state classes"
  );
  assert(
    statusTimelineSource.includes("No lifecycle data available."),
    "StatusTimeline should provide a lifecycle empty-state fallback"
  );

  console.log(
    "PR-UX14 smoke test passed (shared lifecycle rules + StatusTimeline component foundation)."
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
