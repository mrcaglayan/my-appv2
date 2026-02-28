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
  const transactionsSource = await readFile(
    path.resolve(root, "frontend/src/pages/cash/CashTransactionsPage.jsx"),
    "utf8"
  );
  const sessionsSource = await readFile(
    path.resolve(root, "frontend/src/pages/cash/CashSessionsPage.jsx"),
    "utf8"
  );

  assert(
    transactionsSource.includes('import StatusTimeline from "../../components/StatusTimeline.jsx";'),
    "CashTransactionsPage should import StatusTimeline"
  );
  assert(
    transactionsSource.includes("buildCashTransactionLifecycleEvents"),
    "CashTransactionsPage should map transaction timestamps into lifecycle events"
  );
  assert(
    transactionsSource.includes('buildLifecycleTimelineSteps(\n        "cashTransaction"') &&
      transactionsSource.includes('getLifecycleAllowedActions("cashTransaction"') &&
      transactionsSource.includes('getLifecycleStatusMeta("cashTransaction"'),
    "CashTransactionsPage should use shared lifecycle helpers for cashTransaction"
  );
  assert(
    transactionsSource.includes("selectedLifecycleTransactionId") &&
      transactionsSource.includes("cashTransactions.sections.lifecycle") &&
      transactionsSource.includes("cashTransactions.actions.inspectLifecycle"),
    "CashTransactionsPage should expose lifecycle inspection controls and section wiring"
  );
  assert(
    transactionsSource.includes("<StatusTimeline") &&
      transactionsSource.includes("cashTransactions.lifecycle.timelineTitle"),
    "CashTransactionsPage should render transaction StatusTimeline with lifecycle copy keys"
  );

  assert(
    sessionsSource.includes('import StatusTimeline from "../../components/StatusTimeline.jsx";'),
    "CashSessionsPage should import StatusTimeline"
  );
  assert(
    sessionsSource.includes("buildCashSessionLifecycleEvents"),
    "CashSessionsPage should map session timestamps into lifecycle events"
  );
  assert(
    sessionsSource.includes('buildLifecycleTimelineSteps(\n        "cashSession"') &&
      sessionsSource.includes('getLifecycleAllowedActions("cashSession"') &&
      sessionsSource.includes('getLifecycleStatusMeta("cashSession"'),
    "CashSessionsPage should use shared lifecycle helpers for cashSession"
  );
  assert(
    sessionsSource.includes("selectedLifecycleSessionId") &&
      sessionsSource.includes("cashSessions.sections.lifecycle") &&
      sessionsSource.includes("cashSessions.actions.inspectLifecycle"),
    "CashSessionsPage should expose lifecycle inspection controls and section wiring"
  );
  assert(
    sessionsSource.includes("<StatusTimeline") &&
      sessionsSource.includes("cashSessions.lifecycle.timelineTitle"),
    "CashSessionsPage should render session StatusTimeline with lifecycle copy keys"
  );

  console.log(
    "PR-UX16 smoke test passed (Cash Transactions/Sessions lifecycle snapshot + shared timeline wiring)."
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
