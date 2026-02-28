import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.resolve(scriptDir, "..", "..");

  const appPath = path.resolve(repoRoot, "frontend/src/App.jsx");
  const sidebarPath = path.resolve(repoRoot, "frontend/src/layouts/sidebarConfig.js");
  const pagePath = path.resolve(repoRoot, "frontend/src/pages/cari/CariReportsPage.jsx");

  const appSource = await readFile(appPath, "utf8");
  assert(
    appSource.includes('appPath: "/app/cari-raporlari"'),
    "App route for cari reports must be registered"
  );
  assert(
    appSource.includes("element: <CariReportsPage />"),
    "App route should mount CariReportsPage"
  );

  const sidebarSource = await readFile(sidebarPath, "utf8");
  assert(
    sidebarSource.includes('to: "/app/cari-raporlari"'),
    "Sidebar should include cari reports link"
  );
  assert(
    sidebarSource.includes('requiredPermissions: ["cari.report.read"]'),
    "Sidebar cari reports link should require cari.report.read"
  );

  const pageSource = await readFile(pagePath, "utf8");
  assert(
    pageSource.includes("As-Of Date") &&
      pageSource.includes("Customer / Vendor") &&
      pageSource.includes("Status"),
    "Cari reports page should render required filters"
  );
  assert(
    pageSource.includes("API vs row-total reconcile (open items)") &&
      pageSource.includes("Statement reconcile"),
    "Cari reports page should display reconciliation outputs"
  );

  const utils = await import("../../frontend/src/pages/cari/cariReportsUtils.js");

  const arQuery = utils.buildCariReportQuery(
    {
      asOfDate: "2026-04-30",
      legalEntityId: "11",
      counterpartyId: "21",
      role: "CUSTOMER",
      status: "OPEN",
      limit: 100,
      offset: 0,
    },
    utils.REPORT_TABS.AR_AGING
  );
  assert(arQuery.direction === "AR", "AR tab query must include direction=AR");
  assert(arQuery.legalEntityId === 11, "Query builder should normalize legalEntityId");

  const openReconcile = utils.reconcileOpenItemsSummary({
    summary: {
      residualAmountTxnTotal: 50,
      residualAmountBaseTotal: 50,
    },
    rows: [
      { residualAmountTxnAsOf: 20, residualAmountBaseAsOf: 20 },
      { residualAmountTxnAsOf: 30, residualAmountBaseAsOf: 30 },
    ],
  });
  assert(openReconcile.matches === true, "Open-items reconciliation should match equal totals");

  const statementReconcile = utils.reconcileStatementSummary({
    summary: {
      reconcile: {
        openResidualAmountTxnFromOpenItems: 70,
        openResidualAmountTxnFromDocuments: 70,
        openResidualAmountBaseFromOpenItems: 70,
        openResidualAmountBaseFromDocuments: 70,
      },
    },
  });
  assert(
    statementReconcile.matches === true,
    "Statement reconciliation should match equal open residual totals"
  );

  console.log("CARI PR-09 frontend reports smoke test passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
