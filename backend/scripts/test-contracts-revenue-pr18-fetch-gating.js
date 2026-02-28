import { resolveContractsPermissionGates } from "../../frontend/src/pages/contracts/contractsUtils.js";
import { resolveRevenueFetchGates } from "../../frontend/src/pages/revenue/revenueFetchGating.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function assertRevenueCase(name, permissionCodes, expected) {
  const gates = resolveRevenueFetchGates(permissionCodes);
  assert(
    gates.canOpenRoute === expected.canOpenRoute,
    `${name}: canOpenRoute expected ${expected.canOpenRoute}, got ${gates.canOpenRoute}`
  );
  assert(
    gates.shouldFetchSchedules === expected.shouldFetchSchedules,
    `${name}: shouldFetchSchedules expected ${expected.shouldFetchSchedules}, got ${gates.shouldFetchSchedules}`
  );
  assert(
    gates.shouldFetchRuns === expected.shouldFetchRuns,
    `${name}: shouldFetchRuns expected ${expected.shouldFetchRuns}, got ${gates.shouldFetchRuns}`
  );
  assert(
    gates.shouldFetchReports === expected.shouldFetchReports,
    `${name}: shouldFetchReports expected ${expected.shouldFetchReports}, got ${gates.shouldFetchReports}`
  );
}

function assertContractsCase(name, permissionCodes, expected) {
  const gates = resolveContractsPermissionGates(permissionCodes);
  assert(
    gates.shouldFetchCounterparties === expected.shouldFetchCounterparties,
    `${name}: shouldFetchCounterparties expected ${expected.shouldFetchCounterparties}, got ${gates.shouldFetchCounterparties}`
  );
  assert(
    gates.shouldFetchAccounts === expected.shouldFetchAccounts,
    `${name}: shouldFetchAccounts expected ${expected.shouldFetchAccounts}, got ${gates.shouldFetchAccounts}`
  );
  assert(
    gates.shouldFetchDocuments === expected.shouldFetchDocuments,
    `${name}: shouldFetchDocuments expected ${expected.shouldFetchDocuments}, got ${gates.shouldFetchDocuments}`
  );
}

async function main() {
  assertRevenueCase("none", [], {
    canOpenRoute: false,
    shouldFetchSchedules: false,
    shouldFetchRuns: false,
    shouldFetchReports: false,
  });

  assertRevenueCase("schedule-only", ["revenue.schedule.read"], {
    canOpenRoute: true,
    shouldFetchSchedules: true,
    shouldFetchRuns: false,
    shouldFetchReports: false,
  });

  assertRevenueCase("run-only", ["revenue.run.read"], {
    canOpenRoute: true,
    shouldFetchSchedules: false,
    shouldFetchRuns: true,
    shouldFetchReports: false,
  });

  assertRevenueCase("report-only", ["revenue.report.read"], {
    canOpenRoute: true,
    shouldFetchSchedules: false,
    shouldFetchRuns: false,
    shouldFetchReports: true,
  });

  assertRevenueCase(
    "mixed-reads",
    ["revenue.schedule.read", "revenue.run.read", "revenue.report.read"],
    {
      canOpenRoute: true,
      shouldFetchSchedules: true,
      shouldFetchRuns: true,
      shouldFetchReports: true,
    }
  );

  assertContractsCase("contracts-no-picker-read", ["contract.read"], {
    shouldFetchCounterparties: false,
    shouldFetchAccounts: false,
    shouldFetchDocuments: false,
  });

  assertContractsCase("contracts-counterparty-read", ["contract.read", "cari.card.read"], {
    shouldFetchCounterparties: true,
    shouldFetchAccounts: false,
    shouldFetchDocuments: false,
  });

  assertContractsCase("contracts-account-read", ["contract.read", "gl.account.read"], {
    shouldFetchCounterparties: false,
    shouldFetchAccounts: true,
    shouldFetchDocuments: false,
  });

  assertContractsCase("contracts-document-read", ["contract.read", "contract.link_document"], {
    shouldFetchCounterparties: false,
    shouldFetchAccounts: false,
    shouldFetchDocuments: true,
  });

  assertContractsCase(
    "contracts-all-picker-reads",
    ["contract.read", "cari.card.read", "gl.account.read", "contract.link_document"],
    {
      shouldFetchCounterparties: true,
      shouldFetchAccounts: true,
      shouldFetchDocuments: true,
    }
  );

  console.log("PR-18 fetch-gating helper assertions passed.");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

