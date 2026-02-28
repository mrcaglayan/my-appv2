import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createEmptyContractFinancialRollup,
  normalizeContractFinancialRollup,
} from "../../frontend/src/pages/contracts/contractsUtils.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
  const pageSource = await readFile(
    path.resolve(root, "frontend/src/pages/contracts/ContractsPage.jsx"),
    "utf8"
  );
  const utilsSource = await readFile(
    path.resolve(root, "frontend/src/pages/contracts/contractsUtils.js"),
    "utf8"
  );

  assert(
    pageSource.includes("Financial Rollups"),
    "ContractsPage should render the Financial Rollups section"
  );
  assert(
    pageSource.includes("normalizeContractFinancialRollup"),
    "ContractsPage should normalize financialRollup payloads"
  );
  assert(
    pageSource.includes("collectionProgressPct") && pageSource.includes("recognitionProgressPct"),
    "ContractsPage should render collection/recognition progress indicators"
  );
  assert(
    pageSource.includes("openReceivableBase") || pageSource.includes("openPayableBase"),
    "ContractsPage should render open receivable/payable fields"
  );

  assert(
    utilsSource.includes("createEmptyContractFinancialRollup"),
    "contractsUtils should expose createEmptyContractFinancialRollup()"
  );
  assert(
    utilsSource.includes("normalizeContractFinancialRollup"),
    "contractsUtils should expose normalizeContractFinancialRollup()"
  );

  const emptyCustomer = createEmptyContractFinancialRollup("CUSTOMER");
  assert(emptyCustomer.billedAmountBase === 0, "Empty customer rollup should start at zero");
  assert(emptyCustomer.collectedAmountBase === 0, "Empty customer collected should be zero");
  assert(emptyCustomer.openReceivableBase === 0, "Empty customer openReceivable should be zero");
  assert(emptyCustomer.openPayableBase === 0, "Empty customer openPayable should be zero");

  const emptyVendor = createEmptyContractFinancialRollup("VENDOR");
  assert(emptyVendor.openReceivableBase === 0, "Empty vendor openReceivable should be zero");
  assert(emptyVendor.openPayableBase === 0, "Empty vendor openPayable should be zero");

  const normalized = normalizeContractFinancialRollup(
    {
      billedAmountBase: "10000.50",
      collectedAmountBase: "6000.25",
      collectedCoveragePct: "60",
      linkedDocumentCount: "2",
      currencyCode: "usd",
    },
    "CUSTOMER"
  );
  assert(normalized.billedAmountBase === 10000.5, "Normalized billedAmountBase should be numeric");
  assert(
    normalized.collectedAmountBase === 6000.25,
    "Normalized collectedAmountBase should be numeric"
  );
  assert(normalized.collectedCoveragePct === 60, "Normalized collectedCoveragePct should be numeric");
  assert(normalized.linkedDocumentCount === 2, "Normalized linkedDocumentCount should be numeric");
  assert(normalized.currencyCode === "USD", "Normalized currencyCode should be uppercase");
  assert(normalized.uncollectedAmountBase === 0, "Missing numeric fields should default to zero");

  console.log("Contracts PR-24 frontend rollups smoke passed.");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
