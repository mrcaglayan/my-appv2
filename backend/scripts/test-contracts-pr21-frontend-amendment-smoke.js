import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildContractLinePatchPayload,
  createEmptyContractLine,
  mapContractDetailToForm,
  validateContractLinePatchForm,
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
  const apiSource = await readFile(
    path.resolve(root, "frontend/src/api/contracts.js"),
    "utf8"
  );

  assert(pageSource.includes("amendContract"), "ContractsPage should call amendContract()");
  assert(pageSource.includes("patchContractLine"), "ContractsPage should call patchContractLine()");
  assert(pageSource.includes("handlePatchLine"), "ContractsPage should implement handlePatchLine()");
  assert(pageSource.includes("Apply Amendment"), "ContractsPage should render Apply Amendment action");
  assert(pageSource.includes("Patch Line"), "ContractsPage should render Patch Line action");
  assert(
    pageSource.includes("Amend Active/Suspended"),
    "ContractsPage should render amendment mode title"
  );

  assert(
    apiSource.includes("export async function amendContract"),
    "contracts API client should expose amendContract()"
  );
  assert(
    apiSource.includes("export async function patchContractLine"),
    "contracts API client should expose patchContractLine()"
  );
  assert(
    apiSource.includes("export async function listContractAmendments"),
    "contracts API client should expose listContractAmendments()"
  );

  const emptyLine = createEmptyContractLine();
  assert(
    Object.prototype.hasOwnProperty.call(emptyLine, "id"),
    "Empty contract line should include id field"
  );

  const mappedForm = mapContractDetailToForm({
    legalEntityId: 1,
    counterpartyId: 2,
    contractNo: "CTR-1",
    contractType: "CUSTOMER",
    currencyCode: "USD",
    startDate: "2026-01-01",
    lines: [
      {
        id: 77,
        description: "Line",
        lineAmountTxn: 10,
        lineAmountBase: 10,
        recognitionMethod: "MANUAL",
        status: "ACTIVE",
      },
    ],
  });
  assert(mappedForm.lines[0]?.id === "77", "mapContractDetailToForm should preserve line id");

  const invalidPatch = validateContractLinePatchForm(
    {
      description: "",
      lineAmountTxn: "0",
      lineAmountBase: "",
      recognitionMethod: "STRAIGHT_LINE",
      recognitionStartDate: "",
      recognitionEndDate: "",
      status: "ACTIVE",
    },
    ""
  );
  assert(invalidPatch.errors.length >= 3, "Invalid patch payload should return validation errors");

  const validPatch = validateContractLinePatchForm(
    {
      description: "Patched line",
      lineAmountTxn: "25.5",
      lineAmountBase: "25.5",
      recognitionMethod: "MANUAL",
      recognitionStartDate: "",
      recognitionEndDate: "",
      deferredAccountId: "",
      revenueAccountId: "",
      status: "ACTIVE",
    },
    "Manual correction"
  );
  assert(validPatch.errors.length === 0, "Valid line patch payload should pass validation");
  const requestPayload = buildContractLinePatchPayload(validPatch.payload, "Manual correction");
  assert(requestPayload.description === "Patched line", "Patch payload should normalize description");
  assert(requestPayload.lineAmountTxn === 25.5, "Patch payload should normalize lineAmountTxn");
  assert(requestPayload.lineAmountBase === 25.5, "Patch payload should normalize lineAmountBase");
  assert(requestPayload.reason === "Manual correction", "Patch payload should normalize reason");

  const invalidMilestonePatch = validateContractLinePatchForm(
    {
      description: "Milestone line",
      lineAmountTxn: "5",
      lineAmountBase: "5",
      recognitionMethod: "MILESTONE",
      recognitionStartDate: "2026-01-01",
      recognitionEndDate: "2026-01-15",
      deferredAccountId: "",
      revenueAccountId: "",
      status: "ACTIVE",
    },
    "Milestone correction"
  );
  assert(
    invalidMilestonePatch.errors.some((message) => message.includes("must match for MILESTONE")),
    "MILESTONE validation should require equal start/end dates"
  );

  const invalidManualPatch = validateContractLinePatchForm(
    {
      description: "Manual line",
      lineAmountTxn: "5",
      lineAmountBase: "5",
      recognitionMethod: "MANUAL",
      recognitionStartDate: "2026-01-01",
      recognitionEndDate: "2026-01-01",
      deferredAccountId: "",
      revenueAccountId: "",
      status: "ACTIVE",
    },
    "Manual correction"
  );
  assert(
    invalidManualPatch.errors.some((message) => message.includes("must be omitted for MANUAL")),
    "MANUAL validation should reject recognition dates"
  );

  const validMilestonePatch = validateContractLinePatchForm(
    {
      description: "Milestone valid line",
      lineAmountTxn: "5",
      lineAmountBase: "5",
      recognitionMethod: "MILESTONE",
      recognitionStartDate: "2026-01-01",
      recognitionEndDate: "2026-01-01",
      deferredAccountId: "",
      revenueAccountId: "",
      status: "ACTIVE",
    },
    "Milestone correction"
  );
  assert(
    validMilestonePatch.errors.length === 0,
    "MILESTONE validation should accept equal recognition dates"
  );

  const validNegativePatch = validateContractLinePatchForm(
    {
      description: "Credit correction",
      lineAmountTxn: "-12.5",
      lineAmountBase: "-12.5",
      recognitionMethod: "MANUAL",
      recognitionStartDate: "",
      recognitionEndDate: "",
      deferredAccountId: "",
      revenueAccountId: "",
      status: "ACTIVE",
    },
    "Credit note"
  );
  assert(
    validNegativePatch.errors.length === 0,
    "Negative line amounts should be accepted for line patch"
  );
  const negativePayload = buildContractLinePatchPayload(validNegativePatch.payload, "Credit note");
  assert(
    negativePayload.lineAmountTxn === -12.5,
    "Negative patch payload should preserve signed lineAmountTxn"
  );
  assert(
    negativePayload.lineAmountBase === -12.5,
    "Negative patch payload should preserve signed lineAmountBase"
  );

  console.log("PR-21/PR-22/PR-24 frontend amendment + signed-line smoke passed.");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
