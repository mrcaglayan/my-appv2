import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  REVREC_GENERATION_MODES,
  buildContractRevrecPayload,
  createInitialRevrecForm,
  resolveContractsPermissionGates,
  validateContractRevrecForm,
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

  assert(
    apiSource.includes("export async function generateContractRevrec"),
    "contracts API client should expose generateContractRevrec()"
  );

  assert(
    pageSource.includes("generateContractRevrec"),
    "ContractsPage should import/call generateContractRevrec()"
  );
  assert(
    pageSource.includes("handleGenerateRevrec"),
    "ContractsPage should implement handleGenerateRevrec()"
  );
  assert(
    pageSource.includes("Generate RevRec Schedule"),
    "ContractsPage should render RevRec generation section"
  );
  assert(
    pageSource.includes("sourceCariDocumentId"),
    "ContractsPage should render sourceCariDocumentId control for linked mode"
  );
  assert(
    pageSource.includes("gates.canGenerateRevrec"),
    "ContractsPage should gate RevRec action by revenue.schedule.generate"
  );

  assert(
    Array.isArray(REVREC_GENERATION_MODES) &&
      REVREC_GENERATION_MODES.includes("BY_CONTRACT_LINE") &&
      REVREC_GENERATION_MODES.includes("BY_LINKED_DOCUMENT"),
    "contractsUtils should expose both RevRec generation modes"
  );

  const gatesMissing = resolveContractsPermissionGates(["contract.read"]);
  assert(
    gatesMissing.canGenerateRevrec === false,
    "RevRec gate should be false without revenue.schedule.generate permission"
  );

  const gatesPresent = resolveContractsPermissionGates([
    "contract.read",
    "revenue.schedule.generate",
  ]);
  assert(
    gatesPresent.canGenerateRevrec === true,
    "RevRec gate should be true with revenue.schedule.generate permission"
  );

  const initialForm = createInitialRevrecForm();
  assert(
    initialForm.generationMode === "BY_CONTRACT_LINE",
    "Initial RevRec form should default to BY_CONTRACT_LINE"
  );
  assert(
    initialForm.regenerateMissingOnly === true,
    "Initial RevRec form should default regenerateMissingOnly=true"
  );

  const invalidNoPeriod = validateContractRevrecForm({
    ...initialForm,
    fiscalPeriodId: "",
  });
  assert(
    invalidNoPeriod.errors.some((message) => message.includes("fiscalPeriodId is required")),
    "RevRec validation should require fiscalPeriodId"
  );

  const invalidLinkedMode = validateContractRevrecForm({
    fiscalPeriodId: "12",
    generationMode: "BY_LINKED_DOCUMENT",
    sourceCariDocumentId: "",
    regenerateMissingOnly: true,
    contractLineIds: ["10"],
  });
  assert(
    invalidLinkedMode.errors.some((message) => message.includes("sourceCariDocumentId is required")),
    "Linked-document mode should require sourceCariDocumentId"
  );

  const validByLine = validateContractRevrecForm({
    fiscalPeriodId: "22",
    generationMode: "BY_CONTRACT_LINE",
    sourceCariDocumentId: "",
    regenerateMissingOnly: true,
    contractLineIds: ["100", "101", "x"],
  });
  assert(validByLine.errors.length === 0, "BY_CONTRACT_LINE payload should validate");
  const byLinePayload = buildContractRevrecPayload(validByLine.payload);
  assert(byLinePayload.fiscalPeriodId === 22, "Payload should normalize fiscalPeriodId");
  assert(
    byLinePayload.sourceCariDocumentId === null,
    "BY_CONTRACT_LINE should normalize empty sourceCariDocumentId to null"
  );
  assert(
    Array.isArray(byLinePayload.contractLineIds) &&
      byLinePayload.contractLineIds.length === 2 &&
      byLinePayload.contractLineIds[0] === 100 &&
      byLinePayload.contractLineIds[1] === 101,
    "Payload should normalize and filter contractLineIds"
  );

  const validLinkedMode = validateContractRevrecForm({
    fiscalPeriodId: "33",
    generationMode: "BY_LINKED_DOCUMENT",
    sourceCariDocumentId: "44",
    regenerateMissingOnly: false,
    contractLineIds: ["201"],
  });
  assert(validLinkedMode.errors.length === 0, "Valid linked-document payload should pass");
  const linkedPayload = buildContractRevrecPayload(validLinkedMode.payload);
  assert(
    linkedPayload.generationMode === "BY_LINKED_DOCUMENT" &&
      linkedPayload.sourceCariDocumentId === 44 &&
      linkedPayload.regenerateMissingOnly === false,
    "Linked-document payload should preserve normalized mode/source/regenerateMissingOnly values"
  );

  console.log("Contracts PR-22 frontend RevRec smoke passed.");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
