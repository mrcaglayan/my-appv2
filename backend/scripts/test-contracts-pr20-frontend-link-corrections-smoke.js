import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildContractLinkPayload,
  buildContractLinkAdjustmentPayload,
  buildContractLinkUnlinkPayload,
  canAdjustContractLink,
  canUnlinkContractLink,
  createInitialLinkForm,
  createInitialLinkAdjustmentForm,
  createInitialLinkUnlinkForm,
  resolveContractsPermissionGates,
  validateContractLinkForm,
  validateContractLinkAdjustmentForm,
  validateContractLinkUnlinkForm,
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
    pageSource.includes("adjustContractDocumentLink"),
    "ContractsPage should call adjustContractDocumentLink()"
  );
  assert(
    pageSource.includes("unlinkContractDocumentLink"),
    "ContractsPage should call unlinkContractDocumentLink()"
  );
  assert(
    pageSource.includes("handleAdjustLink"),
    "ContractsPage should implement handleAdjustLink()"
  );
  assert(
    pageSource.includes("handleUnlinkLink"),
    "ContractsPage should implement handleUnlinkLink()"
  );
  assert(
    pageSource.includes("linkFxRate"),
    "ContractsPage should wire optional linkFxRate in link-document form"
  );
  assert(
    pageSource.includes("Apply Adjust"),
    "ContractsPage should render Apply Adjust action"
  );
  assert(
    pageSource.includes("Apply Unlink"),
    "ContractsPage should render Apply Unlink action"
  );
  assert(
    pageSource.includes("canAdjustContractLink"),
    "ContractsPage should consume canAdjustContractLink helper"
  );
  assert(
    pageSource.includes("canUnlinkContractLink"),
    "ContractsPage should consume canUnlinkContractLink helper"
  );

  assert(
    apiSource.includes("export async function adjustContractDocumentLink"),
    "contracts API client should expose adjustContractDocumentLink()"
  );
  assert(
    apiSource.includes("export async function unlinkContractDocumentLink"),
    "contracts API client should expose unlinkContractDocumentLink()"
  );

  const initialAdjust = createInitialLinkAdjustmentForm();
  assert(
    Object.prototype.hasOwnProperty.call(initialAdjust, "linkId"),
    "Initial adjust form should include linkId"
  );
  assert(
    Object.prototype.hasOwnProperty.call(initialAdjust, "nextLinkedAmountTxn"),
    "Initial adjust form should include nextLinkedAmountTxn"
  );
  assert(
    Object.prototype.hasOwnProperty.call(initialAdjust, "nextLinkedAmountBase"),
    "Initial adjust form should include nextLinkedAmountBase"
  );
  assert(
    Object.prototype.hasOwnProperty.call(initialAdjust, "reason"),
    "Initial adjust form should include reason"
  );

  const invalidAdjust = validateContractLinkAdjustmentForm({
    linkId: "",
    nextLinkedAmountTxn: "0",
    nextLinkedAmountBase: "-2",
    reason: "",
  });
  assert(
    invalidAdjust.errors.length >= 3,
    "Invalid adjust payload should return multiple validation errors"
  );

  const validAdjust = validateContractLinkAdjustmentForm({
    linkId: "44",
    nextLinkedAmountTxn: "12.75",
    nextLinkedAmountBase: "12.75",
    reason: "Manual correction",
  });
  assert(validAdjust.errors.length === 0, "Valid adjust payload should pass validation");
  const adjustPayload = buildContractLinkAdjustmentPayload(validAdjust.payload);
  assert(adjustPayload.linkId === 44, "Adjust payload should normalize linkId");
  assert(adjustPayload.nextLinkedAmountTxn === 12.75, "Adjust payload should normalize txn amount");
  assert(adjustPayload.nextLinkedAmountBase === 12.75, "Adjust payload should normalize base amount");
  assert(adjustPayload.reason === "Manual correction", "Adjust payload should normalize reason");

  const initialUnlink = createInitialLinkUnlinkForm();
  assert(
    Object.prototype.hasOwnProperty.call(initialUnlink, "linkId"),
    "Initial unlink form should include linkId"
  );
  assert(
    Object.prototype.hasOwnProperty.call(initialUnlink, "reason"),
    "Initial unlink form should include reason"
  );

  const invalidUnlink = validateContractLinkUnlinkForm({
    linkId: "",
    reason: "",
  });
  assert(
    invalidUnlink.errors.length >= 2,
    "Invalid unlink payload should return validation errors"
  );

  const validUnlink = validateContractLinkUnlinkForm({
    linkId: "44",
    reason: "Wrong contract mapping",
  });
  assert(validUnlink.errors.length === 0, "Valid unlink payload should pass validation");
  const unlinkPayload = buildContractLinkUnlinkPayload(validUnlink.payload);
  assert(unlinkPayload.linkId === 44, "Unlink payload should normalize linkId");
  assert(
    unlinkPayload.reason === "Wrong contract mapping",
    "Unlink payload should normalize reason"
  );

  const initialLink = createInitialLinkForm();
  assert(
    Object.prototype.hasOwnProperty.call(initialLink, "linkFxRate"),
    "Initial link form should include optional linkFxRate"
  );

  const invalidLink = validateContractLinkForm({
    cariDocumentId: "12",
    linkType: "BILLING",
    linkedAmountTxn: "10",
    linkedAmountBase: "10",
    linkFxRate: "0",
  });
  assert(
    invalidLink.errors.some((entry) => String(entry).includes("linkFxRate")),
    "linkFxRate <= 0 should fail client validation"
  );

  const validLink = validateContractLinkForm({
    cariDocumentId: "12",
    linkType: "BILLING",
    linkedAmountTxn: "10",
    linkedAmountBase: "10",
    linkFxRate: "35.125",
  });
  assert(validLink.errors.length === 0, "Valid link payload should pass validation");
  const linkPayload = buildContractLinkPayload(validLink.payload);
  assert(linkPayload.cariDocumentId === 12, "Link payload should normalize cariDocumentId");
  assert(linkPayload.linkFxRate === 35.125, "Link payload should normalize linkFxRate");

  const readOnlyGates = resolveContractsPermissionGates(["contract.read"]);
  const writerGates = resolveContractsPermissionGates([
    "contract.read",
    "contract.link_document",
  ]);

  const activeRow = { linkId: 101, isUnlinked: false };
  const unlinkedRow = { linkId: 102, isUnlinked: true };

  assert(
    canAdjustContractLink(activeRow, readOnlyGates).allowed === false,
    "Adjust should be blocked without contract.link_document"
  );
  assert(
    canUnlinkContractLink(activeRow, readOnlyGates).allowed === false,
    "Unlink should be blocked without contract.link_document"
  );
  assert(
    canAdjustContractLink(activeRow, writerGates).allowed === true,
    "Adjust should be allowed with contract.link_document"
  );
  assert(
    canUnlinkContractLink(activeRow, writerGates).allowed === true,
    "Unlink should be allowed with contract.link_document"
  );
  assert(
    canAdjustContractLink(unlinkedRow, writerGates).allowed === false,
    "Adjust should be blocked for already-unlinked row"
  );
  assert(
    canUnlinkContractLink(unlinkedRow, writerGates).allowed === false,
    "Unlink should be blocked for already-unlinked row"
  );

  console.log("PR-20 frontend link correction smoke passed.");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
