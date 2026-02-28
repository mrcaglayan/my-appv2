import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function countOccurrences(source, literal) {
  const escaped = literal.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(escaped, "g");
  return (source.match(re) || []).length;
}

async function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
  const cashPagePath = path.resolve(root, "frontend/src/pages/cash/CashTransactionsPage.jsx");
  const messagesPath = path.resolve(root, "frontend/src/i18n/messages.js");
  const cashPage = await readFile(cashPagePath, "utf8");
  const messages = await readFile(messagesPath, "utf8");
  const cashPageLower = cashPage.toLowerCase();

  const counterAccountSetMatch = cashPage.match(
    /const COUNTER_ACCOUNT_REQUIRED_TXN_TYPES = new Set\(\[([\s\S]*?)\]\);/
  );
  assert(counterAccountSetMatch, "COUNTER_ACCOUNT_REQUIRED_TXN_TYPES set missing");

  const requiredTypes = [
    "RECEIPT",
    "PAYOUT",
    "OPENING_FLOAT",
    "CLOSING_ADJUSTMENT",
    "VARIANCE",
    "DEPOSIT_TO_BANK",
    "WITHDRAWAL_FROM_BANK",
  ];
  for (const txnType of requiredTypes) {
    assert(
      counterAccountSetMatch[1].includes(`"${txnType}"`),
      `counter-account-required set missing txn type ${txnType}`
    );
  }

  assert(
    cashPage.includes("requiresCounterAccountTxnType(normalizedTxnType)"),
    "createWarnings must enforce requiresCounterAccountTxnType(normalizedTxnType)"
  );
  assert(
    cashPage.includes("requiresCounterAccountTxnType(txnType)"),
    "submit validation must enforce requiresCounterAccountTxnType(txnType)"
  );
  assert(
    cashPage.includes("required={requiresCounterAccountTxnType(form.txnType)}"),
    "counterAccount input required rule must be requiresCounterAccountTxnType(form.txnType)"
  );
  assert(
    cashPage.includes("counterAccountId: requiresCounterAccount ? prev.counterAccountId : \"\""),
    "txn type switch should keep/clear counterAccountId by requiresCounterAccountTxnType"
  );

  const requiredPhraseIndex = cashPageLower.indexOf('includes("requires counteraccountid")');
  const requiredPhraseAltIndex = cashPageLower.indexOf(
    'includes("counteraccountid is required")'
  );
  const firstRequiredIndex = [requiredPhraseIndex, requiredPhraseAltIndex]
    .filter((index) => index >= 0)
    .sort((a, b) => a - b)[0];
  const invalidPhraseIndex = cashPageLower.indexOf(
    'includes("counteraccountid not found for tenant")'
  );

  assert(firstRequiredIndex >= 0, "missing required counterAccountId error mapping condition");
  assert(invalidPhraseIndex >= 0, "missing tenant-scope counterAccountId mapping condition");
  assert(
    firstRequiredIndex < invalidPhraseIndex,
    "required counterAccountId mapping must run before tenant-scope invalid mapping"
  );
  assert(
    !cashPageLower.includes('if (lower.includes("counteraccountid"))'),
    "generic counteraccountid mapping must not override required-vs-invalid distinction"
  );

  assert(
    countOccurrences(messages, "counterAccountRequired") >= 2,
    "counterAccountRequired i18n key should exist in both TR and EN maps"
  );
  assert(
    countOccurrences(messages, "counterAccountInvalid") >= 2,
    "counterAccountInvalid i18n key should exist in both TR and EN maps"
  );

  console.log("Cash counter-account validation + UI mapping smoke passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
