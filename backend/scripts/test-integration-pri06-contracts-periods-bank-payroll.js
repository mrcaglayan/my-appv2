#!/usr/bin/env node

import { runScriptChain } from "./_run-script-chain.js";

async function main() {
  await runScriptChain({
    title: "PR-I06 contracts/periods + bank/payroll integration chain",
    scripts: [
      "test-contracts-pr22-revrec-generation.js",
      "test-revenue-pr17c.js",
      "test-payroll-prp03-liabilities-payment-prep.js",
      "test-payroll-prp04-payment-settlement-sync.js",
      "test-payroll-prp08-close-controls-checklist-locks.js",
    ],
  });
}

main()
  .then(() => {
    process.exitCode = 0;
  })
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
