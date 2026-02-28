import { runScriptChain } from "./_run-script-chain.js";

async function main() {
  await runScriptChain({
    title: "contracts/revenue PR-18 frontend gate",
    scripts: [
      "test-contracts-revenue-pr18-fetch-gating.js",
      "test-contracts-revenue-pr18-frontend-smoke.js",
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
