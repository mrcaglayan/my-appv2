import { runScriptChain } from "./_run-script-chain.js";

async function main() {
  await runScriptChain({
    title: "cash posting/reversal characterization",
    scripts: [
      "test-cash-pr07-lifecycle.js",
      "test-cash-pr08-gl-posting.js",
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
