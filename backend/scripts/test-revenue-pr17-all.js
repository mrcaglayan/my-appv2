import { runScriptChain } from "./_run-script-chain.js";

async function main() {
  await runScriptChain({
    title: "revenue PR-17 regression gate",
    scripts: [
      "test-revenue-pr17a-foundation.js",
      "test-revenue-pr17b.js",
      "test-revenue-pr17c.js",
      "test-revenue-pr17d.js",
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
