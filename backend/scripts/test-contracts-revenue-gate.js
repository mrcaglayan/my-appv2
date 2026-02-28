import { runScriptChain } from "./_run-script-chain.js";

async function main() {
  await runScriptChain({
    title: "contracts/revenue module gate",
    scripts: [
      "test-contracts-pr16-schema-and-api.js",
      "test-revenue-pr17-all.js",
      "test-contracts-revenue-pr18.js",
      "test-contracts-pr20-link-corrections.js",
      "test-contracts-pr20-frontend-link-corrections-smoke.js",
      "test-contracts-pr21-amendment-versioning-and-partial-lines.js",
      "test-contracts-pr21-frontend-amendment-smoke.js",
      "test-contracts-pr25-frontend-linkable-documents-smoke.js",
      "test-contracts-pr27-reporting-index-optimization.js",
      "test-contracts-pr28-frontend-rename-and-route-aliases.js",
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
