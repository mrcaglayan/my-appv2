import { runScriptChain } from "./_run-script-chain.js";

async function main() {
  await runScriptChain({
    title: "cash control enforcement characterization",
    scripts: ["test-cash-pr09-gl-cash-control-enforcement.js"],
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
