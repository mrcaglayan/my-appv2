import { runScriptChain } from "./_run-script-chain.js";

async function main() {
  await runScriptChain({
    title: "cash register/session characterization",
    scripts: [
      "test-cash-pr06-register-session.js",
      "test-cash-pr10-variance-policy.js",
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
