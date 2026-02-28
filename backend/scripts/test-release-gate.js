import { spawn } from "node:child_process";

const CORE_SCRIPT = "test:release-gate:core";
const CONTRACTS_REVENUE_SCRIPT = "test:contracts-revenue-gate";
const BANK_PAYROLL_SCRIPT = "test:e2e:bank-payroll";
const INTEGRATION_PRI06_SCRIPT = "test:integration:pri06";

const ENV_SKIP_CORE = "RELEASE_GATE_SKIP_CORE";
const SKIP_CONTRACTS_REVENUE_ENV = "RELEASE_GATE_SKIP_CONTRACTS_REVENUE";
const SKIP_BANK_PAYROLL_ENV = "RELEASE_GATE_SKIP_BANK_PAYROLL";
const ENV_SKIP_INTEGRATION_PRI06 = "RELEASE_GATE_SKIP_INTEGRATION_PRI06";
const ENV_ONLY_STAGES = "RELEASE_GATE_ONLY_STAGES";
const ENV_SKIP_STAGES = "RELEASE_GATE_SKIP_STAGES";
const ENV_DRY_RUN = "RELEASE_GATE_DRY_RUN";

const RELEASE_GATE_STAGES = Object.freeze([
  {
    id: "CORE",
    title: "Core platform and accounting gates",
    script: CORE_SCRIPT,
    skipEnv: ENV_SKIP_CORE,
  },
  {
    id: "CONTRACTS_REVENUE",
    title: "Contracts + revenue module gate",
    script: CONTRACTS_REVENUE_SCRIPT,
    skipEnv: SKIP_CONTRACTS_REVENUE_ENV,
  },
  {
    id: "BANK_PAYROLL",
    title: "Bank + payroll e2e gate",
    script: BANK_PAYROLL_SCRIPT,
    skipEnv: SKIP_BANK_PAYROLL_ENV,
  },
  {
    id: "INTEGRATION_PRI06",
    title: "Contracts/periods + bank/payroll integration chain",
    script: INTEGRATION_PRI06_SCRIPT,
    skipEnv: ENV_SKIP_INTEGRATION_PRI06,
  },
]);

function isTruthy(value) {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();
  return ["1", "true", "yes", "on"].includes(normalized);
}

function parseCsvEnv(value) {
  return new Set(
    String(value || "")
      .split(",")
      .map((item) => item.trim().toUpperCase())
      .filter(Boolean)
  );
}

function assertKnownStageIds(stageIds, label) {
  const known = new Set(RELEASE_GATE_STAGES.map((stage) => stage.id));
  const unknown = [...stageIds].filter((stageId) => !known.has(stageId));
  if (unknown.length > 0) {
    throw new Error(
      `${label} contains unknown stage ids: ${unknown.join(", ")}. Valid stage ids: ${[...known].join(", ")}`
    );
  }
}

async function runNpmScript(scriptName) {
  await new Promise((resolve, reject) => {
    const isWindows = process.platform === "win32";
    const child = isWindows
      ? spawn("cmd.exe", ["/d", "/s", "/c", `npm run ${scriptName}`], {
          cwd: process.cwd(),
          env: { ...process.env },
          stdio: "inherit",
        })
      : spawn("npm", ["run", scriptName], {
          cwd: process.cwd(),
          env: { ...process.env },
          stdio: "inherit",
        });

    child.on("error", (error) => reject(error));
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`npm run ${scriptName} failed with exit code ${code}`));
    });
  });
}

function shouldRunStage(stage, onlySet, skipSet) {
  if (isTruthy(process.env[stage.skipEnv])) {
    return false;
  }
  if (skipSet.has(stage.id)) {
    return false;
  }
  if (onlySet.size > 0 && !onlySet.has(stage.id)) {
    return false;
  }
  return true;
}

function logStageHeader(stage, index, total) {
  console.log("");
  console.log(`=== [${index}/${total}] ${stage.id} ===`);
  console.log(stage.title);
  console.log(`Script: npm run ${stage.script}`);
}

async function main() {
  const onlySet = parseCsvEnv(process.env[ENV_ONLY_STAGES]);
  const skipSet = parseCsvEnv(process.env[ENV_SKIP_STAGES]);
  const dryRun = isTruthy(process.env[ENV_DRY_RUN]);

  assertKnownStageIds(onlySet, ENV_ONLY_STAGES);
  assertKnownStageIds(skipSet, ENV_SKIP_STAGES);

  const selectedStages = RELEASE_GATE_STAGES.filter((stage) =>
    shouldRunStage(stage, onlySet, skipSet)
  );

  if (selectedStages.length === 0) {
    throw new Error("No stages selected for unified release gate");
  }

  console.log("Starting unified release gate...");
  console.log(`Selected stages: ${selectedStages.map((stage) => stage.id).join(", ")}`);
  if (dryRun) {
    console.log("Dry-run mode enabled; scripts will not execute.");
  }

  const stageResults = [];

  for (let i = 0; i < selectedStages.length; i += 1) {
    const stage = selectedStages[i];
    const stageStart = Date.now();
    logStageHeader(stage, i + 1, selectedStages.length);

    if (dryRun) {
      stageResults.push({
        stageId: stage.id,
        script: stage.script,
        ok: true,
        dryRun: true,
        durationMs: Date.now() - stageStart,
      });
      continue;
    }

    try {
      // Keep strict stage order so failures map to one stage at a time.
      // eslint-disable-next-line no-await-in-loop
      await runNpmScript(stage.script);
      stageResults.push({
        stageId: stage.id,
        script: stage.script,
        ok: true,
        durationMs: Date.now() - stageStart,
      });
    } catch (error) {
      const failure = {
        stageId: stage.id,
        script: stage.script,
        ok: false,
        durationMs: Date.now() - stageStart,
        errorMessage: error?.message || "Unknown stage failure",
      };
      stageResults.push(failure);

      console.error("");
      console.error(`Stage failed: ${failure.stageId}`);
      console.error(`Script: npm run ${failure.script}`);
      console.error(`Failure reason: ${failure.errorMessage}`);
      console.error(
        `Tip: rerun this stage only with ${ENV_ONLY_STAGES}=${failure.stageId} npm run test:release-gate`
      );
      throw new Error(`UNIFIED_RELEASE_GATE_FAILED at stage ${failure.stageId}: ${failure.errorMessage}`);
    }
  }

  console.log("");
  console.log("Unified release gate summary:");
  for (const result of stageResults) {
    const seconds = (Number(result.durationMs || 0) / 1000).toFixed(2);
    const suffix = result.dryRun ? " (dry-run)" : "";
    console.log(`- ${result.stageId}: OK in ${seconds}s${suffix}`);
  }
  console.log("Unified release gate passed.");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
