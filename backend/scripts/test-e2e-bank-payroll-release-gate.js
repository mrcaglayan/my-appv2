import { spawn } from "node:child_process";
import { BANK_PAYROLL_RELEASE_STAGES } from "./fixtures/bank-payroll-e2e-fixtures.js";

const ENV_ONLY_STAGES = "BANK_PAYROLL_E2E_ONLY_STAGES";
const ENV_SKIP_STAGES = "BANK_PAYROLL_E2E_SKIP_STAGES";
const ENV_DRY_RUN = "BANK_PAYROLL_E2E_DRY_RUN";

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
      .map((item) => item.trim())
      .filter(Boolean)
  );
}

function stageIsSelected(stageId, onlySet, skipSet) {
  if (skipSet.has(stageId)) return false;
  if (onlySet.size === 0) return true;
  return onlySet.has(stageId);
}

function runNpmScript(scriptName) {
  return new Promise((resolve, reject) => {
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

function logStageHeader(stage, index, total) {
  console.log("");
  console.log(`=== [${index}/${total}] ${stage.id} ===`);
  console.log(stage.title);
  console.log("Scripts:");
  for (const scriptName of stage.scripts || []) {
    console.log(`- ${scriptName}`);
  }
}

async function main() {
  const onlySet = parseCsvEnv(process.env[ENV_ONLY_STAGES]);
  const skipSet = parseCsvEnv(process.env[ENV_SKIP_STAGES]);
  const dryRun = isTruthy(process.env[ENV_DRY_RUN]);

  const selectedStages = (BANK_PAYROLL_RELEASE_STAGES || []).filter((stage) =>
    stageIsSelected(stage.id, onlySet, skipSet)
  );

  if (selectedStages.length === 0) {
    throw new Error("No stages selected for bank/payroll release gate run");
  }

  console.log("Starting bank/payroll e2e release gate...");
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
        ok: true,
        dryRun: true,
        durationMs: Date.now() - stageStart,
      });
      continue;
    }

    try {
      for (const scriptName of stage.scripts || []) {
        // Keep strict sequential order so failures map cleanly to stage + script.
        // eslint-disable-next-line no-await-in-loop
        await runNpmScript(scriptName);
      }
      stageResults.push({
        stageId: stage.id,
        ok: true,
        durationMs: Date.now() - stageStart,
      });
    } catch (error) {
      const result = {
        stageId: stage.id,
        ok: false,
        durationMs: Date.now() - stageStart,
        errorMessage: error?.message || "Unknown stage error",
      };
      stageResults.push(result);

      console.error("");
      console.error(`Stage failed: ${stage.id}`);
      console.error(`Failure reason: ${result.errorMessage}`);
      console.error("Stopping release gate at first failure.");
      throw new Error(`BANK_PAYROLL_RELEASE_GATE_FAILED at stage ${stage.id}: ${result.errorMessage}`);
    }
  }

  console.log("");
  console.log("Bank/payroll e2e release gate summary:");
  for (const result of stageResults) {
    const seconds = (Number(result.durationMs || 0) / 1000).toFixed(2);
    const suffix = result.dryRun ? " (dry-run)" : "";
    console.log(`- ${result.stageId}: OK in ${seconds}s${suffix}`);
  }
  console.log("Bank/payroll e2e release gate passed.");
}

main().catch((error) => {
  console.error(error?.message || error);
  process.exit(1);
});
