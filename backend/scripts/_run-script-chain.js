import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function runNodeScript(scriptFile) {
  return new Promise((resolve, reject) => {
    const scriptPath = path.resolve(__dirname, scriptFile);
    const child = spawn(process.execPath, [scriptPath], {
      cwd: process.cwd(),
      env: { ...process.env },
      stdio: "inherit",
    });

    child.on("error", reject);
    child.on("exit", (code, signal) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(
        new Error(
          `Script failed: ${scriptFile} (exit=${code ?? "null"}, signal=${signal ?? "none"})`
        )
      );
    });
  });
}

export async function runScriptChain({ title, scripts }) {
  if (!Array.isArray(scripts) || scripts.length === 0) {
    throw new Error("scripts must be a non-empty array");
  }

  console.log(`Starting ${title}...`);
  for (const scriptFile of scripts) {
    // Keep execution sequential to avoid DB deadlocks between integration suites.
    // eslint-disable-next-line no-await-in-loop
    await runNodeScript(scriptFile);
  }
  console.log(`${title} passed.`);
}
