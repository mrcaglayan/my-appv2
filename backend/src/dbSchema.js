import { runMigrations } from "./migrationRunner.js";
import { logWarn } from "./observability/logger.js";

const warnedFunctions = new Set();

function warnLegacySchemaCall(functionName) {
  if (warnedFunctions.has(functionName)) {
    return;
  }
  warnedFunctions.add(functionName);

  logWarn("Legacy dbSchema API called; delegating to migrations", {
    functionName,
  });
}

// Legacy compatibility shim.
// Direct table creation is retired; schema ownership is migration-based.
export async function ensureUsersTable() {
  warnLegacySchemaCall("ensureUsersTable");
  await runMigrations();
}

// Legacy compatibility shim.
// Direct table creation is retired; schema ownership is migration-based.
export async function ensureRbacAuditLogsTable() {
  warnLegacySchemaCall("ensureRbacAuditLogsTable");
  await runMigrations();
}
