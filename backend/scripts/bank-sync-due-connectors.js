#!/usr/bin/env node

import { closePool } from "../src/db.js";
import { syncDueBankConnectors } from "../src/services/bank.connectors.service.js";

function parseOptionalPositiveInt(value) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

async function main() {
  const tenantId = parseOptionalPositiveInt(process.env.BANK_SYNC_TENANT_ID);
  const limit = parseOptionalPositiveInt(process.env.BANK_SYNC_LIMIT) || 20;
  const results = await syncDueBankConnectors({
    tenantId,
    limit,
    userId: null,
  });

  // eslint-disable-next-line no-console
  console.log("[bank-sync-due-connectors] completed", {
    tenant_id: tenantId,
    limit,
    processed: results.length,
    ok_count: results.filter((row) => row.ok).length,
    fail_count: results.filter((row) => !row.ok).length,
  });
  // eslint-disable-next-line no-console
  console.log(JSON.stringify(results, null, 2));
}

main()
  .catch(async (err) => {
    // eslint-disable-next-line no-console
    console.error("[bank-sync-due-connectors] fatal", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await closePool();
    } catch {
      // ignore close errors during script shutdown
    }
  });
