#!/usr/bin/env node

import { closePool } from "../src/db.js";
import {
  getSecretsMigrationDependencySummary,
  runSecretsMigrationPass,
  runSecretsMigrationUntilStable,
} from "../src/services/secretsMigration.service.js";

function parseOptionalPositiveInt(value) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

function parseBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  const text = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(text)) return true;
  if (["0", "false", "no", "off"].includes(text)) return false;
  return fallback;
}

function parseMode(value) {
  const text = String(value || "BOTH")
    .trim()
    .toUpperCase();
  if (["BACKFILL", "REENCRYPT", "BOTH"].includes(text)) return text;
  return "BOTH";
}

async function main() {
  const tenantId = parseOptionalPositiveInt(process.env.SECRETS_MIGRATION_TENANT_ID);
  const legalEntityId = parseOptionalPositiveInt(process.env.SECRETS_MIGRATION_LEGAL_ENTITY_ID);
  const userId = parseOptionalPositiveInt(process.env.SECRETS_MIGRATION_USER_ID);
  const limit = parseOptionalPositiveInt(process.env.SECRETS_MIGRATION_LIMIT) || 200;
  const maxPasses = parseOptionalPositiveInt(process.env.SECRETS_MIGRATION_MAX_PASSES) || 30;
  const mode = parseMode(process.env.SECRETS_MIGRATION_MODE);
  const dryRun = parseBoolean(process.env.SECRETS_MIGRATION_DRY_RUN, false);
  const forceReencrypt = parseBoolean(process.env.SECRETS_MIGRATION_FORCE_REENCRYPT, false);

  if (dryRun) {
    const preview = await runSecretsMigrationPass({
      tenantId,
      legalEntityId,
      userId,
      limit,
      mode,
      dryRun: true,
      forceReencrypt,
    });
    // eslint-disable-next-line no-console
    console.log("[secrets-backfill-reencrypt] dry-run preview");
    // eslint-disable-next-line no-console
    console.log(JSON.stringify(preview, null, 2));
    return;
  }

  const runSummary = await runSecretsMigrationUntilStable({
    tenantId,
    legalEntityId,
    userId,
    limit,
    mode,
    forceReencrypt,
    maxPasses,
  });

  const dependency = await getSecretsMigrationDependencySummary({
    tenantId,
    legalEntityId,
  });

  // eslint-disable-next-line no-console
  console.log("[secrets-backfill-reencrypt] completed");
  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        mode,
        tenant_id: tenantId,
        legal_entity_id: legalEntityId,
        limit,
        max_passes: maxPasses,
        force_reencrypt: forceReencrypt,
        totals: runSummary?.totals || {},
        post_check: runSummary?.post_check || {},
        dependency_check: dependency,
      },
      null,
      2
    )
  );
}

main()
  .catch((err) => {
    // eslint-disable-next-line no-console
    console.error("[secrets-backfill-reencrypt] fatal", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await closePool();
    } catch {
      // ignore pool close failures during script shutdown
    }
  });
