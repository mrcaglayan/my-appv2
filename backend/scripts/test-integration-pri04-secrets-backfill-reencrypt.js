#!/usr/bin/env node

import bcrypt from "bcrypt";
import crypto from "node:crypto";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";
import { parseEnvelopeText, decryptJson, encryptJson, serializeEnvelope } from "../src/utils/cryptoEnvelope.js";
import {
  getSecretsMigrationDependencySummary,
  runSecretsMigrationPass,
} from "../src/services/secretsMigration.service.js";
import { enqueueJob, runOneAvailableJob } from "../src/services/jobs.service.js";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toInt(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
}

function setEncryptionEnv({ activeKid, keysByKid }) {
  process.env.APP_ENCRYPTION_ACTIVE_KEY_VERSION = String(activeKid || "").trim();
  process.env.APP_ENCRYPTION_KEYS_JSON = JSON.stringify(keysByKid || {});
}

function randomKey() {
  return crypto.randomBytes(32).toString("base64");
}

async function createFixture(stamp) {
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const tenantCode = `PRI04_T_${stamp}`;
  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)`,
    [tenantCode, `PRI04 Tenant ${stamp}`]
  );
  const tenantId = toInt(
    (
      await query(
        `SELECT id
         FROM tenants
         WHERE code = ?
         LIMIT 1`,
        [tenantCode]
      )
    ).rows?.[0]?.id
  );
  assert(tenantId > 0, "Failed to create tenant fixture");

  const countryRow = (
    await query(
      `SELECT id, default_currency_code
       FROM countries
       WHERE iso2 = 'TR'
       LIMIT 1`
    )
  ).rows?.[0];
  const countryId = toInt(countryRow?.id);
  const currencyCode = String(countryRow?.default_currency_code || "TRY")
    .trim()
    .toUpperCase();
  assert(countryId > 0, "Missing TR country fixture");

  await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)`,
    [tenantId, `PRI04_G_${stamp}`, `PRI04 Group ${stamp}`]
  );
  const groupCompanyId = toInt(
    (
      await query(
        `SELECT id
         FROM group_companies
         WHERE tenant_id = ?
           AND code = ?
         LIMIT 1`,
        [tenantId, `PRI04_G_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(groupCompanyId > 0, "Failed to create group company fixture");

  await query(
    `INSERT INTO legal_entities (
        tenant_id,
        group_company_id,
        code,
        name,
        country_id,
        functional_currency_code,
        status
      ) VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, groupCompanyId, `PRI04_LE_${stamp}`, `PRI04 Legal Entity ${stamp}`, countryId, currencyCode]
  );
  const legalEntityId = toInt(
    (
      await query(
        `SELECT id
         FROM legal_entities
         WHERE tenant_id = ?
           AND code = ?
         LIMIT 1`,
        [tenantId, `PRI04_LE_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(legalEntityId > 0, "Failed to create legal entity fixture");

  const passwordHash = await bcrypt.hash("PRI04#Smoke123", 10);
  const userEmail = `pri04_user_${stamp}@example.com`;
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, userEmail, passwordHash, "PRI04 User"]
  );
  const userId = toInt(
    (
      await query(
        `SELECT id
         FROM users
         WHERE tenant_id = ?
           AND email = ?
         LIMIT 1`,
        [tenantId, userEmail]
      )
    ).rows?.[0]?.id
  );
  assert(userId > 0, "Failed to create user fixture");

  // Legacy plaintext provider connection (needs backfill).
  await query(
    `INSERT INTO payroll_provider_connections (
        tenant_id,
        legal_entity_id,
        provider_code,
        provider_name,
        adapter_version,
        status,
        is_default,
        settings_json,
        secrets_json,
        secrets_encrypted_json,
        secrets_key_version,
        secrets_migrated_at,
        created_by_user_id,
        updated_by_user_id
      ) VALUES (?, ?, 'GENERIC_JSON', ?, 'v1', 'ACTIVE', 1, '{}', ?, NULL, NULL, NULL, ?, ?)`,
    [
      tenantId,
      legalEntityId,
      `PRI04 Legacy Provider ${stamp}`,
      JSON.stringify({ client_id: "legacy-client", client_secret: "legacy-secret" }),
      userId,
      userId,
    ]
  );
  const legacyConnectionId = toInt(
    (
      await query(
        `SELECT id
         FROM payroll_provider_connections
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND provider_name = ?
         LIMIT 1`,
        [tenantId, legalEntityId, `PRI04 Legacy Provider ${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(legacyConnectionId > 0, "Failed to create legacy provider connection fixture");

  // Encrypted provider connection with old key version (needs reencrypt).
  const oldProviderEnvelope = encryptJson({
    api_key: `old-provider-key-${stamp}`,
    api_secret: `old-provider-secret-${stamp}`,
  });
  await query(
    `INSERT INTO payroll_provider_connections (
        tenant_id,
        legal_entity_id,
        provider_code,
        provider_name,
        adapter_version,
        status,
        is_default,
        settings_json,
        secrets_json,
        secrets_encrypted_json,
        secrets_key_version,
        secrets_migrated_at,
        created_by_user_id,
        updated_by_user_id
      ) VALUES (?, ?, 'GENERIC_JSON', ?, 'v1', 'ACTIVE', 0, '{}', NULL, ?, ?, CURRENT_TIMESTAMP, ?, ?)`,
    [
      tenantId,
      legalEntityId,
      `PRI04 Old Encrypted Provider ${stamp}`,
      serializeEnvelope(oldProviderEnvelope),
      String(oldProviderEnvelope.kid || ""),
      userId,
      userId,
    ]
  );
  const oldEncryptedConnectionId = toInt(
    (
      await query(
        `SELECT id
         FROM payroll_provider_connections
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND provider_name = ?
         LIMIT 1`,
        [tenantId, legalEntityId, `PRI04 Old Encrypted Provider ${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(oldEncryptedConnectionId > 0, "Failed to create old-encrypted provider connection fixture");

  // Bank connector encrypted with old key version (needs reencrypt).
  const oldConnectorEnvelope = encryptJson({
    token: `old-connector-token-${stamp}`,
  });
  await query(
    `INSERT INTO bank_connectors (
        tenant_id,
        legal_entity_id,
        connector_code,
        connector_name,
        provider_code,
        connector_type,
        status,
        adapter_version,
        config_json,
        credentials_encrypted_json,
        credentials_key_version,
        sync_mode,
        sync_frequency_minutes,
        next_sync_at,
        created_by_user_id,
        updated_by_user_id
      ) VALUES (?, ?, ?, ?, 'MOCK_OB', 'OPEN_BANKING', 'ACTIVE', 'v1', '{}', ?, ?, 'MANUAL', NULL, NULL, ?, ?)`,
    [
      tenantId,
      legalEntityId,
      `PRI04_CONN_${stamp}`,
      `PRI04 Connector ${stamp}`,
      serializeEnvelope(oldConnectorEnvelope),
      String(oldConnectorEnvelope.kid || ""),
      userId,
      userId,
    ]
  );
  const connectorId = toInt(
    (
      await query(
        `SELECT id
         FROM bank_connectors
         WHERE tenant_id = ?
           AND legal_entity_id = ?
           AND connector_code = ?
         LIMIT 1`,
        [tenantId, legalEntityId, `PRI04_CONN_${stamp}`]
      )
    ).rows?.[0]?.id
  );
  assert(connectorId > 0, "Failed to create bank connector fixture");

  return {
    tenantId,
    legalEntityId,
    userId,
    legacyConnectionId,
    oldEncryptedConnectionId,
    connectorId,
  };
}

async function main() {
  const stamp = Date.now();
  const oldKid = "pri04-old";
  const newKid = "pri04-new";
  const keys = {
    [oldKid]: randomKey(),
    [newKid]: randomKey(),
  };

  // Prepare fixtures with old key.
  setEncryptionEnv({ activeKid: oldKid, keysByKid: keys });
  const fixture = await createFixture(stamp);

  // Rotate active key and run migration.
  setEncryptionEnv({ activeKid: newKid, keysByKid: keys });
  const pass = await runSecretsMigrationPass({
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
    userId: fixture.userId,
    limit: 500,
    mode: "BOTH",
    dryRun: false,
    forceReencrypt: false,
  });

  assert(
    toInt(pass?.results?.payroll_provider_backfilled) === 1,
    "Expected 1 payroll provider plaintext backfill"
  );
  assert(
    toInt(pass?.results?.payroll_provider_reencrypted) === 1,
    "Expected 1 payroll provider re-encryption"
  );
  assert(
    toInt(pass?.results?.bank_connector_reencrypted) === 1,
    "Expected 1 bank connector re-encryption"
  );
  assert(
    toInt(pass?.post_check?.active_provider_plaintext_dependencies) === 0,
    "No active provider should rely on plaintext secrets after migration"
  );

  const legacyRow = (
    await query(
      `SELECT secrets_json, secrets_encrypted_json, secrets_key_version
       FROM payroll_provider_connections
       WHERE tenant_id = ?
         AND id = ?
       LIMIT 1`,
      [fixture.tenantId, fixture.legacyConnectionId]
    )
  ).rows?.[0];
  assert(legacyRow, "Legacy provider row should exist after migration");
  assert(!legacyRow.secrets_json, "Legacy provider secrets_json should be cleared");
  assert(String(legacyRow.secrets_key_version || "") === newKid, "Legacy provider key version mismatch");
  const legacyEnvelope = parseEnvelopeText(legacyRow.secrets_encrypted_json);
  const legacySecrets = decryptJson(legacyEnvelope);
  assert(String(legacySecrets.client_secret || "") === "legacy-secret", "Legacy provider secret decrypt mismatch");

  const oldEncryptedRow = (
    await query(
      `SELECT secrets_encrypted_json, secrets_key_version
       FROM payroll_provider_connections
       WHERE tenant_id = ?
         AND id = ?
       LIMIT 1`,
      [fixture.tenantId, fixture.oldEncryptedConnectionId]
    )
  ).rows?.[0];
  assert(oldEncryptedRow, "Old encrypted provider row should exist");
  assert(
    String(oldEncryptedRow.secrets_key_version || "") === newKid,
    "Old encrypted provider should rotate to new key"
  );
  const oldEncryptedSecrets = decryptJson(parseEnvelopeText(oldEncryptedRow.secrets_encrypted_json));
  assert(
    String(oldEncryptedSecrets.api_key || "") === `old-provider-key-${stamp}`,
    "Old encrypted provider decrypted secret mismatch"
  );

  const connectorRow = (
    await query(
      `SELECT credentials_encrypted_json, credentials_key_version
       FROM bank_connectors
       WHERE tenant_id = ?
         AND id = ?
       LIMIT 1`,
      [fixture.tenantId, fixture.connectorId]
    )
  ).rows?.[0];
  assert(connectorRow, "Bank connector row should exist");
  assert(
    String(connectorRow.credentials_key_version || "") === newKid,
    "Bank connector should rotate to new key"
  );
  const connectorSecrets = decryptJson(parseEnvelopeText(connectorRow.credentials_encrypted_json));
  assert(
    String(connectorSecrets.token || "") === `old-connector-token-${stamp}`,
    "Bank connector decrypted token mismatch"
  );

  const auditRows = (
    await query(
      `SELECT module_code, object_type, action
       FROM sensitive_data_audit
       WHERE tenant_id = ?
         AND action IN ('BACKFILL_ENCRYPTED', 'REENCRYPTED_SECRET')`,
      [fixture.tenantId]
    )
  ).rows || [];
  assert(
    auditRows.length >= 3,
    "Expected at least 3 sensitive_data_audit rows for backfill + reencrypt actions"
  );

  const dependency = await getSecretsMigrationDependencySummary({
    tenantId: fixture.tenantId,
    legalEntityId: fixture.legalEntityId,
  });
  assert(
    toInt(dependency?.active_provider_plaintext_dependencies) === 0,
    "Dependency summary should report zero active plaintext dependencies"
  );

  const queued = await enqueueJob({
    tenantId: fixture.tenantId,
    userId: fixture.userId,
    spec: {
      queue_name: "ops.security",
      module_code: "OPS",
      job_type: "SECRETS_BACKFILL_REENCRYPT",
      idempotency_key: `PRI04_JOB_${stamp}`,
      run_after_at: new Date(Date.now() - 30 * 1000),
      payload: {
        tenant_id: fixture.tenantId,
        legal_entity_id: fixture.legalEntityId,
        acting_user_id: fixture.userId,
        mode: "BOTH",
        max_passes: 2,
      },
    },
  });
  assert(toInt(queued?.job?.id) > 0, "SECRETS_BACKFILL_REENCRYPT job should be enqueued");

  const jobRun = await runOneAvailableJob({
    tenantId: fixture.tenantId,
    workerId: "pri04-worker",
    queueNames: ["ops.security"],
  });
  assert(Boolean(jobRun?.ok) === true, "SECRETS_BACKFILL_REENCRYPT job should run successfully");
  assert(String(jobRun?.status || "") === "SUCCEEDED", "SECRETS_BACKFILL_REENCRYPT job should finish SUCCEEDED");
  assert(
    toInt(jobRun?.result?.post_check?.active_provider_plaintext_dependencies) === 0,
    "Job result should report zero active plaintext dependencies"
  );

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        ok: true,
        step: "PR-I04",
        tenant_id: fixture.tenantId,
        legal_entity_id: fixture.legalEntityId,
        result: pass?.results || {},
        queued_job_id: toInt(queued?.job?.id),
        job_status: String(jobRun?.status || ""),
        dependency,
      },
      null,
      2
    )
  );
}

main()
  .catch((err) => {
    // eslint-disable-next-line no-console
    console.error("[PR-I04 smoke] failed", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await closePool();
    } catch {
      // ignore close failures during shutdown
    }
  });
