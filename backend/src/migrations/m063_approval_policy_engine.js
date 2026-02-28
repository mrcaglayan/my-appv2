const ignorableErrnos = new Set([
  1050, // ER_TABLE_EXISTS_ERROR
  1060, // ER_DUP_FIELDNAME
  1061, // ER_DUP_KEYNAME
  1091, // ER_CANT_DROP_FIELD_OR_KEY
  1826, // ER_FK_DUP_NAME
]);

async function safeExecute(connection, sql, params = []) {
  try {
    await connection.execute(sql, params);
  } catch (err) {
    if (ignorableErrnos.has(Number(err?.errno))) {
      return;
    }
    throw err;
  }
}

const migration063ApprovalPolicyEngine = {
  key: "m063_approval_policy_engine",
  description: "Module-aware unified approval policy engine overlays on B09 approval tables (PR-H04)",

  async up(connection) {
    await safeExecute(
      connection,
      `ALTER TABLE bank_approval_policies
         ADD COLUMN module_code VARCHAR(20) NOT NULL DEFAULT 'BANK' AFTER status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_approval_policies
         ADD KEY ix_bap_module_target_action (tenant_id, module_code, target_type, action_type, status)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_approval_policies
         ADD KEY ix_bap_module_scope (tenant_id, module_code, scope_type, legal_entity_id, bank_account_id)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE bank_approval_requests
         ADD COLUMN module_code VARCHAR(20) NOT NULL DEFAULT 'BANK' AFTER tenant_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_approval_requests
         ADD KEY ix_bar_module_status (tenant_id, module_code, request_status, execution_status)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_approval_requests
         ADD KEY ix_bar_module_target (tenant_id, module_code, target_type, target_id, action_type)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_approval_requests
         ADD KEY ix_bar_module_scope (tenant_id, module_code, legal_entity_id, bank_account_id)`
    );

    // Explicit backfill for installations where ALTER default behavior may not retrofill as expected.
    await safeExecute(
      connection,
      `UPDATE bank_approval_policies
       SET module_code = 'BANK'
       WHERE module_code IS NULL OR module_code = ''`
    );
    await safeExecute(
      connection,
      `UPDATE bank_approval_requests
       SET module_code = 'BANK'
       WHERE module_code IS NULL OR module_code = ''`
    );
  },

  async down(_connection) {
    // Additive hardening migration; no rollback for safety.
  },
};

export default migration063ApprovalPolicyEngine;

