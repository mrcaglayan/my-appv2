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

const migration061BankGovernanceApprovalsSod = {
  key: "m061_bank_governance_approvals_sod",
  description: "Bank approvals, maker-checker (SoD), and threshold governance layer (B09)",
  async up(connection) {
    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_approval_policies (
         id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
         tenant_id BIGINT UNSIGNED NOT NULL,
         policy_code VARCHAR(60) NOT NULL,
         policy_name VARCHAR(190) NOT NULL,
         status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
         target_type VARCHAR(40) NOT NULL,
         action_type VARCHAR(40) NOT NULL,
         scope_type VARCHAR(20) NOT NULL DEFAULT 'GLOBAL',
         legal_entity_id BIGINT UNSIGNED NULL,
         bank_account_id BIGINT UNSIGNED NULL,
         currency_code CHAR(3) NULL,
         min_amount DECIMAL(20,6) NULL,
         max_amount DECIMAL(20,6) NULL,
         required_approvals INT NOT NULL DEFAULT 1,
         maker_checker_required TINYINT(1) NOT NULL DEFAULT 1,
         approver_permission_code VARCHAR(120) NOT NULL DEFAULT 'bank.approvals.requests.approve',
         auto_execute_on_final_approval TINYINT(1) NOT NULL DEFAULT 1,
         effective_from DATE NULL,
         effective_to DATE NULL,
         created_by_user_id INT NULL,
         updated_by_user_id INT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         PRIMARY KEY (id),
         UNIQUE KEY uk_bap_tenant_id (tenant_id, id),
         UNIQUE KEY uk_bap_policy_code (tenant_id, policy_code),
         KEY ix_bap_target (tenant_id, target_type, action_type, status),
         KEY ix_bap_scope (tenant_id, scope_type, legal_entity_id, bank_account_id),
         KEY ix_bap_thresholds (tenant_id, currency_code, min_amount, max_amount),
         CONSTRAINT fk_bap_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bap_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bap_bank_account
           FOREIGN KEY (tenant_id, legal_entity_id, bank_account_id)
           REFERENCES bank_accounts(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_bap_created_by
           FOREIGN KEY (tenant_id, created_by_user_id) REFERENCES users(tenant_id, id)
           ON UPDATE RESTRICT ON DELETE RESTRICT,
         CONSTRAINT fk_bap_updated_by
           FOREIGN KEY (tenant_id, updated_by_user_id) REFERENCES users(tenant_id, id)
           ON UPDATE RESTRICT ON DELETE RESTRICT
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_approval_requests (
         id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
         tenant_id BIGINT UNSIGNED NOT NULL,
         request_code VARCHAR(80) NOT NULL,
         request_key VARCHAR(190) NULL,
         policy_id BIGINT UNSIGNED NOT NULL,
         target_type VARCHAR(40) NOT NULL,
         target_id BIGINT UNSIGNED NULL,
         action_type VARCHAR(40) NOT NULL,
         request_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
         execution_status VARCHAR(20) NOT NULL DEFAULT 'NOT_EXECUTED',
         legal_entity_id BIGINT UNSIGNED NULL,
         bank_account_id BIGINT UNSIGNED NULL,
         threshold_amount DECIMAL(20,6) NULL,
         currency_code CHAR(3) NULL,
         required_approvals INT NOT NULL DEFAULT 1,
         maker_checker_required TINYINT(1) NOT NULL DEFAULT 1,
         approver_permission_code VARCHAR(120) NOT NULL,
         auto_execute_on_final_approval TINYINT(1) NOT NULL DEFAULT 1,
         requested_by_user_id INT NOT NULL,
         submitted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         approved_at TIMESTAMP NULL,
         rejected_at TIMESTAMP NULL,
         executed_at TIMESTAMP NULL,
         executed_by_user_id INT NULL,
         target_snapshot_json JSON NOT NULL,
         action_payload_json JSON NULL,
         policy_snapshot_json JSON NOT NULL,
         execution_result_json JSON NULL,
         execution_error_text TEXT NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         PRIMARY KEY (id),
         UNIQUE KEY uk_bar_tenant_id (tenant_id, id),
         UNIQUE KEY uk_bar_request_code (tenant_id, request_code),
         UNIQUE KEY uk_bar_request_key (tenant_id, request_key),
         KEY ix_bar_status (tenant_id, request_status, execution_status),
         KEY ix_bar_target (tenant_id, target_type, target_id, action_type),
         KEY ix_bar_requested_by (tenant_id, requested_by_user_id),
         KEY ix_bar_scope (tenant_id, legal_entity_id, bank_account_id),
         CONSTRAINT fk_bar_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bar_policy
           FOREIGN KEY (policy_id) REFERENCES bank_approval_policies(id)
           ON UPDATE RESTRICT ON DELETE RESTRICT,
         CONSTRAINT fk_bar_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_bar_requested_by
           FOREIGN KEY (tenant_id, requested_by_user_id) REFERENCES users(tenant_id, id)
           ON UPDATE RESTRICT ON DELETE RESTRICT,
         CONSTRAINT fk_bar_executed_by
           FOREIGN KEY (tenant_id, executed_by_user_id) REFERENCES users(tenant_id, id)
           ON UPDATE RESTRICT ON DELETE RESTRICT
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS bank_approval_request_decisions (
         id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
         tenant_id BIGINT UNSIGNED NOT NULL,
         bank_approval_request_id BIGINT UNSIGNED NOT NULL,
         decided_by_user_id INT NOT NULL,
         decision VARCHAR(20) NOT NULL,
         decision_comment VARCHAR(500) NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         PRIMARY KEY (id),
         UNIQUE KEY uk_bard_tenant_id (tenant_id, id),
         UNIQUE KEY uk_bard_request_user (tenant_id, bank_approval_request_id, decided_by_user_id),
         KEY ix_bard_request (tenant_id, bank_approval_request_id),
         KEY ix_bard_decision (tenant_id, decision),
         CONSTRAINT fk_bard_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_bard_request
           FOREIGN KEY (bank_approval_request_id) REFERENCES bank_approval_requests(id)
           ON UPDATE RESTRICT ON DELETE RESTRICT,
         CONSTRAINT fk_bard_decided_by
           FOREIGN KEY (tenant_id, decided_by_user_id) REFERENCES users(tenant_id, id)
           ON UPDATE RESTRICT ON DELETE RESTRICT
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payment_batches
         ADD COLUMN governance_approval_status VARCHAR(20) NOT NULL DEFAULT 'NOT_REQUIRED' AFTER status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batches
         ADD COLUMN governance_approval_request_id BIGINT UNSIGNED NULL AFTER governance_approval_status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batches
         ADD COLUMN governance_approved_at TIMESTAMP NULL AFTER governance_approval_request_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batches
         ADD COLUMN governance_approved_by_user_id INT NULL AFTER governance_approved_at`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batches
         ADD KEY ix_payment_batches_gov_approval_status (tenant_id, legal_entity_id, governance_approval_status)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batches
         ADD KEY ix_payment_batches_gov_approval_request (tenant_id, governance_approval_request_id)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_rules
         ADD COLUMN approval_state VARCHAR(20) NOT NULL DEFAULT 'APPROVED' AFTER status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_rules
         ADD COLUMN approval_request_id BIGINT UNSIGNED NULL AFTER approval_state`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_rules
         ADD COLUMN version_no INT NOT NULL DEFAULT 1 AFTER approval_request_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_rules
         ADD KEY ix_brr_approval_state (tenant_id, legal_entity_id, approval_state)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_posting_templates
         ADD COLUMN approval_state VARCHAR(20) NOT NULL DEFAULT 'APPROVED' AFTER status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_posting_templates
         ADD COLUMN approval_request_id BIGINT UNSIGNED NULL AFTER approval_state`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_posting_templates
         ADD COLUMN version_no INT NOT NULL DEFAULT 1 AFTER approval_request_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_posting_templates
         ADD KEY ix_brpt_approval_state (tenant_id, legal_entity_id, approval_state)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_difference_profiles
         ADD COLUMN approval_state VARCHAR(20) NOT NULL DEFAULT 'APPROVED' AFTER status`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_difference_profiles
         ADD COLUMN approval_request_id BIGINT UNSIGNED NULL AFTER approval_state`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_difference_profiles
         ADD COLUMN version_no INT NOT NULL DEFAULT 1 AFTER approval_request_id`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_difference_profiles
         ADD KEY ix_brdp_approval_state (tenant_id, legal_entity_id, approval_state)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_exceptions
         ADD COLUMN override_approval_request_id BIGINT UNSIGNED NULL AFTER resolution_note`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_exceptions
         ADD KEY ix_bre_override_approval_request (tenant_id, legal_entity_id, override_approval_request_id)`
    );
  },

  async down(connection) {
    await connection.execute(`DROP TABLE IF EXISTS bank_approval_request_decisions`);
    await connection.execute(`DROP TABLE IF EXISTS bank_approval_requests`);
    await connection.execute(`DROP TABLE IF EXISTS bank_approval_policies`);
    // Additive ALTER reversals intentionally omitted.
  },
};

export default migration061BankGovernanceApprovalsSod;
