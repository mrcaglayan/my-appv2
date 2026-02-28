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

const migration062PerformanceIndexesAndPaginationHardening = {
  key: "m062_performance_indexes_and_pagination_hardening",
  description: "Performance/indexing/pagination hardening for bank and payroll hot paths (PR-H03)",

  async up(connection) {
    // Bank statements / imports hot-path list pagination + filters
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_imports
         ADD KEY ix_bsi_scope_time_id (tenant_id, legal_entity_id, imported_at, id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_imports
         ADD KEY ix_bsi_scope_bank_status_time_id
           (tenant_id, legal_entity_id, bank_account_id, status, imported_at, id)`
    );

    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD KEY ix_bsl_scope_txn_id (tenant_id, legal_entity_id, txn_date, id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD KEY ix_bsl_scope_bank_recon_txn_id
           (tenant_id, legal_entity_id, bank_account_id, recon_status, txn_date, id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_statement_lines
         ADD KEY ix_bsl_scope_import_txn_id
           (tenant_id, legal_entity_id, import_id, txn_date, id)`
    );

    // Reconciliation queue/match aggregation and exception queue ordering
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_matches
         ADD KEY ix_brm_tenant_status_line (tenant_id, status, statement_line_id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_matches
         ADD KEY ix_brm_entity_status (tenant_id, legal_entity_id, matched_entity_type, matched_entity_id, status)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_exceptions
         ADD KEY ix_bre_scope_status_updated_id
           (tenant_id, legal_entity_id, status, updated_at, id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE bank_reconciliation_exceptions
         ADD KEY ix_bre_scope_bank_status_updated_id
           (tenant_id, legal_entity_id, bank_account_id, status, updated_at, id)`
    );

    // Payment/bank operational follow-up queries (B04/B06/B08)
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD KEY ix_pbl_scope_batch_exec_ack
           (tenant_id, legal_entity_id, batch_id, bank_execution_status, ack_status, id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payment_batch_lines
         ADD KEY ix_pbl_scope_batch_return (tenant_id, legal_entity_id, batch_id, return_status, id)`
    );

    // Payroll liabilities/settlements hot paths
    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD KEY ix_prl_scope_status_id (tenant_id, legal_entity_id, status, id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD KEY ix_prl_scope_run_status_out_id
           (tenant_id, legal_entity_id, run_id, status, outstanding_amount, id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_settlements
         ADD KEY ix_pls_scope_liab_settled_id
           (tenant_id, legal_entity_id, payroll_liability_id, settled_at, id)`
    );

    // Payroll provider imports + audit logs
    await safeExecute(
      connection,
      `ALTER TABLE payroll_provider_import_jobs
         ADD KEY ix_ppij_scope_requested_id
           (tenant_id, legal_entity_id, requested_at, id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_provider_import_jobs
         ADD KEY ix_ppij_scope_status_requested_id
           (tenant_id, legal_entity_id, status, requested_at, id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_provider_import_jobs
         ADD KEY ix_ppij_scope_provider_status_period_req_id
           (tenant_id, legal_entity_id, provider_code, status, payroll_period, requested_at, id)`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_provider_import_audit
         ADD KEY ix_ppia_scope_job_acted_id
           (tenant_id, legal_entity_id, payroll_provider_import_job_id, acted_at, id)`
    );

    // Payroll close control list/report filters
    await safeExecute(
      connection,
      `ALTER TABLE payroll_period_closes
         ADD KEY ix_ppc_scope_status_period_id
           (tenant_id, legal_entity_id, status, period_start, period_end, id)`
    );

    // H01 audit list endpoint ordering
    await safeExecute(
      connection,
      `ALTER TABLE sensitive_data_audit
         ADD KEY ix_sda_tenant_acted_id (tenant_id, acted_at, id)`
    );
  },

  async down(_connection) {
    // Additive index hardening; no-op rollback for safety.
  },
};

export default migration062PerformanceIndexesAndPaginationHardening;

