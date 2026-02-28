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
    if (ignorableErrnos.has(err?.errno)) {
      return;
    }
    throw err;
  }
}

const migration044PayrollPartialSettlementAndManualOverride = {
  key: "m044_payroll_partial_settlement_and_manual_override",
  description:
    "Payroll partial settlement tracking and manual settlement override workflow (PR-P06)",
  async up(connection) {
    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         MODIFY COLUMN status ENUM('OPEN','IN_BATCH','PARTIALLY_PAID','PAID','CANCELLED')
           NOT NULL DEFAULT 'OPEN'`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_liability_payment_links
         MODIFY COLUMN status ENUM('LINKED','PARTIALLY_PAID','PAID','RELEASED')
           NOT NULL DEFAULT 'LINKED'`
    );

    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD COLUMN settled_amount DECIMAL(20,6) NOT NULL DEFAULT 0 AFTER amount`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD COLUMN outstanding_amount DECIMAL(20,6) NOT NULL DEFAULT 0 AFTER settled_amount`
    );
    await safeExecute(
      connection,
      `ALTER TABLE payroll_run_liabilities
         ADD KEY ix_payroll_run_liabilities_scope_outstanding
           (tenant_id, legal_entity_id, outstanding_amount)`
    );

    await safeExecute(
      connection,
      `UPDATE payroll_run_liabilities l
       LEFT JOIN (
         SELECT
           tenant_id,
           legal_entity_id,
           payroll_liability_id,
           GREATEST(
             COALESCE(MAX(settled_amount), 0),
             COALESCE(MAX(CASE WHEN status = 'PAID' THEN allocated_amount ELSE 0 END), 0)
           ) AS linked_settled
         FROM payroll_liability_payment_links
         GROUP BY tenant_id, legal_entity_id, payroll_liability_id
       ) pl
         ON pl.tenant_id = l.tenant_id
        AND pl.legal_entity_id = l.legal_entity_id
        AND pl.payroll_liability_id = l.id
       SET
         l.settled_amount = LEAST(
           l.amount,
           GREATEST(
             COALESCE(pl.linked_settled, 0),
             CASE WHEN l.status = 'PAID' THEN l.amount ELSE 0 END
           )
         ),
         l.outstanding_amount = CASE
           WHEN l.status = 'CANCELLED' THEN 0
           ELSE GREATEST(
             0,
             l.amount - LEAST(
               l.amount,
               GREATEST(
                 COALESCE(pl.linked_settled, 0),
                 CASE WHEN l.status = 'PAID' THEN l.amount ELSE 0 END
               )
             )
           )
         END`
    );

    await safeExecute(
      connection,
      `UPDATE payroll_run_liabilities
       SET status = 'PARTIALLY_PAID'
       WHERE status IN ('IN_BATCH','PAID')
         AND settled_amount > 0
         AND outstanding_amount > 0`
    );

    await safeExecute(
      connection,
      `UPDATE payroll_liability_payment_links
       SET status = CASE
         WHEN COALESCE(settled_amount, 0) <= 0 THEN
           CASE WHEN status = 'PAID' THEN 'LINKED' ELSE status END
         WHEN COALESCE(settled_amount, 0) + 0.000001 >= COALESCE(allocated_amount, 0) THEN 'PAID'
         ELSE 'PARTIALLY_PAID'
       END
       WHERE status IN ('LINKED','PAID')`
    );

    await safeExecute(
      connection,
      `CREATE TABLE IF NOT EXISTS payroll_liability_override_requests (
         id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
         tenant_id BIGINT UNSIGNED NOT NULL,
         legal_entity_id BIGINT UNSIGNED NOT NULL,
         run_id BIGINT UNSIGNED NOT NULL,
         payroll_liability_id BIGINT UNSIGNED NOT NULL,
         payroll_liability_payment_link_id BIGINT UNSIGNED NOT NULL,
         request_type ENUM('MANUAL_SETTLEMENT') NOT NULL DEFAULT 'MANUAL_SETTLEMENT',
         requested_amount DECIMAL(20,6) NOT NULL,
         currency_code CHAR(3) NOT NULL,
         settled_at TIMESTAMP NOT NULL,
         reason VARCHAR(500) NOT NULL,
         external_ref VARCHAR(190) NULL,
         status ENUM('REQUESTED','APPLIED','REJECTED') NOT NULL DEFAULT 'REQUESTED',
         idempotency_key VARCHAR(190) NULL,
         requested_by_user_id INT NULL,
         requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         approved_by_user_id INT NULL,
         approved_at TIMESTAMP NULL,
         rejected_by_user_id INT NULL,
         rejected_at TIMESTAMP NULL,
         decision_note VARCHAR(500) NULL,
         applied_settlement_id BIGINT UNSIGNED NULL,
         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
           ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE KEY uk_payroll_liability_override_requests_scope_id (tenant_id, legal_entity_id, id),
         UNIQUE KEY uk_payroll_liability_override_requests_scope_idem
           (tenant_id, legal_entity_id, idempotency_key),
         KEY ix_payroll_liability_override_requests_scope_run (tenant_id, legal_entity_id, run_id),
         KEY ix_payroll_liability_override_requests_scope_liability
           (tenant_id, legal_entity_id, payroll_liability_id),
         KEY ix_payroll_liability_override_requests_scope_link
           (tenant_id, legal_entity_id, payroll_liability_payment_link_id),
         KEY ix_payroll_liability_override_requests_scope_status
           (tenant_id, legal_entity_id, status),
         KEY ix_payroll_liability_override_requests_scope_time
           (tenant_id, legal_entity_id, requested_at),
         CONSTRAINT fk_payroll_liability_override_requests_tenant
           FOREIGN KEY (tenant_id) REFERENCES tenants(id),
         CONSTRAINT fk_payroll_liability_override_requests_legal_entity
           FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
         CONSTRAINT fk_payroll_liability_override_requests_run
           FOREIGN KEY (tenant_id, legal_entity_id, run_id)
           REFERENCES payroll_runs(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_override_requests_liability
           FOREIGN KEY (tenant_id, legal_entity_id, payroll_liability_id)
           REFERENCES payroll_run_liabilities(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_override_requests_link
           FOREIGN KEY (tenant_id, legal_entity_id, payroll_liability_payment_link_id)
           REFERENCES payroll_liability_payment_links(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_override_requests_settlement
           FOREIGN KEY (tenant_id, legal_entity_id, applied_settlement_id)
           REFERENCES payroll_liability_settlements(tenant_id, legal_entity_id, id),
         CONSTRAINT fk_payroll_liability_override_requests_requested_user
           FOREIGN KEY (tenant_id, requested_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payroll_liability_override_requests_approved_user
           FOREIGN KEY (tenant_id, approved_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payroll_liability_override_requests_rejected_user
           FOREIGN KEY (tenant_id, rejected_by_user_id) REFERENCES users(tenant_id, id),
         CONSTRAINT fk_payroll_liability_override_requests_currency
           FOREIGN KEY (currency_code) REFERENCES currencies(code)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
    );
  },

  async down(connection) {
    await safeExecute(connection, `DROP TABLE IF EXISTS payroll_liability_override_requests`);
    // Alter rollback intentionally omitted for dev simplicity.
  },
};

export default migration044PayrollPartialSettlementAndManualOverride;
