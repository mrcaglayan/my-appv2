const ignorableErrnos = new Set([
  1061, // ER_DUP_KEYNAME
]);

async function safeExecute(connection, sql) {
  try {
    await connection.execute(sql);
  } catch (err) {
    if (ignorableErrnos.has(err?.errno)) {
      return;
    }
    throw err;
  }
}

const statements = [
  `
  ALTER TABLE cari_documents
  ADD KEY ix_cari_docs_report_scope_date_direction (
    tenant_id,
    legal_entity_id,
    direction,
    document_date,
    counterparty_id
  )
  `,
  `
  ALTER TABLE cari_open_items
  ADD KEY ix_cari_oi_report_scope_due_counterparty (
    tenant_id,
    legal_entity_id,
    due_date,
    counterparty_id
  )
  `,
  `
  ALTER TABLE cari_settlement_allocations
  ADD KEY ix_cari_alloc_report_open_item_batch (
    tenant_id,
    legal_entity_id,
    open_item_id,
    settlement_batch_id
  )
  `,
  `
  ALTER TABLE cari_settlement_batches
  ADD KEY ix_cari_settle_report_scope_date_counterparty (
    tenant_id,
    legal_entity_id,
    settlement_date,
    counterparty_id
  )
  `,
  `
  ALTER TABLE audit_logs
  ADD KEY ix_audit_tenant_action_resource (
    tenant_id,
    action,
    resource_type,
    resource_id
  )
  `,
];

const migration018CariReportIndexes = {
  key: "m018_cari_report_indexes",
  description: "Add composite index coverage for Cari as-of report queries",
  async up(connection) {
    for (const statement of statements) {
      // eslint-disable-next-line no-await-in-loop
      await safeExecute(connection, statement);
    }
  },
};

export default migration018CariReportIndexes;
