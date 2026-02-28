const IGNORABLE_ERRNOS = new Set([
  1060, // ER_DUP_FIELDNAME
  1061, // ER_DUP_KEYNAME
  1091, // ER_CANT_DROP_FIELD_OR_KEY
]);

async function safeExecute(connection, sql, params = []) {
  try {
    await connection.execute(sql, params);
  } catch (err) {
    if (IGNORABLE_ERRNOS.has(Number(err?.errno))) {
      return;
    }
    throw err;
  }
}

const migration068ExceptionWorkbenchSlaDueAt = {
  key: "m068_exception_workbench_sla_due_at",
  description: "Add SLA due timestamp and urgency index to exception workbench (UX08)",
  async up(connection) {
    await safeExecute(
      connection,
      `ALTER TABLE exception_workbench
         ADD COLUMN sla_due_at TIMESTAMP NULL AFTER severity`
    );

    await safeExecute(
      connection,
      `ALTER TABLE exception_workbench
         ADD KEY ix_exception_workbench_scope_sla_due (tenant_id, status, sla_due_at)`
    );

    await safeExecute(
      connection,
      `UPDATE exception_workbench
          SET sla_due_at = CASE severity
            WHEN 'CRITICAL' THEN DATE_ADD(COALESCE(first_seen_at, created_at), INTERVAL 4 HOUR)
            WHEN 'HIGH' THEN DATE_ADD(COALESCE(first_seen_at, created_at), INTERVAL 24 HOUR)
            WHEN 'MEDIUM' THEN DATE_ADD(COALESCE(first_seen_at, created_at), INTERVAL 72 HOUR)
            ELSE DATE_ADD(COALESCE(first_seen_at, created_at), INTERVAL 120 HOUR)
          END
        WHERE sla_due_at IS NULL`
    );
  },

  async down(connection) {
    await safeExecute(
      connection,
      `ALTER TABLE exception_workbench
         DROP KEY ix_exception_workbench_scope_sla_due`
    );
    await safeExecute(
      connection,
      `ALTER TABLE exception_workbench
         DROP COLUMN sla_due_at`
    );
  },
};

export default migration068ExceptionWorkbenchSlaDueAt;
