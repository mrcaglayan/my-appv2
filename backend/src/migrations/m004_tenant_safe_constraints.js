const ignorableErrnos = new Set([
  1060, // ER_DUP_FIELDNAME
  1061, // ER_DUP_KEYNAME
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

const statements = [
  `
  ALTER TABLE users
  ADD UNIQUE KEY uk_users_tenant_id_id (tenant_id, id)
  `,
  `
  ALTER TABLE roles
  ADD UNIQUE KEY uk_roles_tenant_id_id (tenant_id, id)
  `,
  `
  ALTER TABLE group_companies
  ADD UNIQUE KEY uk_group_companies_tenant_id_id (tenant_id, id)
  `,
  `
  ALTER TABLE legal_entities
  ADD UNIQUE KEY uk_legal_entities_tenant_id_id (tenant_id, id)
  `,
  `
  ALTER TABLE fiscal_calendars
  ADD UNIQUE KEY uk_fiscal_calendars_tenant_id_id (tenant_id, id)
  `,
  `
  ALTER TABLE books
  ADD UNIQUE KEY uk_books_tenant_id_id (tenant_id, id)
  `,
  `
  ALTER TABLE charts_of_accounts
  ADD UNIQUE KEY uk_charts_of_accounts_tenant_id_id (tenant_id, id)
  `,
  `
  ALTER TABLE consolidation_groups
  ADD UNIQUE KEY uk_consolidation_groups_tenant_id_id (tenant_id, id)
  `,
  `
  ALTER TABLE books
  ADD INDEX ix_books_tenant_entity (tenant_id, legal_entity_id)
  `,
  `
  ALTER TABLE books
  ADD INDEX ix_books_tenant_calendar (tenant_id, calendar_id)
  `,
  `
  ALTER TABLE charts_of_accounts
  ADD INDEX ix_coa_tenant_entity (tenant_id, legal_entity_id)
  `,
  `
  ALTER TABLE user_role_scopes
  ADD INDEX ix_urs_tenant_user (tenant_id, user_id)
  `,
  `
  ALTER TABLE user_role_scopes
  ADD INDEX ix_urs_tenant_role (tenant_id, role_id)
  `,
  `
  ALTER TABLE journal_entries
  ADD INDEX ix_journal_tenant_book (tenant_id, book_id)
  `,
  `
  ALTER TABLE intercompany_pairs
  ADD INDEX ix_ic_tenant_to_entity (tenant_id, to_legal_entity_id)
  `,
  `
  ALTER TABLE consolidation_groups
  ADD INDEX ix_cons_groups_tenant_group (tenant_id, group_company_id)
  `,
  `
  ALTER TABLE consolidation_groups
  ADD INDEX ix_cons_groups_tenant_calendar (tenant_id, calendar_id)
  `,
  `
  ALTER TABLE ownership_links
  ADD INDEX ix_owner_tenant_parent (tenant_id, parent_legal_entity_id)
  `,
  `
  ALTER TABLE ownership_links
  ADD INDEX ix_owner_tenant_child (tenant_id, child_legal_entity_id)
  `,
  `
  ALTER TABLE group_coa_mappings
  ADD INDEX ix_gcm_tenant_legal_entity (tenant_id, legal_entity_id)
  `,
  `
  ALTER TABLE group_coa_mappings
  ADD INDEX ix_gcm_tenant_group_coa (tenant_id, group_coa_id)
  `,
  `
  ALTER TABLE group_coa_mappings
  ADD INDEX ix_gcm_tenant_local_coa (tenant_id, local_coa_id)
  `,
  `
  ALTER TABLE elimination_placeholders
  ADD INDEX ix_ep_tenant_group (tenant_id, consolidation_group_id)
  `,
  `
  ALTER TABLE consolidation_run_entries
  ADD INDEX ix_cre_tenant_entity (tenant_id, legal_entity_id)
  `,
  `
  ALTER TABLE rbac_audit_logs
  ADD INDEX ix_rbac_audit_tenant_actor (tenant_id, actor_user_id)
  `,
  `
  ALTER TABLE rbac_audit_logs
  ADD INDEX ix_rbac_audit_tenant_target (tenant_id, target_user_id)
  `,
  `
  ALTER TABLE audit_logs
  ADD INDEX ix_audit_tenant_user (tenant_id, user_id)
  `,
  `
  ALTER TABLE legal_entities
  ADD CONSTRAINT fk_le_group_tenant
  FOREIGN KEY (tenant_id, group_company_id)
  REFERENCES group_companies (tenant_id, id)
  `,
  `
  ALTER TABLE operating_units
  ADD CONSTRAINT fk_ou_entity_tenant
  FOREIGN KEY (tenant_id, legal_entity_id)
  REFERENCES legal_entities (tenant_id, id)
  `,
  `
  ALTER TABLE books
  ADD CONSTRAINT fk_books_entity_tenant
  FOREIGN KEY (tenant_id, legal_entity_id)
  REFERENCES legal_entities (tenant_id, id)
  `,
  `
  ALTER TABLE books
  ADD CONSTRAINT fk_books_calendar_tenant
  FOREIGN KEY (tenant_id, calendar_id)
  REFERENCES fiscal_calendars (tenant_id, id)
  `,
  `
  ALTER TABLE charts_of_accounts
  ADD CONSTRAINT fk_coa_entity_tenant
  FOREIGN KEY (tenant_id, legal_entity_id)
  REFERENCES legal_entities (tenant_id, id)
  `,
  `
  ALTER TABLE user_role_scopes
  ADD CONSTRAINT fk_urs_user_tenant
  FOREIGN KEY (tenant_id, user_id)
  REFERENCES users (tenant_id, id)
  `,
  `
  ALTER TABLE user_role_scopes
  ADD CONSTRAINT fk_urs_role_tenant
  FOREIGN KEY (tenant_id, role_id)
  REFERENCES roles (tenant_id, id)
  `,
  `
  ALTER TABLE data_scopes
  ADD CONSTRAINT fk_data_scope_user_tenant
  FOREIGN KEY (tenant_id, user_id)
  REFERENCES users (tenant_id, id)
  `,
  `
  ALTER TABLE journal_entries
  ADD CONSTRAINT fk_journal_entity_tenant
  FOREIGN KEY (tenant_id, legal_entity_id)
  REFERENCES legal_entities (tenant_id, id)
  `,
  `
  ALTER TABLE journal_entries
  ADD CONSTRAINT fk_journal_book_tenant
  FOREIGN KEY (tenant_id, book_id)
  REFERENCES books (tenant_id, id)
  `,
  `
  ALTER TABLE intercompany_pairs
  ADD CONSTRAINT fk_ic_from_entity_tenant
  FOREIGN KEY (tenant_id, from_legal_entity_id)
  REFERENCES legal_entities (tenant_id, id)
  `,
  `
  ALTER TABLE intercompany_pairs
  ADD CONSTRAINT fk_ic_to_entity_tenant
  FOREIGN KEY (tenant_id, to_legal_entity_id)
  REFERENCES legal_entities (tenant_id, id)
  `,
  `
  ALTER TABLE consolidation_groups
  ADD CONSTRAINT fk_cg_group_tenant
  FOREIGN KEY (tenant_id, group_company_id)
  REFERENCES group_companies (tenant_id, id)
  `,
  `
  ALTER TABLE consolidation_groups
  ADD CONSTRAINT fk_cg_calendar_tenant
  FOREIGN KEY (tenant_id, calendar_id)
  REFERENCES fiscal_calendars (tenant_id, id)
  `,
  `
  ALTER TABLE ownership_links
  ADD CONSTRAINT fk_owner_parent_tenant
  FOREIGN KEY (tenant_id, parent_legal_entity_id)
  REFERENCES legal_entities (tenant_id, id)
  `,
  `
  ALTER TABLE ownership_links
  ADD CONSTRAINT fk_owner_child_tenant
  FOREIGN KEY (tenant_id, child_legal_entity_id)
  REFERENCES legal_entities (tenant_id, id)
  `,
  `
  ALTER TABLE group_coa_mappings
  ADD CONSTRAINT fk_gcm_group_tenant
  FOREIGN KEY (tenant_id, consolidation_group_id)
  REFERENCES consolidation_groups (tenant_id, id)
  `,
  `
  ALTER TABLE group_coa_mappings
  ADD CONSTRAINT fk_gcm_entity_tenant
  FOREIGN KEY (tenant_id, legal_entity_id)
  REFERENCES legal_entities (tenant_id, id)
  `,
  `
  ALTER TABLE group_coa_mappings
  ADD CONSTRAINT fk_gcm_group_coa_tenant
  FOREIGN KEY (tenant_id, group_coa_id)
  REFERENCES charts_of_accounts (tenant_id, id)
  `,
  `
  ALTER TABLE group_coa_mappings
  ADD CONSTRAINT fk_gcm_local_coa_tenant
  FOREIGN KEY (tenant_id, local_coa_id)
  REFERENCES charts_of_accounts (tenant_id, id)
  `,
  `
  ALTER TABLE elimination_placeholders
  ADD CONSTRAINT fk_ep_group_tenant
  FOREIGN KEY (tenant_id, consolidation_group_id)
  REFERENCES consolidation_groups (tenant_id, id)
  `,
  `
  ALTER TABLE consolidation_run_entries
  ADD CONSTRAINT fk_cre_group_tenant
  FOREIGN KEY (tenant_id, consolidation_group_id)
  REFERENCES consolidation_groups (tenant_id, id)
  `,
  `
  ALTER TABLE consolidation_run_entries
  ADD CONSTRAINT fk_cre_entity_tenant
  FOREIGN KEY (tenant_id, legal_entity_id)
  REFERENCES legal_entities (tenant_id, id)
  `,
  `
  ALTER TABLE rbac_audit_logs
  ADD CONSTRAINT fk_rbac_audit_actor_tenant
  FOREIGN KEY (tenant_id, actor_user_id)
  REFERENCES users (tenant_id, id)
  `,
  `
  ALTER TABLE rbac_audit_logs
  ADD CONSTRAINT fk_rbac_audit_target_tenant
  FOREIGN KEY (tenant_id, target_user_id)
  REFERENCES users (tenant_id, id)
  `,
  `
  ALTER TABLE audit_logs
  ADD CONSTRAINT fk_audit_user_tenant
  FOREIGN KEY (tenant_id, user_id)
  REFERENCES users (tenant_id, id)
  `,
];

const migration004TenantSafeConstraints = {
  key: "m004_tenant_safe_constraints",
  description:
    "Add tenant-aware composite indexes and foreign keys to enforce tenant-safe references",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration004TenantSafeConstraints;
