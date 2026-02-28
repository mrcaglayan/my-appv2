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
  CREATE TABLE IF NOT EXISTS data_scopes (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    user_id INT NOT NULL,
    scope_type ENUM('TENANT','GROUP','COUNTRY','LEGAL_ENTITY','OPERATING_UNIT') NOT NULL,
    scope_id BIGINT UNSIGNED NOT NULL,
    effect ENUM('ALLOW','DENY') NOT NULL DEFAULT 'ALLOW',
    created_by_user_id INT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_data_scope_user_scope (tenant_id, user_id, scope_type, scope_id),
    KEY ix_data_scope_lookup (tenant_id, user_id, scope_type, effect),
    CONSTRAINT fk_data_scope_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_data_scope_user
      FOREIGN KEY (user_id) REFERENCES users(id),
    CONSTRAINT fk_data_scope_created_by
      FOREIGN KEY (created_by_user_id) REFERENCES users(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  ALTER TABLE legal_entities
  ADD COLUMN is_intercompany_enabled BOOLEAN NOT NULL DEFAULT TRUE
  `,
  `
  ALTER TABLE legal_entities
  ADD COLUMN intercompany_partner_required BOOLEAN NOT NULL DEFAULT FALSE
  `,
  `
  ALTER TABLE legal_entities
  ADD INDEX ix_legal_entities_tenant_group_country (tenant_id, group_company_id, country_id)
  `,
  `
  ALTER TABLE operating_units
  ADD INDEX ix_operating_units_tenant_entity (tenant_id, legal_entity_id)
  `,
  `
  ALTER TABLE journal_entries
  ADD COLUMN reversed_by_user_id INT NULL
  `,
  `
  ALTER TABLE journal_entries
  ADD COLUMN reversed_at TIMESTAMP NULL
  `,
  `
  ALTER TABLE journal_entries
  ADD COLUMN reversal_journal_entry_id BIGINT UNSIGNED NULL
  `,
  `
  ALTER TABLE journal_entries
  ADD COLUMN reverse_reason VARCHAR(255) NULL
  `,
  `
  ALTER TABLE journal_entries
  ADD CONSTRAINT fk_journal_reversed_by
  FOREIGN KEY (reversed_by_user_id) REFERENCES users(id)
  `,
  `
  ALTER TABLE journal_entries
  ADD CONSTRAINT fk_journal_reversal_entry
  FOREIGN KEY (reversal_journal_entry_id) REFERENCES journal_entries(id)
  `,
  `
  CREATE TABLE IF NOT EXISTS group_coa_mappings (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    consolidation_group_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    group_coa_id BIGINT UNSIGNED NOT NULL,
    local_coa_id BIGINT UNSIGNED NOT NULL,
    status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_group_coa_mapping (
      tenant_id,
      consolidation_group_id,
      legal_entity_id,
      group_coa_id,
      local_coa_id
    ),
    CONSTRAINT fk_group_coa_map_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_group_coa_map_cons_group
      FOREIGN KEY (consolidation_group_id) REFERENCES consolidation_groups(id),
    CONSTRAINT fk_group_coa_map_legal_entity
      FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_group_coa_map_group_coa
      FOREIGN KEY (group_coa_id) REFERENCES charts_of_accounts(id),
    CONSTRAINT fk_group_coa_map_local_coa
      FOREIGN KEY (local_coa_id) REFERENCES charts_of_accounts(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS elimination_placeholders (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    consolidation_group_id BIGINT UNSIGNED NOT NULL,
    placeholder_code VARCHAR(80) NOT NULL,
    name VARCHAR(255) NOT NULL,
    account_id BIGINT UNSIGNED NULL,
    default_direction ENUM('DEBIT','CREDIT','AUTO') NOT NULL DEFAULT 'AUTO',
    description VARCHAR(500) NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_elim_placeholder_code (consolidation_group_id, placeholder_code),
    CONSTRAINT fk_elim_placeholder_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_elim_placeholder_group
      FOREIGN KEY (consolidation_group_id) REFERENCES consolidation_groups(id),
    CONSTRAINT fk_elim_placeholder_account
      FOREIGN KEY (account_id) REFERENCES accounts(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
];

const migration002AuthzOnboardingFoundations = {
  key: "m002_authz_onboarding_foundations",
  description:
    "Authorization data scopes, journal lifecycle metadata, and consolidation foundation tables",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration002AuthzOnboardingFoundations;
