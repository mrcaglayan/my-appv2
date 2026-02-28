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
  CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
  )
  `,
  `
  ALTER TABLE users
  ADD COLUMN tenant_id BIGINT UNSIGNED NULL AFTER id
  `,
  `
  ALTER TABLE users
  ADD COLUMN status ENUM('ACTIVE','DISABLED') NOT NULL DEFAULT 'ACTIVE'
  `,
  `
  ALTER TABLE users
  ADD COLUMN last_login_at TIMESTAMP NULL
  `,
  `
  ALTER TABLE users
  ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  `,
  `
  ALTER TABLE users
  ADD INDEX ix_users_tenant_id (tenant_id)
  `,
  `
  CREATE TABLE IF NOT EXISTS tenants (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    status ENUM('ACTIVE','SUSPENDED') NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tenant_code (code)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  ALTER TABLE users
  ADD CONSTRAINT fk_users_tenant
  FOREIGN KEY (tenant_id) REFERENCES tenants(id)
  `,
  `
  CREATE TABLE IF NOT EXISTS currencies (
    code CHAR(3) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    minor_units TINYINT UNSIGNED NOT NULL DEFAULT 2
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS countries (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    iso2 CHAR(2) NOT NULL,
    iso3 CHAR(3) NOT NULL,
    name VARCHAR(120) NOT NULL,
    default_currency_code CHAR(3) NOT NULL,
    UNIQUE KEY uk_country_iso2 (iso2),
    UNIQUE KEY uk_country_iso3 (iso3),
    CONSTRAINT fk_country_currency
      FOREIGN KEY (default_currency_code) REFERENCES currencies(code)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS group_companies (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_group_company_tenant_code (tenant_id, code),
    CONSTRAINT fk_group_company_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS legal_entities (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    group_company_id BIGINT UNSIGNED NOT NULL,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    tax_id VARCHAR(80) NULL,
    country_id BIGINT UNSIGNED NOT NULL,
    functional_currency_code CHAR(3) NOT NULL,
    status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_legal_entity_tenant_code (tenant_id, code),
    CONSTRAINT fk_legal_entity_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_legal_entity_group
      FOREIGN KEY (group_company_id) REFERENCES group_companies(id),
    CONSTRAINT fk_legal_entity_country
      FOREIGN KEY (country_id) REFERENCES countries(id),
    CONSTRAINT fk_legal_entity_currency
      FOREIGN KEY (functional_currency_code) REFERENCES currencies(code)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS operating_units (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    unit_type ENUM('BRANCH','PLANT','STORE','DEPARTMENT','OTHER') NOT NULL DEFAULT 'BRANCH',
    has_subledger BOOLEAN NOT NULL DEFAULT FALSE,
    status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_operating_unit_entity_code (legal_entity_id, code),
    CONSTRAINT fk_operating_unit_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_operating_unit_entity
      FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS fiscal_calendars (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    year_start_month TINYINT UNSIGNED NOT NULL,
    year_start_day TINYINT UNSIGNED NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_calendar_tenant_code (tenant_id, code),
    CONSTRAINT fk_calendar_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS fiscal_periods (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    calendar_id BIGINT UNSIGNED NOT NULL,
    fiscal_year INT NOT NULL,
    period_no TINYINT UNSIGNED NOT NULL,
    period_name VARCHAR(30) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    is_adjustment BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE KEY uk_period_unique (calendar_id, fiscal_year, period_no, is_adjustment),
    CONSTRAINT fk_period_calendar
      FOREIGN KEY (calendar_id) REFERENCES fiscal_calendars(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS books (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    calendar_id BIGINT UNSIGNED NOT NULL,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    book_type ENUM('LOCAL','GROUP') NOT NULL DEFAULT 'LOCAL',
    base_currency_code CHAR(3) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_book_entity_code (legal_entity_id, code),
    CONSTRAINT fk_book_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_book_entity
      FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_book_calendar
      FOREIGN KEY (calendar_id) REFERENCES fiscal_calendars(id),
    CONSTRAINT fk_book_currency
      FOREIGN KEY (base_currency_code) REFERENCES currencies(code)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS charts_of_accounts (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NULL,
    scope ENUM('LEGAL_ENTITY','GROUP') NOT NULL,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_coa_tenant_code (tenant_id, code),
    CONSTRAINT fk_coa_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_coa_legal_entity
      FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS accounts (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    coa_id BIGINT UNSIGNED NOT NULL,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    account_type ENUM('ASSET','LIABILITY','EQUITY','REVENUE','EXPENSE') NOT NULL,
    normal_side ENUM('DEBIT','CREDIT') NOT NULL,
    allow_posting BOOLEAN NOT NULL DEFAULT TRUE,
    parent_account_id BIGINT UNSIGNED NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_account_coa_code (coa_id, code),
    CONSTRAINT fk_account_coa
      FOREIGN KEY (coa_id) REFERENCES charts_of_accounts(id),
    CONSTRAINT fk_account_parent
      FOREIGN KEY (parent_account_id) REFERENCES accounts(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS account_mappings (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    source_account_id BIGINT UNSIGNED NOT NULL,
    target_account_id BIGINT UNSIGNED NOT NULL,
    mapping_type ENUM('LOCAL_TO_GROUP') NOT NULL DEFAULT 'LOCAL_TO_GROUP',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_account_mapping (tenant_id, source_account_id, target_account_id, mapping_type),
    CONSTRAINT fk_map_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_map_source_account
      FOREIGN KEY (source_account_id) REFERENCES accounts(id),
    CONSTRAINT fk_map_target_account
      FOREIGN KEY (target_account_id) REFERENCES accounts(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS roles (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    code VARCHAR(60) NOT NULL,
    name VARCHAR(120) NOT NULL,
    is_system BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_role_tenant_code (tenant_id, code),
    CONSTRAINT fk_role_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS permissions (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(120) NOT NULL,
    description VARCHAR(255) NOT NULL,
    UNIQUE KEY uk_permission_code (code)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS role_permissions (
    role_id BIGINT UNSIGNED NOT NULL,
    permission_id BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (role_id, permission_id),
    CONSTRAINT fk_role_perm_role
      FOREIGN KEY (role_id) REFERENCES roles(id),
    CONSTRAINT fk_role_perm_perm
      FOREIGN KEY (permission_id) REFERENCES permissions(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS user_role_scopes (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    user_id INT NOT NULL,
    role_id BIGINT UNSIGNED NOT NULL,
    scope_type ENUM('TENANT','GROUP','COUNTRY','LEGAL_ENTITY','OPERATING_UNIT') NOT NULL,
    scope_id BIGINT UNSIGNED NOT NULL,
    effect ENUM('ALLOW','DENY') NOT NULL DEFAULT 'ALLOW',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_role_scope (user_id, role_id, scope_type, scope_id),
    CONSTRAINT fk_user_role_scope_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_user_role_scope_user
      FOREIGN KEY (user_id) REFERENCES users(id),
    CONSTRAINT fk_user_role_scope_role
      FOREIGN KEY (role_id) REFERENCES roles(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS journal_entries (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    book_id BIGINT UNSIGNED NOT NULL,
    fiscal_period_id BIGINT UNSIGNED NOT NULL,
    journal_no VARCHAR(40) NOT NULL,
    source_type ENUM('MANUAL','SYSTEM','INTERCOMPANY','ELIMINATION','ADJUSTMENT') NOT NULL DEFAULT 'MANUAL',
    status ENUM('DRAFT','POSTED','REVERSED') NOT NULL DEFAULT 'DRAFT',
    entry_date DATE NOT NULL,
    document_date DATE NOT NULL,
    currency_code CHAR(3) NOT NULL,
    description VARCHAR(500) NULL,
    reference_no VARCHAR(100) NULL,
    total_debit_base DECIMAL(20,6) NOT NULL DEFAULT 0,
    total_credit_base DECIMAL(20,6) NOT NULL DEFAULT 0,
    created_by_user_id INT NOT NULL,
    posted_by_user_id INT NULL,
    posted_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_journal_book_no (book_id, journal_no),
    KEY ix_journal_tenant_entity_period (tenant_id, legal_entity_id, fiscal_period_id),
    CONSTRAINT fk_journal_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_journal_entity
      FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_journal_book
      FOREIGN KEY (book_id) REFERENCES books(id),
    CONSTRAINT fk_journal_period
      FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
    CONSTRAINT fk_journal_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_journal_created_by
      FOREIGN KEY (created_by_user_id) REFERENCES users(id),
    CONSTRAINT fk_journal_posted_by
      FOREIGN KEY (posted_by_user_id) REFERENCES users(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS journal_lines (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    journal_entry_id BIGINT UNSIGNED NOT NULL,
    line_no INT NOT NULL,
    account_id BIGINT UNSIGNED NOT NULL,
    operating_unit_id BIGINT UNSIGNED NULL,
    counterparty_legal_entity_id BIGINT UNSIGNED NULL,
    description VARCHAR(500) NULL,
    currency_code CHAR(3) NOT NULL,
    amount_txn DECIMAL(20,6) NOT NULL DEFAULT 0,
    debit_base DECIMAL(20,6) NOT NULL DEFAULT 0,
    credit_base DECIMAL(20,6) NOT NULL DEFAULT 0,
    tax_code VARCHAR(40) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_journal_line_no (journal_entry_id, line_no),
    KEY ix_journal_line_account (account_id),
    CONSTRAINT fk_jline_journal
      FOREIGN KEY (journal_entry_id) REFERENCES journal_entries(id),
    CONSTRAINT fk_jline_account
      FOREIGN KEY (account_id) REFERENCES accounts(id),
    CONSTRAINT fk_jline_operating_unit
      FOREIGN KEY (operating_unit_id) REFERENCES operating_units(id),
    CONSTRAINT fk_jline_counterparty
      FOREIGN KEY (counterparty_legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_jline_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CHECK ((debit_base > 0 AND credit_base = 0) OR (credit_base > 0 AND debit_base = 0))
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS period_statuses (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    book_id BIGINT UNSIGNED NOT NULL,
    fiscal_period_id BIGINT UNSIGNED NOT NULL,
    status ENUM('OPEN','SOFT_CLOSED','HARD_CLOSED') NOT NULL DEFAULT 'OPEN',
    closed_by_user_id INT NULL,
    closed_at TIMESTAMP NULL,
    note VARCHAR(255) NULL,
    UNIQUE KEY uk_book_period_status (book_id, fiscal_period_id),
    CONSTRAINT fk_period_status_book
      FOREIGN KEY (book_id) REFERENCES books(id),
    CONSTRAINT fk_period_status_period
      FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
    CONSTRAINT fk_period_status_user
      FOREIGN KEY (closed_by_user_id) REFERENCES users(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS fx_rates (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    rate_date DATE NOT NULL,
    from_currency_code CHAR(3) NOT NULL,
    to_currency_code CHAR(3) NOT NULL,
    rate_type ENUM('SPOT','AVERAGE','CLOSING') NOT NULL,
    rate DECIMAL(20,10) NOT NULL,
    source VARCHAR(120) NULL,
    is_locked BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_fx_unique (tenant_id, rate_date, from_currency_code, to_currency_code, rate_type),
    CONSTRAINT fk_fx_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_fx_from_currency
      FOREIGN KEY (from_currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_fx_to_currency
      FOREIGN KEY (to_currency_code) REFERENCES currencies(code)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS intercompany_pairs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    from_legal_entity_id BIGINT UNSIGNED NOT NULL,
    to_legal_entity_id BIGINT UNSIGNED NOT NULL,
    receivable_account_id BIGINT UNSIGNED NULL,
    payable_account_id BIGINT UNSIGNED NULL,
    status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_intercompany_pair (tenant_id, from_legal_entity_id, to_legal_entity_id),
    CONSTRAINT fk_ic_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_ic_from_entity
      FOREIGN KEY (from_legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_ic_to_entity
      FOREIGN KEY (to_legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_ic_recv_account
      FOREIGN KEY (receivable_account_id) REFERENCES accounts(id),
    CONSTRAINT fk_ic_pay_account
      FOREIGN KEY (payable_account_id) REFERENCES accounts(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS consolidation_groups (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    group_company_id BIGINT UNSIGNED NOT NULL,
    calendar_id BIGINT UNSIGNED NOT NULL,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    presentation_currency_code CHAR(3) NOT NULL,
    status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_cons_group_tenant_code (tenant_id, code),
    CONSTRAINT fk_cons_group_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_cons_group_group_company
      FOREIGN KEY (group_company_id) REFERENCES group_companies(id),
    CONSTRAINT fk_cons_group_calendar
      FOREIGN KEY (calendar_id) REFERENCES fiscal_calendars(id),
    CONSTRAINT fk_cons_group_currency
      FOREIGN KEY (presentation_currency_code) REFERENCES currencies(code)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS consolidation_group_members (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    consolidation_group_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    consolidation_method ENUM('FULL','EQUITY','PROPORTIONATE') NOT NULL DEFAULT 'FULL',
    ownership_pct DECIMAL(9,6) NOT NULL DEFAULT 1.000000,
    effective_from DATE NOT NULL,
    effective_to DATE NULL,
    UNIQUE KEY uk_cons_member (consolidation_group_id, legal_entity_id, effective_from),
    CONSTRAINT fk_cons_member_group
      FOREIGN KEY (consolidation_group_id) REFERENCES consolidation_groups(id),
    CONSTRAINT fk_cons_member_entity
      FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS ownership_links (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    parent_legal_entity_id BIGINT UNSIGNED NOT NULL,
    child_legal_entity_id BIGINT UNSIGNED NOT NULL,
    ownership_pct DECIMAL(9,6) NOT NULL,
    voting_pct DECIMAL(9,6) NULL,
    effective_from DATE NOT NULL,
    effective_to DATE NULL,
    UNIQUE KEY uk_ownership_effective (parent_legal_entity_id, child_legal_entity_id, effective_from),
    CONSTRAINT fk_owner_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_owner_parent
      FOREIGN KEY (parent_legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_owner_child
      FOREIGN KEY (child_legal_entity_id) REFERENCES legal_entities(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS consolidation_runs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    consolidation_group_id BIGINT UNSIGNED NOT NULL,
    fiscal_period_id BIGINT UNSIGNED NOT NULL,
    run_name VARCHAR(100) NOT NULL,
    status ENUM('DRAFT','IN_PROGRESS','COMPLETED','LOCKED','FAILED') NOT NULL DEFAULT 'DRAFT',
    presentation_currency_code CHAR(3) NOT NULL,
    started_by_user_id INT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    notes VARCHAR(500) NULL,
    UNIQUE KEY uk_cons_run_unique (consolidation_group_id, fiscal_period_id, run_name),
    CONSTRAINT fk_run_group
      FOREIGN KEY (consolidation_group_id) REFERENCES consolidation_groups(id),
    CONSTRAINT fk_run_period
      FOREIGN KEY (fiscal_period_id) REFERENCES fiscal_periods(id),
    CONSTRAINT fk_run_currency
      FOREIGN KEY (presentation_currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_run_user
      FOREIGN KEY (started_by_user_id) REFERENCES users(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS elimination_entries (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    consolidation_run_id BIGINT UNSIGNED NOT NULL,
    status ENUM('DRAFT','POSTED') NOT NULL DEFAULT 'DRAFT',
    description VARCHAR(500) NOT NULL,
    reference_no VARCHAR(100) NULL,
    created_by_user_id INT NOT NULL,
    posted_by_user_id INT NULL,
    posted_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_elim_entry_run
      FOREIGN KEY (consolidation_run_id) REFERENCES consolidation_runs(id),
    CONSTRAINT fk_elim_entry_created_by
      FOREIGN KEY (created_by_user_id) REFERENCES users(id),
    CONSTRAINT fk_elim_entry_posted_by
      FOREIGN KEY (posted_by_user_id) REFERENCES users(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS elimination_lines (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    elimination_entry_id BIGINT UNSIGNED NOT NULL,
    line_no INT NOT NULL,
    account_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NULL,
    counterparty_legal_entity_id BIGINT UNSIGNED NULL,
    debit_amount DECIMAL(20,6) NOT NULL DEFAULT 0,
    credit_amount DECIMAL(20,6) NOT NULL DEFAULT 0,
    currency_code CHAR(3) NOT NULL,
    description VARCHAR(500) NULL,
    UNIQUE KEY uk_elim_line_no (elimination_entry_id, line_no),
    CONSTRAINT fk_elim_line_entry
      FOREIGN KEY (elimination_entry_id) REFERENCES elimination_entries(id),
    CONSTRAINT fk_elim_line_account
      FOREIGN KEY (account_id) REFERENCES accounts(id),
    CONSTRAINT fk_elim_line_entity
      FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_elim_line_counterparty
      FOREIGN KEY (counterparty_legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_elim_line_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CHECK ((debit_amount > 0 AND credit_amount = 0) OR (credit_amount > 0 AND debit_amount = 0))
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS consolidation_adjustments (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    consolidation_run_id BIGINT UNSIGNED NOT NULL,
    adjustment_type ENUM('TOPSIDE','RECLASS','MANUAL_FX') NOT NULL DEFAULT 'TOPSIDE',
    status ENUM('DRAFT','POSTED') NOT NULL DEFAULT 'DRAFT',
    legal_entity_id BIGINT UNSIGNED NULL,
    account_id BIGINT UNSIGNED NOT NULL,
    debit_amount DECIMAL(20,6) NOT NULL DEFAULT 0,
    credit_amount DECIMAL(20,6) NOT NULL DEFAULT 0,
    currency_code CHAR(3) NOT NULL,
    description VARCHAR(500) NOT NULL,
    created_by_user_id INT NOT NULL,
    posted_by_user_id INT NULL,
    posted_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_cons_adj_run
      FOREIGN KEY (consolidation_run_id) REFERENCES consolidation_runs(id),
    CONSTRAINT fk_cons_adj_entity
      FOREIGN KEY (legal_entity_id) REFERENCES legal_entities(id),
    CONSTRAINT fk_cons_adj_account
      FOREIGN KEY (account_id) REFERENCES accounts(id),
    CONSTRAINT fk_cons_adj_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_cons_adj_created_by
      FOREIGN KEY (created_by_user_id) REFERENCES users(id),
    CONSTRAINT fk_cons_adj_posted_by
      FOREIGN KEY (posted_by_user_id) REFERENCES users(id),
    CHECK ((debit_amount > 0 AND credit_amount = 0) OR (credit_amount > 0 AND debit_amount = 0))
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    user_id INT NULL,
    action VARCHAR(120) NOT NULL,
    resource_type VARCHAR(80) NOT NULL,
    resource_id VARCHAR(80) NULL,
    scope_type VARCHAR(30) NULL,
    scope_id BIGINT UNSIGNED NULL,
    request_id VARCHAR(80) NULL,
    ip_address VARCHAR(64) NULL,
    user_agent VARCHAR(255) NULL,
    payload_json JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY ix_audit_tenant_time (tenant_id, created_at),
    CONSTRAINT fk_audit_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_audit_user
      FOREIGN KEY (user_id) REFERENCES users(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  INSERT INTO currencies (code, name, minor_units)
  VALUES
    ('USD', 'US Dollar', 2),
    ('EUR', 'Euro', 2),
    ('TRY', 'Turkish Lira', 2),
    ('GBP', 'Pound Sterling', 2),
    ('AFN', 'Afghan Afghani', 2)
  ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    minor_units = VALUES(minor_units)
  `,
  `
  INSERT INTO countries (iso2, iso3, name, default_currency_code)
  VALUES
    ('US', 'USA', 'United States', 'USD'),
    ('TR', 'TUR', 'Turkey', 'TRY'),
    ('GB', 'GBR', 'United Kingdom', 'GBP'),
    ('DE', 'DEU', 'Germany', 'EUR'),
    ('AF', 'AFG', 'Afghanistan', 'AFN')
  ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    default_currency_code = VALUES(default_currency_code)
  `,
];

const migration001GlobalMultiEntity = {
  key: "m001_global_multi_entity",
  description: "Initial global multi-entity ERP accounting schema",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration001GlobalMultiEntity;
