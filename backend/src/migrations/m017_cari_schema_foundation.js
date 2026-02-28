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
  ALTER TABLE journal_entries
  ADD UNIQUE KEY uk_journal_entries_tenant_id_id (tenant_id, id)
  `,
  `
  CREATE TABLE IF NOT EXISTS payment_terms (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    code VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    due_days INT UNSIGNED NOT NULL DEFAULT 0,
    grace_days INT UNSIGNED NOT NULL DEFAULT 0,
    is_end_of_month BOOLEAN NOT NULL DEFAULT FALSE,
    status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_payment_terms_tenant_entity_code (tenant_id, legal_entity_id, code),
    UNIQUE KEY uk_payment_terms_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_payment_terms_tenant_entity_id (tenant_id, legal_entity_id, id),
    KEY ix_payment_terms_tenant_id (tenant_id),
    KEY ix_payment_terms_tenant_entity (tenant_id, legal_entity_id),
    KEY ix_payment_terms_tenant_status (tenant_id, status),
    CONSTRAINT fk_payment_terms_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_payment_terms_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CHECK (due_days >= 0),
    CHECK (grace_days >= 0)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS counterparties (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    code VARCHAR(60) NOT NULL,
    name VARCHAR(255) NOT NULL,
    is_customer BOOLEAN NOT NULL DEFAULT FALSE,
    is_vendor BOOLEAN NOT NULL DEFAULT FALSE,
    tax_id VARCHAR(80) NULL,
    email VARCHAR(255) NULL,
    phone VARCHAR(80) NULL,
    default_currency_code CHAR(3) NULL,
    default_payment_term_id BIGINT UNSIGNED NULL,
    status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
    notes VARCHAR(500) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_counterparties_tenant_entity_code (tenant_id, legal_entity_id, code),
    UNIQUE KEY uk_counterparties_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_counterparties_tenant_entity_id (tenant_id, legal_entity_id, id),
    KEY ix_counterparties_tenant_id (tenant_id),
    KEY ix_counterparties_tenant_entity (tenant_id, legal_entity_id),
    KEY ix_counterparties_tenant_entity_roles (tenant_id, legal_entity_id, is_customer, is_vendor),
    KEY ix_counterparties_tenant_status (tenant_id, status),
    CONSTRAINT fk_counterparties_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_counterparties_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_counterparties_currency
      FOREIGN KEY (default_currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_counterparties_payment_term_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, default_payment_term_id)
      REFERENCES payment_terms(tenant_id, legal_entity_id, id),
    CHECK (CHAR_LENGTH(code) > 0),
    CHECK (is_customer OR is_vendor)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS counterparty_contacts (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    counterparty_id BIGINT UNSIGNED NOT NULL,
    contact_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NULL,
    phone VARCHAR(80) NULL,
    title VARCHAR(120) NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_cp_contacts_tenant_id_id (tenant_id, id),
    KEY ix_cp_contacts_tenant_id (tenant_id),
    KEY ix_cp_contacts_tenant_entity (tenant_id, legal_entity_id),
    KEY ix_cp_contacts_tenant_counterparty (tenant_id, counterparty_id),
    KEY ix_cp_contacts_tenant_status (tenant_id, status),
    CONSTRAINT fk_cp_contacts_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_cp_contacts_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_cp_contacts_counterparty_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, counterparty_id)
      REFERENCES counterparties(tenant_id, legal_entity_id, id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS counterparty_addresses (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    counterparty_id BIGINT UNSIGNED NOT NULL,
    address_type ENUM('BILLING','SHIPPING','REGISTERED','OTHER') NOT NULL DEFAULT 'BILLING',
    address_line1 VARCHAR(255) NOT NULL,
    address_line2 VARCHAR(255) NULL,
    city VARCHAR(120) NULL,
    state_region VARCHAR(120) NULL,
    postal_code VARCHAR(30) NULL,
    country_id BIGINT UNSIGNED NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_cp_addresses_tenant_id_id (tenant_id, id),
    KEY ix_cp_addresses_tenant_id (tenant_id),
    KEY ix_cp_addresses_tenant_entity (tenant_id, legal_entity_id),
    KEY ix_cp_addresses_tenant_counterparty (tenant_id, counterparty_id),
    KEY ix_cp_addresses_tenant_status (tenant_id, status),
    CONSTRAINT fk_cp_addresses_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_cp_addresses_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_cp_addresses_counterparty_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, counterparty_id)
      REFERENCES counterparties(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_cp_addresses_country
      FOREIGN KEY (country_id) REFERENCES countries(id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS cari_documents (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    counterparty_id BIGINT UNSIGNED NOT NULL,
    payment_term_id BIGINT UNSIGNED NULL,
    direction ENUM('AR','AP') NOT NULL,
    document_type ENUM(
      'INVOICE',
      'DEBIT_NOTE',
      'CREDIT_NOTE',
      'PAYMENT',
      'ADJUSTMENT',
      'OPENING_BALANCE',
      'OTHER'
    ) NOT NULL DEFAULT 'INVOICE',
    sequence_namespace VARCHAR(40) NOT NULL,
    fiscal_year INT NOT NULL,
    sequence_no BIGINT UNSIGNED NOT NULL,
    document_no VARCHAR(80) NOT NULL,
    status ENUM('DRAFT','POSTED','PARTIALLY_SETTLED','SETTLED','CANCELLED','REVERSED')
      NOT NULL DEFAULT 'DRAFT',
    document_date DATE NOT NULL,
    due_date DATE NULL,
    amount_txn DECIMAL(20,6) NOT NULL,
    amount_base DECIMAL(20,6) NOT NULL,
    open_amount_txn DECIMAL(20,6) NOT NULL,
    open_amount_base DECIMAL(20,6) NOT NULL,
    currency_code CHAR(3) NOT NULL,
    fx_rate DECIMAL(20,10) NULL,
    counterparty_code_snapshot VARCHAR(60) NOT NULL,
    counterparty_name_snapshot VARCHAR(255) NOT NULL,
    payment_term_snapshot VARCHAR(255) NULL,
    due_date_snapshot DATE NULL,
    currency_code_snapshot CHAR(3) NOT NULL,
    fx_rate_snapshot DECIMAL(20,10) NULL,
    posted_journal_entry_id BIGINT UNSIGNED NULL,
    reversal_of_document_id BIGINT UNSIGNED NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    posted_at TIMESTAMP NULL,
    reversed_at TIMESTAMP NULL,
    UNIQUE KEY uk_cari_docs_tenant_entity_adr_seq (
      tenant_id,
      legal_entity_id,
      direction,
      sequence_namespace,
      fiscal_year,
      sequence_no
    ),
    UNIQUE KEY uk_cari_docs_tenant_entity_adr_doc_no (
      tenant_id,
      legal_entity_id,
      direction,
      sequence_namespace,
      fiscal_year,
      document_no
    ),
    UNIQUE KEY uk_cari_docs_posted_journal (posted_journal_entry_id),
    UNIQUE KEY uk_cari_docs_single_reversal (reversal_of_document_id),
    UNIQUE KEY uk_cari_docs_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_cari_docs_tenant_entity_id (tenant_id, legal_entity_id, id),
    KEY ix_cari_docs_tenant_id (tenant_id),
    KEY ix_cari_docs_tenant_entity (tenant_id, legal_entity_id),
    KEY ix_cari_docs_tenant_counterparty (tenant_id, counterparty_id),
    KEY ix_cari_docs_tenant_status (tenant_id, status),
    KEY ix_cari_docs_tenant_document_date (tenant_id, document_date),
    KEY ix_cari_docs_tenant_due_date (tenant_id, due_date),
    KEY ix_cari_docs_tenant_open_amount_txn (tenant_id, open_amount_txn),
    KEY ix_cari_docs_tenant_open_amount_base (tenant_id, open_amount_base),
    CONSTRAINT fk_cari_docs_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_cari_docs_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_cari_docs_counterparty_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, counterparty_id)
      REFERENCES counterparties(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_cari_docs_payment_term_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, payment_term_id)
      REFERENCES payment_terms(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_cari_docs_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_cari_docs_currency_snapshot
      FOREIGN KEY (currency_code_snapshot) REFERENCES currencies(code),
    CONSTRAINT fk_cari_docs_journal_tenant
      FOREIGN KEY (tenant_id, posted_journal_entry_id) REFERENCES journal_entries(tenant_id, id),
    CONSTRAINT fk_cari_docs_reversal_tenant
      FOREIGN KEY (tenant_id, reversal_of_document_id) REFERENCES cari_documents(tenant_id, id),
    CHECK (amount_txn >= 0),
    CHECK (amount_base >= 0),
    CHECK (open_amount_txn >= 0),
    CHECK (open_amount_base >= 0),
    CHECK (open_amount_txn <= amount_txn),
    CHECK (open_amount_base <= amount_base),
    CHECK (fx_rate IS NULL OR fx_rate > 0),
    CHECK (fx_rate_snapshot IS NULL OR fx_rate_snapshot > 0),
    CHECK (due_date IS NULL OR due_date >= document_date),
    CHECK (due_date_snapshot IS NULL OR due_date_snapshot >= document_date),
    CHECK (
      (
        status IN ('POSTED','PARTIALLY_SETTLED','SETTLED','REVERSED')
        AND posted_journal_entry_id IS NOT NULL
      )
      OR status IN ('DRAFT','CANCELLED')
    )
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS cari_open_items (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    counterparty_id BIGINT UNSIGNED NOT NULL,
    document_id BIGINT UNSIGNED NOT NULL,
    item_no INT UNSIGNED NOT NULL DEFAULT 1,
    status ENUM('OPEN','PARTIALLY_SETTLED','SETTLED','WRITTEN_OFF','CANCELLED')
      NOT NULL DEFAULT 'OPEN',
    document_date DATE NOT NULL,
    due_date DATE NOT NULL,
    original_amount_txn DECIMAL(20,6) NOT NULL,
    original_amount_base DECIMAL(20,6) NOT NULL,
    residual_amount_txn DECIMAL(20,6) NOT NULL,
    residual_amount_base DECIMAL(20,6) NOT NULL,
    settled_amount_txn DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    settled_amount_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    currency_code CHAR(3) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_cari_oi_tenant_doc_item (tenant_id, document_id, item_no),
    UNIQUE KEY uk_cari_oi_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_cari_oi_tenant_entity_id (tenant_id, legal_entity_id, id),
    KEY ix_cari_oi_tenant_id (tenant_id),
    KEY ix_cari_oi_tenant_entity (tenant_id, legal_entity_id),
    KEY ix_cari_oi_tenant_counterparty (tenant_id, counterparty_id),
    KEY ix_cari_oi_tenant_status (tenant_id, status),
    KEY ix_cari_oi_tenant_due_date (tenant_id, due_date),
    KEY ix_cari_oi_tenant_document_date (tenant_id, document_date),
    KEY ix_cari_oi_tenant_residual_txn (tenant_id, residual_amount_txn),
    KEY ix_cari_oi_tenant_residual_base (tenant_id, residual_amount_base),
    CONSTRAINT fk_cari_oi_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_cari_oi_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_cari_oi_counterparty_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, counterparty_id)
      REFERENCES counterparties(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_cari_oi_document_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, document_id)
      REFERENCES cari_documents(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_cari_oi_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CHECK (item_no > 0),
    CHECK (original_amount_txn >= 0),
    CHECK (original_amount_base >= 0),
    CHECK (residual_amount_txn >= 0),
    CHECK (residual_amount_base >= 0),
    CHECK (settled_amount_txn >= 0),
    CHECK (settled_amount_base >= 0),
    CHECK (residual_amount_txn <= original_amount_txn),
    CHECK (residual_amount_base <= original_amount_base),
    CHECK (settled_amount_txn <= original_amount_txn),
    CHECK (settled_amount_base <= original_amount_base),
    CHECK (due_date >= document_date),
    CHECK (
      status <> 'SETTLED'
      OR (residual_amount_txn = 0 AND residual_amount_base = 0)
    )
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS cari_settlement_batches (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    counterparty_id BIGINT UNSIGNED NULL,
    sequence_namespace VARCHAR(40) NOT NULL,
    fiscal_year INT NOT NULL,
    sequence_no BIGINT UNSIGNED NOT NULL,
    settlement_no VARCHAR(80) NOT NULL,
    settlement_date DATE NOT NULL,
    status ENUM('DRAFT','POSTED','REVERSED','CANCELLED') NOT NULL DEFAULT 'DRAFT',
    total_allocated_txn DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    total_allocated_base DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    currency_code CHAR(3) NOT NULL,
    posted_journal_entry_id BIGINT UNSIGNED NULL,
    reversal_of_settlement_batch_id BIGINT UNSIGNED NULL,
    bank_statement_line_id BIGINT UNSIGNED NULL,
    bank_transaction_ref VARCHAR(100) NULL,
    bank_attach_idempotency_key VARCHAR(100) NULL,
    bank_apply_idempotency_key VARCHAR(100) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    posted_at TIMESTAMP NULL,
    reversed_at TIMESTAMP NULL,
    UNIQUE KEY uk_cari_settle_batches_tenant_seq (
      tenant_id,
      legal_entity_id,
      sequence_namespace,
      fiscal_year,
      sequence_no
    ),
    UNIQUE KEY uk_cari_settle_batches_tenant_no (tenant_id, legal_entity_id, settlement_no),
    UNIQUE KEY uk_cari_settle_batches_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_cari_settle_batches_tenant_entity_id (tenant_id, legal_entity_id, id),
    UNIQUE KEY uk_cari_settle_batches_posted_journal (posted_journal_entry_id),
    UNIQUE KEY uk_cari_settle_batches_single_reversal (reversal_of_settlement_batch_id),
    UNIQUE KEY uk_cari_settle_batches_bank_attach_idempo (
      tenant_id,
      legal_entity_id,
      bank_attach_idempotency_key
    ),
    UNIQUE KEY uk_cari_settle_batches_bank_apply_idempo (
      tenant_id,
      legal_entity_id,
      bank_apply_idempotency_key
    ),
    KEY ix_cari_settle_batches_tenant_id (tenant_id),
    KEY ix_cari_settle_batches_tenant_entity (tenant_id, legal_entity_id),
    KEY ix_cari_settle_batches_tenant_counterparty (tenant_id, counterparty_id),
    KEY ix_cari_settle_batches_tenant_status (tenant_id, status),
    KEY ix_cari_settle_batches_tenant_date (tenant_id, settlement_date),
    KEY ix_cari_settle_batches_tenant_alloc_txn (tenant_id, total_allocated_txn),
    KEY ix_cari_settle_batches_tenant_alloc_base (tenant_id, total_allocated_base),
    CONSTRAINT fk_cari_settle_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_cari_settle_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_cari_settle_counterparty_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, counterparty_id)
      REFERENCES counterparties(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_cari_settle_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_cari_settle_journal_tenant
      FOREIGN KEY (tenant_id, posted_journal_entry_id) REFERENCES journal_entries(tenant_id, id),
    CONSTRAINT fk_cari_settle_reversal_tenant
      FOREIGN KEY (tenant_id, reversal_of_settlement_batch_id)
      REFERENCES cari_settlement_batches(tenant_id, id),
    CHECK (fiscal_year >= 1900),
    CHECK (total_allocated_txn >= 0),
    CHECK (total_allocated_base >= 0),
    CHECK (
      bank_attach_idempotency_key IS NULL
      OR CHAR_LENGTH(bank_attach_idempotency_key) > 0
    ),
    CHECK (
      bank_apply_idempotency_key IS NULL
      OR CHAR_LENGTH(bank_apply_idempotency_key) > 0
    ),
    CHECK (
      (
        status IN ('POSTED','REVERSED')
        AND posted_journal_entry_id IS NOT NULL
      )
      OR status IN ('DRAFT','CANCELLED')
    ),
    CHECK (status <> 'REVERSED' OR reversed_at IS NOT NULL)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS cari_settlement_allocations (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    settlement_batch_id BIGINT UNSIGNED NOT NULL,
    open_item_id BIGINT UNSIGNED NOT NULL,
    allocation_date DATE NOT NULL,
    allocation_amount_txn DECIMAL(20,6) NOT NULL,
    allocation_amount_base DECIMAL(20,6) NOT NULL,
    apply_idempotency_key VARCHAR(100) NULL,
    bank_statement_line_id BIGINT UNSIGNED NULL,
    bank_apply_idempotency_key VARCHAR(100) NULL,
    note VARCHAR(500) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_cari_alloc_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_cari_alloc_apply_idempo (tenant_id, legal_entity_id, apply_idempotency_key),
    UNIQUE KEY uk_cari_alloc_bank_apply_idempo (
      tenant_id,
      legal_entity_id,
      bank_apply_idempotency_key
    ),
    KEY ix_cari_alloc_tenant_id (tenant_id),
    KEY ix_cari_alloc_tenant_entity (tenant_id, legal_entity_id),
    KEY ix_cari_alloc_tenant_batch (tenant_id, settlement_batch_id),
    KEY ix_cari_alloc_tenant_open_item (tenant_id, open_item_id),
    KEY ix_cari_alloc_tenant_date (tenant_id, allocation_date),
    KEY ix_cari_alloc_tenant_amount_txn (tenant_id, allocation_amount_txn),
    KEY ix_cari_alloc_tenant_amount_base (tenant_id, allocation_amount_base),
    CONSTRAINT fk_cari_alloc_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_cari_alloc_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_cari_alloc_batch_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, settlement_batch_id)
      REFERENCES cari_settlement_batches(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_cari_alloc_open_item_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, open_item_id)
      REFERENCES cari_open_items(tenant_id, legal_entity_id, id),
    CHECK (allocation_amount_txn > 0),
    CHECK (allocation_amount_base > 0),
    CHECK (apply_idempotency_key IS NULL OR CHAR_LENGTH(apply_idempotency_key) > 0),
    CHECK (
      bank_apply_idempotency_key IS NULL
      OR CHAR_LENGTH(bank_apply_idempotency_key) > 0
    )
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
  `
  CREATE TABLE IF NOT EXISTS cari_unapplied_cash (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id BIGINT UNSIGNED NOT NULL,
    legal_entity_id BIGINT UNSIGNED NOT NULL,
    counterparty_id BIGINT UNSIGNED NULL,
    cash_receipt_no VARCHAR(80) NOT NULL,
    receipt_date DATE NOT NULL,
    status ENUM('UNAPPLIED','PARTIALLY_APPLIED','FULLY_APPLIED','REFUNDED','REVERSED')
      NOT NULL DEFAULT 'UNAPPLIED',
    amount_txn DECIMAL(20,6) NOT NULL,
    amount_base DECIMAL(20,6) NOT NULL,
    residual_amount_txn DECIMAL(20,6) NOT NULL,
    residual_amount_base DECIMAL(20,6) NOT NULL,
    currency_code CHAR(3) NOT NULL,
    posted_journal_entry_id BIGINT UNSIGNED NULL,
    settlement_batch_id BIGINT UNSIGNED NULL,
    reversal_of_unapplied_cash_id BIGINT UNSIGNED NULL,
    bank_statement_line_id BIGINT UNSIGNED NULL,
    bank_transaction_ref VARCHAR(100) NULL,
    bank_attach_idempotency_key VARCHAR(100) NULL,
    bank_apply_idempotency_key VARCHAR(100) NULL,
    note VARCHAR(500) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_cari_unap_tenant_receipt (tenant_id, legal_entity_id, cash_receipt_no),
    UNIQUE KEY uk_cari_unap_tenant_id_id (tenant_id, id),
    UNIQUE KEY uk_cari_unap_tenant_entity_id (tenant_id, legal_entity_id, id),
    UNIQUE KEY uk_cari_unap_posted_journal (posted_journal_entry_id),
    UNIQUE KEY uk_cari_unap_single_reversal (reversal_of_unapplied_cash_id),
    UNIQUE KEY uk_cari_unap_bank_attach_idempo (
      tenant_id,
      legal_entity_id,
      bank_attach_idempotency_key
    ),
    UNIQUE KEY uk_cari_unap_bank_apply_idempo (
      tenant_id,
      legal_entity_id,
      bank_apply_idempotency_key
    ),
    KEY ix_cari_unap_tenant_id (tenant_id),
    KEY ix_cari_unap_tenant_entity (tenant_id, legal_entity_id),
    KEY ix_cari_unap_tenant_counterparty (tenant_id, counterparty_id),
    KEY ix_cari_unap_tenant_status (tenant_id, status),
    KEY ix_cari_unap_tenant_receipt_date (tenant_id, receipt_date),
    KEY ix_cari_unap_tenant_residual_txn (tenant_id, residual_amount_txn),
    KEY ix_cari_unap_tenant_residual_base (tenant_id, residual_amount_base),
    CONSTRAINT fk_cari_unap_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_cari_unap_entity_tenant
      FOREIGN KEY (tenant_id, legal_entity_id) REFERENCES legal_entities(tenant_id, id),
    CONSTRAINT fk_cari_unap_counterparty_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, counterparty_id)
      REFERENCES counterparties(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_cari_unap_currency
      FOREIGN KEY (currency_code) REFERENCES currencies(code),
    CONSTRAINT fk_cari_unap_journal_tenant
      FOREIGN KEY (tenant_id, posted_journal_entry_id) REFERENCES journal_entries(tenant_id, id),
    CONSTRAINT fk_cari_unap_settle_batch_tenant
      FOREIGN KEY (tenant_id, legal_entity_id, settlement_batch_id)
      REFERENCES cari_settlement_batches(tenant_id, legal_entity_id, id),
    CONSTRAINT fk_cari_unap_reversal_tenant
      FOREIGN KEY (tenant_id, reversal_of_unapplied_cash_id)
      REFERENCES cari_unapplied_cash(tenant_id, id),
    CHECK (amount_txn >= 0),
    CHECK (amount_base >= 0),
    CHECK (residual_amount_txn >= 0),
    CHECK (residual_amount_base >= 0),
    CHECK (residual_amount_txn <= amount_txn),
    CHECK (residual_amount_base <= amount_base),
    CHECK (
      bank_attach_idempotency_key IS NULL
      OR CHAR_LENGTH(bank_attach_idempotency_key) > 0
    ),
    CHECK (
      bank_apply_idempotency_key IS NULL
      OR CHAR_LENGTH(bank_apply_idempotency_key) > 0
    ),
    CHECK (
      status <> 'FULLY_APPLIED'
      OR (residual_amount_txn = 0 AND residual_amount_base = 0)
    )
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `,
];

const migration017CariSchemaFoundation = {
  key: "m017_cari_schema_foundation",
  description:
    "Add Cari AR/AP foundation schema: counterparties, payment terms, documents, open items, settlements, allocations, and unapplied cash",
  async up(connection) {
    for (const statement of statements) {
      await safeExecute(connection, statement);
    }
  },
};

export default migration017CariSchemaFoundation;
