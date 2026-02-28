import { query } from "../db.js";

export async function findGroupCompanyByCode({ tenantId, code }) {
  const result = await query(
    `SELECT id
     FROM group_companies
     WHERE tenant_id = ?
       AND code = ?
     LIMIT 1`,
    [tenantId, code]
  );

  return result.rows[0] || null;
}

export async function upsertGroupCompanyRow({ tenantId, code, name }) {
  const result = await query(
    `INSERT INTO group_companies (tenant_id, code, name)
     VALUES (?, ?, ?)
     ON DUPLICATE KEY UPDATE
     name = VALUES(name)`,
    [tenantId, code, name]
  );

  return result.rows?.insertId || null;
}

export async function upsertLegalEntityRowTx(
  tx,
  {
    tenantId,
    groupCompanyId,
    code,
    name,
    taxId,
    countryId,
    functionalCurrencyCode,
    isIntercompanyEnabled,
    intercompanyPartnerRequired,
  }
) {
  const result = await tx.query(
    `INSERT INTO legal_entities (
        tenant_id,
        group_company_id,
        code,
        name,
        tax_id,
        country_id,
        functional_currency_code,
        is_intercompany_enabled,
        intercompany_partner_required
      )
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name),
       tax_id = VALUES(tax_id),
       country_id = VALUES(country_id),
       functional_currency_code = VALUES(functional_currency_code),
       group_company_id = VALUES(group_company_id),
       is_intercompany_enabled = VALUES(is_intercompany_enabled),
       intercompany_partner_required = VALUES(intercompany_partner_required)`,
    [
      tenantId,
      groupCompanyId,
      code,
      name,
      taxId,
      countryId,
      functionalCurrencyCode,
      isIntercompanyEnabled,
      intercompanyPartnerRequired,
    ]
  );

  return result.rows?.insertId || null;
}

export async function upsertOperatingUnitRow({
  tenantId,
  legalEntityId,
  code,
  name,
  unitType,
  hasSubledger,
}) {
  const result = await query(
    `INSERT INTO operating_units (
        tenant_id, legal_entity_id, code, name, unit_type, has_subledger
      )
     VALUES (?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name),
       unit_type = VALUES(unit_type),
       has_subledger = VALUES(has_subledger)`,
    [
      tenantId,
      legalEntityId,
      code,
      name,
      unitType,
      hasSubledger,
    ]
  );

  return result.rows?.insertId || null;
}

export async function upsertFiscalCalendarRow({
  tenantId,
  code,
  name,
  yearStartMonth,
  yearStartDay,
}) {
  const result = await query(
    `INSERT INTO fiscal_calendars (
        tenant_id, code, name, year_start_month, year_start_day
      )
     VALUES (?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       name = VALUES(name),
       year_start_month = VALUES(year_start_month),
       year_start_day = VALUES(year_start_day)`,
    [tenantId, code, name, yearStartMonth, yearStartDay]
  );

  return result.rows?.insertId || null;
}

export async function upsertFiscalPeriodRow({
  calendarId,
  fiscalYear,
  periodNo,
  periodName,
  startDate,
  endDate,
}) {
  await query(
    `INSERT INTO fiscal_periods (
        calendar_id, fiscal_year, period_no, period_name, start_date, end_date, is_adjustment
     )
     VALUES (?, ?, ?, ?, ?, ?, FALSE)
     ON DUPLICATE KEY UPDATE
       period_name = VALUES(period_name),
       start_date = VALUES(start_date),
       end_date = VALUES(end_date)`,
    [calendarId, fiscalYear, periodNo, periodName, startDate, endDate]
  );
}

export async function upsertJournalPurposeAccountTx(
  tx,
  {
    tenantId,
    legalEntityId,
    purposeCode,
    accountId,
  }
) {
  await tx.query(
    `INSERT INTO journal_purpose_accounts (
        tenant_id,
        legal_entity_id,
        purpose_code,
        account_id
     )
     VALUES (?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       account_id = VALUES(account_id),
       updated_at = CURRENT_TIMESTAMP`,
    [tenantId, legalEntityId, purposeCode, accountId]
  );
}

export async function findShareholderJournalConfigRowTx(
  tx,
  {
    tenantId,
    legalEntityId,
    capitalPurposeCode,
    commitmentPurposeCode,
  }
) {
  const configResult = await tx.query(
    `SELECT
       le.id AS legal_entity_id,
       le.code AS legal_entity_code,
       le.name AS legal_entity_name,
       cap.account_id AS capital_credit_parent_account_id,
       capa.code AS capital_credit_parent_account_code,
       capa.name AS capital_credit_parent_account_name,
       deb.account_id AS commitment_debit_parent_account_id,
       deba.code AS commitment_debit_parent_account_code,
       deba.name AS commitment_debit_parent_account_name
     FROM legal_entities le
     LEFT JOIN journal_purpose_accounts cap
       ON cap.tenant_id = le.tenant_id
      AND cap.legal_entity_id = le.id
      AND cap.purpose_code = ?
     LEFT JOIN accounts capa ON capa.id = cap.account_id
     LEFT JOIN journal_purpose_accounts deb
       ON deb.tenant_id = le.tenant_id
      AND deb.legal_entity_id = le.id
      AND deb.purpose_code = ?
     LEFT JOIN accounts deba ON deba.id = deb.account_id
     WHERE le.tenant_id = ?
       AND le.id = ?
     LIMIT 1`,
    [capitalPurposeCode, commitmentPurposeCode, tenantId, legalEntityId]
  );

  return configResult.rows[0] || null;
}
