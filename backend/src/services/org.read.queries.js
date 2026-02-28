import { query } from "../db.js";

export async function fetchGroupCompanyRows({ tenantId, scopeFilter, params }) {
  const result = await query(
    `SELECT id, tenant_id, code, name, created_at
     FROM group_companies
     WHERE tenant_id = ?
       AND ${scopeFilter}
     ORDER BY id`,
    [tenantId, ...params]
  );
  return result.rows || [];
}

export async function fetchCountryRows({ scopeFilter, params }) {
  const result = await query(
    `SELECT c.id, c.iso2, c.iso3, c.name, c.default_currency_code
     FROM countries c
     WHERE ${scopeFilter}
     ORDER BY c.name`,
    params
  );
  return result.rows || [];
}

export async function fetchCurrencyRows() {
  const result = await query(
    `SELECT code, name, minor_units
     FROM currencies
     ORDER BY code`
  );
  return result.rows || [];
}

export async function fetchLegalEntityRows({ conditions, params }) {
  const result = await query(
    `SELECT
       id,
       tenant_id,
       group_company_id,
       code,
       name,
       tax_id,
       country_id,
       functional_currency_code,
       status,
       is_intercompany_enabled,
       intercompany_partner_required,
       created_at,
       updated_at
     FROM legal_entities
     WHERE ${conditions.join(" AND ")}
     ORDER BY id`,
    params
  );
  return result.rows || [];
}

export async function fetchOperatingUnitRows({ conditions, params }) {
  const result = await query(
    `SELECT
       id,
       tenant_id,
       legal_entity_id,
       code,
       name,
       unit_type,
       has_subledger,
       status,
       created_at
     FROM operating_units
     WHERE ${conditions.join(" AND ")}
     ORDER BY id`,
    params
  );
  return result.rows || [];
}

export async function fetchFiscalCalendarRows({ tenantId }) {
  const result = await query(
    `SELECT id, code, name, year_start_month, year_start_day, created_at
     FROM fiscal_calendars
     WHERE tenant_id = ?
     ORDER BY id`,
    [tenantId]
  );
  return result.rows || [];
}

export async function fetchFiscalCalendarById({ tenantId, calendarId }) {
  const result = await query(
    `SELECT id, code, name
     FROM fiscal_calendars
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [calendarId, tenantId]
  );
  return result.rows[0] || null;
}

export async function fetchFiscalPeriodRows({ conditions, params }) {
  const result = await query(
    `SELECT id, calendar_id, fiscal_year, period_no, period_name, start_date, end_date, is_adjustment
     FROM fiscal_periods
     WHERE ${conditions.join(" AND ")}
     ORDER BY fiscal_year, period_no, is_adjustment`,
    params
  );
  return result.rows || [];
}

export async function fetchTreeGroupRows({ tenantId, scopeFilter, params }) {
  const result = await query(
    `SELECT id, code, name, created_at
     FROM group_companies
     WHERE tenant_id = ?
       AND ${scopeFilter}
     ORDER BY id`,
    [tenantId, ...params]
  );
  return result.rows || [];
}

export async function fetchTreeCountryRows({ scopeFilter, params }) {
  const result = await query(
    `SELECT c.id, c.iso2, c.iso3, c.name, c.default_currency_code
     FROM countries c
     WHERE ${scopeFilter}
     ORDER BY c.name`,
    params
  );
  return result.rows || [];
}

export async function fetchTreeLegalEntityRows({ tenantId, scopeFilter, params }) {
  const result = await query(
    `SELECT
       id,
       group_company_id,
       code,
       name,
       tax_id,
       country_id,
       functional_currency_code,
       status,
       is_intercompany_enabled,
       intercompany_partner_required
     FROM legal_entities
     WHERE tenant_id = ?
       AND ${scopeFilter}
     ORDER BY id`,
    [tenantId, ...params]
  );
  return result.rows || [];
}

export async function fetchTreeOperatingUnitRows({ tenantId, scopeFilter, params }) {
  const result = await query(
    `SELECT id, legal_entity_id, code, name, unit_type, has_subledger, status
     FROM operating_units
     WHERE tenant_id = ?
       AND ${scopeFilter}
     ORDER BY id`,
    [tenantId, ...params]
  );
  return result.rows || [];
}

export async function fetchShareholderJournalConfigRows({ conditions, params }) {
  const result = await query(
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
     WHERE ${conditions.join(" AND ")}
     ORDER BY le.code, le.id`,
    params
  );
  return result.rows || [];
}

export async function fetchShareholderRows({ conditions, params }) {
  const result = await query(
    `SELECT
       s.id,
       s.tenant_id,
       s.legal_entity_id,
       s.code,
       s.name,
       s.shareholder_type,
       s.tax_id,
       s.ownership_pct,
       s.committed_capital,
       CASE
         WHEN dc.id IS NULL THEN 0
         ELSE COALESCE(pc.paid_capital_calculated, 0)
       END AS paid_capital,
       CASE WHEN c.id IS NULL THEN NULL ELSE s.capital_sub_account_id END AS capital_sub_account_id,
       CASE
         WHEN dc.id IS NULL THEN NULL
         ELSE s.commitment_debit_sub_account_id
       END AS commitment_debit_sub_account_id,
       s.currency_code,
       s.status,
       s.notes,
       s.created_at,
       s.updated_at,
       CASE WHEN c.id IS NULL THEN NULL ELSE a.code END AS capital_sub_account_code,
       CASE WHEN c.id IS NULL THEN NULL ELSE a.name END AS capital_sub_account_name,
       CASE WHEN c.id IS NULL THEN NULL ELSE a.account_type END AS capital_sub_account_type,
       CASE WHEN dc.id IS NULL THEN NULL ELSE da.code END AS commitment_debit_sub_account_code,
       CASE WHEN dc.id IS NULL THEN NULL ELSE da.name END AS commitment_debit_sub_account_name,
       CASE WHEN dc.id IS NULL THEN NULL ELSE da.account_type END AS commitment_debit_sub_account_type
     FROM shareholders s
     LEFT JOIN accounts a ON a.id = s.capital_sub_account_id
     LEFT JOIN charts_of_accounts c
       ON c.id = a.coa_id
      AND c.tenant_id = s.tenant_id
     LEFT JOIN accounts da ON da.id = s.commitment_debit_sub_account_id
     LEFT JOIN charts_of_accounts dc
       ON dc.id = da.coa_id
      AND dc.tenant_id = s.tenant_id
     LEFT JOIN (
       SELECT
         je.tenant_id,
         je.legal_entity_id,
         jl.account_id,
         SUM(jl.credit_base) AS paid_capital_calculated
       FROM journal_entries je
       JOIN journal_lines jl ON jl.journal_entry_id = je.id
       WHERE je.status = 'POSTED'
       GROUP BY je.tenant_id, je.legal_entity_id, jl.account_id
     ) pc
       ON pc.tenant_id = s.tenant_id
      AND pc.legal_entity_id = s.legal_entity_id
      AND pc.account_id = s.commitment_debit_sub_account_id
     WHERE ${conditions.join(" AND ")}
     ORDER BY s.legal_entity_id, s.code`,
    params
  );
  return result.rows || [];
}
