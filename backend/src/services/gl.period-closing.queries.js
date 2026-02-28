import { query } from "../db.js";

export async function findPeriodCloseRuns({ conditions, params, runQuery = query }) {
  const result = await runQuery(
    `SELECT
       r.*,
       b.code AS book_code,
       b.name AS book_name,
       fp.fiscal_year,
       fp.period_no,
       fp.period_name
     FROM period_close_runs r
     JOIN books b ON b.id = r.book_id
     JOIN fiscal_periods fp ON fp.id = r.fiscal_period_id
     WHERE ${conditions.join(" AND ")}
     ORDER BY r.id DESC
     LIMIT 250`,
    params
  );

  return result.rows || [];
}

export async function findPeriodCloseRunLines({ runIds, runQuery = query }) {
  if (!Array.isArray(runIds) || runIds.length === 0) {
    return [];
  }

  const placeholders = runIds.map(() => "?").join(", ");
  const result = await runQuery(
    `SELECT
       l.period_close_run_id,
       l.line_type,
       l.account_id,
       l.closing_balance,
       l.debit_base,
       l.credit_base,
       a.code AS account_code,
       a.name AS account_name
     FROM period_close_run_lines l
     JOIN accounts a ON a.id = l.account_id
     WHERE l.period_close_run_id IN (${placeholders})
     ORDER BY l.period_close_run_id, l.line_type, a.code`,
    runIds
  );

  return result.rows || [];
}

export async function upsertPeriodStatus({
  bookId,
  fiscalPeriodId,
  status,
  userId,
  note,
  runQuery = query,
}) {
  await runQuery(
    `INSERT INTO period_statuses (
        book_id, fiscal_period_id, status, closed_by_user_id, closed_at, note
      )
     VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
     ON DUPLICATE KEY UPDATE
       status = VALUES(status),
       closed_by_user_id = VALUES(closed_by_user_id),
       closed_at = VALUES(closed_at),
       note = VALUES(note)`,
    [bookId, fiscalPeriodId, status, userId, note]
  );
}
