import { query } from "../db.js";

export const WORKING_CONTEXT_PREFERENCE_KEY = "WORKING_CONTEXT";

function toPositiveIntString(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return "";
  }
  return String(parsed);
}

function normalizeIsoDate(value) {
  const text = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : "";
}

function normalizeWorkingContext(input = {}) {
  return {
    legalEntityId: toPositiveIntString(input.legalEntityId),
    operatingUnitId: toPositiveIntString(input.operatingUnitId),
    fiscalCalendarId: toPositiveIntString(input.fiscalCalendarId),
    fiscalPeriodId: toPositiveIntString(input.fiscalPeriodId),
    dateFrom: normalizeIsoDate(input.dateFrom),
    dateTo: normalizeIsoDate(input.dateTo),
  };
}

function parseJsonValue(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  if (typeof value === "object") {
    return value;
  }
  try {
    return JSON.parse(String(value));
  } catch {
    return null;
  }
}

function isMissingTableError(err) {
  return Number(err?.errno) === 1146;
}

export async function getUserPreferences({ tenantId, userId }) {
  try {
    const result = await query(
      `SELECT preference_key, preference_value_json
       FROM user_preferences
       WHERE tenant_id = ?
         AND user_id = ?`,
      [tenantId, userId]
    );

    const preferences = {};
    for (const row of result.rows || []) {
      const key = String(row?.preference_key || "").trim().toUpperCase();
      const payload = parseJsonValue(row?.preference_value_json);
      if (key === WORKING_CONTEXT_PREFERENCE_KEY) {
        preferences.workingContext = payload
          ? normalizeWorkingContext(payload)
          : null;
      }
    }
    return preferences;
  } catch (err) {
    if (isMissingTableError(err)) {
      return {};
    }
    throw err;
  }
}

export async function saveUserPreferences({ tenantId, userId, preferencesPatch }) {
  const patch = preferencesPatch || {};
  const hasWorkingContextPatch = Object.prototype.hasOwnProperty.call(
    patch,
    "workingContext"
  );

  if (!hasWorkingContextPatch) {
    return getUserPreferences({ tenantId, userId });
  }

  const workingContextInput = patch.workingContext;
  if (workingContextInput === null) {
    try {
      await query(
        `DELETE FROM user_preferences
         WHERE tenant_id = ?
           AND user_id = ?
           AND preference_key = ?`,
        [tenantId, userId, WORKING_CONTEXT_PREFERENCE_KEY]
      );
    } catch (err) {
      if (!isMissingTableError(err)) {
        throw err;
      }
    }
    return getUserPreferences({ tenantId, userId });
  }

  const normalizedContext = normalizeWorkingContext(workingContextInput);
  try {
    await query(
      `INSERT INTO user_preferences (
         tenant_id,
         user_id,
         preference_key,
         preference_value_json
       ) VALUES (?, ?, ?, CAST(? AS JSON))
       ON DUPLICATE KEY UPDATE
         preference_value_json = VALUES(preference_value_json),
         updated_at = CURRENT_TIMESTAMP`,
      [
        tenantId,
        userId,
        WORKING_CONTEXT_PREFERENCE_KEY,
        JSON.stringify(normalizedContext),
      ]
    );
  } catch (err) {
    if (!isMissingTableError(err)) {
      throw err;
    }
  }

  return getUserPreferences({ tenantId, userId });
}

