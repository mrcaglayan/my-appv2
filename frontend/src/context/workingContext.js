import { createContext } from "react";

export const WORKING_CONTEXT_STORAGE_KEY = "working_context.v1";

export const DEFAULT_WORKING_CONTEXT = Object.freeze({
  legalEntityId: "",
  operatingUnitId: "",
  fiscalCalendarId: "",
  fiscalPeriodId: "",
  dateFrom: "",
  dateTo: "",
});

export const WorkingContextContext = createContext(null);

function toPositiveIntString(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return "";
  }
  return String(parsed);
}

export function normalizeIsoDate(value) {
  const text = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : "";
}

export function normalizeWorkingContext(input = {}) {
  return {
    legalEntityId: toPositiveIntString(input.legalEntityId),
    operatingUnitId: toPositiveIntString(input.operatingUnitId),
    fiscalCalendarId: toPositiveIntString(input.fiscalCalendarId),
    fiscalPeriodId: toPositiveIntString(input.fiscalPeriodId),
    dateFrom: normalizeIsoDate(input.dateFrom),
    dateTo: normalizeIsoDate(input.dateTo),
  };
}

