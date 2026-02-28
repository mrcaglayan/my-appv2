import { api } from "./client.js";

function toQueryString(params = {}) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    searchParams.set(key, String(value));
  }
  const query = searchParams.toString();
  return query ? `?${query}` : "";
}

export async function listBooks(params = {}) {
  const response = await api.get(`/api/v1/gl/books${toQueryString(params)}`);
  return response.data;
}

export async function upsertBook(payload) {
  const response = await api.post("/api/v1/gl/books", payload);
  return response.data;
}

export async function listCoas(params = {}) {
  const response = await api.get(`/api/v1/gl/coas${toQueryString(params)}`);
  return response.data;
}

export async function upsertCoa(payload) {
  const response = await api.post("/api/v1/gl/coas", payload);
  return response.data;
}

export async function listAccounts(params = {}) {
  const response = await api.get(`/api/v1/gl/accounts${toQueryString(params)}`);
  return response.data;
}

export async function upsertAccount(payload) {
  const response = await api.post("/api/v1/gl/accounts", payload);
  return response.data;
}

export async function upsertAccountMapping(payload) {
  const response = await api.post("/api/v1/gl/account-mappings", payload);
  return response.data;
}

export async function createJournal(payload) {
  const response = await api.post("/api/v1/gl/journals", payload);
  return response.data;
}

export async function listJournals(params = {}) {
  const response = await api.get(`/api/v1/gl/journals${toQueryString(params)}`);
  return response.data;
}

export async function getJournal(journalId) {
  const response = await api.get(`/api/v1/gl/journals/${journalId}`);
  return response.data;
}

export async function postJournal(journalId, payload = {}) {
  const response = await api.post(`/api/v1/gl/journals/${journalId}/post`, payload);
  return response.data;
}

export async function reverseJournal(journalId, payload) {
  const response = await api.post(
    `/api/v1/gl/journals/${journalId}/reverse`,
    payload
  );
  return response.data;
}

export async function getTrialBalance(params = {}) {
  const response = await api.get(
    `/api/v1/gl/trial-balance${toQueryString(params)}`
  );
  return response.data;
}

export async function createBalanceSplitReclassification(payload) {
  const response = await api.post(
    "/api/v1/gl/reclassifications/balance-split",
    payload
  );
  return response.data;
}

export async function listReclassificationSourceLines(params = {}) {
  const response = await api.get(
    `/api/v1/gl/reclassifications/source-lines${toQueryString(params)}`
  );
  return response.data;
}

export async function createTransactionLineReclassification(payload) {
  const response = await api.post(
    "/api/v1/gl/reclassifications/transaction-lines",
    payload
  );
  return response.data;
}

export async function listReclassificationRuns(params = {}) {
  const response = await api.get(
    `/api/v1/gl/reclassifications/runs${toQueryString(params)}`
  );
  return response.data;
}

export async function closePeriod(bookId, periodId, payload) {
  const response = await api.post(
    `/api/v1/gl/period-statuses/${bookId}/${periodId}/close`,
    payload
  );
  return response.data;
}

export async function runPeriodClose(bookId, periodId, payload = {}) {
  const response = await api.post(
    `/api/v1/gl/period-closing/${bookId}/${periodId}/close-run`,
    payload
  );
  return response.data;
}

export async function reopenPeriodClose(bookId, periodId, payload = {}) {
  const response = await api.post(
    `/api/v1/gl/period-closing/${bookId}/${periodId}/reopen`,
    payload
  );
  return response.data;
}

export async function listPeriodCloseRuns(params = {}) {
  const response = await api.get(
    `/api/v1/gl/period-closing/runs${toQueryString(params)}`
  );
  return response.data;
}

export async function runIntercompanyReconciliation(payload = {}) {
  const response = await api.post("/api/v1/intercompany/reconcile", payload);
  return response.data;
}

export async function listIntercompanyEntityFlags(params = {}) {
  const response = await api.get(
    `/api/v1/intercompany/entity-flags${toQueryString(params)}`
  );
  return response.data;
}

export async function updateIntercompanyEntityFlags(legalEntityId, payload) {
  const response = await api.patch(
    `/api/v1/intercompany/entity-flags/${legalEntityId}`,
    payload
  );
  return response.data;
}

export async function upsertIntercompanyPair(payload) {
  const response = await api.post("/api/v1/intercompany/pairs", payload);
  return response.data;
}

export async function listIntercompanyComplianceIssues(params = {}) {
  const response = await api.get(
    `/api/v1/intercompany/compliance-issues${toQueryString(params)}`
  );
  return response.data;
}

export async function listConsolidationRuns(params = {}) {
  const response = await api.get(
    `/api/v1/consolidation/runs${toQueryString(params)}`
  );
  return response.data;
}

export async function getConsolidatedBalanceSheet(runId, params = {}) {
  const response = await api.get(
    `/api/v1/consolidation/runs/${runId}/reports/balance-sheet${toQueryString(params)}`
  );
  return response.data;
}

export async function getConsolidatedIncomeStatement(runId, params = {}) {
  const response = await api.get(
    `/api/v1/consolidation/runs/${runId}/reports/income-statement${toQueryString(params)}`
  );
  return response.data;
}

export async function listConsolidationEliminations(runId, params = {}) {
  const response = await api.get(
    `/api/v1/consolidation/runs/${runId}/eliminations${toQueryString(params)}`
  );
  return response.data;
}

export async function postConsolidationElimination(runId, eliminationEntryId) {
  const response = await api.post(
    `/api/v1/consolidation/runs/${runId}/eliminations/${eliminationEntryId}/post`
  );
  return response.data;
}

export async function listConsolidationAdjustments(runId, params = {}) {
  const response = await api.get(
    `/api/v1/consolidation/runs/${runId}/adjustments${toQueryString(params)}`
  );
  return response.data;
}

export async function postConsolidationAdjustment(runId, adjustmentId) {
  const response = await api.post(
    `/api/v1/consolidation/runs/${runId}/adjustments/${adjustmentId}/post`
  );
  return response.data;
}
