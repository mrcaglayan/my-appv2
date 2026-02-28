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

export async function listReconciliationQueue(params = {}) {
  const response = await api.get(`/api/v1/bank/reconciliation/queue${toQueryString(params)}`);
  return response.data;
}

export async function getReconciliationSuggestions(lineId) {
  const response = await api.get(`/api/v1/bank/reconciliation/queue/${lineId}/suggestions`);
  return response.data;
}

export async function matchReconciliationLine(lineId, payload) {
  const response = await api.post(`/api/v1/bank/reconciliation/queue/${lineId}/match`, payload);
  return response.data;
}

export async function unmatchReconciliationLine(lineId, payload = {}) {
  const response = await api.post(`/api/v1/bank/reconciliation/queue/${lineId}/unmatch`, payload);
  return response.data;
}

export async function ignoreReconciliationLine(lineId, payload = {}) {
  const response = await api.post(`/api/v1/bank/reconciliation/queue/${lineId}/ignore`, payload);
  return response.data;
}

export async function listReconciliationAudit(params = {}) {
  const response = await api.get(`/api/v1/bank/reconciliation/audit${toQueryString(params)}`);
  return response.data;
}
