import { api } from "./client.js";

function toQueryString(params = {}) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") continue;
    searchParams.set(key, String(value));
  }
  const qs = searchParams.toString();
  return qs ? `?${qs}` : "";
}

export async function previewReconciliationAutoRun(payload = {}) {
  const response = await api.post("/api/v1/bank/reconciliation/auto/preview", payload);
  return response.data;
}

export async function applyReconciliationAutoRun(payload = {}) {
  const response = await api.post("/api/v1/bank/reconciliation/auto/apply", payload);
  return response.data;
}

export async function listReconciliationExceptions(params = {}) {
  const response = await api.get(`/api/v1/bank/reconciliation/exceptions${toQueryString(params)}`);
  return response.data;
}

export async function assignReconciliationException(exceptionId, payload = {}) {
  const response = await api.post(
    `/api/v1/bank/reconciliation/exceptions/${exceptionId}/assign`,
    payload
  );
  return response.data;
}

export async function resolveReconciliationException(exceptionId, payload = {}) {
  const response = await api.post(
    `/api/v1/bank/reconciliation/exceptions/${exceptionId}/resolve`,
    payload
  );
  return response.data;
}

export async function ignoreReconciliationExceptionItem(exceptionId, payload = {}) {
  const response = await api.post(
    `/api/v1/bank/reconciliation/exceptions/${exceptionId}/ignore`,
    payload
  );
  return response.data;
}

export async function retryReconciliationException(exceptionId, payload = {}) {
  const response = await api.post(
    `/api/v1/bank/reconciliation/exceptions/${exceptionId}/retry`,
    payload
  );
  return response.data;
}

export default {
  previewReconciliationAutoRun,
  applyReconciliationAutoRun,
  listReconciliationExceptions,
  assignReconciliationException,
  resolveReconciliationException,
  ignoreReconciliationExceptionItem,
  retryReconciliationException,
};
