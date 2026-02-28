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

export async function importPayrollRunCsv(payload) {
  const response = await api.post("/api/v1/payroll/runs/import", payload);
  return response.data;
}

export async function listPayrollRuns(params = {}) {
  const response = await api.get(`/api/v1/payroll/runs${toQueryString(params)}`);
  return response.data;
}

export async function getPayrollRun(runId) {
  const response = await api.get(`/api/v1/payroll/runs/${runId}`);
  return response.data;
}

export async function listPayrollRunLines(runId, params = {}) {
  const response = await api.get(
    `/api/v1/payroll/runs/${runId}/lines${toQueryString(params)}`
  );
  return response.data;
}

export async function getPayrollRunAccrualPreview(runId) {
  const response = await api.get(`/api/v1/payroll/runs/${runId}/accrual-preview`);
  return response.data;
}

export async function reviewPayrollRun(runId, payload = {}) {
  const response = await api.post(`/api/v1/payroll/runs/${runId}/review`, payload);
  return response.data;
}

export async function finalizePayrollRun(runId, payload = {}) {
  const response = await api.post(`/api/v1/payroll/runs/${runId}/finalize`, payload);
  return response.data;
}
