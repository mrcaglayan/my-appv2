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

export async function listPayrollLiabilities(params = {}) {
  const response = await api.get(`/api/v1/payroll/liabilities${toQueryString(params)}`);
  return response.data;
}

export async function buildPayrollRunLiabilities(runId, payload = {}) {
  const response = await api.post(`/api/v1/payroll/runs/${runId}/liabilities/build`, payload);
  return response.data;
}

export async function getPayrollRunLiabilities(runId) {
  const response = await api.get(`/api/v1/payroll/runs/${runId}/liabilities`);
  return response.data;
}

export async function getPayrollRunPaymentBatchPreview(runId, params = {}) {
  const response = await api.get(
    `/api/v1/payroll/runs/${runId}/payment-batch-preview${toQueryString(params)}`
  );
  return response.data;
}

export async function createPayrollRunPaymentBatch(runId, payload) {
  const response = await api.post(`/api/v1/payroll/runs/${runId}/payment-batches`, payload);
  return response.data;
}

