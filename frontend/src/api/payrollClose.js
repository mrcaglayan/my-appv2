import { api } from "./client.js";

function toQueryString(params = {}) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") continue;
    searchParams.set(key, String(value));
  }
  const query = searchParams.toString();
  return query ? `?${query}` : "";
}

export async function listPayrollCloseControls(params = {}) {
  const response = await api.get(`/api/v1/payroll/close-controls${toQueryString(params)}`);
  return response.data;
}

export async function getPayrollCloseControl(closeId) {
  const response = await api.get(`/api/v1/payroll/close-controls/${closeId}`);
  return response.data;
}

export async function preparePayrollCloseControl(payload = {}) {
  const response = await api.post("/api/v1/payroll/close-controls/prepare", payload);
  return response.data;
}

export async function requestPayrollCloseControl(closeId, payload = {}) {
  const response = await api.post(`/api/v1/payroll/close-controls/${closeId}/request`, payload);
  return response.data;
}

export async function approveClosePayrollCloseControl(closeId, payload = {}) {
  const response = await api.post(
    `/api/v1/payroll/close-controls/${closeId}/approve-close`,
    payload
  );
  return response.data;
}

export async function reopenPayrollCloseControl(closeId, payload = {}) {
  const response = await api.post(`/api/v1/payroll/close-controls/${closeId}/reopen`, payload);
  return response.data;
}
