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

export async function listPayrollProviderAdapters() {
  const response = await api.get("/api/v1/payroll/providers");
  return response.data;
}

export async function listPayrollProviderConnections(params = {}) {
  const response = await api.get(`/api/v1/payroll/provider-connections${toQueryString(params)}`);
  return response.data;
}

export async function createPayrollProviderConnection(payload) {
  const response = await api.post("/api/v1/payroll/provider-connections", payload);
  return response.data;
}

export async function updatePayrollProviderConnection(connectionId, payload) {
  const response = await api.patch(`/api/v1/payroll/provider-connections/${connectionId}`, payload);
  return response.data;
}

export async function listPayrollEmployeeProviderRefs(params = {}) {
  const response = await api.get(`/api/v1/payroll/employee-provider-refs${toQueryString(params)}`);
  return response.data;
}

export async function upsertPayrollEmployeeProviderRef(payload) {
  const response = await api.post("/api/v1/payroll/employee-provider-refs", payload);
  return response.data;
}

export async function listPayrollProviderImports(params = {}) {
  const response = await api.get(`/api/v1/payroll/provider-imports${toQueryString(params)}`);
  return response.data;
}

export async function previewPayrollProviderImport(payload) {
  const response = await api.post("/api/v1/payroll/provider-imports/preview", payload);
  return response.data;
}

export async function getPayrollProviderImport(importJobId) {
  const response = await api.get(`/api/v1/payroll/provider-imports/${importJobId}`);
  return response.data;
}

export async function applyPayrollProviderImport(importJobId, payload = {}) {
  const response = await api.post(`/api/v1/payroll/provider-imports/${importJobId}/apply`, payload);
  return response.data;
}

