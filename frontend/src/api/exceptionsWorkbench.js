import { api } from "./client.js";

export async function listExceptionWorkbench(params = {}) {
  const response = await api.get("/api/v1/exceptions/workbench", { params });
  return response.data;
}

export async function refreshExceptionWorkbench(payload = {}) {
  const response = await api.post("/api/v1/exceptions/workbench/refresh", payload);
  return response.data;
}

export async function getExceptionWorkbenchById(exceptionId) {
  const response = await api.get(`/api/v1/exceptions/workbench/${exceptionId}`);
  return response.data;
}

export async function claimExceptionWorkbench(exceptionId, payload = {}) {
  const response = await api.post(`/api/v1/exceptions/workbench/${exceptionId}/claim`, payload);
  return response.data;
}

export async function resolveExceptionWorkbench(exceptionId, payload = {}) {
  const response = await api.post(`/api/v1/exceptions/workbench/${exceptionId}/resolve`, payload);
  return response.data;
}

export async function ignoreExceptionWorkbench(exceptionId, payload = {}) {
  const response = await api.post(`/api/v1/exceptions/workbench/${exceptionId}/ignore`, payload);
  return response.data;
}

export async function reopenExceptionWorkbench(exceptionId, payload = {}) {
  const response = await api.post(`/api/v1/exceptions/workbench/${exceptionId}/reopen`, payload);
  return response.data;
}

export async function bulkActionExceptionWorkbench(payload = {}) {
  const response = await api.post("/api/v1/exceptions/workbench/bulk-action", payload);
  return response.data;
}
