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

export async function listRetentionPolicies(params = {}) {
  const response = await api.get(`/api/v1/ops/retention/policies${toQueryString(params)}`);
  return response.data;
}

export async function createRetentionPolicy(payload = {}) {
  const response = await api.post("/api/v1/ops/retention/policies", payload);
  return response.data;
}

export async function updateRetentionPolicy(policyId, payload = {}) {
  const response = await api.patch(`/api/v1/ops/retention/policies/${policyId}`, payload);
  return response.data;
}

export async function runRetentionPolicy(policyId, payload = {}, options = {}) {
  const asyncMode = Boolean(options.asyncMode ?? payload?.async);
  const query = asyncMode ? "?async=1" : "";
  const response = await api.post(`/api/v1/ops/retention/policies/${policyId}/run${query}`, payload);
  return response.data;
}

export async function listRetentionRuns(params = {}) {
  const response = await api.get(`/api/v1/ops/retention/runs${toQueryString(params)}`);
  return response.data;
}

export async function getRetentionRun(runId) {
  const response = await api.get(`/api/v1/ops/retention/runs/${runId}`);
  return response.data;
}

export async function listExportSnapshots(params = {}) {
  const response = await api.get(`/api/v1/ops/export-snapshots${toQueryString(params)}`);
  return response.data;
}

export async function getExportSnapshot(snapshotId) {
  const response = await api.get(`/api/v1/ops/export-snapshots/${snapshotId}`);
  return response.data;
}

export async function createExportSnapshot(payload = {}) {
  const response = await api.post("/api/v1/ops/export-snapshots", payload);
  return response.data;
}
