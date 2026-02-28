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

export async function listBankConnectors(params = {}) {
  const response = await api.get(`/api/v1/bank/connectors${toQueryString(params)}`);
  return response.data;
}

export async function getBankConnector(connectorId) {
  const response = await api.get(`/api/v1/bank/connectors/${connectorId}`);
  return response.data;
}

export async function createBankConnector(payload) {
  const response = await api.post("/api/v1/bank/connectors", payload);
  return response.data;
}

export async function updateBankConnector(connectorId, payload) {
  const response = await api.patch(`/api/v1/bank/connectors/${connectorId}`, payload);
  return response.data;
}

export async function upsertBankConnectorAccountLink(connectorId, payload) {
  const response = await api.put(`/api/v1/bank/connectors/${connectorId}/account-links`, payload);
  return response.data;
}

export async function listBankConnectorSyncRuns(connectorId, params = {}) {
  const response = await api.get(
    `/api/v1/bank/connectors/${connectorId}/sync-runs${toQueryString(params)}`
  );
  return response.data;
}

export async function testBankConnectorConnection(connectorId) {
  const response = await api.post(`/api/v1/bank/connectors/${connectorId}/test`);
  return response.data;
}

export async function syncBankConnectorStatements(connectorId, payload = {}) {
  const response = await api.post(`/api/v1/bank/connectors/${connectorId}/sync-statements`, payload);
  return response.data;
}
