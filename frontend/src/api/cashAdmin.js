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

export async function listCashRegisters(params = {}) {
  const response = await api.get(`/api/v1/cash/registers${toQueryString(params)}`);
  return response.data;
}

export async function getCashRegister(registerId, params = {}) {
  const response = await api.get(
    `/api/v1/cash/registers/${registerId}${toQueryString(params)}`
  );
  return response.data;
}

export async function upsertCashRegister(payload) {
  const response = await api.post("/api/v1/cash/registers", payload);
  return response.data;
}

export async function setCashRegisterStatus(registerId, payload) {
  const response = await api.post(`/api/v1/cash/registers/${registerId}/status`, payload);
  return response.data;
}

export async function listCashSessions(params = {}) {
  const response = await api.get(`/api/v1/cash/sessions${toQueryString(params)}`);
  return response.data;
}

export async function getCashSession(sessionId, params = {}) {
  const response = await api.get(
    `/api/v1/cash/sessions/${sessionId}${toQueryString(params)}`
  );
  return response.data;
}

export async function openCashSession(payload) {
  const response = await api.post("/api/v1/cash/sessions/open", payload);
  return response.data;
}

export async function closeCashSession(sessionId, payload) {
  const response = await api.post(`/api/v1/cash/sessions/${sessionId}/close`, payload);
  return response.data;
}

export async function listCashTransactions(params = {}) {
  const response = await api.get(`/api/v1/cash/transactions${toQueryString(params)}`);
  return response.data;
}

export async function getCashTransaction(transactionId, params = {}) {
  const response = await api.get(
    `/api/v1/cash/transactions/${transactionId}${toQueryString(params)}`
  );
  return response.data;
}

export async function createCashTransaction(payload) {
  const response = await api.post("/api/v1/cash/transactions", payload);
  return response.data;
}

export async function postCashTransaction(transactionId, payload) {
  const response = await api.post(
    `/api/v1/cash/transactions/${transactionId}/post`,
    payload
  );
  return response.data;
}

export async function cancelCashTransaction(transactionId, payload) {
  const response = await api.post(
    `/api/v1/cash/transactions/${transactionId}/cancel`,
    payload
  );
  return response.data;
}

export async function reverseCashTransaction(transactionId, payload) {
  const response = await api.post(
    `/api/v1/cash/transactions/${transactionId}/reverse`,
    payload
  );
  return response.data;
}

export async function applyCariForCashTransaction(transactionId, payload) {
  const response = await api.post(
    `/api/v1/cash/transactions/${transactionId}/apply-cari`,
    payload
  );
  return response.data;
}

export async function getCashTransitTransfer(transitTransferId, params = {}) {
  const response = await api.get(
    `/api/v1/cash/transactions/transit/${transitTransferId}${toQueryString(params)}`
  );
  return response.data;
}

export async function listCashTransitTransfers(params = {}) {
  const response = await api.get(`/api/v1/cash/transactions/transit${toQueryString(params)}`);
  return response.data;
}

export async function initiateCashTransitTransfer(payload) {
  const response = await api.post("/api/v1/cash/transactions/transit/initiate", payload);
  return response.data;
}

export async function receiveCashTransitTransfer(transitTransferId, payload) {
  const response = await api.post(
    `/api/v1/cash/transactions/transit/${transitTransferId}/receive`,
    payload
  );
  return response.data;
}

export async function cancelCashTransitTransfer(transitTransferId, payload) {
  const response = await api.post(
    `/api/v1/cash/transactions/transit/${transitTransferId}/cancel`,
    payload
  );
  return response.data;
}

export async function getCashConfig() {
  const response = await api.get("/api/v1/cash/config");
  return response.data;
}

export async function listCashExceptions(params = {}) {
  const response = await api.get(`/api/v1/cash/exceptions${toQueryString(params)}`);
  return response.data;
}
