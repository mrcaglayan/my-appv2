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

export async function listBankAccounts(params = {}) {
  const response = await api.get(`/api/v1/bank/accounts${toQueryString(params)}`);
  return response.data;
}

export async function getBankAccount(bankAccountId) {
  const response = await api.get(`/api/v1/bank/accounts/${bankAccountId}`);
  return response.data;
}

export async function createBankAccount(payload) {
  const response = await api.post("/api/v1/bank/accounts", payload);
  return response.data;
}

export async function updateBankAccount(bankAccountId, payload) {
  const response = await api.put(`/api/v1/bank/accounts/${bankAccountId}`, payload);
  return response.data;
}

export async function activateBankAccount(bankAccountId) {
  const response = await api.post(`/api/v1/bank/accounts/${bankAccountId}/activate`);
  return response.data;
}

export async function deactivateBankAccount(bankAccountId) {
  const response = await api.post(`/api/v1/bank/accounts/${bankAccountId}/deactivate`);
  return response.data;
}
