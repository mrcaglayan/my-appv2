import { api } from "./client.js";

export async function listBankPaymentReturns(params = {}) {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params || {})) {
    if (value === undefined || value === null || value === "") continue;
    search.set(key, String(value));
  }
  const qs = search.toString();
  const response = await api.get(`/api/v1/bank/payment-returns${qs ? `?${qs}` : ""}`);
  return response.data;
}

export async function createBankPaymentReturn(payload) {
  const response = await api.post("/api/v1/bank/payment-returns", payload);
  return response.data;
}

export async function ignoreBankPaymentReturn(eventId, payload = {}) {
  const response = await api.post(`/api/v1/bank/payment-returns/${eventId}/ignore`, payload);
  return response.data;
}
