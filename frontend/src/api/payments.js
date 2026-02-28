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

export async function listPaymentBatches(params = {}) {
  const response = await api.get(`/api/v1/payments/batches${toQueryString(params)}`);
  return response.data;
}

export async function getPaymentBatch(batchId) {
  const response = await api.get(`/api/v1/payments/batches/${batchId}`);
  return response.data;
}

export async function createPaymentBatch(payload) {
  const response = await api.post("/api/v1/payments/batches", payload);
  return response.data;
}

export async function approvePaymentBatch(batchId, payload = {}) {
  const response = await api.post(`/api/v1/payments/batches/${batchId}/approve`, payload);
  return response.data;
}

export async function exportPaymentBatch(batchId, payload = { format: "CSV" }) {
  const response = await api.post(`/api/v1/payments/batches/${batchId}/export`, payload);
  return response.data;
}

export async function postPaymentBatch(batchId, payload = {}) {
  const response = await api.post(`/api/v1/payments/batches/${batchId}/post`, payload);
  return response.data;
}

export async function cancelPaymentBatch(batchId, payload = {}) {
  const response = await api.post(`/api/v1/payments/batches/${batchId}/cancel`, payload);
  return response.data;
}

