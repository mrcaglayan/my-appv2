import { api } from "./client.js";

export async function listPaymentBatchExports(batchId) {
  const response = await api.get(`/api/v1/bank/payment-batches/${batchId}/exports`);
  return response.data;
}

export async function exportPaymentBatchFile(batchId, payload = {}) {
  const response = await api.post(`/api/v1/bank/payment-batches/${batchId}/export-file`, payload);
  return response.data;
}

export async function listPaymentBatchAckImports(batchId) {
  const response = await api.get(`/api/v1/bank/payment-batches/${batchId}/ack-imports`);
  return response.data;
}

export async function importPaymentBatchAck(batchId, payload = {}) {
  const response = await api.post(`/api/v1/bank/payment-batches/${batchId}/import-ack`, payload);
  return response.data;
}

