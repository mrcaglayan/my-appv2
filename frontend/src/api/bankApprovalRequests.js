import { api } from "./client.js";

export async function listBankApprovalRequests(params = {}) {
  const response = await api.get("/api/v1/bank/approvals/requests", { params });
  return response.data;
}

export async function getBankApprovalRequest(requestId) {
  const response = await api.get(`/api/v1/bank/approvals/requests/${requestId}`);
  return response.data;
}

export async function submitBankApprovalRequest(payload) {
  const response = await api.post("/api/v1/bank/approvals/requests", payload);
  return response.data;
}

export async function approveBankApprovalRequest(requestId, payload = {}) {
  const response = await api.post(`/api/v1/bank/approvals/requests/${requestId}/approve`, payload);
  return response.data;
}

export async function rejectBankApprovalRequest(requestId, payload = {}) {
  const response = await api.post(`/api/v1/bank/approvals/requests/${requestId}/reject`, payload);
  return response.data;
}
