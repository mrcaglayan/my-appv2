import { api } from "./client.js";

export async function listBankApprovalPolicies(params = {}) {
  const response = await api.get("/api/v1/bank/approvals/policies", { params });
  return response.data;
}

export async function getBankApprovalPolicy(policyId) {
  const response = await api.get(`/api/v1/bank/approvals/policies/${policyId}`);
  return response.data;
}

export async function createBankApprovalPolicy(payload) {
  const response = await api.post("/api/v1/bank/approvals/policies", payload);
  return response.data;
}

export async function updateBankApprovalPolicy(policyId, payload) {
  const response = await api.patch(`/api/v1/bank/approvals/policies/${policyId}`, payload);
  return response.data;
}
