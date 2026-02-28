import { api } from "./client.js";

export async function listPayrollManualSettlementRequests(liabilityId) {
  const response = await api.get(
    `/api/v1/payroll/liabilities/${liabilityId}/manual-settlement-requests`
  );
  return response.data;
}

export async function createPayrollManualSettlementRequest(liabilityId, payload = {}) {
  const response = await api.post(
    `/api/v1/payroll/liabilities/${liabilityId}/manual-settlement-requests`,
    payload
  );
  return response.data;
}

export async function approveApplyPayrollManualSettlementRequest(requestId, payload = {}) {
  const response = await api.post(
    `/api/v1/payroll/manual-settlement-requests/${requestId}/approve-apply`,
    payload
  );
  return response.data;
}

export async function rejectPayrollManualSettlementRequest(requestId, payload = {}) {
  const response = await api.post(
    `/api/v1/payroll/manual-settlement-requests/${requestId}/reject`,
    payload
  );
  return response.data;
}
