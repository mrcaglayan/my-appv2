import { api } from "./client.js";

export async function listPayrollRunCorrections(runId) {
  const response = await api.get(`/api/v1/payroll/runs/${runId}/corrections`);
  return response.data;
}

export async function reversePayrollRunWithCorrection(runId, payload = {}) {
  const response = await api.post(`/api/v1/payroll/runs/${runId}/reverse`, payload);
  return response.data;
}

export async function createPayrollCorrectionShell(payload = {}) {
  const response = await api.post("/api/v1/payroll/corrections/shell", payload);
  return response.data;
}
