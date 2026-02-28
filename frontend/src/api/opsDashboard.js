import { api } from "./client.js";

export async function getOpsBankReconciliationSummary(params = {}) {
  const response = await api.get("/api/v1/ops/bank/reconciliation-summary", { params });
  return response.data;
}

export async function getOpsBankPaymentBatchesHealth(params = {}) {
  const response = await api.get("/api/v1/ops/bank/payment-batches-health", { params });
  return response.data;
}

export async function getOpsPayrollImportHealth(params = {}) {
  const response = await api.get("/api/v1/ops/payroll/import-health", { params });
  return response.data;
}

export async function getOpsPayrollCloseStatus(params = {}) {
  const response = await api.get("/api/v1/ops/payroll/close-status", { params });
  return response.data;
}

export async function getOpsJobsHealth(params = {}) {
  const response = await api.get("/api/v1/ops/jobs/health", { params });
  return response.data;
}

