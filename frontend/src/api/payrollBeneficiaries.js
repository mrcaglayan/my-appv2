import { api } from "./client.js";

function toQueryString(params = {}) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") continue;
    searchParams.set(key, String(value));
  }
  const query = searchParams.toString();
  return query ? `?${query}` : "";
}

export async function listPayrollBeneficiaryAccounts(params = {}) {
  const response = await api.get(`/api/v1/payroll/beneficiaries/accounts${toQueryString(params)}`);
  return response.data;
}

export async function createPayrollBeneficiaryAccount(payload = {}) {
  const response = await api.post("/api/v1/payroll/beneficiaries/accounts", payload);
  return response.data;
}

export async function updatePayrollBeneficiaryAccount(accountId, payload = {}) {
  const response = await api.patch(`/api/v1/payroll/beneficiaries/accounts/${accountId}`, payload);
  return response.data;
}

export async function setPrimaryPayrollBeneficiaryAccount(accountId, payload = {}) {
  const response = await api.post(
    `/api/v1/payroll/beneficiaries/accounts/${accountId}/set-primary`,
    payload
  );
  return response.data;
}

export async function getPayrollLiabilityBeneficiarySnapshot(liabilityId) {
  const response = await api.get(`/api/v1/payroll/liabilities/${liabilityId}/beneficiary-bank-snapshot`);
  return response.data;
}
