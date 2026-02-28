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

export async function listPayrollMappings(params = {}) {
  const response = await api.get(`/api/v1/payroll/mappings${toQueryString(params)}`);
  return response.data;
}

export async function upsertPayrollMapping(payload) {
  const response = await api.post("/api/v1/payroll/mappings/upsert", payload);
  return response.data;
}

