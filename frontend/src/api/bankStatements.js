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

export async function importBankStatementCsv(payload) {
  const response = await api.post("/api/v1/bank/statements/import", payload);
  return response.data;
}

export async function listBankStatementImports(params = {}) {
  const response = await api.get(`/api/v1/bank/statements/imports${toQueryString(params)}`);
  return response.data;
}

export async function getBankStatementImport(importId) {
  const response = await api.get(`/api/v1/bank/statements/imports/${importId}`);
  return response.data;
}

export async function listBankStatementLines(params = {}) {
  const response = await api.get(`/api/v1/bank/statements/lines${toQueryString(params)}`);
  return response.data;
}

export async function getBankStatementLine(lineId) {
  const response = await api.get(`/api/v1/bank/statements/lines/${lineId}`);
  return response.data;
}
