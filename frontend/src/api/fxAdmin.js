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

export async function listFxRates(params = {}) {
  const response = await api.get(`/api/v1/fx/rates${toQueryString(params)}`);
  return response.data;
}

export async function bulkUpsertFxRates(rates) {
  const response = await api.post("/api/v1/fx/rates/bulk-upsert", { rates });
  return response.data;
}
