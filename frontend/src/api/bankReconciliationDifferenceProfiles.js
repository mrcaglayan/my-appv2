import { api } from "./client.js";

export async function listReconciliationDifferenceProfiles(params = {}) {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params || {})) {
    if (value === undefined || value === null || value === "") continue;
    search.set(key, String(value));
  }
  const qs = search.toString();
  const response = await api.get(
    `/api/v1/bank/reconciliation/difference-profiles${qs ? `?${qs}` : ""}`
  );
  return response.data;
}

export async function createReconciliationDifferenceProfile(payload) {
  const response = await api.post("/api/v1/bank/reconciliation/difference-profiles", payload);
  return response.data;
}

export async function updateReconciliationDifferenceProfile(profileId, payload) {
  const response = await api.patch(
    `/api/v1/bank/reconciliation/difference-profiles/${profileId}`,
    payload
  );
  return response.data;
}
