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

export async function listPolicyPacks(params = {}) {
  const response = await api.get(
    `/api/v1/onboarding/policy-packs${toQueryString(params)}`
  );
  return response.data;
}

export async function getPolicyPack(packId) {
  const response = await api.get(`/api/v1/onboarding/policy-packs/${packId}`);
  return response.data;
}

export async function resolvePolicyPack(packId, payload) {
  const response = await api.post(
    `/api/v1/onboarding/policy-packs/${packId}/resolve`,
    payload
  );
  return response.data;
}

export async function applyPolicyPack(packId, payload) {
  const response = await api.post(
    `/api/v1/onboarding/policy-packs/${packId}/apply`,
    payload
  );
  return response.data;
}
