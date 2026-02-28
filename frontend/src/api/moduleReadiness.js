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

export async function getModuleReadiness(params = {}) {
  const response = await api.get(
    `/api/v1/onboarding/module-readiness${toQueryString(params)}`
  );
  return response.data;
}
