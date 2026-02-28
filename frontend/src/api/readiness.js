import { api } from "./client.js";

export async function getTenantReadiness() {
  const response = await api.get("/api/v1/onboarding/readiness");
  return response.data;
}

export async function bootstrapTenantBaseline(payload = {}) {
  const response = await api.post(
    "/api/v1/onboarding/readiness/bootstrap-baseline",
    payload
  );
  return response.data;
}
