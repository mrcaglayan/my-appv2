import { api } from "./client.js";

export async function bootstrapCompany(payload) {
  const response = await api.post("/api/v1/onboarding/company-bootstrap", payload);
  return response.data;
}
