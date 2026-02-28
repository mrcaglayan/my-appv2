import { api } from "./client.js";

export async function bootstrapTenant(payload, providerKey) {
  const response = await api.post("/api/v1/provider/tenants/bootstrap", payload, {
    headers: {
      "X-Provider-Key": providerKey,
    },
  });
  return response.data;
}
