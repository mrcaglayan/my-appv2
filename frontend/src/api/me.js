import { api } from "./client.js";

export async function getMePreferences() {
  const response = await api.get("/me/preferences");
  return response.data;
}

export async function updateMePreferences(payload = {}) {
  const response = await api.put("/me/preferences", payload);
  return response.data;
}

