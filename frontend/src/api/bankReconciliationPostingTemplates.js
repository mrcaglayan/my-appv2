import { api } from "./client.js";

function toQueryString(params = {}) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") continue;
    searchParams.set(key, String(value));
  }
  const qs = searchParams.toString();
  return qs ? `?${qs}` : "";
}

export async function listReconciliationPostingTemplates(params = {}) {
  const response = await api.get(
    `/api/v1/bank/reconciliation/posting-templates${toQueryString(params)}`
  );
  return response.data;
}

export async function getReconciliationPostingTemplate(templateId) {
  const response = await api.get(`/api/v1/bank/reconciliation/posting-templates/${templateId}`);
  return response.data;
}

export async function createReconciliationPostingTemplate(payload = {}) {
  const response = await api.post("/api/v1/bank/reconciliation/posting-templates", payload);
  return response.data;
}

export async function updateReconciliationPostingTemplate(templateId, payload = {}) {
  const response = await api.patch(
    `/api/v1/bank/reconciliation/posting-templates/${templateId}`,
    payload
  );
  return response.data;
}

export default {
  listReconciliationPostingTemplates,
  getReconciliationPostingTemplate,
  createReconciliationPostingTemplate,
  updateReconciliationPostingTemplate,
};
