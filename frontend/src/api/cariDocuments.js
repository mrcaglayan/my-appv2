import { api } from "./client.js";
import { parseCariApiError, toCariQueryString } from "./cariCommon.js";

async function run(requestFn) {
  try {
    const response = await requestFn();
    return response.data;
  } catch (error) {
    throw parseCariApiError(error);
  }
}

export async function listCariDocuments(params = {}) {
  return run(() => api.get(`/api/v1/cari/documents${toCariQueryString(params)}`));
}

export async function getCariDocument(documentId) {
  return run(() => api.get(`/api/v1/cari/documents/${documentId}`));
}

export async function createCariDocument(payload) {
  return run(() => api.post("/api/v1/cari/documents", payload));
}

export async function updateCariDocument(documentId, payload) {
  return run(() => api.put(`/api/v1/cari/documents/${documentId}`, payload));
}

export async function cancelCariDocument(documentId) {
  return run(() => api.post(`/api/v1/cari/documents/${documentId}/cancel`, {}));
}

export async function postCariDocument(documentId, payload = {}) {
  return run(() => api.post(`/api/v1/cari/documents/${documentId}/post`, payload));
}

export async function reverseCariDocument(documentId, payload = {}) {
  return run(() => api.post(`/api/v1/cari/documents/${documentId}/reverse`, payload));
}
