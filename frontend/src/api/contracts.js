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

export async function listContracts(params = {}) {
  return run(() => api.get(`/api/v1/contracts${toCariQueryString(params)}`));
}

export async function getContract(contractId) {
  return run(() => api.get(`/api/v1/contracts/${contractId}`));
}

export async function createContract(payload) {
  return run(() => api.post("/api/v1/contracts", payload));
}

export async function updateContract(contractId, payload) {
  return run(() => api.put(`/api/v1/contracts/${contractId}`, payload));
}

export async function amendContract(contractId, payload) {
  return run(() => api.post(`/api/v1/contracts/${contractId}/amend`, payload));
}

export async function patchContractLine(contractId, lineId, payload) {
  return run(() => api.patch(`/api/v1/contracts/${contractId}/lines/${lineId}`, payload));
}

export async function activateContract(contractId) {
  return run(() => api.post(`/api/v1/contracts/${contractId}/activate`, {}));
}

export async function suspendContract(contractId) {
  return run(() => api.post(`/api/v1/contracts/${contractId}/suspend`, {}));
}

export async function closeContract(contractId) {
  return run(() => api.post(`/api/v1/contracts/${contractId}/close`, {}));
}

export async function cancelContract(contractId) {
  return run(() => api.post(`/api/v1/contracts/${contractId}/cancel`, {}));
}

export async function linkContractDocument(contractId, payload) {
  return run(() => api.post(`/api/v1/contracts/${contractId}/link-document`, payload));
}

export async function generateContractBilling(contractId, payload) {
  return run(() => api.post(`/api/v1/contracts/${contractId}/generate-billing`, payload));
}

export async function generateContractRevrec(contractId, payload) {
  return run(() => api.post(`/api/v1/contracts/${contractId}/generate-revrec`, payload));
}

export async function adjustContractDocumentLink(contractId, linkId, payload) {
  return run(() =>
    api.post(`/api/v1/contracts/${contractId}/documents/${linkId}/adjust`, payload)
  );
}

export async function unlinkContractDocumentLink(contractId, linkId, payload) {
  return run(() =>
    api.post(`/api/v1/contracts/${contractId}/documents/${linkId}/unlink`, payload)
  );
}

export async function listContractDocuments(contractId) {
  return run(() => api.get(`/api/v1/contracts/${contractId}/documents`));
}

export async function listContractLinkableDocuments(contractId, params = {}) {
  return run(() =>
    api.get(`/api/v1/contracts/${contractId}/linkable-documents${toCariQueryString(params)}`)
  );
}

export async function listContractAmendments(contractId) {
  return run(() => api.get(`/api/v1/contracts/${contractId}/amendments`));
}

