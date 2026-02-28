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

function normalizeCounterpartyPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return payload;
  }
  const normalized = { ...payload };
  if (normalized.arAccountId === undefined) {
    delete normalized.arAccountId;
  }
  if (normalized.apAccountId === undefined) {
    delete normalized.apAccountId;
  }
  return normalized;
}

export async function listCariCounterparties(params = {}) {
  return run(() => api.get(`/api/v1/cari/counterparties${toCariQueryString(params)}`));
}

export async function getCariCounterparty(counterpartyId) {
  return run(() => api.get(`/api/v1/cari/counterparties/${counterpartyId}`));
}

export async function createCariCounterparty(payload) {
  return run(() =>
    api.post("/api/v1/cari/counterparties", normalizeCounterpartyPayload(payload))
  );
}

export async function updateCariCounterparty(counterpartyId, payload) {
  return run(() =>
    api.put(
      `/api/v1/cari/counterparties/${counterpartyId}`,
      normalizeCounterpartyPayload(payload)
    )
  );
}
