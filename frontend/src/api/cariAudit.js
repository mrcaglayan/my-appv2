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

export async function listCariAudit(params = {}) {
  return run(() => api.get(`/api/v1/cari/audit${toCariQueryString(params)}`));
}
