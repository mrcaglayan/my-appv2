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

export async function getCariAgingReport(params = {}) {
  return run(() => api.get(`/api/v1/cari/reports/aging${toCariQueryString(params)}`));
}

export async function getCariArAgingReport(params = {}) {
  return run(() => api.get(`/api/v1/cari/reports/ar-aging${toCariQueryString(params)}`));
}

export async function getCariApAgingReport(params = {}) {
  return run(() => api.get(`/api/v1/cari/reports/ap-aging${toCariQueryString(params)}`));
}

export async function getCariOpenItemsReport(params = {}) {
  return run(() => api.get(`/api/v1/cari/reports/open-items${toCariQueryString(params)}`));
}

export async function getCariCounterpartyStatementReport(params = {}) {
  return run(() => api.get(`/api/v1/cari/reports/statement${toCariQueryString(params)}`));
}
