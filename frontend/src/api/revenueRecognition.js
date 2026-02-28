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

export async function listRevenueRecognitionSchedules(params = {}) {
  return run(() =>
    api.get(`/api/v1/revenue-recognition/schedules${toCariQueryString(params)}`)
  );
}

export async function generateRevenueRecognitionSchedule(payload) {
  return run(() => api.post("/api/v1/revenue-recognition/schedules/generate", payload));
}

export async function listRevenueRecognitionRuns(params = {}) {
  return run(() =>
    api.get(`/api/v1/revenue-recognition/runs${toCariQueryString(params)}`)
  );
}

export async function createRevenueRecognitionRun(payload) {
  return run(() => api.post("/api/v1/revenue-recognition/runs", payload));
}

export async function postRevenueRecognitionRun(runId, payload = {}) {
  return run(() => api.post(`/api/v1/revenue-recognition/runs/${runId}/post`, payload));
}

export async function reverseRevenueRecognitionRun(runId, payload = {}) {
  return run(() => api.post(`/api/v1/revenue-recognition/runs/${runId}/reverse`, payload));
}

export async function generateRevenueAccrual(payload) {
  return run(() => api.post("/api/v1/revenue-recognition/accruals/generate", payload));
}

export async function settleRevenueAccrual(accrualId, payload = {}) {
  return run(() =>
    api.post(`/api/v1/revenue-recognition/accruals/${accrualId}/settle`, payload)
  );
}

export async function reverseRevenueAccrual(accrualId, payload = {}) {
  return run(() =>
    api.post(`/api/v1/revenue-recognition/accruals/${accrualId}/reverse`, payload)
  );
}

export async function getRevenueFutureYearRollforwardReport(params = {}) {
  return run(() =>
    api.get(
      `/api/v1/revenue-recognition/reports/future-year-rollforward${toCariQueryString(params)}`
    )
  );
}

export async function getRevenueDeferredRevenueSplitReport(params = {}) {
  return run(() =>
    api.get(
      `/api/v1/revenue-recognition/reports/deferred-revenue-split${toCariQueryString(params)}`
    )
  );
}

export async function getRevenueAccrualSplitReport(params = {}) {
  return run(() =>
    api.get(`/api/v1/revenue-recognition/reports/accrual-split${toCariQueryString(params)}`)
  );
}

export async function getRevenuePrepaidExpenseSplitReport(params = {}) {
  return run(() =>
    api.get(
      `/api/v1/revenue-recognition/reports/prepaid-expense-split${toCariQueryString(params)}`
    )
  );
}

