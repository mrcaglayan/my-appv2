import { api } from "./client.js";

function toQueryString(params = {}) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    searchParams.set(key, String(value));
  }
  const query = searchParams.toString();
  return query ? `?${query}` : "";
}

export async function listConsolidationGroups(params = {}) {
  const response = await api.get(
    `/api/v1/consolidation/groups${toQueryString(params)}`
  );
  return response.data;
}

export async function upsertConsolidationGroup(payload) {
  const response = await api.post("/api/v1/consolidation/groups", payload);
  return response.data;
}

export async function listConsolidationGroupMembers(groupId, params = {}) {
  const response = await api.get(
    `/api/v1/consolidation/groups/${groupId}/members${toQueryString(params)}`
  );
  return response.data;
}

export async function upsertConsolidationGroupMember(groupId, payload) {
  const response = await api.post(
    `/api/v1/consolidation/groups/${groupId}/members`,
    payload
  );
  return response.data;
}

export async function listConsolidationCoaMappings(groupId, params = {}) {
  const response = await api.get(
    `/api/v1/consolidation/groups/${groupId}/coa-mappings${toQueryString(params)}`
  );
  return response.data;
}

export async function upsertConsolidationCoaMapping(groupId, payload) {
  const response = await api.post(
    `/api/v1/consolidation/groups/${groupId}/coa-mappings`,
    payload
  );
  return response.data;
}

export async function listConsolidationEliminationPlaceholders(
  groupId,
  params = {}
) {
  const response = await api.get(
    `/api/v1/consolidation/groups/${groupId}/elimination-placeholders${toQueryString(
      params
    )}`
  );
  return response.data;
}

export async function upsertConsolidationEliminationPlaceholder(groupId, payload) {
  const response = await api.post(
    `/api/v1/consolidation/groups/${groupId}/elimination-placeholders`,
    payload
  );
  return response.data;
}

export async function listConsolidationRuns(params = {}) {
  const response = await api.get(
    `/api/v1/consolidation/runs${toQueryString(params)}`
  );
  return response.data;
}

export async function createConsolidationRun(payload) {
  const response = await api.post("/api/v1/consolidation/runs", payload);
  return response.data;
}

export async function executeConsolidationRun(runId, payload = {}) {
  const response = await api.post(
    `/api/v1/consolidation/runs/${runId}/execute`,
    payload
  );
  return response.data;
}

export async function finalizeConsolidationRun(runId) {
  const response = await api.post(`/api/v1/consolidation/runs/${runId}/finalize`);
  return response.data;
}
