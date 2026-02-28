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

export async function listPermissions(params = {}) {
  const response = await api.get(
    `/api/v1/security/permissions${toQueryString(params)}`
  );
  return response.data;
}

export async function listUsers(params = {}) {
  const response = await api.get(`/api/v1/security/users${toQueryString(params)}`);
  return response.data;
}

export async function createSecurityUser(payload) {
  const response = await api.post("/api/v1/security/users", payload);
  return response.data;
}

export async function listRoles(params = {}) {
  const response = await api.get(`/api/v1/security/roles${toQueryString(params)}`);
  return response.data;
}

export async function createOrUpdateRole(payload) {
  const response = await api.post("/api/v1/security/roles", payload);
  return response.data;
}

export async function replaceRolePermissions(roleId, permissionCodes) {
  const response = await api.put(
    `/api/v1/security/roles/${roleId}/permissions`,
    { permissionCodes }
  );
  return response.data;
}

export async function listRoleAssignments(params = {}) {
  const response = await api.get(
    `/api/v1/security/role-assignments${toQueryString(params)}`
  );
  return response.data;
}

export async function createRoleAssignment(payload) {
  const response = await api.post("/api/v1/security/role-assignments", payload);
  return response.data;
}

export async function replaceRoleAssignmentScope(assignmentId, payload) {
  const response = await api.put(
    `/api/v1/security/role-assignments/${assignmentId}/scope`,
    payload
  );
  return response.data;
}

export async function deleteRoleAssignment(assignmentId) {
  const response = await api.delete(
    `/api/v1/security/role-assignments/${assignmentId}`
  );
  return response.data;
}

export async function listDataScopes(params = {}) {
  const response = await api.get(
    `/api/v1/security/data-scopes${toQueryString(params)}`
  );
  return response.data;
}

export async function replaceUserDataScopes(userId, scopes) {
  const response = await api.put(
    `/api/v1/security/data-scopes/users/${userId}/replace`,
    { scopes }
  );
  return response.data;
}

export async function listAuditLogs(params = {}) {
  const response = await api.get(`/api/v1/rbac/audit-logs${toQueryString(params)}`);
  return response.data;
}

export async function listGroupCompanies() {
  const response = await api.get("/api/v1/org/group-companies");
  return response.data;
}

export async function listCountries() {
  const response = await api.get("/api/v1/org/countries");
  return response.data;
}

export async function listLegalEntities() {
  const response = await api.get("/api/v1/org/legal-entities");
  return response.data;
}

export async function listOperatingUnits() {
  const response = await api.get("/api/v1/org/operating-units");
  return response.data;
}
