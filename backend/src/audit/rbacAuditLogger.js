import { query } from "../db.js";
import { parsePositiveInt, resolveTenantId } from "../routes/_utils.js";
import {
  buildRequestLogMeta,
  logError,
  resolveRequestId as resolveLoggerRequestId,
} from "../observability/logger.js";

const VALID_SCOPE_TYPES = new Set([
  "TENANT",
  "GROUP",
  "COUNTRY",
  "LEGAL_ENTITY",
  "OPERATING_UNIT",
]);

function toNullableString(value, maxLength = 255) {
  if (value === undefined || value === null) {
    return null;
  }
  const text = String(value).trim();
  if (!text) {
    return null;
  }
  return text.slice(0, maxLength);
}

function resolveIpAddress(req) {
  const forwardedFor = req?.headers?.["x-forwarded-for"];
  if (typeof forwardedFor === "string" && forwardedFor.trim()) {
    return forwardedFor.split(",")[0].trim().slice(0, 64);
  }
  const ip = req?.ip || req?.socket?.remoteAddress || null;
  return toNullableString(ip, 64);
}

function resolveRequestId(req, explicitRequestId) {
  return toNullableString(resolveLoggerRequestId(req, explicitRequestId), 80);
}

function toPayloadJson(value) {
  if (value === undefined) {
    return null;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return JSON.stringify({
      serializationError: "payload_json could not be serialized",
    });
  }
}

export async function logRbacAuditEvent(req, event) {
  try {
    const tenantId =
      parsePositiveInt(event?.tenantId) || parsePositiveInt(resolveTenantId(req));
    if (!tenantId) {
      return false;
    }

    const action = toNullableString(event?.action, 120);
    const resourceType = toNullableString(event?.resourceType, 80);
    if (!action || !resourceType) {
      return false;
    }

    const scopeTypeRaw = toNullableString(event?.scopeType, 32);
    const scopeType = scopeTypeRaw ? scopeTypeRaw.toUpperCase() : null;
    const scopeId = parsePositiveInt(event?.scopeId);
    const actorUserId =
      parsePositiveInt(event?.actorUserId) || parsePositiveInt(req?.user?.userId);
    const targetUserId = parsePositiveInt(event?.targetUserId);
    const requestId = resolveRequestId(req, event?.requestId);
    const ipAddress = resolveIpAddress(req);
    const userAgent = toNullableString(req?.headers?.["user-agent"], 255);
    const resourceId = toNullableString(event?.resourceId, 120);
    const payloadJson = toPayloadJson(event?.payload);

    const normalizedScopeType =
      scopeType && VALID_SCOPE_TYPES.has(scopeType) ? scopeType : null;
    const normalizedScopeId = normalizedScopeType ? scopeId : null;

    await query(
      `INSERT INTO rbac_audit_logs (
          tenant_id,
          actor_user_id,
          target_user_id,
          action,
          resource_type,
          resource_id,
          scope_type,
          scope_id,
          request_id,
          ip_address,
          user_agent,
          payload_json
       )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        tenantId,
        actorUserId,
        targetUserId,
        action,
        resourceType,
        resourceId,
        normalizedScopeType,
        normalizedScopeId,
        requestId,
        ipAddress,
        userAgent,
        payloadJson,
      ]
    );

    return true;
  } catch (err) {
    logError(
      "Failed to write RBAC audit log",
      buildRequestLogMeta(req, {
        tenantId: parsePositiveInt(event?.tenantId) || null,
        action: toNullableString(event?.action, 120),
        resourceType: toNullableString(event?.resourceType, 80),
      }),
      err
    );
    return false;
  }
}
