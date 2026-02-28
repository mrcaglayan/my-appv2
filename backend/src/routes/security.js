import express from "express";
import bcrypt from "bcrypt";
import { query, withTransaction } from "../db.js";
import { assertScopeAccess, invalidateRbacCache, requirePermission } from "../middleware/rbac.js";
import { logRbacAuditEvent } from "../audit/rbacAuditLogger.js";
import {
  assertCountryExists,
  assertGroupCompanyBelongsToTenant,
  assertLegalEntityBelongsToTenant,
  assertOperatingUnitBelongsToTenant,
  assertUserBelongsToTenant,
} from "../tenantGuards.js";
import {
  asyncHandler,
  assertRequiredFields,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";

const router = express.Router();

function forbidden(message) {
  const err = new Error(message);
  err.status = 403;
  return err;
}

function parseBoolean(value) {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value !== "string") {
    return false;
  }
  const normalized = value.trim().toLowerCase();
  return normalized === "true" || normalized === "1" || normalized === "yes";
}

const VALID_USER_STATUSES = new Set(["ACTIVE", "DISABLED"]);

function normalizeUserEmail(value) {
  const email = String(value || "")
    .trim()
    .toLowerCase();
  if (!email) {
    throw badRequest("email is required");
  }
  if (email.length > 255) {
    throw badRequest("email cannot exceed 255 characters");
  }
  if (!email.includes("@") || !email.includes(".")) {
    throw badRequest("email is invalid");
  }
  return email;
}

function normalizeUserName(value) {
  const name = String(value || "").trim();
  if (!name) {
    throw badRequest("name is required");
  }
  if (name.length > 255) {
    throw badRequest("name cannot exceed 255 characters");
  }
  return name;
}

function normalizeUserStatus(value) {
  const normalized = String(value || "ACTIVE")
    .trim()
    .toUpperCase();
  if (!VALID_USER_STATUSES.has(normalized)) {
    throw badRequest("status must be ACTIVE or DISABLED");
  }
  return normalized;
}

function validateUserPassword(value) {
  const password = String(value || "");
  if (password.length < 8) {
    throw badRequest("password must be at least 8 characters");
  }
  if (password.length > 128) {
    throw badRequest("password cannot exceed 128 characters");
  }
  return password;
}

const VALID_SCOPE_TYPES = new Set([
  "TENANT",
  "GROUP",
  "COUNTRY",
  "LEGAL_ENTITY",
  "OPERATING_UNIT",
]);

const VALID_EFFECTS = new Set(["ALLOW", "DENY"]);

function normalizeScopeType(value) {
  const scopeType = String(value || "").toUpperCase();
  if (!VALID_SCOPE_TYPES.has(scopeType)) {
    throw badRequest(
      "scopeType must be one of TENANT, GROUP, COUNTRY, LEGAL_ENTITY, OPERATING_UNIT"
    );
  }
  return scopeType;
}

function normalizeEffect(value, fallback = "ALLOW") {
  const effect = String(value || fallback).toUpperCase();
  if (!VALID_EFFECTS.has(effect)) {
    throw badRequest("effect must be ALLOW or DENY");
  }
  return effect;
}

async function getRoleForTenant(roleId, tenantId) {
  const roleResult = await query(
    `SELECT id, tenant_id, code, name, is_system
     FROM roles
     WHERE id = ?
       AND tenant_id = ?
     LIMIT 1`,
    [roleId, tenantId]
  );
  return roleResult.rows[0] || null;
}

async function isTenantAdminUser(userId, tenantId) {
  const normalizedUserId = parsePositiveInt(userId);
  const normalizedTenantId = parsePositiveInt(tenantId);
  if (!normalizedUserId || !normalizedTenantId) {
    return false;
  }

  const result = await query(
    `SELECT
       SUM(CASE WHEN urs.effect = 'ALLOW' THEN 1 ELSE 0 END) AS allow_count,
       SUM(CASE WHEN urs.effect = 'DENY' THEN 1 ELSE 0 END) AS deny_count
     FROM user_role_scopes urs
     JOIN roles r ON r.id = urs.role_id
     WHERE urs.user_id = ?
       AND urs.tenant_id = ?
       AND urs.scope_type = 'TENANT'
       AND urs.scope_id = ?
       AND r.tenant_id = ?
       AND r.code = 'TenantAdmin'
     LIMIT 1`,
    [normalizedUserId, normalizedTenantId, normalizedTenantId, normalizedTenantId]
  );

  const allowCount = Number(result.rows[0]?.allow_count || 0);
  const denyCount = Number(result.rows[0]?.deny_count || 0);
  return allowCount > 0 && denyCount === 0;
}

async function assertSystemRoleManageAllowed(req, tenantId, role) {
  if (!Boolean(role?.is_system)) {
    return;
  }

  const actorUserId = parsePositiveInt(req.user?.userId);
  const canManageSystemRoles = await isTenantAdminUser(actorUserId, tenantId);
  if (!canManageSystemRoles) {
    throw forbidden("Only TenantAdmin can manage system role assignments");
  }
}

async function upsertPermissionAndGetId(permissionCode, runQuery = query) {
  await runQuery(
    `INSERT INTO permissions (code, description)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE
       description = VALUES(description)`,
    [permissionCode, permissionCode]
  );

  const permissionResult = await runQuery(
    `SELECT id FROM permissions WHERE code = ? LIMIT 1`,
    [permissionCode]
  );
  const permissionId = permissionResult.rows[0]?.id;
  if (!permissionId) {
    throw new Error(`Permission lookup failed for ${permissionCode}`);
  }
  return permissionId;
}

async function assertScopeTargetExists(tenantId, scopeType, scopeId) {
  const normalizedScopeType = String(scopeType || "").toUpperCase();
  const parsedScopeId = parsePositiveInt(scopeId);

  if (!parsedScopeId) {
    throw badRequest("scopeId must be a positive integer");
  }

  if (normalizedScopeType === "TENANT") {
    if (parsePositiveInt(tenantId) !== parsedScopeId) {
      throw badRequest("TENANT scopeId must match tenantId");
    }
    return;
  }

  if (normalizedScopeType === "GROUP") {
    await assertGroupCompanyBelongsToTenant(tenantId, parsedScopeId, "scopeId");
    return;
  }

  if (normalizedScopeType === "COUNTRY") {
    await assertCountryExists(parsedScopeId, "scopeId");
    return;
  }

  if (normalizedScopeType === "LEGAL_ENTITY") {
    await assertLegalEntityBelongsToTenant(tenantId, parsedScopeId, "scopeId");
    return;
  }

  if (normalizedScopeType === "OPERATING_UNIT") {
    await assertOperatingUnitBelongsToTenant(tenantId, parsedScopeId, "scopeId");
    return;
  }

  throw badRequest("Unsupported scopeType");
}

router.get(
  "/permissions",
  requirePermission("security.permission.read"),
  asyncHandler(async (req, res) => {
    const q = req.query.q ? String(req.query.q).trim() : null;
    const params = [];
    let whereClause = "";

    if (q) {
      whereClause = "WHERE code LIKE ? OR description LIKE ?";
      params.push(`%${q}%`, `%${q}%`);
    }

    const result = await query(
      `SELECT id, code, description
       FROM permissions
       ${whereClause}
       ORDER BY code`,
      params
    );

    return res.json({
      rows: result.rows,
    });
  })
);

router.get(
  "/users",
  requirePermission("security.role_assignment.read"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const q = req.query.q ? String(req.query.q).trim() : null;
    const conditions = ["tenant_id = ?"];
    const params = [tenantId];

    if (q) {
      conditions.push("(email LIKE ? OR name LIKE ?)");
      params.push(`%${q}%`, `%${q}%`);
    }

    const result = await query(
      `SELECT id, email, name, status, created_at
       FROM users
       WHERE ${conditions.join(" AND ")}
       ORDER BY name, email
       LIMIT 200`,
      params
    );

    return res.json({
      tenantId,
      rows: result.rows,
    });
  })
);

router.post(
  "/users",
  requirePermission("security.role_assignment.upsert"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    assertRequiredFields(req.body, ["email", "name", "password"]);
    const email = normalizeUserEmail(req.body.email);
    const name = normalizeUserName(req.body.name);
    const password = validateUserPassword(req.body.password);
    const status = normalizeUserStatus(req.body.status);

    const existingUserResult = await query(
      `SELECT id, tenant_id
       FROM users
       WHERE email = ?
       LIMIT 1`,
      [email]
    );
    if (existingUserResult.rows[0]) {
      throw badRequest("email already exists");
    }

    const passwordHash = await bcrypt.hash(password, 10);

    let createdUserId = null;
    try {
      const result = await query(
        `INSERT INTO users (
           tenant_id,
           email,
           password_hash,
           name,
           status
         )
         VALUES (?, ?, ?, ?, ?)`,
        [tenantId, email, passwordHash, name, status]
      );
      createdUserId = parsePositiveInt(result.rows.insertId);
    } catch (err) {
      if (err?.errno === 1062) {
        throw badRequest("email already exists");
      }
      throw err;
    }

    await logRbacAuditEvent(req, {
      tenantId,
      targetUserId: createdUserId,
      action: "user.create",
      resourceType: "user",
      resourceId: createdUserId,
      scopeType: "TENANT",
      scopeId: tenantId,
      payload: {
        userId: createdUserId,
        email,
        name,
        status,
      },
    });

    return res.status(201).json({
      ok: true,
      id: createdUserId,
      tenantId,
    });
  })
);

router.get(
  "/roles",
  requirePermission("security.role.read"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const includePermissions = parseBoolean(req.query.includePermissions);
    const result = await query(
      `SELECT id, tenant_id, code, name, is_system, created_at
       FROM roles
       WHERE tenant_id = ?
       ORDER BY code`,
      [tenantId]
    );

    const rows = result.rows || [];
    if (!includePermissions || rows.length === 0) {
      return res.json({ tenantId, rows });
    }

    const roleIds = rows.map((row) => row.id);
    const permissionResult = await query(
      `SELECT rp.role_id, p.code
       FROM role_permissions rp
       JOIN permissions p ON p.id = rp.permission_id
       WHERE rp.role_id IN (${roleIds.map(() => "?").join(", ")})
       ORDER BY rp.role_id, p.code`,
      roleIds
    );

    const codesByRoleId = new Map();
    for (const row of permissionResult.rows) {
      if (!codesByRoleId.has(row.role_id)) {
        codesByRoleId.set(row.role_id, []);
      }
      codesByRoleId.get(row.role_id).push(row.code);
    }

    const enriched = rows.map((row) => ({
      ...row,
      permissionCodes: codesByRoleId.get(row.id) || [],
    }));

    return res.json({
      tenantId,
      rows: enriched,
    });
  })
);

router.post(
  "/roles",
  requirePermission("security.role.upsert"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    assertRequiredFields(req.body, ["code", "name"]);
    const code = String(req.body.code).trim();
    const name = String(req.body.name).trim();
    const isSystem = Boolean(req.body.isSystem);

    const existingRoleResult = await query(
      `SELECT id
       FROM roles
       WHERE tenant_id = ?
         AND code = ?
       LIMIT 1`,
      [tenantId, code]
    );
    const existingRoleId = parsePositiveInt(existingRoleResult.rows[0]?.id);

    const result = await query(
      `INSERT INTO roles (tenant_id, code, name, is_system)
       VALUES (?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         name = VALUES(name),
         is_system = VALUES(is_system)`,
      [tenantId, code, name, isSystem]
    );

    const roleId = result.rows.insertId || existingRoleId || null;
    const wasCreated = !existingRoleId && Boolean(result.rows.insertId);

    if (wasCreated) {
      await logRbacAuditEvent(req, {
        tenantId,
        action: "role.create",
        resourceType: "role",
        resourceId: roleId,
        payload: {
          code,
          name,
          isSystem,
        },
      });
    }

    return res.status(201).json({ ok: true, id: roleId });
  })
);

router.get(
  "/roles/:roleId/permissions",
  requirePermission("security.role.read"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const roleId = parsePositiveInt(req.params.roleId);
    if (!roleId) {
      throw badRequest("roleId must be a positive integer");
    }

    const role = await getRoleForTenant(roleId, tenantId);
    if (!role) {
      throw badRequest("Role not found");
    }

    const permissionResult = await query(
      `SELECT p.id, p.code, p.description
       FROM role_permissions rp
       JOIN permissions p ON p.id = rp.permission_id
       WHERE rp.role_id = ?
       ORDER BY p.code`,
      [roleId]
    );

    return res.json({
      role,
      permissions: permissionResult.rows,
    });
  })
);

router.post(
  "/roles/:roleId/permissions",
  requirePermission("security.role_permissions.assign"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const roleId = parsePositiveInt(req.params.roleId);
    if (!roleId) {
      throw badRequest("roleId must be a positive integer");
    }

    const role = await getRoleForTenant(roleId, tenantId);
    if (!role) {
      throw badRequest("Role not found");
    }

    const permissionCodes = Array.isArray(req.body?.permissionCodes)
      ? req.body.permissionCodes.map((code) => String(code).trim()).filter(Boolean)
      : [];

    if (permissionCodes.length === 0) {
      throw badRequest("permissionCodes must be a non-empty array");
    }

    await withTransaction(async (tx) => {
      for (const permissionCode of permissionCodes) {
        // eslint-disable-next-line no-await-in-loop
        const permissionId = await upsertPermissionAndGetId(permissionCode, tx.query);

        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT IGNORE INTO role_permissions (role_id, permission_id)
           VALUES (?, ?)`,
          [roleId, permissionId]
        );
      }
    });
    await invalidateRbacCache(tenantId);

    return res.status(201).json({
      ok: true,
      roleId,
      assignedPermissionCount: permissionCodes.length,
    });
  })
);

router.put(
  "/roles/:roleId/permissions",
  requirePermission("security.role_permissions.assign"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const roleId = parsePositiveInt(req.params.roleId);
    if (!roleId) {
      throw badRequest("roleId must be a positive integer");
    }

    const role = await getRoleForTenant(roleId, tenantId);
    if (!role) {
      throw badRequest("Role not found");
    }

    const permissionCodesRaw = Array.isArray(req.body?.permissionCodes)
      ? req.body.permissionCodes
      : null;
    if (!permissionCodesRaw) {
      throw badRequest("permissionCodes must be an array");
    }

    const normalizedPermissionCodes = Array.from(
      new Set(permissionCodesRaw.map((code) => String(code).trim()).filter(Boolean))
    );

    const beforeCodes = await withTransaction(async (tx) => {
      const beforeResult = await tx.query(
        `SELECT p.code
         FROM role_permissions rp
         JOIN permissions p ON p.id = rp.permission_id
         WHERE rp.role_id = ?
         ORDER BY p.code`,
        [roleId]
      );
      const beforeCodeRows = beforeResult.rows.map((row) => row.code);

      await tx.query(`DELETE FROM role_permissions WHERE role_id = ?`, [roleId]);

      for (const permissionCode of normalizedPermissionCodes) {
        const permissionId = await upsertPermissionAndGetId(permissionCode, tx.query);
        await tx.query(
          `INSERT IGNORE INTO role_permissions (role_id, permission_id)
           VALUES (?, ?)`,
          [roleId, permissionId]
        );
      }

      return beforeCodeRows;
    });
    await invalidateRbacCache(tenantId);

    await logRbacAuditEvent(req, {
      tenantId,
      action: "role.permission.replace",
      resourceType: "role",
      resourceId: roleId,
      scopeType: "TENANT",
      scopeId: tenantId,
      payload: {
        roleCode: role.code,
        beforePermissionCodes: beforeCodes,
        afterPermissionCodes: normalizedPermissionCodes,
      },
    });

    return res.json({
      ok: true,
      roleId,
      permissionCount: normalizedPermissionCodes.length,
    });
  })
);

router.get(
  "/role-assignments",
  requirePermission("security.role_assignment.read"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const userId = parsePositiveInt(req.query.userId);
    const roleId = parsePositiveInt(req.query.roleId);
    const scopeId = parsePositiveInt(req.query.scopeId);
    const scopeType = req.query.scopeType
      ? String(req.query.scopeType).toUpperCase()
      : null;

    const conditions = ["urs.tenant_id = ?"];
    const params = [tenantId];

    if (userId) {
      conditions.push("urs.user_id = ?");
      params.push(userId);
    }
    if (roleId) {
      conditions.push("urs.role_id = ?");
      params.push(roleId);
    }
    if (scopeType) {
      conditions.push("urs.scope_type = ?");
      params.push(scopeType);
    }
    if (scopeId) {
      conditions.push("urs.scope_id = ?");
      params.push(scopeId);
    }

    const result = await query(
      `SELECT
         urs.id,
         urs.user_id,
         u.email AS user_email,
         u.name AS user_name,
         urs.role_id,
         r.code AS role_code,
         r.name AS role_name,
         urs.scope_type,
         urs.scope_id,
         urs.effect,
         urs.created_at
       FROM user_role_scopes urs
       JOIN users u ON u.id = urs.user_id
       JOIN roles r ON r.id = urs.role_id
       WHERE ${conditions.join(" AND ")}
       ORDER BY urs.id DESC`,
      params
    );

    return res.json({
      tenantId,
      rows: result.rows,
    });
  })
);

router.post(
  "/role-assignments",
  requirePermission("security.role_assignment.upsert", {
    resolveScope: (req, tenantId) => {
      const scopeType = String(req.body?.scopeType || "TENANT").toUpperCase();
      const scopeId =
        parsePositiveInt(req.body?.scopeId) || parsePositiveInt(tenantId);
      return { scopeType, scopeId };
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    assertRequiredFields(req.body, [
      "userId",
      "roleId",
      "scopeType",
      "scopeId",
      "effect",
    ]);

    const userId = parsePositiveInt(req.body.userId);
    const roleId = parsePositiveInt(req.body.roleId);
    const scopeId = parsePositiveInt(req.body.scopeId);
    const scopeType = normalizeScopeType(req.body.scopeType);
    const effect = normalizeEffect(req.body.effect);

    if (!userId || !roleId || !scopeId) {
      throw badRequest("userId, roleId and scopeId must be positive integers");
    }

    await assertUserBelongsToTenant(tenantId, userId, "userId");
    const role = await getRoleForTenant(roleId, tenantId);
    if (!role) {
      throw badRequest("Role not found");
    }
    await assertSystemRoleManageAllowed(req, tenantId, role);
    await assertScopeTargetExists(tenantId, scopeType, scopeId);

    const existingResult = await query(
      `SELECT id, effect
       FROM user_role_scopes
       WHERE tenant_id = ?
         AND user_id = ?
         AND role_id = ?
         AND scope_type = ?
         AND scope_id = ?
       LIMIT 1`,
      [tenantId, userId, roleId, scopeType, scopeId]
    );
    const existingAssignmentId = parsePositiveInt(existingResult.rows[0]?.id);

    await query(
      `INSERT INTO user_role_scopes (
          tenant_id, user_id, role_id, scope_type, scope_id, effect
        )
       VALUES (?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
       effect = VALUES(effect)`,
      [tenantId, userId, roleId, scopeType, scopeId, effect]
    );
    await invalidateRbacCache(tenantId);

    const currentResult = await query(
      `SELECT id
       FROM user_role_scopes
       WHERE tenant_id = ?
         AND user_id = ?
         AND role_id = ?
         AND scope_type = ?
         AND scope_id = ?
       LIMIT 1`,
      [tenantId, userId, roleId, scopeType, scopeId]
    );
    const currentAssignmentId = parsePositiveInt(currentResult.rows[0]?.id);

    if (!existingAssignmentId) {
      await logRbacAuditEvent(req, {
        tenantId,
        targetUserId: userId,
        action: "assignment.create",
        resourceType: "user_role_scope",
        scopeType,
        scopeId,
        payload: {
          userId,
          roleId,
          scopeType,
          scopeId,
          effect,
        },
      });
    }

    return res.status(201).json({
      ok: true,
      created: !existingAssignmentId,
      assignmentId: currentAssignmentId || existingAssignmentId || null,
    });
  })
);

router.put(
  "/role-assignments/:assignmentId/scope",
  requirePermission("security.role_assignment.upsert", {
    resolveScope: (req, tenantId) => {
      const scopeType = String(req.body?.scopeType || "TENANT").toUpperCase();
      const scopeId =
        parsePositiveInt(req.body?.scopeId) || parsePositiveInt(tenantId);
      return { scopeType, scopeId };
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const assignmentId = parsePositiveInt(req.params.assignmentId);
    if (!assignmentId) {
      throw badRequest("assignmentId must be a positive integer");
    }

    assertRequiredFields(req.body, ["scopeType", "scopeId", "effect"]);

    const scopeType = normalizeScopeType(req.body.scopeType);
    const scopeId = parsePositiveInt(req.body.scopeId);
    const effect = normalizeEffect(req.body.effect);

    if (!scopeId) {
      throw badRequest("scopeId must be a positive integer");
    }

    await assertScopeTargetExists(tenantId, scopeType, scopeId);

    const assignmentResult = await query(
      `SELECT id, user_id, role_id, scope_type, scope_id, effect
       FROM user_role_scopes
       WHERE id = ?
         AND tenant_id = ?
       LIMIT 1`,
      [assignmentId, tenantId]
    );
    const assignment = assignmentResult.rows[0];
    if (!assignment) {
      throw badRequest("Role assignment not found");
    }
    const role = await getRoleForTenant(assignment.role_id, tenantId);
    if (!role) {
      throw badRequest("Role not found");
    }
    await assertSystemRoleManageAllowed(req, tenantId, role);

    const oldScopeType = String(assignment.scope_type || "").toLowerCase();
    const oldScopeId = parsePositiveInt(assignment.scope_id);
    if (oldScopeType && oldScopeType !== "tenant" && oldScopeId) {
      assertScopeAccess(req, oldScopeType, oldScopeId, "existing scope");
    }

    await query(
      `UPDATE user_role_scopes
       SET scope_type = ?,
           scope_id = ?,
           effect = ?
       WHERE id = ?
         AND tenant_id = ?`,
      [scopeType, scopeId, effect, assignmentId, tenantId]
    );
    await invalidateRbacCache(tenantId);

    await logRbacAuditEvent(req, {
      tenantId,
      targetUserId: parsePositiveInt(assignment.user_id),
      action: "assignment.scope_replace",
      resourceType: "user_role_scope",
      resourceId: assignmentId,
      scopeType,
      scopeId,
      payload: {
        assignmentId,
        userId: parsePositiveInt(assignment.user_id),
        roleId: parsePositiveInt(assignment.role_id),
        before: {
          scopeType: String(assignment.scope_type || "").toUpperCase(),
          scopeId: parsePositiveInt(assignment.scope_id),
          effect: String(assignment.effect || "").toUpperCase(),
        },
        after: {
          scopeType,
          scopeId,
          effect,
        },
      },
    });

    return res.json({
      ok: true,
      assignmentId,
      scopeType,
      scopeId,
      effect,
    });
  })
);

router.delete(
  "/role-assignments/:assignmentId",
  requirePermission("security.role_assignment.upsert"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const assignmentId = parsePositiveInt(req.params.assignmentId);
    if (!assignmentId) {
      throw badRequest("assignmentId must be a positive integer");
    }

    const assignmentResult = await query(
      `SELECT
         urs.id,
         urs.role_id,
         r.code AS role_code,
         r.is_system
       FROM user_role_scopes urs
       LEFT JOIN roles r
         ON r.id = urs.role_id
        AND r.tenant_id = urs.tenant_id
       WHERE urs.id = ?
         AND urs.tenant_id = ?
       LIMIT 1`,
      [assignmentId, tenantId]
    );
    const assignment = assignmentResult.rows[0] || null;
    if (assignment) {
      await assertSystemRoleManageAllowed(req, tenantId, assignment);
    }

    await query(
      `DELETE FROM user_role_scopes
       WHERE id = ?
         AND tenant_id = ?`,
      [assignmentId, tenantId]
    );
    await invalidateRbacCache(tenantId);

    return res.json({ ok: true });
  })
);

router.get(
  "/data-scopes",
  requirePermission("security.data_scope.read"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const userId = parsePositiveInt(req.query.userId);
    const scopeType = req.query.scopeType
      ? String(req.query.scopeType).toUpperCase()
      : null;
    const scopeId = parsePositiveInt(req.query.scopeId);

    const conditions = ["ds.tenant_id = ?"];
    const params = [tenantId];

    if (userId) {
      conditions.push("ds.user_id = ?");
      params.push(userId);
    }
    if (scopeType) {
      conditions.push("ds.scope_type = ?");
      params.push(scopeType);
    }
    if (scopeId) {
      conditions.push("ds.scope_id = ?");
      params.push(scopeId);
    }

    const result = await query(
      `SELECT
         ds.id,
         ds.tenant_id,
         ds.user_id,
         u.email AS user_email,
         u.name AS user_name,
         ds.scope_type,
         ds.scope_id,
         ds.effect,
         ds.created_by_user_id,
         ds.created_at,
         ds.updated_at
       FROM data_scopes ds
       JOIN users u ON u.id = ds.user_id
       WHERE ${conditions.join(" AND ")}
       ORDER BY ds.id DESC`,
      params
    );

    return res.json({
      tenantId,
      rows: result.rows,
    });
  })
);

router.post(
  "/data-scopes",
  requirePermission("security.data_scope.upsert", {
    resolveScope: (req, tenantId) => {
      const scopeType = String(req.body?.scopeType || "TENANT").toUpperCase();
      const scopeId =
        parsePositiveInt(req.body?.scopeId) || parsePositiveInt(tenantId);
      return { scopeType, scopeId };
    },
  }),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    assertRequiredFields(req.body, ["userId", "scopeType", "scopeId"]);
    const userId = parsePositiveInt(req.body.userId);
    const scopeType = normalizeScopeType(req.body.scopeType);
    const scopeId = parsePositiveInt(req.body.scopeId);
    const effect = normalizeEffect(req.body.effect);
    const createdByUserId = parsePositiveInt(req.user?.userId);

    if (!userId || !scopeId) {
      throw badRequest("userId and scopeId must be positive integers");
    }

    await assertUserBelongsToTenant(tenantId, userId, "userId");
    await assertScopeTargetExists(tenantId, scopeType, scopeId);

    await query(
      `INSERT INTO data_scopes (
          tenant_id, user_id, scope_type, scope_id, effect, created_by_user_id
       )
       VALUES (?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         effect = VALUES(effect),
         created_by_user_id = VALUES(created_by_user_id)`,
      [tenantId, userId, scopeType, scopeId, effect, createdByUserId]
    );
    await invalidateRbacCache(tenantId);

    return res.status(201).json({ ok: true });
  })
);

router.put(
  "/data-scopes/users/:userId/replace",
  requirePermission("security.data_scope.upsert"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const userId = parsePositiveInt(req.params.userId);
    if (!userId) {
      throw badRequest("userId must be a positive integer");
    }
    await assertUserBelongsToTenant(tenantId, userId, "userId");

    const scopes = Array.isArray(req.body?.scopes) ? req.body.scopes : null;
    if (!scopes) {
      throw badRequest("scopes must be an array");
    }

    const normalizedScopes = [];
    const seenScopeKeys = new Set();
    for (const scope of scopes) {
      const scopeType = normalizeScopeType(scope?.scopeType);
      const scopeId = parsePositiveInt(scope?.scopeId);
      const effect = normalizeEffect(scope?.effect);
      if (!scopeId) {
        throw badRequest("Each scope item requires a positive scopeId");
      }
      const scopeKey = `${scopeType}:${scopeId}`;
      if (seenScopeKeys.has(scopeKey)) {
        throw badRequest(`Duplicate scope is not allowed: ${scopeKey}`);
      }
      seenScopeKeys.add(scopeKey);
      // Enforce scope target ownership/existence before replacing all scopes.
      // This prevents deleting valid rows when payload contains invalid scope references.
      // Validation runs outside transaction to fail fast.
      // eslint-disable-next-line no-await-in-loop
      await assertScopeTargetExists(tenantId, scopeType, scopeId);
      normalizedScopes.push({ scopeType, scopeId, effect });
    }

    const createdByUserId = parsePositiveInt(req.user?.userId);

    const beforeRows = await withTransaction(async (tx) => {
      const beforeResult = await tx.query(
        `SELECT scope_type, scope_id, effect
         FROM data_scopes
         WHERE tenant_id = ?
           AND user_id = ?
         ORDER BY scope_type, scope_id`,
        [tenantId, userId]
      );

      await tx.query(
        `DELETE FROM data_scopes
         WHERE tenant_id = ?
           AND user_id = ?`,
        [tenantId, userId]
      );

      for (const scope of normalizedScopes) {
        // eslint-disable-next-line no-await-in-loop
        await tx.query(
          `INSERT INTO data_scopes (
              tenant_id, user_id, scope_type, scope_id, effect, created_by_user_id
           )
           VALUES (?, ?, ?, ?, ?, ?)`,
          [
            tenantId,
            userId,
            scope.scopeType,
            scope.scopeId,
            scope.effect,
            createdByUserId,
          ]
        );
      }

      return beforeResult.rows;
    });
    await invalidateRbacCache(tenantId);

    await logRbacAuditEvent(req, {
      tenantId,
      targetUserId: userId,
      action: "assignment.scope_replace",
      resourceType: "data_scope",
      resourceId: userId,
      scopeType: "TENANT",
      scopeId: tenantId,
      payload: {
        userId,
        beforeScopes: beforeRows,
        afterScopes: normalizedScopes,
      },
    });

    return res.json({
      ok: true,
      userId,
      scopeCount: normalizedScopes.length,
    });
  })
);

router.delete(
  "/data-scopes/:dataScopeId",
  requirePermission("security.data_scope.upsert"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const dataScopeId = parsePositiveInt(req.params.dataScopeId);
    if (!dataScopeId) {
      throw badRequest("dataScopeId must be a positive integer");
    }

    await query(
      `DELETE FROM data_scopes
       WHERE id = ?
         AND tenant_id = ?`,
      [dataScopeId, tenantId]
    );
    await invalidateRbacCache(tenantId);

    return res.json({ ok: true });
  })
);

export default router;
