import express from "express";
import bcrypt from "bcrypt";
import jwt from "jsonwebtoken";
import { query, withTransaction } from "../db.js";
import { invalidateRbacCache } from "../middleware/rbac.js";
import {
  asyncHandler,
  assertRequiredFields,
  badRequest,
  parsePositiveInt,
} from "./_utils.js";

const router = express.Router();
const TENANT_STATUSES = new Set(["ACTIVE", "SUSPENDED"]);

function parseBooleanEnv(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return ["1", "true", "yes", "on"].includes(normalized);
}

function isProviderPanelEnabled() {
  const raw = process.env.PROVIDER_CONTROL_PANEL_ENABLED;
  if (raw === undefined || raw === null || String(raw).trim() === "") {
    return String(process.env.NODE_ENV || "").toLowerCase() !== "production";
  }
  return parseBooleanEnv(raw);
}

function requireProviderPanelEnabled() {
  if (isProviderPanelEnabled()) {
    return;
  }

  const err = new Error("Route not found");
  err.status = 404;
  throw err;
}

function requireLegacyBootstrapEnabled() {
  const enabled = parseBooleanEnv(process.env.PROVIDER_BOOTSTRAP_ENABLED);
  if (enabled) {
    return;
  }

  const err = new Error("Route not found");
  err.status = 404;
  throw err;
}

function getProviderJwtSecret() {
  const secret = String(process.env.PROVIDER_JWT_SECRET || "").trim();
  if (!secret) {
    const err = new Error("Provider control panel is not configured");
    err.status = 503;
    throw err;
  }
  return secret;
}

function requireProviderKey(req) {
  const configuredKey = String(process.env.PROVIDER_API_KEY || "").trim();
  if (!configuredKey) {
    const err = new Error("Provider provisioning is not configured");
    err.status = 503;
    throw err;
  }

  const providedKey = String(req.headers["x-provider-key"] || "").trim();
  if (!providedKey || providedKey !== configuredKey) {
    const err = new Error("Invalid provider key");
    err.status = 401;
    throw err;
  }
}

function parseBearerToken(req) {
  const header = String(req.headers.authorization || "");
  const [type, token] = header.split(" ");
  if (type !== "Bearer" || !token) {
    const err = new Error("Missing token");
    err.status = 401;
    throw err;
  }
  return token;
}

function requireProviderAuth(req, res, next) {
  try {
    requireProviderPanelEnabled();
    const token = parseBearerToken(req);
    const payload = jwt.verify(token, getProviderJwtSecret());
    const providerAdminId = parsePositiveInt(payload?.providerAdminId);
    if (!providerAdminId) {
      const err = new Error("Invalid token");
      err.status = 401;
      throw err;
    }

    req.providerAdmin = {
      providerAdminId,
      email: String(payload.email || "").trim().toLowerCase(),
    };
    return next();
  } catch (err) {
    if (!err.status) {
      err.status = 401;
      err.message = "Invalid or expired token";
    }
    return next(err);
  }
}

function normalizeTenantCode(rawValue) {
  const code = String(rawValue || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9_-]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");

  if (!code) {
    throw badRequest("tenantCode is required");
  }
  if (code.length > 50) {
    throw badRequest("tenantCode cannot exceed 50 characters");
  }

  return code;
}

function normalizeName(value, label, maxLength = 255) {
  const normalized = String(value || "").trim();
  if (!normalized) {
    throw badRequest(`${label} is required`);
  }
  if (normalized.length > maxLength) {
    throw badRequest(`${label} cannot exceed ${maxLength} characters`);
  }
  return normalized;
}

function normalizeEmail(value) {
  const email = String(value || "").trim().toLowerCase();
  if (!email) {
    throw badRequest("adminEmail is required");
  }
  if (email.length > 255) {
    throw badRequest("adminEmail cannot exceed 255 characters");
  }
  if (!email.includes("@") || !email.includes(".")) {
    throw badRequest("adminEmail is invalid");
  }
  return email;
}

function validatePassword(value) {
  const password = String(value || "");
  if (password.length < 8) {
    throw badRequest("adminPassword must be at least 8 characters");
  }
  if (password.length > 128) {
    throw badRequest("adminPassword cannot exceed 128 characters");
  }
  return password;
}

function normalizeTenantStatus(value) {
  const status = String(value || "").trim().toUpperCase();
  if (!TENANT_STATUSES.has(status)) {
    throw badRequest("status must be ACTIVE or SUSPENDED");
  }
  return status;
}

async function ensureTenantAdminRole(tx, tenantId) {
  const roleResult = await tx.query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = 'TenantAdmin'
     LIMIT 1`,
    [tenantId]
  );
  let roleId = parsePositiveInt(roleResult.rows[0]?.id);

  if (!roleId) {
    const insertRoleResult = await tx.query(
      `INSERT INTO roles (tenant_id, code, name, is_system)
       VALUES (?, 'TenantAdmin', 'Tenant Administrator', TRUE)`,
      [tenantId]
    );
    roleId = parsePositiveInt(insertRoleResult.rows.insertId);
  }

  if (!roleId) {
    throw new Error("Failed to initialize TenantAdmin role");
  }

  const permissionResult = await tx.query(`SELECT id FROM permissions ORDER BY id`);
  const permissionIds = (permissionResult.rows || [])
    .map((row) => parsePositiveInt(row.id))
    .filter(Boolean);

  if (permissionIds.length === 0) {
    throw badRequest(
      "Permissions catalog is empty. Run core seed before provider provisioning."
    );
  }

  for (const permissionId of permissionIds) {
    // eslint-disable-next-line no-await-in-loop
    await tx.query(
      `INSERT IGNORE INTO role_permissions (role_id, permission_id)
       VALUES (?, ?)`,
      [roleId, permissionId]
    );
  }

  return roleId;
}

async function createTenantWithAdmin(tx, input) {
  const tenantCode = normalizeTenantCode(input.tenantCode);
  const tenantName = normalizeName(input.tenantName, "tenantName", 255);
  const adminName = normalizeName(input.adminName, "adminName", 255);
  const adminEmail = normalizeEmail(input.adminEmail);
  const adminPassword = validatePassword(input.adminPassword);
  const passwordHash = await bcrypt.hash(adminPassword, 10);

  const existingTenantResult = await tx.query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [tenantCode]
  );
  if (existingTenantResult.rows[0]) {
    throw badRequest("tenantCode already exists");
  }

  const existingEmailResult = await tx.query(
    `SELECT id
     FROM users
     WHERE email = ?
     LIMIT 1`,
    [adminEmail]
  );
  if (existingEmailResult.rows[0]) {
    throw badRequest("adminEmail already exists");
  }

  const tenantInsertResult = await tx.query(
    `INSERT INTO tenants (code, name, status)
     VALUES (?, ?, 'ACTIVE')`,
    [tenantCode, tenantName]
  );
  const tenantId = parsePositiveInt(tenantInsertResult.rows.insertId);
  if (!tenantId) {
    throw new Error("Failed to create tenant");
  }

  const roleId = await ensureTenantAdminRole(tx, tenantId);

  const userInsertResult = await tx.query(
    `INSERT INTO users (
        tenant_id,
        email,
        password_hash,
        name,
        status
     )
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, adminEmail, passwordHash, adminName]
  );
  const userId = parsePositiveInt(userInsertResult.rows.insertId);
  if (!userId) {
    throw new Error("Failed to create admin user");
  }

  await tx.query(
    `INSERT INTO user_role_scopes (
        tenant_id,
        user_id,
        role_id,
        scope_type,
        scope_id,
        effect
     )
     VALUES (?, ?, ?, 'TENANT', ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE
       effect = VALUES(effect)`,
    [tenantId, userId, roleId, tenantId]
  );

  return {
    tenantId,
    tenantCode,
    tenantName,
    adminUserId: userId,
    adminEmail,
    adminName,
    adminRoleId: roleId,
  };
}

async function requireProviderAdminRecord(providerAdminId) {
  const result = await query(
    `SELECT id, email, name, status, created_at, updated_at, last_login_at
     FROM provider_admin_users
     WHERE id = ?
     LIMIT 1`,
    [providerAdminId]
  );
  const row = result.rows[0];
  if (!row) {
    const err = new Error("Provider admin not found");
    err.status = 401;
    throw err;
  }
  if (String(row.status || "").toUpperCase() !== "ACTIVE") {
    const err = new Error("Provider admin is disabled");
    err.status = 403;
    throw err;
  }
  return row;
}

router.post(
  "/auth/login",
  asyncHandler(async (req, res) => {
    requireProviderPanelEnabled();
    assertRequiredFields(req.body, ["email", "password"]);

    const email = String(req.body.email || "").trim().toLowerCase();
    const password = String(req.body.password || "");
    if (!email || !password) {
      throw badRequest("email and password are required");
    }

    const result = await query(
      `SELECT id, email, password_hash, name, status
       FROM provider_admin_users
       WHERE email = ?
       LIMIT 1`,
      [email]
    );
    const providerAdmin = result.rows[0];
    const ok = providerAdmin
      ? await bcrypt.compare(password, providerAdmin.password_hash)
      : false;
    const isActive =
      String(providerAdmin?.status || "").toUpperCase() === "ACTIVE";
    if (!providerAdmin || !ok || !isActive) {
      const err = new Error("Invalid credentials");
      err.status = 401;
      throw err;
    }

    const providerAdminId = parsePositiveInt(providerAdmin.id);
    if (!providerAdminId) {
      throw new Error("Invalid provider admin record");
    }

    const token = jwt.sign(
      {
        providerAdminId,
        email: String(providerAdmin.email || "").trim().toLowerCase(),
      },
      getProviderJwtSecret(),
      { expiresIn: "12h" }
    );

    await query(
      `UPDATE provider_admin_users
       SET last_login_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [providerAdminId]
    );

    return res.json({ token });
  })
);

router.get(
  "/me",
  requireProviderAuth,
  asyncHandler(async (req, res) => {
    const providerAdmin = await requireProviderAdminRecord(
      req.providerAdmin.providerAdminId
    );

    return res.json({
      id: parsePositiveInt(providerAdmin.id),
      email: String(providerAdmin.email || "").trim().toLowerCase(),
      name: providerAdmin.name || null,
      status: String(providerAdmin.status || "").toUpperCase(),
      createdAt: providerAdmin.created_at || null,
      updatedAt: providerAdmin.updated_at || null,
      lastLoginAt: providerAdmin.last_login_at || null,
    });
  })
);

router.get(
  "/tenants",
  requireProviderAuth,
  asyncHandler(async (req, res) => {
    await requireProviderAdminRecord(req.providerAdmin.providerAdminId);

    const q = req.query.q ? String(req.query.q).trim() : null;
    const limitRaw = Number(req.query.limit);
    const offsetRaw = Number(req.query.offset);
    const limit =
      Number.isInteger(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 200) : 50;
    const offset = Number.isInteger(offsetRaw) && offsetRaw >= 0 ? offsetRaw : 0;

    const conditions = [];
    const params = [];
    if (q) {
      conditions.push("(t.code LIKE ? OR t.name LIKE ?)");
      params.push(`%${q}%`, `%${q}%`);
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const countResult = await query(
      `SELECT COUNT(*) AS total
       FROM tenants t
       ${whereClause}`,
      params
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const rowsResult = await query(
      `SELECT
         t.id,
         t.code,
         t.name,
         t.status,
         t.created_at,
         t.updated_at,
         COUNT(u.id) AS user_count,
         SUM(CASE WHEN u.status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_user_count
       FROM tenants t
       LEFT JOIN users u ON u.tenant_id = t.id
       ${whereClause}
       GROUP BY t.id, t.code, t.name, t.status, t.created_at, t.updated_at
       ORDER BY t.id DESC
       LIMIT ${limit}
       OFFSET ${offset}`,
      params
    );

    return res.json({
      rows: rowsResult.rows.map((row) => ({
        id: parsePositiveInt(row.id),
        code: row.code || null,
        name: row.name || null,
        status: String(row.status || "").toUpperCase(),
        createdAt: row.created_at || null,
        updatedAt: row.updated_at || null,
        userCount: Number(row.user_count || 0),
        activeUserCount: Number(row.active_user_count || 0),
      })),
      total,
      limit,
      offset,
    });
  })
);

router.post(
  "/tenants",
  requireProviderAuth,
  asyncHandler(async (req, res) => {
    await requireProviderAdminRecord(req.providerAdmin.providerAdminId);
    assertRequiredFields(req.body, [
      "tenantCode",
      "tenantName",
      "adminName",
      "adminEmail",
      "adminPassword",
    ]);

    const result = await withTransaction((tx) => createTenantWithAdmin(tx, req.body));
    await invalidateRbacCache(result.tenantId);

    return res.status(201).json({
      ok: true,
      ...result,
      createdByProviderAdminId: req.providerAdmin.providerAdminId,
    });
  })
);

router.patch(
  "/tenants/:tenantId/status",
  requireProviderAuth,
  asyncHandler(async (req, res) => {
    await requireProviderAdminRecord(req.providerAdmin.providerAdminId);
    const tenantId = parsePositiveInt(req.params.tenantId);
    if (!tenantId) {
      throw badRequest("tenantId must be a positive integer");
    }

    const status = normalizeTenantStatus(req.body?.status);
    const updateResult = await query(
      `UPDATE tenants
       SET status = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [status, tenantId]
    );
    if (Number(updateResult.rows.affectedRows || 0) === 0) {
      throw badRequest("tenantId not found");
    }

    const tenantResult = await query(
      `SELECT id, code, name, status, created_at, updated_at
       FROM tenants
       WHERE id = ?
       LIMIT 1`,
      [tenantId]
    );
    const row = tenantResult.rows[0];

    return res.json({
      ok: true,
      row: {
        id: parsePositiveInt(row?.id),
        code: row?.code || null,
        name: row?.name || null,
        status: String(row?.status || "").toUpperCase(),
        createdAt: row?.created_at || null,
        updatedAt: row?.updated_at || null,
      },
    });
  })
);

router.post(
  "/tenants/bootstrap",
  asyncHandler(async (req, res) => {
    requireLegacyBootstrapEnabled();
    requireProviderKey(req);
    assertRequiredFields(req.body, [
      "tenantCode",
      "tenantName",
      "adminName",
      "adminEmail",
      "adminPassword",
    ]);

    const result = await withTransaction((tx) => createTenantWithAdmin(tx, req.body));
    await invalidateRbacCache(result.tenantId);

    return res.status(201).json({
      ok: true,
      ...result,
    });
  })
);

export default router;
