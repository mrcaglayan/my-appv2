import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CARI_PR02_TEST_PORT || 3121);
const BASE_URL =
  process.env.CARI_PR02_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;

const CARI_PERMISSION_CODES = [
  "cari.card.read",
  "cari.card.upsert",
  "cari.doc.read",
  "cari.doc.create",
  "cari.doc.update",
  "cari.doc.post",
  "cari.doc.reverse",
  "cari.settlement.apply",
  "cari.settlement.reverse",
  "cari.report.read",
  "cari.fx.override",
  "cari.audit.read",
  "cari.bank.attach",
  "cari.bank.apply",
];

const CARI_ROLE_EXPECTATIONS = {
  CountryController: [
    "cari.card.upsert",
    "cari.doc.post",
    "cari.doc.reverse",
    "cari.settlement.apply",
    "cari.settlement.reverse",
    "cari.fx.override",
    "cari.bank.apply",
  ],
  EntityAccountant: [
    "cari.card.read",
    "cari.card.upsert",
    "cari.doc.create",
    "cari.doc.post",
    "cari.doc.reverse",
    "cari.settlement.apply",
    "cari.settlement.reverse",
    "cari.bank.attach",
    "cari.bank.apply",
  ],
  AuditorReadOnly: [
    "cari.card.read",
    "cari.doc.read",
    "cari.report.read",
    "cari.audit.read",
  ],
};

const CARI_MENU_EXPECTATIONS = {
  "/app/alici-kart-olustur": "cari.card.upsert",
  "/app/alici-kart-listesi": "cari.card.read",
  "/app/satici-kart-olustur": "cari.card.upsert",
  "/app/satici-kart-listesi": "cari.card.read",
};

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function makeInClause(items) {
  if (!Array.isArray(items) || items.length === 0) {
    throw new Error("makeInClause expects a non-empty array");
  }
  return items.map(() => "?").join(", ");
}

async function apiRequest({
  token,
  method = "GET",
  path: requestPath,
  body,
  expectedStatus,
}) {
  const headers = { "Content-Type": "application/json" };
  if (token) {
    headers.Cookie = token;
  }

  const response = await fetch(`${BASE_URL}${requestPath}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  let json = null;
  try {
    json = await response.json();
  } catch {
    json = null;
  }

  const setCookieHeader = response.headers.get("set-cookie");
  const cookie = setCookieHeader
    ? String(setCookieHeader).split(";")[0].trim()
    : null;

  if (expectedStatus !== undefined && response.status !== expectedStatus) {
    throw new Error(
      `${method} ${requestPath} expected ${expectedStatus}, got ${response.status}. response=${JSON.stringify(
        json
      )}`
    );
  }

  return { status: response.status, json, cookie };
}

function startServerProcess() {
  const child = spawn(process.execPath, ["src/index.js"], {
    cwd: process.cwd(),
    env: { ...process.env, PORT: String(PORT) },
    stdio: ["ignore", "pipe", "pipe"],
  });

  child.stdout.on("data", (chunk) => {
    process.stdout.write(`[server] ${chunk}`);
  });
  child.stderr.on("data", (chunk) => {
    process.stderr.write(`[server] ${chunk}`);
  });

  return child;
}

async function waitForServer() {
  const startedAt = Date.now();
  while (Date.now() - startedAt < SERVER_START_TIMEOUT_MS) {
    try {
      const response = await fetch(`${BASE_URL}/health`);
      if (response.ok) {
        return;
      }
    } catch {
      // wait for start
    }
    await sleep(350);
  }
  throw new Error(`Server did not start within ${SERVER_START_TIMEOUT_MS}ms`);
}

async function login(email, password) {
  const response = await apiRequest({
    method: "POST",
    path: "/auth/login",
    body: { email, password },
    expectedStatus: 200,
  });
  const sessionCookie = response.cookie;
  assert(Boolean(sessionCookie), `Login cookie missing for ${email}`);
  return sessionCookie;
}

async function assertCariPermissionsSeededOnce() {
  await seedCore({ ensureDefaultTenantIfMissing: true });
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const permissionResult = await query(
    `SELECT code, COUNT(*) AS row_count
     FROM permissions
     WHERE code IN (${makeInClause(CARI_PERMISSION_CODES)})
     GROUP BY code`,
    CARI_PERMISSION_CODES
  );

  const countByCode = new Map(
    permissionResult.rows.map((row) => [String(row.code), toNumber(row.row_count)])
  );
  for (const code of CARI_PERMISSION_CODES) {
    assert(countByCode.has(code), `Missing seeded permission: ${code}`);
    assert(
      countByCode.get(code) === 1,
      `Permission ${code} should be inserted once; got ${countByCode.get(code)}`
    );
  }
}

async function createTenant() {
  const stamp = Date.now();
  const tenantCode = `CARI_PR02_${stamp}`;

  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE name = VALUES(name)`,
    [tenantCode, `CARI PR02 ${stamp}`]
  );

  await seedCore({ ensureDefaultTenantIfMissing: true });
  await seedCore({ ensureDefaultTenantIfMissing: true });

  const tenantResult = await query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [tenantCode]
  );
  const tenantId = toNumber(tenantResult.rows[0]?.id);
  assert(tenantId > 0, "Failed to resolve tenantId for PR-02 tests");
  return { tenantId, stamp };
}

async function getCariPermissionCountsByRole(tenantId) {
  const result = await query(
    `SELECT r.code AS role_code, COUNT(*) AS permission_count
     FROM roles r
     JOIN role_permissions rp ON rp.role_id = r.id
     JOIN permissions p ON p.id = rp.permission_id
     WHERE r.tenant_id = ?
       AND p.code LIKE 'cari.%'
     GROUP BY r.code`,
    [tenantId]
  );

  return new Map(
    result.rows.map((row) => [String(row.role_code), toNumber(row.permission_count)])
  );
}

async function assertRolePermissionMappings(tenantId) {
  const before = await getCariPermissionCountsByRole(tenantId);
  await seedCore({ ensureDefaultTenantIfMissing: true });
  const after = await getCariPermissionCountsByRole(tenantId);

  for (const [roleCode, beforeCount] of before.entries()) {
    assert(
      after.get(roleCode) === beforeCount,
      `Role permission count changed after reseed for ${roleCode}; before=${beforeCount} after=${after.get(
        roleCode
      )}`
    );
  }

  for (const [roleCode, expectedPermissionCodes] of Object.entries(
    CARI_ROLE_EXPECTATIONS
  )) {
    const rolePermissionRows = await query(
      `SELECT p.code
       FROM roles r
       JOIN role_permissions rp ON rp.role_id = r.id
       JOIN permissions p ON p.id = rp.permission_id
       WHERE r.tenant_id = ?
         AND r.code = ?
       ORDER BY p.code`,
      [tenantId, roleCode]
    );
    const permissionSet = new Set(
      (rolePermissionRows.rows || []).map((row) => String(row.code))
    );
    for (const permissionCode of expectedPermissionCodes) {
      assert(
        permissionSet.has(permissionCode),
        `Role ${roleCode} is missing mapped permission ${permissionCode}`
      );
    }
  }
}

async function createUsersAndAssignments({ tenantId, stamp }) {
  const password = "CariPR02#12345";
  const passwordHash = await bcrypt.hash(password, 10);
  const allowedEmail = `cari_pr02_allowed_${stamp}@example.com`;
  const deniedEmail = `cari_pr02_denied_${stamp}@example.com`;

  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, allowedEmail, passwordHash, "CARI PR02 Allowed"]
  );
  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, deniedEmail, passwordHash, "CARI PR02 Denied"]
  );

  const userRows = await query(
    `SELECT id, email
     FROM users
     WHERE tenant_id = ?
       AND email IN (?, ?)`,
    [tenantId, allowedEmail, deniedEmail]
  );
  const userIdByEmail = new Map(
    userRows.rows.map((row) => [String(row.email), toNumber(row.id)])
  );

  const allowedUserId = userIdByEmail.get(allowedEmail);
  const deniedUserId = userIdByEmail.get(deniedEmail);
  assert(allowedUserId > 0, "Failed to resolve allowed user id");
  assert(deniedUserId > 0, "Failed to resolve denied user id");

  const roleRows = await query(
    `SELECT id, code
     FROM roles
     WHERE tenant_id = ?
       AND code IN ('EntityAccountant', 'AuditorReadOnly')`,
    [tenantId]
  );
  const roleIdByCode = new Map(
    roleRows.rows.map((row) => [String(row.code), toNumber(row.id)])
  );

  const entityAccountantRoleId = roleIdByCode.get("EntityAccountant");
  const auditorRoleId = roleIdByCode.get("AuditorReadOnly");
  assert(entityAccountantRoleId > 0, "EntityAccountant role not found");
  assert(auditorRoleId > 0, "AuditorReadOnly role not found");

  await query(
    `INSERT INTO user_role_scopes (
        tenant_id, user_id, role_id, scope_type, scope_id, effect
      )
      VALUES
        (?, ?, ?, 'TENANT', ?, 'ALLOW'),
        (?, ?, ?, 'TENANT', ?, 'ALLOW')
      ON DUPLICATE KEY UPDATE effect = VALUES(effect)`,
    [
      tenantId,
      allowedUserId,
      entityAccountantRoleId,
      tenantId,
      tenantId,
      deniedUserId,
      auditorRoleId,
      tenantId,
    ]
  );

  return {
    password,
    allowedEmail,
    deniedEmail,
  };
}

async function runApiPermissionAssertions(identity) {
  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const allowedToken = await login(identity.allowedEmail, identity.password);
    const deniedToken = await login(identity.deniedEmail, identity.password);

    await apiRequest({
      token: allowedToken,
      method: "POST",
      path: "/api/v1/cari/cards",
      body: {},
      expectedStatus: 200,
    });
    await apiRequest({
      token: deniedToken,
      method: "POST",
      path: "/api/v1/cari/cards",
      body: {},
      expectedStatus: 403,
    });

    await apiRequest({
      token: deniedToken,
      method: "GET",
      path: "/api/v1/cari/audit",
      expectedStatus: 200,
    });
    await apiRequest({
      token: allowedToken,
      method: "GET",
      path: "/api/v1/cari/audit",
      expectedStatus: 403,
    });
  } finally {
    if (!serverStopped) {
      server.kill("SIGTERM");
      serverStopped = true;
    }
  }
}

async function runFrontendGuardSmokeAssertions() {
  const sidebarModule = await import("../../frontend/src/layouts/sidebarConfig.js");
  const links = sidebarModule.collectSidebarLinks(sidebarModule.sidebarItems);
  const linkByPath = new Map(links.map((row) => [String(row.to), row]));

  for (const [routePath, expectedPermission] of Object.entries(
    CARI_MENU_EXPECTATIONS
  )) {
    const link = linkByPath.get(routePath);
    assert(link, `Sidebar link missing for ${routePath}`);
    assert(
      link.implemented === true,
      `Sidebar link should be implemented for ${routePath}`
    );

    const requiredPermissions = Array.isArray(link.requiredPermissions)
      ? link.requiredPermissions
      : [];
    assert(
      requiredPermissions.includes(expectedPermission),
      `Sidebar link ${routePath} is missing required permission ${expectedPermission}`
    );
  }

  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  const appFilePath = path.resolve(scriptDir, "../../frontend/src/App.jsx");
  const appSource = await readFile(appFilePath, "utf8");

  for (const routePath of Object.keys(CARI_MENU_EXPECTATIONS)) {
    assert(
      appSource.includes(`appPath: "${routePath}"`),
      `App route is not implemented for ${routePath}`
    );
  }
  assert(
    appSource.includes("element={withPermissionGuard(route.appPath, route.element)}"),
    "App routes should be wrapped with permission guard"
  );
}

async function main() {
  await assertCariPermissionsSeededOnce();
  const tenant = await createTenant();
  await assertRolePermissionMappings(tenant.tenantId);

  const identities = await createUsersAndAssignments({
    tenantId: tenant.tenantId,
    stamp: tenant.stamp,
  });
  await runApiPermissionAssertions(identities);

  await runFrontendGuardSmokeAssertions();

  console.log("CARI PR-02 RBAC + seed + guard test passed.");
  console.log(
    JSON.stringify(
      {
        tenantId: tenant.tenantId,
        checkedPermissionCount: CARI_PERMISSION_CODES.length,
        checkedRoleCount: Object.keys(CARI_ROLE_EXPECTATIONS).length,
        checkedMenuRouteCount: Object.keys(CARI_MENU_EXPECTATIONS).length,
      },
      null,
      2
    )
  );
}

main()
  .then(async () => {
    await closePool();
    process.exit(0);
  })
  .catch(async (err) => {
    console.error(err);
    try {
      await closePool();
    } catch {
      // ignore close failures
    }
    process.exit(1);
  });
