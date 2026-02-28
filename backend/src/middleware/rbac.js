import { query } from "../db.js";
import { createClient } from "redis";
import { logWarn } from "../observability/logger.js";
import { parsePositiveInt, resolveTenantId } from "../routes/_utils.js";

const VALID_SCOPE_TYPES = new Set([
  "TENANT",
  "GROUP",
  "COUNTRY",
  "LEGAL_ENTITY",
  "OPERATING_UNIT",
]);

const SCOPE_KIND_TO_KEY = {
  group: "groups",
  country: "countries",
  legal_entity: "legalEntities",
  operating_unit: "operatingUnits",
};
const RBAC_CACHE_TTL_MS = parsePositiveIntEnv(process.env.RBAC_CACHE_TTL_MS, 30_000);
const RBAC_HIERARCHY_CACHE_TTL_MS = parsePositiveIntEnv(
  process.env.RBAC_HIERARCHY_CACHE_TTL_MS,
  60_000
);
const RBAC_CACHE_MAX_ENTRIES = parsePositiveIntEnv(
  process.env.RBAC_CACHE_MAX_ENTRIES,
  10_000
);
const RBAC_CACHE_STORE_MODE = normalizeCacheStoreMode(process.env.RBAC_CACHE_STORE);
const RBAC_CACHE_REDIS_URL = String(
  process.env.RBAC_CACHE_REDIS_URL || process.env.REDIS_URL || ""
).trim();
const RBAC_CACHE_REDIS_CONNECT_TIMEOUT_MS = parsePositiveIntEnv(
  process.env.RBAC_CACHE_REDIS_CONNECT_TIMEOUT_MS,
  1_500
);
const RBAC_REDIS_LOG_COOLDOWN_MS = 60 * 1000;
const MEMORY_TENANT_VERSION_TTL_MS = parsePositiveIntEnv(
  process.env.RBAC_CACHE_TENANT_VERSION_TTL_MS,
  5_000
);

const memoryPermissionBundleCache = new Map();
const memoryHierarchyCache = new Map();
const memoryTenantVersionCache = new Map();
const memoryTenantVersionExpiryCache = new Map();

let rbacRedisClient = null;
let rbacRedisConnectPromise = null;
let resolvedRbacCacheBackend = null; // "redis" | "memory"
let lastRedisErrorLogAt = 0;

function parsePositiveIntEnv(value, fallback) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function normalizeCacheStoreMode(value) {
  const normalized = String(value || "auto")
    .trim()
    .toLowerCase();
  if (["auto", "redis", "memory"].includes(normalized)) {
    return normalized;
  }
  return "auto";
}

function shouldAttemptRbacRedis() {
  if (RBAC_CACHE_STORE_MODE === "memory") {
    return false;
  }
  return Boolean(RBAC_CACHE_REDIS_URL);
}

function logRbacRedisWarn(message, err = null) {
  const now = Date.now();
  if (now - lastRedisErrorLogAt < RBAC_REDIS_LOG_COOLDOWN_MS) {
    return;
  }
  lastRedisErrorLogAt = now;
  logWarn("[rbac-cache] Redis warning", { detail: message }, err || null);
}

async function connectRbacRedisClient() {
  if (!shouldAttemptRbacRedis()) {
    return null;
  }

  if (rbacRedisClient?.isOpen) {
    return rbacRedisClient;
  }

  if (!rbacRedisConnectPromise) {
    rbacRedisConnectPromise = (async () => {
      const client = createClient({
        url: RBAC_CACHE_REDIS_URL,
        socket: {
          connectTimeout: RBAC_CACHE_REDIS_CONNECT_TIMEOUT_MS,
          reconnectStrategy: () => false,
        },
      });
      client.on("error", (err) => {
        logRbacRedisWarn("Redis client error. Falling back to in-memory RBAC cache.", err);
      });

      try {
        await client.connect();
        rbacRedisClient = client;
        return rbacRedisClient;
      } catch (err) {
        try {
          if (client.isOpen) {
            await client.quit();
          }
        } catch {
          // Ignore redis disconnect failures.
        }
        logRbacRedisWarn("Could not connect to Redis. Falling back to in-memory RBAC cache.", err);
        return null;
      }
    })();
  }

  try {
    return await rbacRedisConnectPromise;
  } finally {
    rbacRedisConnectPromise = null;
  }
}

function useMemoryCacheFallback(err = null) {
  resolvedRbacCacheBackend = "memory";
  if (err) {
    logRbacRedisWarn("Redis operation failed. Using in-memory RBAC cache.", err);
  }
}

async function resolveRbacCacheBackend() {
  if (resolvedRbacCacheBackend) {
    return resolvedRbacCacheBackend;
  }

  if (RBAC_CACHE_STORE_MODE === "memory") {
    resolvedRbacCacheBackend = "memory";
    return resolvedRbacCacheBackend;
  }

  if (!shouldAttemptRbacRedis()) {
    resolvedRbacCacheBackend = "memory";
    return resolvedRbacCacheBackend;
  }

  const client = await connectRbacRedisClient();
  if (client) {
    resolvedRbacCacheBackend = "redis";
    return resolvedRbacCacheBackend;
  }

  resolvedRbacCacheBackend = "memory";
  return resolvedRbacCacheBackend;
}

function getMemoryCacheEntry(cache, key) {
  const item = cache.get(key);
  if (!item) {
    return null;
  }
  if (Number(item.expiresAt || 0) <= Date.now()) {
    cache.delete(key);
    return null;
  }
  return item.value;
}

function pruneMemoryCache(cache) {
  const now = Date.now();
  for (const [key, item] of cache.entries()) {
    if (Number(item?.expiresAt || 0) <= now) {
      cache.delete(key);
    }
  }

  if (cache.size <= RBAC_CACHE_MAX_ENTRIES) {
    return;
  }

  const overflow = cache.size - RBAC_CACHE_MAX_ENTRIES;
  const oldestKeys = Array.from(cache.entries())
    .sort((a, b) => Number(a[1]?.updatedAt || 0) - Number(b[1]?.updatedAt || 0))
    .slice(0, overflow)
    .map(([key]) => key);

  for (const key of oldestKeys) {
    cache.delete(key);
  }
}

function setMemoryCacheEntry(cache, key, value, ttlMs) {
  const now = Date.now();
  cache.set(key, {
    value,
    updatedAt: now,
    expiresAt: now + ttlMs,
  });
  pruneMemoryCache(cache);
}

function toPositiveVersion(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return 1;
  }
  return parsed;
}

function setMemoryTenantVersion(tenantId, version) {
  memoryTenantVersionCache.set(tenantId, version);
  memoryTenantVersionExpiryCache.set(tenantId, Date.now() + MEMORY_TENANT_VERSION_TTL_MS);
}

function getMemoryTenantVersion(tenantId) {
  const expiresAt = Number(memoryTenantVersionExpiryCache.get(tenantId) || 0);
  if (expiresAt > Date.now()) {
    return toPositiveVersion(memoryTenantVersionCache.get(tenantId));
  }
  return null;
}

function buildTenantVersionRedisKey(tenantId) {
  return `rbac:version:tenant:${tenantId}`;
}

function buildPermissionBundleCacheKey(tenantId, userId, permissionCode, tenantVersion) {
  return `rbac:perm:v${tenantVersion}:t${tenantId}:u${userId}:p:${permissionCode}`;
}

function buildHierarchyCacheKey(tenantId, tenantVersion) {
  return `rbac:hier:v${tenantVersion}:t${tenantId}`;
}

function purgeTenantScopedMemoryEntries(tenantId) {
  const tenantMarker = `:t${tenantId}:`;
  for (const key of memoryPermissionBundleCache.keys()) {
    if (key.includes(tenantMarker)) {
      memoryPermissionBundleCache.delete(key);
    }
  }
  for (const key of memoryHierarchyCache.keys()) {
    if (key.includes(tenantMarker)) {
      memoryHierarchyCache.delete(key);
    }
  }
}

function serializeScopeContext(scopeContext) {
  if (!scopeContext) {
    return null;
  }
  return {
    tenantId: parsePositiveInt(scopeContext.tenantId),
    sourceRows: Number(scopeContext.sourceRows || 0),
    tenantWide: Boolean(scopeContext.tenantWide),
    groups: Array.from(scopeContext.groups || []),
    countries: Array.from(scopeContext.countries || []),
    legalEntities: Array.from(scopeContext.legalEntities || []),
    operatingUnits: Array.from(scopeContext.operatingUnits || []),
  };
}

function hydrateScopeContext(payload) {
  if (!payload) {
    return null;
  }
  return {
    tenantId: parsePositiveInt(payload.tenantId),
    sourceRows: Number(payload.sourceRows || 0),
    tenantWide: Boolean(payload.tenantWide),
    groups: new Set((payload.groups || []).map((id) => parsePositiveInt(id)).filter(Boolean)),
    countries: new Set(
      (payload.countries || []).map((id) => parsePositiveInt(id)).filter(Boolean)
    ),
    legalEntities: new Set(
      (payload.legalEntities || []).map((id) => parsePositiveInt(id)).filter(Boolean)
    ),
    operatingUnits: new Set(
      (payload.operatingUnits || []).map((id) => parsePositiveInt(id)).filter(Boolean)
    ),
  };
}

function serializeHierarchy(hierarchy) {
  if (!hierarchy) {
    return null;
  }
  return {
    groupIds: Array.from(hierarchy.groupIds || []),
    countryIds: Array.from(hierarchy.countryIds || []),
    legalEntityIds: Array.from(hierarchy.legalEntityIds || []),
    operatingUnitIds: Array.from(hierarchy.operatingUnitIds || []),
    entityById: Array.from(hierarchy.entityById?.entries() || []),
    legalEntityIdsByGroupId: Array.from(
      hierarchy.legalEntityIdsByGroupId?.entries() || []
    ).map(([key, values]) => [key, Array.from(values || [])]),
    legalEntityIdsByCountryId: Array.from(
      hierarchy.legalEntityIdsByCountryId?.entries() || []
    ).map(([key, values]) => [key, Array.from(values || [])]),
    operatingUnitIdsByLegalEntityId: Array.from(
      hierarchy.operatingUnitIdsByLegalEntityId?.entries() || []
    ).map(([key, values]) => [key, Array.from(values || [])]),
  };
}

function hydrateHierarchy(payload) {
  if (!payload) {
    return null;
  }

  const entityById = new Map();
  for (const row of payload.entityById || []) {
    const id = parsePositiveInt(row?.[0]);
    const value = row?.[1] || {};
    const groupId = parsePositiveInt(value.groupId);
    const countryId = parsePositiveInt(value.countryId);
    if (!id || !groupId || !countryId) {
      continue;
    }
    entityById.set(id, { id, groupId, countryId });
  }

  const legalEntityIdsByGroupId = new Map();
  for (const row of payload.legalEntityIdsByGroupId || []) {
    const key = parsePositiveInt(row?.[0]);
    if (!key) {
      continue;
    }
    legalEntityIdsByGroupId.set(
      key,
      new Set((row?.[1] || []).map((id) => parsePositiveInt(id)).filter(Boolean))
    );
  }

  const legalEntityIdsByCountryId = new Map();
  for (const row of payload.legalEntityIdsByCountryId || []) {
    const key = parsePositiveInt(row?.[0]);
    if (!key) {
      continue;
    }
    legalEntityIdsByCountryId.set(
      key,
      new Set((row?.[1] || []).map((id) => parsePositiveInt(id)).filter(Boolean))
    );
  }

  const operatingUnitIdsByLegalEntityId = new Map();
  for (const row of payload.operatingUnitIdsByLegalEntityId || []) {
    const key = parsePositiveInt(row?.[0]);
    if (!key) {
      continue;
    }
    operatingUnitIdsByLegalEntityId.set(
      key,
      new Set((row?.[1] || []).map((id) => parsePositiveInt(id)).filter(Boolean))
    );
  }

  return {
    groupIds: new Set((payload.groupIds || []).map((id) => parsePositiveInt(id)).filter(Boolean)),
    countryIds: new Set(
      (payload.countryIds || []).map((id) => parsePositiveInt(id)).filter(Boolean)
    ),
    legalEntityIds: new Set(
      (payload.legalEntityIds || []).map((id) => parsePositiveInt(id)).filter(Boolean)
    ),
    operatingUnitIds: new Set(
      (payload.operatingUnitIds || []).map((id) => parsePositiveInt(id)).filter(Boolean)
    ),
    entityById,
    legalEntityIdsByGroupId,
    legalEntityIdsByCountryId,
    operatingUnitIdsByLegalEntityId,
  };
}

function serializePermissionBundle(bundle) {
  return {
    missingPermission: Boolean(bundle?.missingPermission),
    source: String(bundle?.source || ""),
    permissionScopeContext: serializeScopeContext(bundle?.permissionScopeContext),
    scopeContext: serializeScopeContext(bundle?.scopeContext),
  };
}

function hydratePermissionBundle(payload) {
  if (!payload) {
    return null;
  }
  return {
    missingPermission: Boolean(payload.missingPermission),
    source: String(payload.source || ""),
    permissionScopeContext: hydrateScopeContext(payload.permissionScopeContext),
    scopeContext: hydrateScopeContext(payload.scopeContext),
  };
}

async function getRedisClientForCache() {
  const backend = await resolveRbacCacheBackend();
  if (backend !== "redis") {
    return null;
  }
  return connectRbacRedisClient();
}

async function getTenantCacheVersion(tenantId) {
  const localVersion = getMemoryTenantVersion(tenantId);
  if (localVersion) {
    return localVersion;
  }

  const client = await getRedisClientForCache();
  if (client) {
    try {
      const versionKey = buildTenantVersionRedisKey(tenantId);
      const raw = await client.get(versionKey);
      if (raw) {
        const parsed = toPositiveVersion(raw);
        setMemoryTenantVersion(tenantId, parsed);
        return parsed;
      }
      await client.set(versionKey, "1", { NX: true });
      setMemoryTenantVersion(tenantId, 1);
      return 1;
    } catch (err) {
      useMemoryCacheFallback(err);
    }
  }

  const current = toPositiveVersion(memoryTenantVersionCache.get(tenantId));
  setMemoryTenantVersion(tenantId, current);
  return current;
}

async function getCachedJson(cache, key, ttlMs) {
  const memoryHit = getMemoryCacheEntry(cache, key);
  if (memoryHit) {
    return memoryHit;
  }

  const client = await getRedisClientForCache();
  if (!client) {
    return null;
  }

  try {
    const raw = await client.get(key);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw);
    setMemoryCacheEntry(cache, key, parsed, ttlMs);
    return parsed;
  } catch (err) {
    useMemoryCacheFallback(err);
    return null;
  }
}

async function setCachedJson(cache, key, value, ttlMs) {
  setMemoryCacheEntry(cache, key, value, ttlMs);

  const client = await getRedisClientForCache();
  if (!client) {
    return;
  }

  try {
    await client.set(key, JSON.stringify(value), { PX: ttlMs });
  } catch (err) {
    useMemoryCacheFallback(err);
  }
}

async function loadCachedHierarchy(tenantId, tenantVersion) {
  const hierarchyCacheKey = buildHierarchyCacheKey(tenantId, tenantVersion);
  const cached = await getCachedJson(
    memoryHierarchyCache,
    hierarchyCacheKey,
    RBAC_HIERARCHY_CACHE_TTL_MS
  );
  if (cached) {
    const hydrated = hydrateHierarchy(cached);
    if (hydrated) {
      return hydrated;
    }
  }

  const fresh = await loadHierarchyFromDb(tenantId);
  await setCachedJson(
    memoryHierarchyCache,
    hierarchyCacheKey,
    serializeHierarchy(fresh),
    RBAC_HIERARCHY_CACHE_TTL_MS
  );
  return fresh;
}

function getRequestPermissionBundleCache(req) {
  if (!req._rbacPermissionBundleCache) {
    req._rbacPermissionBundleCache = new Map();
  }
  return req._rbacPermissionBundleCache;
}

async function loadPermissionBundle(userId, tenantId, permissionCode, tenantVersion) {
  const permissionCacheKey = buildPermissionBundleCacheKey(
    tenantId,
    userId,
    permissionCode,
    tenantVersion
  );

  const cached = await getCachedJson(
    memoryPermissionBundleCache,
    permissionCacheKey,
    RBAC_CACHE_TTL_MS
  );
  const hydratedCached = hydratePermissionBundle(cached);
  if (hydratedCached) {
    return hydratedCached;
  }

  const permissionResult = await query(
    `SELECT urs.effect, urs.scope_type, urs.scope_id
     FROM user_role_scopes urs
     JOIN roles r ON r.id = urs.role_id
     JOIN role_permissions rp ON rp.role_id = r.id
     JOIN permissions p ON p.id = rp.permission_id
     WHERE urs.user_id = ?
       AND urs.tenant_id = ?
       AND p.code = ?`,
    [userId, tenantId, permissionCode]
  );

  const permissionRows = permissionResult.rows || [];
  if (permissionRows.length === 0) {
    const missingBundle = {
      missingPermission: true,
      source: "",
      permissionScopeContext: null,
      scopeContext: null,
    };
    await setCachedJson(
      memoryPermissionBundleCache,
      permissionCacheKey,
      serializePermissionBundle(missingBundle),
      RBAC_CACHE_TTL_MS
    );
    return missingBundle;
  }

  const [hierarchy, dataScopeRows] = await Promise.all([
    loadCachedHierarchy(tenantId, tenantVersion),
    getUserDataScopeRows(userId, tenantId),
  ]);

  const permissionScopeContext = buildScopeContext(tenantId, permissionRows, hierarchy);
  const scopeRowsForData = dataScopeRows.length > 0 ? dataScopeRows : permissionRows;
  const scopeContext = buildScopeContext(tenantId, scopeRowsForData, hierarchy);

  const bundle = {
    missingPermission: false,
    source: dataScopeRows.length > 0 ? "data_scopes" : "permission_scopes",
    permissionScopeContext,
    scopeContext,
  };

  await setCachedJson(
    memoryPermissionBundleCache,
    permissionCacheKey,
    serializePermissionBundle(bundle),
    RBAC_CACHE_TTL_MS
  );
  return bundle;
}

async function getPermissionBundleForRequest(req, userId, tenantId, permissionCode) {
  const tenantVersion = await getTenantCacheVersion(tenantId);
  const requestCacheKey = `${tenantVersion}:${tenantId}:${userId}:${permissionCode}`;
  const requestCache = getRequestPermissionBundleCache(req);
  if (requestCache.has(requestCacheKey)) {
    return requestCache.get(requestCacheKey);
  }

  const bundle = await loadPermissionBundle(userId, tenantId, permissionCode, tenantVersion);
  requestCache.set(requestCacheKey, bundle);
  return bundle;
}

function badRequest(message) {
  const err = new Error(message);
  err.status = 400;
  return err;
}

function forbidden(message) {
  const err = new Error(message);
  err.status = 403;
  return err;
}

function normalizeScope(scope, tenantId) {
  if (!scope) {
    return null;
  }

  const scopeType = String(scope.scopeType || "").toUpperCase();
  const scopeId = parsePositiveInt(scope.scopeId);

  if (!VALID_SCOPE_TYPES.has(scopeType)) {
    throw badRequest(`Invalid RBAC scopeType: ${scopeType}`);
  }
  if (!scopeId) {
    throw badRequest("RBAC scopeId must be a positive integer");
  }
  if (scopeType === "TENANT" && scopeId !== tenantId) {
    throw forbidden("Tenant scope does not match authenticated tenant");
  }

  return { scopeType, scopeId };
}

function addScopeId(set, value) {
  const parsed = parsePositiveInt(value);
  if (parsed) {
    set.add(parsed);
  }
}

function parseScopeRows(rows) {
  const allow = {
    tenant: false,
    groups: new Set(),
    countries: new Set(),
    legalEntities: new Set(),
    operatingUnits: new Set(),
  };
  const deny = {
    tenant: false,
    groups: new Set(),
    countries: new Set(),
    legalEntities: new Set(),
    operatingUnits: new Set(),
  };

  for (const row of rows) {
    const effect = String(row.effect || "").toUpperCase();
    const scopeType = String(row.scope_type || "").toUpperCase();
    const scopeId = parsePositiveInt(row.scope_id);
    if (!VALID_SCOPE_TYPES.has(scopeType) || !scopeId || !["ALLOW", "DENY"].includes(effect)) {
      continue;
    }

    const target = effect === "ALLOW" ? allow : deny;

    if (scopeType === "TENANT") {
      target.tenant = true;
      continue;
    }
    if (scopeType === "GROUP") {
      target.groups.add(scopeId);
      continue;
    }
    if (scopeType === "COUNTRY") {
      target.countries.add(scopeId);
      continue;
    }
    if (scopeType === "LEGAL_ENTITY") {
      target.legalEntities.add(scopeId);
      continue;
    }
    if (scopeType === "OPERATING_UNIT") {
      target.operatingUnits.add(scopeId);
    }
  }

  return { allow, deny };
}

async function loadHierarchyFromDb(tenantId) {
  const [groupResult, entityResult, unitResult] = await Promise.all([
    query("SELECT id FROM group_companies WHERE tenant_id = ?", [tenantId]),
    query(
      `SELECT id, group_company_id, country_id
       FROM legal_entities
       WHERE tenant_id = ?`,
      [tenantId]
    ),
    query(
      `SELECT id, legal_entity_id
       FROM operating_units
       WHERE tenant_id = ?`,
      [tenantId]
    ),
  ]);

  const groupIds = new Set();
  const countryIds = new Set();
  const legalEntityIds = new Set();
  const operatingUnitIds = new Set();

  const entityById = new Map();
  const legalEntityIdsByGroupId = new Map();
  const legalEntityIdsByCountryId = new Map();
  const operatingUnitIdsByLegalEntityId = new Map();

  for (const row of groupResult.rows) {
    addScopeId(groupIds, row.id);
  }

  for (const row of entityResult.rows) {
    const id = parsePositiveInt(row.id);
    const groupId = parsePositiveInt(row.group_company_id);
    const countryId = parsePositiveInt(row.country_id);
    if (!id || !groupId || !countryId) {
      continue;
    }

    legalEntityIds.add(id);
    groupIds.add(groupId);
    countryIds.add(countryId);
    entityById.set(id, { id, groupId, countryId });

    if (!legalEntityIdsByGroupId.has(groupId)) {
      legalEntityIdsByGroupId.set(groupId, new Set());
    }
    legalEntityIdsByGroupId.get(groupId).add(id);

    if (!legalEntityIdsByCountryId.has(countryId)) {
      legalEntityIdsByCountryId.set(countryId, new Set());
    }
    legalEntityIdsByCountryId.get(countryId).add(id);
  }

  for (const row of unitResult.rows) {
    const id = parsePositiveInt(row.id);
    const legalEntityId = parsePositiveInt(row.legal_entity_id);
    if (!id || !legalEntityId) {
      continue;
    }

    operatingUnitIds.add(id);
    if (!operatingUnitIdsByLegalEntityId.has(legalEntityId)) {
      operatingUnitIdsByLegalEntityId.set(legalEntityId, new Set());
    }
    operatingUnitIdsByLegalEntityId.get(legalEntityId).add(id);
  }

  return {
    groupIds,
    countryIds,
    legalEntityIds,
    operatingUnitIds,
    entityById,
    legalEntityIdsByGroupId,
    legalEntityIdsByCountryId,
    operatingUnitIdsByLegalEntityId,
  };
}

function mergeSet(target, source) {
  for (const value of source) {
    target.add(value);
  }
}

function removeSet(target, source) {
  for (const value of source) {
    target.delete(value);
  }
}

function buildScopeContext(tenantId, scopeRows, hierarchy) {
  const { allow, deny } = parseScopeRows(scopeRows);

  if (deny.tenant) {
    return {
      tenantId,
      sourceRows: scopeRows.length,
      tenantWide: false,
      groups: new Set(),
      countries: new Set(),
      legalEntities: new Set(),
      operatingUnits: new Set(),
    };
  }

  const groups = new Set();
  const countries = new Set();
  const legalEntities = new Set();
  const operatingUnits = new Set();

  if (allow.tenant) {
    mergeSet(groups, hierarchy.groupIds);
    mergeSet(countries, hierarchy.countryIds);
    mergeSet(legalEntities, hierarchy.legalEntityIds);
    mergeSet(operatingUnits, hierarchy.operatingUnitIds);
  }

  mergeSet(groups, allow.groups);
  mergeSet(countries, allow.countries);
  mergeSet(legalEntities, allow.legalEntities);
  mergeSet(operatingUnits, allow.operatingUnits);

  for (const groupId of allow.groups) {
    const entityIds = hierarchy.legalEntityIdsByGroupId.get(groupId);
    if (entityIds) {
      mergeSet(legalEntities, entityIds);
    }
  }
  for (const countryId of allow.countries) {
    const entityIds = hierarchy.legalEntityIdsByCountryId.get(countryId);
    if (entityIds) {
      mergeSet(legalEntities, entityIds);
    }
  }
  for (const legalEntityId of legalEntities) {
    const unitIds = hierarchy.operatingUnitIdsByLegalEntityId.get(legalEntityId);
    if (unitIds) {
      mergeSet(operatingUnits, unitIds);
    }
  }

  removeSet(groups, deny.groups);
  removeSet(countries, deny.countries);
  removeSet(legalEntities, deny.legalEntities);
  removeSet(operatingUnits, deny.operatingUnits);

  for (const groupId of deny.groups) {
    const entityIds = hierarchy.legalEntityIdsByGroupId.get(groupId);
    if (entityIds) {
      removeSet(legalEntities, entityIds);
      for (const entityId of entityIds) {
        const unitIds = hierarchy.operatingUnitIdsByLegalEntityId.get(entityId);
        if (unitIds) {
          removeSet(operatingUnits, unitIds);
        }
      }
    }
  }

  for (const countryId of deny.countries) {
    const entityIds = hierarchy.legalEntityIdsByCountryId.get(countryId);
    if (entityIds) {
      removeSet(legalEntities, entityIds);
      for (const entityId of entityIds) {
        const unitIds = hierarchy.operatingUnitIdsByLegalEntityId.get(entityId);
        if (unitIds) {
          removeSet(operatingUnits, unitIds);
        }
      }
    }
  }

  for (const legalEntityId of deny.legalEntities) {
    const unitIds = hierarchy.operatingUnitIdsByLegalEntityId.get(legalEntityId);
    if (unitIds) {
      removeSet(operatingUnits, unitIds);
    }
  }

  for (const legalEntityId of legalEntities) {
    const entity = hierarchy.entityById.get(legalEntityId);
    if (entity) {
      addScopeId(groups, entity.groupId);
      addScopeId(countries, entity.countryId);
    }
  }

  const tenantWide =
    allow.tenant &&
    !deny.tenant &&
    deny.groups.size === 0 &&
    deny.countries.size === 0 &&
    deny.legalEntities.size === 0 &&
    deny.operatingUnits.size === 0;

  return {
    tenantId,
    sourceRows: scopeRows.length,
    tenantWide,
    groups,
    countries,
    legalEntities,
    operatingUnits,
  };
}

function getScopeSetByType(scopeContext, scopeType) {
  if (scopeType === "GROUP") {
    return scopeContext.groups;
  }
  if (scopeType === "COUNTRY") {
    return scopeContext.countries;
  }
  if (scopeType === "LEGAL_ENTITY") {
    return scopeContext.legalEntities;
  }
  if (scopeType === "OPERATING_UNIT") {
    return scopeContext.operatingUnits;
  }
  return null;
}

function isScopeAllowed(scopeContext, requestedScope) {
  if (!requestedScope) {
    return (
      scopeContext.tenantWide ||
      scopeContext.groups.size > 0 ||
      scopeContext.countries.size > 0 ||
      scopeContext.legalEntities.size > 0 ||
      scopeContext.operatingUnits.size > 0
    );
  }

  if (requestedScope.scopeType === "TENANT") {
    return scopeContext.tenantWide;
  }

  const set = getScopeSetByType(scopeContext, requestedScope.scopeType);
  if (!set) {
    return false;
  }
  return set.has(requestedScope.scopeId);
}

async function getUserDataScopeRows(userId, tenantId) {
  try {
    const result = await query(
      `SELECT effect, scope_type, scope_id
       FROM data_scopes
       WHERE tenant_id = ?
         AND user_id = ?`,
      [tenantId, userId]
    );
    return result.rows || [];
  } catch (err) {
    if (err?.errno === 1146) {
      return [];
    }
    throw err;
  }
}

export async function invalidateRbacCache(tenantId) {
  const normalizedTenantId = parsePositiveInt(tenantId);
  if (!normalizedTenantId) {
    return;
  }

  const currentVersion = toPositiveVersion(memoryTenantVersionCache.get(normalizedTenantId));
  const nextVersion = currentVersion + 1;
  setMemoryTenantVersion(normalizedTenantId, nextVersion);
  purgeTenantScopedMemoryEntries(normalizedTenantId);

  const client = await getRedisClientForCache();
  if (!client) {
    return;
  }

  try {
    const redisVersion = await client.incr(buildTenantVersionRedisKey(normalizedTenantId));
    setMemoryTenantVersion(normalizedTenantId, toPositiveVersion(redisVersion));
  } catch (err) {
    useMemoryCacheFallback(err);
  }
}

export function getScopeContext(req) {
  return req.rbac?.scopeContext || null;
}

function scopeKeyFromKind(scopeKind) {
  const normalizedKind = String(scopeKind || "").toLowerCase();
  return SCOPE_KIND_TO_KEY[normalizedKind] || null;
}

export function hasScopeAccess(req, scopeKind, scopeId) {
  const context = getScopeContext(req);
  if (!context) {
    return false;
  }
  if (context.tenantWide) {
    return true;
  }

  const key = scopeKeyFromKind(scopeKind);
  const parsedId = parsePositiveInt(scopeId);
  if (!key || !parsedId) {
    return false;
  }

  return context[key].has(parsedId);
}

export function assertScopeAccess(req, scopeKind, scopeId, label = "scope") {
  if (!hasScopeAccess(req, scopeKind, scopeId)) {
    throw forbidden(`Access denied for ${label}`);
  }
}

export function buildScopeFilter(req, scopeKind, columnName, params) {
  const context = getScopeContext(req);
  if (!context) {
    return "1 = 0";
  }
  if (context.tenantWide) {
    return "1 = 1";
  }

  const key = scopeKeyFromKind(scopeKind);
  if (!key) {
    throw badRequest(`Unsupported scope kind: ${scopeKind}`);
  }

  const ids = Array.from(context[key]);
  if (ids.length === 0) {
    return "1 = 0";
  }

  params.push(...ids);
  return `${columnName} IN (${ids.map(() => "?").join(", ")})`;
}

export function requirePermission(permissionCode, options = {}) {
  const normalizedPermissionCode = String(permissionCode || "").trim();
  if (!normalizedPermissionCode) {
    throw new Error("permissionCode is required");
  }

  const resolveScope = options.resolveScope;

  return async (req, res, next) => {
    try {
      const userId = parsePositiveInt(req.user?.userId);
      if (!userId) {
        throw badRequest("Authenticated user is required");
      }

      const tenantId = resolveTenantId(req);
      if (!tenantId) {
        throw badRequest("tenantId is required");
      }

      const permissionBundle = await getPermissionBundleForRequest(
        req,
        userId,
        tenantId,
        normalizedPermissionCode
      );

      if (
        permissionBundle?.missingPermission ||
        !permissionBundle?.permissionScopeContext ||
        !permissionBundle?.scopeContext
      ) {
        throw forbidden(`Missing permission: ${normalizedPermissionCode}`);
      }

      const permissionScopeContext = permissionBundle.permissionScopeContext;

      let requestedScope = null;
      if (typeof resolveScope === "function") {
        const rawScope = await resolveScope(req, tenantId);
        requestedScope = normalizeScope(rawScope, tenantId);
      }

      if (!isScopeAllowed(permissionScopeContext, requestedScope)) {
        throw forbidden(`Missing permission: ${normalizedPermissionCode}`);
      }

      const scopeContext = permissionBundle.scopeContext;

      if (requestedScope && !isScopeAllowed(scopeContext, requestedScope)) {
        throw forbidden(`Data scope denied: ${normalizedPermissionCode}`);
      }
      if (!requestedScope && !isScopeAllowed(scopeContext, null)) {
        throw forbidden(`Data scope denied: ${normalizedPermissionCode}`);
      }

      req.rbac = {
        permissionCode: normalizedPermissionCode,
        tenantId,
        requestedScope,
        source: permissionBundle.source || "permission_scopes",
        permissionScopeContext,
        scopeContext,
      };

      return next();
    } catch (err) {
      return next(err);
    }
  };
}
